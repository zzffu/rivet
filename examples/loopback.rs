//! Real TCP/UDP smoke scenario, not a throughput benchmark.
use futures_lite::future::zip;
use rivet::{
    BufferPool, Optimization, Policy, ReadBuf, Runtime, SendPayload, TcpListener, TcpStream,
    UdpSocket,
};
use std::{
    collections::HashSet,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    rc::Rc,
    time::Duration,
};
mod support;
use support::{configuration, error};

const BULK_BYTES: usize = 1024 * 1024;
const PREFIX: &[u8] = b"rivet";

async fn echo(stream: TcpStream) -> io::Result<usize> {
    let mut total = 0;
    while let Some(data) = stream.recv().await? {
        let size = data.len();
        let outcome = stream.send_all(SendPayload::Single(data)).await;
        if outcome.result? != size {
            return Err(error("send_all did not consume the complete echo chunk"));
        }
        total += size;
    }
    Ok(total)
}

async fn tcp_roundtrip(ip: IpAddr, pool: &BufferPool) -> io::Result<ReadBuf> {
    let listener = TcpListener::bind(SocketAddr::new(ip, 0))?;
    let address = listener.local_addr();
    let serving = rivet::spawn_local(async move { echo(listener.accept().await?).await })
        .map_err(|e| error(format!("spawn_local failed: {e:?}")))?;
    let client = TcpStream::connect(address).await?;

    // Dropping this waiting future must not destroy the persistent receive queue.
    if rivet::time::timeout(Duration::from_millis(10), client.recv())
        .await
        .is_ok()
    {
        return Err(error("idle TCP receive unexpectedly completed"));
    }
    let mut first_part = pool.try_acquire()?;
    let mut second_part = pool.try_acquire()?;
    first_part.extend_from_slice(&PREFIX[..2])?;
    second_part.extend_from_slice(&PREFIX[2..])?;
    let sent = client
        .send_all(SendPayload::Vectored(vec![
            first_part.freeze(),
            second_part.freeze(),
        ]))
        .await;
    if sent.result? != PREFIX.len() {
        return Err(error("vectored prefix length mismatch"));
    }
    rivet::time::sleep(Duration::from_millis(10)).await?;
    let retained = client
        .recv()
        .await?
        .ok_or_else(|| error("EOF before echoed prefix"))?;
    if retained.is_empty()
        || retained.len() > PREFIX.len()
        || retained.as_slice() != &PREFIX[..retained.len()]
    {
        return Err(error("echoed prefix corrupted after receive cancellation"));
    }

    let mut expected = Vec::with_capacity(PREFIX.len() + BULK_BYTES);
    expected.extend_from_slice(PREFIX);
    expected.extend((0..BULK_BYTES).map(|index| (index.wrapping_mul(31) ^ (index >> 8)) as u8));
    let writer = async {
        let mut written = PREFIX.len();
        for chunk in expected[PREFIX.len()..].chunks(16 * 1024) {
            let mut buffer = pool.try_acquire_at_least(chunk.len())?;
            buffer.extend_from_slice(chunk)?;
            let outcome = client.send_all(SendPayload::Single(buffer.freeze())).await;
            let count = outcome.result?;
            if count != chunk.len() {
                return Err(error("bulk send_all length mismatch"));
            }
            written += count;
        }
        Ok::<_, io::Error>(written)
    };
    let reader = async {
        // Keep the first lease alive throughout subsequent receives. Spare pool
        // budget must be usable even when an earlier chunk remains borrowed.
        let mut read = retained.len();
        while read < expected.len() {
            let data = client.recv().await?.ok_or_else(|| error("early TCP EOF"))?;
            let end = read
                .checked_add(data.len())
                .ok_or_else(|| error("receive length overflow"))?;
            if end > expected.len() || data.as_slice() != &expected[read..end] {
                return Err(error("bulk TCP byte sequence mismatch"));
            }
            read = end;
        }
        Ok::<_, io::Error>(read)
    };
    let (written, read) = zip(writer, reader).await;
    if written? != expected.len() || read? != expected.len() {
        return Err(error("TCP total mismatch"));
    }
    drop(client);
    let echoed = serving
        .await
        .map_err(|e| error(format!("echo task failed: {e:?}")))??;
    if echoed != expected.len() {
        return Err(error("server byte total mismatch"));
    }
    println!(
        "TCP {ip}: {echoed} bytes each direction; vectored prefix, cancelled waiter, retained lease"
    );
    Ok(retained)
}

async fn udp_roundtrip(ip: IpAddr, pool: &BufferPool) -> io::Result<()> {
    let server = UdpSocket::bind(SocketAddr::new(ip, 0))?;
    let client = UdpSocket::bind(SocketAddr::new(ip, 0))?;
    let server_address = server.local_addr();
    let client_address = client.local_addr();
    for length in [0, 1, 63, 1200, 8192] {
        let expected: Vec<u8> = (0..length)
            .map(|index| (index * 17 + length) as u8)
            .collect();
        let mut buffer = pool.try_acquire_at_least(length.max(1))?;
        buffer.extend_from_slice(&expected)?;
        let sent = client
            .send_to(SendPayload::Single(buffer.freeze()), server_address)
            .await;
        if sent.result? != length {
            return Err(error("UDP send length mismatch"));
        }
        let received = server.recv().await?;
        if received.peer != Some(client_address)
            || received.truncated
            || received.data.as_slice() != expected
        {
            return Err(error(
                "UDP payload, source address, or truncation metadata mismatch",
            ));
        }
        let reply = server
            .send_to(SendPayload::Single(received.data), client_address)
            .await;
        if reply.result? != length {
            return Err(error("UDP echo send length mismatch"));
        }
        let echoed = client.recv().await?;
        if echoed.peer != Some(server_address)
            || echoed.truncated
            || echoed.data.as_slice() != expected
        {
            return Err(error("UDP reply payload or metadata mismatch"));
        }
    }
    println!("UDP {ip}: payload/source boundaries verified, including a zero-length datagram");
    Ok(())
}

fn main() -> io::Result<()> {
    let mut runtime = Runtime::new(configuration()?)?;
    for report in runtime.capabilities() {
        println!(
            "worker={} backend={} kernel={:?} receive_mode={:?}",
            report.worker, report.backend, report.kernel, report.receive_mode
        );
        for state in report
            .states()
            .iter()
            .filter(|state| state.policy != Policy::Off)
        {
            println!(
                "  {} policy={:?} compiled={} supported={} enabled={} reason={:?}",
                state.optimization,
                state.policy,
                state.compiled,
                state.supported,
                state.enabled,
                state.reason
            );
        }
    }
    let pool = runtime.buffer_pool();
    let handle = runtime.handle();
    let ipv4_only =
        std::env::var_os("RIVET_VERIFY_IPV4_ONLY").as_deref() == Some(std::ffi::OsStr::new("1"));
    if ipv4_only {
        println!("IPv6 not exercised: RIVET_VERIFY_IPV4_ONLY=1");
    }
    let retained = runtime.block_on(async {
        rivet::time::timeout(Duration::from_secs(60), async {
            let mut jobs = Vec::new();
            for _ in 0..8 {
                jobs.push(handle.spawn(|| async {
                    let owner = Rc::new(std::thread::current().id());
                    rivet::time::sleep(Duration::from_millis(2)).await?;
                    if *owner != std::thread::current().id() { return Err(error("local future moved between threads")); }
                    Ok::<_, io::Error>(*owner)
                }).map_err(|e| error(format!("automatic placement failed: {e:?}")))?);
            }
            let mut threads = HashSet::new();
            for job in jobs {
                threads.insert(job.await.map_err(|e| error(format!("factory task failed: {e:?}")))??);
            }
            println!("automatic factories progressed on {} worker thread(s); local futures retained their owner", threads.len());
            let ipv4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
            let mut retained = tcp_roundtrip(ipv4, &pool).await?;
            udp_roundtrip(ipv4, &pool).await?;
            if !ipv4_only {
                let ipv6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
                retained = tcp_roundtrip(ipv6, &pool).await?;
                udp_roundtrip(ipv6, &pool).await?;
            }
            Ok::<_, io::Error>(retained)
        }).await.map_err(|e| error(format!("loopback deadline: {e:?}")))?
    })?;
    if runtime.capabilities()[0].enabled(Optimization::ZcObserve) {
        println!(
            "root-driver observations (shared ZCRX counters may be instance-wide): {:?}",
            runtime.zc_stats()
        );
    }
    drop(runtime);
    if retained.as_slice() != &PREFIX[..retained.len()] {
        return Err(error("receive lease did not survive Runtime shutdown"));
    }
    drop(retained);
    println!(
        "PASS: selected address families TCP+UDP, owner-local tasks, cancellation, retained buffers, drained shutdown"
    );
    Ok(())
}
