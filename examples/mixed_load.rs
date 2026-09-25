//! Concurrent loopback RPC, bulk TCP and batched UDP. Not NIC throughput evidence.
use futures_lite::future::zip;
use parking_lot::Mutex;
use rivet::{
    BufferPool, Runtime, SendPayload, SocketOptions, TcpListener, TcpStream, UdpSocket,
    net::{Datagram, Received},
    runtime,
};
use std::{
    array, io,
    net::{Ipv4Addr, Shutdown, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
mod support;
use support::{configuration, error};

const RPC_ROUNDS: usize = 512;
const BULK_BYTES: usize = 8 * 1024 * 1024;
const UDP_WINDOW: usize = 32;
const UDP_ROUNDS: usize = 64;
const UDP_BYTES: usize = 256;
type Failures = Arc<Mutex<Vec<String>>>;

fn local() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}
fn task_error(error_value: impl std::fmt::Debug) -> io::Error {
    error(format!("task failed: {error_value:?}"))
}

async fn echo_stream(stream: TcpStream) -> io::Result<()> {
    while let Some(data) = stream.recv().await? {
        let expected = data.len();
        let outcome = stream.send_all(SendPayload::Single(data)).await;
        if outcome.result? != expected {
            return Err(error("TCP echo short send_all"));
        }
    }
    stream.shutdown(Shutdown::Write)
}

async fn tcp_server(failures: Failures) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(local())?;
    let address = listener.local_addr();
    rivet::spawn_local(async move {
        let handler_failures = failures.clone();
        if let Err(e) = listener
            .serve(move |stream| {
                let failures = handler_failures.clone();
                async move {
                    if let Err(e) = echo_stream(stream).await {
                        failures.lock().push(e.to_string());
                    }
                }
            })
            .await
        {
            failures.lock().push(e.to_string());
        }
    })
    .map_err(task_error)?
    .detach();
    Ok(address)
}

fn check_batch(batch: &mut [Datagram], length: usize) -> io::Result<()> {
    for packet in batch {
        let outcome = packet
            .take_outcome()
            .ok_or_else(|| error("batch lost a per-packet outcome"))?;
        if outcome.result? != length {
            return Err(error("datagram was partially sent"));
        }
    }
    Ok(())
}

async fn echo_datagrams(socket: UdpSocket) -> io::Result<()> {
    let mut incoming: [Option<Received>; UDP_WINDOW] = array::from_fn(|_| None);
    let mut replies = Vec::with_capacity(UDP_WINDOW);
    loop {
        let count = socket.recv_batch(&mut incoming).await?;
        replies.clear();
        for slot in &mut incoming[..count] {
            let received = slot
                .take()
                .ok_or_else(|| error("receive batch has an empty completed slot"))?;
            if received.truncated || received.data.len() != UDP_BYTES {
                return Err(error("UDP echo input boundary mismatch"));
            }
            let peer = received
                .peer
                .ok_or_else(|| error("UDP source address missing"))?;
            replies.push(Datagram::new(
                SendPayload::Single(received.data),
                Some(peer),
            ));
        }
        if socket.send_batch(&mut replies).await? != count {
            return Err(error("UDP echo batch completion count mismatch"));
        }
        check_batch(&mut replies, UDP_BYTES)?;
    }
}

async fn udp_server(failures: Failures) -> io::Result<SocketAddr> {
    let mut options = SocketOptions::udp();
    options.receive_chunk = UDP_BYTES;
    options.receive_buffer_bytes = Some(1024 * 1024);
    let socket = UdpSocket::bind_with_options(local(), options)?;
    let address = socket.local_addr();
    rivet::spawn_local(async move {
        if let Err(e) = echo_datagrams(socket).await {
            failures.lock().push(e.to_string());
        }
    })
    .map_err(task_error)?
    .detach();
    Ok(address)
}

async fn read_exact_bytes(stream: &TcpStream, expected: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < expected.len() {
        let data = stream
            .recv()
            .await?
            .ok_or_else(|| error("TCP response ended early"))?;
        let end = offset + data.len();
        if end > expected.len() || data.as_slice() != &expected[offset..end] {
            return Err(error("TCP response content mismatch"));
        }
        offset = end;
    }
    Ok(())
}

async fn rpc_client(address: SocketAddr) -> io::Result<Duration> {
    let stream = TcpStream::connect(address).await?;
    let pool = runtime::buffer_pool()?;
    let mut maximum = Duration::ZERO;
    let mut payload = [0x5au8; 128];
    for sequence in 0..RPC_ROUNDS {
        payload[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
        let mut data = pool.try_acquire_at_least(payload.len())?;
        data.extend_from_slice(&payload)?;
        let started = Instant::now();
        let outcome = stream.send_all(SendPayload::Single(data.freeze())).await;
        if outcome.result? != payload.len() {
            return Err(error("RPC request length mismatch"));
        }
        read_exact_bytes(&stream, &payload).await?;
        maximum = maximum.max(started.elapsed());
    }
    stream.shutdown(Shutdown::Write)?;
    if stream.recv().await?.is_some() {
        return Err(error("unexpected bytes after last RPC response"));
    }
    Ok(maximum)
}

async fn bulk_client(address: SocketAddr, expected: Arc<Vec<u8>>) -> io::Result<usize> {
    let stream = TcpStream::connect(address).await?;
    let pool = runtime::buffer_pool()?;
    let writer = async {
        for chunk in expected.chunks(16 * 1024) {
            let mut data = pool.try_acquire_at_least(chunk.len())?;
            data.extend_from_slice(chunk)?;
            let outcome = stream.send_all(SendPayload::Single(data.freeze())).await;
            if outcome.result? != chunk.len() {
                return Err(error("bulk TCP short send_all"));
            }
        }
        stream.shutdown(Shutdown::Write)
    };
    let reader = async {
        read_exact_bytes(&stream, &expected).await?;
        if stream.recv().await?.is_some() {
            return Err(error("bulk TCP exceeded expected length"));
        }
        Ok::<_, io::Error>(())
    };
    let (written, received) = zip(writer, reader).await;
    written?;
    received?;
    Ok(expected.len())
}

fn datagram(pool: &BufferPool, sequence: usize) -> io::Result<Datagram> {
    let mut bytes = [0u8; UDP_BYTES];
    bytes[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
    for (index, byte) in bytes[8..].iter_mut().enumerate() {
        *byte = (sequence.wrapping_mul(19) + index) as u8;
    }
    let mut data = pool.try_acquire_at_least(bytes.len())?;
    data.extend_from_slice(&bytes)?;
    Ok(Datagram::new(SendPayload::Single(data.freeze()), None))
}

async fn udp_client(address: SocketAddr) -> io::Result<usize> {
    let mut options = SocketOptions::udp();
    options.receive_chunk = UDP_BYTES;
    options.receive_buffer_bytes = Some(1024 * 1024);
    let socket = UdpSocket::bind_connected(local(), address, options)?;
    let pool = runtime::buffer_pool()?;
    let mut outgoing = Vec::with_capacity(UDP_WINDOW);
    let mut incoming: [Option<Received>; UDP_WINDOW] = array::from_fn(|_| None);
    for round in 0..UDP_ROUNDS {
        outgoing.clear();
        let first = round * UDP_WINDOW;
        for offset in 0..UDP_WINDOW {
            outgoing.push(datagram(&pool, first + offset)?);
        }
        if socket.send_batch(&mut outgoing).await? != UDP_WINDOW {
            return Err(error("UDP client batch completion mismatch"));
        }
        check_batch(&mut outgoing, UDP_BYTES)?;
        let mut seen = [false; UDP_WINDOW];
        let mut received = 0;
        while received < UDP_WINDOW {
            let count = socket.recv_batch(&mut incoming).await?;
            for slot in &mut incoming[..count] {
                let packet = slot
                    .take()
                    .ok_or_else(|| error("missing UDP batch result"))?;
                if packet.peer != Some(address)
                    || packet.truncated
                    || packet.data.len() != UDP_BYTES
                {
                    return Err(error("UDP reply source/length/truncation mismatch"));
                }
                let sequence =
                    u64::from_le_bytes(packet.data.as_slice()[..8].try_into().unwrap()) as usize;
                let offset = sequence
                    .checked_sub(first)
                    .filter(|offset| *offset < UDP_WINDOW)
                    .ok_or_else(|| error("UDP reply belongs to a different window"))?;
                if std::mem::replace(&mut seen[offset], true) {
                    return Err(error("duplicate UDP reply"));
                }
                if packet.data.as_slice()[8..]
                    .iter()
                    .enumerate()
                    .any(|(index, &byte)| byte != (sequence.wrapping_mul(19) + index) as u8)
                {
                    return Err(error("UDP payload corrupted"));
                }
                received += 1;
            }
        }
    }
    Ok(UDP_WINDOW * UDP_ROUNDS)
}

fn main() -> io::Result<()> {
    let mut config = configuration()?;
    config.limits.max_pending_receives = 4 * UDP_WINDOW;
    let mut runtime = Runtime::new(config)?;
    let handle = runtime.handle();
    let failures = Arc::new(Mutex::new(Vec::new()));
    let started = Instant::now();
    let (maximum_rpc, tcp_bytes, datagrams) = runtime.block_on(async {
        rivet::time::timeout(Duration::from_secs(90), async {
            let errors = failures.clone();
            let tcp_address = handle
                .spawn(move || tcp_server(errors))
                .map_err(task_error)?
                .await
                .map_err(task_error)??;
            let errors = failures.clone();
            let udp_address = handle
                .spawn(move || udp_server(errors))
                .map_err(task_error)?
                .await
                .map_err(task_error)??;
            let expected = Arc::new(
                (0..BULK_BYTES)
                    .map(|index| (index.wrapping_mul(29) ^ (index >> 7) ^ (index >> 17)) as u8)
                    .collect::<Vec<_>>(),
            );
            let mut rpc_jobs = Vec::new();
            let mut bulk_jobs = Vec::new();
            let mut udp_jobs = Vec::new();
            for _ in 0..8 {
                rpc_jobs.push(
                    handle
                        .spawn(move || rpc_client(tcp_address))
                        .map_err(task_error)?,
                );
            }
            for _ in 0..2 {
                let expected = expected.clone();
                bulk_jobs.push(
                    handle
                        .spawn(move || bulk_client(tcp_address, expected))
                        .map_err(task_error)?,
                );
            }
            for _ in 0..4 {
                udp_jobs.push(
                    handle
                        .spawn(move || udp_client(udp_address))
                        .map_err(task_error)?,
                );
            }
            let mut maximum_rpc = Duration::ZERO;
            for job in rpc_jobs {
                maximum_rpc = maximum_rpc.max(job.await.map_err(task_error)??);
            }
            let mut tcp_bytes = 0;
            for job in bulk_jobs {
                tcp_bytes += job.await.map_err(task_error)??;
            }
            let mut datagrams = 0;
            for job in udp_jobs {
                datagrams += job.await.map_err(task_error)??;
            }
            Ok::<_, io::Error>((maximum_rpc, tcp_bytes, datagrams))
        })
        .await
        .map_err(task_error)?
    })?;
    let elapsed = started.elapsed();
    drop(runtime);
    let failures = failures.lock();
    if !failures.is_empty() {
        return Err(error(format!("server failures: {failures:?}")));
    }
    println!(
        "PASS mixed loopback: {} small RPC exchanges, {} bulk bytes each direction, {} UDP datagrams each direction",
        8 * RPC_ROUNDS,
        tcp_bytes,
        datagrams
    );
    println!(
        "elapsed={elapsed:?}, maximum observed RPC roundtrip={maximum_rpc:?}; not a NIC throughput or latency guarantee"
    );
    Ok(())
}
