//! IPv4 native traits and resource observation: proxy, datagrams and execution.
use futures_lite::future::poll_once;
use futures_util::future::{Either, select, try_join};
use rivet::{
    BufferPool, Runtime, SendPayload, SocketOptions, TcpListener, TcpStream, UdpSocket,
    net::{DatagramRecv, DatagramSend, ServeConfig, StreamRecv, StreamSend, StreamShutdown},
    runtime::{self, BlockingSpawn, Current, LocalSpawn, Spawn},
    sync::{CancellationToken, mpsc},
    time::{self, Timer},
};
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::pin,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};
mod support;

const REQUEST: &[u8] = b"request through a generic Rivet proxy, followed by write EOF";
const RESPONSE: &[u8] = b"response is still readable after the client's write EOF";

fn failed(cause: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("{cause:?}"))
}

fn require(condition: bool, message: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message))
    }
}

fn local() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

fn payload(pool: &BufferPool, bytes: &[u8]) -> io::Result<SendPayload> {
    let mut buffer = pool.try_acquire_at_least(bytes.len().max(1))?;
    buffer.extend_from_slice(bytes)?;
    Ok(SendPayload::Single(buffer.freeze()))
}

async fn expect_bytes<R: StreamRecv + ?Sized>(reader: &R, expected: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while let Some(block) = reader.recv().await? {
        let remaining = &expected[offset..];
        require(
            block.len() <= remaining.len() && block.as_slice() == &remaining[..block.len()],
            "stream byte sequence differs from the expected response",
        )?;
        offset += block.len();
    }
    require(
        offset == expected.len(),
        "EOF arrived before all expected bytes",
    )
}

// No task, buffer allocation or payload copy is needed for each forwarded block.
async fn forward<R, W>(reader: &R, writer: &W) -> io::Result<usize>
where
    R: StreamRecv + ?Sized,
    W: StreamSend + StreamShutdown + ?Sized,
{
    let mut total = 0;
    while let Some(block) = reader.recv().await? {
        let length = block.len();
        let sent = writer.send_all(SendPayload::Single(block)).await;
        require(
            sent.result? == length && sent.data.is_empty(),
            "send_all did not consume its complete input",
        )?;
        writer.flush().await?;
        total += length;
    }
    writer.shutdown_write().await?;
    Ok(total)
}

async fn send_request<W: StreamSend + ?Sized>(writer: &W, pool: &BufferPool) -> io::Result<()> {
    // This application has one writer. Native send_all is preferable when other
    // concurrent sends must not interleave between short-write continuations.
    let mut remaining = payload(pool, REQUEST)?;
    while !remaining.is_empty() {
        let outcome = writer.send(remaining).await;
        let accepted = outcome.result?;
        if accepted == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "request made no progress",
            ));
        }
        remaining = outcome.data.remaining(accepted);
    }
    writer.flush().await
}

async fn respond<S>(stream: &S, pool: &BufferPool) -> io::Result<()>
where
    S: StreamRecv + StreamSend + StreamShutdown,
{
    // The response is deliberately withheld until the complete request AND EOF.
    expect_bytes(stream, REQUEST).await?;
    let sent = stream.send_all(payload(pool, RESPONSE)?).await;
    require(
        sent.result? == RESPONSE.len() && sent.data.is_empty(),
        "origin response was not fully accepted",
    )?;
    stream.flush().await?;
    stream.shutdown_write().await
}

async fn proxy_roundtrip<L: LocalSpawn>(executor: &L, pool: &BufferPool) -> io::Result<()> {
    let origin = TcpListener::bind(local())?;
    let origin_address = origin.local_addr();
    let outbound_source = SocketAddr::from((Ipv4Addr::new(127, 0, 0, 2), 0));
    let origin_task = executor
        .spawn_local(async move {
            let stream = origin.accept().await?;
            require(
                stream
                    .peer_addr()
                    .is_some_and(|peer| peer.ip() == outbound_source.ip()),
                "origin did not observe the requested proxy source address",
            )?;
            respond(&stream, &runtime::buffer_pool()?).await
        })
        .map_err(failed)?;

    let listener = TcpListener::bind(local())?;
    let proxy_address = listener.local_addr();
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let (results, completed) = mpsc::bounded(1);
    let serving =
        executor
            .spawn_local(async move {
                listener
                    .serve_until(
                        ServeConfig {
                            max_connections: 1,
                            shutdown_grace: Duration::from_secs(1),
                        },
                        stopping.cancelled(),
                        move |incoming, cancellation| {
                            let results = results.clone();
                            async move {
                                // Setup/routing belongs here, outside the generic transfer code.
                                // serve_until already placed this unused connection on its worker.
                                let transfer = async {
                                    let outgoing = TcpStream::connect_from(
                                        outbound_source,
                                        origin_address,
                                        SocketOptions::default(),
                                    )
                                    .await?;
                                    let actual_source = outgoing.local_addr();
                                    require(
                                        actual_source.ip() == outbound_source.ip()
                                            && actual_source.port() != 0,
                                        "proxy connection did not retain its selected source",
                                    )?;
                                    try_join(
                                        forward(&incoming, &outgoing),
                                        forward(&outgoing, &incoming),
                                    )
                                    .await
                                };
                                let result =
                                    match select(pin!(transfer), pin!(cancellation.cancelled()))
                                        .await
                                    {
                                        Either::Left((result, _)) => result,
                                        Either::Right(_) => Err(io::Error::new(
                                            io::ErrorKind::Interrupted,
                                            "proxy connection was stopped",
                                        )),
                                    };
                                // The caller owns the result; a dropped receiver means it is stopping.
                                let _ = results.send(result).await;
                            }
                        },
                    )
                    .await
            })
            .map_err(failed)?;

    let client = TcpStream::connect(proxy_address).await?;
    send_request(&client, pool).await?;
    StreamShutdown::shutdown_write(&client).await?;
    expect_bytes(&client, RESPONSE).await?;
    let (request, response) = completed.recv().await.map_err(failed)??;
    require(
        (request, response) == (REQUEST.len(), RESPONSE.len()),
        "proxy transfer totals differ from its input byte domains",
    )?;
    origin_task.await.map_err(failed)??;
    stop.cancel();
    serving.await.map_err(failed)??;
    println!(
        "TCP proxy: source={}, {request} request bytes, {response} response bytes after write EOF; supervised workers joined",
        outbound_source.ip()
    );
    Ok(())
}

async fn exchange_datagrams<D>(
    server: &D,
    client: &D,
    server_address: SocketAddr,
    client_address: SocketAddr,
    pool: &BufferPool,
) -> io::Result<()>
where
    D: DatagramRecv + DatagramSend,
{
    for bytes in [&b""[..], &b"native datagram"[..]] {
        let sent = client.send(payload(pool, bytes)?).await;
        require(sent.result? == bytes.len(), "datagram send length mismatch")?;
        let received = server.recv().await?;
        require(
            received.peer == Some(client_address)
                && !received.truncated
                && received.data.as_slice() == bytes,
            "datagram source, boundary or payload mismatch",
        )?;
        let echoed = server
            .send_to(SendPayload::Single(received.data), client_address)
            .await;
        require(
            echoed.result? == bytes.len(),
            "datagram reply length mismatch",
        )?;
        let reply = client.recv().await?;
        require(
            reply.peer == Some(server_address)
                && !reply.truncated
                && reply.data.as_slice() == bytes,
            "datagram reply source, boundary or payload mismatch",
        )?;
    }
    println!(
        "UDP traits: connected send and addressed replies preserve bytes, peers and an empty datagram"
    );
    Ok(())
}

fn print_datagram_resources(
    stage: &str,
    pool: &BufferPool,
    server: &UdpSocket,
    client: &UdpSocket,
) -> io::Result<()> {
    // Diagnostics stay on the concrete sockets, outside the generic transfer
    // traits. These synchronous observations do not poll or replenish receives.
    let worker = runtime::resource_snapshot()?;
    let usage = pool.usage();
    let server = server.receive_snapshot()?;
    let client = client.receive_snapshot()?;
    require(
        server.worker() == worker.worker()
            && client.worker() == worker.worker()
            && *worker.pool() == usage,
        "UDP diagnostics did not describe their current owner and pool",
    )?;
    println!(
        "UDP resources ({stage}; current worker only, not a runtime total):\n  worker: {worker:?}\n  pool: {usage:?}\n  server receive: {server:?}\n  client receive: {client:?}"
    );
    Ok(())
}

async fn execution<S, L, T>(handle: &S, local: &L, timer: &T) -> io::Result<()>
where
    S: Spawn + BlockingSpawn,
    L: LocalSpawn,
    T: Timer,
{
    let marker = Rc::new(thread::current().id());
    let captured = marker.clone();
    let local_task = local
        .spawn_local(async move {
            runtime::yield_now().await;
            captured
        })
        .map_err(failed)?;
    let returned = local_task.await.map_err(failed)?;
    require(
        Rc::ptr_eq(&marker, &returned),
        "local task lost its non-Send output",
    )?;

    let factory = handle
        .spawn(|| async {
            let owner = Rc::new(thread::current().id());
            runtime::yield_now().await;
            require(
                *owner == thread::current().id(),
                "factory future changed worker",
            )?;
            Ok::<_, io::Error>(*owner)
        })
        .map_err(failed)?;
    let factory_worker = factory.await.map_err(failed)??;
    let blocking_worker = handle
        .spawn_blocking(|| thread::current().id())
        .map_err(failed)?
        .await
        .map_err(failed)?;
    require(
        blocking_worker != *marker,
        "blocking work ran on the async caller",
    )?;

    let mut sleep = timer.sleep_until(Instant::now() + Duration::from_secs(3600));
    require(
        poll_once(&mut sleep).await.is_none(),
        "distant timer completed early",
    )?;
    sleep.reset(Instant::now())?;
    (&mut sleep).await?;
    sleep.reset(Instant::now() + Duration::from_millis(1))?;
    sleep.await?;
    timer.sleep(Duration::from_millis(1)).await?;
    println!(
        "Execution traits: non-Send local output, factory on {factory_worker:?}, blocking on {blocking_worker:?}, active/completed timer reset"
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let mut runtime = Runtime::new(support::configuration()?)?;
    let pool = runtime.buffer_pool();
    let handle = runtime.handle();
    println!("IPv4 loopback only; IPv6 not exercised; host TUN/routing configuration is unchanged");
    runtime.block_on(async {
        time::timeout(Duration::from_secs(30), async {
            execution(&handle, &Current, &Current).await?;
            proxy_roundtrip(&Current, &pool).await?;
            let server = UdpSocket::bind(local())?;
            let client =
                UdpSocket::bind_connected(local(), server.local_addr(), SocketOptions::udp())?;
            print_datagram_resources("after bind", &pool, &server, &client)?;
            exchange_datagrams(
                &server,
                &client,
                server.local_addr(),
                client.local_addr(),
                &pool,
            )
            .await?;
            print_datagram_resources("after exchange", &pool, &server, &client)
        })
        .await
        .map_err(failed)?
    })?;
    drop(runtime);
    println!(
        "PASS: native traits, IPv4 proxy half-close, UDP metadata and resource snapshots, task ownership and reusable timers"
    );
    Ok(())
}
