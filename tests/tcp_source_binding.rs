use futures_lite::future::{poll_once, zip};
use rivet::{
    Runtime, RuntimeConfig, SendPayload, SocketOptions, TcpListener, TcpStream, runtime, time,
};
use std::{
    future::Future,
    io,
    net::{Shutdown, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn config() -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 8;
    config.limits.max_sockets = 4;
    config.limits.max_operations = 8;
    config.limits.max_pending_accepts = 1;
    config.limits.max_pending_receives = 1;
    config.limits.pool.bytes = 1024 * 1024;
    config.limits.pool.block_size = 4096;
    config.limits.pool.max_leases = 64;
    config
}

fn configurations() -> impl Iterator<Item = RuntimeConfig> {
    [
        config(),
        #[cfg(all(target_os = "linux", feature = "fixed-files"))]
        config().enable(rivet::Optimization::FixedFiles),
        #[cfg(all(target_os = "linux", feature = "direct-descriptors"))]
        config().enable(rivet::Optimization::DirectDescriptors),
    ]
    .into_iter()
}

async fn deadline<F: Future>(future: F) -> F::Output {
    time::timeout(Duration::from_secs(10), future)
        .await
        .expect("outbound TCP source binding timed out")
}

fn reserve_source(reuse_address: bool) -> (socket2::Socket, SocketAddr) {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    socket.set_reuse_address(reuse_address).unwrap();
    let address: SocketAddr = "127.0.0.2:0".parse().unwrap();
    socket.bind(&address.into()).unwrap();
    let address = socket.local_addr().unwrap().as_socket().unwrap();
    (socket, address)
}

async fn assert_source_and_transfer(
    client: &TcpStream,
    accepted: &TcpStream,
    requested: SocketAddr,
) {
    let actual = client.local_addr();
    assert_eq!(actual.ip(), requested.ip());
    if requested.port() == 0 {
        assert_ne!(actual.port(), 0);
    } else {
        assert_eq!(actual, requested);
    }
    assert_eq!(accepted.peer_addr(), Some(actual));
    assert_eq!(client.peer_addr(), Some(accepted.local_addr()));

    let bytes = b"payload from the explicitly selected TCP source";
    let mut buffer = runtime::buffer_pool()
        .unwrap()
        .try_acquire_at_least(bytes.len())
        .unwrap();
    buffer.extend_from_slice(bytes).unwrap();
    assert_eq!(
        client
            .send_all(SendPayload::Single(buffer.freeze()))
            .await
            .result
            .unwrap(),
        bytes.len(),
    );
    client.shutdown(Shutdown::Write).unwrap();
    let mut received = Vec::new();
    while let Some(block) = accepted.recv().await.unwrap() {
        received.extend_from_slice(block.as_slice());
    }
    assert_eq!(received, bytes);
}

#[test]
fn explicit_source_ip_and_ephemeral_port_reach_the_accepted_peer() {
    let families = if std::env::var_os("RIVET_VERIFY_IPV4_ONLY").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        eprintln!("IPv6 not exercised: RIVET_VERIFY_IPV4_ONLY=1");
        1
    } else {
        2
    };
    for configuration in configurations() {
        for (source, listen) in [("127.0.0.2:0", "127.0.0.1:0"), ("[::1]:0", "[::1]:0")]
            .into_iter()
            .take(families)
        {
            let mut runtime = Runtime::new(configuration.clone()).unwrap();
            runtime.block_on(deadline(async {
                let source = source.parse().unwrap();
                let listener = TcpListener::bind(listen.parse().unwrap()).unwrap();
                let (client, accepted) = zip(
                    TcpStream::connect_from(
                        source,
                        listener.local_addr(),
                        SocketOptions::default(),
                    ),
                    listener.accept(),
                )
                .await;
                assert_source_and_transfer(&client.unwrap(), &accepted.unwrap(), source).await;
            }));
        }
    }
}

#[test]
fn unpolled_connections_are_lazy_and_the_specified_source_port_is_preserved() {
    for configuration in configurations() {
        // Both sockets opt into address reuse, but the reservation never listens
        // or connects. Keeping it bound avoids a free-port close/rebind race.
        let (reservation, source) = reserve_source(true);
        let calls = Arc::new(AtomicUsize::new(0));
        let hook_calls = calls.clone();
        let options = SocketOptions {
            reuse_address: true,
            hook: Some(Arc::new(
                move |_socket: rivet::socket::BorrowedSocket<'_>| {
                    hook_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )),
            ..SocketOptions::default()
        };
        let mut runtime = Runtime::new(configuration).unwrap();
        let listener =
            runtime.block_on(async { TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap() });
        let peer = listener.local_addr();

        // This future must obtain its runtime only when it is eventually polled.
        let connect = TcpStream::connect_from(source, peer, options.clone());
        drop(TcpStream::connect_from(source, peer, options.clone()));
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        runtime.block_on(deadline(async {
            drop(TcpStream::connect_from(source, peer, options.clone()));
            assert_eq!(calls.load(Ordering::Relaxed), 0);
            let (client, accepted) = zip(connect, listener.accept()).await;
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_source_and_transfer(&client.unwrap(), &accepted.unwrap(), source).await;
        }));
        drop(reservation);
    }
}

#[test]
fn occupied_source_port_never_falls_back_or_exhausts_admission() {
    for configuration in configurations() {
        let (occupied, source) = reserve_source(false);
        let options = SocketOptions {
            reuse_address: false,
            ..SocketOptions::default()
        };
        let mut runtime = Runtime::new(configuration).unwrap();
        runtime.block_on(deadline(async {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
            // More failures than either the socket/fixed-slot or operation limit
            // must still report the native bind error, not leaked admission.
            for attempt in 0..32 {
                let error = TcpStream::connect_from(source, listener.local_addr(), options.clone())
                    .await
                    .expect_err("an occupied source port must not fall back to another port");
                assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "attempt {attempt}");
            }

            // Keep the conflicting endpoint occupied while proving that the
            // same runtime can still create and use a fresh bound connection.
            let ephemeral = SocketAddr::new(source.ip(), 0);
            let (client, accepted) = zip(
                TcpStream::connect_from(ephemeral, listener.local_addr(), options),
                listener.accept(),
            )
            .await;
            assert_source_and_transfer(&client.unwrap(), &accepted.unwrap(), ephemeral).await;
        }));
        drop(occupied);
    }
}

#[test]
fn both_family_mismatches_are_rejected_before_native_hooks() {
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = calls.clone();
    let options = SocketOptions {
        hook: Some(Arc::new(
            move |_socket: rivet::socket::BorrowedSocket<'_>| {
                hook_calls.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "family validation must run before this hook",
                ))
            },
        )),
        ..SocketOptions::default()
    };
    let mut runtime = Runtime::new(config()).unwrap();
    runtime.block_on(deadline(async {
        // These IPv6 values are addresses only: neither case should create an
        // IPv6 socket or submit any connection, even in IPv4-only verification.
        for (source, peer) in [("127.0.0.2:0", "[::1]:9"), ("[::1]:0", "127.0.0.1:9")] {
            let error = poll_once(TcpStream::connect_from(
                source.parse().unwrap(),
                peer.parse().unwrap(),
                options.clone(),
            ))
            .await
            .expect("a family mismatch must be rejected on its first poll")
            .expect_err("mismatched local and peer address families must be rejected");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
    }));
}
