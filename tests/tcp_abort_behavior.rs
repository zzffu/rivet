use futures_lite::future::poll_once;
use rivet::{
    Runtime, RuntimeConfig, SendPayload, SocketOptions, TcpListener, runtime, sync::oneshot, time,
};
use std::{
    future::Future,
    io::{self, Read},
    thread,
    time::Duration,
};

fn config() -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 16;
    config.limits.max_sockets = 16;
    config.limits.max_operations = 64;
    config.limits.max_pending_accepts = 2;
    config.limits.max_pending_receives = 2;
    config.limits.pool.bytes = 4 * 1024 * 1024;
    config.limits.pool.block_size = 4096;
    config.limits.pool.max_leases = 128;
    config
}

async fn deadline<F: Future>(future: F) -> F::Output {
    time::timeout(Duration::from_secs(10), future)
        .await
        .expect("native abortive close timed out")
}

fn native_close_result(config: RuntimeConfig, abort: bool) -> io::Result<usize> {
    let mut runtime = Runtime::new(config).unwrap();
    let (result, peer) = runtime.block_on(deadline(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let (send_result, receive_result) = oneshot::channel();
        let peer = thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            send_result.send(stream.read(&mut [0; 1])).unwrap();
        });
        let stream = listener.accept().await.unwrap();
        if abort {
            stream.abort().unwrap();
        } else {
            drop(stream);
        }
        (receive_result.await.unwrap(), peer)
    }));
    peer.join().unwrap();
    result
}

#[test]
fn abort_reports_native_reset_while_ordinary_drop_still_reports_eof() {
    assert_eq!(native_close_result(config(), false).unwrap(), 0);
    assert_eq!(
        native_close_result(config(), true).unwrap_err().kind(),
        io::ErrorKind::ConnectionReset,
    );
}

#[cfg(unix)]
#[test]
fn abort_disconnects_imported_stream_while_native_alias_remains_open() {
    let mut runtime = Runtime::new(config()).unwrap();
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let listener = std::net::TcpListener::bind(address).unwrap();
        let native = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let alias = native.try_clone().unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let peer = thread::spawn(move || peer.read(&mut [0; 1]));

        runtime.block_on(async {
            rivet::TcpStream::import(native.into(), SocketOptions::default())
                .unwrap()
                .abort()
                .unwrap();
        });
        assert_eq!(
            peer.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionReset,
            "abort must disconnect {address} before the last native alias closes",
        );
        drop(alias);
    }
}

#[cfg(all(target_os = "linux", feature = "direct-descriptors"))]
#[test]
fn direct_and_fixed_socket_references_do_not_turn_abort_into_fin() {
    let config = config().enable(rivet::Optimization::DirectDescriptors);
    assert_eq!(
        native_close_result(config, true).unwrap_err().kind(),
        io::ErrorKind::ConnectionReset,
    );
}

fn abort_with_inflight_send(configuration: RuntimeConfig) {
    const BYTES: usize = 2 * 1024 * 1024;
    let mut runtime = Runtime::new(configuration).unwrap();
    let (guard, peer) = runtime.block_on(deadline(async {
        let options = SocketOptions {
            send_buffer_bytes: Some(4096),
            ..SocketOptions::default()
        };
        let listener =
            TcpListener::bind_with_options("127.0.0.1:0".parse().unwrap(), options).unwrap();
        let address = listener.local_addr();
        let (arrived, first_byte) = oneshot::channel();
        let (allow_read, read_allowed) = std::sync::mpsc::sync_channel(1);
        let (finish, finished) = oneshot::channel();
        let peer = thread::spawn(move || {
            let socket = socket2::Socket::new(
                socket2::Domain::IPV4,
                socket2::Type::STREAM,
                Some(socket2::Protocol::TCP),
            )
            .unwrap();
            socket.set_recv_buffer_size(1024).unwrap();
            socket.connect(&address.into()).unwrap();
            let mut stream = std::net::TcpStream::from(socket);
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut first = [0; 1];
            stream.read_exact(&mut first).unwrap();
            assert_eq!(first, [0x5a]);
            arrived.send(()).unwrap();
            read_allowed.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut bytes = [0; 8192];
            loop {
                match stream.read(&mut bytes) {
                    Ok(0) => panic!("abortive close produced ordinary EOF"),
                    Ok(count) => assert!(bytes[..count].iter().all(|byte| *byte == 0x5a)),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        finish.send(error.kind()).unwrap();
                        break;
                    }
                }
            }
        });
        let stream = listener.accept().await.unwrap();
        let pool = runtime::buffer_pool().unwrap();
        let mut write = pool.try_acquire_at_least(BYTES).unwrap();
        write.extend_from_slice(&vec![0x5a; BYTES]).unwrap();
        let data = write.freeze();
        let guard = data.clone();
        let mut send = stream.send_all(SendPayload::Single(data));
        if let Some(outcome) = poll_once(&mut send).await {
            outcome.result.unwrap();
        }
        first_byte.await.unwrap();
        drop(send);
        stream.abort().unwrap();
        // Releasing the application send future/connection must not expose a
        // writable alias while a driver still owns a native send reference.
        assert!(guard.as_slice().iter().all(|byte| *byte == 0x5a));
        allow_read.send(()).unwrap();
        assert_eq!(finished.await.unwrap(), io::ErrorKind::ConnectionReset);
        (guard, peer)
    }));
    peer.join().unwrap();
    drop(runtime);
    let mut reclaimed = guard
        .try_into_write()
        .expect("native send guards survived runtime drain");
    assert_eq!(reclaimed.as_slice()[0], 0x5a);
    reclaimed.clear();
    reclaimed
        .extend_from_slice(b"reclaimed after abort")
        .unwrap();
    assert_eq!(reclaimed.as_slice(), b"reclaimed after abort");
}

#[test]
fn abort_reclaims_inflight_send_storage_only_through_native_convergence() {
    abort_with_inflight_send(config());
}

#[cfg(all(target_os = "linux", feature = "zc-tx", feature = "direct-descriptors"))]
#[test]
fn direct_zero_copy_abort_retains_send_storage_until_real_notifications() {
    let mut config = config()
        .enable(rivet::Optimization::DirectDescriptors)
        .enable(rivet::Optimization::ZcTx);
    config.linux.zc_send_threshold = 0;
    abort_with_inflight_send(config);
}
