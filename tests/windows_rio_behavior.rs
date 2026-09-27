#![cfg(target_os = "windows")]

use futures_lite::future::{poll_once, race, zip};
use rivet::{
    BufferPool, Runtime, RuntimeConfig, SendBuf, SendPayload, SocketOptions, TcpListener,
    TcpStream, UdpSocket, sync::oneshot,
};
use std::{
    future::{Future, poll_fn},
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr},
    os::windows::io::{AsRawSocket, FromRawSocket, OwnedSocket},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::Networking::WinSock::*;
mod support;
use support::registered_udp;

fn config() -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_sockets = 32;
    config.limits.max_operations = 128;
    config.limits.pool.bytes = 2 * 1024 * 1024;
    config.limits.pool.block_size = 1024;
    config.limits.pool.max_leases = 512;
    config.limits.max_pending_receives = 4;
    config.limits.max_pending_accepts = 4;
    config
}

fn filled(pool: &BufferPool, bytes: &[u8]) -> SendBuf {
    let mut buffer = pool.try_acquire_at_least(bytes.len()).unwrap();
    buffer.extend_from_slice(bytes).unwrap();
    buffer.freeze()
}

async fn deadline<F: Future>(future: F) -> F::Output {
    rivet::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("native RIO operation timed out")
}

async fn pair(address: SocketAddr, options: SocketOptions) -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind_with_options(address, options.clone()).unwrap();
    let (client, server) = deadline(zip(
        TcpStream::connect_with_options(listener.local_addr(), options),
        listener.accept(),
    ))
    .await;
    (client.unwrap(), server.unwrap())
}

#[test]
fn tcp_vectors_preserve_bytes_and_held_receives_across_half_close() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    for address in ["127.0.0.1:0", "[::1]:0"]
        .into_iter()
        .take(support::family_count())
    {
        runtime.block_on(deadline(async {
            let options = SocketOptions {
                receive_chunk: 2048,
                send_buffer_bytes: Some(4096),
                ..SocketOptions::default()
            };
            let (client, server) = pair(address.parse().unwrap(), options).await;
            let expected: Vec<u8> = (0..512 * 1024)
                .map(|n| ((n * 37 + n / 251) % 256) as u8)
                .collect();
            let payload = SendPayload::Vectored(
                expected
                    .chunks(32 * 1024)
                    .map(|part| filled(&pool, part))
                    .collect(),
            );
            let sender = async {
                let outcome = client.send_all(payload).await;
                assert_eq!(outcome.result.unwrap(), expected.len());
                assert!(outcome.data.is_empty());
                client.shutdown(Shutdown::Write).unwrap();
                let mut reply = Vec::new();
                while let Some(data) = client.recv().await.unwrap() {
                    reply.extend_from_slice(data.as_slice());
                }
                assert_eq!(reply, b"reply after peer EOF");
            };
            let receiver = async {
                let first = server.recv().await.unwrap().unwrap();
                assert_eq!(first.as_slice(), &expected[..first.len()]);
                let first_snapshot = first.as_slice().to_vec();
                // Holding this lease must not freeze the entire receive stream.
                let second = server.recv().await.unwrap().unwrap();
                let mut actual = first.as_slice().to_vec();
                actual.extend_from_slice(second.as_slice());
                drop(second);
                while let Some(data) = server.recv().await.unwrap() {
                    actual.extend_from_slice(data.as_slice());
                }
                assert_eq!(actual, expected);
                assert_eq!(first.as_slice(), first_snapshot);
                let outcome = server
                    .send_all(SendPayload::Single(filled(&pool, b"reply after peer EOF")))
                    .await;
                assert_eq!(outcome.result.unwrap(), 20);
                server.shutdown(Shutdown::Write).unwrap();
            };
            zip(sender, receiver).await;
        }));
    }
}

#[test]
fn tcp_backpressure_in_each_direction_preserves_reverse_progress_and_half_close() {
    const BYTES: usize = 2 * 1024 * 1024;
    const REVERSE_MARKER: &[u8] = b"receive while RIO send is blocked";
    const FORWARD_MARKER: &[u8] = b"send while receive queue is full";
    const PEER_WAIT: Duration = Duration::from_secs(5);

    fn fill_until_blocked(peer: &mut std::net::TcpStream, bytes: &[u8], sent: &mut usize) {
        let until = Instant::now() + PEER_WAIT;
        loop {
            assert!(
                Instant::now() < until,
                "native send did not reach backpressure"
            );
            assert!(
                *sent < bytes.len(),
                "bounded output never reached backpressure"
            );
            // Winsock may accept one oversized write despite SO_SNDBUF.
            // Probe fresh admission in bounded buffer-sized writes instead.
            let end = (*sent + 4096).min(bytes.len());
            match peer.write(&bytes[*sent..end]) {
                Ok(0) => panic!("native send made no progress"),
                Ok(count) => *sent += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(error) => panic!("native send failed: {error}"),
            }
        }
    }

    let forward_bytes: Vec<u8> = (0..BYTES)
        .map(|n| ((n * 37 + n / 251) % 256) as u8)
        .collect();
    let reverse_bytes: Vec<u8> = (0..BYTES)
        .map(|n| ((n * 53 + n / 127 + 19) % 256) as u8)
        .collect();
    let mut configuration = config();
    configuration.limits.pool.bytes = 4 * 1024 * 1024;
    let mut runtime = Runtime::new(configuration).unwrap();
    let pool = runtime.buffer_pool();
    // The scope joins the peer even if an assertion or the root deadline
    // unwinds. Every peer-side I/O and gate also has a bounded wait.
    thread::scope(|scope| {
        runtime.block_on(deadline(async {
            let options = SocketOptions {
                receive_chunk: 4096,
                receive_buffer_bytes: Some(4096),
                send_buffer_bytes: Some(4096),
                ..SocketOptions::default()
            };
            let listener =
                TcpListener::bind_with_options("127.0.0.1:0".parse().unwrap(), options).unwrap();
            let address = listener.local_addr();
            let (arrived, first_byte) = oneshot::channel();
            let (send_marker, marker_allowed) = mpsc::sync_channel(1);
            let (allow_forward_read, forward_read_allowed) = mpsc::sync_channel(1);
            let (blocked, reverse_blocked) = oneshot::channel();
            let (marked, marker_received_while_blocked) = oneshot::channel();
            let (allow_reverse_read, reverse_read_allowed) = mpsc::sync_channel(1);
            let (finish, finished) = oneshot::channel();
            let forward_expected = &forward_bytes;
            let reverse_expected = &reverse_bytes;
            let peer = scope.spawn(move || {
                let socket = socket2::Socket::new(
                    socket2::Domain::IPV4,
                    socket2::Type::STREAM,
                    Some(socket2::Protocol::TCP),
                )
                .unwrap();
                socket.set_recv_buffer_size(4096).unwrap();
                socket.set_send_buffer_size(4096).unwrap();
                socket.connect_timeout(&address.into(), PEER_WAIT).unwrap();
                let mut peer = std::net::TcpStream::from(socket);
                peer.set_nodelay(true).unwrap();
                peer.set_read_timeout(Some(PEER_WAIT)).unwrap();
                peer.set_write_timeout(Some(PEER_WAIT)).unwrap();

                let mut actual = vec![0; BYTES];
                peer.read_exact(&mut actual[..1]).unwrap();
                assert_eq!(actual[0], forward_expected[0]);
                arrived.send(()).unwrap();
                marker_allowed.recv_timeout(PEER_WAIT).unwrap();
                // No forward reads until the root has received this marker
                // and re-polled its already-submitted send as Pending.
                peer.write_all(REVERSE_MARKER).unwrap();
                forward_read_allowed.recv_timeout(PEER_WAIT).unwrap();
                peer.read_exact(&mut actual[1..]).unwrap();
                assert_eq!(actual, *forward_expected);

                // Exchange the pressured direction. A real nonblocking
                // WouldBlock proves the native sender cannot finish while
                // Rivet leaves its bounded receive queue unconsumed.
                let mut sent = 0;
                peer.set_nonblocking(true).unwrap();
                fill_until_blocked(&mut peer, reverse_expected, &mut sent);
                blocked.send(sent).unwrap();
                peer.set_nonblocking(false).unwrap();
                let mut marker = vec![0; FORWARD_MARKER.len()];
                peer.read_exact(&mut marker).unwrap();
                assert_eq!(marker, FORWARD_MARKER);
                assert_eq!(peer.read(&mut [0; 1]).unwrap(), 0);
                peer.set_nonblocking(true).unwrap();
                fill_until_blocked(&mut peer, reverse_expected, &mut sent);
                marked.send(sent).unwrap();
                peer.set_nonblocking(false).unwrap();
                reverse_read_allowed.recv_timeout(PEER_WAIT).unwrap();
                peer.write_all(&reverse_expected[sent..]).unwrap();
                peer.shutdown(Shutdown::Write).unwrap();
                finish.send(()).unwrap();
            });

            let stream = listener.accept().await.unwrap();
            let mut forward = stream.send_all(SendPayload::Single(filled(&pool, &forward_bytes)));
            assert!(poll_once(&mut forward).await.is_none());
            first_byte.await.unwrap();
            // Native receipt, not the first submission's Pending, establishes
            // that this send is in flight while the peer's read gate is closed.
            assert!(poll_once(&mut forward).await.is_none());
            send_marker.send(()).unwrap();
            {
                let mut marker = std::pin::pin!(async {
                    let mut received = 0;
                    while received != REVERSE_MARKER.len() {
                        let data = stream.recv().await.unwrap().unwrap();
                        let end = received + data.len();
                        assert!(end <= REVERSE_MARKER.len());
                        assert_eq!(data.as_slice(), &REVERSE_MARKER[received..end]);
                        received = end;
                    }
                });
                poll_fn(|cx| {
                    assert!(
                        Pin::new(&mut forward).poll(cx).is_pending(),
                        "forward send completed before the peer read gate opened"
                    );
                    marker.as_mut().poll(cx)
                })
                .await;
            }
            assert!(poll_once(&mut forward).await.is_none());
            allow_forward_read.send(()).unwrap();
            let outcome = forward.await;
            assert_eq!(outcome.result.unwrap(), BYTES);
            assert!(outcome.data.is_empty());

            let first_blocked_at = reverse_blocked.await.unwrap();
            assert!(first_blocked_at > 0 && first_blocked_at < BYTES);
            let outcome = stream
                .send_all(SendPayload::Single(filled(&pool, FORWARD_MARKER)))
                .await;
            assert_eq!(outcome.result.unwrap(), FORWARD_MARKER.len());
            stream.shutdown(Shutdown::Write).unwrap();
            let still_blocked_at = marker_received_while_blocked.await.unwrap();
            assert!(still_blocked_at >= first_blocked_at && still_blocked_at < BYTES);
            allow_reverse_read.send(()).unwrap();
            let mut received = 0;
            while let Some(data) = stream.recv().await.unwrap() {
                let end = received + data.len();
                assert!(end <= BYTES);
                assert_eq!(data.as_slice(), &reverse_bytes[received..end]);
                received = end;
            }
            assert_eq!(received, BYTES);
            finished.await.unwrap();
            peer.join().unwrap();
        }));
    });
}

#[test]
fn udp_vectors_zero_length_and_truncation_preserve_datagram_metadata() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    for address in ["127.0.0.1:0", "[::1]:0"]
        .into_iter()
        .take(support::family_count())
    {
        runtime.block_on(deadline(async {
            let address: SocketAddr = address.parse().unwrap();
            let mut options = SocketOptions::udp();
            options.receive_chunk = 4;
            let receiver = UdpSocket::bind_with_options(address, options).unwrap();
            let sender =
                UdpSocket::bind_connected(address, receiver.local_addr(), SocketOptions::udp())
                    .unwrap();
            let packet = SendPayload::Vectored(vec![
                filled(&pool, b"ab"),
                filled(&pool, b""),
                filled(&pool, b"cd"),
            ]);
            assert_eq!(sender.send(packet).await.result.unwrap(), 4);
            let first = receiver.recv().await.unwrap();
            assert_eq!(first.data.as_slice(), b"abcd");
            assert_eq!(first.peer, Some(sender.local_addr()));
            assert!(!first.truncated);
            drop(first);

            assert_eq!(
                sender
                    .send(SendPayload::Single(filled(&pool, b"abcdefgh")))
                    .await
                    .result
                    .unwrap(),
                8
            );
            let truncated = receiver.recv().await.unwrap();
            assert_eq!(truncated.data.as_slice(), b"abcd");
            assert_eq!(truncated.peer, Some(sender.local_addr()));
            assert!(truncated.truncated);
            assert!(truncated.original_len.is_none() || truncated.original_len == Some(8));
            drop(truncated);

            assert_eq!(
                sender
                    .send(SendPayload::Single(filled(&pool, b"")))
                    .await
                    .result
                    .unwrap(),
                0
            );
            let empty = receiver.recv().await.unwrap();
            assert!(empty.data.is_empty());
            assert_eq!(empty.peer, Some(sender.local_addr()));
            assert!(!empty.truncated);
            drop(empty);
            assert_eq!(
                sender
                    .send(SendPayload::Single(filled(&pool, b"next")))
                    .await
                    .result
                    .unwrap(),
                4
            );
            assert_eq!(receiver.recv().await.unwrap().data.as_slice(), b"next");
        }));
    }
}

#[test]
fn cancelling_a_receive_waiter_keeps_consumed_tcp_bytes_and_pool_reserves_fair() {
    let mut configuration = config();
    configuration.limits.pool.bytes = 3 * 1024;
    let mut runtime = Runtime::new(configuration).unwrap();
    let pool = runtime.buffer_pool();
    runtime.block_on(deadline(async {
        let options = SocketOptions {
            receive_chunk: 1024,
            ..SocketOptions::default()
        };
        let (_idle_sender, idle_receiver) =
            pair("127.0.0.1:0".parse().unwrap(), options.clone()).await;
        let (sender, receiver) = pair("127.0.0.1:0".parse().unwrap(), options).await;
        let mut idle_waiter = idle_receiver.recv();
        assert!(poll_once(&mut idle_waiter).await.is_none());
        drop(idle_waiter);
        let mut abandoned = receiver.recv();
        assert!(poll_once(&mut abandoned).await.is_none());
        drop(abandoned);
        // One idle socket pins a receive block and the active receiver reserves
        // another. The last block is the send payload. Reuse of the active
        // receiver's own reserve must work even with no spare global block.
        for sequence in 0u8..16 {
            let bytes = [sequence; 1024];
            let outcome = sender
                .send_all(SendPayload::Single(filled(&pool, &bytes)))
                .await;
            assert_eq!(*outcome.result.as_ref().unwrap(), bytes.len());
            drop(outcome);
            let mut actual = Vec::new();
            while actual.len() < bytes.len() {
                let received = receiver.recv().await.unwrap().unwrap();
                actual.extend_from_slice(received.as_slice());
            }
            assert_eq!(actual, bytes);
        }
        sender.shutdown(Shutdown::Write).unwrap();
        assert!(receiver.recv().await.unwrap().is_none());
    }));
}

#[test]
fn import_rejects_non_rio_without_losing_the_original_socket() {
    let mut runtime = Runtime::new(config()).unwrap();
    let original = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let raw = original.as_raw_socket();
    let local = original.local_addr().unwrap();
    let owned: OwnedSocket = original.into();
    let returned = runtime.block_on(async {
        match UdpSocket::import(owned, SocketOptions::udp()) {
            Ok(_) => {
                panic!("ordinary Winsock sockets must not be silently recreated as RIO sockets")
            }
            Err(error) => {
                assert_eq!(error.socket.as_raw_socket(), raw);
                std::net::UdpSocket::from(error.socket)
            }
        }
    });
    assert_eq!(returned.local_addr().unwrap(), local);
    let observer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    observer
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    returned
        .send_to(b"ownership retained", observer.local_addr().unwrap())
        .unwrap();
    let mut data = [0u8; 64];
    let (bytes, source) = observer.recv_from(&mut data).unwrap();
    assert_eq!(&data[..bytes], b"ownership retained");
    assert_eq!(source, local);
}

#[test]
fn registered_socket_import_preserves_source_and_received_lease_outlives_runtime() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    let raw = unsafe {
        WSASocketW(
            AF_INET as i32,
            SOCK_DGRAM,
            IPPROTO_UDP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    assert_ne!(raw, INVALID_SOCKET);
    let socket = unsafe { socket2::Socket::from_raw_socket(raw as _) };
    socket
        .bind(&socket2::SockAddr::from(
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        ))
        .unwrap();
    let original_address = socket.local_addr().unwrap().as_socket().unwrap();
    let owned: OwnedSocket = socket.into();
    let retained = runtime.block_on(deadline(async {
        let sender = UdpSocket::import(owned, SocketOptions::udp()).unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        assert_eq!(sender.local_addr(), original_address);
        let outcome = sender
            .send_to(
                SendPayload::Single(filled(&pool, b"lease survives driver")),
                receiver.local_addr(),
            )
            .await;
        assert_eq!(outcome.result.unwrap(), 21);
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.peer, Some(original_address));
        received.data
    }));
    drop(runtime);
    assert_eq!(retained.as_slice(), b"lease survives driver");
    drop(retained);
    let recovered = pool
        .try_acquire_at_least(config().limits.pool.bytes)
        .unwrap();
    assert_eq!(recovered.capacity(), config().limits.pool.bytes);
}

#[test]
fn idle_accepted_socket_can_move_to_automatic_handler_worker_before_rio_queue_creation() {
    let mut configuration = config();
    configuration.workers = 2;
    let mut runtime = Runtime::new(configuration).unwrap();
    let pool = runtime.buffer_pool();
    runtime.block_on(deadline(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let serving = async {
            listener
                .serve(|stream| async move {
                    let pool = rivet::runtime::buffer_pool().unwrap();
                    let mut reply = pool.try_acquire().unwrap();
                    while reply.initialized_len() < b"handoff before RQ".len() {
                        let data = stream.recv().await.unwrap().unwrap();
                        reply.extend_from_slice(data.as_slice()).unwrap();
                    }
                    assert_eq!(reply.as_slice(), b"handoff before RQ");
                    let outcome = stream.send_all(SendPayload::Single(reply.freeze())).await;
                    assert_eq!(outcome.result.unwrap(), b"handoff before RQ".len());
                    stream.shutdown(Shutdown::Write).unwrap();
                })
                .await
                .unwrap();
            panic!("serve unexpectedly terminated");
        };
        let client = async {
            let stream = TcpStream::connect(listener.local_addr()).await.unwrap();
            assert_eq!(
                stream
                    .send_all(SendPayload::Single(filled(&pool, b"handoff before RQ")))
                    .await
                    .result
                    .unwrap(),
                17
            );
            let mut actual = Vec::new();
            while let Some(data) = stream.recv().await.unwrap() {
                actual.extend_from_slice(data.as_slice());
            }
            assert_eq!(actual, b"handoff before RQ");
        };
        race(serving, client).await;
    }));
}

#[test]
fn overlapping_send_aliases_on_distinct_rio_queues_preserve_each_datagram() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    runtime.block_on(deadline(async {
        let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let first_sender = UdpSocket::bind(address).unwrap();
        let second_sender = UdpSocket::bind(address).unwrap();
        let first_receiver = UdpSocket::bind(address).unwrap();
        let second_receiver = UdpSocket::bind(address).unwrap();
        let bytes = b"overlapping immutable send leases";
        let data = filled(&pool, bytes);
        let (first, second) = zip(
            first_sender.send_to(
                SendPayload::Single(data.clone()),
                first_receiver.local_addr(),
            ),
            second_sender.send_to(
                SendPayload::Single(data.slice(5..19)),
                second_receiver.local_addr(),
            ),
        )
        .await;
        assert_eq!(first.result.unwrap(), bytes.len());
        assert_eq!(second.result.unwrap(), 14);
        let (first, second) = zip(first_receiver.recv(), second_receiver.recv()).await;
        assert_eq!(first.unwrap().data.as_slice(), bytes);
        assert_eq!(second.unwrap().data.as_slice(), &bytes[5..19]);
        assert_eq!(data.as_slice(), bytes);
    }));
}

#[test]
fn positive_tcp_linger_import_preserves_ownership_and_allows_retry_before_rq_creation() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let raw = unsafe {
        WSASocketW(
            AF_INET as i32,
            SOCK_STREAM,
            IPPROTO_TCP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    assert_ne!(raw, INVALID_SOCKET);
    let socket = unsafe { socket2::Socket::from_raw_socket(raw as _) };
    socket
        .connect(&socket2::SockAddr::from(listener.local_addr().unwrap()))
        .unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    socket.set_linger(Some(Duration::from_secs(2))).unwrap();
    let socket = runtime.block_on(async {
        let error = TcpStream::import(socket.into(), SocketOptions::default())
            .expect_err("positive inherited linger was accepted");
        assert_eq!(error.error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.socket.as_raw_socket(), raw as _);
        socket2::Socket::from(error.socket)
    });
    assert_eq!(socket.linger().unwrap(), Some(Duration::from_secs(2)));

    socket.set_linger(None).unwrap();
    let options = SocketOptions {
        hook: Some(Arc::new(|socket: rivet::socket::BorrowedSocket<'_>| {
            socket2::SockRef::from(&socket).set_linger(Some(Duration::from_secs(2)))
        })),
        ..SocketOptions::default()
    };
    let socket = runtime.block_on(async {
        let error = TcpStream::import(socket.into(), options)
            .expect_err("positive hook linger was accepted");
        assert_eq!(error.error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.socket.as_raw_socket(), raw as _);
        socket2::Socket::from(error.socket)
    });
    assert_eq!(socket.linger().unwrap(), Some(Duration::from_secs(2)));
    socket.set_linger(None).unwrap();
    runtime.block_on(deadline(async {
        // Successful re-import proves neither rejection consumed the native RQ.
        let stream = TcpStream::import(socket.into(), SocketOptions::default()).unwrap();
        let outcome = stream
            .send_all(SendPayload::Single(filled(
                &pool,
                b"original socket retained",
            )))
            .await;
        assert_eq!(outcome.result.unwrap(), b"original socket retained".len());
        stream.shutdown(Shutdown::Write).unwrap();
    }));
    let mut received = Vec::new();
    peer.read_to_end(&mut received).unwrap();
    assert_eq!(received, b"original socket retained");
}

#[test]
fn tcp_setup_hooks_reject_positive_linger_for_listen_connect_and_accept() {
    let mut runtime = Runtime::new(config()).unwrap();
    runtime.block_on(deadline(async {
        let enabled = Arc::new(AtomicBool::new(true));
        let hook_enabled = enabled.clone();
        let options = SocketOptions {
            hook: Some(Arc::new(
                move |socket: rivet::socket::BorrowedSocket<'_>| {
                    let linger = hook_enabled
                        .load(Ordering::Relaxed)
                        .then_some(Duration::from_secs(2));
                    socket2::SockRef::from(&socket).set_linger(linger)
                },
            )),
            ..SocketOptions::default()
        };
        let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let error = TcpListener::bind_with_options(address, options.clone())
            .expect_err("listener hook linger was accepted");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        let outside_listener = std::net::TcpListener::bind(address).unwrap();
        let error = TcpStream::connect_with_options(
            outside_listener.local_addr().unwrap(),
            options.clone(),
        )
        .await
        .expect_err("connect hook linger was accepted");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        enabled.store(false, Ordering::Relaxed);
        let listener = TcpListener::bind_with_options(address, options).unwrap();
        enabled.store(true, Ordering::Relaxed);
        let error = listener
            .accept()
            .await
            .expect_err("accepted child hook linger was accepted");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }));
}

#[test]
fn udp_bind_posts_whole_burst_before_first_recv_and_rotates_held_leases() {
    const WINDOW: usize = 32;
    let mut configuration = config();
    configuration.limits.max_pending_receives = WINDOW;
    let mut runtime = Runtime::new(configuration).unwrap();
    runtime.block_on(deadline(async {
        let mut options = SocketOptions::udp();
        options.receive_chunk = 256;
        let receiver =
            UdpSocket::bind_with_options("127.0.0.1:0".parse().unwrap(), options).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let source = sender.local_addr().unwrap();
        // There is deliberately no await, receive future, or worker poll
        // between bind and all WINDOW datagrams arriving at the native socket.
        for sequence in 0..WINDOW {
            let bytes = [sequence as u8; 256];
            let packet = if sequence == 0 {
                &bytes[..0]
            } else {
                &bytes[..]
            };
            assert_eq!(
                sender.send_to(packet, receiver.local_addr()).unwrap(),
                packet.len()
            );
        }
        let mut held = Vec::with_capacity(WINDOW);
        while held.len() != WINDOW {
            let mut batch: [Option<rivet::net::Received>; WINDOW] = std::array::from_fn(|_| None);
            let count = receiver.recv_batch(&mut batch).await.unwrap();
            for packet in batch.iter_mut().take(count) {
                let packet = packet.take().unwrap();
                let sequence = held.len();
                assert_eq!(packet.peer, Some(source));
                assert!(!packet.truncated);
                assert_eq!(
                    packet.original_len,
                    Some(if sequence == 0 { 0 } else { 256 })
                );
                let expected = [sequence as u8; 256];
                assert_eq!(
                    packet.data.as_slice(),
                    if sequence == 0 {
                        &expected[..0]
                    } else {
                        &expected[..]
                    }
                );
                held.push(packet.data);
            }
        }
        // Keep the complete preceding window immutable. New receives need
        // distinct spare leases, not writable aliases or a single pinned shot.
        for sequence in 0..WINDOW {
            sender
                .send_to(&[128 + sequence as u8; 256], receiver.local_addr())
                .unwrap();
        }
        for sequence in 0..WINDOW {
            let packet = receiver.recv().await.unwrap();
            assert_eq!(packet.data.as_slice(), &[128 + sequence as u8; 256]);
            assert_eq!(packet.peer, Some(source));
        }
        for (sequence, data) in held.iter().enumerate() {
            let expected = [sequence as u8; 256];
            assert_eq!(
                data.as_slice(),
                if sequence == 0 {
                    &expected[..0]
                } else {
                    &expected[..]
                }
            );
        }
    }));
}

#[test]
fn udp_dropping_a_woken_waiter_preserves_the_queued_window_and_held_leases() {
    struct ReceiveWake {
        notified: AtomicBool,
        root: Waker,
    }

    impl Wake for ReceiveWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.notified.store(true, Ordering::Release);
            self.root.wake_by_ref();
        }
    }

    const WINDOW: usize = 4;
    const CHUNK: usize = 256;
    let mut configuration = config();
    configuration.limits.max_pending_receives = WINDOW;
    let mut runtime = Runtime::new(configuration).unwrap();
    runtime.block_on(deadline(async {
        let mut options = SocketOptions::udp();
        options.receive_chunk = CHUNK;
        let receiver =
            UdpSocket::bind_with_options("127.0.0.1:0".parse().unwrap(), options).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let source = sender.local_addr().unwrap();
        let mut abandoned = receiver.recv();
        let notice = poll_fn(|cx| {
            let notice = Arc::new(ReceiveWake {
                notified: AtomicBool::new(false),
                root: cx.waker().clone(),
            });
            let waker = Waker::from(notice.clone());
            assert!(
                Pin::new(&mut abandoned)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            Poll::Ready(notice)
        })
        .await;
        assert!(!notice.notified.load(Ordering::Acquire));
        // Fill every native lane without polling the receiver again.
        for sequence in 0..WINDOW {
            let bytes = [sequence as u8; CHUNK];
            let packet = if sequence == WINDOW / 2 {
                &bytes[..0]
            } else {
                &bytes[..]
            };
            assert_eq!(
                sender.send_to(packet, receiver.local_addr()).unwrap(),
                packet.len()
            );
        }
        poll_fn(|_| {
            if notice.notified.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        // The forwarded wake witnesses native data entering the receive
        // queue. Do not poll the old waiter to consume that queued result.
        drop(abandoned);

        let mut held = Vec::with_capacity(WINDOW);
        for sequence in 0..WINDOW {
            let packet = receiver.recv().await.unwrap();
            let expected = [sequence as u8; CHUNK];
            let expected = if sequence == WINDOW / 2 {
                &expected[..0]
            } else {
                &expected[..]
            };
            assert_eq!(packet.data.as_slice(), expected);
            assert_eq!(packet.peer, Some(source));
            assert_eq!(packet.original_len, Some(expected.len()));
            assert!(!packet.truncated);
            held.push(packet.data);
        }
        // Keep all old leases alive while another complete, unpolled window
        // arrives. Replacement lanes must not overwrite published data.
        for sequence in 0..WINDOW {
            let packet = [128 + sequence as u8; CHUNK];
            assert_eq!(
                sender.send_to(&packet, receiver.local_addr()).unwrap(),
                packet.len()
            );
        }
        for sequence in 0..WINDOW {
            let packet = receiver.recv().await.unwrap();
            assert_eq!(packet.data.as_slice(), &[128 + sequence as u8; CHUNK]);
            assert_eq!(packet.peer, Some(source));
            assert_eq!(packet.original_len, Some(CHUNK));
            assert!(!packet.truncated);
        }
        let marker = b"after cancelled windows";
        sender.send_to(marker, receiver.local_addr()).unwrap();
        let packet = receiver.recv().await.unwrap();
        assert_eq!(packet.data.as_slice(), marker);
        assert_eq!(packet.peer, Some(source));
        for (sequence, data) in held.iter().enumerate() {
            let expected = [sequence as u8; CHUNK];
            assert_eq!(
                data.as_slice(),
                if sequence == WINDOW / 2 {
                    &expected[..0]
                } else {
                    &expected[..]
                }
            );
        }
    }));
}

#[test]
fn udp_send_and_recv_batches_replenish_complete_windows() {
    const WINDOW: usize = 32;
    let mut configuration = config();
    configuration.limits.max_pending_receives = WINDOW;
    let mut runtime = Runtime::new(configuration).unwrap();
    let pool = runtime.buffer_pool();
    runtime.block_on(deadline(async {
        let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut options = SocketOptions::udp();
        options.receive_chunk = 256;
        let receiver = UdpSocket::bind_with_options(address, options.clone()).unwrap();
        let sender = UdpSocket::bind_connected(address, receiver.local_addr(), options).unwrap();
        for window in 0u8..16 {
            let mut packets: Vec<_> = (0..WINDOW)
                .map(|sequence| {
                    let mut bytes = [sequence as u8; 256];
                    bytes[0] = window;
                    rivet::net::Datagram::new(SendPayload::Single(filled(&pool, &bytes)), None)
                })
                .collect();
            assert_eq!(sender.send_batch(&mut packets).await.unwrap(), WINDOW);
            for packet in &mut packets {
                assert_eq!(packet.take_outcome().unwrap().result.unwrap(), 256);
            }
            let mut sequence = 0;
            while sequence != WINDOW {
                let mut batch: [Option<rivet::net::Received>; WINDOW] =
                    std::array::from_fn(|_| None);
                let count = receiver.recv_batch(&mut batch).await.unwrap();
                for packet in batch.iter_mut().take(count) {
                    let packet = packet.take().unwrap();
                    let mut expected = [sequence as u8; 256];
                    expected[0] = window;
                    assert_eq!(packet.data.as_slice(), expected);
                    assert_eq!(packet.peer, Some(sender.local_addr()));
                    sequence += 1;
                }
            }
        }
    }));
}

#[test]
fn udp_import_buffer_admission_preserves_original_and_retries_without_poisoned_rq() {
    let mut configuration = config();
    let mut options = SocketOptions::udp();
    options.receive_chunk = 2048;
    let window_bytes = configuration
        .limits
        .windows_udp_receive_bytes(options.receive_chunk)
        .unwrap();
    configuration.limits.pool.bytes = window_bytes;
    let mut runtime = Runtime::new(configuration).unwrap();
    let pool = runtime.buffer_pool();
    let blocker = pool.try_acquire().unwrap();
    let original = registered_udp();
    let raw = original.as_raw_socket();
    let local = original.local_addr().unwrap().as_socket().unwrap();
    let retained = runtime.block_on(deadline(async {
        let error = UdpSocket::import(original.into(), options.clone()).unwrap_err();
        assert_eq!(error.error.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(error.socket.as_raw_socket(), raw);
        drop(blocker);
        // Successful re-import proves partial buffer admission did not create
        // an inseparable RQ or leave operation slots/leases behind.
        let receiver = UdpSocket::import(error.socket, options).unwrap();
        assert_eq!(receiver.local_addr(), local);
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"same imported descriptor", local).unwrap();
        let packet = receiver.recv().await.unwrap();
        assert_eq!(packet.peer, Some(sender.local_addr().unwrap()));
        drop(receiver); // Other native receive lanes are still outstanding.
        packet.data
    }));
    drop(runtime);
    assert_eq!(retained.as_slice(), b"same imported descriptor");
    drop(retained);
    assert_eq!(
        pool.try_acquire_at_least(window_bytes).unwrap().capacity(),
        window_bytes
    );
}

#[test]
fn udp_operation_admission_rejects_import_before_native_queue_ownership() {
    let mut configuration = config();
    configuration.limits.max_operations = 4;
    let mut options = SocketOptions::udp();
    options.receive_chunk = 1024;
    let mut runtime = Runtime::new(configuration.clone()).unwrap();
    let original = runtime.block_on(async {
        let _first =
            UdpSocket::bind_with_options("127.0.0.1:0".parse().unwrap(), options.clone()).unwrap();
        let original = registered_udp();
        let raw = original.as_raw_socket();
        let error = UdpSocket::import(original.into(), options.clone()).unwrap_err();
        assert_eq!(error.error.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(error.socket.as_raw_socket(), raw);
        error.socket
    });
    // Runtime drop drains all original receive lanes, without a timer or a
    // guessed cancellation delay. The rejected descriptor is still importable.
    drop(runtime);
    let mut runtime = Runtime::new(configuration).unwrap();
    runtime.block_on(deadline(async {
        let receiver = UdpSocket::import(original, options).unwrap();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender
            .send_to(b"operation slots reclaimed", receiver.local_addr())
            .unwrap();
        assert_eq!(
            receiver.recv().await.unwrap().data.as_slice(),
            b"operation slots reclaimed"
        );
    }));
}
