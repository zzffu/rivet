#![cfg(target_os = "linux")]

use futures_lite::future::{poll_once, zip};
use rivet::{
    BufferPool, Runtime, RuntimeConfig, SendBuf, SendPayload, SocketOptions, TcpListener,
    TcpStream, UdpSocket,
};
use std::{
    future::Future,
    io::{self, Read},
    net::{Shutdown, SocketAddr},
    os::fd::AsRawFd,
    sync::Arc,
    time::Duration,
};

fn config() -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 32;
    config.limits.max_sockets = 32;
    config.limits.max_operations = 128;
    config.limits.max_pending_receives = 1;
    config.limits.max_pending_accepts = 2;
    config.limits.completion_budget = 1;
    config.limits.pool.bytes = 1024 * 1024;
    config.limits.pool.block_size = 4096;
    config.limits.pool.max_leases = 128;
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
        .expect("native Linux networking timed out")
}

async fn pair(address: SocketAddr) -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(address).unwrap();
    let (client, server) = zip(TcpStream::connect(listener.local_addr()), listener.accept()).await;
    (client.unwrap(), server.unwrap())
}

#[test]
fn tiny_submission_queues_progress_past_background_udp_receives() {
    for entries in [1, 2] {
        let mut config = config();
        config.linux.sq_entries = entries;
        let mut runtime = Runtime::new(config).unwrap();
        let pool = runtime.buffer_pool();
        for address in ["127.0.0.1:0", "[::1]:0"] {
            runtime.block_on(async {
                rivet::time::timeout(Duration::from_secs(1), async {
                    let address: SocketAddr = address.parse().unwrap();
                    let mut options = SocketOptions::udp();
                    options.receive_chunk = 8;
                    // Each lane needs an exclusive endpoint even when binding port zero.
                    options.reuse_address = false;
                    let receivers: Vec<_> = (0..8)
                        .map(|_| UdpSocket::bind_with_options(address, options.clone()).unwrap())
                        .collect();
                    for receiver in &receivers {
                        let mut waiter = receiver.recv();
                        assert!(poll_once(&mut waiter).await.is_none());
                        drop(waiter);
                    }
                    let sender = UdpSocket::bind_with_options(address, options).unwrap();
                    // The early SQ entries are receives with no incoming data.
                    // The send must be submitted without waiting for their CQEs.
                    for (sequence, receiver) in receivers.iter().enumerate().rev() {
                        let expected = [sequence as u8; 4];
                        assert_eq!(
                            sender
                                .send_to(
                                    SendPayload::Single(filled(&pool, &expected)),
                                    receiver.local_addr(),
                                )
                                .await
                                .result
                                .unwrap(),
                            expected.len()
                        );
                        let packet = receiver.recv().await.unwrap();
                        assert_eq!(packet.data.as_slice(), expected);
                        assert_eq!(packet.peer, Some(sender.local_addr()));
                        assert_eq!(packet.original_len, Some(expected.len()));
                        assert!(!packet.truncated);
                    }
                })
                .await
                .expect("SQ pressure stalled UDP submission");
            });
        }
    }
}

#[cfg(feature = "registered-wait")]
#[test]
fn registered_wait_preserves_timeouts_and_network_progress() {
    use rivet::Optimization::*;
    let configurations = [
        config().enable(RegisteredWait),
        #[cfg(all(
            feature = "registered-ring",
            feature = "registered-buffers",
            feature = "direct-descriptors"
        ))]
        config()
            .enable(RegisteredWait)
            .enable(RegisteredRing)
            .enable(RegisteredBuffers)
            .enable(DirectDescriptors),
        #[cfg(all(feature = "sq-rewind", feature = "mixed-cqe"))]
        config()
            .enable(RegisteredWait)
            .enable(SqRewind)
            .enable(MixedCqe),
        #[cfg(all(feature = "uring-sqpoll", feature = "registered-ring"))]
        config()
            .enable(RegisteredWait)
            .enable(SqPoll)
            .enable(RegisteredRing),
    ];
    for config in configurations {
        let mut runtime = Runtime::new(config).unwrap();
        for address in ["127.0.0.1:0", "[::1]:0"] {
            runtime.block_on(deadline(async {
                let address = address.parse().unwrap();
                let receiver = UdpSocket::bind(address).unwrap();
                let duration = Duration::from_millis(3);
                let started = std::time::Instant::now();
                assert!(matches!(
                    rivet::time::timeout(duration, receiver.recv()).await,
                    Err(rivet::time::TimeoutError::Elapsed)
                ));
                assert!(started.elapsed() >= duration);
                let sender = std::net::UdpSocket::bind(address).unwrap();
                sender
                    .send_to(b"after registered timeout", receiver.local_addr())
                    .unwrap();
                let packet = receiver.recv().await.unwrap();
                assert_eq!(packet.data.as_slice(), b"after registered timeout");
                assert_eq!(packet.peer, Some(sender.local_addr().unwrap()));
                assert!(!packet.truncated);
            }));
        }
    }
}

#[cfg(feature = "multishot-recv")]
#[test]
fn provided_multishot_tcp_eof_preserves_payload_and_reverse_direction() {
    use rivet::Optimization::*;
    let configurations = [
        config().enable(MultishotRecv),
        #[cfg(feature = "incremental-buffers")]
        config().enable(MultishotRecv).enable(IncrementalBuffers),
        #[cfg(all(feature = "incremental-buffers", feature = "buffer-bundles"))]
        config()
            .enable(MultishotRecv)
            .enable(IncrementalBuffers)
            .enable(BufferBundles),
    ];
    for config in configurations {
        let mut runtime = Runtime::new(config).unwrap();
        let pool = runtime.buffer_pool();
        for address in ["127.0.0.1:0", "[::1]:0"] {
            runtime.block_on(deadline(async {
                let (sender, receiver) = pair(address.parse().unwrap()).await;
                let request = b"request before half-close";
                assert_eq!(
                    sender
                        .send_all(SendPayload::Single(filled(&pool, request)))
                        .await
                        .result
                        .unwrap(),
                    request.len()
                );
                sender.shutdown(Shutdown::Write).unwrap();
                let mut received = Vec::new();
                while let Some(data) = receiver.recv().await.unwrap() {
                    received.extend_from_slice(data.as_slice());
                }
                assert_eq!(received, request);
                let reply = b"reply after multishot EOF";
                assert_eq!(
                    receiver
                        .send_all(SendPayload::Single(filled(&pool, reply)))
                        .await
                        .result
                        .unwrap(),
                    reply.len()
                );
                receiver.shutdown(Shutdown::Write).unwrap();
                received.clear();
                while let Some(data) = sender.recv().await.unwrap() {
                    received.extend_from_slice(data.as_slice());
                }
                assert_eq!(received, reply);
            }));
        }
    }
}

#[cfg(all(feature = "zc-tx-fixed", feature = "zc-tx-vectored"))]
fn fixed_zc_config() -> RuntimeConfig {
    let mut config = config()
        .enable(rivet::Optimization::ZcTxFixed)
        .enable(rivet::Optimization::ZcTxVectored);
    config.linux.zc_send_threshold = 0;
    config
}

#[cfg(all(feature = "zc-tx-fixed", feature = "zc-tx-vectored"))]
#[test]
fn fixed_zero_copy_tcp_vectors_ignore_empty_segments_without_losing_bytes() {
    let mut runtime = Runtime::new(fixed_zc_config()).unwrap();
    let pool = runtime.buffer_pool();
    for address in ["127.0.0.1:0", "[::1]:0"] {
        runtime.block_on(deadline(async {
            let (sender, receiver) = pair(address.parse().unwrap()).await;
            let data = filled(&pool, b"fixed vectors preserve bytes");
            let vector = SendPayload::Vectored(vec![
                data.slice(0..0),
                data.slice(0..6),
                data.slice(6..6),
                data.slice(6..data.len()),
                data.slice(data.len()..data.len()),
            ]);
            assert_eq!(sender.send_all(vector).await.result.unwrap(), data.len());
            // Use send, not send_all: a zero-byte payload must reach the backend.
            assert_eq!(
                sender
                    .send(SendPayload::Vectored(vec![
                        data.slice(0..0),
                        data.slice(data.len()..data.len())
                    ]))
                    .await
                    .result
                    .unwrap(),
                0
            );
            assert_eq!(
                sender
                    .send(SendPayload::Vectored(Vec::new()))
                    .await
                    .result
                    .unwrap(),
                0
            );
            assert_eq!(
                sender
                    .send(SendPayload::Single(data.slice(0..0)))
                    .await
                    .result
                    .unwrap(),
                0
            );
            assert_eq!(
                sender
                    .send_all(SendPayload::Single(filled(&pool, b" after empties")))
                    .await
                    .result
                    .unwrap(),
                14
            );
            sender.shutdown(Shutdown::Write).unwrap();
            let mut received = Vec::new();
            while let Some(block) = receiver.recv().await.unwrap() {
                received.extend_from_slice(block.as_slice());
            }
            assert_eq!(received, b"fixed vectors preserve bytes after empties");
        }));
    }
}

#[cfg(all(feature = "zc-tx-fixed", feature = "zc-tx-vectored"))]
#[test]
fn fixed_zero_copy_udp_vectors_keep_empty_datagrams_and_peer_addresses() {
    let mut runtime = Runtime::new(fixed_zc_config()).unwrap();
    let pool = runtime.buffer_pool();
    for address in ["127.0.0.1:0", "[::1]:0"] {
        runtime.block_on(deadline(async {
            let address: SocketAddr = address.parse().unwrap();
            let receiver = UdpSocket::bind(address).unwrap();
            let connected =
                UdpSocket::bind_connected(address, receiver.local_addr(), SocketOptions::udp())
                    .unwrap();
            let unconnected = UdpSocket::bind(address).unwrap();
            let data = filled(&pool, b"abcdefgh");
            for (sender, destination) in [
                (&connected, None),
                (&unconnected, Some(receiver.local_addr())),
            ] {
                let packets = [
                    SendPayload::Vectored(vec![
                        data.slice(0..0),
                        data.slice(0..3),
                        data.slice(3..3),
                        data.slice(3..8),
                        data.slice(8..8),
                    ]),
                    SendPayload::Vectored(vec![data.slice(0..0), data.slice(8..8)]),
                    SendPayload::Vectored(Vec::new()),
                    SendPayload::Single(data.slice(0..0)),
                    SendPayload::Single(data.clone()),
                ];
                for (packet, expected) in
                    packets
                        .into_iter()
                        .zip([&b"abcdefgh"[..], b"", b"", b"", b"abcdefgh"])
                {
                    let outcome = match destination {
                        Some(destination) => sender.send_to(packet, destination).await,
                        None => sender.send(packet).await,
                    };
                    assert_eq!(outcome.result.unwrap(), expected.len());
                    let received = receiver.recv().await.unwrap();
                    assert_eq!(received.data.as_slice(), expected);
                    assert_eq!(received.peer, Some(sender.local_addr()));
                    assert_eq!(received.original_len, Some(expected.len()));
                    assert!(!received.truncated);
                }
            }
        }));
    }
}

fn exercise_udp_metadata(config: RuntimeConfig, receive_chunk: usize) {
    let mut runtime = Runtime::new(config).unwrap();
    for address in ["127.0.0.1:0", "[::1]:0"] {
        runtime.block_on(deadline(async {
            let address: SocketAddr = address.parse().unwrap();
            let mut options = SocketOptions::udp();
            options.receive_chunk = receive_chunk;
            let receiver = UdpSocket::bind_with_options(address, options).unwrap();
            let first = std::net::UdpSocket::bind(address).unwrap();
            let second = std::net::UdpSocket::bind(address).unwrap();
            for sequence in 0..8u8 {
                let mut waiter = receiver.recv();
                assert!(poll_once(&mut waiter).await.is_none());
                drop(waiter);
                // Submit recvmsg, then force other operation metadata borrows
                // while the receive owns its native output pointers.
                rivet::time::sleep(Duration::from_millis(1)).await.unwrap();
                let (client, server) = pair(address).await;
                let large = [sequence; 9];
                let next = [sequence ^ 0x5a; 11];
                first.send_to(&large, receiver.local_addr()).unwrap();
                second.send_to(&next, receiver.local_addr()).unwrap();
                first.send_to(b"", receiver.local_addr()).unwrap();
                second.send_to(b"last", receiver.local_addr()).unwrap();
                let _churn = TcpListener::bind(address).unwrap();
                // One publication credit pauses native rearm (or retains
                // raced multishot CQEs). Datagram metadata must follow its CQE.
                rivet::time::sleep(Duration::from_millis(1)).await.unwrap();
                drop((client, server));
                let retained = receiver.recv().await.unwrap();
                assert_eq!(retained.peer, Some(first.local_addr().unwrap()));
                assert_eq!(retained.original_len, Some(9));
                assert_eq!(
                    retained.data.as_slice(),
                    &large[..large.len().min(receive_chunk)]
                );
                assert_eq!(retained.truncated, large.len() > receive_chunk);
                for (expected, source) in [(&next[..], &second), (b"", &first), (b"last", &second)]
                {
                    let packet = receiver.recv().await.unwrap();
                    assert_eq!(
                        packet.data.as_slice(),
                        &expected[..expected.len().min(receive_chunk)]
                    );
                    assert_eq!(packet.peer, Some(source.local_addr().unwrap()));
                    assert_eq!(packet.original_len, Some(expected.len()));
                    assert_eq!(packet.truncated, expected.len() > receive_chunk);
                }
                assert_eq!(
                    retained.data.as_slice(),
                    &large[..large.len().min(receive_chunk)]
                );
            }
        }));
    }
}

#[test]
fn recvmsg_metadata_survives_other_operations_and_credit_pauses() {
    exercise_udp_metadata(config(), 4);
}

#[cfg(feature = "provided-buffers")]
#[test]
fn single_shot_provided_recvmsg_retains_its_output_until_publication() {
    exercise_udp_metadata(config().enable(rivet::Optimization::ProvidedBuffers), 4);
}

#[cfg(feature = "uring-sqpoll")]
#[test]
fn sqpoll_recvmsg_metadata_is_not_aliased_by_owner_bookkeeping() {
    exercise_udp_metadata(config().enable(rivet::Optimization::SqPoll), 4);
}

#[cfg(all(
    feature = "provided-buffers",
    feature = "multishot-recv",
    feature = "incremental-buffers",
    feature = "direct-descriptors"
))]
#[test]
fn provided_multishot_datagram_metadata_survives_deferred_completions() {
    exercise_udp_metadata(
        config()
            .enable(rivet::Optimization::MultishotRecv)
            .enable(rivet::Optimization::IncrementalBuffers)
            .enable(rivet::Optimization::DirectDescriptors),
        4,
    );
}

#[test]
fn imported_udp_gro_keeps_datagram_boundaries_with_off_policy() {
    let configurations = [
        config(),
        #[cfg(feature = "provided-buffers")]
        config().enable(rivet::Optimization::ProvidedBuffers),
        #[cfg(feature = "multishot-recv")]
        config().enable(rivet::Optimization::MultishotRecv),
    ];
    for config in configurations {
        let mut runtime = Runtime::new(
            config.with_policy(rivet::Optimization::UdpGro, rivet::Policy::Off),
        )
        .unwrap();
        for address in ["127.0.0.1:0", "[::1]:0"] {
            let receiver = std::net::UdpSocket::bind(address).unwrap();
            let sender = std::net::UdpSocket::bind(address).unwrap();
            for (socket, option, value) in [
                (&receiver, libc::UDP_GRO, 1i32),
                (&sender, libc::UDP_SEGMENT, 4i32),
            ] {
                assert_eq!(
                    unsafe {
                        libc::setsockopt(
                            socket.as_raw_fd(),
                            libc::IPPROTO_UDP,
                            option,
                            (&value as *const i32).cast(),
                            size_of::<i32>() as libc::socklen_t,
                        )
                    },
                    0,
                    "{}",
                    io::Error::last_os_error()
                );
            }
            receiver
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let local = receiver.local_addr().unwrap();
            let peer = sender.local_addr().unwrap();
            assert_eq!(sender.send_to(b"abcdEFGH", local).unwrap(), 8);
            // std::net::UdpSocket::peek uses MSG_PEEK: require a real aggregate
            // before import, rather than passing on two ordinary datagrams.
            let mut aggregate = [0; 16];
            assert_eq!(receiver.peek(&mut aggregate).unwrap(), 8);
            assert_eq!(&aggregate[..8], b"abcdEFGH");
            runtime.block_on(deadline(async {
                let mut options = SocketOptions::udp();
                options.receive_chunk = 8;
                let receiver = UdpSocket::import(receiver.into(), options).unwrap();
                let first = receiver.recv().await.unwrap();
                assert_eq!(first.data.as_slice(), b"abcd");
                assert_eq!(first.peer, Some(peer));
                assert_eq!(first.original_len, Some(4));
                assert_eq!(first.gro_segment_size, Some(4));
                assert!(!first.truncated);
                let second = receiver.recv().await.unwrap();
                assert_eq!(second.data.as_slice(), b"EFGH");
                assert_eq!(second.peer, Some(peer));
                assert_eq!(second.original_len, Some(4));
                assert_eq!(second.gro_segment_size, Some(4));
                assert!(!second.truncated);
                assert_eq!(first.data.as_slice(), b"abcd");
            }));
        }
    }
}

#[cfg(all(feature = "udp-gso", feature = "udp-gro"))]
#[test]
fn udp_gso_gro_control_messages_preserve_segment_boundaries() {
    let mut runtime = Runtime::new(
        config()
            .enable(rivet::Optimization::UdpGso)
            .enable(rivet::Optimization::UdpGro),
    )
    .unwrap();
    let pool = runtime.buffer_pool();
    for address in ["127.0.0.1:0", "[::1]:0"] {
        runtime.block_on(deadline(async {
            let address = address.parse().unwrap();
            let sender = UdpSocket::bind(address).unwrap();
            let receiver = UdpSocket::bind(address).unwrap();
            let data = filled(&pool, b"abcdEFGHij");
            assert_eq!(
                sender
                    .send_segments(SendPayload::Single(data), 4, Some(receiver.local_addr()))
                    .await
                    .result
                    .unwrap(),
                10
            );
            for expected in [&b"abcd"[..], b"EFGH", b"ij"] {
                let packet = receiver.recv().await.unwrap();
                assert_eq!(packet.data.as_slice(), expected);
                assert_eq!(packet.peer, Some(sender.local_addr()));
                assert_eq!(packet.original_len, Some(expected.len()));
                assert!(!packet.truncated);
            }
            assert_eq!(
                sender
                    .send_to(
                        SendPayload::Single(filled(&pool, b"normal")),
                        receiver.local_addr()
                    )
                    .await
                    .result
                    .unwrap(),
                6
            );
            let packet = receiver.recv().await.unwrap();
            assert_eq!(packet.data.as_slice(), b"normal");
            assert_eq!(packet.peer, Some(sender.local_addr()));
            assert_eq!(packet.original_len, Some(6));
            assert!(!packet.truncated);
        }));
    }
}

#[test]
fn positive_linger_import_rejection_preserves_socket_and_blocking_mode() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    for set_by_hook in [false, true] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let original = socket2::Socket::from(
            std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap(),
        );
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let raw = original.as_raw_fd();
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
        assert!(flags >= 0);
        let mut options = SocketOptions::default();
        if set_by_hook {
            options.hook = Some(Arc::new(|socket: rivet::socket::BorrowedSocket<'_>| {
                socket2::SockRef::from(&socket).set_linger(Some(Duration::from_secs(2)))
            }));
        } else {
            original.set_linger(Some(Duration::from_secs(2))).unwrap();
        }
        let error =
            runtime.block_on(async { TcpStream::import(original.into(), options).unwrap_err() });
        assert_eq!(error.error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.socket.as_raw_fd(), raw);
        assert_eq!(unsafe { libc::fcntl(raw, libc::F_GETFL) }, flags);
        let returned = socket2::Socket::from(error.socket);
        assert_eq!(returned.linger().unwrap(), Some(Duration::from_secs(2)));
        returned.set_linger(None).unwrap();
        runtime.block_on(deadline(async {
            let stream = TcpStream::import(returned.into(), SocketOptions::default()).unwrap();
            assert_eq!(
                stream
                    .send_all(SendPayload::Single(filled(&pool, b"ownership kept")))
                    .await
                    .result
                    .unwrap(),
                14
            );
            stream.shutdown(Shutdown::Write).unwrap();
        }));
        let mut bytes = [0; 14];
        peer.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"ownership kept");
    }
}

#[test]
fn tcp_creation_rejects_hook_configured_positive_linger() {
    let configurations = [
        config(),
        #[cfg(feature = "direct-descriptors")]
        config().enable(rivet::Optimization::DirectDescriptors),
    ];
    for config in configurations {
        let mut runtime = Runtime::new(config).unwrap();
        runtime.block_on(deadline(async {
            let options = SocketOptions {
                hook: Some(Arc::new(|socket: rivet::socket::BorrowedSocket<'_>| {
                    socket2::SockRef::from(&socket).set_linger(Some(Duration::from_secs(2)))
                })),
                ..SocketOptions::default()
            };
            assert_eq!(
                TcpListener::bind_with_options("127.0.0.1:0".parse().unwrap(), options.clone())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            let target = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            assert_eq!(
                TcpStream::connect_with_options(target.local_addr().unwrap(), options)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            target.set_nonblocking(true).unwrap();
            assert_eq!(
                target.accept().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }));
    }
}
