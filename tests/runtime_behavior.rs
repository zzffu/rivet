use futures_lite::future::{poll_once, zip};
use parking_lot::Mutex;
use rivet::{
    Runtime, RuntimeConfig, SendPayload, SocketOptions, TcpListener, TcpStream, UdpSocket,
    runtime::{self, JoinError, SpawnError},
    time::{self, TimeoutError},
};
use std::{
    cell::Cell,
    future::{Future, pending},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr},
    pin::pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::Poll,
    thread,
    time::Duration,
};
mod support;

fn config(workers: usize) -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.workers = workers;
    config.limits.max_tasks = 64;
    config.limits.max_sockets = 64;
    config.limits.max_operations = 256;
    config.limits.max_pending_receives = 4;
    config.limits.max_pending_accepts = 4;
    config.limits.pool.bytes = 4 * 1024 * 1024;
    config.limits.pool.block_size = 16 * 1024;
    config.limits.pool.max_leases = 512;
    config
}
fn address(v6: bool) -> SocketAddr {
    SocketAddr::new(
        if v6 {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        } else {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        },
        0,
    )
}
fn payload(bytes: &[u8]) -> SendPayload {
    let pool = runtime::buffer_pool().unwrap();
    let mut buffer = pool.try_acquire_at_least(bytes.len()).unwrap();
    buffer.extend_from_slice(bytes).unwrap();
    SendPayload::Single(buffer.freeze())
}
async fn pair(v6: bool) -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(address(v6)).unwrap();
    let (client, server) = zip(TcpStream::connect(listener.local_addr()), listener.accept()).await;
    (client.unwrap(), server.unwrap())
}
async fn bytes_until_eof(stream: &TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(block) = stream.recv().await.unwrap() {
        bytes.extend_from_slice(block.as_slice());
    }
    bytes
}
async fn blocks_exactly(stream: &TcpStream, length: usize) -> Vec<rivet::ReadBuf> {
    let mut blocks = Vec::new();
    let mut received = 0;
    while received < length {
        let block = stream
            .recv()
            .await
            .unwrap()
            .expect("EOF before expected bytes");
        received += block.len();
        blocks.push(block);
    }
    assert_eq!(received, length);
    blocks
}
fn flatten(blocks: &[rivet::ReadBuf]) -> Vec<u8> {
    blocks
        .iter()
        .flat_map(|block| block.as_slice().iter().copied())
        .collect()
}

#[test]
fn owner_root_and_factory_local_future() {
    let owner = thread::current().id();
    let mut runtime = Runtime::new(config(2)).unwrap();
    // The caller worker is inactive: placement must use a running background
    // worker, where this factory constructs an intentionally non-Send future.
    let task = runtime
        .handle()
        .spawn(|| async {
            let value = Rc::new(Cell::new(40));
            runtime::yield_now().await;
            value.set(value.get() + 2);
            (thread::current().id(), value.get())
        })
        .unwrap();
    let (worker, value) = runtime.block_on(async {
        assert_eq!(thread::current().id(), owner);
        task.await.unwrap()
    });
    assert_ne!(worker, owner);
    assert_eq!(value, 42);
}

#[test]
fn single_worker_handle_rejects_unprogressable_work() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    assert_eq!(
        runtime.handle().spawn(|| async { 7 }).unwrap_err(),
        SpawnError::NotRunning
    );
    let result = runtime.block_on(async { runtime::spawn(|| async { 7 }).unwrap().await.unwrap() });
    assert_eq!(result, 7);
}

#[test]
fn inactive_owner_and_full_background_worker_leave_other_capacity_usable() {
    let mut limits = config(3);
    limits.limits.max_tasks = 1;
    let runtime = Runtime::new(limits).unwrap();
    let handle = runtime.handle();
    let owner = thread::current().id();
    let (started, receive_started) = mpsc::sync_channel(1);
    let occupied = handle
        .spawn(move || async move {
            started.send(thread::current().id()).unwrap();
            pending::<()>().await;
        })
        .unwrap();
    let occupied_worker = receive_started
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert_ne!(occupied_worker, owner);

    let (completed, receive_completed) = mpsc::sync_channel(1);
    let available = handle
        .spawn(move || async move {
            let worker = thread::current().id();
            completed.send(worker).unwrap();
            worker
        })
        .unwrap();
    let available_worker = receive_completed
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert_ne!(available_worker, owner);
    assert_ne!(available_worker, occupied_worker);
    assert_eq!(
        futures_lite::future::block_on(available),
        Ok(available_worker)
    );
    assert_eq!(
        futures_lite::future::block_on(occupied.cancel()),
        Err(JoinError::Cancelled)
    );
}

struct DropThread(Arc<Mutex<Option<thread::ThreadId>>>);
impl Drop for DropThread {
    fn drop(&mut self) {
        *self.0.lock() = Some(thread::current().id());
    }
}

struct PanicOnDrop(Arc<Mutex<Vec<thread::ThreadId>>>);
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.lock().push(thread::current().id());
        panic!("intentional capture drop panic");
    }
}

#[test]
fn foreign_wake_storm_cannot_move_or_destroy_local_future() {
    let owner = thread::current().id();
    let dropped = Arc::new(Mutex::new(None));
    let ready = Arc::new(AtomicBool::new(false));
    let (send_waker, receive_waker) = mpsc::sync_channel(1);
    let foreign_ready = ready.clone();
    let thread = thread::spawn(move || {
        let waker: std::task::Waker = receive_waker.recv().unwrap();
        for _ in 0..10_000 {
            waker.wake_by_ref();
        }
        foreign_ready.store(true, Ordering::Release);
        waker.wake_by_ref();
        waker
    });
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        let drop_probe = DropThread(dropped.clone());
        let local = Rc::new(11);
        let task = runtime::spawn_local(async move {
            let _drop_probe = drop_probe;
            let mut sent = false;
            std::future::poll_fn(|cx| {
                if !sent {
                    send_waker.send(cx.waker().clone()).unwrap();
                    sent = true;
                }
                if ready.load(Ordering::Acquire) {
                    Poll::Ready(*local)
                } else {
                    Poll::Pending
                }
            })
            .await
        })
        .unwrap();
        assert_eq!(task.await.unwrap(), 11);
    });
    assert_eq!(*dropped.lock(), Some(owner));
    let stale_waker = thread.join().unwrap();
    drop(runtime);
    // A standard Waker is still safe to call from a foreign thread after both
    // task completion and runtime destruction.
    thread::spawn(move || {
        for _ in 0..100 {
            stale_waker.wake_by_ref();
        }
    })
    .join()
    .unwrap();
}

#[test]
fn admission_cancel_detach_and_owner_shutdown_are_bounded() {
    let mut limits = config(1);
    limits.limits.max_tasks = 1;
    let mut runtime = Runtime::new(limits).unwrap();
    let dropped = Arc::new(Mutex::new(None));
    let owner = thread::current().id();
    runtime.block_on(async {
        let task = runtime::spawn_local(pending::<()>()).unwrap();
        assert_eq!(
            runtime::spawn_local(async {}).unwrap_err(),
            SpawnError::AtCapacity
        );
        assert_eq!(task.cancel().await, Err(JoinError::Cancelled));
        let probe = DropThread(dropped.clone());
        runtime::spawn_local(async move {
            let _probe = probe;
            pending::<()>().await;
        })
        .unwrap()
        .detach();
        runtime::yield_now().await;
    });
    assert_eq!(*dropped.lock(), None);
    drop(runtime);
    assert_eq!(*dropped.lock(), Some(owner));
}

#[test]
fn panic_is_reported_without_killing_worker() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        let task = runtime::spawn_local(async {
            panic!("intentional task panic");
        })
        .unwrap();
        assert_eq!(task.await, Err(JoinError::Panicked));
        assert_eq!(
            runtime::spawn_local(async { 13 }).unwrap().await.unwrap(),
            13
        );
    });
}

#[test]
fn cancelled_factory_or_future_drop_panic_releases_admission_on_owner() {
    for launch in [false, true] {
        let mut limits = config(1);
        limits.limits.max_tasks = 1;
        let mut runtime = Runtime::new(limits).unwrap();
        let owner = thread::current().id();
        let dropped = Arc::new(Mutex::new(Vec::new()));
        let launched = Arc::new(AtomicBool::new(false));
        runtime.block_on(async {
            let probe = PanicOnDrop(dropped.clone());
            let factory_launched = launched.clone();
            let task = runtime::spawn(move || {
                factory_launched.store(true, Ordering::Release);
                async move {
                    let _probe = probe;
                    pending::<()>().await;
                }
            })
            .unwrap();
            if launch {
                runtime::yield_now().await;
            }
            assert_eq!(launched.load(Ordering::Acquire), launch);
            assert_eq!(
                runtime::spawn(|| async {}).unwrap_err(),
                SpawnError::AtCapacity
            );
            assert_eq!(task.cancel().await, Err(JoinError::Cancelled));
            assert_eq!(launched.load(Ordering::Acquire), launch);
            assert_eq!(*dropped.lock(), vec![owner]);

            // One slot is returned, not leaked or returned twice: a replacement
            // occupies the entire budget until its own cancellation completes.
            let replacement = runtime::spawn(pending::<()>).unwrap();
            assert_eq!(
                runtime::spawn(|| async {}).unwrap_err(),
                SpawnError::AtCapacity
            );
            assert_eq!(replacement.cancel().await, Err(JoinError::Cancelled));
            assert_eq!(runtime::spawn(|| async { 13 }).unwrap().await, Ok(13));
        });
        drop(runtime);
        assert_eq!(*dropped.lock(), vec![owner]);
    }
}

#[test]
fn shutdown_isolates_factory_and_future_drop_panics_and_finishes_owner_cleanup() {
    for launch in [false, true] {
        let mut limits = config(1);
        limits.limits.max_tasks = 3;
        let mut runtime = Runtime::new(limits).unwrap();
        let handle = runtime.handle();
        let owner = thread::current().id();
        let panicking_drops = Arc::new(Mutex::new(Vec::new()));
        let local_drop = Arc::new(Mutex::new(None));
        let queued_drop = Arc::new(Mutex::new(None));
        let launched = Arc::new(AtomicBool::new(false));
        let queued_launched = Arc::new(AtomicBool::new(false));
        let (mut panicking, mut queued, mut local) = runtime.block_on(async {
            let probe = DropThread(local_drop.clone());
            let local = runtime::spawn_local(async move {
                let _probe = probe;
                pending::<()>().await;
            })
            .unwrap();
            runtime::yield_now().await;

            let probe = PanicOnDrop(panicking_drops.clone());
            let factory_launched = launched.clone();
            let panicking = runtime::spawn(move || {
                factory_launched.store(true, Ordering::Release);
                async move {
                    let _probe = probe;
                    pending::<()>().await;
                }
            })
            .unwrap();
            if launch {
                runtime::yield_now().await;
            }
            let probe = DropThread(queued_drop.clone());
            let factory_launched = queued_launched.clone();
            let queued = runtime::spawn(move || {
                factory_launched.store(true, Ordering::Release);
                async move {
                    let _probe = probe;
                    pending::<()>().await;
                }
            })
            .unwrap();
            (panicking, queued, local)
        });
        assert_eq!(launched.load(Ordering::Acquire), launch);
        assert!(!queued_launched.load(Ordering::Acquire));
        assert!(panicking_drops.lock().is_empty());
        assert_eq!(*local_drop.lock(), None);
        assert_eq!(*queued_drop.lock(), None);

        drop(runtime);

        assert_eq!(*panicking_drops.lock(), vec![owner]);
        assert_eq!(launched.load(Ordering::Acquire), launch);
        assert_eq!(*local_drop.lock(), Some(owner));
        assert_eq!(*queued_drop.lock(), Some(owner));
        assert!(!queued_launched.load(Ordering::Acquire));
        for join in [&mut panicking, &mut queued, &mut local] {
            assert_eq!(
                futures_lite::future::block_on(poll_once(join)),
                Some(Err(JoinError::Cancelled))
            );
        }
        assert_eq!(
            handle.spawn(|| async {}).unwrap_err(),
            SpawnError::ShuttingDown
        );
    }
}

#[test]
fn caller_local_tasks_resume_on_next_block_on() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    let completed = Rc::new(Cell::new(false));
    let mut task = None;
    runtime.block_on({
        let completed = completed.clone();
        async {
            task = Some(
                runtime::spawn_local(async move {
                    runtime::yield_now().await;
                    completed.set(true);
                })
                .unwrap(),
            );
        }
    });
    assert!(!completed.get());
    runtime.block_on(task.unwrap()).unwrap();
    assert!(completed.get());
}

#[test]
fn sleep_cancellation_releases_capacity_and_timeout_cancels_waiter() {
    let mut limits = config(1);
    limits.limits.max_operations = 2;
    let mut runtime = Runtime::new(limits).unwrap();
    runtime.block_on(async {
        {
            let mut first = time::sleep(Duration::from_secs(3600));
            let mut second = time::sleep(Duration::from_secs(3600));
            assert!(poll_once(&mut first).await.is_none());
            assert!(poll_once(&mut second).await.is_none());
            let error = time::sleep(Duration::from_secs(3600)).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        }
        time::sleep(Duration::from_millis(1)).await.unwrap();
        assert!(matches!(
            time::timeout(Duration::from_millis(1), pending::<()>()).await,
            Err(TimeoutError::Elapsed)
        ));
        assert_eq!(
            time::timeout(Duration::ZERO, async { 19 }).await.unwrap(),
            19
        );
    });
}

#[test]
fn idle_spin_does_not_delay_timer_deadlines() {
    let mut limits = config(1);
    limits.idle_spin = Duration::from_secs(2);
    let mut runtime = Runtime::new(limits).unwrap();
    let started = std::time::Instant::now();
    runtime
        .block_on(time::sleep(Duration::from_millis(10)))
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "idle spin delayed an already due timer"
    );
}

#[test]
fn tcp_cancelled_receive_keeps_queued_bytes_and_half_close() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        for v6 in [false, true].into_iter().take(support::family_count()) {
            let (client, server) = pair(v6).await;
            {
                let mut cancelled = pin!(server.recv());
                assert!(poll_once(cancelled.as_mut()).await.is_none());
                assert_eq!(
                    client
                        .send_all(payload(b"bytes surviving cancelled waiter"))
                        .await
                        .result
                        .unwrap(),
                    32
                );
                time::sleep(Duration::from_millis(2)).await.unwrap();
                // The pending waiter is dropped only after the driver has had
                // an opportunity to queue the received bytes.
            }
            client.shutdown(Shutdown::Write).unwrap();
            assert_eq!(
                bytes_until_eof(&server).await,
                b"bytes surviving cancelled waiter"
            );
            assert_eq!(
                server
                    .send_all(payload(b"reverse half remains open"))
                    .await
                    .result
                    .unwrap(),
                25
            );
            server.shutdown(Shutdown::Write).unwrap();
            assert_eq!(bytes_until_eof(&client).await, b"reverse half remains open");
        }
    });
}

#[test]
fn retained_receive_lease_is_immutable_during_next_receive() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        let (client, server) = pair(false).await;
        client
            .send_all(payload(b"first lease"))
            .await
            .result
            .unwrap();
        let first = blocks_exactly(&server, 11).await;
        assert_eq!(flatten(&first), b"first lease");
        client
            .send_all(payload(b"second lease"))
            .await
            .result
            .unwrap();
        let second = blocks_exactly(&server, 12).await;
        assert_eq!(flatten(&second), b"second lease");
        assert_eq!(flatten(&first), b"first lease");
    });
}

#[test]
fn full_duplex_large_tcp_writes_keep_order_and_exact_bytes() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        let (left, right) = pair(false).await;
        let left_bytes: Vec<u8> = (0..256 * 1024).map(|index| (index % 251) as u8).collect();
        let right_bytes: Vec<u8> = (0..192 * 1024).map(|index| (index % 239) as u8).collect();
        let left_send = payload(&left_bytes);
        let right_send = payload(&right_bytes);
        let left_side = async {
            let send = async {
                let outcome = left.send_all(left_send).await;
                assert_eq!(outcome.result.unwrap(), left_bytes.len());
                assert!(outcome.data.is_empty());
                left.shutdown(Shutdown::Write).unwrap();
            };
            zip(send, bytes_until_eof(&left)).await.1
        };
        let right_side = async {
            let send = async {
                assert_eq!(
                    right.send_all(right_send).await.result.unwrap(),
                    right_bytes.len()
                );
                right.shutdown(Shutdown::Write).unwrap();
            };
            zip(send, bytes_until_eof(&right)).await.1
        };
        let (left_received, right_received) = zip(left_side, right_side).await;
        assert_eq!(left_received, right_bytes);
        assert_eq!(right_received, left_bytes);
    });
}

#[test]
fn udp_zero_datagram_addresses_batches_and_truncation() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        for v6 in [false, true].into_iter().take(support::family_count()) {
            let sender = UdpSocket::bind(address(v6)).unwrap();
            let receiver = UdpSocket::bind(address(v6)).unwrap();
            assert_eq!(
                sender
                    .send_to(payload(b""), receiver.local_addr())
                    .await
                    .result
                    .unwrap(),
                0
            );
            let packet = receiver.recv().await.unwrap();
            assert_eq!(packet.data.as_slice(), b"");
            assert_eq!(packet.peer, Some(sender.local_addr()));
            assert!(!packet.truncated);
            let mut packets = [
                rivet::net::Datagram::new(payload(b"one"), Some(receiver.local_addr())),
                rivet::net::Datagram::new(payload(b"two"), Some(receiver.local_addr())),
                rivet::net::Datagram::new(payload(b"three"), Some(receiver.local_addr())),
            ];
            assert_eq!(sender.send_batch(&mut packets).await.unwrap(), 3);
            for (packet, length) in packets.iter_mut().zip([3, 3, 5]) {
                assert_eq!(packet.take_outcome().unwrap().result.unwrap(), length);
            }
            let mut received = Vec::new();
            while received.len() < 3 {
                let mut batch = [None, None, None];
                let count = receiver.recv_batch(&mut batch).await.unwrap();
                for packet in batch.iter_mut().take(count) {
                    received.push(packet.take().unwrap().data.as_slice().to_vec());
                }
            }
            assert_eq!(
                received,
                [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
            );
            let mut options = SocketOptions::udp();
            options.receive_chunk = 4;
            let short = UdpSocket::bind_with_options(address(v6), options).unwrap();
            sender
                .send_to(payload(b"0123456789"), short.local_addr())
                .await
                .result
                .unwrap();
            let packet = short.recv().await.unwrap();
            assert!(packet.truncated);
            assert_eq!(packet.data.as_slice(), b"0123");
            if let Some(original) = packet.original_len {
                assert_eq!(original, 10);
            }
        }
    });
}

#[test]
fn udp_waiter_timeout_does_not_turn_empty_packet_into_eof() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        let receiver = UdpSocket::bind(address(false)).unwrap();
        let sender =
            UdpSocket::bind_connected(address(false), receiver.local_addr(), SocketOptions::udp())
                .unwrap();
        assert!(matches!(
            time::timeout(Duration::from_millis(1), receiver.recv()).await,
            Err(TimeoutError::Elapsed)
        ));
        sender.send(payload(b"")).await.result.unwrap();
        assert_eq!(receiver.recv().await.unwrap().data.as_slice(), b"");
        sender.send(payload(b"after empty")).await.result.unwrap();
        assert_eq!(
            receiver.recv().await.unwrap().data.as_slice(),
            b"after empty"
        );
    });
}

#[test]
fn failed_import_returns_original_native_socket() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        #[cfg(windows)]
        let original: std::net::UdpSocket = support::registered_udp().into();
        #[cfg(not(windows))]
        let original = std::net::UdpSocket::bind(address(false)).unwrap();
        let address = original.local_addr().unwrap();
        let error = TcpStream::import(original.into(), SocketOptions::default()).unwrap_err();
        let returned = std::net::UdpSocket::from(error.socket);
        assert_eq!(returned.local_addr().unwrap(), address);
        let imported = UdpSocket::import(returned.into(), SocketOptions::udp()).unwrap();
        assert_eq!(imported.local_addr(), address);
    });
}

#[test]
fn serve_dispatches_idle_connection_before_first_data_io() {
    let owner = thread::current().id();
    let (sender, receiver) = mpsc::sync_channel(1);
    let mut runtime = Runtime::new(config(2)).unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind(address(false)).unwrap();
        let address = listener.local_addr();
        let serve = runtime::spawn_local(async move {
            listener
                .serve(move |stream| {
                    let sender = sender.clone();
                    async move {
                        let owner_local = Rc::new(37);
                        sender.send(thread::current().id()).unwrap();
                        while let Some(request) = stream.recv().await.unwrap() {
                            assert_eq!(*owner_local, 37);
                            stream
                                .send_all(SendPayload::Single(request))
                                .await
                                .result
                                .unwrap();
                        }
                        stream.shutdown(Shutdown::Write).unwrap();
                    }
                })
                .await
        })
        .unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        client
            .send_all(payload(b"dispatched native connection"))
            .await
            .result
            .unwrap();
        client.shutdown(Shutdown::Write).unwrap();
        assert_eq!(
            bytes_until_eof(&client).await,
            b"dispatched native connection"
        );
        assert_ne!(receiver.try_recv().unwrap(), owner);
        assert_eq!(serve.cancel().await.unwrap_err(), JoinError::Cancelled);
    });
}

#[test]
fn serve_acknowledges_dispatch_cancelled_before_import() {
    struct ReadySignal(AtomicBool);
    impl std::task::Wake for ReadySignal {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }
    let mut runtime = Runtime::new(config(1)).unwrap();
    let listener = runtime.block_on(async { TcpListener::bind(address(false)).unwrap() });
    let _client = std::net::TcpStream::connect(listener.local_addr()).unwrap();
    let signal = Arc::new(ReadySignal(AtomicBool::new(false)));
    let waker = std::task::Waker::from(signal.clone());
    let mut context = std::task::Context::from_waker(&waker);
    let mut serve = pin!(listener.serve(|_| pending::<()>()));
    runtime.block_on(async {
        assert!(serve.as_mut().poll(&mut context).is_pending());
        time::timeout(Duration::from_secs(5), async {
            while !signal.0.load(Ordering::Acquire) {
                time::sleep(Duration::from_millis(1)).await.unwrap();
            }
        })
        .await
        .unwrap();
    });
    // The accepted socket is now queued. Polling once consumes it and queues the
    // import factory, but returning the root prevents that local factory running.
    runtime.block_on(async {
        assert!(serve.as_mut().poll(&mut context).is_pending());
    });
    drop(runtime);
    let result = futures_lite::future::block_on(poll_once(serve.as_mut()));
    assert!(
        matches!(result, Some(Err(_))),
        "cancelled dispatch left serve permanently pending"
    );
}

#[test]
fn unsupported_splice_is_explicit_without_consuming_tcp_bytes() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(async {
        let (producer, source) = pair(false).await;
        let (destination, consumer) = pair(false).await;
        producer
            .send_all(payload(b"not silently forwarded"))
            .await
            .result
            .unwrap();
        producer.shutdown(Shutdown::Write).unwrap();
        let error = source.splice_to(&destination, 4096).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(bytes_until_eof(&source).await, b"not silently forwarded");
        assert!(matches!(
            time::timeout(Duration::from_millis(1), consumer.recv()).await,
            Err(TimeoutError::Elapsed)
        ));
    });
}

#[cfg(all(target_os = "linux", feature = "udp-gso", feature = "udp-gro"))]
#[test]
fn gso_gro_preserve_three_original_datagram_boundaries() {
    let config = config(1)
        .enable(rivet::Optimization::UdpGso)
        .enable(rivet::Optimization::UdpGro);
    let mut runtime = Runtime::new(config).unwrap();
    runtime.block_on(async {
        let sender = UdpSocket::bind(address(false)).unwrap();
        let receiver = UdpSocket::bind(address(false)).unwrap();
        let result = sender
            .send_segments(payload(b"aaaabbbbcc"), 4, Some(receiver.local_addr()))
            .await;
        assert_eq!(result.result.unwrap(), 10);
        for expected in [&b"aaaa"[..], &b"bbbb"[..], &b"cc"[..]] {
            let packet = receiver.recv().await.unwrap();
            assert_eq!(packet.data.as_slice(), expected);
            assert_eq!(packet.peer, Some(sender.local_addr()));
            assert!(!packet.truncated);
        }
        let rejected = sender
            .send_segments(payload(b"preserved"), 0, Some(receiver.local_addr()))
            .await;
        assert_eq!(
            rejected.result.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(rejected.data.segments()[0].as_slice(), b"preserved");
    });
}

#[cfg(all(target_os = "linux", feature = "tcp-splice"))]
#[test]
fn selected_splice_preserves_reverse_traffic_after_half_close() {
    let mut runtime = Runtime::new(config(1).enable(rivet::Optimization::TcpSplice)).unwrap();
    runtime.block_on(async {
        let (left_client, left_proxy) = pair(false).await;
        let (right_proxy, right_client) = pair(false).await;
        let left = async {
            left_client
                .send_all(payload(b"request through pipe"))
                .await
                .result
                .unwrap();
            left_client.shutdown(Shutdown::Write).unwrap();
            assert_eq!(bytes_until_eof(&left_client).await, b"response after EOF");
        };
        let right = async {
            assert_eq!(
                bytes_until_eof(&right_client).await,
                b"request through pipe"
            );
            right_client
                .send_all(payload(b"response after EOF"))
                .await
                .result
                .unwrap();
            right_client.shutdown(Shutdown::Write).unwrap();
        };
        let (_, forwarded) = zip(
            zip(left, right),
            rivet::net::splice_bidirectional(&left_proxy, &right_proxy),
        )
        .await;
        assert_eq!(forwarded.unwrap(), (20, 18));
    });
}

#[test]
fn shutdown_drains_native_send_and_preserves_returned_immutable_ownership() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    let (stream, _peer) = runtime.block_on(pair(false));
    let pool = runtime.buffer_pool();
    let mut data = pool.try_acquire().unwrap();
    data.extend_from_slice(b"lease survives native shutdown")
        .unwrap();
    let mut send = pin!(stream.send(SendPayload::Single(data.freeze())));
    let mut completed = None;
    runtime.block_on(std::future::poll_fn(|cx| {
        if let Poll::Ready(outcome) = send.as_mut().poll(cx) {
            completed = Some(outcome);
        }
        Poll::Ready(())
    }));
    drop(runtime);
    // Whether the native send wins the close race or is cancelled, its exact
    // immutable ownership must survive until the operation future is consumed.
    let outcome = completed.unwrap_or_else(|| futures_lite::future::block_on(send.as_mut()));
    let mut scratch = pool.try_acquire().unwrap();
    scratch
        .extend_from_slice(b"overwrite a different allocation")
        .unwrap();
    assert_eq!(
        outcome.data.segments()[0].as_slice(),
        b"lease survives native shutdown"
    );
}
