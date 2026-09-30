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
fn spawn_factory<F, Fut, T>(
    spawner: &impl runtime::Spawn,
    factory: F,
) -> Result<runtime::JoinHandle<T>, SpawnError>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    spawner.spawn(factory)
}
fn spawn_local<F: Future + 'static>(
    spawner: &impl runtime::LocalSpawn,
    future: F,
) -> Result<runtime::JoinHandle<F::Output>, SpawnError>
where
    F::Output: 'static,
{
    spawner.spawn_local(future)
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
    let task = spawn_factory(&runtime.handle(), || async {
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
        spawn_factory(&runtime.handle(), || async { 7 }).unwrap_err(),
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
        let task = spawn_local(&runtime::Current, async move {
            let _drop_probe = drop_probe;
            let mut sent = false;
            std::future::poll_fn(|cx| {
                if !sent {
                    send_waker.send(cx.waker().clone()).unwrap();
                    sent = true;
                }
                if ready.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            local
        })
        .unwrap();
        assert_eq!(*task.await.unwrap(), 11);
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
fn disabled_splice_is_explicit_without_consuming_tcp_bytes() {
    let mut runtime =
        Runtime::new(config(1).with_policy(rivet::Optimization::TcpSplice, rivet::Policy::Off))
            .unwrap();
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

thread_local! {
    static CANCELLED_IO: std::cell::RefCell<Option<std::pin::Pin<Box<dyn Future<Output = ()>>>>> =
        const { std::cell::RefCell::new(None) };
}

fn cancel_registered_io() -> bool {
    let future = CANCELLED_IO.with(|slot| slot.borrow_mut().take());
    let cancelled = future.is_some();
    drop(future);
    cancelled
}

struct CancelIoWake {
    cancelled: AtomicBool,
    root: std::task::Waker,
}
impl std::task::Wake for CancelIoWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if cancel_registered_io() {
            self.cancelled.store(true, Ordering::Release);
        }
        self.root.wake_by_ref();
    }
}

async fn cancel_on_io_wake(future: impl Future<Output = ()> + 'static) -> Arc<CancelIoWake> {
    let mut future = Box::pin(future);
    let notice = std::future::poll_fn(|cx| {
        let notice = Arc::new(CancelIoWake {
            cancelled: AtomicBool::new(false),
            root: cx.waker().clone(),
        });
        let waker = std::task::Waker::from(notice.clone());
        assert!(
            future
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        Poll::Ready(notice)
    })
    .await;
    CANCELLED_IO.with(|slot| *slot.borrow_mut() = Some(future));
    notice
}

async fn wait_for_io_cancellation(notice: &CancelIoWake) {
    std::future::poll_fn(|_| {
        if notice.cancelled.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

#[test]
fn completion_wakers_can_destroy_pending_tcp_operations() {
    use std::io::{Read, Write};
    let mut owner = Runtime::new(config(1)).unwrap();
    owner.block_on(async {
        time::timeout(Duration::from_secs(10), async {
            let listener = Rc::new(TcpListener::bind(address(false)).unwrap());
            let accepting = listener.clone();
            let notice = cancel_on_io_wake(async move {
                let _ = accepting.accept().await;
            })
            .await;
            let mut client = std::net::TcpStream::connect(listener.local_addr()).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            client
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            wait_for_io_cancellation(&notice).await;
            // Accept cancellation must retain the newly queued connection.
            let server = Rc::new(listener.accept().await.unwrap());

            let receiving = server.clone();
            let notice = cancel_on_io_wake(async move {
                let _ = receiving.recv().await;
            })
            .await;
            client.write_all(b"retained receive").unwrap();
            wait_for_io_cancellation(&notice).await;
            let received = blocks_exactly(&server, b"retained receive".len()).await;
            assert_eq!(flatten(&received), b"retained receive");

            let sending = server.clone();
            let data = payload(b"submitted send");
            let notice = cancel_on_io_wake(async move {
                let _ = sending.send(data).await;
            })
            .await;
            wait_for_io_cancellation(&notice).await;
            let mut bytes = [0; 14];
            client.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"submitted send");

            let native_listener = std::net::TcpListener::bind(address(false)).unwrap();
            let peer = native_listener.local_addr().unwrap();
            let notice = cancel_on_io_wake(async move {
                let _ = TcpStream::connect(peer).await;
            })
            .await;
            wait_for_io_cancellation(&notice).await;
            assert!(!cancel_registered_io());
            assert_eq!(runtime::spawn_local(async { 17 }).unwrap().await, Ok(17));
        })
        .await
        .unwrap();
    });
}

#[test]
fn socket_hooks_can_cancel_pending_accept_and_receive_lanes() {
    use std::io::Write;

    async fn cancel_in_hook(future: impl Future<Output = ()> + 'static, via_connect: bool) {
        let _notice = cancel_on_io_wake(future).await;
        let options = SocketOptions {
            hook: Some(Arc::new(|_: rivet::socket::BorrowedSocket<'_>| {
                assert!(cancel_registered_io());
                Ok(())
            })),
            ..SocketOptions::default()
        };
        if via_connect {
            let target = std::net::TcpListener::bind(address(false)).unwrap();
            let _trigger = TcpStream::connect_with_options(target.local_addr().unwrap(), options)
                .await
                .unwrap();
        } else {
            let _trigger = TcpListener::bind_with_options(address(false), options).unwrap();
        }
        assert!(!cancel_registered_io());
    }

    let mut owner = Runtime::new(config(1)).unwrap();
    owner.block_on(async {
        time::timeout(Duration::from_secs(10), async {
            // Keep each original Rc alive: the hook cancels only the waiter,
            // not the socket or its persistent native receive/accept operation.
            let listener = Rc::new(TcpListener::bind(address(false)).unwrap());
            for via_connect in [false, true] {
                let accepting = listener.clone();
                cancel_in_hook(
                    async move {
                        let _ = accepting.accept().await;
                    },
                    via_connect,
                )
                .await;
            }
            let mut peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
            peer.set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let stream = Rc::new(listener.accept().await.unwrap());
            for via_connect in [false, true] {
                let receiving = stream.clone();
                cancel_in_hook(
                    async move {
                        let _ = receiving.recv().await;
                    },
                    via_connect,
                )
                .await;
            }
            peer.write_all(b"TCP after hook cancellation").unwrap();
            assert_eq!(
                flatten(&blocks_exactly(&stream, b"TCP after hook cancellation".len()).await),
                b"TCP after hook cancellation"
            );

            let socket = Rc::new(UdpSocket::bind(address(false)).unwrap());
            let sender = std::net::UdpSocket::bind(address(false)).unwrap();
            for batch in [false, true] {
                for via_connect in [false, true] {
                    let receiving = socket.clone();
                    cancel_in_hook(
                        async move {
                            if batch {
                                let mut output = [None, None];
                                let _ = receiving.recv_batch(&mut output).await;
                            } else {
                                let _ = receiving.recv().await;
                            }
                        },
                        via_connect,
                    )
                    .await;
                }
                assert!(!socket.receive_snapshot().unwrap().waiter_registered());
                sender
                    .send_to(b"UDP after hook cancellation", socket.local_addr())
                    .unwrap();
                let received = socket.recv().await.unwrap();
                assert_eq!(received.data.as_slice(), b"UDP after hook cancellation");
                assert_eq!(received.peer, Some(sender.local_addr().unwrap()));
            }
        })
        .await
        .unwrap();
    });
}

struct CancelIoOnDrop(Arc<AtomicBool>);
impl std::task::Wake for CancelIoOnDrop {
    fn wake(self: Arc<Self>) {
        // The last owned reference runs the cancellation destructor below.
        drop(self);
    }
}
impl Drop for CancelIoOnDrop {
    fn drop(&mut self) {
        self.0.store(cancel_registered_io(), Ordering::Release);
    }
}

#[test]
fn replacing_or_removing_an_io_waker_can_cancel_another_waiter() {
    use std::{
        pin::Pin,
        task::{Context, Waker},
    };
    for replace in [false, true] {
        let mut owner = Runtime::new(config(1)).unwrap();
        owner.block_on(async {
            let victim = Rc::new(UdpSocket::bind(address(false)).unwrap());
            let receiving = victim.clone();
            let _notice = cancel_on_io_wake(async move {
                let _ = receiving.recv().await;
            })
            .await;
            let socket = UdpSocket::bind(address(false)).unwrap();
            let mut trigger = socket.recv();
            let cancelled = Arc::new(AtomicBool::new(false));
            {
                let waker = Waker::from(Arc::new(CancelIoOnDrop(cancelled.clone())));
                assert!(
                    Pin::new(&mut trigger)
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
            }
            if replace {
                assert!(
                    Pin::new(&mut trigger)
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
            } else {
                drop(trigger);
            }
            assert!(cancelled.load(Ordering::Acquire));
            assert!(!victim.receive_snapshot().unwrap().waiter_registered());
            let mut replacement = victim.recv();
            assert!(poll_once(&mut replacement).await.is_none());
        });
    }
}

#[test]
fn panicking_panic_payloads_do_not_strand_async_joins() {
    use std::process::{Child, Command};
    struct Guard(Child);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Guard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "async_panic_payload_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("RIVET_ASYNC_PANIC_PAYLOAD_CHILD", "1")
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "panic-payload child failed: {status}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "panic-payload child did not finish"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn async_panic_payload_child() {
    use std::{pin::Pin, sync::atomic::AtomicUsize, task::Context};
    if std::env::var_os("RIVET_ASYNC_PANIC_PAYLOAD_CHILD").is_none() {
        return;
    }
    struct Payload(Arc<AtomicUsize>);
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
            panic!("intentional panic-payload destructor panic");
        }
    }
    struct DropFuture {
        ready: bool,
        drops: Arc<AtomicUsize>,
    }
    impl Future for DropFuture {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for DropFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
            std::panic::panic_any(Payload(self.drops.clone()));
        }
    }
    for case in 0..7 {
        let mut limits = config(1);
        limits.limits.max_tasks = 1;
        let mut owner = Runtime::new(limits).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let counter = drops.clone();
        let mut task = None;
        owner.block_on(async {
            task = Some(match case {
                0 => runtime::spawn_local(async move {
                    std::panic::panic_any(Payload(counter));
                })
                .unwrap(),
                1 => runtime::spawn(move || -> std::future::Ready<()> {
                    std::panic::panic_any(Payload(counter));
                })
                .unwrap(),
                2 | 3 | 5 => runtime::spawn_local(DropFuture {
                    ready: case == 2,
                    drops: counter,
                })
                .unwrap(),
                4 | 6 => {
                    let capture = DropFuture {
                        ready: false,
                        drops: counter,
                    };
                    runtime::spawn(move || async move {
                        let _capture = capture;
                        pending::<()>().await;
                    })
                    .unwrap()
                }
                _ => unreachable!(),
            });
        });
        let task = task.unwrap();
        let expected = if case < 3 {
            JoinError::Panicked
        } else {
            JoinError::Cancelled
        };
        let finished = task.abort_handle();
        if case >= 5 {
            drop(owner);
            assert_eq!(
                futures_lite::future::block_on(poll_once(task)),
                Some(Err(expected))
            );
        } else {
            if case >= 3 {
                task.abort();
            }
            owner.block_on(async {
                assert_eq!(
                    time::timeout(Duration::from_secs(5), task).await.unwrap(),
                    Err(expected)
                );
                // Admission is returned exactly once, not leaked or doubled.
                let replacement = runtime::spawn_local(pending::<()>()).unwrap();
                assert_eq!(
                    runtime::spawn_local(async {}).unwrap_err(),
                    SpawnError::AtCapacity
                );
                assert_eq!(replacement.cancel().await, Err(JoinError::Cancelled));
                assert_eq!(runtime::spawn_local(async { 23 }).unwrap().await, Ok(23));
            });
        }
        assert!(finished.is_finished());
        assert_eq!(drops.load(Ordering::Acquire), if case < 2 { 1 } else { 2 });
    }
}

#[test]
fn cloning_an_io_waker_can_cancel_another_waiter() {
    use std::{
        mem::ManuallyDrop,
        pin::Pin,
        task::{Context, RawWaker, RawWakerVTable, Waker},
    };
    struct CloneWake(AtomicBool);
    unsafe fn clone(data: *const ()) -> RawWaker {
        // Each RawWaker owns one Arc. Its clone hook may synchronously run
        // arbitrary owner-local code without making the Waker itself !Send.
        let state = ManuallyDrop::new(unsafe { Arc::<CloneWake>::from_raw(data.cast()) });
        if state.0.swap(false, Ordering::AcqRel) {
            assert!(cancel_registered_io());
        }
        RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &VTABLE)
    }
    unsafe fn consume(data: *const ()) {
        drop(unsafe { Arc::<CloneWake>::from_raw(data.cast()) });
    }
    unsafe fn wake_by_ref(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, consume, wake_by_ref, consume);

    let mut owner = Runtime::new(config(1)).unwrap();
    owner.block_on(async {
        let victim = Rc::new(UdpSocket::bind(address(false)).unwrap());
        let receiving = victim.clone();
        let _notice = cancel_on_io_wake(async move {
            let _ = receiving.recv().await;
        })
        .await;
        let state = Arc::new(CloneWake(AtomicBool::new(true)));
        let raw = RawWaker::new(Arc::into_raw(state.clone()).cast(), &VTABLE);
        let waker = unsafe { Waker::from_raw(raw) };
        let trigger = UdpSocket::bind(address(false)).unwrap();
        let mut receiving = trigger.recv();
        assert!(
            Pin::new(&mut receiving)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(!state.0.load(Ordering::Acquire));
        assert!(!victim.receive_snapshot().unwrap().waiter_registered());
        let mut replacement = victim.recv();
        assert!(poll_once(&mut replacement).await.is_none());
    });
}

#[test]
fn shutdown_callback_panics_still_close_waiters_and_native_resources() {
    use std::process::{Child, Command};
    struct Guard(Child);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Guard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "shutdown_callback_panic_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("RIVET_SHUTDOWN_CALLBACK_PANIC_CHILD", "1")
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "shutdown-callback child failed: {status}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "shutdown-callback child did not finish"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn shutdown_callback_panic_child() {
    use std::{
        io::Read,
        pin::Pin,
        sync::atomic::AtomicUsize,
        task::{Context, Wake, Waker},
    };
    if std::env::var_os("RIVET_SHUTDOWN_CALLBACK_PANIC_CHILD").is_none() {
        return;
    }
    struct Payload(Arc<AtomicUsize>);
    impl Drop for Payload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
            panic!("intentional shutdown panic-payload destructor panic");
        }
    }
    struct CallbackWake {
        called: Arc<AtomicUsize>,
        payload_drops: Arc<AtomicUsize>,
        panics: bool,
        on_drop: bool,
        payload_panics: bool,
    }
    impl CallbackWake {
        fn call(&self) {
            self.called.fetch_add(1, Ordering::AcqRel);
            if self.panics {
                if self.payload_panics {
                    std::panic::panic_any(Payload(self.payload_drops.clone()));
                }
                panic!("intentional shutdown callback panic");
            }
        }
    }
    impl Wake for CallbackWake {
        fn wake(self: Arc<Self>) {
            if !self.on_drop {
                self.call();
            }
        }
    }
    impl Drop for CallbackWake {
        fn drop(&mut self) {
            if self.on_drop {
                self.call();
            }
        }
    }
    fn closed<T>(result: Poll<io::Result<T>>) {
        match result {
            Poll::Ready(Err(error)) => assert_eq!(error.kind(), io::ErrorKind::BrokenPipe),
            _ => panic!("retained operation did not reach its shutdown error"),
        }
    }

    // Timer callbacks, socket-close callbacks, operation-completion callbacks
    // and detached registration destruction must each leave shutdown running.
    for source in 0..4 {
        for payload_panics in [false, true] {
            let mut owner = Runtime::new(config(1)).unwrap();
            let pool = owner.buffer_pool();
            let baseline = pool.usage();
            let (listener, stream, mut peer, udp) = owner.block_on(async {
                let listener = TcpListener::bind(address(false)).unwrap();
                let peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
                let stream = listener.accept().await.unwrap();
                let udp = Rc::new(
                    UdpSocket::bind_with_options(
                        address(false),
                        SocketOptions {
                            reuse_address: false,
                            ..SocketOptions::default()
                        },
                    )
                    .unwrap(),
                );
                (listener, stream, peer, udp)
            });
            let target = std::net::TcpListener::bind(address(false)).unwrap();
            let mut connects = [
                TcpStream::connect(target.local_addr().unwrap()),
                TcpStream::connect(target.local_addr().unwrap()),
            ];
            let mut sleeps = [
                time::sleep(Duration::from_secs(3600)),
                time::sleep(Duration::from_secs(3600)),
            ];
            let mut accepting = pin!(listener.accept());
            let mut receiving = pin!(stream.recv());
            let mut cancellation_sleep = None;
            let mut cancellation = None;
            let mut callbacks = Vec::new();
            let payload_drops = Arc::new(AtomicUsize::new(0));
            let mut waker = |panics, on_drop| {
                let called = Arc::new(AtomicUsize::new(0));
                callbacks.push(called.clone());
                Waker::from(Arc::new(CallbackWake {
                    called,
                    payload_drops: payload_drops.clone(),
                    panics,
                    on_drop,
                    payload_panics,
                }))
            };
            owner.block_on(std::future::poll_fn(|_| {
                for sleep in &mut sleeps {
                    let wake = waker(source == 0, false);
                    assert!(
                        Pin::new(sleep)
                            .poll(&mut Context::from_waker(&wake))
                            .is_pending()
                    );
                }
                let wake = waker(source == 1, false);
                assert!(
                    accepting
                        .as_mut()
                        .poll(&mut Context::from_waker(&wake))
                        .is_pending()
                );
                drop(wake);
                let wake = waker(source == 1, false);
                assert!(
                    receiving
                        .as_mut()
                        .poll(&mut Context::from_waker(&wake))
                        .is_pending()
                );
                drop(wake);
                // Return Ready without a worker turn: these Connects retain
                // the Worker after Runtime drop, exposing skipped native drain.
                for connect in &mut connects {
                    let wake = waker(source == 2, false);
                    assert!(
                        Pin::new(connect)
                            .poll(&mut Context::from_waker(&wake))
                            .is_pending()
                    );
                }
                if source == 3 {
                    let socket = udp.clone();
                    let mut future = Box::pin(async move {
                        let _ = socket.recv().await;
                    });
                    let wake = waker(true, true);
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(&wake))
                            .is_pending()
                    );
                    drop(wake);
                    CANCELLED_IO.with(|slot| *slot.borrow_mut() = Some(future));
                    let notice = Arc::new(CancelIoWake {
                        cancelled: AtomicBool::new(false),
                        root: Waker::noop().clone(),
                    });
                    let wake = Waker::from(notice.clone());
                    let mut sleep = time::sleep(Duration::from_secs(3600));
                    assert!(
                        Pin::new(&mut sleep)
                            .poll(&mut Context::from_waker(&wake))
                            .is_pending()
                    );
                    cancellation_sleep = Some(sleep);
                    cancellation = Some(notice);
                }
                Poll::Ready(())
            }));
            drop(owner);

            for called in callbacks {
                assert_eq!(called.load(Ordering::Acquire), 1);
            }
            assert_eq!(
                payload_drops.load(Ordering::Acquire),
                if payload_panics {
                    if source == 3 { 1 } else { 2 }
                } else {
                    0
                }
            );
            if let Some(notice) = cancellation {
                // This statement follows the throwing registration destructor
                // inside the timer callback, not merely an outer panic catch.
                assert!(notice.cancelled.load(Ordering::Acquire));
                assert!(!cancel_registered_io());
            }
            let mut cx = Context::from_waker(Waker::noop());
            for sleep in &mut sleeps {
                closed(Pin::new(sleep).poll(&mut cx));
            }
            if let Some(sleep) = &mut cancellation_sleep {
                closed(Pin::new(sleep).poll(&mut cx));
            }
            closed(accepting.as_mut().poll(&mut cx));
            closed(receiving.as_mut().poll(&mut cx));
            for connect in &mut connects {
                closed(Pin::new(connect).poll(&mut cx));
            }
            assert!(pool.usage().leases_in_use() <= baseline.leases_in_use());
            assert!(pool.usage().payload_in_use() <= baseline.payload_in_use());
            // Socket wrappers and completed Connects are still retained. Their
            // native resources must nevertheless already have been released.
            let _rebound = std::net::UdpSocket::bind(udp.local_addr()).unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            match peer.read(&mut [0; 1]) {
                Ok(0) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                    ) => {}
                result => panic!("native TCP peer remained open after shutdown: {result:?}"),
            }
        }
    }
}

#[test]
fn running_io_callbacks_still_propagate_panics_without_losing_the_receive_lane() {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        pin::Pin,
        task::{Context, Wake, Waker},
    };
    struct CallbackPanic;
    struct Panics {
        on_drop: bool,
    }
    impl Wake for Panics {
        fn wake(self: Arc<Self>) {
            if !self.on_drop {
                panic_any(CallbackPanic);
            }
        }
    }
    impl Drop for Panics {
        fn drop(&mut self) {
            if self.on_drop {
                panic_any(CallbackPanic);
            }
        }
    }
    for on_drop in [false, true] {
        let mut owner = Runtime::new(config(1)).unwrap();
        let socket = owner.block_on(async { UdpSocket::bind(address(false)).unwrap() });
        let mut receiving = socket.recv();
        owner.block_on(std::future::poll_fn(|_| {
            let waker = Waker::from(Arc::new(Panics { on_drop }));
            assert!(
                Pin::new(&mut receiving)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            Poll::Ready(())
        }));
        let sender = std::net::UdpSocket::bind(address(false)).unwrap();
        sender
            .send_to(b"survives a running callback panic", socket.local_addr())
            .unwrap();
        if on_drop {
            let panic = catch_unwind(AssertUnwindSafe(|| drop(receiving))).unwrap_err();
            assert!(panic.is::<CallbackPanic>());
        } else {
            let panic = catch_unwind(AssertUnwindSafe(|| {
                owner.block_on(time::timeout(Duration::from_secs(5), pending::<()>()))
            }))
            .unwrap_err();
            assert!(panic.is::<CallbackPanic>());
            drop(receiving);
        }
        owner.block_on(async {
            let received = time::timeout(Duration::from_secs(5), socket.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                received.data.as_slice(),
                b"survives a running callback panic"
            );
        });
    }
}

#[test]
fn rejected_connect_hooks_release_unsubmitted_admission_and_wakers() {
    use std::{
        io::Read,
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        pin::Pin,
        sync::atomic::AtomicUsize,
        task::{Context, Wake, Waker},
    };
    struct HookPanic;
    struct Registration(Arc<AtomicUsize>);
    impl Wake for Registration {
        fn wake(self: Arc<Self>) {
            drop(self);
        }
    }
    impl Drop for Registration {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    for panics in [false, true] {
        let mut limits = config(1);
        limits.limits.max_operations = 1;
        // Exercise Core admission rollback, independently of optional native
        // direct-descriptor creation and its separate unwind ownership.
        #[cfg(target_os = "linux")]
        let limits = limits.with_policy(rivet::Optimization::DirectDescriptors, rivet::Policy::Off);
        let mut owner = Runtime::new(limits).unwrap();
        let target = std::net::TcpListener::bind(address(false)).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        for attempt in 1..=3 {
            let mut connect = TcpStream::connect_with_options(
                target.local_addr().unwrap(),
                SocketOptions {
                    hook: Some(Arc::new(move |_: rivet::socket::BorrowedSocket<'_>| {
                        if panics {
                            panic_any(HookPanic);
                        }
                        Err(io::Error::from(io::ErrorKind::PermissionDenied))
                    })),
                    ..SocketOptions::default()
                },
            );
            let waker = Waker::from(Arc::new(Registration(drops.clone())));
            let result = catch_unwind(AssertUnwindSafe(|| {
                owner.block_on(std::future::poll_fn(|_| {
                    Poll::Ready(Pin::new(&mut connect).poll(&mut Context::from_waker(&waker)))
                }))
            }));
            if panics {
                assert!(result.unwrap_err().is::<HookPanic>());
            } else {
                match result.unwrap() {
                    Poll::Ready(Err(error)) => {
                        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                    }
                    result => panic!("hook rejection was not preserved: {result:?}"),
                }
            }
            drop(waker);
            assert_eq!(drops.load(Ordering::Acquire), attempt);
            drop(connect);
        }
        owner.block_on(async {
            time::timeout(Duration::from_secs(5), async {
                let stream = TcpStream::connect(target.local_addr().unwrap())
                    .await
                    .unwrap();
                let (mut peer, _) = target.accept().unwrap();
                peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                stream
                    .send_all(payload(b"after rejected hooks"))
                    .await
                    .result
                    .unwrap();
                let mut bytes = [0; 20];
                peer.read_exact(&mut bytes).unwrap();
                assert_eq!(&bytes, b"after rejected hooks");
            })
            .await
            .unwrap();
        });
    }
}

mod hook_waker_destruction {
    use super::*;
    use std::{
        cell::RefCell,
        io::{Read, Write},
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        pin::Pin,
        sync::atomic::AtomicUsize,
        task::{Context, Wake, Waker},
    };

    thread_local! {
        static WAITERS: RefCell<Vec<Pin<Box<dyn Future<Output = ()>>>>> = const { RefCell::new(Vec::new()) };
    }

    struct Stats {
        drops: AtomicUsize,
        depth: AtomicUsize,
        maximum_depth: AtomicUsize,
        in_hook: AtomicBool,
        sockets: usize,
    }
    impl Stats {
        fn new(sockets: usize) -> Arc<Self> {
            Arc::new(Self {
                drops: AtomicUsize::new(0),
                depth: AtomicUsize::new(0),
                maximum_depth: AtomicUsize::new(0),
                in_hook: AtomicBool::new(false),
                sockets,
            })
        }
    }
    struct HookPanic;
    struct CallbackPanic;
    struct BadPayload;
    impl Drop for BadPayload {
        fn drop(&mut self) {
            panic!("intentional deferred panic-payload destructor");
        }
    }
    struct NativeOnDrop {
        stats: Arc<Stats>,
        panic: u8,
        reenter: bool,
    }
    impl Wake for NativeOnDrop {
        fn wake(self: Arc<Self>) {
            panic!("a cancelled waiter must be dropped, not woken");
        }
    }
    impl Drop for NativeOnDrop {
        fn drop(&mut self) {
            assert!(!self.stats.in_hook.load(Ordering::Acquire));
            let depth = self.stats.depth.fetch_add(1, Ordering::AcqRel) + 1;
            self.stats.maximum_depth.fetch_max(depth, Ordering::AcqRel);
            struct Reset<'a>(&'a AtomicUsize);
            impl Drop for Reset<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
            let _reset = Reset(&self.stats.depth);
            let resources = runtime::resource_snapshot().unwrap();
            assert_eq!(resources.sockets(), self.stats.sockets);
            // Exercise Driver on both creation and close, not only Core reentry.
            drop(TcpListener::bind(address(false)).unwrap());
            self.stats.drops.fetch_add(1, Ordering::AcqRel);
            if self.reenter {
                replace_retired_lane_repeatedly();
                // The other retired source must stay queued while this
                // destructor registers/cancels new I/O, not run recursively.
                assert_eq!(self.stats.drops.load(Ordering::Acquire), 1);
            }
            match self.panic {
                0 => {}
                1 => panic_any(CallbackPanic),
                2 => panic_any(BadPayload),
                _ => unreachable!(),
            }
        }
    }

    fn register(
        future: impl Future<Output = ()> + 'static,
        stats: &Arc<Stats>,
        panic: u8,
        reenter: bool,
    ) {
        let mut future = Box::pin(future);
        let waker = Waker::from(Arc::new(NativeOnDrop {
            stats: stats.clone(),
            panic,
            reenter,
        }));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        WAITERS.with(|slot| slot.borrow_mut().push(future));
        // The Core registration is now the sole owner of this Wake object.
    }
    fn options(stats: &Arc<Stats>, exit: u8) -> SocketOptions {
        let stats = stats.clone();
        SocketOptions {
            hook: Some(Arc::new(move |_: rivet::socket::BorrowedSocket<'_>| {
                assert!(!stats.in_hook.swap(true, Ordering::AcqRel));
                struct Reset<'a>(&'a AtomicBool);
                impl Drop for Reset<'_> {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::Release);
                    }
                }
                let _reset = Reset(&stats.in_hook);
                let waiters = WAITERS.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
                drop(waiters);
                assert_eq!(stats.drops.load(Ordering::Acquire), 0);
                match exit {
                    0 => Ok(()),
                    1 => Err(io::Error::from(io::ErrorKind::PermissionDenied)),
                    2 => panic_any(HookPanic),
                    _ => unreachable!(),
                }
            })),
            ..SocketOptions::default()
        }
    }

    struct Lanes {
        listener: Rc<TcpListener>,
        stream: Rc<TcpStream>,
        peer: std::net::TcpStream,
        udp: [Rc<UdpSocket>; 2],
    }
    impl Lanes {
        async fn new() -> Self {
            let listener = Rc::new(TcpListener::bind(address(false)).unwrap());
            let peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
            peer.set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let stream = Rc::new(listener.accept().await.unwrap());
            let udp = std::array::from_fn(|_| Rc::new(UdpSocket::bind(address(false)).unwrap()));
            Self {
                listener,
                stream,
                peer,
                udp,
            }
        }
        fn register(&self, stats: &Arc<Stats>, panic_callbacks: bool) {
            let listener = self.listener.clone();
            register(
                async move {
                    let _ = listener.accept().await;
                },
                stats,
                u8::from(panic_callbacks),
                false,
            );
            let stream = self.stream.clone();
            register(
                async move {
                    let _ = stream.recv().await;
                },
                stats,
                if panic_callbacks { 2 } else { 0 },
                false,
            );
            let udp = self.udp[0].clone();
            register(
                async move {
                    let _ = udp.recv().await;
                },
                stats,
                0,
                false,
            );
            let udp = self.udp[1].clone();
            register(
                async move {
                    let mut output = [None, None];
                    let _ = udp.recv_batch(&mut output).await;
                },
                stats,
                0,
                false,
            );
        }
        async fn prove_reusable(&mut self) {
            let mut peer = std::net::TcpStream::connect(self.listener.local_addr()).unwrap();
            peer.set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let accepted = self.listener.accept().await.unwrap();
            peer.write_all(b"accept").unwrap();
            assert_eq!(flatten(&blocks_exactly(&accepted, 6).await), b"accept");
            self.peer.write_all(b"stream").unwrap();
            assert_eq!(flatten(&blocks_exactly(&self.stream, 6).await), b"stream");
            let sender = std::net::UdpSocket::bind(address(false)).unwrap();
            for udp in &self.udp {
                assert!(!udp.receive_snapshot().unwrap().waiter_registered());
                sender.send_to(b"datagram", udp.local_addr()).unwrap();
                let received = udp.recv().await.unwrap();
                assert_eq!(received.data.as_slice(), b"datagram");
                assert_eq!(received.peer, Some(sender.local_addr().unwrap()));
            }
        }
    }

    #[test]
    fn every_native_hook_releases_cancelled_wakers_after_publication_or_rollback() {
        for entry in 0..4 {
            for exit in 0..3 {
                let limits = config(1);
                // Hook unwinding is independent of optional direct-create ownership.
                #[cfg(target_os = "linux")]
                let limits = if exit == 2 {
                    limits.with_policy(rivet::Optimization::DirectDescriptors, rivet::Policy::Off)
                } else {
                    limits
                };
                let mut owner = Runtime::new(limits).unwrap();
                owner.block_on(async {
                    time::timeout(Duration::from_secs(10), async {
                        let mut lanes = Lanes::new().await;
                        let stats = Stats::new(4 + usize::from(exit == 0 && entry != 3));
                        lanes.register(&stats, exit == 2);
                        let options = options(&stats, exit);
                        let target = std::net::TcpListener::bind(address(false)).unwrap();
                        let result = catch_unwind(AssertUnwindSafe(|| match entry {
                            0 => TcpListener::bind_with_options(address(false), options).map(drop),
                            1 => UdpSocket::bind_with_options(address(false), options).map(drop),
                            2 => {
                                #[cfg(windows)]
                                let socket = support::registered_udp().into();
                                #[cfg(unix)]
                                let socket =
                                    std::net::UdpSocket::bind(address(false)).unwrap().into();
                                UdpSocket::import(socket, options)
                                    .map(drop)
                                    .map_err(|error| error.error)
                            }
                            3 => {
                                let mut connect = TcpStream::connect_with_options(
                                    target.local_addr().unwrap(),
                                    options,
                                );
                                match Pin::new(&mut connect)
                                    .poll(&mut Context::from_waker(Waker::noop()))
                                {
                                    Poll::Pending => Ok(()),
                                    Poll::Ready(result) => result.map(drop),
                                }
                            }
                            _ => unreachable!(),
                        }));
                        match exit {
                            0 => result.unwrap().unwrap(),
                            1 => assert_eq!(
                                result.unwrap().unwrap_err().kind(),
                                io::ErrorKind::PermissionDenied
                            ),
                            2 => assert!(result.unwrap_err().is::<HookPanic>()),
                            _ => unreachable!(),
                        }
                        assert_eq!(stats.drops.load(Ordering::Acquire), 4);
                        assert_eq!(stats.maximum_depth.load(Ordering::Acquire), 1);
                        lanes.prove_reusable().await;
                    })
                    .await
                    .unwrap();
                });
            }
        }
    }

    #[test]
    fn callback_panics_preserve_the_connect_token_and_drain_other_retirements() {
        let mut owner = Runtime::new(config(1)).unwrap();
        owner.block_on(async {
            time::timeout(Duration::from_secs(10), async {
                let mut lanes = Lanes::new().await;
                let stats = Stats::new(4);
                lanes.register(&stats, true);
                let target = std::net::TcpListener::bind(address(false)).unwrap();
                let mut connect = TcpStream::connect_with_options(
                    target.local_addr().unwrap(),
                    options(&stats, 0),
                );
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    Pin::new(&mut connect).poll(&mut Context::from_waker(Waker::noop()))
                }))
                .unwrap_err();
                assert!(panic.is::<CallbackPanic>());
                assert_eq!(stats.drops.load(Ordering::Acquire), 4);
                assert_eq!(stats.maximum_depth.load(Ordering::Acquire), 1);
                // Continue the same future: a lost handoff would admit a second
                // connection and leave the first accepted peer without these bytes.
                let stream = connect.await.unwrap();
                let (mut peer, _) = target.accept().unwrap();
                peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                stream
                    .send_all(payload(b"original token"))
                    .await
                    .result
                    .unwrap();
                let mut bytes = [0; 14];
                peer.read_exact(&mut bytes).unwrap();
                assert_eq!(&bytes, b"original token");
                lanes.prove_reusable().await;
            })
            .await
            .unwrap();
        });
    }

    #[cfg(windows)]
    #[test]
    fn accept_preparation_hooks_defer_destruction_until_after_event_dispatch() {
        let mut owner = Runtime::new(config(1)).unwrap();
        owner.block_on(async {
            time::timeout(Duration::from_secs(10), async {
                let udp = Rc::new(UdpSocket::bind(address(false)).unwrap());
                let stats = Stats::new(2);
                let armed = Arc::new(AtomicBool::new(false));
                let trigger = armed.clone();
                let cancel = options(&stats, 0).hook.unwrap();
                let listener = TcpListener::bind_with_options(
                    address(false),
                    SocketOptions {
                        hook: Some(Arc::new(
                            move |socket: rivet::socket::BorrowedSocket<'_>| {
                                if trigger.swap(false, Ordering::AcqRel) {
                                    cancel.configure(socket)?;
                                }
                                Ok(())
                            },
                        )),
                        ..SocketOptions::default()
                    },
                )
                .unwrap();
                let receiving = udp.clone();
                register(
                    async move {
                        let _ = receiving.recv().await;
                    },
                    &stats,
                    0,
                    false,
                );
                let sender = std::net::UdpSocket::bind(address(false)).unwrap();
                sender
                    .send_to(b"queued during accept", udp.local_addr())
                    .unwrap();
                armed.store(true, Ordering::Release);
                let mut peer = std::net::TcpStream::connect(listener.local_addr()).unwrap();
                peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let stream = listener.accept().await.unwrap();
                assert!(!armed.load(Ordering::Acquire));
                assert_eq!(stats.drops.load(Ordering::Acquire), 1);
                assert_eq!(stats.maximum_depth.load(Ordering::Acquire), 1);
                let received = udp.recv().await.unwrap();
                assert_eq!(received.data.as_slice(), b"queued during accept");
                assert_eq!(received.peer, Some(sender.local_addr().unwrap()));
                stream.send_all(payload(b"accepted")).await.result.unwrap();
                let mut bytes = [0; 8];
                peer.read_exact(&mut bytes).unwrap();
                assert_eq!(&bytes, b"accepted");
            })
            .await
            .unwrap();
        });
    }

    #[cfg(windows)]
    #[test]
    fn deferred_hook_drop_wakes_an_idle_native_poll() {
        const CHILD: &str = "RIVET_DEFERRED_HOOK_DROP_CHILD";
        if std::env::var_os(CHILD).is_none() {
            use std::process::{Child, Command};
            struct Guard(Child);
            impl Drop for Guard {
                fn drop(&mut self) {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }
            let mut child = Guard(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "hook_waker_destruction::deferred_hook_drop_wakes_an_idle_native_poll",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env(CHILD, "1")
                    .spawn()
                    .unwrap(),
            );
            // An external watchdog cannot supply an accidental timer/root wake.
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "peerless hook child failed: {status}");
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "deferred destructor stranded in native poll without a peer"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }

        #[derive(Default)]
        struct State {
            armed: AtomicBool,
            inline: AtomicBool,
            in_hook: AtomicBool,
            cancelled: AtomicBool,
            dropped: AtomicBool,
        }
        struct DropRoot {
            state: Arc<State>,
            root: Waker,
        }
        impl Wake for DropRoot {
            fn wake(self: Arc<Self>) {
                panic!("no peer can complete the victim accept");
            }
        }
        impl Drop for DropRoot {
            fn drop(&mut self) {
                assert!(!self.state.in_hook.load(Ordering::Acquire));
                assert!(self.state.cancelled.load(Ordering::Acquire));
                runtime::resource_snapshot().unwrap();
                self.state.dropped.store(true, Ordering::Release);
                self.root.wake_by_ref();
            }
        }
        struct ArmAccept(Arc<State>);
        impl Wake for ArmAccept {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                if self.0.inline.swap(true, Ordering::AcqRel) {
                    return;
                }
                let mut waiters = WAITERS.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
                assert!(
                    waiters[0]
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                WAITERS.with(|slot| *slot.borrow_mut() = waiters);
            }
        }

        let mut limits = config(1);
        limits.idle_spin = Duration::ZERO;
        limits.limits.max_pending_accepts = 1;
        limits.limits.completion_budget = 64;
        let mut owner = Runtime::new(limits).unwrap();
        owner.block_on(async {
            let state = Arc::new(State::default());
            let victim = Rc::new(TcpListener::bind(address(false)).unwrap());
            let hook_state = state.clone();
            let late = Rc::new(TcpListener::bind_with_options(address(false), SocketOptions {
                hook: Some(Arc::new(move |_: rivet::socket::BorrowedSocket<'_>| {
                    if hook_state.armed.swap(false, Ordering::AcqRel) {
                        assert!(hook_state.inline.load(Ordering::Acquire));
                        hook_state.in_hook.store(true, Ordering::Release);
                        assert!(cancel_registered_io());
                        hook_state.cancelled.store(true, Ordering::Release);
                        assert!(!hook_state.dropped.load(Ordering::Acquire));
                        hook_state.in_hook.store(false, Ordering::Release);
                        eprintln!("accept hook retired its waiter; root awaits only its destructor");
                    }
                    Ok(())
                })),
                ..SocketOptions::default()
            }).unwrap());
            state.armed.store(true, Ordering::Release);

            // Fail child preparation, not bind: the nonblocking driver pass
            // publishes an accept error without any connection or payload.
            let first = AtomicBool::new(true);
            let trigger = TcpListener::bind_with_options(address(false), SocketOptions {
                hook: Some(Arc::new(move |_: rivet::socket::BorrowedSocket<'_>| {
                    if first.swap(false, Ordering::AcqRel) {
                        Ok(())
                    } else {
                        Err(io::Error::from(io::ErrorKind::PermissionDenied))
                    }
                })),
                ..SocketOptions::default()
            }).unwrap();
            let mut trigger_accept = trigger.accept();
            let mut started = false;
            std::future::poll_fn(|cx| {
                if !started {
                    started = true;
                    let listening = victim.clone();
                    let mut accept = Box::pin(async move { let _ = listening.accept().await; });
                    let waker = Waker::from(Arc::new(DropRoot {
                        state: state.clone(),
                        root: cx.waker().clone(),
                    }));
                    assert!(accept.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
                    CANCELLED_IO.with(|slot| *slot.borrow_mut() = Some(accept));
                    drop(waker);
                    let listening = late.clone();
                    WAITERS.with(|slot| slot.borrow_mut().push(Box::pin(async move {
                        let _ = listening.accept().await;
                    })));
                    // Inline wake registers the next accept after the
                    // nonblocking pass; it deliberately never wakes root.
                    let waker = Waker::from(Arc::new(ArmAccept(state.clone())));
                    assert!(Pin::new(&mut trigger_accept).poll(&mut Context::from_waker(&waker)).is_pending());
                }
                if state.dropped.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }).await;
            assert!(state.inline.load(Ordering::Acquire));
            assert!(state.cancelled.load(Ordering::Acquire));
            assert_eq!(trigger_accept.await.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
            let mut replacement = victim.accept();
            assert!(Pin::new(&mut replacement).poll(&mut Context::from_waker(Waker::noop())).is_pending());
            drop(replacement);
            let waiters = WAITERS.with(|slot| std::mem::take(&mut *slot.borrow_mut()));
            drop(waiters);
        });
    }

    fn replace_retired_lane_repeatedly() {
        struct Registration(Arc<AtomicUsize>);
        impl Wake for Registration {
            fn wake(self: Arc<Self>) {
                panic!("a rejected connect registration must not be woken");
            }
        }
        impl Drop for Registration {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }
        let lane = Rc::new(UdpSocket::bind(address(false)).unwrap());
        let target = std::net::TcpListener::bind(address(false)).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        for attempt in 1..=136 {
            let receiving = lane.clone();
            let mut waiter = Box::pin(async move {
                let _ = receiving.recv().await;
            });
            assert!(
                waiter
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            CANCELLED_IO.with(|slot| {
                assert!(slot.borrow().is_none());
                *slot.borrow_mut() = Some(waiter);
            });
            let mut connect = TcpStream::connect_with_options(
                target.local_addr().unwrap(),
                SocketOptions {
                    hook: Some(Arc::new(|_: rivet::socket::BorrowedSocket<'_>| {
                        assert!(cancel_registered_io());
                        Err(io::Error::from(io::ErrorKind::PermissionDenied))
                    })),
                    ..SocketOptions::default()
                },
            );
            let waker = Waker::from(Arc::new(Registration(drops.clone())));
            let Poll::Ready(Err(error)) =
                Pin::new(&mut connect).poll(&mut Context::from_waker(&waker))
            else {
                panic!("hook rejection must finish before leaving nested setup");
            };
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            drop(waker);
            assert_eq!(drops.load(Ordering::Acquire), attempt);
            assert!(!lane.receive_snapshot().unwrap().waiter_registered());
        }
    }

    #[test]
    fn reentry_can_replace_retired_sources_without_accumulating_callback_history() {
        let limits = config(1);
        #[cfg(target_os = "linux")]
        let limits = limits.with_policy(rivet::Optimization::DirectDescriptors, rivet::Policy::Off);
        let mut owner = Runtime::new(limits).unwrap();
        owner.block_on(async {
            time::timeout(Duration::from_secs(10), async {
                let sockets: [Rc<UdpSocket>; 2] =
                    std::array::from_fn(|_| Rc::new(UdpSocket::bind(address(false)).unwrap()));
                let stats = Stats::new(3);
                for (index, socket) in sockets.iter().enumerate() {
                    let receiving = socket.clone();
                    register(
                        async move {
                            let _ = receiving.recv().await;
                        },
                        &stats,
                        0,
                        index == 0,
                    );
                }
                drop(TcpListener::bind_with_options(address(false), options(&stats, 0)).unwrap());
                assert_eq!(stats.drops.load(Ordering::Acquire), 2);
                assert_eq!(stats.maximum_depth.load(Ordering::Acquire), 1);
                let sender = std::net::UdpSocket::bind(address(false)).unwrap();
                for socket in sockets {
                    sender.send_to(b"reused", socket.local_addr()).unwrap();
                    let received = socket.recv().await.unwrap();
                    assert_eq!(received.data.as_slice(), b"reused");
                    assert_eq!(received.peer, Some(sender.local_addr().unwrap()));
                }
            })
            .await
            .unwrap();
        });
    }
}
