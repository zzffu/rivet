use futures_lite::future::block_on;
use parking_lot::Mutex;
use rivet::{
    BufferPool, Runtime, RuntimeConfig, SocketOptions, UdpSocket,
    diagnostics::{ReceiveResources, WorkerResources},
    runtime,
    sync::oneshot,
    time,
};
use std::{
    future::{Future, poll_fn},
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(10);

fn config() -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 16;
    config.limits.max_sockets = 8;
    config.limits.max_operations = 32;
    config.limits.max_pending_receives = 4;
    config.limits.max_pending_accepts = 4;
    config.limits.pool.bytes = 256 * 1024;
    config.limits.pool.block_size = 256;
    config.limits.pool.max_leases = 64;
    config
}

fn local() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

fn udp_options() -> SocketOptions {
    SocketOptions {
        receive_chunk: 256,
        ..SocketOptions::udp()
    }
}

async fn deadline<F: Future>(future: F) -> F::Output {
    time::timeout(WAIT, future)
        .await
        .expect("resource observation scenario timed out")
}

async fn drive_until<T>(state: &'static str, mut observe: impl FnMut() -> Option<T>) -> T {
    let until = Instant::now() + WAIT;
    loop {
        if let Some(result) = observe() {
            return result;
        }
        assert!(Instant::now() < until, "timed out waiting for {state}");
        runtime::yield_now().await;
    }
}

fn unchanged_queries(pool: &BufferPool, socket: &UdpSocket) -> (WorkerResources, ReceiveResources) {
    let usage = pool.usage();
    let worker = runtime::resource_snapshot().unwrap();
    let receive = socket.receive_snapshot().unwrap();
    assert_eq!(*worker.pool(), usage);
    // No yield or I/O here: repetition must not drive native progress, flush
    // publication credits, recycle leases, or attempt another rearm.
    for _ in 0..8 {
        assert_eq!(socket.receive_snapshot().unwrap(), receive);
        assert_eq!(runtime::resource_snapshot().unwrap(), worker);
        assert_eq!(pool.usage(), usage);
    }
    (worker, receive)
}

#[test]
fn snapshots_require_the_current_owner_and_reject_a_destroyed_runtime() {
    assert_eq!(
        runtime::resource_snapshot().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    let mut owner = Runtime::new(config()).unwrap();
    let mut other = Runtime::new(config()).unwrap();
    let backend = owner.capabilities()[0].backend;
    let owner_worker = owner.capabilities()[0].worker;
    let pool = owner.buffer_pool();
    let socket = owner.block_on(async {
        let before = runtime::resource_snapshot().unwrap();
        let socket = UdpSocket::bind_with_options(local(), udp_options()).unwrap();
        let (worker, receive) = unchanged_queries(&pool, &socket);
        assert_eq!(worker.worker(), owner_worker);
        assert_eq!(worker.backend(), backend);
        assert_eq!(receive.worker(), worker.worker());
        assert_eq!(worker.sockets(), before.sockets() + 1);
        assert_eq!(
            worker.available_socket_slots() + 1,
            before.available_socket_slots()
        );
        assert_eq!(receive.queued_results(), 0);
        assert_eq!(receive.queue_available(), receive.queue_capacity());
        assert!(!receive.waiter_registered());
        #[cfg(not(windows))]
        {
            // A query must not create the lazily started receive operation.
            assert!(!receive.active());
            assert_eq!(worker.operations(), before.operations());
            assert!(receive.rio().is_none());
            assert!(worker.driver().rio_receive_queue_slots().is_none());
            assert!(
                worker
                    .driver()
                    .udp_rearm_allocation_failures_total()
                    .is_none()
            );
        }
        #[cfg(target_os = "android")]
        {
            assert!(receive.native_outstanding().is_none());
            assert!(worker.driver().native_outstanding().is_none());
            assert!(worker.driver().retiring_native().is_none());
        }
        socket
    });
    assert_eq!(
        socket.receive_snapshot().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    other.block_on(async {
        let before = runtime::resource_snapshot().unwrap();
        assert_eq!(
            socket.receive_snapshot().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(runtime::resource_snapshot().unwrap(), before);
    });
    owner.block_on(async {
        assert_eq!(socket.receive_snapshot().unwrap().worker(), owner_worker);
    });
    drop(owner);
    assert_eq!(
        socket.receive_snapshot().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    other.block_on(async {
        let before = runtime::resource_snapshot().unwrap();
        assert_eq!(
            socket.receive_snapshot().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(runtime::resource_snapshot().unwrap(), before);
    });
    assert_eq!(
        runtime::resource_snapshot().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[test]
fn worker_snapshot_excludes_a_handshaken_background_workers_live_socket() {
    let mut configuration = config();
    configuration.workers = 2;
    let mut runtime = Runtime::new(configuration).unwrap();
    let (started, observed) = mpsc::sync_channel(1);
    let (release, released) = oneshot::channel();
    // Worker zero is inactive until block_on. This factory witnesses exactly
    // one background owner, not an assumed sample of every configured worker.
    let background = runtime
        .handle()
        .spawn(move || async move {
            let socket = UdpSocket::bind_with_options(local(), udp_options()).unwrap();
            let snapshot = runtime::resource_snapshot().unwrap();
            assert_eq!(
                socket.receive_snapshot().unwrap().worker(),
                snapshot.worker()
            );
            started.send(snapshot).unwrap();
            released.await.unwrap();
            drop(socket);
        })
        .unwrap();
    let remote = observed.recv_timeout(WAIT).unwrap();
    runtime.block_on(deadline(async {
        let caller = runtime::resource_snapshot().unwrap();
        assert_ne!(caller.worker(), remote.worker());
        assert_eq!(caller.backend(), remote.backend());
        assert_eq!(remote.sockets(), 1);
        assert_eq!(caller.sockets(), 0);
        assert_eq!(caller.driver().sockets(), 0);
        release.send(()).unwrap();
        background.await.unwrap();
    }));
}

struct ReceiveWake {
    outcome: Mutex<Option<Result<(), io::ErrorKind>>>,
    root: Waker,
}

impl Wake for ReceiveWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let observed = runtime::resource_snapshot()
            .map(|_| ())
            .map_err(|error| error.kind());
        let mut outcome = self.outcome.lock();
        // Do not let a later expected callback hide an earlier unexpected one.
        if outcome.is_none() || observed != Err(io::ErrorKind::WouldBlock) {
            *outcome = Some(observed);
        }
        drop(outcome);
        self.root.wake_by_ref();
    }
}

#[test]
fn receive_waker_reentry_is_nonblocking_and_cancellation_preserves_queued_bytes() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    runtime.block_on(deadline(async {
        let socket = UdpSocket::bind_with_options(local(), udp_options()).unwrap();
        let peer = std::net::UdpSocket::bind(local()).unwrap();
        peer.set_write_timeout(Some(WAIT)).unwrap();
        let mut cancelled = socket.recv();
        let notice = poll_fn(|cx| {
            let notice = Arc::new(ReceiveWake {
                outcome: Mutex::new(None),
                root: cx.waker().clone(),
            });
            let waker = Waker::from(notice.clone());
            assert!(
                Pin::new(&mut cancelled)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            Poll::Ready(notice)
        })
        .await;
        let (_, waiting) = unchanged_queries(&pool, &socket);
        assert!(waiting.active());
        assert!(waiting.waiter_registered());
        assert_eq!(waiting.queued_results(), 0);
        let bytes = b"queued bytes survive a reentrant observer and cancelled waiter";
        assert_eq!(
            peer.send_to(bytes, socket.local_addr()).unwrap(),
            bytes.len()
        );
        poll_fn(|_| {
            if notice.outcome.lock().is_some() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        // This is the real receive Waker invoked while Core/driver mutation is
        // borrowed, not a synthetic borrow or an unrelated executor wake.
        assert_eq!(*notice.outcome.lock(), Some(Err(io::ErrorKind::WouldBlock)));
        let (queued_worker, queued) = unchanged_queries(&pool, &socket);
        assert_eq!(queued.queued_results(), 1);
        assert_eq!(queued_worker.queued_receives(), 1);
        assert!(queued.waiter_registered());
        drop(cancelled);
        let (cancelled_worker, cancelled_state) = unchanged_queries(&pool, &socket);
        assert!(!cancelled_state.waiter_registered());
        assert!(cancelled_state.active());
        assert_eq!(cancelled_state.queued_results(), queued.queued_results());
        assert_eq!(
            cancelled_state.backend_publication_credits(),
            queued.backend_publication_credits()
        );
        assert_eq!(cancelled_state.credits_pending(), queued.credits_pending());
        assert_eq!(cancelled_worker, queued_worker);
        let packet = socket.recv().await.unwrap();
        assert_eq!(packet.data.as_slice(), bytes);
        assert_eq!(packet.peer, Some(peer.local_addr().unwrap()));
        assert!(!packet.truncated);
        let (consumed_worker, consumed) = unchanged_queries(&pool, &socket);
        assert_eq!(consumed.queued_results(), 0);
        assert_eq!(consumed_worker.queued_receives(), 0);
        assert!(!consumed.waiter_registered());
    }));
    assert_eq!(
        runtime::resource_snapshot().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[test]
fn finished_join_result_keeps_its_lease_live_across_runtime_destruction() {
    let mut runtime = Runtime::new(config()).unwrap();
    let pool = runtime.buffer_pool();
    let baseline = pool.usage();
    let bytes = b"finished task result still owns this lease";
    let mut completed = None;
    runtime.block_on(deadline(async {
        let task = runtime::spawn_local(async move {
            let pool = runtime::buffer_pool().unwrap();
            let mut buffer = pool.try_acquire().unwrap();
            buffer.extend_from_slice(bytes).unwrap();
            buffer.freeze()
        })
        .unwrap();
        drive_until("local task result publication", || {
            task.is_finished().then_some(())
        })
        .await;
        let worker = runtime::resource_snapshot().unwrap();
        assert_eq!(worker.operations(), 0);
        assert_eq!(worker.sockets(), 0);
        assert_eq!(pool.usage().leases_in_use(), baseline.leases_in_use() + 1);
        assert_eq!(
            pool.usage().payload_in_use(),
            baseline.payload_in_use() + 256
        );
        assert_eq!(*worker.pool(), pool.usage());
        completed = Some(task);
    }));
    let completed = completed.unwrap();
    let retained = pool.usage();
    drop(runtime);
    assert!(completed.is_finished());
    assert_eq!(pool.usage(), retained);
    let data = block_on(completed).unwrap();
    assert_eq!(data.as_slice(), bytes);
    assert_eq!(pool.usage(), retained);
    drop(data);
    assert_eq!(pool.usage(), baseline);
}

#[cfg(windows)]
#[test]
fn rio_pool_pressure_observes_attempts_recovery_and_native_retirement_without_driving_them() {
    const WINDOW: usize = 2;
    const CHUNK: usize = 256;
    let mut configuration = config();
    configuration.limits.max_pending_receives = WINDOW;
    configuration.limits.pool.bytes = configuration
        .limits
        .windows_udp_receive_bytes(CHUNK)
        .unwrap();
    configuration.limits.pool.max_leases = WINDOW;
    let mut runtime = Runtime::new(configuration).unwrap();
    let pool = runtime.buffer_pool();
    let empty = pool.usage();
    let held = runtime.block_on(deadline(async {
        let before = runtime::resource_snapshot().unwrap();
        let socket = UdpSocket::bind_with_options(local(), udp_options()).unwrap();
        let (admitted, receive) = unchanged_queries(&pool, &socket);
        let rio = receive.rio().unwrap();
        assert_eq!(admitted.sockets(), before.sockets() + 1);
        assert_eq!(admitted.operations(), before.operations() + 1);
        assert_eq!(admitted.driver().operations(), WINDOW);
        assert_eq!(
            admitted.driver().rio_receive_queue_slots(),
            Some(WINDOW - 1)
        );
        assert_eq!(receive.native_outstanding(), Some(WINDOW));
        assert_eq!(rio.admitted_lanes(), WINDOW);
        assert_eq!(rio.ready_results(), 0);
        assert_eq!(rio.idle_lanes(), 0);
        assert_eq!(rio.last_pool_blocked_lanes(), 0);
        assert_eq!(rio.rearm_allocation_failures_total(), 0);
        assert!(!rio.commit_pending());
        assert!(!rio.stopping());
        assert!(receive.active());
        assert!(!receive.waiter_registered());
        assert_eq!(receive.backend_publication_credits(), WINDOW);
        assert_eq!(pool.usage().payload_in_use(), WINDOW * CHUNK);
        assert_eq!(pool.usage().leases_in_use(), WINDOW);
        assert_eq!(pool.usage().payload_available(), 0);
        assert_eq!(pool.usage().leases_available(), 0);
        assert_eq!(pool.usage().largest_free_extent(), 0);

        let peer = std::net::UdpSocket::bind(local()).unwrap();
        peer.set_write_timeout(Some(WAIT)).unwrap();
        for byte in [0x31, 0x52] {
            assert_eq!(
                peer.send_to(&[byte; CHUNK], socket.local_addr()).unwrap(),
                CHUNK
            );
        }
        let first = socket.recv().await.unwrap();
        let held = socket.recv().await.unwrap();
        assert_eq!(first.data.as_slice(), &[0x31; CHUNK]);
        assert_eq!(held.data.as_slice(), &[0x52; CHUNK]);
        assert_eq!(first.peer, Some(peer.local_addr().unwrap()));
        assert_eq!(held.peer, first.peer);
        assert!(!first.truncated && !held.truncated);
        drive_until(
            "every retained UDP lane to encounter actual pool pressure",
            || {
                let state = socket.receive_snapshot().unwrap();
                let rio = state.rio().unwrap();
                (state.native_outstanding() == Some(0)
                    && state.queued_results() == 0
                    && rio.idle_lanes() == WINDOW
                    && rio.last_pool_blocked_lanes() == WINDOW
                    && rio.rearm_allocation_failures_total() > 0)
                    .then_some(())
            },
        )
        .await;
        let (blocked_worker, blocked) = unchanged_queries(&pool, &socket);
        let failures = blocked.rio().unwrap().rearm_allocation_failures_total();
        assert_eq!(
            blocked_worker
                .driver()
                .udp_rearm_allocation_failures_total(),
            Some(failures)
        );
        assert_eq!(blocked_worker.driver().native_outstanding(), Some(0));
        assert_eq!(blocked_worker.driver().retiring_native(), Some(0));
        assert_eq!(blocked.rio().unwrap().ready_results(), 0);
        assert_eq!(blocked.backend_publication_credits(), WINDOW);
        assert_eq!(pool.usage().payload_available(), 0);

        drop(first);
        // The lane still has its immutable reserve. Releasing its user alias
        // makes reuse possible, but neither release nor observation posts I/O.
        let (released_worker, released) = unchanged_queries(&pool, &socket);
        assert_eq!(released_worker, blocked_worker);
        assert_eq!(released, blocked);
        drive_until(
            "released UDP lease to be rearmed by driver progress",
            || {
                let state = socket.receive_snapshot().unwrap();
                let rio = state.rio().unwrap();
                (state.native_outstanding() == Some(1)
                    && rio.idle_lanes() == WINDOW - 1
                    && rio.last_pool_blocked_lanes() == WINDOW - 1
                    && !rio.commit_pending())
                .then_some(())
            },
        )
        .await;
        let (recovered_worker, recovered) = unchanged_queries(&pool, &socket);
        let recovered_failures = recovered.rio().unwrap().rearm_allocation_failures_total();
        assert!(recovered_failures >= failures);
        assert_eq!(recovered_worker.driver().native_outstanding(), Some(1));
        assert_eq!(pool.usage().payload_in_use(), WINDOW * CHUNK);
        assert_eq!(held.data.as_slice(), &[0x52; CHUNK]);

        let marker = b"receive resumed on the released lane";
        assert_eq!(
            peer.send_to(marker, socket.local_addr()).unwrap(),
            marker.len()
        );
        let resumed = socket.recv().await.unwrap();
        assert_eq!(resumed.data.as_slice(), marker);
        assert_eq!(resumed.peer, held.peer);
        assert!(!resumed.truncated);
        assert_eq!(held.data.as_slice(), &[0x52; CHUNK]);
        drop(resumed);
        // Leave one known native receive outstanding for the close barrier,
        // rather than assuming a number of turns rearmed it after this packet.
        let receptive = drive_until("reused lane to become receptive before close", || {
            let state = socket.receive_snapshot().unwrap();
            (state.native_outstanding() == Some(1) && !state.rio().unwrap().commit_pending())
                .then_some(state)
        })
        .await;
        let retiring_failures = receptive.rio().unwrap().rearm_allocation_failures_total();
        assert!(retiring_failures >= recovered_failures);

        drop(socket);
        let retiring = runtime::resource_snapshot().unwrap();
        assert_eq!(retiring.sockets(), 0);
        assert_eq!(
            retiring.available_socket_slots(),
            before.available_socket_slots()
        );
        assert_eq!(retiring.queued_receives(), 0);
        assert_eq!(retiring.operations(), 1);
        assert_eq!(retiring.driver().sockets(), 1);
        assert_eq!(retiring.driver().closing_sockets(), 1);
        assert_eq!(retiring.driver().operations(), WINDOW);
        assert_eq!(retiring.driver().native_outstanding(), Some(1));
        assert_eq!(retiring.driver().retiring_native(), Some(1));
        assert_eq!(
            retiring.driver().rio_receive_queue_slots(),
            Some(WINDOW - 1)
        );
        for _ in 0..8 {
            assert_eq!(runtime::resource_snapshot().unwrap(), retiring);
            assert_eq!(pool.usage(), *retiring.pool());
        }
        let drained = drive_until(
            "closed UDP native records and Core operation to retire",
            || {
                let state = runtime::resource_snapshot().unwrap();
                let driver = state.driver();
                (state.operations() == 0
                    && driver.operations() == 0
                    && driver.sockets() == 0
                    && driver.closing_sockets() == 0
                    && driver.pending_completions() == 0
                    && driver.native_outstanding() == Some(0)
                    && driver.retiring_native() == Some(0))
                .then_some(state)
            },
        )
        .await;
        assert_eq!(drained.sockets(), 0);
        assert_eq!(drained.driver().rio_receive_queue_slots(), Some(0));
        assert_eq!(
            drained.driver().udp_rearm_allocation_failures_total(),
            Some(retiring_failures)
        );
        assert_eq!(drained.pool().payload_in_use(), CHUNK);
        assert_eq!(drained.pool().leases_in_use(), 1);
        assert_eq!(drained.pool().payload_available(), CHUNK);
        assert_eq!(held.data.as_slice(), &[0x52; CHUNK]);
        held.data
    }));
    let retained = pool.usage();
    drop(runtime);
    assert_eq!(pool.usage(), retained);
    assert_eq!(held.as_slice(), &[0x52; CHUNK]);
    drop(held);
    assert_eq!(pool.usage(), empty);
}
