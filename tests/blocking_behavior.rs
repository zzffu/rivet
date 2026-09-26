use futures_lite::future::{block_on, poll_once};
use rivet::{
    BlockingConfig, Runtime, RuntimeConfig,
    runtime::{self, BlockingSpawnError, JoinError},
};
use std::{
    cell::RefCell,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(10);

fn config(threads: usize, queue_capacity: usize) -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.blocking = BlockingConfig {
        threads,
        queue_capacity,
    };
    config.limits.max_tasks = 16;
    config.limits.max_sockets = 16;
    config.limits.max_operations = 64;
    config.limits.pool.bytes = 1024 * 1024;
    config.limits.pool.max_leases = 64;
    config
}

struct PanicOnDrop(Arc<AtomicBool>);
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        panic!("intentional blocking capture/result drop panic");
    }
}

struct ThreadExit(Arc<AtomicBool>);
impl Drop for ThreadExit {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
thread_local! {
    #[cfg_attr(
        target_os = "android",
        allow(
            clippy::missing_const_for_thread_local,
            reason = "Already const; Android std TLS false positive (rust-lang/rust-clippy#13422)."
        )
    )]
    static EXIT: RefCell<Option<ThreadExit>> = const { RefCell::new(None) };
}

#[test]
fn blocking_work_progresses_without_an_active_async_worker() {
    assert!(matches!(
        runtime::spawn_blocking(|| 1),
        Err(BlockingSpawnError::NotRunning)
    ));
    let mut runtime = Runtime::new(config(1, 2)).unwrap();
    let owner = thread::current().id();
    let external = runtime
        .handle()
        .spawn_blocking(move || {
            assert_ne!(thread::current().id(), owner);
            (1_u64..=100).sum::<u64>()
        })
        .unwrap();
    assert_eq!(block_on(external), Ok(5050));
    assert_eq!(
        runtime.block_on(async {
            runtime::spawn_blocking(|| vec![3, 5, 8])
                .unwrap()
                .await
                .unwrap()
        }),
        vec![3, 5, 8]
    );
}

#[test]
fn full_queue_rejects_and_queued_cancellation_immediately_reuses_its_slot() {
    let runtime = Runtime::new(config(1, 1)).unwrap();
    let handle = runtime.handle();
    let (started, receive_started) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let running = handle
        .spawn_blocking(move || {
            started.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
            41
        })
        .unwrap();
    receive_started.recv_timeout(WAIT).unwrap();
    let executed = Arc::new(AtomicBool::new(false));
    let queued_executed = executed.clone();
    let queued = handle
        .spawn_blocking(move || queued_executed.store(true, Ordering::Release))
        .unwrap();
    let rejected_dropped = Arc::new(AtomicBool::new(false));
    let rejected_probe = PanicOnDrop(rejected_dropped.clone());
    assert!(matches!(
        handle.spawn_blocking(move || drop(rejected_probe)),
        Err(BlockingSpawnError::AtCapacity)
    ));
    assert!(rejected_dropped.load(Ordering::Acquire));
    assert_eq!(block_on(queued.cancel()), Err(JoinError::Cancelled));
    assert!(!executed.load(Ordering::Acquire));
    let abandoned_dropped = Arc::new(AtomicBool::new(false));
    let abandoned_probe = PanicOnDrop(abandoned_dropped.clone());
    let abandoned = handle
        .spawn_blocking(move || drop(abandoned_probe))
        .unwrap();
    drop(abandoned);
    assert!(abandoned_dropped.load(Ordering::Acquire));
    let replacement = handle.spawn_blocking(|| 43).unwrap();
    release.send(()).unwrap();
    assert_eq!(block_on(running), Ok(41));
    assert_eq!(block_on(replacement), Ok(43));
}

#[test]
fn aborting_running_work_waits_for_its_result_and_keeps_capacity_occupied() {
    let runtime = Runtime::new(config(1, 1)).unwrap();
    let handle = runtime.handle();
    let (started, receive_started) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let running = handle
        .spawn_blocking(move || {
            started.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
            47
        })
        .unwrap();
    receive_started.recv_timeout(WAIT).unwrap();
    let mut cancellation = Box::pin(running.cancel());
    assert_eq!(block_on(poll_once(&mut cancellation)), None);
    let mut queued = handle.spawn_blocking(|| 53).unwrap();
    assert_eq!(block_on(poll_once(&mut queued)), None);
    assert!(matches!(
        handle.spawn_blocking(|| 59),
        Err(BlockingSpawnError::AtCapacity)
    ));
    release.send(()).unwrap();
    assert_eq!(block_on(cancellation), Ok(47));
    assert_eq!(block_on(queued), Ok(53));
}

#[test]
fn dropping_running_waiters_cannot_start_more_than_the_thread_limit() {
    let runtime = Runtime::new(config(2, 1)).unwrap();
    let handle = runtime.handle();
    let (started, receive_started) = mpsc::sync_channel(2);
    let mut releases = Vec::new();
    let mut workers = Vec::new();
    for _ in 0..2 {
        let (release, wait_release) = mpsc::sync_channel(1);
        let started = started.clone();
        let running = handle
            .spawn_blocking(move || {
                started.send(thread::current().id()).unwrap();
                wait_release.recv_timeout(WAIT).unwrap();
            })
            .unwrap();
        workers.push(receive_started.recv_timeout(WAIT).unwrap());
        releases.push(release);
        drop(running);
    }
    assert_ne!(workers[0], workers[1]);
    let mut queued = handle.spawn_blocking(|| thread::current().id()).unwrap();
    assert_eq!(block_on(poll_once(&mut queued)), None);
    assert!(matches!(
        handle.spawn_blocking(|| ()),
        Err(BlockingSpawnError::AtCapacity)
    ));
    for release in releases {
        release.send(()).unwrap();
    }
    assert!(workers.contains(&block_on(queued).unwrap()));
}

#[test]
fn closure_panics_and_abandoned_result_drop_panics_do_not_kill_the_worker() {
    let runtime = Runtime::new(config(1, 1)).unwrap();
    let handle = runtime.handle();
    let (started, receive_started) = mpsc::sync_channel(1);
    let panicking = handle
        .spawn_blocking(move || {
            started.send(thread::current().id()).unwrap();
            panic!("intentional blocking closure panic");
        })
        .unwrap();
    let worker = receive_started.recv_timeout(WAIT).unwrap();
    assert_eq!(block_on(panicking), Err(JoinError::Panicked));

    let dropped = Arc::new(AtomicBool::new(false));
    let result = PanicOnDrop(dropped.clone());
    let (started, receive_started) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let abandoned = handle
        .spawn_blocking(move || {
            started.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
            result
        })
        .unwrap();
    receive_started.recv_timeout(WAIT).unwrap();
    drop(abandoned);
    let survivor = handle.spawn_blocking(|| thread::current().id()).unwrap();
    release.send(()).unwrap();
    assert_eq!(block_on(survivor), Ok(worker));
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn dropping_a_completed_result_isolates_its_destructor_panic() {
    let runtime = Runtime::new(config(1, 2)).unwrap();
    let handle = runtime.handle();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = PanicOnDrop(dropped.clone());
    let result = handle.spawn_blocking(move || probe).unwrap();
    // FIFO execution on one thread puts the result in its receiver before this
    // marker completes, without polling (and thereby taking) the result.
    let marker = handle.spawn_blocking(|| 61).unwrap();
    assert_eq!(block_on(marker), Ok(61));
    assert!(result.is_finished());
    drop(result);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(block_on(handle.spawn_blocking(|| 67).unwrap()), Ok(67));
}

#[test]
fn abandoned_result_cleanup_keeps_its_thread_busy_until_destruction_finishes() {
    struct BlockingDrop {
        started: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            self.started.send(()).unwrap();
            self.release.recv().unwrap();
        }
    }
    let runtime = Runtime::new(config(2, 1)).unwrap();
    let handle = runtime.handle();
    let (started, receive_started) = mpsc::sync_channel(1);
    let (release_body, wait_body) = mpsc::sync_channel(1);
    let (drop_started, receive_drop_started) = mpsc::sync_channel(1);
    let (release_drop, wait_drop) = mpsc::sync_channel(1);
    let result = BlockingDrop {
        started: drop_started,
        release: wait_drop,
    };
    let first = handle
        .spawn_blocking(move || {
            started.send(()).unwrap();
            wait_body.recv_timeout(WAIT).unwrap();
            result
        })
        .unwrap();
    receive_started.recv_timeout(WAIT).unwrap();
    drop(first);
    release_body.send(()).unwrap();
    receive_drop_started.recv_timeout(WAIT).unwrap();
    let (completed, receive_completed) = mpsc::sync_channel(1);
    let second = handle
        .spawn_blocking(move || {
            completed.send(89).unwrap();
            89
        })
        .unwrap();
    // The first thread is still in user cleanup. The second submission must
    // start the remaining allowed thread, not mistake cleanup for an idle one.
    let completion = receive_completed.recv_timeout(WAIT);
    release_drop.send(()).unwrap();
    assert_eq!(completion.unwrap(), 89);
    assert_eq!(block_on(second), Ok(89));
}

#[test]
fn shutdown_cancels_all_queued_work_and_joins_detached_threads_with_live_handles() {
    let runtime = Runtime::new(config(1, 2)).unwrap();
    let handle = runtime.handle();
    let exited = Arc::new(AtomicBool::new(false));
    let exited_on_thread = exited.clone();
    let (started, receive_started) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let running = handle
        .spawn_blocking(move || {
            EXIT.with(|slot| *slot.borrow_mut() = Some(ThreadExit(exited_on_thread)));
            started.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
        })
        .unwrap();
    receive_started.recv_timeout(WAIT).unwrap();
    running.detach();
    let capture_dropped = Arc::new(AtomicBool::new(false));
    let probe = PanicOnDrop(capture_dropped.clone());
    let first = handle.spawn_blocking(move || drop(probe)).unwrap();
    let queued_executed = Arc::new(AtomicBool::new(false));
    let executed = queued_executed.clone();
    let second = handle
        .spawn_blocking(move || executed.store(true, Ordering::Release))
        .unwrap();
    let external_handle = handle.clone();
    let observer = thread::spawn(move || {
        assert_eq!(block_on(second), Err(JoinError::Cancelled));
        let rejection = external_handle.spawn_blocking(|| 71);
        // Always release the running closure before checking the rejection, so
        // a broken implementation produces a failure rather than a stuck Drop.
        release.send(()).unwrap();
        assert!(matches!(rejection, Err(BlockingSpawnError::ShuttingDown)));
    });
    drop(runtime);
    observer.join().unwrap();
    assert!(exited.load(Ordering::Acquire));
    assert!(capture_dropped.load(Ordering::Acquire));
    assert!(!queued_executed.load(Ordering::Acquire));
    assert_eq!(block_on(first), Err(JoinError::Cancelled));
    assert!(matches!(
        handle.spawn_blocking(|| 73),
        Err(BlockingSpawnError::ShuttingDown)
    ));
}

#[test]
fn shutdown_drops_async_tasks_before_waiting_for_running_blocking_work() {
    struct ReleaseOnDrop(mpsc::SyncSender<()>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let mut runtime = Runtime::new(config(1, 1)).unwrap();
    let (release, wait_release) = mpsc::sync_channel(1);
    runtime.block_on(async {
        let release = ReleaseOnDrop(release);
        runtime::spawn_local(async move {
            let _release = release;
            std::future::pending::<()>().await;
        })
        .unwrap()
        .detach();
        runtime::yield_now().await;
    });
    let (started, receive_started) = mpsc::sync_channel(1);
    let running = runtime
        .handle()
        .spawn_blocking(move || {
            started.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
            79
        })
        .unwrap();
    receive_started.recv_timeout(WAIT).unwrap();
    drop(runtime);
    assert_eq!(block_on(running), Ok(79));
}

#[test]
fn invalid_host_capacities_are_rejected_before_native_allocation() {
    let invalid: [fn(&mut RuntimeConfig); 5] = [
        |config| config.blocking.threads = 0,
        |config| config.blocking.threads = usize::MAX,
        |config| config.blocking.queue_capacity = 0,
        |config| config.blocking.queue_capacity = usize::MAX,
        |config| config.max_async_io = 0,
    ];
    for invalidate in invalid {
        let mut config = config(1, 1);
        invalidate(&mut config);
        assert_eq!(
            config.normalized().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    #[cfg(target_pointer_width = "64")]
    {
        let mut config = config(1, 1);
        config.max_async_io = u32::MAX as usize + 1;
        assert_eq!(
            config.normalized().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn destroying_an_idle_runtime_from_another_worker_cleans_and_restores_ownership() {
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let mut idle = Runtime::new(config(1, 1)).unwrap();
    let mut active = Runtime::new(config(1, 1)).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = Dropped(dropped.clone());
    idle.block_on(async {
        runtime::spawn_local(async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        })
        .unwrap()
        .detach();
        runtime::yield_now().await;
    });
    active.block_on(async {
        drop(idle);
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(runtime::spawn_local(async { 83 }).unwrap().await, Ok(83));
    });
}
