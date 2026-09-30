use futures_lite::future::poll_once;
use rivet::{
    Runtime, RuntimeConfig, TcpListener, TcpStream,
    net::ServeConfig,
    runtime::{self, JoinError, SpawnError, TaskGroup},
    sync::{CancellationToken, mpsc, oneshot},
    time,
};
use std::{
    cell::Cell,
    future::{Future, pending},
    io,
    pin::{Pin, pin},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc as blocking_channel,
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

fn config(workers: usize) -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.workers = workers;
    config.limits.max_tasks = 32;
    config.limits.max_sockets = 32;
    config.limits.max_operations = 128;
    config.limits.max_pending_accepts = 2;
    config.limits.max_pending_receives = 2;
    config.limits.pool.bytes = 1024 * 1024;
    config.limits.pool.block_size = 4096;
    config.limits.pool.max_leases = 128;
    config
}

async fn deadline<F: Future>(future: F) -> F::Output {
    time::timeout(Duration::from_secs(5), future)
        .await
        .expect("task supervision timed out")
}

#[test]
fn bounded_local_group_yields_completion_order_and_reuses_joined_slots() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(deadline(async {
        assert!(TaskGroup::<()>::new(0).is_err());
        let mut group = TaskGroup::new(2).unwrap();
        // A completed empty wait does not close admission.
        assert!(matches!(poll_once(group.join_next()).await, Some(None)));
        let local = Rc::new(Cell::new(0));
        let value = local.clone();
        let (release, released) = oneshot::channel();
        group
            .spawn_local(async move {
                released.await.unwrap();
                value.set(19);
                value
            })
            .unwrap();
        group.spawn_local(async { Rc::new(Cell::new(7)) }).unwrap();
        runtime::yield_now().await;
        assert_eq!(
            group
                .spawn_local(async { Rc::new(Cell::new(99)) })
                .unwrap_err(),
            SpawnError::AtCapacity
        );
        assert_eq!(group.join_next().await.unwrap().unwrap().get(), 7);
        assert_eq!(local.get(), 0);
        assert!(poll_once(group.join_next()).await.is_none());
        release.send(()).unwrap();
        assert!(Rc::ptr_eq(
            &group.join_next().await.unwrap().unwrap(),
            &local
        ));
        assert_eq!(local.get(), 19);
        assert!(matches!(poll_once(group.join_next()).await, Some(None)));
        group.spawn_local(async { Rc::new(Cell::new(23)) }).unwrap();
        assert_eq!(group.join_next().await.unwrap().unwrap().get(), 23);
        assert!(group.join_next().await.is_none());
    }));
}

struct CleanupBarrier {
    entered: blocking_channel::SyncSender<thread::ThreadId>,
    release: blocking_channel::Receiver<()>,
    local: Rc<()>,
}
impl Future for CleanupBarrier {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        assert_eq!(Rc::strong_count(&self.local), 1);
        Poll::Ready(41)
    }
}
impl Drop for CleanupBarrier {
    fn drop(&mut self) {
        self.entered.send(thread::current().id()).unwrap();
        self.release.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}

struct PanicOnDrop;
impl Future for PanicOnDrop {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        Poll::Ready(9)
    }
}
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("observable future destructor panic");
    }
}

#[test]
fn cross_worker_join_waits_for_cleanup_and_cancelled_shutdown_keeps_failures() {
    let owner = thread::current().id();
    let mut runtime = Runtime::new(config(2)).unwrap();
    let mut group = TaskGroup::new(2).unwrap();
    let (entered, receive_entered) = blocking_channel::sync_channel(1);
    let (release, released) = blocking_channel::sync_channel(1);
    let control = group
        .spawn_on(&runtime.handle(), move || CleanupBarrier {
            entered,
            release: released,
            local: Rc::new(()),
        })
        .unwrap();
    assert_ne!(
        receive_entered
            .recv_timeout(Duration::from_secs(5))
            .unwrap(),
        owner
    );
    let unpublished = !control.is_finished();
    let no_early_join = futures_lite::future::block_on(poll_once(group.join_next())).is_none();
    runtime.block_on(deadline(async {
        group.spawn_local(PanicOnDrop).unwrap();
        runtime::yield_now().await;
        {
            let mut shutdown = pin!(group.shutdown());
            assert!(poll_once(shutdown.as_mut()).await.is_none());
        }
        assert_eq!(
            group.spawn_local(async { 1 }).unwrap_err(),
            SpawnError::ShuttingDown
        );
        release.send(()).unwrap();
        assert_eq!(group.shutdown().await, Err(JoinError::Panicked));
        assert!(group.is_empty());
        assert!(control.is_finished());
    }));
    assert!(
        unpublished,
        "is_finished became true during child destruction"
    );
    assert!(no_early_join, "join returned before child cleanup finished");
}

#[test]
fn shutdown_drains_results_when_a_panic_payload_destructor_also_panics() {
    struct PanicPayload;
    impl Drop for PanicPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }
    struct Output {
        drops: Rc<Cell<usize>>,
        panics: bool,
    }
    impl Drop for Output {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if self.panics {
                std::panic::panic_any(PanicPayload);
            }
        }
    }

    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(deadline(async {
        let drops = Rc::new(Cell::new(0));
        let mut group = TaskGroup::new(2).unwrap();
        let mut controls = Vec::new();
        for panics in [true, false] {
            let drops = drops.clone();
            controls.push(
                group
                    .spawn_local(async move { Output { drops, panics } })
                    .unwrap(),
            );
        }
        while !controls.iter().all(|control| control.is_finished()) {
            runtime::yield_now().await;
        }
        assert_eq!(group.shutdown().await, Err(JoinError::Panicked));
        assert!(group.is_empty());
        assert_eq!(drops.get(), 2);
        assert_eq!(group.shutdown().await, Err(JoinError::Panicked));
        assert_eq!(drops.get(), 2);
    }));
}

struct LocalDrop {
    drops: Rc<Cell<usize>>,
    notify: Option<oneshot::Sender<()>>,
}
impl Drop for LocalDrop {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
        if let Some(notify) = self.notify.take() {
            let _ = notify.send(());
        }
    }
}

#[test]
fn cloned_abort_controls_and_group_drop_cancel_on_the_owner_thread() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(deadline(async {
        let drops = Rc::new(Cell::new(0));
        let mut group = TaskGroup::new(1).unwrap();
        let guard = LocalDrop {
            drops: drops.clone(),
            notify: None,
        };
        let control = group
            .spawn_local(async move {
                let _guard = guard;
                pending::<()>().await;
            })
            .unwrap();
        let cloned = control.clone();
        drop(control);
        cloned.abort();
        assert_eq!(group.join_next().await, Some(Err(JoinError::Cancelled)));
        assert_eq!(drops.get(), 1);
        assert!(cloned.is_finished());
        assert!(matches!(poll_once(group.join_next()).await, Some(None)));

        let (notify, notified) = oneshot::channel();
        let guard = LocalDrop {
            drops: drops.clone(),
            notify: Some(notify),
        };
        let dropped = group
            .spawn_local(async move {
                let _guard = guard;
                pending::<()>().await;
            })
            .unwrap();
        drop(group);
        notified.await.unwrap();
        assert_eq!(drops.get(), 2);
        assert!(dropped.is_finished());
    }));
}

#[test]
fn aborting_unstarted_factories_destroys_captures_without_launching() {
    struct FactoryGuard(Arc<AtomicUsize>);
    impl Drop for FactoryGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(deadline(async {
        let destroyed = Arc::new(AtomicUsize::new(0));
        let launched = Arc::new(AtomicUsize::new(0));
        let mut group = TaskGroup::new(3).unwrap();
        for _ in 0..3 {
            let guard = FactoryGuard(destroyed.clone());
            let launched = launched.clone();
            group
                .spawn(move || {
                    launched.fetch_add(1, Ordering::AcqRel);
                    async move { drop(guard) }
                })
                .unwrap();
        }
        {
            let mut shutdown = pin!(group.shutdown());
            assert!(poll_once(shutdown.as_mut()).await.is_none());
        }
        assert_eq!(group.len(), 3);
        assert_eq!(group.shutdown().await, Ok(()));
        assert_eq!(launched.load(Ordering::Acquire), 0);
        assert_eq!(destroyed.load(Ordering::Acquire), 3);
    }));
}

struct HandlerDrop(Arc<AtomicUsize>);
impl Drop for HandlerDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn serve_limits_admission_then_cooperates_and_aborts_after_grace() {
    let mut runtime = Runtime::new(config(2)).unwrap();
    runtime.block_on(deadline(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let stop = CancellationToken::new();
        let service_stop = stop.clone();
        let entered = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let cooperating = Arc::new(AtomicUsize::new(0));
        let starts = entered.clone();
        let drops = dropped.clone();
        let cooperated = cooperating.clone();
        let (ready, started) = mpsc::bounded(2);
        let service = runtime::spawn_local(async move {
            listener
                .serve_until(
                    ServeConfig {
                        max_connections: 2,
                        shutdown_grace: Duration::from_millis(10),
                    },
                    service_stop.cancelled(),
                    move |stream, cancellation| {
                        let ready = ready.clone();
                        let index = starts.fetch_add(1, Ordering::AcqRel);
                        let guard = HandlerDrop(drops.clone());
                        let cooperated = cooperated.clone();
                        async move {
                            let (_stream, _guard, local) = (stream, guard, Rc::new(17));
                            ready.send(()).await.unwrap();
                            if index == 0 {
                                cancellation.cancelled().await;
                                cooperated.fetch_add(*local, Ordering::AcqRel);
                            } else {
                                pending::<()>().await;
                            }
                        }
                    },
                )
                .await
        })
        .unwrap();
        let first = TcpStream::connect(address).await.unwrap();
        let second = TcpStream::connect(address).await.unwrap();
        started.recv().await.unwrap();
        started.recv().await.unwrap();
        let waiting = TcpStream::connect(address).await.unwrap();
        stop.cancel();
        service.await.unwrap().unwrap();
        assert_eq!(entered.load(Ordering::Acquire), 2);
        assert_eq!(cooperating.load(Ordering::Acquire), 17);
        assert_eq!(dropped.load(Ordering::Acquire), 2);
        drop((first, second, waiting));
    }));
}

#[test]
fn ready_stop_prevents_accepting_an_already_connected_peer() {
    let mut runtime = Runtime::new(config(1)).unwrap();
    runtime.block_on(deadline(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let peer = TcpStream::connect(listener.local_addr()).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        listener
            .serve_until(
                ServeConfig {
                    max_connections: 1,
                    shutdown_grace: Duration::ZERO,
                },
                async {},
                move |_, _| {
                    observed.fetch_add(1, Ordering::AcqRel);
                    async {}
                },
            )
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Acquire), 0);
        drop(peer);
    }));
}

#[test]
fn handler_panic_is_returned_only_after_siblings_are_reclaimed() {
    let mut runtime = Runtime::new(config(2)).unwrap();
    runtime.block_on(deadline(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let dropped = Arc::new(AtomicUsize::new(0));
        let drops = dropped.clone();
        let indices = AtomicUsize::new(0);
        let panic_now = CancellationToken::new();
        let panic_signal = panic_now.clone();
        let (ready, started) = mpsc::bounded(2);
        let service = runtime::spawn_local(async move {
            listener
                .serve_until(
                    ServeConfig {
                        max_connections: 2,
                        shutdown_grace: Duration::ZERO,
                    },
                    pending(),
                    move |stream, _| {
                        let index = indices.fetch_add(1, Ordering::AcqRel);
                        let panic_signal = panic_signal.clone();
                        let ready = ready.clone();
                        let guard = HandlerDrop(drops.clone());
                        async move {
                            let (_stream, _guard) = (stream, guard);
                            ready.send(()).await.unwrap();
                            if index == 0 {
                                panic_signal.cancelled().await;
                                panic!("observable supervised handler panic");
                            }
                            pending::<()>().await;
                        }
                    },
                )
                .await
        })
        .unwrap();
        let first = TcpStream::connect(address).await.unwrap();
        let second = TcpStream::connect(address).await.unwrap();
        started.recv().await.unwrap();
        started.recv().await.unwrap();
        panic_now.cancel();
        let error = service.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(
            error.get_ref().and_then(|e| e.downcast_ref::<JoinError>()),
            Some(&JoinError::Panicked)
        );
        assert_eq!(dropped.load(Ordering::Acquire), 2);
        drop((first, second));
    }));
}

#[test]
fn cancelling_simple_serve_does_not_detach_live_handlers() {
    let mut runtime = Runtime::new(config(2)).unwrap();
    runtime.block_on(deadline(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr();
        let (ready, started) = mpsc::bounded(1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let drops = dropped.clone();
        let service = runtime::spawn_local(async move {
            listener
                .serve(move |stream| {
                    let ready = ready.clone();
                    let guard = HandlerDrop(drops.clone());
                    async move {
                        let (_stream, _guard) = (stream, guard);
                        ready.send(()).await.unwrap();
                        pending::<()>().await;
                    }
                })
                .await
        })
        .unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        started.recv().await.unwrap();
        assert_eq!(service.cancel().await.unwrap_err(), JoinError::Cancelled);
        assert!(client.recv().await.unwrap().is_none());
        assert_eq!(dropped.load(Ordering::Acquire), 1);
    }));
}
