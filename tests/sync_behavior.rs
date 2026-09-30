use futures_lite::future::{block_on, poll_once, zip};
use parking_lot::Mutex;
use rivet::sync::{CancellationToken, Notify, watch};
use std::{
    future::Future,
    pin::{Pin, pin},
    process::{Child, Command},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, RawWaker, RawWakerVTable, Wake, Waker},
    time::{Duration, Instant},
};

#[test]
fn cancellation_broadcasts_and_remains_sticky_without_runtime() {
    block_on(async {
        let token = CancellationToken::new();
        let clone = token.clone();
        let mut first = pin!(token.cancelled());
        let mut second = pin!(clone.cancelled());
        assert!(poll_once(&mut first).await.is_none());
        assert!(poll_once(&mut second).await.is_none());
        let cancellation = token.clone();
        std::thread::spawn(move || cancellation.cancel())
            .join()
            .unwrap();
        zip(first, second).await;
        assert!(token.is_cancelled());
        assert_eq!(poll_once(token.cancelled()).await, Some(()));
    });
}

#[test]
fn notify_coalesces_and_cancelled_wait_does_not_consume_permit() {
    block_on(async {
        let notify = Notify::new();
        {
            let mut abandoned = pin!(notify.notified());
            assert!(poll_once(&mut abandoned).await.is_none());
            notify.notify_one();
        }
        assert_eq!(poll_once(notify.notified()).await, Some(()));
        notify.notify_one();
        notify.notify_one();
        assert_eq!(poll_once(notify.notified()).await, Some(()));
        assert!(poll_once(notify.notified()).await.is_none());
    });
}

#[test]
fn notify_broadcast_wakes_current_waiters_without_storing_a_permit() {
    block_on(async {
        let notify = Notify::new();
        let mut first = pin!(notify.notified());
        let mut second = pin!(notify.notified());
        assert!(poll_once(&mut first).await.is_none());
        assert!(poll_once(&mut second).await.is_none());
        notify.notify_waiters();
        zip(first, second).await;
        assert!(poll_once(notify.notified()).await.is_none());
        notify.notify_one();
        assert_eq!(poll_once(notify.notified()).await, Some(()));
    });
}

#[test]
fn cancelled_broadcast_cannot_notify_a_subsequent_wait() {
    block_on(async {
        let notify = Notify::new();
        let mut later = pin!(notify.notified());
        {
            let mut abandoned = pin!(notify.notified());
            assert!(poll_once(&mut abandoned).await.is_none());
            notify.notify_waiters();
            assert!(poll_once(&mut later).await.is_none());
        }
        assert!(poll_once(&mut later).await.is_none());
        notify.notify_one();
        later.await;
    });
}

#[test]
fn watch_coalesces_and_drains_last_update_after_sender_closes() {
    block_on(async {
        let (sender, mut receiver) = watch::channel(0);
        let mut other = receiver.clone();
        sender.send(1).unwrap();
        sender.send(2).unwrap();
        drop(sender);
        receiver.changed().await.unwrap();
        other.changed().await.unwrap();
        assert_eq!(*receiver.borrow_and_update(), 2);
        assert_eq!(*other.borrow(), 2);
        assert_eq!(receiver.has_changed(), Err(watch::RecvError));
        assert_eq!(other.changed().await, Err(watch::RecvError));
    });
}

#[test]
fn watch_cancelled_wait_retains_unseen_update_and_last_sender_wakes() {
    block_on(async {
        let (sender, mut receiver) = watch::channel(7);
        {
            let mut abandoned = pin!(receiver.changed());
            assert!(poll_once(&mut abandoned).await.is_none());
            sender.send(8).unwrap();
        }
        assert_eq!(receiver.has_changed(), Ok(true));
        receiver.changed().await.unwrap();
        assert_eq!(*receiver.borrow(), 8);
        let mut closed = pin!(receiver.changed());
        assert!(poll_once(&mut closed).await.is_none());
        drop(sender);
        assert_eq!(closed.await, Err(watch::RecvError));
    });
}

#[test]
fn watch_rejected_send_returns_value_and_subscribe_sees_latest_replacement() {
    let (sender, receiver) = watch::channel(String::from("initial"));
    drop(receiver);
    assert_eq!(
        sender.send(String::from("rejected")).unwrap_err().0,
        "rejected"
    );
    assert_eq!(sender.send_replace(String::from("latest")), "initial");
    let receiver = sender.subscribe();
    assert_eq!(&**receiver.borrow(), "latest");
    assert_eq!(receiver.has_changed(), Ok(false));
    sender.send(String::from("next")).unwrap();
    assert_eq!(receiver.has_changed(), Ok(true));
}

type PendingTask = Pin<Box<dyn Future<Output = ()> + Send>>;
type Action = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct WakeAction(Mutex<Option<Action>>);

impl Wake for WakeAction {
    fn wake(self: Arc<Self>) {
        let action = self.0.lock().take();
        if let Some(action) = action {
            action();
        }
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn pending(future: impl Future<Output = ()> + Send + 'static, waker: &Waker) -> PendingTask {
    let mut future: PendingTask = Box::pin(future);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(waker))
            .is_pending()
    );
    future
}

fn cancel_from_wake(case: &str) {
    let action = Arc::new(WakeAction::default());
    let waker = Waker::from(action.clone());
    let tasks = Arc::new(Mutex::new(Vec::<PendingTask>::new()));
    let cancelled = tasks.clone();
    *action.0.lock() = Some(Box::new(move || {
        let old = std::mem::take(&mut *cancelled.lock());
        drop(old);
    }));
    match case {
        "cancel" => {
            let token = CancellationToken::new();
            let waiting = token.clone();
            tasks
                .lock()
                .push(pending(async move { waiting.cancelled().await }, &waker));
            token.cancel();
            assert!(tasks.lock().is_empty());
            assert_eq!(block_on(poll_once(token.cancelled())), Some(()));
        }
        "one" | "broadcast" => {
            let notify = Arc::new(Notify::new());
            let waiting = notify.clone();
            tasks
                .lock()
                .push(pending(async move { waiting.notified().await }, &waker));
            let mut survivor = Box::pin(notify.notified());
            assert!(block_on(poll_once(survivor.as_mut())).is_none());
            if case == "one" {
                notify.notify_one();
            } else {
                notify.notify_waiters();
            }
            assert!(tasks.lock().is_empty());
            assert_eq!(block_on(poll_once(survivor.as_mut())), Some(()));
            assert!(block_on(poll_once(notify.notified())).is_none());
        }
        "watch" | "watch-close" => {
            let (sender, mut receiver) = watch::channel(0u8);
            let mut survivor = receiver.clone();
            tasks.lock().push(pending(
                async move {
                    let _ = receiver.changed().await;
                },
                &waker,
            ));
            if case == "watch" {
                sender.send(7).unwrap();
                drop(sender);
                assert_eq!(block_on(survivor.changed()), Ok(()));
                assert_eq!(*survivor.borrow(), 7);
            } else {
                drop(sender);
            }
            assert!(tasks.lock().is_empty());
            assert_eq!(block_on(survivor.changed()), Err(watch::RecvError));
        }
        _ => panic!("unknown cancellation case: {case}"),
    }
}

fn broadcast_drops_other_and_registers_new_wait() {
    let notify = Arc::new(Notify::new());
    let action = Arc::new(WakeAction::default());
    let waker = Waker::from(action.clone());
    let tasks = Arc::new(Mutex::new(Vec::<PendingTask>::new()));
    let later = Arc::new(Mutex::new(None::<PendingTask>));
    let count = Arc::new(WakeCount::default());
    let later_waker = Waker::from(count.clone());
    let first = notify.clone();
    tasks
        .lock()
        .push(pending(async move { first.notified().await }, &waker));
    let other = notify.clone();
    tasks.lock().push(pending(
        async move { other.notified().await },
        Waker::noop(),
    ));
    let mut survivor = Box::pin(notify.notified());
    assert!(block_on(poll_once(survivor.as_mut())).is_none());

    let cancelled = tasks.clone();
    let restarted = later.clone();
    let waiting = notify.clone();
    *action.0.lock() = Some(Box::new(move || {
        // Neither the notifying node nor a cached next pointer remains valid.
        let old = std::mem::take(&mut *cancelled.lock());
        drop(old);
        *restarted.lock() = Some(pending(
            async move { waiting.notified().await },
            &later_waker,
        ));
    }));
    notify.notify_waiters();
    assert!(tasks.lock().is_empty());
    assert_eq!(block_on(poll_once(survivor.as_mut())), Some(()));
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    let mut later = later.lock().take().unwrap();
    assert!(block_on(poll_once(later.as_mut())).is_none());
    notify.notify_one();
    assert_eq!(block_on(poll_once(later.as_mut())), Some(()));
    assert!(block_on(poll_once(notify.notified())).is_none());
}

#[derive(Default)]
struct RawActions {
    clone: Mutex<Option<Action>>,
    drop: Mutex<Option<Action>>,
}

fn run_action(action: &Mutex<Option<Action>>) {
    let action = action.lock().take();
    if let Some(action) = action {
        action();
    }
}

unsafe fn raw_clone(data: *const ()) -> RawWaker {
    // SAFETY: Each raw Waker owns one Arc count. ManuallyDrop borrows that count
    // while the returned Waker receives an independent owned clone.
    let state = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<RawActions>()) });
    run_action(&state.clone);
    RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &RAW_ACTIONS)
}

unsafe fn raw_wake(data: *const ()) {
    // SAFETY: wake consumes precisely the Arc count owned by this raw Waker.
    drop(unsafe { Arc::from_raw(data.cast::<RawActions>()) });
}

unsafe fn raw_wake_by_ref(_data: *const ()) {}

unsafe fn raw_drop(data: *const ()) {
    // SAFETY: drop consumes precisely the Arc count owned by this raw Waker.
    let state = unsafe { Arc::from_raw(data.cast::<RawActions>()) };
    run_action(&state.drop);
}

static RAW_ACTIONS: RawWakerVTable =
    RawWakerVTable::new(raw_clone, raw_wake, raw_wake_by_ref, raw_drop);

fn clone_and_drop_can_reenter_notifications() {
    let notify = Arc::new(Notify::new());
    let actions = Arc::new(RawActions::default());
    // SAFETY: The callbacks above maintain an owned, Send + Sync Arc per Waker.
    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(actions.clone()).cast(),
            &RAW_ACTIONS,
        ))
    };
    let clone_notify = notify.clone();
    *actions.clone.lock() = Some(Box::new(move || clone_notify.notify_waiters()));
    let mut first = Box::pin(notify.notified());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );

    let mut replaced = Box::pin(notify.notified());
    assert!(
        replaced
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let drop_notify = notify.clone();
    *actions.drop.lock() = Some(Box::new(move || drop_notify.notify_waiters()));
    assert!(
        replaced
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        replaced
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(block_on(poll_once(notify.notified())).is_none());
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn sync_reentrant_child() {
    let Ok(case) = std::env::var("RIVET_SYNC_REENTRANT_CASE") else {
        return;
    };
    match case.as_str() {
        "relisten" => broadcast_drops_other_and_registers_new_wait(),
        "raw" => clone_and_drop_can_reenter_notifications(),
        case => cancel_from_wake(case),
    }
}

#[test]
fn notification_callbacks_can_cancel_and_reenter_without_deadlocking() {
    for case in [
        "cancel",
        "one",
        "broadcast",
        "watch",
        "watch-close",
        "relisten",
        "raw",
    ] {
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "sync_reentrant_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("RIVET_SYNC_REENTRANT_CASE", case)
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "notification child {case} failed: {status}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "notification child {case} deadlocked"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn broadcast_completion_preserves_an_independent_single_permit() {
    block_on(async {
        let notify = Notify::new();
        let mut waiting = pin!(notify.notified());
        assert!(poll_once(waiting.as_mut()).await.is_none());
        notify.notify_one();
        notify.notify_waiters();
        assert_eq!(poll_once(waiting.as_mut()).await, Some(()));
        assert_eq!(poll_once(notify.notified()).await, Some(()));
        assert!(poll_once(notify.notified()).await.is_none());
    });
}
