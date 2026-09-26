use futures_lite::future::{block_on, poll_once, zip};
use rivet::sync::{CancellationToken, Notify, watch};
use std::pin::pin;

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
