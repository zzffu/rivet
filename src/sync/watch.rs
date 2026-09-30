//! Latest-value broadcast. Updates coalesce; the final unread value remains
//! observable after the last sender closes. Wait cancellation consumes no update.

use super::notification::Event;
use parking_lot::{RwLock, RwLockReadGuard};
use std::{
    fmt,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct Value<T> {
    value: T,
    version: u64,
}

struct Shared<T> {
    value: RwLock<Value<T>>,
    changed: Event,
    senders: AtomicUsize,
    receivers: AtomicUsize,
}

/// Creates a latest-value channel. The initial value is already marked seen.
pub fn channel<T>(value: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        value: RwLock::new(Value { value, version: 0 }),
        changed: Event::new(),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared, seen: 0 },
    )
}

pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    seen: u64,
}

/// A synchronous shared borrow. Drop it before awaiting or publishing an update.
pub struct Ref<'a, T>(RwLockReadGuard<'a, Value<T>>);
impl<T> Deref for Ref<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}
impl<T: fmt::Debug> fmt::Debug for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.value.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecvError;
impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("watch channel has closed")
    }
}
impl std::error::Error for RecvError {}

/// Retains the rejected value when no receivers remain.
#[derive(Debug)]
pub struct SendError<T>(pub T);
impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("watch channel has no receivers")
    }
}
impl<T: fmt::Debug> std::error::Error for SendError<T> {}

impl<T> Sender<T> {
    pub fn is_closed(&self) -> bool {
        self.shared.receivers.load(Ordering::Acquire) == 0
    }

    /// Publishes only while at least one receiver exists.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.is_closed() {
            return Err(SendError(value));
        }
        drop(self.send_replace(value));
        Ok(())
    }

    /// Publishes even with no receivers, returning the previous value. User
    /// destructors run outside the lock and after all waiters have been notified.
    pub fn send_replace(&self, value: T) -> T {
        let previous = {
            let mut current = self.shared.value.write();
            let version = current
                .version
                .checked_add(1)
                .expect("watch version exhausted");
            let previous = std::mem::replace(&mut current.value, value);
            current.version = version;
            previous
        };
        self.shared.changed.notify_all();
        previous
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(self.shared.value.read())
    }

    /// New receivers start with the current value marked seen.
    pub fn subscribe(&self) -> Receiver<T> {
        let seen = self.shared.value.read().version;
        self.shared.receivers.fetch_add(1, Ordering::Relaxed);
        Receiver {
            shared: self.shared.clone(),
            seen,
        }
    }
}

impl<T> Receiver<T> {
    /// Borrows the latest value without marking it seen.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(self.shared.value.read())
    }

    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let value = self.shared.value.read();
        self.seen = value.version;
        Ref(value)
    }

    /// Unread final updates take precedence over channel closure.
    pub fn has_changed(&self) -> Result<bool, RecvError> {
        let value = self.shared.value.read();
        if value.version != self.seen {
            Ok(true)
        } else if self.shared.senders.load(Ordering::Acquire) == 0 {
            Err(RecvError)
        } else {
            Ok(false)
        }
    }

    /// Waits for a new version, marking it seen only on successful completion.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        loop {
            let listener = self.shared.changed.listen();
            {
                let value = self.shared.value.read();
                if value.version != self.seen {
                    self.seen = value.version;
                    return Ok(());
                }
                if self.shared.senders.load(Ordering::Acquire) == 0 {
                    return Err(RecvError);
                }
            }
            listener.await;
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.changed.notify_all();
        }
    }
}
impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
            seen: self.seen,
        }
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receivers.fetch_sub(1, Ordering::AcqRel);
    }
}
