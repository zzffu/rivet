//! Executor-independent coordination. No operation requires a Rivet worker.
//!
//! Channels retain their own bounds; moving a sender does not make a `!Send`
//! payload transferable. Never hold a synchronous watch borrow across `.await`.
//!
//! # Public dependencies
//!
//! Locks, guards and semaphores are the actual `async-lock` 3.x types; [`mpsc`]
//! exposes `async-channel` 2.x types and errors; [`oneshot`] is the
//! `futures-channel` 0.3.x module. These are direct re-exports, not Rivet wrappers.
//! Their type identity, public methods, bounds, errors and documented
//! cancellation/close behavior are part of Rivet's compatibility contract.
//! Dependency upgrades, replacements and feature changes must preserve that
//! contract and pass downstream compilation and affected behavior checks.

pub use async_lock::{
    Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore, SemaphoreGuard,
};
pub use futures_channel::oneshot;

/// Bounded multi-producer, multi-consumer channels with cancellation-safe receive.
/// Closing a channel rejects new sends while receivers can drain queued values.
pub mod mpsc {
    pub use async_channel::{
        Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError, bounded,
    };
}

pub mod watch;

pub(crate) mod notification;

use notification::Event;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Sticky cooperative cancellation shared by tasks on any executor.
/// Cancellation requests termination; it is not proof that a task has exited.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    event: Event,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Requests cancellation once and wakes every current waiter.
    pub fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.state.event.notify_all();
        }
    }

    /// Cancellation-safe; future waiters also observe an earlier cancellation.
    pub async fn cancelled(&self) {
        loop {
            let listener = self.state.event.listen();
            if self.is_cancelled() {
                return;
            }
            listener.await;
        }
    }
}

/// A single stored notification permit, plus broadcast to currently waiting tasks.
/// Repeated `notify_one` calls coalesce while the stored permit is unconsumed.
/// Dropping a wait does not consume a permit. `notify_waiters` stores no permit.
#[derive(Debug)]
pub struct Notify {
    event: Event,
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl Notify {
    pub fn new() -> Self {
        Self {
            event: Event::new(),
        }
    }

    /// Wakes one waiter or preserves a single permit for a future waiter.
    pub fn notify_one(&self) {
        self.event.notify_one();
    }

    /// Wakes waits that have been polled, without notifying subsequent waits.
    pub fn notify_waiters(&self) {
        self.event.notify_all();
    }

    pub async fn notified(&self) {
        self.event.listen().await;
    }
}
