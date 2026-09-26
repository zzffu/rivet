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

use event_listener::Event;
use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
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
            self.state.event.notify(usize::MAX);
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
    sender: async_channel::Sender<()>,
    receiver: async_channel::Receiver<()>,
    broadcast: Event,
    generation: AtomicU64,
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl Notify {
    pub fn new() -> Self {
        let (sender, receiver) = async_channel::bounded(1);
        Self {
            sender,
            receiver,
            broadcast: Event::new(),
            generation: AtomicU64::new(0),
        }
    }

    /// Wakes one waiter or preserves a single permit for a future waiter.
    pub fn notify_one(&self) {
        let _ = self.sender.try_send(());
    }

    /// Wakes waits that have been polled, without notifying subsequent waits.
    pub fn notify_waiters(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        self.broadcast.notify(usize::MAX);
    }

    pub async fn notified(&self) {
        let generation = self.generation.load(Ordering::Acquire);
        loop {
            let listener = self.broadcast.listen();
            if self.generation.load(Ordering::Acquire) != generation {
                return;
            }
            let mut listener = pin!(listener);
            let mut receive = pin!(self.receiver.recv());
            let broadcast = poll_fn(|cx| {
                // A broadcast must not consume an independent stored permit.
                if listener.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(true);
                }
                receive.as_mut().poll(cx).map(|_| false)
            })
            .await;
            if !broadcast {
                return;
            }
            // EventListener forwards unconsumed notifications on cancellation.
            // The generation prevents a forwarded broadcast waking a new wait.
        }
    }
}
