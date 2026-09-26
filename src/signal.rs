//! Scoped, executor-independent subscriptions to host shutdown signals.
//!
//! Creating the first [`ShutdownSignals`] installs the native handlers and starts
//! one shared waiting thread. Dropping the last subscription unregisters those
//! handlers and stops the thread. No handler is installed merely by linking Rivet.
//! Each subscriber receives both signal kinds independently; repeated signals of
//! the same kind may coalesce, and ordering between different kinds is unspecified.
//!
//! On Unix, an existing custom SIGINT or SIGTERM handler makes subscription fail
//! with [`io::ErrorKind::AlreadyExists`]. Default and ignored dispositions are
//! restored on the last drop, unless another owner has replaced Rivet's handler.
//! Applications must serialize other changes to these process-wide dispositions
//! with subscription creation and destruction: POSIX has no compare-and-swap for
//! `sigaction`. Rivet never chains an application callback from its signal handler.

use event_listener::Event;
use std::{
    io,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU8, Ordering},
    },
    thread::{self, JoinHandle},
};

#[cfg(unix)]
#[path = "signal/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "signal/windows.rs"]
mod platform;

const FIRST: u8 = 1;
const SECOND: u8 = 2;

/// A native host shutdown request. Receiving one does not exit the process.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SignalKind {
    /// Unix SIGINT.
    #[cfg(unix)]
    Interrupt,
    /// Unix SIGTERM.
    #[cfg(unix)]
    Terminate,
    /// Windows CTRL_C_EVENT.
    #[cfg(windows)]
    CtrlC,
    /// Windows CTRL_BREAK_EVENT.
    #[cfg(windows)]
    CtrlBreak,
}

impl SignalKind {
    fn from_bit(bit: u8) -> Self {
        match bit {
            #[cfg(unix)]
            FIRST => Self::Interrupt,
            #[cfg(unix)]
            _ => Self::Terminate,
            #[cfg(windows)]
            FIRST => Self::CtrlC,
            #[cfg(windows)]
            _ => Self::CtrlBreak,
        }
    }
}

struct Subscriber {
    pending: AtomicU8,
    changed: Event,
}

impl Subscriber {
    fn take(&self) -> Option<SignalKind> {
        self.pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                (pending != 0).then(|| pending & (pending - 1))
            })
            .ok()
            .map(|pending| SignalKind::from_bit(pending.isolate_lowest_one()))
    }
}

struct Hub {
    subscribers: Mutex<Vec<Arc<Subscriber>>>,
    failure: Mutex<Option<Arc<io::Error>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

impl Hub {
    fn error(&self) -> Option<io::Error> {
        lock(&self.failure)
            .as_ref()
            .map(|error| io::Error::new(error.kind(), Arc::clone(error)))
    }
}

struct Registry {
    hub: Arc<Hub>,
    native: platform::Registration,
    thread: Option<JoinHandle<()>>,
}

static REGISTRY: Mutex<Option<Registry>> = Mutex::new(None);

impl Registry {
    fn start(subscriber: Arc<Subscriber>) -> io::Result<Self> {
        let hub = Arc::new(Hub {
            subscribers: Mutex::new(vec![subscriber]),
            failure: Mutex::new(None),
        });
        let native = platform::Registration::new()?;
        let state = native.state();
        let thread_hub = Arc::clone(&hub);
        let thread = thread::Builder::new()
            .name("rivet-signals".into())
            .spawn(move || dispatch(state, thread_hub))?;
        Ok(Self {
            hub,
            native,
            thread: Some(thread),
        })
    }

    fn join(mut self) {
        if let Some(thread) = self.thread.take() {
            // A custom Waker may drop the last subscription while being woken on
            // this very thread. It cannot join itself; the stop flag makes the
            // thread release its final native-state Arc when the callback returns.
            if thread.thread().id() != thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

fn dispatch(state: Arc<platform::State>, hub: Arc<Hub>) {
    let mut subscribers = Vec::new();
    loop {
        let failure = state.wait().err();
        if state.stopped() {
            return;
        }
        let pending = state.take_pending();
        if failure.is_none() && pending == 0 {
            continue;
        }
        if let Some(error) = failure.as_ref() {
            *lock(&hub.failure) = Some(Arc::new(io::Error::new(error.kind(), error.to_string())));
        }
        subscribers.extend(lock(&hub.subscribers).iter().cloned());
        for subscriber in subscribers.drain(..) {
            if state.stopped() {
                return;
            }
            subscriber.pending.fetch_or(pending, Ordering::Release);
            // Never invoke Wakers under a registry/subscriber-list lock. They may
            // synchronously create or destroy another subscription.
            subscriber.changed.notify(usize::MAX);
        }
        if failure.is_some() {
            return;
        }
    }
}

/// An explicit subscription to Unix SIGINT/SIGTERM or Windows Ctrl+C/Ctrl+Break.
///
/// This object does not require a Rivet runtime. All live subscribers receive
/// each signal kind independently. There is at most one unconsumed notification
/// per kind per subscriber, rather than an unbounded signal queue.
///
/// On Unix, custom pre-existing handlers cause [`Self::new`] to return
/// [`io::ErrorKind::AlreadyExists`]; SIG_DFL and SIG_IGN are saved and restored.
/// Windows uses the calling process's current console and never attaches to or
/// allocates a console. Native console attachment changes reset Windows handler
/// registrations, so the host must not change consoles while subscribed.
pub struct ShutdownSignals {
    subscriber: Arc<Subscriber>,
    hub: Arc<Hub>,
}

impl ShutdownSignals {
    /// Subscribes explicitly, installing native handling only for the first owner.
    pub fn new() -> io::Result<Self> {
        let subscriber = Arc::new(Subscriber {
            pending: AtomicU8::new(0),
            changed: Event::new(),
        });
        let mut registry = lock(&REGISTRY);
        if let Some(active) = registry.as_ref() {
            if let Some(error) = active.hub.error() {
                return Err(error);
            }
            lock(&active.hub.subscribers).push(Arc::clone(&subscriber));
        } else {
            *registry = Some(Registry::start(Arc::clone(&subscriber))?);
        }
        let hub = Arc::clone(&registry.as_ref().expect("registry just initialized").hub);
        Ok(Self { subscriber, hub })
    }

    /// Waits for the next signal kind without consuming it on cancellation.
    ///
    /// Dropping a pending receive is safe: another call observes the outstanding
    /// notification. A native waiting failure is returned after pending signals
    /// have been drained. Simultaneously pending kinds have unspecified order.
    pub async fn recv(&mut self) -> io::Result<SignalKind> {
        loop {
            if let Some(kind) = self.subscriber.take() {
                return Ok(kind);
            }
            if let Some(error) = self.hub.error() {
                return Err(error);
            }
            let changed = self.subscriber.changed.listen();
            if let Some(kind) = self.subscriber.take() {
                return Ok(kind);
            }
            if let Some(error) = self.hub.error() {
                return Err(error);
            }
            changed.await;
        }
    }
}

impl Drop for ShutdownSignals {
    fn drop(&mut self) {
        let retiring = {
            let mut registry = lock(&REGISTRY);
            let Some(active) = registry.as_mut() else {
                return;
            };
            let empty = {
                let mut subscribers = lock(&active.hub.subscribers);
                subscribers.retain(|entry| !Arc::ptr_eq(entry, &self.subscriber));
                subscribers.is_empty()
            };
            if !empty {
                return;
            }
            let mut retiring = registry.take().expect("active registry");
            // Serialize native disarming with the next registration, but release
            // the registry lock before joining a thread that may invoke Wakers.
            retiring.native.stop();
            retiring
        };
        retiring.join();
    }
}
