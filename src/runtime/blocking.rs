use super::JoinError;
use crate::config::BlockingConfig;
use parking_lot::{Condvar, Mutex};
use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    future::Future,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle as Thread},
};

/// A rejected blocking submission never runs its closure.
#[derive(Debug)]
pub enum BlockingSpawnError {
    AtCapacity,
    ShuttingDown,
    NotRunning,
    /// Starting an additional blocking thread failed.
    Thread(io::Error),
}
impl fmt::Display for BlockingSpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtCapacity => f.write_str("the blocking queue is at capacity"),
            Self::ShuttingDown => f.write_str("the runtime is shutting down"),
            Self::NotRunning => f.write_str("no Rivet worker is currently running on this thread"),
            Self::Thread(error) => write!(f, "cannot start a blocking thread: {error}"),
        }
    }
}
impl std::error::Error for BlockingSpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Thread(error) => Some(error),
            _ => None,
        }
    }
}

struct JoinState<T> {
    result: Option<Result<T, JoinError>>,
    waiter: Option<Waker>,
    receiver: bool,
}
struct JoinCell<T> {
    state: Mutex<JoinState<T>>,
    finished: AtomicBool,
}

/// An owned result from the runtime's bounded blocking pool.
///
/// Dropping or aborting the handle cancels work that is still queued. Once a
/// closure starts, it runs to completion and keeps its thread occupied even if
/// this handle is dropped. Cancellation of running work therefore still joins
/// its actual result. Runtime destruction joins every blocking thread, including
/// detached work; blocking closures must not wait for stopped async workers.
pub struct BlockingJoinHandle<T> {
    cell: Arc<JoinCell<T>>,
    pool: Weak<Pool>,
    cancel_on_drop: bool,
}
impl<T> fmt::Debug for BlockingJoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingJoinHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}
impl<T> BlockingJoinHandle<T> {
    pub fn is_finished(&self) -> bool {
        self.cell.finished.load(Ordering::Acquire)
    }

    /// Cancel a queued closure. This has no effect after execution has started.
    /// Queued captures are destroyed synchronously on the cancelling thread.
    pub fn abort(&self) {
        if self.is_finished() {
            return;
        }
        if let Some(pool) = self.pool.upgrade() {
            pool.cancel(Arc::as_ptr(&self.cell) as usize);
        }
    }

    /// Stop receiving the result without cancelling queued work.
    pub fn detach(mut self) {
        self.cancel_on_drop = false;
    }

    /// Request cancellation, then wait for cancellation or the running result.
    pub async fn cancel(self) -> Result<T, JoinError> {
        self.abort();
        self.await
    }
}
impl<T> Unpin for BlockingJoinHandle<T> {}
impl<T> Future for BlockingJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut replacement = Some(cx.waker().clone());
        let mut state = self.cell.state.lock();
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        assert!(
            !self.is_finished(),
            "BlockingJoinHandle polled after completion"
        );
        if !state
            .waiter
            .as_ref()
            .is_some_and(|waiter| waiter.will_wake(cx.waker()))
        {
            let previous = std::mem::replace(&mut state.waiter, replacement.take());
            drop(state);
            drop_safely(previous);
        }
        Poll::Pending
    }
}
impl<T> Drop for BlockingJoinHandle<T> {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.abort();
        }
        // A completed result is user-owned data too. Its destructor must not
        // unwind through cancellation or an enclosing runtime shutdown.
        let (result, waiter) = {
            let mut state = self.cell.state.lock();
            state.receiver = false;
            (state.result.take(), state.waiter.take())
        };
        drop_safely(waiter);
        drop_safely(result);
    }
}

trait Job: Send {
    fn id(&self) -> usize;
    fn run(self: Box<Self>, running: &mut Running<'_>);
    fn cancel(self: Box<Self>);
}
struct Work<F, T> {
    function: Option<F>,
    cell: Arc<JoinCell<T>>,
}
impl<F, T> Work<F, T> {
    fn complete(&self, result: Result<T, JoinError>, running: Option<&mut Running<'_>>) {
        let mut state = self.cell.state.lock();
        if state.receiver {
            state.result = Some(result);
        } else {
            drop(state);
            drop_safely(result);
            state = self.cell.state.lock();
        }
        // No path takes the result lock while holding the pool lock. Releasing
        // the occupied slot here makes a ready result proof of completed cleanup
        // and accounting, without running a user Waker under either lock.
        if let Some(running) = running {
            running.release();
        }
        self.cell.finished.store(true, Ordering::Release);
        let wake = state.waiter.take();
        drop(state);
        if let Some(wake) = wake {
            ignore_panic(|| wake.wake());
        }
    }
}
impl<F, T> Job for Work<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    fn id(&self) -> usize {
        // A handle and its queued job both own this allocation, so its address
        // cannot be reused while any cancellation could still target the job.
        Arc::as_ptr(&self.cell) as usize
    }

    fn run(mut self: Box<Self>, running: &mut Running<'_>) {
        let function = self.function.take().unwrap();
        let result = match catch_unwind(AssertUnwindSafe(function)) {
            Ok(value) => Ok(value),
            Err(panic) => {
                discard_panic(panic);
                Err(JoinError::Panicked)
            }
        };
        self.complete(result, Some(running));
    }

    fn cancel(mut self: Box<Self>) {
        drop_safely(self.function.take());
        self.complete(Err(JoinError::Cancelled), None);
    }
}
impl<F, T> Drop for Work<F, T> {
    fn drop(&mut self) {
        drop_safely(self.function.take());
        if !self.cell.finished.load(Ordering::Acquire) {
            self.complete(Err(JoinError::Cancelled), None);
        }
    }
}

struct Running<'a> {
    pool: &'a Pool,
    released: bool,
}
impl Running<'_> {
    fn release(&mut self) {
        if !self.released {
            self.pool.state.lock().running -= 1;
            self.released = true;
        }
    }
}
impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

struct State {
    stopping: bool,
    queue: VecDeque<Box<dyn Job>>,
    threads: Vec<Thread<()>>,
    running: usize,
}
pub(super) struct Pool {
    config: BlockingConfig,
    state: Mutex<State>,
    ready: Condvar,
}
impl Pool {
    pub(super) fn new(config: BlockingConfig) -> io::Result<Arc<Self>> {
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(config.queue_capacity)
            .map_err(io::Error::other)?;
        let mut threads = Vec::new();
        threads
            .try_reserve_exact(config.threads)
            .map_err(io::Error::other)?;
        Ok(Arc::new(Self {
            config,
            state: Mutex::new(State {
                stopping: false,
                queue,
                threads,
                running: 0,
            }),
            ready: Condvar::new(),
        }))
    }

    pub(super) fn spawn<F, T>(
        self: &Arc<Self>,
        function: F,
    ) -> Result<BlockingJoinHandle<T>, BlockingSpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let mut state = self.state.lock();
        let rejection = if state.stopping {
            Some(BlockingSpawnError::ShuttingDown)
        } else if state.queue.len() == self.config.queue_capacity {
            Some(BlockingSpawnError::AtCapacity)
        } else {
            None
        };
        if let Some(error) = rejection {
            drop(state);
            drop_safely(function);
            return Err(error);
        }
        if state.running + state.queue.len() >= state.threads.len()
            && state.threads.len() < self.config.threads
        {
            let pool = self.clone();
            let index = state.threads.len();
            match thread::Builder::new()
                .name(format!("rivet-blocking-{index}"))
                .spawn(move || pool.run())
            {
                Ok(thread) => state.threads.push(thread),
                Err(error) => {
                    // No job or admission was committed. Drop captures outside
                    // the pool lock so their destructors may reenter the handle.
                    drop(state);
                    drop_safely(function);
                    return Err(BlockingSpawnError::Thread(error));
                }
            }
        }
        let cell = Arc::new(JoinCell {
            state: Mutex::new(JoinState {
                result: None,
                waiter: None,
                receiver: true,
            }),
            finished: AtomicBool::new(false),
        });
        state.queue.push_back(Box::new(Work {
            function: Some(function),
            cell: cell.clone(),
        }));
        drop(state);
        self.ready.notify_one();
        Ok(BlockingJoinHandle {
            cell,
            pool: Arc::downgrade(self),
            cancel_on_drop: true,
        })
    }

    fn run(&self) {
        loop {
            let job = {
                let mut state = self.state.lock();
                while state.queue.is_empty() && !state.stopping {
                    self.ready.wait(&mut state);
                }
                if state.stopping {
                    return;
                }
                let job = state.queue.pop_front().unwrap();
                state.running += 1;
                job
            };
            // Keep the occupied thread counted through closure execution and
            // destruction of an abandoned result. The guard also covers panic.
            let mut running = Running {
                pool: self,
                released: false,
            };
            ignore_panic(|| job.run(&mut running));
        }
    }

    fn cancel(&self, id: usize) {
        let job = {
            let mut state = self.state.lock();
            let index = state.queue.iter().position(|job| job.id() == id);
            index.and_then(|index| state.queue.remove(index))
        };
        if let Some(job) = job {
            ignore_panic(|| job.cancel());
        }
    }

    pub(super) fn stop(&self) {
        let queue = {
            let mut state = self.state.lock();
            state.stopping = true;
            std::mem::take(&mut state.queue)
        };
        self.ready.notify_all();
        for job in queue {
            ignore_panic(|| job.cancel());
        }
    }

    pub(super) fn join(&self) {
        self.stop();
        let threads = std::mem::take(&mut self.state.lock().threads);
        for thread in threads {
            if let Err(panic) = thread.join() {
                discard_panic(panic);
            }
        }
    }
}

pub(super) fn ignore_panic(action: impl FnOnce()) {
    if let Err(panic) = catch_unwind(AssertUnwindSafe(action)) {
        discard_panic(panic);
    }
}
pub(super) fn drop_safely<T>(value: T) {
    ignore_panic(|| drop(value));
}
pub(super) fn discard_panic(panic: Box<dyn Any + Send>) {
    // A panic payload can itself have a panicking destructor. Do not let that
    // second panic bypass the rest of shutdown or kill a pool thread.
    if let Err(nested) = catch_unwind(AssertUnwindSafe(|| drop(panic))) {
        std::mem::forget(nested);
    }
}
