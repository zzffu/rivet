use super::{Worker, WorkerShared};
use crate::driver::Arena;
use parking_lot::Mutex;
use std::{
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

const QUEUED: u8 = 1;
const CLOSED: u8 = 2;

/// Admission failures never enqueue a partial task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpawnError {
    AtCapacity,
    ShuttingDown,
    NotRunning,
}
impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AtCapacity => "all workers are at their task admission limit",
            Self::ShuttingDown => "the runtime is shutting down",
            Self::NotRunning => "no worker is currently driving this runtime",
        })
    }
}
impl std::error::Error for SpawnError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinError {
    Cancelled,
    Panicked,
}
impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cancelled => "task cancelled",
            Self::Panicked => "task panicked",
        })
    }
}
impl std::error::Error for JoinError {}

struct JoinState<T> {
    result: Option<Result<T, JoinError>>,
    waiter: Option<Waker>,
    receiver: bool,
}
pub(crate) struct JoinCell<T> {
    state: Mutex<JoinState<T>>,
    control: Arc<Control>,
}
impl<T> JoinCell<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(JoinState {
                result: None,
                waiter: None,
                receiver: true,
            }),
            control: Arc::new(Control {
                cancelled: AtomicBool::new(false),
                done: AtomicBool::new(false),
                wake: Mutex::new(None),
            }),
        })
    }
    fn complete(&self, result: Result<T, JoinError>) {
        let wake = {
            let mut state = self.state.lock();
            if state.receiver {
                state.result = Some(result);
            }
            self.control.done.store(true, Ordering::Release);
            state.waiter.take()
        };
        self.control.wake.lock().take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

/// Joining transfers the result. Dropping a live handle requests owner-thread
/// cancellation; use `detach` to let a task outlive its handle.
pub struct JoinHandle<T> {
    cell: Arc<JoinCell<T>>,
    cancel_on_drop: bool,
}
impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}
impl<T> JoinHandle<T> {
    pub fn is_finished(&self) -> bool {
        self.cell.control.done.load(Ordering::Acquire)
    }
    pub fn abort(&self) {
        self.cell.control.cancel();
    }
    pub fn detach(mut self) {
        self.cancel_on_drop = false;
    }
    pub async fn cancel(self) -> Result<T, JoinError> {
        self.abort();
        self.await
    }
}
impl<T> Unpin for JoinHandle<T> {}
impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.cell.state.lock();
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }
        assert!(
            !self.cell.control.done.load(Ordering::Acquire),
            "JoinHandle polled after completion"
        );
        if !state
            .waiter
            .as_ref()
            .is_some_and(|w| w.will_wake(cx.waker()))
        {
            state.waiter = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}
impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.cell.control.cancel();
        }
        let mut state = self.cell.state.lock();
        state.receiver = false;
        state.waiter = None;
        state.result = None;
    }
}

pub(crate) struct Control {
    cancelled: AtomicBool,
    done: AtomicBool,
    wake: Mutex<Option<Waker>>,
}
impl Control {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let wake = self.wake.lock().clone();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

pub(crate) struct Admission(pub Arc<WorkerShared>);
impl Drop for Admission {
    fn drop(&mut self) {
        self.0.admitted.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Only generation-bearing integers cross threads. In particular, a Waker never
/// contains a local future, a Runnable, an Rc, or a destructor for local state.
pub(crate) struct TaskWake {
    state: AtomicU8,
    id: u64,
    shared: Arc<WorkerShared>,
}
impl TaskWake {
    fn schedule(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & (CLOSED | QUEUED) != 0 {
                return;
            }
            match self.state.compare_exchange_weak(
                state,
                state | QUEUED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }
        // Each admitted slot has at most one token. A completed task keeps its
        // admission and arena slot until its last queued token is consumed.
        assert!(
            self.shared.ready.push(self.id).is_ok(),
            "bounded task queue invariant violated"
        );
        self.shared.notifier.notify();
    }
}
impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.schedule();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
    }
}

pub(crate) trait Body {
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;
    fn fail(self: Pin<&Self>, error: JoinError);
}
pin_project_lite::pin_project! {
    struct TypedBody<F: Future> { #[pin] future: F, cell: Arc<JoinCell<F::Output>> }
}
impl<F: Future> Body for TypedBody<F> {
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        match this.future.poll(cx) {
            Poll::Ready(result) => {
                this.cell.complete(Ok(result));
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn fail(self: Pin<&Self>, error: JoinError) {
        self.cell.complete(Err(error));
    }
}

struct Entry {
    body: Option<Pin<Box<dyn Body>>>,
    wake: Arc<TaskWake>,
    control: Arc<Control>,
    _admission: Admission,
}
pub(crate) struct TaskSet {
    entries: Arena<Entry>,
}
pub(crate) struct Running {
    id: u64,
    body: Pin<Box<dyn Body>>,
    wake: Arc<TaskWake>,
    control: Arc<Control>,
}
impl TaskSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Arena::new(capacity),
        }
    }
    fn insert<F: Future + 'static>(
        &mut self,
        future: F,
        cell: Arc<JoinCell<F::Output>>,
        admission: Admission,
    ) where
        F::Output: 'static,
    {
        // No wake can escape before its generation is installed.
        let shared = admission.0.clone();
        let entry = Entry {
            body: Some(Box::pin(TypedBody {
                future,
                cell: cell.clone(),
            })),
            wake: Arc::new(TaskWake {
                state: AtomicU8::new(0),
                id: 0,
                shared,
            }),
            control: cell.control.clone(),
            _admission: admission,
        };
        let id = match self.entries.insert(entry) {
            Ok(id) => id,
            Err(_) => unreachable!("admission reserves an arena slot"),
        };
        let entry = self.entries.get_mut(id).unwrap();
        Arc::get_mut(&mut entry.wake).unwrap().id = id;
        let waker = Waker::from(entry.wake.clone());
        *entry.control.wake.lock() = Some(waker);
        entry.wake.schedule();
    }
    pub fn take(&mut self, id: u64) -> Option<Running> {
        let entry = self.entries.get_mut(id)?;
        entry.wake.state.fetch_and(!QUEUED, Ordering::AcqRel);
        let Some(body) = entry.body.take() else {
            self.retire(id);
            return None;
        };
        Some(Running {
            id,
            body,
            wake: entry.wake.clone(),
            control: entry.control.clone(),
        })
    }
    pub fn finish(&mut self, running: Running, finished: bool) -> Option<Pin<Box<dyn Body>>> {
        if !finished {
            self.entries.get_mut(running.id).unwrap().body = Some(running.body);
            return None;
        }
        let old = running.wake.state.fetch_or(CLOSED, Ordering::AcqRel);
        if old & QUEUED == 0 {
            self.retire(running.id);
        }
        Some(running.body)
    }
    fn retire(&mut self, id: u64) {
        if let Some(entry) = self.entries.remove(id)
            && (id >> 32) as u32 == u32::MAX
        {
            entry._admission.0.retired.fetch_add(1, Ordering::AcqRel);
        }
    }
    pub fn take_shutdown(&mut self) -> Option<Pin<Box<dyn Body>>> {
        let key = self.entries.iter().next().map(|(key, _)| key)?;
        let mut entry = self.entries.remove(key).unwrap();
        entry.wake.state.fetch_or(CLOSED, Ordering::AcqRel);
        // Shutdown has closed admission. Late foreign wakes may enqueue only
        // integer tombstones, so owner cleanup need not wait for that thread
        // to resume between its scheduling CAS and queue publication.
        entry.body.take()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
impl Running {
    pub fn poll(&mut self) -> bool {
        if self.control.cancelled.load(Ordering::Acquire) {
            self.body.as_ref().fail(JoinError::Cancelled);
            return true;
        }
        let waker = Waker::from(self.wake.clone());
        let mut cx = Context::from_waker(&waker);
        match catch_unwind(AssertUnwindSafe(|| self.body.as_mut().poll(&mut cx))) {
            Ok(Poll::Ready(())) => true,
            Ok(Poll::Pending) => false,
            Err(_) => {
                self.body.as_ref().fail(JoinError::Panicked);
                true
            }
        }
    }
}

pub(crate) trait Launch: Send {
    fn launch(self: Box<Self>, worker: &Rc<Worker>);
}
struct Factory<F, T> {
    factory: Option<F>,
    cell: Arc<JoinCell<T>>,
    admission: Option<Admission>,
}
impl<F, T> Drop for Factory<F, T> {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.take() {
            self.cell.complete(Err(JoinError::Cancelled));
            let _ = catch_unwind(AssertUnwindSafe(|| drop(factory)));
        }
    }
}
impl<F, Fut, T> Launch for Factory<F, T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    fn launch(mut self: Box<Self>, worker: &Rc<Worker>) {
        if self.cell.control.cancelled.load(Ordering::Acquire) {
            return;
        }
        let factory = self.factory.take().unwrap();
        match catch_unwind(AssertUnwindSafe(factory)) {
            Ok(future) => worker.tasks.borrow_mut().insert(
                future,
                self.cell.clone(),
                self.admission.take().unwrap(),
            ),
            Err(_) => self.cell.complete(Err(JoinError::Panicked)),
        }
    }
}
pub(crate) fn factory<F, Fut, T>(
    factory: F,
    admission: Admission,
) -> (Box<dyn Launch>, JoinHandle<T>)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let cell = JoinCell::new();
    (
        Box::new(Factory {
            factory: Some(factory),
            cell: cell.clone(),
            admission: Some(admission),
        }),
        JoinHandle {
            cell,
            cancel_on_drop: true,
        },
    )
}
pub(crate) fn local<F: Future + 'static>(
    worker: &Rc<Worker>,
    future: F,
    admission: Admission,
) -> JoinHandle<F::Output>
where
    F::Output: 'static,
{
    let cell = JoinCell::new();
    worker
        .tasks
        .borrow_mut()
        .insert(future, cell.clone(), admission);
    JoinHandle {
        cell,
        cancel_on_drop: true,
    }
}

/// Cooperatively return to the worker's fair task/I/O/timer rotation.
pub async fn yield_now() {
    struct Yield(bool);
    impl Future for Yield {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    Yield(false).await
}
