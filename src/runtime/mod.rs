//! Owner-thread execution with bounded admission and cross-thread-safe wakes.
//!
//! Worker zero is the thread constructing `Runtime`, and only makes progress
//! inside `block_on`. Its local tasks and sockets pause between calls and resume
//! on the next call. Other workers run continuously. Automatic placement never
//! chooses an inactive caller worker; in a single-thread runtime an external
//! spawn while `block_on` is not running returns `SpawnError::NotRunning`.
//! Dropping the runtime cancels local tasks and drains native memory releases on
//! every owner thread before joining those threads.

pub(crate) mod io;
mod task;
pub(crate) mod timer;

use crate::{
    buffer::BufferPool,
    capability::{CapabilityReport, ZcStats},
    config::RuntimeConfig,
    driver::{Driver, Event, Notifier, Shared},
};
use crossbeam_queue::ArrayQueue;
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io as stdio,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::pin,
    rc::Rc,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread::{self, JoinHandle as Thread},
    time::{Duration, Instant},
};
use task::{Admission, Launch, TaskSet};
pub use task::{JoinError, JoinHandle, SpawnError, yield_now};

thread_local! {
    #[cfg_attr(
        target_os = "android",
        allow(
            clippy::missing_const_for_thread_local,
            reason = "Already const; Android std TLS false positive (rust-lang/rust-clippy#13422)."
        )
    )]
    static CURRENT: RefCell<Option<Rc<Worker>>> = const { RefCell::new(None) };
}
pub(crate) fn current() -> stdio::Result<Rc<Worker>> {
    CURRENT.with(|slot| slot.borrow().clone()).ok_or_else(|| {
        stdio::Error::new(
            stdio::ErrorKind::NotConnected,
            "operation requires a running Rivet worker",
        )
    })
}
struct Enter;
impl Enter {
    fn new(worker: &Rc<Worker>) -> Self {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            assert!(slot.is_none(), "nested Rivet execution is not supported");
            *slot = Some(worker.clone());
        });
        Self
    }
}
impl Drop for Enter {
    fn drop(&mut self) {
        CURRENT.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

struct Inbox {
    closed: bool,
    factories: VecDeque<Box<dyn Launch>>,
}
pub(crate) struct WorkerShared {
    notifier: Arc<Notifier>,
    ready: ArrayQueue<u64>,
    inbox: Mutex<Inbox>,
    admitted: AtomicUsize,
    retired: AtomicUsize,
    active: AtomicBool,
    limit: usize,
}
impl WorkerShared {
    fn admit(self: &Arc<Self>) -> Result<Admission, SpawnError> {
        let mut load = self.admitted.load(Ordering::Acquire);
        loop {
            if load >= self.limit - self.retired.load(Ordering::Acquire) {
                return Err(SpawnError::AtCapacity);
            }
            match self.admitted.compare_exchange_weak(
                load,
                load + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Admission(self.clone())),
                Err(next) => load = next,
            }
        }
    }
}
struct Group {
    workers: Vec<Arc<WorkerShared>>,
    stopping: AtomicBool,
    cursor: AtomicUsize,
}
impl Group {
    fn enqueue<F, Fut, T>(&self, start: usize, factory: F) -> Result<JoinHandle<T>, SpawnError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let mut any_active = false;
        for step in 0..self.workers.len() {
            let worker = &self.workers[(start + step) % self.workers.len()];
            let mut inbox = worker.inbox.lock();
            if inbox.closed || self.stopping.load(Ordering::Acquire) {
                drop(inbox);
                return Err(SpawnError::ShuttingDown);
            }
            if !worker.active.load(Ordering::Acquire) {
                continue;
            }
            any_active = true;
            let Ok(admission) = worker.admit() else {
                continue;
            };
            // Inactivity and shutdown use this same lock. Commit to exactly
            // one owner only after checking that it can still drive the task.
            let (command, join) = task::factory(factory, admission);
            inbox.factories.push_back(command);
            drop(inbox);
            worker.notifier.notify();
            return Ok(join);
        }
        Err(if self.stopping.load(Ordering::Acquire) {
            SpawnError::ShuttingDown
        } else if any_active {
            SpawnError::AtCapacity
        } else {
            SpawnError::NotRunning
        })
    }
    fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        for worker in &self.workers {
            worker.inbox.lock().closed = true;
            worker.notifier.notify();
        }
    }
}

/// A thread-safe factory submitter. The factory crosses threads; the future it
/// creates does not, so it may freely contain `Rc`, local sockets and leases.
#[derive(Clone)]
pub struct Handle {
    group: Arc<Group>,
}
impl Handle {
    pub fn current() -> stdio::Result<Self> {
        current()?
            .group
            .upgrade()
            .map(|group| Self { group })
            .ok_or_else(|| stdio::Error::new(stdio::ErrorKind::BrokenPipe, "runtime has stopped"))
    }
    pub fn spawn<F, Fut, T>(&self, factory: F) -> Result<JoinHandle<T>, SpawnError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        if self.group.stopping.load(Ordering::Acquire) {
            return Err(SpawnError::ShuttingDown);
        }
        let count = self.group.workers.len();
        let start = self.group.cursor.fetch_add(1, Ordering::Relaxed) % count;
        let mut selected = None;
        // Least admitted load, with a rotating tie break. This is only a
        // placement hint; enqueue rechecks each candidate under its inbox lock.
        for step in 0..count {
            let index = (start + step) % count;
            let worker = &self.group.workers[index];
            if !worker.active.load(Ordering::Acquire) {
                continue;
            }
            let load = worker.admitted.load(Ordering::Acquire);
            if load < worker.limit && selected.is_none_or(|(_, best)| load < best) {
                selected = Some((index, load));
            }
        }
        self.group
            .enqueue(selected.map_or(start, |(index, _)| index), factory)
    }
}

pub fn spawn<F, Fut, T>(factory: F) -> Result<JoinHandle<T>, SpawnError>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    Handle::current()
        .map_err(|_| SpawnError::NotRunning)?
        .spawn(factory)
}
pub fn spawn_local<F: Future + 'static>(future: F) -> Result<JoinHandle<F::Output>, SpawnError>
where
    F::Output: 'static,
{
    let worker = current().map_err(|_| SpawnError::NotRunning)?;
    if worker.shutting_down.get() {
        return Err(SpawnError::ShuttingDown);
    }
    let admission = worker.shared.admit()?;
    Ok(task::local(&worker, future, admission))
}
pub fn buffer_pool() -> stdio::Result<BufferPool> {
    Ok(current()?.pool.clone())
}

/// A runtime is intentionally `!Send` and `!Sync`: its caller worker, driver,
/// buffers, root future and all local future destructors stay on one thread.
pub struct Runtime {
    worker: Rc<Worker>,
    group: Arc<Group>,
    threads: Vec<Thread<stdio::Result<()>>>,
    capabilities: Vec<CapabilityReport>,
    _local: PhantomData<Rc<()>>,
}
impl Runtime {
    pub fn new(config: RuntimeConfig) -> stdio::Result<Self> {
        if CURRENT.with(|slot| slot.borrow().is_some()) {
            return Err(stdio::Error::new(
                stdio::ErrorKind::AlreadyExists,
                "cannot construct a runtime inside another worker",
            ));
        }
        let config = Arc::new(config.normalized()?);
        let backend_shared = Arc::new(Shared::new(config.workers));
        let mut workers = Vec::with_capacity(config.workers);
        for _ in 0..config.workers {
            workers.push(Arc::new(WorkerShared {
                notifier: Arc::new(Notifier::new()?),
                ready: ArrayQueue::new(config.limits.max_tasks),
                inbox: Mutex::new(Inbox {
                    closed: false,
                    factories: VecDeque::with_capacity(config.limits.max_tasks),
                }),
                admitted: AtomicUsize::new(0),
                retired: AtomicUsize::new(0),
                active: AtomicBool::new(false),
                limit: config.limits.max_tasks,
            }));
        }
        let group = Arc::new(Group {
            workers,
            stopping: AtomicBool::new(false),
            cursor: AtomicUsize::new(0),
        });
        let worker = Worker::new(config.clone(), 0, &group, backend_shared.clone())?;
        let mut capabilities = vec![worker.driver.borrow().capabilities().clone()];
        let mut threads = Vec::with_capacity(config.workers - 1);
        for index in 1..config.workers {
            let config = config.clone();
            let group_thread = group.clone();
            let shared = backend_shared.clone();
            let (sender, receiver) = mpsc::sync_channel(1);
            let thread = thread::Builder::new()
                .name(format!("rivet-{index}"))
                .spawn(move || {
                    let worker = match Worker::new(config, index, &group_thread, shared) {
                        Ok(worker) => worker,
                        Err(error) => {
                            let _ = sender.send(Err(error));
                            return Ok(());
                        }
                    };
                    let _enter = Enter::new(&worker);
                    worker.shared.active.store(true, Ordering::Release);
                    if sender
                        .send(Ok(worker.driver.borrow().capabilities().clone()))
                        .is_err()
                    {
                        group_thread.stop();
                    }
                    let run = catch_unwind(AssertUnwindSafe(|| worker.run_background()));
                    worker.shared.active.store(false, Ordering::Release);
                    let cleanup = worker.shutdown();
                    match run {
                        Ok(result) => result.and(cleanup),
                        Err(panic) => {
                            group_thread.stop();
                            resume_unwind(panic)
                        }
                    }
                });
            let initialization = match thread {
                Ok(thread) => {
                    threads.push(thread);
                    receiver.recv().unwrap_or_else(|_| {
                        Err(stdio::Error::other("worker exited during initialization"))
                    })
                }
                Err(error) => Err(error),
            };
            match initialization {
                Ok(report) => capabilities.push(report),
                Err(error) => {
                    group.stop();
                    {
                        let _enter = Enter::new(&worker);
                        let _ = worker.shutdown();
                    }
                    for thread in threads {
                        let _ = thread.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            worker,
            group,
            threads,
            capabilities,
            _local: PhantomData,
        })
    }
    pub fn handle(&self) -> Handle {
        Handle {
            group: self.group.clone(),
        }
    }
    pub fn capabilities(&self) -> &[CapabilityReport] {
        &self.capabilities
    }
    pub fn buffer_pool(&self) -> BufferPool {
        self.worker.pool.clone()
    }
    /// Snapshot of the caller's driver. RX copy/allocation counters belong to
    /// the ZCRX instance and must not be summed again through imported views.
    pub fn zc_stats(&self) -> ZcStats {
        self.worker.driver.borrow().zc_stats()
    }
    /// Run the root future on this thread. Existing local tasks pause when the
    /// root returns and resume on the next call. Background workers keep running.
    pub fn block_on<F: Future>(&mut self, future: F) -> F::Output {
        assert!(
            !self.group.stopping.load(Ordering::Acquire),
            "runtime has stopped"
        );
        let _enter = Enter::new(&self.worker);
        self.worker.shared.active.store(true, Ordering::Release);
        let root = Arc::new(RootWake {
            ready: AtomicBool::new(true),
            notifier: self.worker.shared.notifier.clone(),
        });
        let waker = Waker::from(root.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        let result = catch_unwind(AssertUnwindSafe(|| {
            loop {
                if root.ready.swap(false, Ordering::AcqRel)
                    && let Poll::Ready(value) = future.as_mut().poll(&mut cx)
                {
                    break value;
                }
                if self.group.stopping.load(Ordering::Acquire) {
                    panic!("a Rivet worker stopped");
                }
                if let Err(error) = self.worker.turn(Some(&root.ready)) {
                    self.group.stop();
                    panic!("native worker failed: {error}");
                }
            }
        }));
        // Serialize inactivity with admission. Tasks already placed on worker 0
        // are deliberately retained for a subsequent block_on, never migrated.
        {
            let _inbox = self.worker.shared.inbox.lock();
            self.worker.shared.active.store(false, Ordering::Release);
        }
        match result {
            Ok(value) => value,
            Err(panic) => resume_unwind(panic),
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.group.stop();
        {
            let _enter = Enter::new(&self.worker);
            let _ = self.worker.shutdown();
        }
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
/// Snapshot of the current driver. Shared ZCRX instance counters are not
/// independently attributable to each worker; see [`ZcStats`].
pub fn zc_stats() -> stdio::Result<ZcStats> {
    Ok(current()?.driver.borrow().zc_stats())
}

struct RootWake {
    ready: AtomicBool,
    notifier: Arc<Notifier>,
}
impl Wake for RootWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        self.notifier.notify();
    }
}
struct PoolWake(Arc<Notifier>);
impl Wake for PoolWake {
    fn wake(self: Arc<Self>) {
        self.0.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify();
    }
}

pub(crate) struct Worker {
    pub(crate) driver: RefCell<Driver>,
    pub(crate) io: RefCell<io::IoState>,
    pub(crate) timers: RefCell<timer::TimerQueue>,
    pool: BufferPool,
    tasks: RefCell<TaskSet>,
    shared: Arc<WorkerShared>,
    group: Weak<Group>,
    config: Arc<RuntimeConfig>,
    events: RefCell<Vec<Event>>,
    shutting_down: std::cell::Cell<bool>,
}
impl Worker {
    fn new(
        config: Arc<RuntimeConfig>,
        index: usize,
        group: &Arc<Group>,
        backend_shared: Arc<Shared>,
    ) -> stdio::Result<Rc<Self>> {
        if let Some(cpus) = &config.affinity {
            set_affinity(cpus[index])?;
        }
        let shared = group.workers[index].clone();
        let pool = BufferPool::new(config.limits.pool)?;
        pool.set_recycle_waker(Waker::from(Arc::new(PoolWake(shared.notifier.clone()))));
        let driver = Driver::new(
            &config,
            index,
            pool.clone(),
            shared.notifier.clone(),
            backend_shared,
        )?;
        Ok(Rc::new(Self {
            driver: RefCell::new(driver),
            io: RefCell::new(io::IoState::new(&config.limits)),
            timers: RefCell::new(timer::TimerQueue::new(config.limits.max_operations)),
            pool,
            tasks: RefCell::new(TaskSet::new(config.limits.max_tasks)),
            shared,
            group: Arc::downgrade(group),
            events: RefCell::new(Vec::with_capacity(config.limits.completion_budget)),
            config,
            shutting_down: std::cell::Cell::new(false),
        }))
    }
    fn run_background(self: &Rc<Self>) -> stdio::Result<()> {
        while !self
            .group
            .upgrade()
            .is_none_or(|group| group.stopping.load(Ordering::Acquire))
        {
            if let Err(error) = self.turn(None) {
                if let Some(group) = self.group.upgrade() {
                    group.stop();
                }
                return Err(error);
            }
        }
        Ok(())
    }
    fn immediate_work(&self, root: Option<&AtomicBool>) -> bool {
        root.is_some_and(|ready| ready.load(Ordering::Acquire))
            || !self.shared.ready.is_empty()
            || !self.shared.inbox.lock().factories.is_empty()
            || self
                .group
                .upgrade()
                .is_none_or(|group| group.stopping.load(Ordering::Acquire))
    }
    fn turn(self: &Rc<Self>, root: Option<&AtomicBool>) -> stdio::Result<()> {
        for _ in 0..self.config.limits.task_budget {
            let command = self.shared.inbox.lock().factories.pop_front();
            let Some(command) = command else {
                break;
            };
            command.launch(self);
        }
        self.run_ready();
        self.timers
            .borrow_mut()
            .expire(Instant::now(), self.config.limits.completion_budget);
        self.poll_driver(Some(Duration::ZERO))?;
        self.pool.flush_recycles();
        if self.immediate_work(root) {
            return Ok(());
        }
        if !self.config.idle_spin.is_zero() {
            let now = Instant::now();
            let spin_limit = now.checked_add(self.config.idle_spin).unwrap_or(now);
            let spin_until = self
                .timers
                .borrow()
                .next_deadline()
                .map_or(spin_limit, |deadline| deadline.min(spin_limit));
            while Instant::now() < spin_until {
                self.poll_driver(Some(Duration::ZERO))?;
                if self.immediate_work(root) {
                    return Ok(());
                }
                std::hint::spin_loop();
            }
        }
        // Clear/recheck/wait: a notification before reset is represented by
        // queued work, and a notification after recheck wakes the native wait.
        self.shared.notifier.reset();
        if self.immediate_work(root) {
            return Ok(());
        }
        let timeout = self
            .timers
            .borrow()
            .next_deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        self.poll_driver(timeout)
    }
    fn run_ready(&self) {
        for _ in 0..self.config.limits.task_budget {
            let Some(id) = self.shared.ready.pop() else {
                break;
            };
            let running = self.tasks.borrow_mut().take(id);
            if let Some(mut running) = running {
                let finished = running.poll();
                let body = self.tasks.borrow_mut().finish(running, finished);
                // A future's destructor may close sockets or spawn work. Never
                // drop it while borrowing the task table or driver.
                let _ = catch_unwind(AssertUnwindSafe(|| drop(body)));
            }
        }
    }
    fn poll_driver(&self, timeout: Option<Duration>) -> stdio::Result<()> {
        let mut events = self.events.borrow_mut();
        self.driver.borrow_mut().poll(timeout, &mut events)?;
        let mut io = self.io.borrow_mut();
        let mut driver = self.driver.borrow_mut();
        for event in events.drain(..) {
            io.event(&mut driver, event);
        }
        io.flush_capacities(&mut driver);
        Ok(())
    }
    fn shutdown(self: &Rc<Self>) -> stdio::Result<()> {
        if self.shutting_down.replace(true) {
            return Ok(());
        }
        {
            let mut inbox = self.shared.inbox.lock();
            inbox.closed = true;
        }
        loop {
            let command = self.shared.inbox.lock().factories.pop_front();
            match command {
                Some(command) => drop(command),
                None => break,
            }
        }
        while !self.tasks.borrow().is_empty() {
            let body = self.tasks.borrow_mut().take_shutdown();
            let _ = catch_unwind(AssertUnwindSafe(|| {
                if let Some(body) = body {
                    body.as_ref().fail(JoinError::Cancelled);
                    drop(body);
                }
            }));
        }
        self.timers.borrow_mut().clear();
        self.io
            .borrow_mut()
            .begin_shutdown(&mut self.driver.borrow_mut());
        self.driver.borrow_mut().begin_shutdown();
        let mut first_error = None;
        loop {
            // Even an already idle backend may have synchronous cancellation
            // results waiting for publication after begin_shutdown.
            let timeout = if self.driver.borrow().is_idle() {
                Duration::ZERO
            } else {
                Duration::from_millis(10)
            };
            if let Err(error) = self.poll_driver(Some(timeout)) {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                // Backend shutdown owns native convergence even after a poll
                // error; retaining the Driver is safer than freeing live DMA/
                // kernel references. No operation slot is reused in this phase.
                self.driver.borrow_mut().begin_shutdown();
            }
            self.pool.flush_recycles();
            if self.driver.borrow().is_idle() {
                break;
            }
        }
        self.io.borrow_mut().finish_shutdown();
        self.pool.flush_recycles();
        self.shared.notifier.close();
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(unix)]
fn set_affinity(cpu: usize) -> stdio::Result<()> {
    if cpu >= std::mem::size_of::<libc::cpu_set_t>() * 8 {
        return Err(stdio::Error::new(
            stdio::ErrorKind::InvalidInput,
            "CPU affinity index exceeds native CPU set",
        ));
    }
    // SAFETY: the CPU index has been checked and the set has the native size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
            return Err(stdio::Error::last_os_error());
        }
    }
    Ok(())
}
#[cfg(windows)]
fn set_affinity(cpu: usize) -> stdio::Result<()> {
    if cpu >= usize::BITS as usize {
        return Err(stdio::Error::new(
            stdio::ErrorKind::Unsupported,
            "CPU affinity requires a processor in the current Windows processor group",
        ));
    }
    // SAFETY: pseudo thread handle and a nonzero, in-range processor mask.
    let old = unsafe {
        windows_sys::Win32::System::Threading::SetThreadAffinityMask(
            windows_sys::Win32::System::Threading::GetCurrentThread(),
            1usize << cpu,
        )
    };
    if old == 0 {
        Err(stdio::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::{block_on, poll_once};

    fn group(active: &[bool]) -> Group {
        Group {
            workers: active
                .iter()
                .map(|&active| {
                    Arc::new(WorkerShared {
                        notifier: Arc::new(Notifier::new().unwrap()),
                        ready: ArrayQueue::new(1),
                        inbox: Mutex::new(Inbox {
                            closed: false,
                            factories: VecDeque::with_capacity(1),
                        }),
                        admitted: AtomicUsize::new(0),
                        retired: AtomicUsize::new(0),
                        active: AtomicBool::new(active),
                        limit: 1,
                    })
                })
                .collect(),
            stopping: AtomicBool::new(false),
            cursor: AtomicUsize::new(0),
        }
    }

    #[test]
    fn admission_skips_inactive_candidate_and_uses_remaining_capacity() {
        let group = group(&[false, true, true]);
        let occupied = group.workers[1].admit().unwrap();
        // Candidate zero became inactive after selection. The next worker is
        // full, but the last worker must still accept exactly one factory.
        let mut join = group.enqueue(0, || async { 7 }).unwrap();
        assert_eq!(group.workers[0].admitted.load(Ordering::Acquire), 0);
        assert!(group.workers[0].inbox.lock().factories.is_empty());
        assert!(group.workers[1].inbox.lock().factories.is_empty());
        assert_eq!(group.workers[2].admitted.load(Ordering::Acquire), 1);
        assert_eq!(
            group.enqueue(0, || async {}).unwrap_err(),
            SpawnError::AtCapacity
        );

        let command = group.workers[2].inbox.lock().factories.pop_front().unwrap();
        drop(command);
        assert_eq!(block_on(poll_once(&mut join)), Some(Err(JoinError::Cancelled)));
        assert_eq!(group.workers[2].admitted.load(Ordering::Acquire), 0);
        assert!(group.workers[2].inbox.lock().factories.is_empty());
        drop(occupied);
        assert_eq!(group.workers[1].admitted.load(Ordering::Acquire), 0);
    }

    #[test]
    fn admission_distinguishes_inactive_full_and_stopped_workers() {
        let group = group(&[false, false]);
        assert_eq!(
            group.enqueue(0, || async {}).unwrap_err(),
            SpawnError::NotRunning
        );
        group.workers[1].active.store(true, Ordering::Release);
        let occupied = group.workers[1].admit().unwrap();
        assert_eq!(
            group.enqueue(0, || async {}).unwrap_err(),
            SpawnError::AtCapacity
        );
        group.stop();
        assert_eq!(
            group.enqueue(0, || async {}).unwrap_err(),
            SpawnError::ShuttingDown
        );
        drop(occupied);
    }
}
