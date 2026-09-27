use super::{AbortHandle, Handle, JoinError, JoinHandle, SpawnError};
use futures_util::{Stream, stream::FuturesUnordered};
use std::{
    fmt,
    future::{Future, poll_fn},
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

/// Bounded ownership of child tasks, yielding results in completion order.
///
/// Completed but unjoined children still consume capacity. Dropping the group
/// requests cancellation; joining acknowledges destruction of child futures and
/// their captures, not resources moved into results or native I/O retirement.
/// Cancelling a `join_next` or `shutdown` wait never detaches children.
pub struct TaskGroup<T> {
    tasks: FuturesUnordered<JoinHandle<T>>,
    capacity: usize,
    closing: bool,
    shutdown_error: Option<JoinError>,
}

impl<T> TaskGroup<T> {
    pub fn new(capacity: usize) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "task group capacity must be nonzero",
            ));
        }
        Ok(Self {
            tasks: FuturesUnordered::new(),
            capacity,
            closing: false,
            shutdown_error: None,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity
    }

    fn admit(&self) -> Result<(), SpawnError> {
        if self.closing {
            Err(SpawnError::ShuttingDown)
        } else if self.is_full() {
            Err(SpawnError::AtCapacity)
        } else {
            Ok(())
        }
    }

    /// Place a Send factory automatically; its future is created on the chosen
    /// worker and need not be Send. Results crossing workers must be Send.
    pub fn spawn<F, Fut>(&mut self, factory: F) -> Result<AbortHandle, SpawnError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.admit()?;
        let handle = super::spawn(factory)?;
        Ok(self.insert(handle))
    }

    /// Use a particular runtime's automatic placement, including outside a
    /// currently entered runtime when it has active background workers.
    pub fn spawn_on<F, Fut>(
        &mut self,
        runtime: &Handle,
        factory: F,
    ) -> Result<AbortHandle, SpawnError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        self.admit()?;
        let handle = runtime.spawn(factory)?;
        Ok(self.insert(handle))
    }

    /// Own a future on the current worker. Both future and result may be !Send.
    pub fn spawn_local<F>(&mut self, future: F) -> Result<AbortHandle, SpawnError>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        self.admit()?;
        let handle = super::spawn_local(future)?;
        Ok(self.insert(handle))
    }

    fn insert(&mut self, handle: JoinHandle<T>) -> AbortHandle {
        let abort = handle.abort_handle();
        self.tasks.push(handle);
        abort
    }

    pub(crate) fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, JoinError>>> {
        let result = Pin::new(&mut self.tasks).poll_next(cx);
        if self.closing && matches!(&result, Poll::Ready(Some(Err(JoinError::Panicked)))) {
            // Preserve a failure even if a shutdown waiter is cancelled after
            // observing this child but before all other children are joined.
            self.shutdown_error = Some(JoinError::Panicked);
        }
        result
    }

    /// Wait for the next child result in completion order.
    ///
    /// An empty group returns `None` immediately, without waiting for future
    /// children or closing admission. Unless [`Self::shutdown`] has closed the
    /// group, callers may spawn again and create a new `join_next` future.
    /// Dynamic owners should wait for external admission or stop notifications
    /// while empty, rather than busy-looping on `None`.
    ///
    /// Each call creates a new wait. Once that future returns `Ready`, do not
    /// poll it again. Ordinary futures do not promise post-completion polling;
    /// explicitly fused or resettable types have their own contracts, and not
    /// every future is required to panic when misused.
    ///
    /// Cancelling this wait leaves all unjoined children owned by the group.
    /// [`Self::abort_all`] and dropping the group only request cancellation;
    /// receiving a child result proves its future and captures have been
    /// destroyed. Resources moved into the result remain owned by its receiver,
    /// and native I/O references may retire later, independently of task joining.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        poll_fn(|cx| self.poll_join_next(cx)).await
    }

    /// Request cancellation without consuming results or closing admission.
    pub fn abort_all(&self) {
        for task in self.tasks.iter() {
            task.abort();
        }
    }

    /// Permanently close admission, abort all children, and wait for cleanup.
    /// Expected cancellation is successful (even when cancelled cleanup panics).
    /// Execution and normal-completion destructor panics are reported only after
    /// every child is joined. A cancelled wait may safely be retried.
    pub async fn shutdown(&mut self) -> Result<(), JoinError> {
        self.closing = true;
        self.abort_all();
        while let Some(result) = self.join_next().await {
            if catch_unwind(AssertUnwindSafe(|| drop(result))).is_err() {
                self.shutdown_error = Some(JoinError::Panicked);
            }
        }
        self.shutdown_error.map_or(Ok(()), Err)
    }
}

impl<T> Drop for TaskGroup<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

impl<T> fmt::Debug for TaskGroup<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskGroup")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .field("closing", &self.closing)
            .finish()
    }
}
