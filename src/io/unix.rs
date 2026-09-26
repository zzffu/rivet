use super::{ImportError, Table, closed};
use event_listener::Event;
use parking_lot::Mutex;
use std::{
    fmt,
    future::Future,
    io,
    marker::PhantomData,
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd},
    rc::Rc,
    sync::Arc,
    thread::{self, JoinHandle},
};

/// The readiness lane cleared when `try_io` returns `WouldBlock`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Interest {
    Readable,
    Writable,
}
impl Interest {
    fn index(self) -> usize {
        match self {
            Self::Readable => 0,
            Self::Writable => 1,
        }
    }
    fn bit(self) -> u8 {
        1 << self.index()
    }
}

#[derive(Clone, Copy)]
enum Failure {
    Closed,
    Native(i32),
}
impl Failure {
    fn error(self) -> io::Error {
        match self {
            Self::Closed => closed(),
            Self::Native(error) => io::Error::from_raw_os_error(error),
        }
    }
}
struct Readiness {
    bits: u8,
    epochs: [u64; 2],
    failure: Option<Failure>,
}
struct Entry {
    state: Mutex<Readiness>,
    changed: [Event; 2],
}
impl Entry {
    fn new() -> Self {
        Self {
            state: Mutex::new(Readiness {
                bits: 0,
                epochs: [0; 2],
                failure: None,
            }),
            changed: [Event::new(), Event::new()],
        }
    }
    fn observe(&self, bits: u8) {
        {
            let mut state = self.state.lock();
            if state.failure.is_some() {
                return;
            }
            state.bits |= bits;
            for (index, epoch) in state.epochs.iter_mut().enumerate() {
                if bits & (1 << index) != 0 {
                    *epoch = epoch.wrapping_add(1);
                }
            }
        }
        for (index, changed) in self.changed.iter().enumerate() {
            if bits & (1 << index) != 0 {
                changed.notify(usize::MAX);
            }
        }
    }
    fn fail(&self, failure: Failure) {
        self.state.lock().failure = Some(failure);
        for changed in &self.changed {
            changed.notify(usize::MAX);
        }
    }
    async fn wait(self: Arc<Self>, interest: Interest) -> io::Result<()> {
        loop {
            // A stack listener avoids allocating per readiness wait. Installing
            // it before inspecting state closes the notification race.
            event_listener::listener!(self.changed[interest.index()] => listener);
            {
                let state = self.state.lock();
                if let Some(failure) = state.failure {
                    return Err(failure.error());
                }
                if state.bits & interest.bit() != 0 {
                    return Ok(());
                }
            }
            listener.await;
        }
    }
}

const WAKE: u64 = u64::MAX;
struct Poller {
    epoll: OwnedFd,
    wake: OwnedFd,
}
impl Poller {
    fn new() -> io::Result<Self> {
        let epoll = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epoll < 0 {
            return Err(io::Error::last_os_error());
        }
        let epoll = unsafe { OwnedFd::from_raw_fd(epoll) };
        let wake = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake < 0 {
            return Err(io::Error::last_os_error());
        }
        let wake = unsafe { OwnedFd::from_raw_fd(wake) };
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: WAKE,
        };
        if unsafe {
            libc::epoll_ctl(
                epoll.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                wake.as_raw_fd(),
                &mut event,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { epoll, wake })
    }
    fn add(&self, fd: RawFd, key: u64) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP | libc::EPOLLET) as u32,
            u64: key,
        };
        if unsafe { libc::epoll_ctl(self.epoll.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut event) }
            < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    fn remove(&self, fd: RawFd) -> io::Result<()> {
        if unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                libc::EPOLL_CTL_DEL,
                fd,
                std::ptr::null_mut(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    fn wake(&self) {
        let value = 1u64;
        loop {
            let result = unsafe {
                libc::write(
                    self.wake.as_raw_fd(),
                    (&value as *const u64).cast(),
                    size_of::<u64>(),
                )
            };
            if result >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                break;
            }
        }
    }
}
struct Registration {
    fd: RawFd,
    entry: Arc<Entry>,
}
struct State {
    table: Option<Table<Registration>>,
    failure: Option<i32>,
    poller: Option<Arc<Poller>>,
    helper: Option<JoinHandle<()>>,
}
struct Shared {
    state: Mutex<State>,
}

pub(crate) struct Registry {
    shared: Arc<Shared>,
    // Serializes native teardown with import/drop and concurrent shutdowns.
    lifecycle: Mutex<()>,
}
impl Registry {
    pub(crate) fn validate_capacity(capacity: usize) -> io::Result<()> {
        Table::<Registration>::validate_capacity(capacity)
    }
    pub(crate) fn new(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    table: Some(Table::new(capacity)?),
                    failure: None,
                    poller: None,
                    helper: None,
                }),
            }),
            lifecycle: Mutex::new(()),
        })
    }
    fn register(&self, fd: RawFd) -> io::Result<(u64, Arc<Entry>)> {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if flags & libc::O_NONBLOCK == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AsyncFd requires an already nonblocking descriptor",
            ));
        }
        let _lifecycle = self.lifecycle.lock();
        let mut state = self.shared.state.lock();
        let key = state.table.as_ref().ok_or_else(closed)?.vacant_key()?;
        if let Some(error) = state.failure {
            return Err(io::Error::from_raw_os_error(error));
        }
        if state.poller.is_none() {
            let poller = Arc::new(Poller::new()?);
            let shared = self.shared.clone();
            let thread_poller = poller.clone();
            let helper = thread::Builder::new()
                .name("rivet-native-io".into())
                .spawn(move || run(shared, thread_poller))?;
            state.poller = Some(poller);
            state.helper = Some(helper);
        }
        // epoll rejects regular files and other non-pollable objects. Nothing
        // changes the imported open-file description, including on failure.
        state.poller.as_ref().unwrap().add(fd, key)?;
        let entry = Arc::new(Entry::new());
        state.table.as_mut().unwrap().insert(
            key,
            Registration {
                fd,
                entry: entry.clone(),
            },
        );
        Ok((key, entry))
    }
    fn unregister(&self, key: u64) -> io::Result<()> {
        let _lifecycle = self.lifecycle.lock();
        let (registration, poller) = {
            let mut state = self.shared.state.lock();
            let registration = state.table.as_mut().and_then(|table| table.remove(key));
            (registration, state.poller.clone())
        };
        let Some(registration) = registration else {
            return Ok(());
        };
        let result = poller.unwrap().remove(registration.fd);
        registration.entry.fail(Failure::Closed);
        result
    }
    pub(crate) fn shutdown(&self) {
        let _lifecycle = self.lifecycle.lock();
        let (table, poller, helper) = {
            let mut state = self.shared.state.lock();
            (state.table.take(), state.poller.take(), state.helper.take())
        };
        if let Some(table) = table {
            for registration in table.into_values() {
                if let Some(poller) = &poller {
                    let _ = poller.remove(registration.fd);
                }
                registration.entry.fail(Failure::Closed);
            }
        }
        if let Some(poller) = poller {
            poller.wake();
            if let Some(helper) = helper {
                let _ = helper.join();
            }
        }
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run(shared: Arc<Shared>, poller: Arc<Poller>) {
    let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
    loop {
        let count = unsafe {
            libc::epoll_wait(
                poller.epoll.as_raw_fd(),
                events.as_mut_ptr(),
                events.len() as i32,
                -1,
            )
        };
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            let error = error.raw_os_error().unwrap_or(libc::EIO);
            let mut state = shared.state.lock();
            state.failure = Some(error);
            if let Some(table) = &state.table {
                for registration in table.values() {
                    registration.entry.fail(Failure::Native(error));
                }
            }
            return;
        }
        for event in &events[..count as usize] {
            let key = event.u64;
            if key == WAKE {
                return;
            }
            let entry = {
                let state = shared.state.lock();
                state
                    .table
                    .as_ref()
                    .and_then(|table| table.get(key))
                    .map(|registration| registration.entry.clone())
            };
            if let Some(entry) = entry {
                let flags = event.events as i32;
                let mut bits = 0;
                if flags & (libc::EPOLLIN | libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) != 0
                {
                    bits |= Interest::Readable.bit();
                }
                if flags & (libc::EPOLLOUT | libc::EPOLLHUP | libc::EPOLLERR) != 0 {
                    bits |= Interest::Writable.bit();
                }
                entry.observe(bits);
            }
        }
    }
}

/// A local, owned nonblocking descriptor registered with the current runtime.
///
/// Pipes, eventfds, devices supporting epoll, and nonblocking socketpairs can be
/// imported. Regular files are rejected. No raw-descriptor accessor is exposed.
/// The caller must not close, retain, or change the blocking mode of the borrowed
/// descriptor passed to `try_io`, nor change it through a duplicate descriptor.
/// Readiness can be spurious; retry `WouldBlock` by waiting again.
pub struct AsyncFd {
    fd: OwnedFd,
    registry: Arc<Registry>,
    entry: Arc<Entry>,
    key: Option<u64>,
    local: PhantomData<Rc<()>>,
}
impl AsyncFd {
    pub fn import(fd: OwnedFd) -> Result<Self, ImportError<OwnedFd>> {
        let registry = match crate::runtime::io_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(ImportError {
                    error,
                    resource: fd,
                });
            }
        };
        match registry.register(fd.as_raw_fd()) {
            Ok((key, entry)) => Ok(Self {
                fd,
                registry,
                entry,
                key: Some(key),
                local: PhantomData,
            }),
            Err(error) => Err(ImportError {
                error,
                resource: fd,
            }),
        }
    }
    /// Wait without consuming cached readiness. Dropping this future does not
    /// lose a notification. Closing the object also wakes already-created waits.
    pub fn readable(&self) -> impl Future<Output = io::Result<()>> + use<> {
        self.entry.clone().wait(Interest::Readable)
    }
    pub fn writable(&self) -> impl Future<Output = io::Result<()>> + use<> {
        self.entry.clone().wait(Interest::Writable)
    }
    /// Perform one synchronous, nonblocking operation with a temporary borrow.
    /// Only `WouldBlock` clears the selected lane. Readiness arriving during the
    /// operation is preserved, even if the operation itself observed no data.
    pub fn try_io<T>(
        &self,
        interest: Interest,
        operation: impl for<'fd> FnOnce(BorrowedFd<'fd>) -> io::Result<T>,
    ) -> io::Result<T> {
        let epoch = {
            let state = self.entry.state.lock();
            if let Some(failure) = state.failure {
                return Err(failure.error());
            }
            state.epochs[interest.index()]
        };
        let result = operation(self.fd.as_fd());
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
        {
            let mut state = self.entry.state.lock();
            if state.epochs[interest.index()] == epoch {
                state.bits &= !interest.bit();
            }
        }
        result
    }
    /// Unregister before closing the owned descriptor, reporting native errors.
    pub fn close(mut self) -> io::Result<()> {
        self.registry.unregister(self.key.take().unwrap())
    }
}
impl Drop for AsyncFd {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let _ = self.registry.unregister(key);
        }
    }
}
impl fmt::Debug for AsyncFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncFd").finish_non_exhaustive()
    }
}
