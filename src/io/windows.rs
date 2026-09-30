use super::{ImportError, Table, closed};
use crate::sync::notification::Event;
use parking_lot::Mutex;
use std::{
    ffi::c_void,
    fmt,
    future::Future,
    io,
    marker::PhantomData,
    os::windows::io::{AsRawHandle, OwnedHandle, RawHandle},
    rc::Rc,
    sync::Arc,
};
use windows_sys::Win32::{
    Foundation::{HANDLE, RtlNtStatusToDosError, WAIT_OBJECT_0},
    System::Threading::{
        CloseThreadpoolWait, CreateThreadpoolWait, DisassociateCurrentThreadFromCallback,
        PTP_CALLBACK_INSTANCE, PTP_WAIT, SYNCHRONIZATION_SYNCHRONIZE, SetThreadpoolWait,
        WaitForThreadpoolWaitCallbacks,
    },
};

#[repr(C)]
struct ObjectBasicInformation {
    attributes: u32,
    granted_access: u32,
    handle_count: u32,
    pointer_count: u32,
    reserved: [u32; 10],
}
#[repr(C)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: *const u16,
}
#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtQueryObject(
        handle: HANDLE,
        class: u32,
        information: *mut c_void,
        length: u32,
        returned: *mut u32,
    ) -> i32;
}
fn nt_result(status: i32) -> io::Result<()> {
    if status < 0 {
        Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(status) } as i32,
        ))
    } else {
        Ok(())
    }
}
fn validate(handle: RawHandle) -> io::Result<()> {
    // A zero-time wait would consume an auto-reset event or a semaphore permit.
    // Query type and access instead, before installing any native wait.
    let mut basic = ObjectBasicInformation {
        attributes: 0,
        granted_access: 0,
        handle_count: 0,
        pointer_count: 0,
        reserved: [0; 10],
    };
    nt_result(unsafe {
        NtQueryObject(
            handle,
            0,
            (&mut basic as *mut ObjectBasicInformation).cast(),
            size_of::<ObjectBasicInformation>() as u32,
            std::ptr::null_mut(),
        )
    })?;
    if basic.granted_access & SYNCHRONIZATION_SYNCHRONIZE == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "AsyncHandle requires SYNCHRONIZE access",
        ));
    }
    // OBJECT_TYPE_INFORMATION begins with UNICODE_STRING. Native waitable type
    // names and their fixed information fit in this aligned stack buffer.
    let mut storage = [0usize; 128];
    nt_result(unsafe {
        NtQueryObject(
            handle,
            2,
            storage.as_mut_ptr().cast(),
            size_of_val(&storage) as u32,
            std::ptr::null_mut(),
        )
    })?;
    let name = unsafe { &*storage.as_ptr().cast::<UnicodeString>() };
    let begin = storage.as_ptr() as usize;
    let end = begin + size_of_val(&storage);
    let address = name.buffer as usize;
    if !name.length.is_multiple_of(2)
        || address < begin
        || address > end
        || usize::from(name.length) > end - address
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid native object type information",
        ));
    }
    let name = unsafe { std::slice::from_raw_parts(name.buffer, usize::from(name.length) / 2) };
    if ![
        b"Event".as_slice(),
        b"Semaphore",
        b"Timer",
        b"Process",
        b"Thread",
        b"Job",
    ]
    .iter()
    .any(|expected| {
        name.iter()
            .copied()
            .eq(expected.iter().map(|byte| u16::from(*byte)))
    }) {
        // Threadpool waits cannot own mutexes. File handles are deliberately not
        // treated as asynchronous files, even when SYNCHRONIZE is granted.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AsyncHandle requires an event, semaphore, waitable timer, process, thread, or job",
        ));
    }
    Ok(())
}

struct WaitState {
    closed: bool,
    armed: bool,
    pending: bool,
    failure: Option<u32>,
}
struct Entry {
    state: Mutex<WaitState>,
    changed: Event,
}
impl Entry {
    fn new() -> Self {
        Self {
            state: Mutex::new(WaitState {
                closed: false,
                armed: false,
                pending: false,
                failure: None,
            }),
            changed: Event::new(),
        }
    }
}
struct Callback {
    entry: Arc<Entry>,
}
unsafe extern "system" fn notified(
    instance: PTP_CALLBACK_INSTANCE,
    context: *mut c_void,
    _wait: PTP_WAIT,
    result: u32,
) {
    // Keep only an owned Entry after publishing the native result. The callback
    // is disassociated before invoking arbitrary Waker code, which may close
    // this registration (or the whole Registry) on this very callback thread.
    // Neither a Waker panic nor its payload's destructor may unwind across
    // the native callback boundary.
    crate::runtime::ignore_panic(|| {
        // SAFETY: Teardown joins callbacks before freeing this context. Clone
        // the Entry before disassociation and never access context again.
        let entry = unsafe { &*context.cast::<Callback>() }.entry.clone();
        {
            let mut state = entry.state.lock();
            if state.closed {
                return;
            }
            state.armed = false;
            if result == WAIT_OBJECT_0 {
                state.pending = true;
            } else {
                state.failure = Some(result);
            }
        }
        // SAFETY: Native state publication is finished; after disassociation we
        // touch neither the native wait, its HANDLE, nor its callback context.
        // Close can now join the native portion without self-joining user code.
        unsafe { DisassociateCurrentThreadFromCallback(instance) };
        entry.changed.notify_all();
    });
}
struct NativeWait {
    wait: PTP_WAIT,
    handle: usize,
    callback: Box<Callback>,
}
impl NativeWait {
    fn new(handle: RawHandle, entry: Arc<Entry>) -> io::Result<Self> {
        let mut callback = Box::new(Callback { entry });
        let wait = unsafe {
            CreateThreadpoolWait(
                Some(notified),
                (&mut *callback as *mut Callback).cast(),
                std::ptr::null(),
            )
        };
        if wait == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            wait,
            handle: handle as usize,
            callback,
        })
    }
    fn take_or_arm(&self) -> io::Result<bool> {
        let mut state = self.callback.entry.state.lock();
        if state.closed {
            return Err(closed());
        }
        if let Some(result) = state.failure {
            return Err(io::Error::other(format!(
                "unexpected native wait result: {result}"
            )));
        }
        if state.pending {
            state.pending = false;
            return Ok(true);
        }
        if !state.armed {
            state.armed = true;
            unsafe { SetThreadpoolWait(self.wait, self.handle as HANDLE, std::ptr::null()) };
        }
        Ok(false)
    }
    fn stop(&mut self) {
        let wait = std::mem::replace(&mut self.wait, 0);
        if wait == 0 {
            return;
        }
        self.callback.entry.state.lock().closed = true;
        unsafe {
            SetThreadpoolWait(wait, std::ptr::null_mut(), std::ptr::null());
            WaitForThreadpoolWaitCallbacks(wait, 1);
            CloseThreadpoolWait(wait);
        }
    }
}
impl Drop for NativeWait {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) struct Registry {
    table: Mutex<Option<Table<NativeWait>>>,
    lifecycle: Mutex<()>,
}
impl Registry {
    pub(crate) fn validate_capacity(capacity: usize) -> io::Result<()> {
        Table::<NativeWait>::validate_capacity(capacity)
    }
    pub(crate) fn new(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            table: Mutex::new(Some(Table::new(capacity)?)),
            lifecycle: Mutex::new(()),
        })
    }
    fn register(&self, handle: RawHandle) -> io::Result<(u64, Arc<Entry>)> {
        validate(handle)?;
        let _lifecycle = self.lifecycle.lock();
        let mut table = self.table.lock();
        let table = table.as_mut().ok_or_else(closed)?;
        let key = table.vacant_key()?;
        let entry = Arc::new(Entry::new());
        let wait = NativeWait::new(handle, entry.clone())?;
        table.insert(key, wait);
        Ok((key, entry))
    }
    fn take_or_arm(&self, key: u64) -> io::Result<bool> {
        let table = self.table.lock();
        table
            .as_ref()
            .and_then(|table| table.get(key))
            .ok_or_else(closed)?
            .take_or_arm()
    }
    fn unregister(&self, key: u64) {
        let wait = {
            let _lifecycle = self.lifecycle.lock();
            let mut wait = self
                .table
                .lock()
                .as_mut()
                .and_then(|table| table.remove(key));
            if let Some(wait) = &mut wait {
                wait.stop();
            }
            wait
        };
        if let Some(wait) = wait {
            wait.callback.entry.changed.notify_all();
        }
    }
    pub(crate) fn shutdown(&self) {
        let table = {
            let _lifecycle = self.lifecycle.lock();
            let mut table = self.table.lock().take();
            // Removing the whole table first prevents rearming during teardown.
            if let Some(table) = &mut table {
                for wait in table.values_mut() {
                    wait.stop();
                }
            }
            table
        };
        // All native waits have stopped before any application callback runs.
        if let Some(table) = table {
            for wait in table.into_values() {
                wait.callback.entry.changed.notify_all_safely();
            }
        }
    }
}
impl Drop for Registry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A local, owned native waitable object registered with the current runtime.
///
/// Events (manual or auto reset), semaphores, waitable timers, processes, threads,
/// and jobs require SYNCHRONIZE access. Mutexes and file handles are rejected.
/// A successful wait consumes one observed native completion. A cancelled wait
/// leaves the native wait armed and preserves its completion for a later retry.
/// Permanently signaled objects are only rearmed when another wait is polled,
/// never in a background callback loop. Closing wakes all existing waiters.
pub struct AsyncHandle {
    _handle: OwnedHandle,
    registry: Arc<Registry>,
    entry: Arc<Entry>,
    key: Option<u64>,
    local: PhantomData<Rc<()>>,
}
impl AsyncHandle {
    pub fn import(handle: OwnedHandle) -> Result<Self, ImportError<OwnedHandle>> {
        let registry = match crate::runtime::io_registry() {
            Ok(registry) => registry,
            Err(error) => {
                return Err(ImportError {
                    error,
                    resource: handle,
                });
            }
        };
        match registry.register(handle.as_raw_handle()) {
            Ok((key, entry)) => Ok(Self {
                _handle: handle,
                registry,
                entry,
                key: Some(key),
                local: PhantomData,
            }),
            Err(error) => Err(ImportError {
                error,
                resource: handle,
            }),
        }
    }
    pub fn wait(&self) -> impl Future<Output = io::Result<()>> + use<> {
        let registry = self.registry.clone();
        let entry = self.entry.clone();
        let key = self.key.unwrap();
        async move {
            loop {
                let listener = entry.changed.listen();
                if registry.take_or_arm(key)? {
                    return Ok(());
                }
                listener.await;
            }
        }
    }
    /// Stops and joins native wait access before closing the owned handle.
    /// A Waker already being invoked may finish after close returns.
    pub fn close(mut self) -> io::Result<()> {
        self.registry.unregister(self.key.take().unwrap());
        Ok(())
    }
}
impl Drop for AsyncHandle {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.registry.unregister(key);
        }
    }
}
impl fmt::Debug for AsyncHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncHandle").finish_non_exhaustive()
    }
}

#[cfg(test)]
pub(super) fn callback_shutdown() {
    use std::{os::windows::io::FromRawHandle, pin::pin, task::Context, time::Duration};
    use windows_sys::Win32::System::Threading::{CreateEventW, SetEvent};

    let handle = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
    assert!(!handle.is_null());
    // SAFETY: CreateEventW returned a new owned HANDLE.
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    let registry = Arc::new(Registry::new(1).unwrap());
    let (key, entry) = registry.register(handle.as_raw_handle()).unwrap();
    let stopped = registry.clone();
    let (waker, completed) = super::tests::stop_waker(move || stopped.shutdown());
    let mut waiting = pin!(entry.changed.listen());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(!registry.take_or_arm(key).unwrap());
    assert_ne!(unsafe { SetEvent(handle.as_raw_handle()) }, 0);
    completed.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        registry.take_or_arm(key).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        registry
            .register(handle.as_raw_handle())
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}
