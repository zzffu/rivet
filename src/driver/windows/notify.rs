use std::{
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    ptr,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
};
use windows_sys::Win32::{
    Foundation::{GetLastError, HANDLE, INVALID_HANDLE_VALUE},
    System::IO::{CreateIoCompletionPort, PostQueuedCompletionStatus},
};

pub(super) const WAKE_KEY: usize = 1;
pub(super) const RIO_KEY: usize = 2;
pub(super) const SOCKET_KEY: usize = 3;

/// The handle stays open until the last Arc disappears, including concurrent
/// wake calls. `close` disables producers; it never races CloseHandle with one.
pub(crate) struct Notifier {
    port: OwnedHandle,
    pending: AtomicBool,
    closed: AtomicBool,
    error: AtomicU32,
}

impl Notifier {
    pub fn new() -> io::Result<Self> {
        let handle = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            port: unsafe { OwnedHandle::from_raw_handle(handle.cast()) },
            pending: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            error: AtomicU32::new(0),
        })
    }

    pub fn notify(&self) {
        if self.closed.load(Ordering::Acquire) || self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        if unsafe { PostQueuedCompletionStatus(self.handle(), 0, WAKE_KEY, ptr::null()) } == 0 {
            self.error
                .store(unsafe { GetLastError() }, Ordering::Release);
            self.pending.store(false, Ordering::Release);
        }
    }

    /// The executor rechecks its queues after this release and before waiting.
    /// An old IOCP packet is harmless; a producer after reset posts a new one.
    pub fn reset(&self) {
        self.pending.store(false, Ordering::Release);
    }

    pub fn close(&self) {
        self.notify();
        self.closed.store(true, Ordering::Release);
    }

    pub(super) fn handle(&self) -> HANDLE {
        self.port.as_raw_handle().cast()
    }

    pub(super) fn check(&self) -> io::Result<()> {
        let error = self.error.swap(0, Ordering::AcqRel);
        if error == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(error as i32))
        }
    }
}
