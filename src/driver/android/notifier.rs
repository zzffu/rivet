use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    sync::atomic::{AtomicBool, Ordering},
};

/// The fd stays owned until the final Arc is dropped. Closing the notifier is a
/// logical transition, so a racing sender can never write to a recycled fd.
pub(crate) struct Notifier {
    fd: OwnedFd,
    pending: AtomicBool,
    closed: AtomicBool,
}

impl Notifier {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
            pending: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    pub fn notify(&self) {
        if !self.closed.load(Ordering::Acquire) && !self.pending.swap(true, Ordering::AcqRel) {
            self.signal();
        }
    }

    pub fn reset(&self) {
        // Drain before clearing: notifications coalesced during the drain are
        // covered by the runtime's mandatory queue recheck after this method.
        self.drain();
        self.pending.store(false, Ordering::Release);
    }

    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.signal();
        }
    }

    pub(super) fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    pub(super) fn drain(&self) {
        let mut value: u64 = 0;
        loop {
            let result =
                unsafe { libc::read(self.fd(), (&mut value as *mut u64).cast(), size_of::<u64>()) };
            if result >= 0 {
                continue;
            }
            if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }

    fn signal(&self) {
        let value: u64 = 1;
        loop {
            let result =
                unsafe { libc::write(self.fd(), (&value as *const u64).cast(), size_of::<u64>()) };
            if result >= 0 {
                break;
            }
            // EAGAIN means the counter is already readable. Other errors cannot
            // arise for our privately owned eventfd except interruption.
            if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
}
