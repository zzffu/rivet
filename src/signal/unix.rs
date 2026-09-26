use super::{FIRST, SECOND};
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
    },
    thread,
};

const SIGNALS: [libc::c_int; 2] = [libc::SIGINT, libc::SIGTERM];

// A callback increments READERS before loading ROUTE. All route accesses and
// reader counts use SeqCst: after disarming, a reader either sees null or is
// included in the drain before any descriptor may be closed/reused. These
// atomics are lock-free on every supported Unix target (x86_64/aarch64).
static ROUTE: AtomicPtr<State> = AtomicPtr::new(ptr::null_mut());
static READERS: AtomicUsize = AtomicUsize::new(0);

pub(super) struct State {
    read: OwnedFd,
    write: OwnedFd,
    pending: AtomicU8,
    stopping: AtomicBool,
}

impl State {
    fn new() -> io::Result<Self> {
        let mut descriptors = [-1; 2];
        if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            // pipe2 initialized both descriptors and ownership is transferred once.
            read: unsafe { OwnedFd::from_raw_fd(descriptors[0]) },
            write: unsafe { OwnedFd::from_raw_fd(descriptors[1]) },
            pending: AtomicU8::new(0),
            stopping: AtomicBool::new(false),
        })
    }

    fn wake(&self) {
        let byte = 1u8;
        loop {
            let result =
                unsafe { libc::write(self.write.as_raw_fd(), (&byte as *const u8).cast(), 1) };
            // A full pipe is already readable; the pending bits retain both
            // signal kinds. EINTR is the only error requiring a retry.
            if result >= 0 || unsafe { *errno() } != libc::EINTR {
                return;
            }
        }
    }

    pub(super) fn wait(&self) -> io::Result<()> {
        let mut descriptor = libc::pollfd {
            fd: self.read.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            if unsafe { libc::poll(&mut descriptor, 1, -1) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if descriptor.revents & libc::POLLIN == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "signal wake pipe closed",
                ));
            }
            let mut bytes = [0u8; 64];
            let count = unsafe {
                libc::read(
                    self.read.as_raw_fd(),
                    bytes.as_mut_ptr().cast(),
                    bytes.len(),
                )
            };
            if count > 0 {
                return Ok(());
            }
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "signal wake pipe closed",
                ));
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            ) {
                return Err(error);
            }
        }
    }

    pub(super) fn stopped(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    pub(super) fn take_pending(&self) -> u8 {
        self.pending.swap(0, Ordering::AcqRel)
    }
}

unsafe fn errno() -> *mut libc::c_int {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::__errno_location()
    }
    #[cfg(target_os = "android")]
    unsafe {
        libc::__errno()
    }
}

extern "C" fn handler(signal: libc::c_int) {
    // POSIX permits the thread-local errno access and write here. Preserve the
    // interrupted code's errno; no allocation, mutex, Waker, or user code runs.
    let saved_errno = unsafe { *errno() };
    READERS.fetch_add(1, Ordering::SeqCst);
    let state = ROUTE.load(Ordering::SeqCst);
    if !state.is_null() {
        // READERS prevents reclamation until after the final use of this pointer.
        let state = unsafe { &*state };
        let bit = if signal == libc::SIGINT {
            FIRST
        } else {
            SECOND
        };
        state.pending.fetch_or(bit, Ordering::Release);
        state.wake();
    }
    READERS.fetch_sub(1, Ordering::SeqCst);
    unsafe { *errno() = saved_errno };
}

pub(super) struct Registration {
    state: Arc<State>,
    previous: [libc::sigaction; 2],
    installed: [bool; 2],
    active: bool,
}

fn disposition(signal: libc::c_int) -> io::Result<libc::sigaction> {
    let mut action = unsafe { std::mem::zeroed() };
    if unsafe { libc::sigaction(signal, ptr::null(), &mut action) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(action)
}

fn available(action: &libc::sigaction) -> bool {
    action.sa_sigaction == libc::SIG_DFL || action.sa_sigaction == libc::SIG_IGN
}

fn occupied() -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        "SIGINT or SIGTERM already has a custom handler",
    )
}

impl Registration {
    pub(super) fn new() -> io::Result<Self> {
        let previous = [disposition(SIGNALS[0])?, disposition(SIGNALS[1])?];
        if previous.iter().any(|action| !available(action)) {
            return Err(occupied());
        }
        let mut registration = Self {
            state: Arc::new(State::new()?),
            previous,
            installed: [false; 2],
            active: true,
        };
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = handler as *const () as usize;
        action.sa_flags = libc::SA_RESTART as _;
        unsafe {
            libc::sigemptyset(&mut action.sa_mask);
            libc::sigaddset(&mut action.sa_mask, libc::SIGINT);
            libc::sigaddset(&mut action.sa_mask, libc::SIGTERM);
        }
        ROUTE.store(
            Arc::as_ptr(&registration.state).cast_mut(),
            Ordering::SeqCst,
        );
        for (index, signal) in SIGNALS.into_iter().enumerate() {
            if unsafe { libc::sigaction(signal, &action, &mut registration.previous[index]) } != 0 {
                return Err(io::Error::last_os_error());
            }
            registration.installed[index] = true;
            // Also inspect the action actually replaced. If a foreign installer
            // raced the earlier query, rollback restores it rather than retaining
            // ownership. Hosts must serialize native handler changes with us.
            if !available(&registration.previous[index]) {
                return Err(occupied());
            }
        }
        Ok(registration)
    }

    pub(super) fn state(&self) -> Arc<State> {
        Arc::clone(&self.state)
    }

    pub(super) fn stop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        for (index, signal) in SIGNALS.into_iter().enumerate() {
            if self.installed[index]
                && let Ok(current) = disposition(signal)
                && current.sa_sigaction == handler as *const () as usize
            {
                // Only restore dispositions we still own. In particular, do not
                // reset an independently installed later handler to SIG_DFL.
                unsafe { libc::sigaction(signal, &self.previous[index], ptr::null_mut()) };
            }
        }
        ROUTE.store(ptr::null_mut(), Ordering::SeqCst);
        while READERS.load(Ordering::SeqCst) != 0 {
            thread::yield_now();
        }
        self.state.stopping.store(true, Ordering::Release);
        self.state.wake();
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.stop();
    }
}
