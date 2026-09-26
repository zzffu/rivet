use super::{FIRST, SECOND};
use std::{
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
    },
    thread,
};
use windows_sys::Win32::{
    Foundation::WAIT_OBJECT_0,
    System::{
        Console::{CTRL_BREAK_EVENT, CTRL_C_EVENT, SetConsoleCtrlHandler},
        Threading::{CreateEventW, INFINITE, SetEvent, WaitForSingleObject},
    },
};

// A callback enters READERS before loading ROUTE. SeqCst ordering ensures that
// after disarming it either sees null or is counted before the event can close.
// The storage is static, so even a late OS callback can safely enter the gate.
static ROUTE: AtomicPtr<State> = AtomicPtr::new(ptr::null_mut());
static READERS: AtomicUsize = AtomicUsize::new(0);

pub(super) struct State {
    event: OwnedHandle,
    pending: AtomicU8,
    stopping: AtomicBool,
}

impl State {
    fn new() -> io::Result<Self> {
        let event = unsafe { CreateEventW(ptr::null(), 0, 0, ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            event: unsafe { OwnedHandle::from_raw_handle(event.cast()) },
            pending: AtomicU8::new(0),
            stopping: AtomicBool::new(false),
        })
    }

    fn wake(&self) {
        unsafe { SetEvent(self.event.as_raw_handle().cast()) };
    }

    pub(super) fn wait(&self) -> io::Result<()> {
        if unsafe { WaitForSingleObject(self.event.as_raw_handle().cast(), INFINITE) }
            != WAIT_OBJECT_0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn stopped(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    pub(super) fn take_pending(&self) -> u8 {
        self.pending.swap(0, Ordering::AcqRel)
    }
}

unsafe extern "system" fn handler(event: u32) -> i32 {
    let bit = match event {
        CTRL_C_EVENT => FIRST,
        CTRL_BREAK_EVENT => SECOND,
        _ => return 0,
    };
    READERS.fetch_add(1, Ordering::SeqCst);
    let state = ROUTE.load(Ordering::SeqCst);
    let handled = if state.is_null() {
        // If removal raced an already queued callback, continue the host's
        // handler chain instead of swallowing its default exit behavior.
        0
    } else {
        // READERS protects the pointed-to state and event through wake().
        let state = unsafe { &*state };
        state.pending.fetch_or(bit, Ordering::Release);
        state.wake();
        1
    };
    READERS.fetch_sub(1, Ordering::SeqCst);
    handled
}

pub(super) struct Registration {
    state: Arc<State>,
    active: bool,
}

impl Registration {
    pub(super) fn new() -> io::Result<Self> {
        let mut registration = Self {
            state: Arc::new(State::new()?),
            active: true,
        };
        ROUTE.store(
            Arc::as_ptr(&registration.state).cast_mut(),
            Ordering::SeqCst,
        );
        if unsafe { SetConsoleCtrlHandler(Some(handler), 1) } == 0 {
            let error = io::Error::last_os_error();
            registration.stop();
            return Err(error);
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
        // Removes only our entry. Other registrations and the console's default
        // handler remain untouched, including any host Ctrl+C ignore setting.
        unsafe { SetConsoleCtrlHandler(Some(handler), 0) };
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
