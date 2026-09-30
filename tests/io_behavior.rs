use futures_lite::future::{block_on, poll_once};
use parking_lot::Mutex;
use rivet::{Runtime, RuntimeConfig, time};
use std::{
    future::Future,
    io,
    pin::{Pin, pin},
    process::{Child, Command},
    sync::{Arc, mpsc},
    task::{Context, Wake, Waker},
    time::{Duration, Instant},
};

fn runtime(capacity: usize) -> Runtime {
    let mut config = RuntimeConfig::single_thread();
    config.max_async_io = capacity;
    config.limits.max_tasks = 16;
    config.limits.max_sockets = 16;
    config.limits.max_operations = 128;
    config.limits.max_pending_receives = 4;
    config.limits.max_pending_accepts = 4;
    config.limits.pool.bytes = 1024 * 1024;
    config.limits.pool.block_size = 16 * 1024;
    config.limits.pool.max_leases = 128;
    Runtime::new(config).unwrap()
}
struct Notice(mpsc::SyncSender<()>);
impl Wake for Notice {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.try_send(());
    }
}
fn notice() -> (Waker, mpsc::Receiver<()>) {
    let (send, receive) = mpsc::sync_channel(1);
    (Waker::from(Arc::new(Notice(send))), receive)
}
async fn ready(future: impl Future<Output = io::Result<()>>) {
    time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap()
        .unwrap();
}

type PendingWait = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

struct CancelWait {
    wait: Mutex<Option<PendingWait>>,
    completed: mpsc::SyncSender<()>,
}

impl Wake for CancelWait {
    fn wake(self: Arc<Self>) {
        let wait = self.wait.lock().take();
        drop(wait);
        let _ = self.completed.try_send(());
    }
}

fn cancel_on_wake(
    wait: impl Future<Output = io::Result<()>> + Send + 'static,
) -> (Arc<CancelWait>, mpsc::Receiver<()>) {
    let (completed, receive) = mpsc::sync_channel(1);
    let cancel = Arc::new(CancelWait {
        wait: Mutex::new(None),
        completed,
    });
    let waker = Waker::from(cancel.clone());
    let mut wait: PendingWait = Box::pin(wait);
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    *cancel.wait.lock() = Some(wait);
    (cancel, receive)
}

struct ReenterRegistry {
    action: fn(),
    owner: std::thread::ThreadId,
    completed: mpsc::SyncSender<()>,
}

impl Wake for ReenterRegistry {
    fn wake(self: Arc<Self>) {
        // The Waker itself is Send + Sync. Local objects are only reached via
        // the current thread's TLS, never transferred through the Waker.
        assert_eq!(self.owner, std::thread::current().id());
        (self.action)();
        let _ = self.completed.try_send(());
    }
}

fn reenter_on_wake(action: fn()) -> (Waker, mpsc::Receiver<()>) {
    let (completed, receive) = mpsc::sync_channel(1);
    (
        Waker::from(Arc::new(ReenterRegistry {
            action,
            owner: std::thread::current().id(),
            completed,
        })),
        receive,
    )
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn io_reentrant_child() {
    let Ok(case) = std::env::var("RIVET_IO_REENTRANT_CASE") else {
        return;
    };
    #[cfg(unix)]
    unix::exercise_reentry(&case);
    #[cfg(windows)]
    windows::exercise_reentry(&case);
}

#[test]
fn native_callbacks_and_close_allow_synchronous_waker_reentry() {
    for case in ["callback", "close", "shutdown"] {
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "io_reentrant_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("RIVET_IO_REENTRANT_CASE", case)
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success(), "native I/O child {case} failed: {status}");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "native I/O child {case} deadlocked"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use rivet::io::{AsyncFd, Interest};
    use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

    fn pipe(nonblocking: bool) -> (OwnedFd, OwnedFd) {
        let mut fds = [-1; 2];
        let flags = libc::O_CLOEXEC | if nonblocking { libc::O_NONBLOCK } else { 0 };
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), flags) }, 0);
        unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
    }
    fn read(fd: BorrowedFd<'_>, buffer: &mut [u8]) -> io::Result<usize> {
        let result =
            unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }
    fn write(fd: i32, buffer: &[u8]) -> io::Result<usize> {
        let result = unsafe { libc::write(fd, buffer.as_ptr().cast(), buffer.len()) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    #[test]
    fn pipe_readiness_survives_cancel_and_would_block_rearms() {
        let mut runtime = runtime(2);
        runtime.block_on(async {
            let (read_fd, write_fd) = pipe(true);
            let input = AsyncFd::import(read_fd).unwrap();
            let (waker, notified) = notice();
            {
                let mut cancelled = pin!(input.readable());
                assert!(
                    cancelled
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert_eq!(write(write_fd.as_raw_fd(), b"pipe-data").unwrap(), 9);
                notified.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            ready(input.readable()).await;
            let mut bytes = [0; 16];
            let count = input
                .try_io(Interest::Readable, |fd| read(fd, &mut bytes))
                .unwrap();
            assert_eq!(&bytes[..count], b"pipe-data");
            assert_eq!(
                input
                    .try_io(Interest::Readable, |fd| read(fd, &mut bytes))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert!(poll_once(input.readable()).await.is_none());
            drop(write_fd);
            ready(input.readable()).await;
            assert_eq!(
                input
                    .try_io(Interest::Readable, |fd| read(fd, &mut bytes))
                    .unwrap(),
                0
            );
        });
    }

    #[test]
    fn full_pipe_writable_wait_resumes_after_peer_drain() {
        use std::os::fd::AsFd;
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (reader, writer) = pipe(true);
            let output = AsyncFd::import(writer).unwrap();
            ready(output.writable()).await;
            let bytes = [7; 4096];
            let mut written = 0;
            loop {
                match output.try_io(Interest::Writable, |fd| write(fd.as_raw_fd(), &bytes)) {
                    Ok(count) => written += count,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => panic!("pipe write failed: {error}"),
                }
            }
            assert!(poll_once(output.writable()).await.is_none());
            let mut received = 0;
            let mut buffer = [0; 4096];
            while received < written {
                let count = read(reader.as_fd(), &mut buffer).unwrap();
                assert!(buffer[..count].iter().all(|byte| *byte == 7));
                received += count;
            }
            assert_eq!(received, written);
            ready(output.writable()).await;
            assert_eq!(
                output
                    .try_io(Interest::Writable, |fd| write(fd.as_raw_fd(), b"again"))
                    .unwrap(),
                5
            );
            let count = read(reader.as_fd(), &mut buffer).unwrap();
            assert_eq!(&buffer[..count], b"again");
        });
    }

    #[test]
    fn import_rejects_blocking_and_unpollable_fds_without_changing_them() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (reader, _writer) = pipe(false);
            let raw = reader.as_raw_fd();
            let failed = AsyncFd::import(reader).unwrap_err();
            assert_eq!(failed.error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(failed.resource.as_raw_fd(), raw);
            assert_eq!(
                unsafe { libc::fcntl(raw, libc::F_GETFL) } & libc::O_NONBLOCK,
                0
            );
            let raw = unsafe {
                libc::open(
                    c"/dev/null".as_ptr(),
                    libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC,
                )
            };
            assert!(raw >= 0);
            let failed = AsyncFd::import(unsafe { OwnedFd::from_raw_fd(raw) }).unwrap_err();
            assert_eq!(failed.error.raw_os_error(), Some(libc::EPERM));
            assert!(unsafe { libc::fcntl(failed.resource.as_raw_fd(), libc::F_GETFD) } >= 0);
        });
    }

    #[test]
    fn capacity_returns_ownership_and_close_wakes_old_registration() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (reader, _writer) = pipe(true);
            let first = AsyncFd::import(reader).unwrap();
            let old_wait = first.readable();
            let (reader, writer) = pipe(true);
            let raw = reader.as_raw_fd();
            let failed = AsyncFd::import(reader).unwrap_err();
            assert_eq!(failed.error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(failed.resource.as_raw_fd(), raw);
            first.close().unwrap();
            let second = AsyncFd::import(failed.resource).unwrap();
            assert_eq!(
                old_wait.await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert!(poll_once(second.readable()).await.is_none());
            write(writer.as_raw_fd(), b"new-slot").unwrap();
            ready(second.readable()).await;
            let mut buffer = [0; 8];
            assert_eq!(
                second
                    .try_io(Interest::Readable, |fd| read(fd, &mut buffer))
                    .unwrap(),
                8
            );
            assert_eq!(&buffer, b"new-slot");
        });
    }

    #[test]
    fn queued_events_never_complete_a_reused_registration() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            for _ in 0..32 {
                let (reader, writer) = pipe(true);
                let old = AsyncFd::import(reader).unwrap();
                let stale = old.readable();
                write(writer.as_raw_fd(), b"old").unwrap();
                old.close().unwrap();
                drop(writer);
                let (reader, writer) = pipe(true);
                let new = AsyncFd::import(reader).unwrap();
                assert_eq!(stale.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
                assert!(
                    time::timeout(Duration::from_millis(1), new.readable())
                        .await
                        .is_err()
                );
                write(writer.as_raw_fd(), b"new").unwrap();
                ready(new.readable()).await;
                let mut bytes = [0; 3];
                assert_eq!(
                    new.try_io(Interest::Readable, |fd| read(fd, &mut bytes))
                        .unwrap(),
                    3
                );
                assert_eq!(&bytes, b"new");
            }
        });
    }

    #[test]
    fn runtime_shutdown_wakes_wait_and_revokes_try_io() {
        let mut runtime = runtime(1);
        let (reader, _writer) = pipe(true);
        let object = runtime.block_on(async { AsyncFd::import(reader).unwrap() });
        let (waker, notified) = notice();
        let mut wait = pin!(object.readable());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(runtime);
        notified.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            block_on(wait).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            object
                .try_io(Interest::Readable, |_| Ok(()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    thread_local! {
        #[cfg_attr(
            target_os = "android",
            allow(
                clippy::missing_const_for_thread_local,
                reason = "Already const; Android std TLS false positive (rust-lang/rust-clippy#13422)."
            )
        )]
        static OTHER: std::cell::RefCell<Option<AsyncFd>> = const { std::cell::RefCell::new(None) };
    }

    fn drop_other() {
        let other = OTHER.with(|slot| slot.borrow_mut().take());
        other.unwrap().close().unwrap();
    }

    fn replace_other() {
        drop_other();
        let (reader, _writer) = pipe(true);
        AsyncFd::import(reader).unwrap().close().unwrap();
    }

    pub(super) fn exercise_reentry(case: &str) {
        let mut runtime = runtime(2);
        if case == "callback" {
            runtime.block_on(async {
                let (reader, writer) = pipe(true);
                let object = AsyncFd::import(reader).unwrap();
                let (cancel, completed) = cancel_on_wake(object.readable());
                write(writer.as_raw_fd(), b"retained").unwrap();
                completed.recv_timeout(Duration::from_secs(5)).unwrap();
                assert!(cancel.wait.lock().is_none());
                ready(object.readable()).await;
                let mut bytes = [0; 8];
                assert_eq!(
                    object
                        .try_io(Interest::Readable, |fd| read(fd, &mut bytes))
                        .unwrap(),
                    8
                );
                assert_eq!(&bytes, b"retained");
                object.close().unwrap();
            });
            return;
        }
        let (object, mut waiting, completed, _writer) = runtime.block_on(async {
            let (reader, writer) = pipe(true);
            let object = AsyncFd::import(reader).unwrap();
            let (other, _other_writer) = pipe(true);
            OTHER.with(|slot| *slot.borrow_mut() = Some(AsyncFd::import(other).unwrap()));
            let (waker, completed) = reenter_on_wake(if case == "close" {
                replace_other
            } else {
                drop_other
            });
            let mut waiting = Box::pin(object.readable());
            assert!(
                waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            if case == "close" {
                object.close().unwrap();
                (None, waiting, completed, writer)
            } else {
                (Some(object), waiting, completed, writer)
            }
        });
        drop(runtime);
        completed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            block_on(waiting.as_mut()).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(OTHER.with(|slot| slot.borrow().is_none()));
        drop(object);
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use rivet::io::AsyncHandle;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use windows_sys::Win32::{
        Foundation::WAIT_OBJECT_0,
        System::Threading::{
            CreateEventW, CreateMutexW, CreateSemaphoreW, ReleaseMutex, ResetEvent, SetEvent,
            WaitForSingleObject,
        },
    };

    fn event(manual: bool, signaled: bool) -> (OwnedHandle, OwnedHandle) {
        let handle = unsafe {
            CreateEventW(
                std::ptr::null(),
                manual as i32,
                signaled as i32,
                std::ptr::null(),
            )
        };
        assert!(!handle.is_null());
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        let control = handle.try_clone().unwrap();
        (handle, control)
    }
    fn signal(handle: &OwnedHandle) {
        assert_ne!(unsafe { SetEvent(handle.as_raw_handle()) }, 0);
    }

    #[test]
    fn auto_reset_completion_survives_cancel_exactly_once() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (handle, control) = event(false, false);
            let object = AsyncHandle::import(handle).unwrap();
            let (waker, notified) = notice();
            {
                let mut cancelled = pin!(object.wait());
                assert!(
                    cancelled
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                signal(&control);
                notified.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            ready(object.wait()).await;
            assert!(poll_once(object.wait()).await.is_none());
            signal(&control);
            ready(object.wait()).await;
            assert!(poll_once(object.wait()).await.is_none());
        });
    }

    #[test]
    fn import_does_not_consume_a_presignaled_auto_reset_event() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (handle, _control) = event(false, true);
            let object = AsyncHandle::import(handle).unwrap();
            ready(object.wait()).await;
            assert!(poll_once(object.wait()).await.is_none());
        });
    }

    #[test]
    fn manual_reset_event_stays_ready_until_reset() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (handle, control) = event(true, true);
            let object = AsyncHandle::import(handle).unwrap();
            ready(object.wait()).await;
            ready(object.wait()).await;
            assert_ne!(unsafe { ResetEvent(control.as_raw_handle()) }, 0);
            assert!(poll_once(object.wait()).await.is_none());
            signal(&control);
            ready(object.wait()).await;
        });
    }

    #[test]
    fn semaphore_completions_consume_one_permit_each() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let handle = unsafe { CreateSemaphoreW(std::ptr::null(), 2, 2, std::ptr::null()) };
            assert!(!handle.is_null());
            let object =
                AsyncHandle::import(unsafe { OwnedHandle::from_raw_handle(handle) }).unwrap();
            ready(object.wait()).await;
            ready(object.wait()).await;
            assert!(poll_once(object.wait()).await.is_none());
        });
    }

    #[test]
    fn mutex_import_is_rejected_without_acquiring_or_closing_it() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let handle = unsafe { CreateMutexW(std::ptr::null(), 0, std::ptr::null()) };
            assert!(!handle.is_null());
            let failed =
                AsyncHandle::import(unsafe { OwnedHandle::from_raw_handle(handle) }).unwrap_err();
            assert_eq!(failed.error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(failed.resource.as_raw_handle(), handle);
            assert_eq!(
                unsafe { WaitForSingleObject(failed.resource.as_raw_handle(), 0) },
                WAIT_OBJECT_0
            );
            assert_ne!(unsafe { ReleaseMutex(failed.resource.as_raw_handle()) }, 0);
        });
    }

    #[test]
    fn capacity_returns_handle_and_old_wait_does_not_reach_reused_slot() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            let (handle, old_control) = event(false, false);
            let first = AsyncHandle::import(handle).unwrap();
            let old_wait = first.wait();
            let (handle, control) = event(false, false);
            let raw = handle.as_raw_handle();
            let failed = AsyncHandle::import(handle).unwrap_err();
            assert_eq!(failed.error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(failed.resource.as_raw_handle(), raw);
            first.close().unwrap();
            let second = AsyncHandle::import(failed.resource).unwrap();
            signal(&old_control);
            assert_eq!(
                old_wait.await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert!(poll_once(second.wait()).await.is_none());
            signal(&control);
            ready(second.wait()).await;
        });
    }

    #[test]
    fn closing_pending_native_callbacks_keeps_reused_slots_isolated() {
        let mut runtime = runtime(1);
        runtime.block_on(async {
            for _ in 0..64 {
                let (handle, control) = event(false, false);
                let first = AsyncHandle::import(handle).unwrap();
                let mut stale = pin!(first.wait());
                assert!(poll_once(stale.as_mut()).await.is_none());
                signal(&control);
                first.close().unwrap();
                let (handle, new_control) = event(false, false);
                let second = AsyncHandle::import(handle).unwrap();
                assert_eq!(stale.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
                assert!(poll_once(second.wait()).await.is_none());
                signal(&new_control);
                ready(second.wait()).await;
            }
        });
    }

    #[test]
    fn runtime_shutdown_cancels_wait_and_wakes_external_future() {
        let mut runtime = runtime(1);
        let (handle, control) = event(false, false);
        let object = runtime.block_on(async { AsyncHandle::import(handle).unwrap() });
        let (waker, notified) = notice();
        let mut wait = pin!(object.wait());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(runtime);
        notified.recv_timeout(Duration::from_secs(5)).unwrap();
        signal(&control);
        assert_eq!(
            block_on(wait).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            block_on(object.wait()).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    thread_local! {
        static OTHER: std::cell::RefCell<Option<AsyncHandle>> = const { std::cell::RefCell::new(None) };
    }

    fn drop_other() {
        let other = OTHER.with(|slot| slot.borrow_mut().take());
        other.unwrap().close().unwrap();
    }

    fn replace_other() {
        drop_other();
        let (handle, _control) = event(false, false);
        AsyncHandle::import(handle).unwrap().close().unwrap();
    }

    pub(super) fn exercise_reentry(case: &str) {
        let mut runtime = runtime(2);
        if case == "callback" {
            runtime.block_on(async {
                let (handle, control) = event(false, false);
                let object = AsyncHandle::import(handle).unwrap();
                let (cancel, completed) = cancel_on_wake(object.wait());
                signal(&control);
                completed.recv_timeout(Duration::from_secs(5)).unwrap();
                assert!(cancel.wait.lock().is_none());
                ready(object.wait()).await;
                assert!(poll_once(object.wait()).await.is_none());
                object.close().unwrap();
            });
            return;
        }
        let (object, mut waiting, completed) = runtime.block_on(async {
            let (handle, _control) = event(false, false);
            let object = AsyncHandle::import(handle).unwrap();
            let (other, _other_control) = event(false, false);
            OTHER.with(|slot| *slot.borrow_mut() = Some(AsyncHandle::import(other).unwrap()));
            let (waker, completed) = reenter_on_wake(if case == "close" {
                replace_other
            } else {
                drop_other
            });
            let mut waiting = Box::pin(object.wait());
            assert!(
                waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            if case == "close" {
                object.close().unwrap();
                (None, waiting, completed)
            } else {
                (Some(object), waiting, completed)
            }
        });
        drop(runtime);
        completed.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            block_on(waiting.as_mut()).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(OTHER.with(|slot| slot.borrow().is_none()));
        drop(object);
    }
}
