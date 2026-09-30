//! Bounded, runtime-owned waiting for native objects other than TCP/UDP sockets.
//!
//! Linux and Android expose readiness for nonblocking, pollable descriptors.
//! Windows exposes waits for events, semaphores, waitable timers, processes,
//! threads, and jobs. Neither Interface promises asynchronous regular-file I/O.
//! Imported objects stay local to their owner thread. Import failures return
//! ownership; dropping an object unregisters native waits before closing it.

use std::{fmt, io};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(crate) use unix::Registry;
#[cfg(unix)]
pub use unix::{AsyncFd, Interest};
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::AsyncHandle;
#[cfg(windows)]
pub(crate) use windows::Registry;

/// An unsuccessful import, retaining the original owned native object.
#[derive(Debug)]
pub struct ImportError<T> {
    pub error: io::Error,
    pub resource: T,
}
impl<T> ImportError<T> {
    pub fn into_parts(self) -> (io::Error, T) {
        (self.error, self.resource)
    }
}
impl<T> fmt::Display for ImportError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native I/O import failed: {}", self.error)
    }
}
impl<T: fmt::Debug> std::error::Error for ImportError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "native I/O registration is closed",
    )
}
fn at_capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "native I/O registration limit reached",
    )
}

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

/// Slots are reserved once. A generation that would wrap retires its slot;
/// an event identity is never assigned to a different registration.
struct Table<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
}
impl<T> Table<T> {
    fn validate_capacity(capacity: usize) -> io::Result<()> {
        if capacity == 0
            || capacity > u32::MAX as usize
            || std::alloc::Layout::array::<Slot<T>>(capacity).is_err()
            || std::alloc::Layout::array::<u32>(capacity).is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native I/O capacity",
            ));
        }
        Ok(())
    }
    fn new(capacity: usize) -> io::Result<Self> {
        Self::validate_capacity(capacity)?;
        let mut slots = Vec::new();
        let mut free = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        free.try_reserve_exact(capacity).map_err(io::Error::other)?;
        slots.resize_with(capacity, || Slot {
            generation: 1,
            value: None,
        });
        free.extend((0..capacity as u32).rev());
        Ok(Self { slots, free })
    }
    fn vacant_key(&self) -> io::Result<u64> {
        let index = *self.free.last().ok_or_else(at_capacity)?;
        Ok((u64::from(self.slots[index as usize].generation) << 32) | u64::from(index))
    }
    fn insert(&mut self, key: u64, value: T) {
        let index = self.free.pop().expect("reserved native I/O slot");
        debug_assert_eq!(index, key as u32);
        let slot = &mut self.slots[index as usize];
        debug_assert_eq!(slot.generation, (key >> 32) as u32);
        debug_assert!(slot.value.is_none());
        slot.value = Some(value);
    }
    fn get(&self, key: u64) -> Option<&T> {
        self.slots
            .get(key as u32 as usize)
            .filter(|slot| slot.generation == (key >> 32) as u32)?
            .value
            .as_ref()
    }
    fn remove(&mut self, key: u64) -> Option<T> {
        let index = key as u32;
        let slot = self.slots.get_mut(index as usize)?;
        if slot.generation != (key >> 32) as u32 {
            return None;
        }
        let value = slot.value.take()?;
        if let Some(generation) = slot.generation.checked_add(1) {
            slot.generation = generation;
            self.free.push(index);
        }
        Some(value)
    }
    #[cfg(unix)]
    fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(|slot| slot.value.as_ref())
    }
    #[cfg(windows)]
    fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.slots.iter_mut().filter_map(|slot| slot.value.as_mut())
    }
    fn into_values(self) -> impl Iterator<Item = T> {
        self.slots.into_iter().filter_map(|slot| slot.value)
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;
    use std::{
        process::{Child, Command},
        sync::{Arc, mpsc},
        task::{Wake, Waker},
        time::{Duration, Instant},
    };

    type Stop = Box<dyn FnOnce() + Send>;

    struct StopOnWake {
        stop: Mutex<Option<Stop>>,
        completed: mpsc::SyncSender<()>,
    }

    impl Wake for StopOnWake {
        fn wake(self: Arc<Self>) {
            let stop = self.stop.lock().take();
            if let Some(stop) = stop {
                stop();
                self.completed.send(()).unwrap();
            }
        }
    }

    pub(super) fn stop_waker(stop: impl FnOnce() + Send + 'static) -> (Waker, mpsc::Receiver<()>) {
        let (completed, receive) = mpsc::sync_channel(1);
        (
            Waker::from(Arc::new(StopOnWake {
                stop: Mutex::new(Some(Box::new(stop))),
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
    fn callback_shutdown_child() {
        if std::env::var_os("RIVET_IO_CALLBACK_SHUTDOWN").is_none() {
            return;
        }
        #[cfg(unix)]
        super::unix::callback_shutdown();
        #[cfg(windows)]
        super::windows::callback_shutdown();
    }

    #[test]
    fn native_dispatcher_can_stop_from_its_own_callback() {
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "io::tests::callback_shutdown_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("RIVET_IO_CALLBACK_SHUTDOWN", "1")
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "native callback shutdown failed: {status}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "native callback shutdown deadlocked"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
