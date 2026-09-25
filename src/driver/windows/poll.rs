use super::*;
use windows_sys::Win32::{
    Foundation::{RtlNtStatusToDosError, WAIT_TIMEOUT},
    System::IO::GetQueuedCompletionStatusEx,
};

impl Driver {
    /// Arm-before-recheck is essential: a CQ can become nonempty between its
    /// last dequeue and the IOCP wait. RIONotify covers both existing and future
    /// entries, while the dedicated OVERLAPPED stays stable for the CQ lifetime.
    pub fn poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()> {
        self.notifier.check()?;
        self.pool.flush_recycles();
        let initial_events = events.len();
        let mut event_budget = self.limits.completion_budget;
        let mut completion_budget = self.limits.completion_budget;
        self.service(events, &mut event_budget);
        self.commit_deferred()?;
        // Share the hard budget, but alternate first service so neither queue
        // can starve the other even when the entire budget is one completion.
        let iocp_first = self.iocp_first;
        self.iocp_first = !iocp_first;
        let mut progressed = if iocp_first {
            self.dequeue_iocp(Some(Duration::ZERO), &mut completion_budget)?
        } else {
            false
        };
        progressed |= self.drain_rio(&mut completion_budget)?;
        self.service(events, &mut event_budget);
        self.commit_deferred()?;
        self.rio.arm()?;
        progressed |= self.drain_rio(&mut completion_budget)?;
        self.service(events, &mut event_budget);
        self.commit_deferred()?;

        let wait = if progressed
            || events.len() != initial_events
            || event_budget == 0
            || completion_budget == 0
        {
            Some(Duration::ZERO)
        } else {
            timeout
        };
        self.dequeue_iocp(wait, &mut completion_budget)?;
        self.drain_rio(&mut completion_budget)?;
        self.service(events, &mut event_budget);
        self.commit_deferred()?;
        Ok(())
    }

    fn drain_rio(&mut self, budget: &mut usize) -> io::Result<bool> {
        let mut progressed = false;
        while *budget != 0 {
            let capacity = self.rio_completions.len().min(*budget);
            let count = unsafe {
                self.rio.table.RIODequeueCompletion.unwrap()(
                    self.rio.cq,
                    self.rio_completions.as_mut_ptr(),
                    capacity as u32,
                )
            };
            if count == RIO_CORRUPT_CQ {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Windows reported a corrupt RIO completion queue",
                ));
            }
            if count as usize > capacity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RIO returned more completions than its output capacity",
                ));
            }
            if count == 0 {
                break;
            }
            progressed = true;
            *budget -= count as usize;
            for index in 0..count as usize {
                self.complete_rio(self.rio_completions[index])?;
            }
            if (count as usize) < capacity {
                break;
            }
        }
        Ok(progressed)
    }

    fn dequeue_iocp(&mut self, timeout: Option<Duration>, budget: &mut usize) -> io::Result<bool> {
        if *budget == 0 {
            return Ok(false);
        }
        let milliseconds = match timeout {
            None => u32::MAX,
            Some(duration) if duration.is_zero() => 0,
            Some(duration) => duration
                .as_nanos()
                .div_ceil(1_000_000)
                .min(u128::from(u32::MAX - 1)) as u32,
        };
        let mut count = 0;
        let capacity = self.iocp_completions.len().min(*budget);
        let success = unsafe {
            GetQueuedCompletionStatusEx(
                self.notifier.handle(),
                self.iocp_completions.as_mut_ptr(),
                capacity as u32,
                &mut count,
                milliseconds,
                0,
            )
        };
        if success == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(WAIT_TIMEOUT as i32) {
                return Ok(false);
            }
            return Err(error);
        }
        *budget -= count as usize;
        for index in 0..count as usize {
            let completion = self.iocp_completions[index];
            match completion.lpCompletionKey {
                notify::WAKE_KEY => {}
                notify::RIO_KEY => {
                    if !ptr::eq(completion.lpOverlapped, self.rio.notification.as_ptr()) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "RIO notification has an unexpected OVERLAPPED",
                        ));
                    }
                    self.rio.armed = false;
                }
                notify::SOCKET_KEY => {
                    let status = completion.Internal as i32;
                    let error = if status >= 0 {
                        0
                    } else {
                        (unsafe { RtlNtStatusToDosError(status) }) as i32
                    };
                    self.complete_overlapped(completion.lpOverlapped, error)?;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unrecognized IOCP completion key",
                    ));
                }
            }
        }
        Ok(count != 0)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.begin_shutdown();
        let mut events = Vec::with_capacity(self.limits.completion_budget);
        while !self.is_idle() {
            // closesocket is nonblocking; the completion port is the actual
            // retirement barrier, never a polling sleep or a guessed timeout.
            if self.poll(None, &mut events).is_err() {
                // A corrupt completion stream cannot prove that kernel memory
                // references ended. Unwinding would free that memory anyway.
                // Fail-stop rather than exposing a use-after-free to the process.
                std::process::abort();
            }
            events.clear();
        }
        // Rio::drop now closes the CQ and deregisters storage, then releases its
        // pool reference. User-owned immutable leases may outlive this Driver.
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
