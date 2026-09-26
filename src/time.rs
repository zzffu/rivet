//! Worker-local, bounded timers, reusable sleeps, and periodic schedules.
use crate::runtime::{Current, Worker, current};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

/// Construct reusable native sleeps without borrowing the capability.
///
/// [`Current`] delegates to the free constructors: creating a sleep requires no
/// running worker, captures no worker identity, and reserves no timer slot. The
/// returned [`Sleep`] binds to the current worker on its first poll, even for an
/// elapsed deadline; a missing worker then returns `NotConnected`. Admission
/// pressure returns `WouldBlock`, a stopped owner returns `BrokenPipe`, and use
/// from a different current worker returns `InvalidInput`.
///
/// [`Sleep::reset`] reuses the native timer and preserves a bound sleep's owner,
/// including after completion. Moving or copying `Current` cannot rebind it.
/// Dropping the sleep releases its timer admission immediately.
pub trait Timer {
    /// Create a sleep with a deadline relative to this invocation.
    ///
    /// As with [`sleep`], a deadline that overflows [`Instant`] is reported as
    /// `InvalidInput` when the sleep is polled, not when it is constructed.
    fn sleep(&self, duration: Duration) -> Sleep;

    /// Create a sleep for a deadline, binding to a worker only on first poll.
    fn sleep_until(&self, deadline: Instant) -> Sleep;
}

/// A worker-local deadline future that can be reused with [`Sleep::reset`].
///
/// The first poll binds the sleep to the current Rivet worker, even if its
/// deadline has already passed. A bound sleep cannot be polled or reset from
/// another worker. Dropping it immediately releases any timer admission slot.
#[must_use = "a sleep does not register until polled"]
pub struct Sleep {
    deadline: Option<Instant>,
    owner: Option<Weak<Worker>>,
    token: Option<u64>,
    done: bool,
}
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now().checked_add(duration),
        owner: None,
        token: None,
        done: false,
    }
}
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline: Some(deadline),
        owner: None,
        token: None,
        done: false,
    }
}

impl Timer for Current {
    fn sleep(&self, duration: Duration) -> Sleep {
        sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> Sleep {
        sleep_until(deadline)
    }
}

impl Sleep {
    /// Replace the deadline without changing this sleep's worker.
    ///
    /// Unpolled, expired, and completed sleeps can all be reset. An admitted
    /// timer is updated in its existing indexed-heap slot, without allocating
    /// or leaving stale deadlines behind. If the old timer has already expired,
    /// the next poll must obtain a slot again and may return `WouldBlock`.
    ///
    /// An unpolled sleep remains unbound until it is polled. Once bound, a sleep
    /// retains its owner even after completion: a stopped runtime returns
    /// `BrokenPipe`, and resetting from a different current worker returns
    /// `InvalidInput`. An error leaves the previous deadline and state intact.
    pub fn reset(&mut self, deadline: Instant) -> io::Result<()> {
        if let Some(owner) = self.bound_owner()?
            && let Some(token) = self.token
            && !owner.timers.borrow_mut().reset(token, deadline)
        {
            self.token = None;
        }
        self.deadline = Some(deadline);
        self.done = false;
        Ok(())
    }

    fn bound_owner(&self) -> io::Result<Option<Rc<Worker>>> {
        let Some(owner) = &self.owner else {
            return Ok(None);
        };
        let owner = owner.upgrade().ok_or_else(timer_stopped)?;
        if owner.timers.borrow().is_closed() {
            return Err(timer_stopped());
        }
        if let Ok(active) = current()
            && !Rc::ptr_eq(&owner, &active)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sleep belongs to a different Rivet worker",
            ));
        }
        Ok(Some(owner))
    }

    fn poll_owner(&mut self) -> io::Result<Rc<Worker>> {
        if let Some(owner) = self.bound_owner()? {
            return Ok(owner);
        }
        let owner = current()?;
        if owner.timers.borrow().is_closed() {
            return Err(timer_stopped());
        }
        self.owner = Some(Rc::downgrade(&owner));
        Ok(owner)
    }
}

fn timer_stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "timer runtime has stopped")
}

impl Future for Sleep {
    type Output = io::Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Sleep polled after completion");
        let Some(deadline) = self.deadline else {
            self.done = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sleep deadline overflows Instant",
            )));
        };
        let owner = match self.poll_owner() {
            Ok(owner) => owner,
            Err(error) => {
                self.done = true;
                return Poll::Ready(Err(error));
            }
        };
        if Instant::now() >= deadline {
            if let Some(token) = self.token.take() {
                owner.timers.borrow_mut().remove(token);
            }
            self.done = true;
            return Poll::Ready(Ok(()));
        }
        if let Some(token) = self.token {
            if owner.timers.borrow_mut().update(token, cx.waker()) {
                return Poll::Pending;
            }
            self.done = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "timer cancelled by runtime shutdown",
            )));
        }
        match owner
            .timers
            .borrow_mut()
            .insert(deadline, cx.waker().clone())
        {
            Ok(token) => {
                self.token = Some(token);
                Poll::Pending
            }
            Err(error) => {
                self.done = true;
                Poll::Ready(Err(error))
            }
        }
    }
}
impl Drop for Sleep {
    fn drop(&mut self) {
        if let (Some(owner), Some(token)) = (
            self.owner.as_ref().and_then(Weak::upgrade),
            self.token.take(),
        ) {
            owner.timers.borrow_mut().remove(token);
        }
    }
}

/// How a periodic timer advances after its scheduled tick is observed late.
///
/// There is no lateness tolerance: any time after the scheduled instant is
/// considered late. Each [`Interval::tick`] returns the current *scheduled*
/// instant, not the time at which its future was actually polled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MissedTickBehavior {
    /// Advance one period from the scheduled instant. Missed ticks remain in
    /// the schedule and later calls may complete immediately, one tick per call.
    Burst,
    /// Advance to the first instant strictly after the observation time on the
    /// original period grid. Missed ticks are skipped without a catch-up loop.
    #[default]
    Skip,
    /// Schedule the next tick one period after the actual observation time.
    /// Unlike `Skip`, this changes the phase of the schedule.
    Delay,
}

/// A periodic schedule backed by a single reusable [`Sleep`].
///
/// The default missed-tick behavior is [`MissedTickBehavior::Skip`]. Timers are
/// admitted lazily and remain local to the worker that first polls a tick.
/// Canceling a pending `tick` does not advance the schedule; the next call waits
/// for the same tick. The underlying timer remains registered until it expires
/// or the interval is dropped.
pub struct Interval {
    sleep: Sleep,
    period: Duration,
    behavior: MissedTickBehavior,
}

/// Create a periodic schedule whose first tick is immediately ready.
///
/// A zero period or a first successor that overflows `Instant` returns
/// `InvalidInput`. Creating an interval does not require a current runtime.
pub fn interval(period: Duration) -> io::Result<Interval> {
    interval_at(Instant::now(), period)
}

/// Create a periodic schedule beginning at `start`.
///
/// A start in the past is allowed: the first tick still returns `start`, and
/// the missed-tick behavior controls the following tick. A zero period or an
/// unrepresentable `start + period` returns `InvalidInput`.
pub fn interval_at(start: Instant, period: Duration) -> io::Result<Interval> {
    if period.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interval period must be nonzero",
        ));
    }
    start.checked_add(period).ok_or_else(interval_overflow)?;
    Ok(Interval {
        sleep: sleep_until(start),
        period,
        behavior: MissedTickBehavior::Skip,
    })
}

impl Interval {
    /// Wait for the next tick and return its scheduled instant.
    ///
    /// The returned instant is the planned deadline, not the actual wake time.
    /// Advancement uses the time at which the ready tick is observed. Dropping
    /// a pending waiter does not consume a tick or change its scheduled instant.
    ///
    /// Timer admission and runtime errors are returned unchanged. If computing
    /// the following deadline overflows `Instant`, this returns `InvalidInput`
    /// without consuming the tick. Failed waits can be retried.
    pub async fn tick(&mut self) -> io::Result<Instant> {
        let scheduled = self.sleep.deadline.unwrap();
        if self.sleep.done {
            self.sleep.reset(scheduled)?;
        }
        (&mut self.sleep).await?;
        let next = next_tick(scheduled, self.period, Instant::now(), self.behavior)?;
        self.sleep.reset(next)?;
        Ok(scheduled)
    }

    /// Change how the schedule advances after subsequent completed ticks.
    ///
    /// This does not change the deadline of the currently pending tick.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }
}

fn interval_overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "interval deadline overflows Instant",
    )
}

// Pure schedule advancement keeps lateness policy independent of native waits.
// Constructors guarantee a nonzero period.
fn next_tick(
    scheduled: Instant,
    period: Duration,
    observed: Instant,
    behavior: MissedTickBehavior,
) -> io::Result<Instant> {
    let next = match behavior {
        MissedTickBehavior::Skip if observed > scheduled => {
            let remainder = observed.duration_since(scheduled).as_nanos() % period.as_nanos();
            let remainder = Duration::new(
                (remainder / 1_000_000_000) as u64,
                (remainder % 1_000_000_000) as u32,
            );
            observed.checked_add(period - remainder)
        }
        MissedTickBehavior::Delay if observed > scheduled => observed.checked_add(period),
        _ => scheduled.checked_add(period),
    };
    next.ok_or_else(interval_overflow)
}

#[derive(Debug)]
pub enum TimeoutError {
    Elapsed,
    Timer(io::Error),
}
impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Elapsed => f.write_str("operation deadline elapsed"),
            Self::Timer(error) => write!(f, "timer failed: {error}"),
        }
    }
}
impl std::error::Error for TimeoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Timer(error) => Some(error),
            Self::Elapsed => None,
        }
    }
}
pin_project_lite::pin_project! {
    #[must_use = "timeouts do not run until polled"]
    pub struct Timeout<F> { #[pin] future: F, #[pin] sleep: Sleep }
}
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep(duration),
    }
}
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep_until(deadline),
    }
}
impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, TimeoutError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // Ready work wins a simultaneous deadline, without an unnecessary timer
        // allocation or rejecting an immediately ready operation at capacity.
        if let Poll::Ready(value) = this.future.poll(cx) {
            return Poll::Ready(Ok(value));
        }
        match this.sleep.poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(TimeoutError::Elapsed)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(TimeoutError::Timer(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missed_tick_policies_preserve_their_scheduling_contracts() {
        let scheduled = Instant::now();
        let period = Duration::from_nanos(10);
        let default_behavior = interval_at(scheduled, period).unwrap().behavior;
        for (late, burst, skip, delay) in [
            (0, 10, 10, 10),
            (1, 10, 10, 11),
            (30, 10, 40, 40),
            (35, 10, 40, 45),
        ] {
            let observed = scheduled + Duration::from_nanos(late);
            for (behavior, expected) in [
                (MissedTickBehavior::Burst, burst),
                (default_behavior, skip),
                (MissedTickBehavior::Delay, delay),
            ] {
                assert_eq!(
                    next_tick(scheduled, period, observed, behavior).unwrap(),
                    scheduled + Duration::from_nanos(expected),
                    "{behavior:?} observed {late}ns late"
                );
            }
        }
    }

    #[test]
    fn skip_handles_billions_of_missed_ticks_and_fractional_seconds() {
        let scheduled = Instant::now();
        let observed = scheduled + Duration::from_secs(60);
        assert_eq!(
            next_tick(
                scheduled,
                Duration::from_nanos(7),
                observed,
                MissedTickBehavior::Skip,
            )
            .unwrap(),
            observed + Duration::from_nanos(4)
        );
        assert_eq!(
            next_tick(
                scheduled,
                Duration::from_millis(1500),
                scheduled + Duration::from_millis(6250),
                MissedTickBehavior::Skip,
            )
            .unwrap(),
            scheduled + Duration::from_millis(7500)
        );
    }

    #[test]
    fn interval_rejects_zero_period_and_deadline_overflow() {
        let scheduled = Instant::now();
        for period in [Duration::ZERO, Duration::MAX] {
            assert_eq!(
                interval(period).err().unwrap().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                interval_at(scheduled, period).err().unwrap().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        for behavior in [
            MissedTickBehavior::Burst,
            MissedTickBehavior::Skip,
            MissedTickBehavior::Delay,
        ] {
            assert_eq!(
                next_tick(
                    scheduled,
                    Duration::MAX,
                    scheduled + Duration::from_nanos(1),
                    behavior,
                )
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }
}
