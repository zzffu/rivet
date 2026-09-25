//! Worker-local timers with bounded admission and eager cancellation.
use crate::runtime::{Worker, current};
use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};

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
        if Instant::now() >= deadline {
            if let (Some(owner), Some(token)) = (
                self.owner.as_ref().and_then(Weak::upgrade),
                self.token.take(),
            ) {
                owner.timers.borrow_mut().remove(token);
            }
            self.done = true;
            return Poll::Ready(Ok(()));
        }
        let owner = match self.owner.as_ref() {
            Some(owner) => match owner.upgrade() {
                Some(owner) => owner,
                None => {
                    self.done = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "timer runtime has stopped",
                    )));
                }
            },
            None => match current() {
                Ok(owner) => {
                    self.owner = Some(Rc::downgrade(&owner));
                    owner
                }
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
            },
        };
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
