use rivet::{Runtime, RuntimeConfig, SendPayload, TcpListener, TcpStream};
use rivet::{
    net::ServeConfig,
    runtime::{JoinError, TaskGroup},
    sync::{CancellationToken, mpsc, oneshot, watch},
};
use std::{
    cell::Cell,
    future::{Future, pending, poll_fn},
    io,
    net::{Ipv4Addr, Shutdown},
    pin::pin,
    rc::Rc,
    task::Poll,
    time::{Duration, Instant},
};

fn error(value: impl std::fmt::Display) -> io::Error {
    io::Error::other(value.to_string())
}
fn require(value: bool, message: &'static str) -> io::Result<()> {
    if value {
        Ok(())
    } else {
        Err(io::Error::other(message))
    }
}

#[cfg(unix)]
async fn non_socket_io() -> io::Result<()> {
    use rivet::io::{AsyncFd, Interest};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let mut pipe = [-1; 2];
    if unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let read = unsafe { OwnedFd::from_raw_fd(pipe[0]) };
    let write = unsafe { OwnedFd::from_raw_fd(pipe[1]) };
    let read = AsyncFd::import(read).map_err(|failure| failure.error)?;
    let mut output = [0u8; 4];
    let read_into = |fd: std::os::fd::BorrowedFd<'_>, output: &mut [u8]| {
        let length =
            unsafe { libc::read(fd.as_raw_fd(), output.as_mut_ptr().cast(), output.len()) };
        if length < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(length as usize)
        }
    };
    let empty = read.try_io(Interest::Readable, |fd| read_into(fd, &mut output));
    require(
        empty.is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock),
        "empty pipe did not report WouldBlock",
    )?;
    let bytes = b"pipe";
    let written = unsafe { libc::write(write.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
    require(written == bytes.len() as isize, "pipe write failed")?;
    read.readable().await?;
    let length = read.try_io(Interest::Readable, |fd| read_into(fd, &mut output))?;
    require(
        length == bytes.len() && output == *bytes,
        "async descriptor changed pipe contents",
    )?;
    let pending = read.readable();
    read.close()?;
    require(
        pending.await.is_err(),
        "closed descriptor retained a successful waiter",
    )
}

#[cfg(windows)]
async fn non_socket_io() -> io::Result<()> {
    use rivet::io::AsyncHandle;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use windows_sys::Win32::System::Threading::{CreateEventW, SetEvent};
    let raw = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    let owned = unsafe { OwnedHandle::from_raw_handle(raw) };
    let event = AsyncHandle::import(owned).map_err(|failure| failure.error)?;
    if unsafe { SetEvent(raw) } == 0 {
        return Err(io::Error::last_os_error());
    }
    event.wait().await?;
    let pending = event.wait();
    event.close()?;
    require(
        pending.await.is_err(),
        "closed waitable object retained a successful waiter",
    )
}

async fn echo_and_stop() -> io::Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let address = listener.local_addr();
    let stop = CancellationToken::new();
    let stopped = stop.clone();
    let (reports, report) = mpsc::bounded(1);
    let serving = rivet::spawn_local(async move {
        listener
            .serve_until(
                ServeConfig {
                    max_connections: 1,
                    shutdown_grace: Duration::from_secs(2),
                },
                stopped.cancelled(),
                move |stream, _cancel| {
                    let reports = reports.clone();
                    async move {
                        let result = async {
                            while let Some(data) = stream.recv().await? {
                                stream.send_all(SendPayload::Single(data)).await.result?;
                            }
                            stream.shutdown(Shutdown::Write)
                        }
                        .await;
                        let _ = reports.send(result).await;
                    }
                },
            )
            .await
    })
    .map_err(error)?;
    let client = TcpStream::connect(address).await?;
    let expected = b"supervised native echo";
    let mut data = rivet::runtime::buffer_pool()?.try_acquire_at_least(expected.len())?;
    data.extend_from_slice(expected)?;
    client
        .send_all(SendPayload::Single(data.freeze()))
        .await
        .result?;
    client.shutdown(Shutdown::Write)?;
    let mut received = Vec::new();
    while let Some(data) = client.recv().await? {
        received.extend_from_slice(data.as_slice());
    }
    require(
        received == expected,
        "supervised echo changed stream contents",
    )?;
    report.recv().await.map_err(error)??;
    stop.cancel();
    serving.await.map_err(error)??;
    Ok(())
}

async fn abortive_close() -> io::Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let connecting =
        rivet::spawn_local(TcpStream::connect(listener.local_addr())).map_err(error)?;
    let server = listener.accept().await?;
    let client = connecting.await.map_err(error)??;
    server.abort()?;
    match client.recv().await {
        Err(failure)
            if matches!(
                failure.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        Err(failure) => Err(failure),
        Ok(_) => Err(io::Error::other(
            "abortive close did not produce peer reset",
        )),
    }
}

async fn dynamic_admission(tasks: &mut TaskGroup<usize>) -> io::Result<()> {
    enum Command {
        Complete(usize),
        Wait(oneshot::Sender<()>),
        Stop,
    }
    enum Event {
        Command(Command),
        Joined(Option<Result<usize, JoinError>>),
    }
    struct LocalCleanup(Rc<Cell<usize>>);
    impl Drop for LocalCleanup {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    let (admit, commands) = mpsc::bounded(1);
    let (idle, waiting) = mpsc::bounded(1);
    let controller = rivet::spawn_local(async move {
        // Admit each new job only after the owner has returned to its empty wait.
        for value in [7, 11] {
            waiting.recv().await.map_err(error)?;
            admit.send(Command::Complete(value)).await.map_err(error)?;
        }
        waiting.recv().await.map_err(error)?;
        let (started, ready) = oneshot::channel();
        admit.send(Command::Wait(started)).await.map_err(error)?;
        ready.await.map_err(error)?;
        admit.send(Command::Stop).await.map_err(error)?;
        Ok::<_, io::Error>(())
    })
    .map_err(error)?;

    let drops = Rc::new(Cell::new(0));
    let mut idle_waits = 0;
    let mut total = 0;
    loop {
        let event = if tasks.is_empty() {
            idle_waits += 1;
            idle.send(()).await.map_err(error)?;
            // An empty join would return None immediately, not wait for work.
            Event::Command(commands.recv().await.map_err(error)?)
        } else {
            // Both waits are fresh each turn. The losing pending wait is dropped,
            // not detached or fused; a cancelled join still leaves tasks owned.
            let mut command = pin!(commands.recv());
            let mut joined = pin!(tasks.join_next());
            poll_fn(|cx| {
                // Observe a queued stop before collecting another child result.
                if let Poll::Ready(command) = command.as_mut().poll(cx) {
                    return Poll::Ready(command.map(Event::Command).map_err(error));
                }
                joined
                    .as_mut()
                    .poll(cx)
                    .map(|value| Ok(Event::Joined(value)))
            })
            .await?
        };
        match event {
            Event::Command(Command::Complete(value)) => {
                tasks.spawn_local(async move { value }).map_err(error)?;
            }
            Event::Command(Command::Wait(started)) => {
                let cleanup = LocalCleanup(drops.clone());
                tasks
                    .spawn_local(async move {
                        let _cleanup = cleanup;
                        let _ = started.send(());
                        pending::<usize>().await
                    })
                    .map_err(error)?;
            }
            Event::Command(Command::Stop) => break,
            Event::Joined(Some(value)) => total += value.map_err(error)?,
            Event::Joined(None) => {
                return Err(io::Error::other("nonempty task group lost its child"));
            }
        }
    }

    // Abort is only a request. Join acknowledges destruction of the local
    // future/captures; it says nothing about native I/O retirement.
    tasks.abort_all();
    let mut cancelled = 0;
    while let Some(result) = tasks.join_next().await {
        require(
            matches!(result, Err(JoinError::Cancelled)),
            "stopped task group did not report cancellation",
        )?;
        cancelled += 1;
    }
    controller.await.map_err(error)??;
    require(
        idle_waits == 3 && total == 18,
        "task group did not return to idle and readmit work",
    )?;
    require(
        cancelled == 1 && drops.get() == 1,
        "joining cancellation did not finish local cleanup",
    )
}

/// One bounded scenario shared by native desktop and the ordinary Android App.
pub async fn exercise() -> io::Result<String> {
    let owner = std::thread::current().id();
    let (started, ready) = oneshot::channel();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    // Keep the only release sender here: error/cancellation drops it and unblocks
    // the synchronous receive before Runtime destruction joins the blocking pool.
    let blocking = rivet::runtime::spawn_blocking(move || {
        let _ = started.send(std::thread::current().id());
        released.recv().map_err(error)
    })
    .map_err(error)?;
    require(
        ready.await.map_err(error)? != owner,
        "blocking work ran on network worker",
    )?;

    // The blocking worker is parked on a real synchronous receive throughout
    // these asynchronous operations; none may depend on releasing it first.
    let mut tasks = TaskGroup::new(2)?;
    tasks
        .spawn_local(async {
            let local = Rc::new(17);
            rivet::runtime::yield_now().await;
            *local
        })
        .map_err(error)?;
    tasks
        .spawn(|| async {
            let local = Rc::new(25);
            rivet::runtime::yield_now().await;
            *local
        })
        .map_err(error)?;
    let mut total = 0;
    while let Some(value) = tasks.join_next().await {
        total += value.map_err(error)?;
    }
    require(total == 42, "task group lost a result")?;
    dynamic_admission(&mut tasks).await?;

    let mut sleep = rivet::time::sleep(Duration::from_secs(3600));
    sleep.reset(Instant::now())?;
    (&mut sleep).await?;
    sleep.reset(Instant::now())?;
    (&mut sleep).await?;
    let mut interval = rivet::time::interval(Duration::from_millis(1))?;
    let first = interval.tick().await?;
    let second = interval.tick().await?;
    require(
        second > first,
        "interval did not advance its scheduled deadline",
    )?;

    let (updates, mut latest) = watch::channel(0);
    updates.send(1).map_err(error)?;
    updates.send(2).map_err(error)?;
    drop(updates);
    latest.changed().await.map_err(error)?;
    require(
        *latest.borrow() == 2,
        "watch failed to preserve final coalesced value",
    )?;
    require(
        latest.changed().await.is_err(),
        "watch failed to close after final value",
    )?;

    non_socket_io().await?;
    echo_and_stop().await?;
    abortive_close().await?;
    release.send(42).map_err(error)?;
    require(
        blocking.await.map_err(error)?? == 42,
        "blocking result was lost",
    )?;
    tasks.shutdown().await.map_err(error)?;
    Ok("blocking isolation; native descriptor/wait object; local/automatic task groups; dynamic task admission (3 idle waits, 2 results, cancel/join with local cleanup); timer reset/interval; watch drain; supervised TCP echo/half-close/stop; peer-observed RST".to_owned())
}

pub fn run(mut config: RuntimeConfig) -> io::Result<String> {
    config.blocking.threads = 1;
    config.blocking.queue_capacity = 2;
    let mut runtime = Runtime::new(config)?;
    let detail = runtime
        .block_on(rivet::time::timeout(Duration::from_secs(15), exercise()))
        .map_err(error)??;
    drop(runtime);
    Ok(detail)
}
