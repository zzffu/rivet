//! Native, worker-local TCP/UDP sockets and allocation-free operation futures.
//! Receive waiter cancellation never cancels the persistent receive or discards
//! its queued bytes. Sending transfers immutable leases; `Sent` and native memory
//! release are independent, so returned views remain read-only.

pub use crate::driver::{Received, SendOutcome};
use crate::{
    buffer::{ReadBuf, SendPayload},
    driver::{SocketInfo, SocketKind, Token},
    runtime::{self, JoinError, TaskGroup, Worker, io::SendRequest},
    socket::{ImportError, OwnedSocket, SocketOptions},
    sync::CancellationToken,
    time::{self, TimeoutError},
};
use std::{
    cell::Cell,
    fmt,
    future::{Future, pending, poll_fn},
    io,
    net::{Shutdown, SocketAddr},
    pin::{Pin, pin},
    rc::{Rc, Weak},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

/// Receive owned byte blocks from a full-duplex stream.
///
/// Only one active waiter may own the receive direction; a competing waiter
/// returns [`io::ErrorKind::WouldBlock`]. Cancelling a receive wait must not
/// discard queued, undelivered bytes. Receiving and sending progress
/// independently: implementations must not hold a whole-stream lock across a
/// network wait that prevents the other direction from progressing.
pub trait StreamRecv {
    /// Receive the next block, or `None` at EOF.
    fn recv(&self) -> impl Future<Output = io::Result<Option<ReadBuf>>>;
}

/// Send immutable payloads without blocking the independent receive direction.
///
/// Every byte count describes the caller's original input, even when an
/// implementation encodes or encrypts it. Accepted output may be buffered, but
/// an implementation must not also return that output as unaccepted input.
/// Success does not imply peer delivery or release of the kernel's read-only
/// memory lease.
///
/// Dropping a submitted send future abandons observation, not already-submitted
/// output. Retrying the original payload blindly can duplicate bytes. Native
/// leases remain protected until the underlying operation releases them.
///
/// [`TcpStream`] retains its bounded logical send sequence: admission, not
/// future construction, determines ordering, and exhausted admission limits
/// can return [`io::ErrorKind::WouldBlock`].
pub trait StreamSend {
    /// Attempt a send, returning the original immutable payload even on error.
    ///
    /// A successful result counts accepted input bytes and may be short.
    fn send(&self, data: SendPayload) -> impl Future<Output = SendOutcome>;

    /// Send all input as one ordered logical send, retaining short-write progress.
    ///
    /// The outcome's payload is the unaccepted suffix, empty on success. An
    /// error still returns only that suffix, so accepted input is not resent.
    /// A successful result counts all accepted bytes in the original input
    /// domain. Other sends must not interleave between this send's short writes.
    /// The native implementation uses [`SendAll`], not a loop of independent
    /// [`send`](Self::send) calls.
    fn send_all(&self, data: SendPayload) -> impl Future<Output = SendOutcome>;

    /// Drain this layer's accepted buffered output and flush the lower layer.
    ///
    /// Finish your own send futures first. This does not implicitly join newly
    /// submitted concurrent sends, wait for peer acknowledgement, or wait for
    /// read-only lease reclamation.
    ///
    /// Native TCP has no additional user-space output buffer. Its lazy flush
    /// validates socket ownership on first poll and completes without another
    /// driver operation or allocation. Missing current worker, wrong worker,
    /// and destroyed runtime still report the native context errors.
    fn flush(&self) -> impl Future<Output = io::Result<()>>;
}

/// Orderly, lazy shutdown of only a stream's write direction.
///
/// Constructing or dropping an unpolled future must not close anything.
/// Buffered or transforming implementations drain accepted output and protocol
/// trailers before shutting down the lower write direction. Callers first
/// finish their own sends; concurrently submitted sends are not implicitly
/// joined. After success, callers must not send more application data, but the
/// receive direction remains usable for the peer's response.
///
/// Cancellation after polling does not promise rollback. Dropping or aborting
/// the stream is not a substitute for orderly draining. [`TcpStream`] invokes
/// its existing [`shutdown`](TcpStream::shutdown) with [`Shutdown::Write`] only
/// when polled, preserving native errors without adding socket state.
pub trait StreamShutdown {
    /// Drain accepted output and half-close writing, leaving receiving open.
    fn shutdown_write(&self) -> impl Future<Output = io::Result<()>>;
}

/// Receive datagrams with their boundaries and native metadata intact.
///
/// Only one active receive waiter is allowed; a competing waiter returns
/// [`io::ErrorKind::WouldBlock`]. Cancelling the wait does not discard queued,
/// undelivered datagrams. Sending and receiving progress independently;
/// implementations must not hold a whole-socket lock across a network wait
/// that prevents the other direction from progressing.
pub trait DatagramRecv {
    /// Receive one datagram, preserving its source, truncation and original
    /// length when available. A zero-length datagram is a message, not EOF.
    fn recv(&self) -> impl Future<Output = io::Result<Received>>;
}

/// Send individual datagrams without blocking the receive direction.
///
/// Each operation preserves a datagram boundary, including an empty payload.
/// Both methods return the original immutable payload even on error; successful
/// byte counts describe the caller's input, not an encoded representation.
/// Success does not imply peer delivery or release of a read-only memory lease.
///
/// Native operation and in-flight byte limits remain bounded and can reject
/// admission with [`io::ErrorKind::WouldBlock`]. Dropping a submitted future
/// abandons observation, not already-submitted output; blindly retrying can
/// duplicate a datagram. Native leases remain protected until memory release.
pub trait DatagramSend {
    /// Send one datagram to the socket's connected peer.
    fn send(&self, data: SendPayload) -> impl Future<Output = SendOutcome>;

    /// Send one datagram to `destination`.
    fn send_to(
        &self,
        data: SendPayload,
        destination: SocketAddr,
    ) -> impl Future<Output = SendOutcome>;
}

fn gone() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "socket runtime is no longer available",
    )
}
struct Socket {
    owner: Weak<Worker>,
    key: Cell<Option<u64>>,
    info: SocketInfo,
}
impl Socket {
    fn owner(&self) -> io::Result<Rc<Worker>> {
        let owner = self.owner.upgrade().ok_or_else(gone)?;
        if owner.is_stopping() {
            return Err(gone());
        }
        let current = runtime::current()?;
        if !Rc::ptr_eq(&owner, &current) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket belongs to another runtime worker",
            ));
        }
        if self.key.get().is_none() {
            return Err(gone());
        }
        Ok(owner)
    }
    fn key(&self) -> u64 {
        self.key.get().expect("socket ownership already dispatched")
    }
    fn register(owner: &Rc<Worker>, info: SocketInfo) -> io::Result<Self> {
        let key = match owner.io.borrow_mut().register(info.clone()) {
            Ok(key) => key,
            Err(error) => {
                let _ = owner.driver.borrow_mut().close(info.id);
                return Err(error);
            }
        };
        #[cfg(target_os = "windows")]
        owner.with_io(|io, driver| io.prime_udp(driver, key));
        Ok(Self {
            owner: Rc::downgrade(owner),
            key: Cell::new(Some(key)),
            info,
        })
    }
    fn import(
        socket: OwnedSocket,
        kind: SocketKind,
        options: SocketOptions,
    ) -> Result<Self, ImportError> {
        let owner = match runtime::current() {
            Ok(owner) => owner,
            Err(error) => return Err(ImportError { error, socket }),
        };
        let _callbacks = owner.defer_io_callbacks();
        if let Err(error) = options
            .validate()
            .and_then(|_| owner.io.borrow().can_open_kind(kind))
        {
            return Err(ImportError { error, socket });
        }
        let info = owner.driver.borrow_mut().import(socket, kind, &options)?;
        // The core slot was checked on this thread and import cannot poll tasks.
        let key = owner
            .io
            .borrow_mut()
            .register(info.clone())
            .expect("reserved core socket capacity");
        #[cfg(target_os = "windows")]
        owner.with_io(|io, driver| io.prime_udp(driver, key));
        Ok(Self {
            owner: Rc::downgrade(&owner),
            key: Cell::new(Some(key)),
            info,
        })
    }
    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        let owner = self.owner()?;
        owner.driver.borrow_mut().shutdown(self.info.id, how)
    }
    fn take_idle(&self) -> io::Result<OwnedSocket> {
        let owner = self.owner()?;
        owner.io.borrow().is_idle_socket(self.key())?;
        let socket = owner.driver.borrow_mut().take_idle_socket(self.info.id)?;
        owner.with_io(|io, _| io.forget_idle(self.key()));
        self.key.set(None);
        Ok(socket)
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        if let (Some(owner), Some(key)) = (self.owner.upgrade(), self.key.take()) {
            owner.with_io(|io, driver| io.close_socket(driver, key));
        }
    }
}
impl fmt::Debug for Socket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socket")
            .field("local", &self.info.local_addr)
            .field("peer", &self.info.peer_addr)
            .finish()
    }
}

#[derive(Debug)]
pub struct TcpStream {
    socket: Socket,
}
impl TcpStream {
    pub fn connect(address: SocketAddr) -> Connect {
        Self::connect_with_options(address, SocketOptions::default())
    }
    pub fn connect_with_options(address: SocketAddr, options: SocketOptions) -> Connect {
        Connect {
            address,
            local: None,
            options,
            owner: None,
            token: None,
            done: false,
        }
    }
    /// Connect to `peer` using an explicit local IP address and port.
    ///
    /// Setup is lazy and worker-local: no native socket, hook or bind occurs
    /// until the returned future is first polled in a Rivet worker. Dropping
    /// an unpolled future has no native side effects.
    ///
    /// A local port of zero requests an ephemeral port; a nonzero port is used
    /// exactly. Bind errors propagate without falling back to another endpoint.
    /// After the usual worker and option checks, mismatched local/peer address
    /// families return [`io::ErrorKind::InvalidInput`] before socket creation
    /// or hook invocation.
    ///
    /// Any [`SocketOptions::hook`] must only configure host protection or
    /// interface options; it must not bind or connect the socket itself.
    pub fn connect_from(local: SocketAddr, peer: SocketAddr, options: SocketOptions) -> Connect {
        let mut connect = Self::connect_with_options(peer, options);
        connect.local = Some(local);
        connect
    }
    pub fn import(socket: OwnedSocket, options: SocketOptions) -> Result<Self, ImportError> {
        Socket::import(socket, SocketKind::TcpStream, options).map(|socket| Self { socket })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.info.local_addr
    }
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.socket.info.peer_addr
    }
    pub fn recv(&self) -> Recv<'_> {
        Recv {
            socket: &self.socket,
            waiter: None,
            done: false,
        }
    }
    pub fn send(&self, data: SendPayload) -> Send<'_> {
        Send::new(&self.socket, data, None, None)
    }
    /// Send every byte, preserving short-write progress. `data` in the outcome
    /// is the unsent suffix (empty on success); `result` counts accepted bytes.
    /// On an error the unsent suffix can be retried without duplicating bytes.
    pub fn send_all(&self, data: SendPayload) -> SendAll<'_> {
        SendAll {
            send: self.send(data),
            accepted: 0,
            group: None,
            done: false,
        }
    }
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.socket.shutdown(how)
    }
    /// Consume this connection and request TCP reset, not an orderly FIN.
    /// Setting abortive linger or initiating native close can fail; even then
    /// the consumed connection is closed. Submitted send storage remains held
    /// until the driver observes its real memory-release completion.
    pub fn abort(self) -> io::Result<()> {
        let result = self
            .socket
            .owner()
            .and_then(|owner| owner.driver.borrow_mut().abort(self.socket.info.id));
        drop(self);
        result
    }
    /// Explicit transparent kernel forwarding. Unsupported/unselected backends
    /// return Unsupported; this never silently copies bytes through userspace.
    pub fn splice_to<'a>(&'a self, destination: &'a TcpStream, bytes: usize) -> Splice<'a> {
        Splice {
            source: &self.socket,
            destination: &destination.socket,
            bytes,
            token: None,
            done: false,
        }
    }
}

impl StreamRecv for TcpStream {
    fn recv(&self) -> impl Future<Output = io::Result<Option<ReadBuf>>> {
        TcpStream::recv(self)
    }
}

impl StreamSend for TcpStream {
    fn send(&self, data: SendPayload) -> impl Future<Output = SendOutcome> {
        TcpStream::send(self, data)
    }

    fn send_all(&self, data: SendPayload) -> impl Future<Output = SendOutcome> {
        TcpStream::send_all(self, data)
    }

    async fn flush(&self) -> io::Result<()> {
        self.socket.owner().map(|_| ())
    }
}

impl StreamShutdown for TcpStream {
    async fn shutdown_write(&self) -> io::Result<()> {
        self.shutdown(Shutdown::Write)
    }
}

#[must_use = "connections start when polled"]
pub struct Connect {
    address: SocketAddr,
    local: Option<SocketAddr>,
    options: SocketOptions,
    owner: Option<Rc<Worker>>,
    token: Option<Token>,
    done: bool,
}
impl Future for Connect {
    type Output = io::Result<TcpStream>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Connect polled after completion");
        if self.owner.is_none() {
            match runtime::current().and_then(|owner| {
                self.options.validate()?;
                if self
                    .local
                    .is_some_and(|local| local.is_ipv4() != self.address.is_ipv4())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "TCP local and peer address families differ",
                    ));
                }
                Ok(owner)
            }) {
                Ok(owner) => self.owner = Some(owner),
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
            }
        }
        let owner = self.owner.as_ref().unwrap().clone();
        let mut waker = Some(cx.waker().clone());
        let initiating = self.token.is_none();
        // A destructor may reenter or panic: publish the admitted token to this
        // future before releasing callbacks captured by its native hook.
        let _callbacks = initiating.then(|| owner.defer_io_callbacks());
        if initiating {
            match owner.submit_connect(self.address, self.local, &self.options, &mut waker) {
                Ok(token) => self.token = Some(token),
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
            }
        }
        let result = owner.with_io(|io, _| io.poll_connect(self.token.unwrap(), &mut waker));
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.token = None;
                self.done = true;
                Poll::Ready(
                    result
                        .and_then(|info| Socket::register(&owner, info))
                        .map(|socket| TcpStream { socket }),
                )
            }
        }
    }
}
impl Drop for Connect {
    fn drop(&mut self) {
        if let (Some(owner), Some(token)) = (&self.owner, self.token.take()) {
            owner.with_io(|io, driver| io.abandon_connect(driver, token));
        }
    }
}

/// Business-level connection admission and cooperative shutdown policy.
/// The driver's separately bounded pre-accept queue does not consume a handler
/// slot until the service admits that connection.
#[derive(Clone, Copy, Debug)]
pub struct ServeConfig {
    pub max_connections: usize,
    pub shutdown_grace: Duration,
}
impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub struct TcpListener {
    socket: Socket,
    options: SocketOptions,
}
impl TcpListener {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Self::bind_with_options(address, SocketOptions::default())
    }
    pub fn bind_with_options(address: SocketAddr, options: SocketOptions) -> io::Result<Self> {
        options.validate()?;
        let owner = runtime::current()?;
        let _callbacks = owner.defer_io_callbacks();
        owner.io.borrow().can_open()?;
        let info = owner.driver.borrow_mut().listen(address, &options)?;
        Ok(Self {
            socket: Socket::register(&owner, info)?,
            options,
        })
    }
    pub fn import(socket: OwnedSocket, options: SocketOptions) -> Result<Self, ImportError> {
        Socket::import(socket, SocketKind::TcpListener, options.clone())
            .map(|socket| Self { socket, options })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.info.local_addr
    }
    pub fn accept(&self) -> Accept<'_> {
        Accept {
            listener: self,
            waiter: None,
            done: false,
        }
    }
    /// Serve with bounded supervision and no external stop signal.
    /// Every handler is owned until completion; dropping this wait requests
    /// cancellation of all handlers. For cooperative draining use `serve_until`.
    pub async fn serve<F, Fut>(&self, handler: F) -> io::Result<()>
    where
        F: Fn(TcpStream) -> Fut + std::marker::Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        self.serve_until(ServeConfig::default(), pending(), move |stream, _| {
            handler(stream)
        })
        .await
    }

    /// Stop takes priority over accept and handler completion. A business slot
    /// is reserved before polling accept; idle sockets are then automatically
    /// placed before their first data I/O. Each handler receives the same
    /// cooperative cancellation token.
    ///
    /// Stop or an error cancels the token, drains for `shutdown_grace`, then
    /// aborts and joins any remaining handlers. Execution panics, failed normal
    /// destruction, and import failures are returned, never detached. Expected
    /// abort remains cancellation even if a cancelled handler's destructor panics.
    /// Cancelling this service future requests immediate handler cancellation;
    /// awaiting its normal stop is required to prove all cleanup has finished.
    pub async fn serve_until<S, F, Fut>(
        &self,
        config: ServeConfig,
        stop: S,
        handler: F,
    ) -> io::Result<()>
    where
        S: Future<Output = ()>,
        F: Fn(TcpStream, CancellationToken) -> Fut + std::marker::Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let mut tasks = TaskGroup::new(config.max_connections)?;
        let handle = runtime::Handle::current()?;
        let handler = Arc::new(handler);
        let cancellation = CancellationToken::new();
        let mut stop = pin!(stop);
        let mut error = loop {
            let mut accept = pin!(self.accept());
            let event = poll_fn(|cx| {
                if stop.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(ServeEvent::Stopped);
                }
                if let Poll::Ready(Some(result)) = tasks.poll_join_next(cx) {
                    return Poll::Ready(ServeEvent::Completed(result));
                }
                if !tasks.is_full()
                    && let Poll::Ready(result) = accept.as_mut().poll(cx)
                {
                    return Poll::Ready(ServeEvent::Accepted(result));
                }
                Poll::Pending
            })
            .await;
            let stream = match event {
                ServeEvent::Stopped => break None,
                ServeEvent::Completed(result) => {
                    if let Err(error) = handler_result(result) {
                        break Some(error);
                    }
                    continue;
                }
                ServeEvent::Accepted(Ok(stream)) => stream,
                ServeEvent::Accepted(Err(error)) => break Some(error),
            };
            let socket = match stream.socket.take_idle() {
                Ok(socket) => socket,
                Err(error) => break Some(error),
            };
            let mut options = self.options.clone();
            // Already configured: Android Network/protection hooks must not
            // run again when transferring this accepted socket.
            options.android_network = None;
            options.hook = None;
            let handler = handler.clone();
            let cancellation = cancellation.clone();
            if let Err(error) = tasks.spawn_on(&handle, move || async move {
                let stream = TcpStream::import(socket, options).map_err(|error| error.error)?;
                handler(stream, cancellation).await;
                Ok(())
            }) {
                break Some(io::Error::new(io::ErrorKind::WouldBlock, error));
            }
        };
        cancellation.cancel();
        let drain = async {
            while let Some(result) = tasks.join_next().await {
                let panicked = matches!(&result, Err(JoinError::Panicked));
                if let Err(next) = handler_result(result)
                    && (error.is_none() || panicked)
                {
                    error = Some(next);
                }
            }
        };
        if let Err(TimeoutError::Timer(next)) = time::timeout(config.shutdown_grace, drain).await
            && error.is_none()
        {
            error = Some(next);
        }
        if let Err(panic) = tasks.shutdown().await {
            error = Some(io::Error::other(panic));
        }
        error.map_or(Ok(()), Err)
    }
}
enum ServeEvent {
    Stopped,
    Completed(Result<io::Result<()>, JoinError>),
    Accepted(io::Result<TcpStream>),
}

fn handler_result(result: Result<io::Result<()>, JoinError>) -> io::Result<()> {
    result.map_err(|error| {
        io::Error::new(
            match error {
                JoinError::Cancelled => io::ErrorKind::Interrupted,
                JoinError::Panicked => io::ErrorKind::Other,
            },
            error,
        )
    })?
}
#[must_use]
pub struct Accept<'a> {
    listener: &'a TcpListener,
    waiter: Option<u64>,
    done: bool,
}
impl Future for Accept<'_> {
    type Output = io::Result<TcpStream>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Accept polled after completion");
        let owner = match self.listener.socket.owner() {
            Ok(owner) => owner,
            Err(error) => {
                self.done = true;
                return Poll::Ready(Err(error));
            }
        };
        let key = self.listener.socket.key();
        let mut waker = Some(cx.waker().clone());
        let result =
            owner.with_io(|io, driver| io.poll_accept(driver, key, &mut self.waiter, &mut waker));
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.done = true;
                Poll::Ready(
                    result
                        .and_then(|info| Socket::register(&owner, info))
                        .map(|socket| TcpStream { socket }),
                )
            }
        }
    }
}
impl Drop for Accept<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(waiter)) =
            (self.listener.socket.owner.upgrade(), self.waiter.take())
        {
            owner.cancel_io_waiter(self.listener.socket.key(), waiter, true);
        }
    }
}

#[must_use]
pub struct Recv<'a> {
    socket: &'a Socket,
    waiter: Option<u64>,
    done: bool,
}
impl Future for Recv<'_> {
    type Output = io::Result<Option<ReadBuf>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Recv polled after completion");
        let owner = match self.socket.owner() {
            Ok(owner) => owner,
            Err(error) => {
                self.done = true;
                return Poll::Ready(Err(error));
            }
        };
        let key = self.socket.key();
        let mut waker = Some(cx.waker().clone());
        let result =
            owner.with_io(|io, driver| io.poll_receive(driver, key, &mut self.waiter, &mut waker));
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.done = true;
                Poll::Ready(result.map(|data| data.map(|data| data.data)))
            }
        }
    }
}
impl Drop for Recv<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(waiter)) = (self.socket.owner.upgrade(), self.waiter.take()) {
            owner.cancel_io_waiter(self.socket.key(), waiter, false);
        }
    }
}

#[must_use]
pub struct Send<'a> {
    socket: &'a Socket,
    data: Option<SendPayload>,
    destination: Option<SocketAddr>,
    segment_size: Option<u16>,
    token: Option<Token>,
    group: Option<u64>,
    owner: Option<Rc<Worker>>,
    done: bool,
}
impl<'a> Send<'a> {
    fn new(
        socket: &'a Socket,
        data: SendPayload,
        destination: Option<SocketAddr>,
        segment_size: Option<u16>,
    ) -> Self {
        Self {
            socket,
            data: Some(data),
            destination,
            segment_size,
            token: None,
            group: None,
            owner: None,
            done: false,
        }
    }
}
impl Future for Send<'_> {
    type Output = SendOutcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Send polled after completion");
        let owner = match self.owner.clone() {
            Some(owner) => owner,
            None => match self.socket.owner() {
                Ok(owner) => {
                    self.owner = Some(owner.clone());
                    owner
                }
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(SendOutcome {
                        result: Err(error),
                        data: self.data.take().unwrap(),
                    });
                }
            },
        };
        let mut waker = Some(cx.waker().clone());
        if self.token.is_none() {
            let data = self.data.take().unwrap();
            if let Some(segment) = self.segment_size {
                let segments = if segment == 0 {
                    usize::MAX
                } else {
                    data.len().div_ceil(usize::from(segment))
                };
                if segment == 0 || data.is_empty() || segments > 64 {
                    self.done = true;
                    return Poll::Ready(SendOutcome {
                        result: Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "GSO requires 1..=64 nonempty segments with a nonzero segment size",
                        )),
                        data,
                    });
                }
            }
            let request = SendRequest {
                data,
                destination: self.destination,
                segment_size: self.segment_size,
                group: self.group,
            };
            match owner
                .with_io(|io, driver| io.send(driver, self.socket.key(), request, &mut waker))
            {
                Ok(token) => self.token = Some(token),
                Err(outcome) => {
                    self.done = true;
                    return Poll::Ready(outcome);
                }
            }
        }
        let result = owner.with_io(|io, _| io.poll_send(self.token.unwrap(), &mut waker));
        if result.is_ready() {
            self.token = None;
            self.done = true;
        }
        result
    }
}
impl Drop for Send<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(token)) = (self.owner.as_ref(), self.token.take()) {
            owner.with_io(|io, driver| io.abandon_send(driver, token));
        }
    }
}

#[must_use]
pub struct SendAll<'a> {
    send: Send<'a>,
    accepted: usize,
    group: Option<u64>,
    done: bool,
}
impl Future for SendAll<'_> {
    type Output = SendOutcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "SendAll polled after completion");
        if self.group.is_none()
            && let Ok(owner) = self.send.socket.owner()
        {
            let group = match owner.io.borrow_mut().new_send_group() {
                Ok(group) => group,
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(SendOutcome {
                        result: Err(error),
                        data: self.send.data.take().unwrap(),
                    });
                }
            };
            self.group = Some(group);
            self.send.group = Some(group);
        }
        for _ in 0..32 {
            let outcome = match Pin::new(&mut self.send).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(outcome) => outcome,
            };
            match outcome.result {
                Err(error) => {
                    self.release_group();
                    self.done = true;
                    return Poll::Ready(SendOutcome {
                        result: Err(error),
                        data: outcome.data,
                    });
                }
                Ok(bytes) => {
                    if bytes > outcome.data.len() {
                        panic!("backend reported more sent bytes than submitted");
                    }
                    if bytes == 0 && !outcome.data.is_empty() {
                        self.release_group();
                        self.done = true;
                        return Poll::Ready(SendOutcome {
                            result: Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "TCP send made no progress",
                            )),
                            data: outcome.data,
                        });
                    }
                    self.accepted += bytes;
                    let remaining = outcome.data.remaining(bytes);
                    if remaining.is_empty() {
                        self.release_group();
                        self.done = true;
                        return Poll::Ready(SendOutcome {
                            result: Ok(self.accepted),
                            data: remaining,
                        });
                    }
                    self.send = Send::new(self.send.socket, remaining, None, None);
                    self.send.group = self.group;
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl SendAll<'_> {
    fn release_group(&mut self) {
        if let (Some(owner), Some(group)) = (self.send.socket.owner.upgrade(), self.group.take()) {
            owner.with_io(|io, driver| io.finish_send_group(driver, self.send.socket.key(), group));
        }
    }
}
impl Drop for SendAll<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(token)) = (self.send.owner.as_ref(), self.send.token.take()) {
            owner.with_io(|io, driver| io.abandon_send(driver, token));
        }
        self.release_group();
    }
}

#[derive(Debug)]
pub struct UdpSocket {
    socket: Socket,
}
impl UdpSocket {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Self::bind_with_options(address, SocketOptions::udp())
    }
    pub fn bind_with_options(address: SocketAddr, options: SocketOptions) -> io::Result<Self> {
        Self::bind_inner(address, None, options)
    }
    pub fn bind_connected(
        local: SocketAddr,
        peer: SocketAddr,
        options: SocketOptions,
    ) -> io::Result<Self> {
        Self::bind_inner(local, Some(peer), options)
    }
    fn bind_inner(
        address: SocketAddr,
        peer: Option<SocketAddr>,
        options: SocketOptions,
    ) -> io::Result<Self> {
        options.validate()?;
        if peer.is_some_and(|peer| peer.is_ipv4() != address.is_ipv4()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP local and peer address families differ",
            ));
        }
        let owner = runtime::current()?;
        let _callbacks = owner.defer_io_callbacks();
        owner.io.borrow().can_open_kind(SocketKind::Udp)?;
        let info = owner
            .driver
            .borrow_mut()
            .bind_udp(address, peer, &options)?;
        Ok(Self {
            socket: Socket::register(&owner, info)?,
        })
    }
    pub fn import(socket: OwnedSocket, options: SocketOptions) -> Result<Self, ImportError> {
        Socket::import(socket, SocketKind::Udp, options).map(|socket| Self { socket })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.info.local_addr
    }
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.socket.info.peer_addr
    }
    /// Observe this socket's queued receives and existing backend receive state.
    ///
    /// Requires the owning worker, with the same context/lifetime errors as I/O.
    /// Callback reentry during Core/driver mutation returns `WouldBlock`.
    /// This does not register a waiter, start receiving, harvest completions,
    /// flush pending credits, rearm lanes, recycle storage or allocate.
    ///
    /// Core queue availability may differ from backend publication credits while
    /// an update is pending. Native counts describe software bookkeeping, not
    /// instantaneous kernel/NIC capacity. RIO-specific state is optional.
    pub fn receive_snapshot(&self) -> io::Result<crate::diagnostics::ReceiveResources> {
        let owner = self.socket.owner()?;
        let core = owner
            .io
            .try_borrow()
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        let driver = owner
            .driver
            .try_borrow()
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        core.receive_snapshot(
            self.socket.key(),
            driver.capabilities().worker,
            driver.receive_snapshot(self.socket.info.id)?,
        )
    }

    pub fn recv(&self) -> RecvDatagram<'_> {
        RecvDatagram {
            socket: &self.socket,
            waiter: None,
            done: false,
        }
    }
    pub fn send(&self, data: SendPayload) -> Send<'_> {
        Send::new(&self.socket, data, None, None)
    }
    pub fn send_to(&self, data: SendPayload, destination: SocketAddr) -> Send<'_> {
        Send::new(&self.socket, data, Some(destination), None)
    }
    /// Every segment has the same destination and socket options by construction.
    /// Payload length determines the final (possibly short) segment. Vectored
    /// leases are submitted directly; no payload concatenation is performed.
    pub fn send_segments(
        &self,
        data: SendPayload,
        segment_size: u16,
        destination: Option<SocketAddr>,
    ) -> Send<'_> {
        Send::new(&self.socket, data, destination, Some(segment_size))
    }
    pub fn recv_batch<'a>(&'a self, output: &'a mut [Option<Received>]) -> RecvBatch<'a> {
        RecvBatch {
            socket: &self.socket,
            output,
            waiter: None,
            done: false,
        }
    }
    pub fn send_batch<'a>(&'a self, packets: &'a mut [Datagram]) -> SendBatch<'a> {
        SendBatch {
            socket: &self.socket,
            packets,
            owner: None,
            done: false,
        }
    }
}

impl DatagramRecv for UdpSocket {
    fn recv(&self) -> impl Future<Output = io::Result<Received>> {
        UdpSocket::recv(self)
    }
}

impl DatagramSend for UdpSocket {
    fn send(&self, data: SendPayload) -> impl Future<Output = SendOutcome> {
        UdpSocket::send(self, data)
    }

    fn send_to(
        &self,
        data: SendPayload,
        destination: SocketAddr,
    ) -> impl Future<Output = SendOutcome> {
        UdpSocket::send_to(self, data, destination)
    }
}

#[must_use]
pub struct RecvDatagram<'a> {
    socket: &'a Socket,
    waiter: Option<u64>,
    done: bool,
}
impl Future for RecvDatagram<'_> {
    type Output = io::Result<Received>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "RecvDatagram polled after completion");
        let owner = match self.socket.owner() {
            Ok(owner) => owner,
            Err(error) => {
                self.done = true;
                return Poll::Ready(Err(error));
            }
        };
        let key = self.socket.key();
        let mut waker = Some(cx.waker().clone());
        let result =
            owner.with_io(|io, driver| io.poll_receive(driver, key, &mut self.waiter, &mut waker));
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.done = true;
                Poll::Ready(result.and_then(|result| {
                    result.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "UDP backend emitted stream EOF")
                    })
                }))
            }
        }
    }
}
impl Drop for RecvDatagram<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(waiter)) = (self.socket.owner.upgrade(), self.waiter.take()) {
            owner.cancel_io_waiter(self.socket.key(), waiter, false);
        }
    }
}
#[must_use]
pub struct RecvBatch<'a> {
    socket: &'a Socket,
    output: &'a mut [Option<Received>],
    waiter: Option<u64>,
    done: bool,
}
impl Future for RecvBatch<'_> {
    type Output = io::Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "RecvBatch polled after completion");
        if self.output.is_empty() {
            self.done = true;
            return Poll::Ready(Ok(0));
        }
        if self.output.iter().any(Option::is_some) {
            self.done = true;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "receive batch output slots must be empty",
            )));
        }
        let owner = match self.socket.owner() {
            Ok(owner) => owner,
            Err(error) => {
                self.done = true;
                return Poll::Ready(Err(error));
            }
        };
        let key = self.socket.key();
        let mut count = 0;
        while count < self.output.len() {
            let mut waker = Some(cx.waker().clone());
            let result = owner
                .with_io(|io, driver| io.poll_receive(driver, key, &mut self.waiter, &mut waker));
            match result {
                Poll::Pending if count == 0 => return Poll::Pending,
                Poll::Pending => break,
                Poll::Ready(Ok(Some(packet))) => {
                    self.output[count] = Some(packet);
                    count += 1;
                }
                Poll::Ready(Err(error)) if count == 0 => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(error)) => {
                    // A partial batch remains visible; retain this error for
                    // the next receive rather than hiding it behind the count.
                    owner.with_io(|io, driver| io.restore_receive_error(driver, key, error));
                    break;
                }
                Poll::Ready(Ok(None)) => {
                    self.done = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UDP backend emitted EOF",
                    )));
                }
            }
        }
        if let Some(waiter) = self.waiter.take() {
            owner.cancel_io_waiter(key, waiter, false);
        }
        self.done = true;
        Poll::Ready(Ok(count))
    }
}
impl Drop for RecvBatch<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(waiter)) = (self.socket.owner.upgrade(), self.waiter.take()) {
            owner.cancel_io_waiter(self.socket.key(), waiter, false);
        }
    }
}

/// Reusable batch metadata. Each packet retains its own destination, result and
/// immutable payload ownership; a zero-byte packet is a real datagram.
pub struct Datagram {
    data: Option<SendPayload>,
    destination: Option<SocketAddr>,
    outcome: Option<SendOutcome>,
    token: Option<Token>,
}
impl Datagram {
    pub fn new(data: SendPayload, destination: Option<SocketAddr>) -> Self {
        Self {
            data: Some(data),
            destination,
            outcome: None,
            token: None,
        }
    }
    pub fn outcome(&self) -> Option<&SendOutcome> {
        self.outcome.as_ref()
    }
    pub fn take_outcome(&mut self) -> Option<SendOutcome> {
        self.outcome.take()
    }
    pub fn reset(&mut self, data: SendPayload, destination: Option<SocketAddr>) {
        assert!(self.token.is_none(), "cannot reset an in-flight datagram");
        self.data = Some(data);
        self.destination = destination;
        self.outcome = None;
    }
}
#[must_use]
pub struct SendBatch<'a> {
    socket: &'a Socket,
    packets: &'a mut [Datagram],
    owner: Option<Rc<Worker>>,
    done: bool,
}
impl Future for SendBatch<'_> {
    type Output = io::Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "SendBatch polled after completion");
        let owner = match self.owner.clone() {
            Some(owner) => owner,
            None => match self.socket.owner() {
                Ok(owner) => {
                    self.owner = Some(owner.clone());
                    owner
                }
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
            },
        };
        let key = self.socket.key();
        let mut complete = 0;
        for packet in self.packets.iter_mut() {
            let mut waker =
                (packet.data.is_some() || packet.token.is_some()).then(|| cx.waker().clone());
            if let Some(data) = packet.data.take() {
                let request = SendRequest {
                    data,
                    destination: packet.destination,
                    segment_size: None,
                    group: None,
                };
                match owner.with_io(|io, driver| io.send(driver, key, request, &mut waker)) {
                    Ok(token) => packet.token = Some(token),
                    Err(outcome) => packet.outcome = Some(outcome),
                }
            }
            if let Some(token) = packet.token
                && let Poll::Ready(outcome) = owner.with_io(|io, _| io.poll_send(token, &mut waker))
            {
                packet.token = None;
                packet.outcome = Some(outcome);
            }
            if packet.token.is_none() {
                complete += 1;
            }
        }
        if complete == self.packets.len() {
            self.done = true;
            Poll::Ready(Ok(complete))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for SendBatch<'_> {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.as_ref() {
            for packet in self.packets.iter_mut() {
                if let Some(token) = packet.token.take() {
                    owner.with_io(|io, driver| io.abandon_send(driver, token));
                }
            }
        }
    }
}

#[must_use]
pub struct Splice<'a> {
    source: &'a Socket,
    destination: &'a Socket,
    bytes: usize,
    token: Option<Token>,
    done: bool,
}
impl Future for Splice<'_> {
    type Output = io::Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "Splice polled after completion");
        let owner = match self.source.owner().and_then(|owner| {
            let destination = self.destination.owner()?;
            if !Rc::ptr_eq(&owner, &destination) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "splice sockets must share an owner",
                ));
            }
            Ok(owner)
        }) {
            Ok(owner) => owner,
            Err(error) => {
                self.done = true;
                return Poll::Ready(Err(error));
            }
        };
        let mut waker = Some(cx.waker().clone());
        if self.token.is_none() {
            match owner.with_io(|io, driver| {
                io.splice(
                    driver,
                    self.source.key(),
                    self.destination.key(),
                    self.bytes,
                    &mut waker,
                )
            }) {
                Ok(token) => self.token = Some(token),
                Err(error) => {
                    self.done = true;
                    return Poll::Ready(Err(error));
                }
            }
        }
        let result = owner.with_io(|io, _| io.poll_splice(self.token.unwrap(), &mut waker));
        if result.is_ready() {
            self.token = None;
            self.done = true;
        }
        result
    }
}
impl Drop for Splice<'_> {
    fn drop(&mut self) {
        if let (Some(owner), Some(token)) = (self.source.owner.upgrade(), self.token.take()) {
            owner.with_io(|io, driver| io.abandon_splice(driver, token));
        }
    }
}
/// Forward both directions without userspace payload processing. EOF shuts down
/// only the opposite write half; the reverse direction keeps draining to EOF.
pub async fn splice_bidirectional(left: &TcpStream, right: &TcpStream) -> io::Result<(u64, u64)> {
    async fn direction(source: &TcpStream, destination: &TcpStream) -> io::Result<u64> {
        let mut total = 0u64;
        loop {
            let bytes = source.splice_to(destination, 1024 * 1024).await?;
            if bytes == 0 {
                destination.shutdown(Shutdown::Write)?;
                return Ok(total);
            }
            total = total
                .checked_add(bytes as u64)
                .ok_or_else(|| io::Error::other("splice byte counter overflow"))?;
        }
    }
    let mut forward = std::pin::pin!(direction(left, right));
    let mut reverse = std::pin::pin!(direction(right, left));
    let mut forward_result = None;
    let mut reverse_result = None;
    std::future::poll_fn(|cx| {
        if forward_result.is_none()
            && let Poll::Ready(result) = forward.as_mut().poll(cx)
        {
            match result {
                Ok(bytes) => forward_result = Some(bytes),
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        if reverse_result.is_none()
            && let Poll::Ready(result) = reverse.as_mut().poll(cx)
        {
            match result {
                Ok(bytes) => reverse_result = Some(bytes),
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        match (forward_result, reverse_result) {
            (Some(a), Some(b)) => Poll::Ready(Ok((a, b))),
            _ => Poll::Pending,
        }
    })
    .await
}
