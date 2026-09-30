//! Android API 23+ readiness backend. Only synchronous nonblocking socket
//! syscalls touch payload memory; the kernel never retains a completed lease.
mod notifier;
mod socket;
mod udp;

pub(crate) use notifier::Notifier;

use super::{Arena, Event, Received, SendOutcome, SocketId, SocketInfo, SocketKind, Token};
use crate::{
    buffer::{BufferPool, SendPayload},
    capability::{CapabilityReport, ZcStats},
    config::{Optimization, Policy, RuntimeConfig},
    diagnostics::DriverResources,
    socket::{ImportError, OwnedSocket, SocketOptions},
};
use std::{
    collections::VecDeque,
    io,
    marker::PhantomData,
    mem,
    net::{Shutdown, SocketAddr},
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};

const WAKE: u64 = 0;
const EDGE: u32 = (libc::EPOLLET | libc::EPOLLRDHUP) as u32;

/// Android has no cross-worker kernel queues or shared socket state. This
/// immutable topology is only used to validate the automatic worker placement.
pub(crate) struct Shared {
    workers: usize,
}
impl Shared {
    pub fn new(workers: usize) -> Self {
        Self { workers }
    }
}

struct SocketState {
    socket: OwnedSocket,
    kind: SocketKind,
    local: SocketAddr,
    peer: Option<SocketAddr>,
    options: SocketOptions,
    connect: Option<Token>,
    accept: Option<Token>,
    recv: Option<Token>,
    accept_credit: usize,
    recv_credit: usize,
    sends_first: Option<u64>,
    sends_last: Option<u64>,
    interest: u32,
    read_ready: bool,
    write_ready: bool,
    read_turn: bool,
    memory_blocked: bool,
    accept_blocked: bool,
    touched: bool,
    queued: bool,
    previous: Option<u64>,
    next: Option<u64>,
}

struct PendingSend {
    socket: SocketId,
    token: Token,
    data: SendPayload,
    destination: Option<SocketAddr>,
    #[cfg(feature = "udp-gso")]
    segment_size: Option<u16>,
    previous: Option<u64>,
    next: Option<u64>,
}

pub(crate) struct Driver {
    epoll: OwnedFd,
    notifier: Arc<Notifier>,
    pool: BufferPool,
    capabilities: CapabilityReport,
    sockets: Arena<SocketState>,
    sends: Arena<PendingSend>,
    pending: VecDeque<Event>,
    delivery: Option<Event>,
    prefer_pending: bool,
    blocked: Vec<u64>,
    kernel_events: Box<[libc::epoll_event]>,
    iovecs: Vec<libc::iovec>,
    control: udp::Control,
    ready_first: Option<u64>,
    ready_last: Option<u64>,
    operations: usize,
    max_operations: usize,
    completion_budget: usize,
    max_receive_credit: usize,
    max_accept_credit: usize,
    send_bytes: usize,
    max_send_bytes: usize,
    max_iovecs: usize,
    shutting_down: bool,
    local: PhantomData<Rc<()>>,
}

impl Driver {
    pub fn new(
        config: &RuntimeConfig,
        worker: usize,
        pool: BufferPool,
        notifier: Arc<Notifier>,
        shared: Arc<Shared>,
    ) -> io::Result<Self> {
        socket::require_api()?;
        if worker >= shared.workers || shared.workers != config.workers {
            return Err(invalid("invalid Android worker topology"));
        }
        let mut capabilities = CapabilityReport::new("android-epoll", worker);
        for optimization in [Optimization::UdpGso, Optimization::UdpGro] {
            let policy = config.policy(optimization);
            let support = if policy != Policy::Off && optimization.compiled() {
                match optimization {
                    #[cfg(feature = "udp-gso")]
                    Optimization::UdpGso => udp::probe(udp::Offload::Gso),
                    #[cfg(feature = "udp-gro")]
                    Optimization::UdpGro => udp::probe(udp::Offload::Gro),
                    _ => Err("UDP offload implementation is not compiled".to_owned()),
                }
            } else {
                Err("UDP offload was not selected and probed".to_owned())
            };
            capabilities.decide(optimization, policy, support)?;
        }
        // No Linux io_uring probing, and no fallback to another global backend.
        capabilities.finish(config)?;
        let epoll = socket::cvt(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
        let epoll = unsafe { OwnedFd::from_raw_fd(epoll) };
        let mut wake = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLET) as u32,
            u64: WAKE,
        };
        socket::cvt(unsafe {
            libc::epoll_ctl(
                epoll.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                notifier.fd(),
                &mut wake,
            )
        })?;
        let limits = &config.limits;
        let event_capacity = limits.completion_budget.min(i32::MAX as usize);
        if event_capacity == 0 {
            return Err(invalid("completion budget must be nonzero"));
        }
        Ok(Self {
            epoll,
            notifier,
            pool,
            capabilities,
            sockets: Arena::new(limits.max_sockets),
            sends: Arena::new(limits.max_operations),
            pending: VecDeque::with_capacity(limits.max_operations),
            delivery: None,
            prefer_pending: false,
            blocked: Vec::with_capacity(limits.max_sockets),
            kernel_events: (0..event_capacity)
                .map(|_| libc::epoll_event { events: 0, u64: 0 })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            iovecs: Vec::with_capacity(limits.max_iovecs),
            control: udp::Control::new(),
            ready_first: None,
            ready_last: None,
            operations: 0,
            max_operations: limits.max_operations,
            completion_budget: limits.completion_budget,
            max_receive_credit: limits.max_pending_receives,
            max_accept_credit: limits.max_pending_accepts,
            send_bytes: 0,
            max_send_bytes: limits.max_send_bytes,
            max_iovecs: limits.max_iovecs,
            shutting_down: false,
            local: PhantomData,
        })
    }

    pub fn capabilities(&self) -> &CapabilityReport {
        &self.capabilities
    }
    pub fn zc_stats(&self) -> ZcStats {
        ZcStats::default()
    }

    /// Operations use the existing logical admission budget, including queued
    /// terminal results until publication. Software completions count those
    /// queued events and any synchronous result awaiting delivery, not epoll
    /// readiness entries. Close removes socket storage synchronously, so there
    /// are no retained closing sockets; asynchronous native ownership and RIO
    /// measurements do not apply to this backend.
    pub fn resource_snapshot(&self) -> DriverResources {
        DriverResources {
            sockets: self.sockets.len(),
            available_socket_slots: self.sockets.available(),
            operations: self.operations,
            available_operation_slots: self.max_operations - self.operations,
            pending_completions: self.pending.len() + usize::from(self.delivery.is_some()),
            closing_sockets: 0,
            native_outstanding: None,
            retiring_native: None,
            rio_receive_queue_slots: None,
            udp_rearm_allocation_failures_total: None,
        }
    }

    /// Reports installed publication credits without running a receive syscall.
    /// A synchronous readiness backend has no asynchronous native receive count.
    pub fn receive_snapshot(&self, socket: SocketId) -> io::Result<crate::driver::ReceiveState> {
        let state = self.get(socket)?;
        Ok(crate::driver::ReceiveState {
            publication_credits: state.recv_credit,
            native_outstanding: None,
            rio: None,
        })
    }

    pub fn listen(&mut self, addr: SocketAddr, options: &SocketOptions) -> io::Result<SocketInfo> {
        self.check_creation()?;
        let socket = socket::create(addr, SocketKind::TcpListener)?;
        socket::configure(
            &socket,
            SocketKind::TcpListener,
            addr.is_ipv6(),
            options,
            false,
        )?;
        socket::bind(socket.as_raw_fd(), addr)?;
        socket::cvt(unsafe { libc::listen(socket.as_raw_fd(), options.backlog) })?;
        self.insert_socket(socket, SocketKind::TcpListener, options.clone())
            .map_err(|error| error.error)
    }

    pub fn bind_udp(
        &mut self,
        addr: SocketAddr,
        peer: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> io::Result<SocketInfo> {
        self.check_creation()?;
        let socket = socket::create(addr, SocketKind::Udp)?;
        socket::configure(&socket, SocketKind::Udp, addr.is_ipv6(), options, false)?;
        self.configure_udp(socket.as_raw_fd())?;
        socket::bind(socket.as_raw_fd(), addr)?;
        if let Some(peer) = peer
            && !socket::connect(socket.as_raw_fd(), peer)?
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "UDP connect did not complete synchronously",
            ));
        }
        self.insert_socket(socket, SocketKind::Udp, options.clone())
            .map_err(|error| error.error)
    }

    pub fn connect(
        &mut self,
        token: Token,
        addr: SocketAddr,
        local: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> io::Result<()> {
        self.check_creation()?;
        self.check_operation()?;
        socket::validate_unicast(addr)?;
        let socket = socket::create(addr, SocketKind::TcpStream)?;
        socket::configure(
            &socket,
            SocketKind::TcpStream,
            addr.is_ipv6(),
            options,
            false,
        )?;
        if let Some(local) = local {
            socket::bind(socket.as_raw_fd(), local)?;
        }
        let ready = socket::connect(socket.as_raw_fd(), addr)?;
        let info = self
            .insert_socket(socket, SocketKind::TcpStream, options.clone())
            .map_err(|error| error.error)?;
        self.operations += 1;
        let state = self.sockets.get_mut(info.id.0).unwrap();
        state.connect = Some(token);
        state.peer = Some(addr);
        if ready {
            self.finish_connect(info.id.0, Ok(()));
        } else if let Err(error) = self.update_interest(info.id.0) {
            self.finish_connect(info.id.0, Err(error));
        }
        Ok(())
    }

    pub fn import(
        &mut self,
        socket: OwnedSocket,
        kind: SocketKind,
        options: &SocketOptions,
    ) -> Result<SocketInfo, ImportError> {
        let prepare = || -> io::Result<()> {
            self.check_creation()?;
            let (local, peer) = socket::validate_import(socket.as_raw_fd(), kind)?;
            if peer.is_some() && options.android_network.is_some() {
                return Err(invalid(
                    "Android Network binding must precede connection; import connected sockets without a new binding request",
                ));
            }
            if kind != SocketKind::Udp {
                crate::socket::reject_blocking_linger(&socket2::SockRef::from(&socket))?;
            }
            socket::configure(&socket, kind, local.is_ipv6(), options, true)?;
            if kind == SocketKind::Udp {
                self.configure_udp(socket.as_raw_fd())?;
            }
            Ok(())
        };
        if let Err(error) = prepare() {
            return Err(ImportError { error, socket });
        }
        let fd = socket.as_raw_fd();
        let old_status = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        let old_descriptor = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if old_status < 0 || old_descriptor < 0 {
            return Err(ImportError {
                error: io::Error::last_os_error(),
                socket,
            });
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFL, old_status | libc::O_NONBLOCK) } < 0 {
            return Err(ImportError {
                error: io::Error::last_os_error(),
                socket,
            });
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFD, old_descriptor | libc::FD_CLOEXEC) } < 0 {
            let error = io::Error::last_os_error();
            unsafe {
                libc::fcntl(fd, libc::F_SETFL, old_status);
            }
            return Err(ImportError { error, socket });
        }
        match self.insert_socket(socket, kind, options.clone()) {
            Ok(info) => {
                // Prior external I/O cannot be disproved; arbitrary imports
                // are not eligible for the initial accepted-socket handoff.
                self.sockets.get_mut(info.id.0).unwrap().touched = true;
                Ok(info)
            }
            Err(error) => {
                unsafe {
                    libc::fcntl(error.socket.as_raw_fd(), libc::F_SETFL, old_status);
                    libc::fcntl(error.socket.as_raw_fd(), libc::F_SETFD, old_descriptor);
                }
                Err(error)
            }
        }
    }

    pub fn take_idle_socket(&mut self, socket: SocketId) -> io::Result<OwnedSocket> {
        let state = self.get(socket)?;
        if state.kind != SocketKind::TcpStream
            || state.touched
            || state.connect.is_some()
            || state.recv.is_some()
            || state.sends_first.is_some()
        {
            return Err(invalid(
                "only an untouched, connected TCP socket can be transferred",
            ));
        }
        socket::cvt(unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                libc::EPOLL_CTL_DEL,
                state.socket.as_raw_fd(),
                std::ptr::null_mut(),
            )
        })?;
        self.unlink_ready(socket.0);
        Ok(self.sockets.remove(socket.0).unwrap().socket)
    }

    pub fn start_accept(&mut self, socket: SocketId, token: Token) -> io::Result<()> {
        self.check_operation()?;
        let state = self.get(socket)?;
        if state.kind != SocketKind::TcpListener || state.accept.is_some() {
            return Err(invalid("accept requires an inactive TCP listener"));
        }
        let state = self.sockets.get_mut(socket.0).unwrap();
        state.accept = Some(token);
        state.accept_credit = 0;
        state.read_ready = true;
        state.touched = true;
        if let Err(error) = self.update_interest(socket.0) {
            self.sockets.get_mut(socket.0).unwrap().accept = None;
            return Err(error);
        }
        self.operations += 1;
        self.enqueue_ready(socket.0);
        Ok(())
    }

    pub fn start_recv(&mut self, socket: SocketId, token: Token) -> io::Result<()> {
        self.check_operation()?;
        let state = self.get(socket)?;
        if state.kind == SocketKind::TcpListener || state.recv.is_some() || state.connect.is_some()
        {
            return Err(invalid(
                "receive requires an established TCP/UDP socket without another receive operation",
            ));
        }
        let state = self.sockets.get_mut(socket.0).unwrap();
        state.recv = Some(token);
        state.recv_credit = 0;
        state.read_ready = true;
        state.memory_blocked = false;
        state.touched = true;
        if let Err(error) = self.update_interest(socket.0) {
            self.sockets.get_mut(socket.0).unwrap().recv = None;
            return Err(error);
        }
        self.operations += 1;
        self.enqueue_ready(socket.0);
        Ok(())
    }

    pub fn receive_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()> {
        if slots > self.max_receive_credit {
            return Err(invalid("receive capacity exceeds the configured bound"));
        }
        if self.get(socket)?.kind == SocketKind::TcpListener {
            return Err(invalid("receive capacity requires a TCP/UDP data socket"));
        }
        self.sockets.get_mut(socket.0).unwrap().recv_credit = slots;
        self.update_interest(socket.0)?;
        self.unlink_ready(socket.0);
        self.enqueue_ready(socket.0);
        Ok(())
    }

    pub fn accept_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()> {
        if slots > self.max_accept_credit {
            return Err(invalid("accept capacity exceeds the configured bound"));
        }
        if self.get(socket)?.kind != SocketKind::TcpListener {
            return Err(invalid("accept capacity requires a TCP listener"));
        }
        self.sockets.get_mut(socket.0).unwrap().accept_credit = slots;
        self.update_interest(socket.0)?;
        self.unlink_ready(socket.0);
        self.enqueue_ready(socket.0);
        Ok(())
    }

    pub fn send(
        &mut self,
        socket: SocketId,
        token: Token,
        data: SendPayload,
        destination: Option<SocketAddr>,
        segment_size: Option<u16>,
    ) -> Result<(), SendOutcome> {
        if let Err(error) = self.validate_send(socket, &data, destination, segment_size) {
            return Err(SendOutcome {
                result: Err(error),
                data,
            });
        }
        let size = data.len();
        let previous = self.sockets.get(socket.0).unwrap().sends_last;
        let operation = PendingSend {
            socket,
            token,
            data,
            destination,
            #[cfg(feature = "udp-gso")]
            segment_size,
            previous,
            next: None,
        };
        let key = match self.sends.insert(operation) {
            Ok(key) => key,
            Err(operation) => {
                return Err(SendOutcome {
                    result: Err(exhausted("send operation limit reached")),
                    data: operation.data,
                });
            }
        };
        self.operations += 1;
        self.send_bytes += size;
        if let Some(previous) = previous {
            self.sends.get_mut(previous).unwrap().next = Some(key);
        }
        let state = self.sockets.get_mut(socket.0).unwrap();
        state.sends_last = Some(key);
        state.touched = true;
        if previous.is_none() {
            state.sends_first = Some(key);
            state.write_ready = true;
            // Most sends complete here without epoll_ctl or epoll_wait.
            self.process_send(socket.0);
            if let Err(error) = self.update_interest(socket.0)
                && self.sends.get(key).is_some()
            {
                self.finish_send(key, Err(error));
            }
        }
        self.enqueue_ready(socket.0);
        Ok(())
    }

    pub fn splice(
        &mut self,
        _token: Token,
        _source: SocketId,
        _destination: SocketId,
        _bytes: usize,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TCP splice is a Linux io_uring optimization, not an Android capability",
        ))
    }

    pub fn shutdown(&mut self, socket: SocketId, how: Shutdown) -> io::Result<()> {
        let state = self.get(socket)?;
        if state.kind != SocketKind::TcpStream {
            return Err(invalid("half-close requires a TCP stream"));
        }
        let how = match how {
            Shutdown::Read => libc::SHUT_RD,
            Shutdown::Write => libc::SHUT_WR,
            Shutdown::Both => libc::SHUT_RDWR,
        };
        socket::cvt(unsafe { libc::shutdown(state.socket.as_raw_fd(), how) })?;
        let state = self.sockets.get_mut(socket.0).unwrap();
        state.read_ready = true;
        state.write_ready = true;
        self.enqueue_ready(socket.0);
        Ok(())
    }

    pub fn abort(&mut self, socket: SocketId) -> io::Result<()> {
        let state = self.get(socket)?;
        if state.kind != SocketKind::TcpStream {
            return Err(invalid("abortive close requires a TCP stream"));
        }
        socket2::SockRef::from(&state.socket).set_linger(Some(Duration::ZERO))?;
        // An imported socket may still have host-owned aliases. Disconnect the
        // underlying TCP connection now; closing our fd need not be the last close.
        // Older SELinux hooks validate length using the socket's family before
        // handling AF_UNSPEC, so supply a full IPv6 address for either family.
        let address = libc::sockaddr_in6 {
            sin6_family: libc::AF_UNSPEC as libc::sa_family_t,
            sin6_port: 0,
            sin6_flowinfo: 0,
            sin6_addr: libc::in6_addr { s6_addr: [0; 16] },
            sin6_scope_id: 0,
        };
        socket::cvt(unsafe {
            libc::connect(
                state.socket.as_raw_fd(),
                std::ptr::from_ref(&address).cast(),
                mem::size_of_val(&address) as libc::socklen_t,
            )
        })?;
        self.close(socket)
    }

    pub fn close(&mut self, socket: SocketId) -> io::Result<()> {
        let Some(state) = self.sockets.get(socket.0) else {
            return Ok(());
        };
        let half_close = if state.kind == SocketKind::TcpStream {
            socket::half_close(state.socket.as_raw_fd())
        } else {
            Ok(())
        };
        let result = socket::cvt(unsafe {
            libc::epoll_ctl(
                self.epoll.as_raw_fd(),
                libc::EPOLL_CTL_DEL,
                state.socket.as_raw_fd(),
                std::ptr::null_mut(),
            )
        })
        .map(|_| ());
        self.unlink_ready(socket.0);
        self.blocked.retain(|key| *key != socket.0);
        let mut state = self.sockets.remove(socket.0).unwrap();
        if let Some(token) = state.connect.take() {
            self.queue(Event::Connected {
                token,
                result: Err(cancelled()),
            });
        }
        for token in [state.recv.take(), state.accept.take()]
            .into_iter()
            .flatten()
        {
            self.queue(Event::Stopped {
                token,
                result: Ok(()),
            });
        }
        let mut send = state.sends_first;
        while let Some(key) = send {
            let operation = self.sends.remove(key).unwrap();
            send = operation.next;
            self.queue(Event::Sent {
                token: operation.token,
                outcome: SendOutcome {
                    result: Err(cancelled()),
                    data: operation.data,
                },
                memory_released: true,
            });
        }
        // No kernel-owned payload references survive any nonblocking syscall.
        drop(state);
        result.and(half_close)
    }

    pub fn cancel(&mut self, token: Token) -> io::Result<()> {
        let socket = self.sockets.iter().find_map(|(key, state)| {
            (state.connect == Some(token)
                || state.recv == Some(token)
                || state.accept == Some(token))
            .then_some(key)
        });
        if let Some(key) = socket {
            if self.sockets.get(key).unwrap().connect == Some(token) {
                return self.close(SocketId(key));
            }
            let state = self.sockets.get_mut(key).unwrap();
            if state.recv == Some(token) {
                state.recv = None;
                state.memory_blocked = false;
            }
            if state.accept == Some(token) {
                state.accept = None;
                state.accept_blocked = false;
            }
            self.queue(Event::Stopped {
                token,
                result: Ok(()),
            });
            self.update_interest(key)?;
            self.unlink_ready(key);
            self.enqueue_ready(key);
            return Ok(());
        }
        let send = self
            .sends
            .iter()
            .find_map(|(key, operation)| (operation.token == token).then_some(key));
        if let Some(key) = send {
            let socket = self.sends.get(key).unwrap().socket.0;
            self.finish_send(key, Err(cancelled()));
            self.update_interest(socket)?;
            self.enqueue_ready(socket);
        }
        // An already queued result is not retracted by a late cancellation.
        Ok(())
    }

    pub fn poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()> {
        self.pool.flush_recycles();
        self.retry_blocked();
        let start = events.len();
        self.service_ready(events, start)?;
        if events.len() - start == self.completion_budget {
            return Ok(());
        }
        let wait = if events.len() > start || self.ready_first.is_some() || !self.pending.is_empty()
        {
            Some(Duration::ZERO)
        } else {
            timeout
        };
        let deadline = wait.and_then(|duration| Instant::now().checked_add(duration));
        let count = loop {
            let remaining =
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
            let milliseconds = match remaining.or(wait) {
                None => -1,
                Some(duration) if duration.is_zero() => 0,
                Some(duration) => duration
                    .as_millis()
                    .saturating_add(u128::from(duration.subsec_nanos() % 1_000_000 != 0))
                    .min(i32::MAX as u128) as i32,
            };
            let count = unsafe {
                libc::epoll_wait(
                    self.epoll.as_raw_fd(),
                    self.kernel_events.as_mut_ptr(),
                    self.kernel_events.len() as i32,
                    milliseconds,
                )
            };
            if count >= 0 {
                break count as usize;
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                return Err(error);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                break 0;
            }
        };
        for index in 0..count {
            let key = self.kernel_events[index].u64;
            let flags = self.kernel_events[index].events;
            if key == WAKE {
                self.notifier.drain();
                continue;
            }
            if let Some(state) = self.sockets.get_mut(key) {
                if flags
                    & (libc::EPOLLIN | libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32
                    != 0
                {
                    state.read_ready = true;
                }
                if flags & (libc::EPOLLOUT | libc::EPOLLHUP | libc::EPOLLERR) as u32 != 0 {
                    state.write_ready = true;
                }
                self.enqueue_ready(key);
            }
        }
        self.pool.flush_recycles();
        self.retry_blocked();
        self.service_ready(events, start)
    }

    pub fn is_idle(&self) -> bool {
        self.operations == 0 && self.pending.is_empty()
    }

    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
        loop {
            let next = self.sockets.iter().next().map(|(key, _)| key);
            let Some(key) = next else {
                break;
            };
            // Closing the owned fd still completes every local request if the
            // epoll deletion itself reports an error during shutdown.
            let _ = self.close(SocketId(key));
        }
    }

    fn configure_udp(&self, fd: RawFd) -> io::Result<()> {
        // Imported sockets and host hooks may carry offload defaults. Plain
        // sends must not inherit implicit segmentation. Preserve inherited GRO:
        // disabling it can hide the boundaries of already queued aggregates.
        match socket::get_int(fd, libc::IPPROTO_UDP, udp::UDP_SEGMENT) {
            Ok(current) if current != 0 => {
                socket::set_int(fd, libc::IPPROTO_UDP, udp::UDP_SEGMENT, 0)?
            }
            Ok(_) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENOPROTOOPT) => {}
            Err(error) => return Err(error),
        }
        #[cfg(feature = "udp-gro")]
        if self.capabilities.enabled(Optimization::UdpGro) {
            socket::set_int(fd, libc::IPPROTO_UDP, udp::UDP_GRO, 1)?;
        }
        Ok(())
    }

    fn check_creation(&self) -> io::Result<()> {
        if self.shutting_down {
            return Err(cancelled());
        }
        if self.sockets.available() == 0 {
            return Err(exhausted("socket limit reached"));
        }
        Ok(())
    }

    fn check_operation(&self) -> io::Result<()> {
        if self.shutting_down {
            return Err(cancelled());
        }
        if self.operations == self.max_operations {
            return Err(exhausted("operation limit reached"));
        }
        Ok(())
    }

    fn get(&self, socket: SocketId) -> io::Result<&SocketState> {
        self.sockets.get(socket.0).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "socket generation is no longer alive",
            )
        })
    }

    fn insert_socket(
        &mut self,
        socket: OwnedSocket,
        kind: SocketKind,
        options: SocketOptions,
    ) -> Result<SocketInfo, ImportError> {
        let fd = socket.as_raw_fd();
        let addresses = socket::local_addr(fd)
            .and_then(|local| socket::peer_addr(fd).map(|peer| (local, peer)));
        let (local, peer) = match addresses {
            Ok(addresses) => addresses,
            Err(error) => return Err(ImportError { error, socket }),
        };
        let state = SocketState {
            socket,
            kind,
            local,
            peer,
            options,
            connect: None,
            accept: None,
            recv: None,
            accept_credit: 0,
            recv_credit: 0,
            sends_first: None,
            sends_last: None,
            interest: EDGE,
            read_ready: false,
            write_ready: false,
            read_turn: true,
            memory_blocked: false,
            accept_blocked: false,
            touched: false,
            queued: false,
            previous: None,
            next: None,
        };
        let key = match self.sockets.insert(state) {
            Ok(key) => key,
            Err(state) => {
                return Err(ImportError {
                    error: exhausted("socket limit reached"),
                    socket: state.socket,
                });
            }
        };
        let mut event = libc::epoll_event {
            events: EDGE,
            u64: key,
        };
        if let Err(error) = socket::cvt(unsafe {
            libc::epoll_ctl(self.epoll.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut event)
        }) {
            return Err(ImportError {
                error,
                socket: self.sockets.remove(key).unwrap().socket,
            });
        }
        Ok(SocketInfo {
            id: SocketId(key),
            kind,
            local_addr: local,
            peer_addr: peer,
        })
    }

    fn update_interest(&mut self, key: u64) -> io::Result<()> {
        let Some(state) = self.sockets.get_mut(key) else {
            return Ok(());
        };
        let mut interest = EDGE;
        if (state.accept.is_some() && state.accept_credit != 0 && !state.accept_blocked)
            || (state.recv.is_some() && state.recv_credit != 0 && !state.memory_blocked)
        {
            interest |= libc::EPOLLIN as u32;
        }
        if state.connect.is_some() || state.sends_first.is_some() {
            interest |= libc::EPOLLOUT as u32;
        }
        if interest != state.interest {
            let mut event = libc::epoll_event {
                events: interest,
                u64: key,
            };
            socket::cvt(unsafe {
                libc::epoll_ctl(
                    self.epoll.as_raw_fd(),
                    libc::EPOLL_CTL_MOD,
                    state.socket.as_raw_fd(),
                    &mut event,
                )
            })?;
            state.interest = interest;
        }
        Ok(())
    }

    fn runnable(state: &SocketState) -> bool {
        (state.read_ready
            && !state.memory_blocked
            && !state.accept_blocked
            && ((state.recv.is_some() && state.recv_credit != 0)
                || (state.accept.is_some() && state.accept_credit != 0)))
            || (state.write_ready && (state.connect.is_some() || state.sends_first.is_some()))
    }

    fn enqueue_ready(&mut self, key: u64) {
        let Some(state) = self.sockets.get_mut(key) else {
            return;
        };
        if state.queued || !Self::runnable(state) {
            return;
        }
        state.queued = true;
        state.previous = self.ready_last;
        state.next = None;
        if let Some(previous) = self.ready_last {
            self.sockets.get_mut(previous).unwrap().next = Some(key);
        } else {
            self.ready_first = Some(key);
        }
        self.ready_last = Some(key);
    }

    fn unlink_ready(&mut self, key: u64) {
        let Some(state) = self.sockets.get_mut(key) else {
            return;
        };
        if !state.queued {
            return;
        }
        state.queued = false;
        let previous = state.previous.take();
        let next = state.next.take();
        if let Some(previous) = previous {
            self.sockets.get_mut(previous).unwrap().next = next;
        } else {
            self.ready_first = next;
        }
        if let Some(next) = next {
            self.sockets.get_mut(next).unwrap().previous = previous;
        } else {
            self.ready_last = previous;
        }
    }

    fn retry_blocked(&mut self) {
        // A pool-return wake resumes known-readable sockets without requiring a
        // new ET edge. Only resource-blocked sockets are visited.
        while let Some(key) = self.blocked.pop() {
            if let Some(state) = self.sockets.get_mut(key) {
                state.memory_blocked = false;
                state.accept_blocked = false;
                self.enqueue_ready(key);
            }
        }
    }

    fn service_ready(&mut self, events: &mut Vec<Event>, start: usize) -> io::Result<()> {
        let mut turns = 0;
        let turn_budget = self.completion_budget.saturating_mul(2).max(1);
        while events.len() - start < self.completion_budget {
            if !self.pending.is_empty()
                && (self.prefer_pending || self.ready_first.is_none() || turns == turn_budget)
            {
                let event = self.pending.pop_front().unwrap();
                if let Event::Sent { outcome, .. } = &event {
                    self.send_bytes -= outcome.data.len();
                }
                self.operations -= 1;
                events.push(event);
                self.prefer_pending = false;
                continue;
            }
            if turns == turn_budget {
                break;
            }
            let Some(key) = self.ready_first else {
                break;
            };
            self.unlink_ready(key);
            self.process_ready(key);
            turns += 1;
            if let Some(event) = self.delivery.take() {
                // Receive syscalls run only when a slot is available and their
                // result is published immediately, so Android needs no retained
                // multishot-race payload queue.
                let state = self.sockets.get_mut(key).unwrap();
                match &event {
                    Event::Accepted { .. } => state.accept_credit -= 1,
                    Event::Received { .. } | Event::ReceiveEof { .. } => state.recv_credit -= 1,
                    _ => unreachable!(),
                }
                events.push(event);
                self.prefer_pending = true;
            }
            self.update_interest(key)?;
            self.enqueue_ready(key);
        }
        Ok(())
    }

    fn process_ready(&mut self, key: u64) {
        let Some(state) = self.sockets.get_mut(key) else {
            return;
        };
        if state.connect.is_some() && state.write_ready {
            let fd = state.socket.as_raw_fd();
            let result = socket::get_int(fd, libc::SOL_SOCKET, libc::SO_ERROR).and_then(|error| {
                if error == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(error))
                }
            });
            self.finish_connect(key, result);
            return;
        }
        let readable = state.read_ready
            && !state.memory_blocked
            && !state.accept_blocked
            && ((state.recv.is_some() && state.recv_credit != 0)
                || (state.accept.is_some() && state.accept_credit != 0));
        let writable = state.write_ready && state.sends_first.is_some();
        let read = readable && (!writable || state.read_turn);
        state.read_turn = !state.read_turn;
        if read {
            if state.accept.is_some() {
                self.process_accept(key);
            } else {
                self.process_recv(key);
            }
        } else if writable {
            self.process_send(key);
        }
    }

    fn finish_connect(&mut self, key: u64, result: io::Result<()>) {
        let state = self.sockets.get_mut(key).unwrap();
        let token = state.connect.take().unwrap();
        let result = result.and_then(|()| {
            state.local = socket::local_addr(state.socket.as_raw_fd())?;
            state.peer = socket::peer_addr(state.socket.as_raw_fd())?;
            if state.peer.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "connect readiness without an established peer",
                ));
            }
            Ok(SocketInfo {
                id: SocketId(key),
                kind: state.kind,
                local_addr: state.local,
                peer_addr: state.peer,
            })
        });
        if result.is_err() {
            let _ = self.close(SocketId(key));
        }
        self.queue(Event::Connected { token, result });
    }

    fn process_accept(&mut self, key: u64) {
        if self.sockets.available() == 0 {
            self.sockets.get_mut(key).unwrap().accept_blocked = true;
            self.blocked.push(key);
            return;
        }
        let state = self.sockets.get(key).unwrap();
        let token = state.accept.unwrap();
        let fd = unsafe {
            libc::accept4(
                state.socket.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        };
        if fd < 0 {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EAGAIN) => self.sockets.get_mut(key).unwrap().read_ready = false,
                Some(
                    libc::EINTR
                    | libc::ECONNABORTED
                    | libc::EPROTO
                    | libc::ENETDOWN
                    | libc::ENOPROTOOPT
                    | libc::EHOSTDOWN
                    | libc::ENONET
                    | libc::EHOSTUNREACH
                    | libc::EOPNOTSUPP
                    | libc::ENETUNREACH,
                ) => {}
                _ => {
                    self.sockets.get_mut(key).unwrap().accept = None;
                    self.delivery = Some(Event::Accepted {
                        token,
                        result: Err(error),
                    });
                    self.queue(Event::Stopped {
                        token,
                        result: Ok(()),
                    });
                }
            }
            return;
        }
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };
        let options = self.sockets.get(key).unwrap().options.clone();
        // Network/protection state is inherited from the configured listener.
        // Rebinding an already accepted connected fd would be incorrect.
        let result = self
            .insert_socket(socket, SocketKind::TcpStream, options)
            .map_err(|error| error.error);
        self.delivery = Some(Event::Accepted { token, result });
    }

    fn process_recv(&mut self, key: u64) {
        let state = self.sockets.get(key).unwrap();
        let token = state.recv.unwrap();
        let kind = state.kind;
        let chunk = state.options.receive_chunk;
        let fd = state.socket.as_raw_fd();
        let mut buffer = match self.pool.try_acquire_at_least(chunk) {
            Ok(buffer) => buffer,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.sockets.get_mut(key).unwrap().memory_blocked = true;
                self.blocked.push(key);
                return;
            }
            Err(error) => {
                self.fail_receive(key, token, error);
                return;
            }
        };
        let mut source: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let mut iovec = libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: chunk.min(buffer.capacity()),
        };
        let mut message: libc::msghdr = unsafe { mem::zeroed() };
        message.msg_iov = &mut iovec;
        message.msg_iovlen = 1;
        if kind == SocketKind::Udp {
            message.msg_name = (&mut source as *mut libc::sockaddr_storage).cast();
            message.msg_namelen = mem::size_of_val(&source) as _;
            message.msg_control = self.control.as_mut_ptr();
            message.msg_controllen = self.control.capacity();
        }
        let flags = libc::MSG_DONTWAIT
            | if kind == SocketKind::Udp {
                libc::MSG_TRUNC
            } else {
                0
            };
        let received = unsafe { libc::recvmsg(fd, &mut message, flags) };
        if received < 0 {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EAGAIN) => self.sockets.get_mut(key).unwrap().read_ready = false,
                Some(libc::EINTR) => {}
                _ => self.fail_receive(key, token, error),
            }
            return;
        }
        if received == 0 && kind == SocketKind::TcpStream {
            self.sockets.get_mut(key).unwrap().recv = None;
            self.delivery = Some(Event::ReceiveEof { token });
            self.queue(Event::Stopped {
                token,
                result: Ok(()),
            });
            return;
        }
        let length = received as usize;
        unsafe {
            buffer.set_initialized_len(length.min(iovec.iov_len));
        }
        let metadata = if kind == SocketKind::Udp {
            socket::decode(&source, message.msg_namelen).and_then(|peer| {
                self.control
                    .gro_segment_size(message.msg_controllen, message.msg_flags)
                    .map(|segment| (Some(peer), segment))
            })
        } else {
            Ok((None, None))
        };
        let (peer, gro_segment_size) = match metadata {
            Ok(metadata) => metadata,
            Err(error) => {
                self.fail_receive(key, token, error);
                return;
            }
        };
        self.delivery = Some(Event::Received {
            token,
            result: Ok(Received {
                data: buffer.freeze(),
                peer,
                truncated: message.msg_flags & libc::MSG_TRUNC != 0 || length > iovec.iov_len,
                original_len: (kind == SocketKind::Udp).then_some(length),
                gro_segment_size,
            }),
        });
    }

    fn fail_receive(&mut self, key: u64, token: Token, error: io::Error) {
        self.sockets.get_mut(key).unwrap().recv = None;
        self.delivery = Some(Event::Received {
            token,
            result: Err(error),
        });
        self.queue(Event::Stopped {
            token,
            result: Ok(()),
        });
    }

    fn validate_send(
        &self,
        socket: SocketId,
        data: &SendPayload,
        destination: Option<SocketAddr>,
        segment_size: Option<u16>,
    ) -> io::Result<()> {
        self.check_operation()?;
        let state = self.get(socket)?;
        if state.kind == SocketKind::TcpListener || state.connect.is_some() {
            return Err(invalid("send requires an established TCP/UDP socket"));
        }
        if data.segments().len() > self.max_iovecs {
            return Err(invalid("send exceeds the configured iovec limit"));
        }
        if data.len() > self.max_send_bytes.saturating_sub(self.send_bytes) {
            return Err(exhausted("send byte budget reached"));
        }
        if state.kind != SocketKind::Udp && (destination.is_some() || segment_size.is_some()) {
            return Err(invalid("TCP sends cannot contain datagram metadata"));
        }
        if state.kind == SocketKind::Udp && destination.is_none() && state.peer.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "unconnected UDP send requires a destination",
            ));
        }
        if let Some(destination) = destination {
            socket::validate_unicast(destination)?;
        }
        if let Some(segment) = segment_size {
            if segment == 0 {
                return Err(invalid("UDP GSO segment size must be nonzero"));
            }
            #[cfg(not(feature = "udp-gso"))]
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UDP GSO is not enabled",
            ));
            #[cfg(feature = "udp-gso")]
            {
                if !self.capabilities.enabled(Optimization::UdpGso) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "UDP GSO is not enabled",
                    ));
                }
                if data.len().div_ceil(segment as usize) > 64 {
                    return Err(invalid("UDP GSO supports at most 64 segments per send"));
                }
            }
        }
        Ok(())
    }

    fn process_send(&mut self, socket: u64) {
        let state = self.sockets.get(socket).unwrap();
        let Some(key) = state.sends_first else {
            return;
        };
        let fd = state.socket.as_raw_fd();
        let kind = state.kind;
        let operation = self.sends.get(key).unwrap();
        self.iovecs.clear();
        for segment in operation.data.segments() {
            self.iovecs.push(libc::iovec {
                iov_base: segment.as_ptr().cast_mut().cast(),
                iov_len: segment.len(),
            });
        }
        let destination = operation.destination.map(socket2::SockAddr::from);
        let mut message: libc::msghdr = unsafe { mem::zeroed() };
        message.msg_iov = self.iovecs.as_mut_ptr();
        message.msg_iovlen = self.iovecs.len();
        if let Some(destination) = &destination {
            message.msg_name = destination.as_ptr().cast_mut().cast();
            message.msg_namelen = destination.len();
        }
        #[cfg(feature = "udp-gso")]
        if let Some(segment) = operation.segment_size {
            self.control.segment(segment, &mut message);
        }
        let sent = unsafe { libc::sendmsg(fd, &message, libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL) };
        if sent < 0 {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EAGAIN) => self.sockets.get_mut(socket).unwrap().write_ready = false,
                Some(libc::EINTR) => {}
                _ => self.finish_send(key, Err(error)),
            }
        } else if kind == SocketKind::Udp && sent as usize != operation.data.len() {
            self.finish_send(
                key,
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "kernel returned a short UDP send",
                )),
            );
        } else {
            self.finish_send(key, Ok(sent as usize));
        }
    }

    fn finish_send(&mut self, key: u64, result: io::Result<usize>) {
        let operation = self.sends.remove(key).unwrap();
        if let Some(previous) = operation.previous {
            self.sends.get_mut(previous).unwrap().next = operation.next;
        } else if let Some(state) = self.sockets.get_mut(operation.socket.0) {
            state.sends_first = operation.next;
        }
        if let Some(next) = operation.next {
            self.sends.get_mut(next).unwrap().previous = operation.previous;
        } else if let Some(state) = self.sockets.get_mut(operation.socket.0) {
            state.sends_last = operation.previous;
        }
        self.queue(Event::Sent {
            token: operation.token,
            outcome: SendOutcome {
                result,
                data: operation.data,
            },
            memory_released: true,
        });
    }

    fn queue(&mut self, event: Event) {
        debug_assert!(self.pending.len() < self.pending.capacity());
        self.pending.push_back(event);
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn exhausted(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}
fn cancelled() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "operation cancelled on its owning worker",
    )
}
