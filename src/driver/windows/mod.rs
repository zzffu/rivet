//! Registered Winsock I/O with one owner for every RQ, CQ and OVERLAPPED.
//!
//! RIO accepts one data buffer per request. TCP vectors are advanced in order
//! through completion (including short completions); only UDP vectors coalesce.
//! Imported handles must have been created with WSA_FLAG_REGISTERED_IO and must
//! not already own a RIO request queue. Import never substitutes a new socket.

mod datagram;
mod native;
mod notify;
mod operations;
mod poll;
mod rio;
mod sys;

pub(crate) use notify::Notifier;

use crate::{
    buffer::{BufferPool, SendBuf, SendPayload, WriteBuf},
    capability::{CapabilityReport, ZcStats},
    config::{Limits, RuntimeConfig},
    diagnostics::{DriverResources, RioReceiveResources},
    driver::{Arena, Event, Received, SendOutcome, SocketId, SocketInfo, SocketKind, Token},
    socket::{ImportError, OwnedSocket, SocketOptions},
};
use socket2::{SockAddr, Socket};
use std::{
    collections::VecDeque,
    io,
    mem::size_of,
    net::{Shutdown, SocketAddr},
    os::windows::io::AsRawSocket,
    ptr,
    sync::Arc,
    time::Duration,
};
use windows_sys::Win32::{
    Networking::WinSock::*,
    System::IO::{CreateIoCompletionPort, OVERLAPPED},
};

pub(crate) struct Shared {
    workers: usize,
}
impl Shared {
    pub fn new(workers: usize) -> Self {
        Self { workers }
    }
}

struct SocketRecord {
    handle: Option<Socket>,
    info: SocketInfo,
    options: SocketOptions,
    rq: RIO_RQ,
    iocp: bool,
    ever_active: bool,
    operation_refs: usize,
    receive: Option<u64>,
    datagrams: Option<DatagramReceive>,
    accept: Option<u64>,
    send_head: Option<u64>,
    send_tail: Option<u64>,
    receive_credits: usize,
    accept_credits: usize,
    receive_commit: bool,
    send_commit: bool,
    read_shutdown: bool,
    write_shutdown: bool,
    accept_ex: LPFN_ACCEPTEX,
}

struct DatagramReceive {
    token: Option<Token>,
    lanes: usize,
    pending: usize,
    ready: VecDeque<u64>,
    idle: VecDeque<u64>,
    rearm_allocation_failures_total: u64,
    stopping: bool,
    discard: bool,
    error: Option<io::Error>,
}

impl SocketRecord {
    fn raw(&self) -> io::Result<SOCKET> {
        self.handle
            .as_ref()
            .map(|s| s.as_raw_socket() as SOCKET)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "socket is closing"))
    }
}

const ACCEPT_ADDRESS_BYTES: usize = size_of::<SOCKADDR_STORAGE>() + 16;

// Native-writable bytes live outside the arena, whose ordinary references may
// be traversed while an overlapped request completes on another CPU.
#[repr(C)]
struct NativeControl {
    overlapped: OVERLAPPED,
    key: u64,
    addresses: [u8; 2 * ACCEPT_ADDRESS_BYTES],
}

impl NativeControl {
    fn new(key: u64) -> Self {
        Self {
            overlapped: OVERLAPPED::default(),
            key,
            addresses: [0; 2 * ACCEPT_ADDRESS_BYTES],
        }
    }
}

struct Operation {
    token: Option<Token>,
    socket: SocketId,
    in_flight: bool,
    cancelled: bool,
    next_send: Option<u64>,
    data_buf: RIO_BUF,
    address_buf: RIO_BUF,
    flags_buf: RIO_BUF,
    kind: OperationKind,
}

enum OperationKind {
    Connect {
        address: SockAddr,
    },
    Accept {
        child: Option<Socket>,
        ready: Option<io::Result<SocketInfo>>,
    },
    Receive {
        writable: Option<WriteBuf>,
        reserve: Option<SendBuf>,
        ready: Option<io::Result<Received>>,
        eof: bool,
    },
    DatagramReceive {
        next: Option<u64>,
        writable: Option<WriteBuf>,
        reserve: Option<SendBuf>,
        ready: Option<Received>,
        last_pool_blocked: bool,
    },
    Send {
        data: Option<SendPayload>,
        coalesced: Option<SendBuf>,
        destination: Option<SocketAddr>,
        segment: usize,
        offset: usize,
        transferred: usize,
        registration: Option<usize>,
        result: Option<io::Result<usize>>,
    },
}

impl Operation {
    fn new(token: Token, socket: SocketId, kind: OperationKind) -> Self {
        Self {
            token: Some(token),
            socket,
            in_flight: false,
            cancelled: false,
            next_send: None,
            data_buf: RIO_BUF::default(),
            address_buf: RIO_BUF::default(),
            flags_buf: RIO_BUF::default(),
            kind,
        }
    }

    fn datagram(socket: SocketId, next: Option<u64>, buffer: WriteBuf) -> Self {
        let mut operation = Self::new(
            Token(0),
            socket,
            OperationKind::DatagramReceive {
                next,
                writable: Some(buffer),
                reserve: None,
                ready: None,
                last_pool_blocked: false,
            },
        );
        operation.token = None;
        operation
    }
}

pub(crate) struct Driver {
    // Rio owns an additional pool reference, and is dropped only after drain.
    rio: rio::Rio,
    pool: BufferPool,
    notifier: Arc<Notifier>,
    capabilities: CapabilityReport,
    limits: Limits,
    sockets: Arena<SocketRecord>,
    operations: Arena<Operation>,
    controls: native::Native<[NativeControl]>,
    pending: VecDeque<Event>,
    keys: Vec<u64>,
    socket_keys: Vec<u64>,
    rio_completions: Vec<RIORESULT>,
    iocp_completions: Vec<windows_sys::Win32::System::IO::OVERLAPPED_ENTRY>,
    iocp_first: bool,
    accept_reservations: usize,
    datagram_queue_slots: usize,
    udp_rearm_allocation_failures_total: u64,
    send_bytes: usize,
    shutting_down: bool,
}

impl Driver {
    pub fn new(
        config: &RuntimeConfig,
        worker: usize,
        pool: BufferPool,
        notifier: Arc<Notifier>,
        shared: Arc<Shared>,
    ) -> io::Result<Self> {
        if worker >= shared.workers {
            return Err(sys::invalid("Windows worker index exceeds Shared capacity"));
        }
        let mut capabilities = CapabilityReport::new("windows-rio-iocp", worker);
        // Linux feature selection never makes a Windows path silently fall back.
        capabilities.finish(config)?;
        let limits = config.limits.clone();
        let rio = rio::Rio::new(
            pool.clone(),
            &notifier,
            limits.max_sockets,
            limits.max_operations,
        )?;
        let batch = limits.completion_budget.clamp(1, 256);
        Ok(Self {
            rio,
            pool,
            notifier,
            capabilities,
            sockets: Arena::new(limits.max_sockets),
            operations: Arena::new(limits.max_operations),
            controls: native::Native::new(
                (0..limits.max_operations)
                    .map(|_| NativeControl::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            pending: VecDeque::with_capacity(limits.max_operations.saturating_mul(2)),
            keys: Vec::with_capacity(limits.max_operations),
            socket_keys: Vec::with_capacity(limits.max_sockets),
            rio_completions: vec![RIORESULT::default(); batch],
            iocp_completions: vec![
                windows_sys::Win32::System::IO::OVERLAPPED_ENTRY::default();
                batch
            ],
            iocp_first: true,
            accept_reservations: 0,
            datagram_queue_slots: 0,
            udp_rearm_allocation_failures_total: 0,
            send_bytes: 0,
            shutting_down: false,
            limits,
        })
    }

    pub fn capabilities(&self) -> &CapabilityReport {
        &self.capabilities
    }
    pub fn zc_stats(&self) -> ZcStats {
        ZcStats::default()
    }

    /// Native counts retain one record per outstanding request, including
    /// deferred RIO commits. Software completions include queued events and
    /// stored operation results, EOFs and terminal datagram errors, never CQ
    /// contents. Admission availability also respects accept reservations and
    /// the shared operation/completion budget.
    pub fn resource_snapshot(&self) -> DriverResources {
        let mut pending_completions = self.pending.len();
        let mut closing_sockets = 0;
        for (_, record) in self.sockets.iter() {
            closing_sockets += usize::from(record.handle.is_none());
            if let Some(group) = &record.datagrams {
                pending_completions += usize::from(group.error.is_some());
            }
        }
        let mut native_outstanding = 0;
        let mut retiring_native = 0;
        for (_, operation) in self.operations.iter() {
            if operation.in_flight {
                native_outstanding += 1;
                let record = self.sockets.get(operation.socket.0).unwrap();
                if operation.cancelled
                    || record.handle.is_none()
                    || record
                        .datagrams
                        .as_ref()
                        .is_some_and(|group| group.stopping)
                    || self.shutting_down
                {
                    retiring_native += 1;
                }
            }
            pending_completions += match &operation.kind {
                OperationKind::Connect { .. } => 0,
                OperationKind::Accept { ready, .. } => usize::from(ready.is_some()),
                OperationKind::Receive { ready, eof, .. } => {
                    usize::from(ready.is_some()) + usize::from(*eof)
                }
                OperationKind::DatagramReceive { ready, .. } => usize::from(ready.is_some()),
                OperationKind::Send { result, .. } => usize::from(result.is_some()),
            };
        }
        DriverResources {
            sockets: self.sockets.len(),
            available_socket_slots: self.sockets.available() - self.accept_reservations,
            operations: self.operations.len(),
            available_operation_slots: self.operations.available().min(
                self.limits
                    .max_operations
                    .saturating_sub(self.operations.len() + self.pending.len()),
            ),
            pending_completions,
            closing_sockets,
            native_outstanding: Some(native_outstanding),
            retiring_native: Some(retiring_native),
            rio_receive_queue_slots: Some(self.datagram_queue_slots),
            udp_rearm_allocation_failures_total: Some(self.udp_rearm_allocation_failures_total),
        }
    }

    /// Visits only this socket's receive list. The group's pending count mixes
    /// native requests and ready results, so it cannot stand in for posted I/O.
    pub fn receive_snapshot(&self, socket: SocketId) -> io::Result<crate::driver::ReceiveState> {
        let record = self.socket(socket)?;
        let mut native_outstanding = 0;
        let mut last_pool_blocked_lanes = 0;
        let mut next = record.receive;
        while let Some(key) = next {
            let operation = self.operations.get(key).unwrap();
            native_outstanding += usize::from(operation.in_flight);
            if let OperationKind::DatagramReceive {
                next: following,
                last_pool_blocked,
                ..
            } = &operation.kind
            {
                last_pool_blocked_lanes += usize::from(*last_pool_blocked);
                next = *following;
            } else {
                break;
            }
        }
        Ok(crate::driver::ReceiveState {
            publication_credits: record.receive_credits,
            native_outstanding: Some(native_outstanding),
            rio: record.datagrams.as_ref().map(|group| RioReceiveResources {
                admitted_lanes: group.lanes,
                ready_results: group.ready.len(),
                idle_lanes: group.idle.len(),
                last_pool_blocked_lanes,
                rearm_allocation_failures_total: group.rearm_allocation_failures_total,
                commit_pending: record.receive_commit,
                stopping: group.stopping,
            }),
        })
    }

    fn socket(&self, id: SocketId) -> io::Result<&SocketRecord> {
        self.sockets
            .get(id.0)
            .filter(|s| s.handle.is_some())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "stale or closed socket identifier",
                )
            })
    }

    fn room_for_socket(&self) -> io::Result<()> {
        if self.shutting_down {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "driver is shutting down",
            ));
        }
        if self.sockets.available() <= self.accept_reservations {
            return Err(sys::exhausted("Windows socket budget exhausted"));
        }
        Ok(())
    }

    fn room_for_operations(&self, count: usize) -> io::Result<()> {
        if self.shutting_down {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "driver is shutting down",
            ));
        }
        if count > self.operations.available()
            || count
                > self
                    .limits
                    .max_operations
                    .saturating_sub(self.operations.len() + self.pending.len())
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "Windows operation/completion budget exhausted: requested {count} slots, \
                     {} native slots and {} completion slots available; poll and retry",
                    self.operations.available(),
                    self.limits
                        .max_operations
                        .saturating_sub(self.operations.len() + self.pending.len()),
                ),
            ));
        }
        Ok(())
    }

    fn check_token(&self, token: Token) -> io::Result<()> {
        if self.shutting_down {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "driver is shutting down",
            ));
        }
        if self
            .operations
            .iter()
            .any(|(_, op)| op.token == Some(token))
        {
            return Err(sys::invalid("operation token is already active"));
        }
        Ok(())
    }

    fn room_for_operation(&self, token: Token) -> io::Result<()> {
        self.room_for_operations(1)?;
        self.check_token(token)
    }

    fn insert_operation(&mut self, operation: Operation) -> u64 {
        let key = self
            .operations
            .insert(operation)
            .unwrap_or_else(|_| unreachable!("operation admission reserved a slot"));
        // Arena slots are reused only after native completion; no reference to
        // this raw-owned allocation survives submission.
        unsafe {
            ptr::addr_of_mut!((*self.control(key)).key).write(key);
        }
        let op = self.operations.get_mut(key).unwrap();
        self.sockets.get_mut(op.socket.0).unwrap().operation_refs += 1;
        unsafe {
            *self.rio.metadata_mut(key) = rio::Metadata::default();
        }
        key
    }

    fn control(&self, key: u64) -> *mut NativeControl {
        let index = key as u32 as usize;
        debug_assert!(index < self.limits.max_operations);
        unsafe { self.controls.as_ptr().cast::<NativeControl>().add(index) }
    }

    fn insert_socket(
        &mut self,
        handle: Socket,
        kind: SocketKind,
        local_addr: SocketAddr,
        peer_addr: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> SocketInfo {
        let record = SocketRecord {
            handle: Some(handle),
            info: SocketInfo {
                id: SocketId(0),
                kind,
                local_addr,
                peer_addr,
            },
            options: options.clone(),
            rq: 0,
            iocp: false,
            ever_active: false,
            operation_refs: 0,
            receive: None,
            accept: None,
            send_head: None,
            send_tail: None,
            datagrams: (kind == SocketKind::Udp).then(|| DatagramReceive {
                token: None,
                lanes: 0,
                pending: 0,
                ready: VecDeque::with_capacity(self.limits.max_pending_receives),
                idle: VecDeque::with_capacity(self.limits.max_pending_receives),
                rearm_allocation_failures_total: 0,
                stopping: false,
                discard: false,
                error: None,
            }),
            receive_credits: 0,
            accept_credits: 0,
            receive_commit: false,
            send_commit: false,
            read_shutdown: false,
            write_shutdown: false,
            accept_ex: None,
        };
        let key = self
            .sockets
            .insert(record)
            .unwrap_or_else(|_| unreachable!("socket admission reserved a slot"));
        let record = self.sockets.get_mut(key).unwrap();
        record.info.id = SocketId(key);
        record.info.clone()
    }

    fn ensure_rq(&mut self, socket: SocketId) -> io::Result<RIO_RQ> {
        let record = self.socket(socket)?;
        if record.rq != 0 {
            return Ok(record.rq);
        }
        let receives = record.datagrams.as_ref().map_or(1, |group| group.lanes);
        let extra = receives - 1;
        if extra > self.limits.max_operations - self.datagram_queue_slots {
            return Err(sys::exhausted(
                "Windows RIO receive-queue reservation budget exhausted",
            ));
        }
        let rq = self.rio.create_queue(record.raw()?, socket.0, receives)?;
        self.sockets.get_mut(socket.0).unwrap().rq = rq;
        self.datagram_queue_slots += extra;
        Ok(rq)
    }

    /// Reserve the complete native window before creating an inseparable RQ.
    /// None of these lanes is submitted until Core installs the logical token
    /// and absolute credits. Failed admission can still return an original fd.
    fn prepare_datagrams(&mut self, socket: SocketId) -> io::Result<()> {
        let count = self.limits.max_pending_receives;
        self.room_for_operations(count)?;
        let chunk = self.socket(socket)?.options.receive_chunk;
        for lane in 0..count {
            let buffer = self.pool.try_acquire_at_least(chunk).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "Windows UDP receive window needs {count} leases with {} payload bytes \
                         each; reserved {lane}/{count} lanes; configured worker pool has {} \
                         payload bytes and {} lease slots ({error})",
                        chunk.max(self.limits.pool.block_size),
                        self.limits.pool.bytes,
                        self.limits.pool.max_leases,
                    ),
                )
            })?;
            let next = self.sockets.get(socket.0).unwrap().receive;
            let key = self.insert_operation(Operation::datagram(socket, next, buffer));
            let record = self.sockets.get_mut(socket.0).unwrap();
            record.receive = Some(key);
            let group = record.datagrams.as_mut().unwrap();
            group.lanes += 1;
            group.idle.push_back(key);
            self.rio.prepare_metadata(key)?;
        }
        Ok(())
    }

    fn rollback_datagrams(&mut self, socket: SocketId) {
        let record = self.sockets.get_mut(socket.0).unwrap();
        debug_assert_eq!(record.rq, 0);
        let mut next = record.receive.take();
        while let Some(key) = next {
            let operation = self.operations.remove(key).unwrap();
            debug_assert!(!operation.in_flight);
            let OperationKind::DatagramReceive {
                next: following, ..
            } = operation.kind
            else {
                unreachable!()
            };
            next = following;
            record.operation_refs -= 1;
        }
        record.datagrams.as_mut().unwrap().lanes = 0;
    }

    fn attach_iocp(&mut self, socket: SocketId) -> io::Result<()> {
        let record = self.socket(socket)?;
        if record.iocp {
            return Ok(());
        }
        let port = unsafe {
            CreateIoCompletionPort(
                record.raw()? as _,
                self.notifier.handle(),
                notify::SOCKET_KEY,
                0,
            )
        };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        self.sockets.get_mut(socket.0).unwrap().iocp = true;
        Ok(())
    }

    pub fn listen(&mut self, addr: SocketAddr, options: &SocketOptions) -> io::Result<SocketInfo> {
        self.room_for_socket()?;
        let socket = sys::new_socket(addr, SocketKind::TcpListener)?;
        sys::configure(&socket, SocketKind::TcpListener, options, true)?;
        socket.bind(&SockAddr::from(addr))?;
        socket.listen(options.backlog)?;
        let local = sys::address(socket.local_addr()?)?;
        Ok(self.insert_socket(socket, SocketKind::TcpListener, local, None, options))
    }

    pub fn bind_udp(
        &mut self,
        addr: SocketAddr,
        peer: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> io::Result<SocketInfo> {
        self.room_for_socket()?;
        if let Some(peer) = peer {
            sys::unicast(peer)?;
        }
        let socket = sys::new_socket(addr, SocketKind::Udp)?;
        sys::configure(&socket, SocketKind::Udp, options, true)?;
        socket.bind(&SockAddr::from(addr))?;
        if let Some(peer) = peer {
            socket.connect(&SockAddr::from(peer))?;
        }
        let local = sys::address(socket.local_addr()?)?;
        let info = self.insert_socket(socket, SocketKind::Udp, local, peer, options);
        if let Err(error) = self
            .prepare_datagrams(info.id)
            .and_then(|_| self.ensure_rq(info.id).map(|_| ()))
        {
            self.rollback_datagrams(info.id);
            self.sockets.remove(info.id.0);
            return Err(error);
        }
        Ok(info)
    }

    pub fn connect(
        &mut self,
        token: Token,
        addr: SocketAddr,
        local: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> io::Result<()> {
        self.room_for_operation(token)?;
        self.room_for_socket()?;
        let socket = sys::new_socket(addr, SocketKind::TcpStream)?;
        sys::configure(&socket, SocketKind::TcpStream, options, true)?;
        let bind_address = local.unwrap_or_else(|| {
            if addr.is_ipv4() {
                SocketAddr::from(([0; 4], 0))
            } else {
                SocketAddr::from(([0; 8], 0))
            }
        });
        socket.bind(&SockAddr::from(bind_address))?;
        let connect: LPFN_CONNECTEX =
            unsafe { sys::extension(socket.as_raw_socket() as _, &WSAID_CONNECTEX)? };
        let connect =
            connect.ok_or_else(|| sys::unsupported("Winsock provider has no ConnectEx"))?;
        let local = sys::address(socket.local_addr()?)?;
        let info = self.insert_socket(socket, SocketKind::TcpStream, local, None, options);
        if let Err(error) = self.attach_iocp(info.id) {
            self.sockets.remove(info.id.0);
            return Err(error);
        }
        let key = self.insert_operation(Operation::new(
            token,
            info.id,
            OperationKind::Connect {
                address: SockAddr::from(addr),
            },
        ));
        let raw = self.socket(info.id)?.raw()?;
        let control = self.control(key);
        unsafe {
            ptr::addr_of_mut!((*control).overlapped).write(OVERLAPPED::default());
        }
        let op = self.operations.get_mut(key).unwrap();
        let OperationKind::Connect { address } = &op.kind else {
            unreachable!()
        };
        let mut bytes = 0;
        let result = unsafe {
            connect(
                raw,
                address.as_ptr().cast(),
                address.len(),
                ptr::null(),
                0,
                &mut bytes,
                ptr::addr_of_mut!((*control).overlapped),
            )
        };
        if result == 0 && unsafe { WSAGetLastError() } != WSA_IO_PENDING {
            let error = sys::wsa_error();
            self.operations.remove(key);
            self.sockets.remove(info.id.0);
            return Err(error);
        }
        op.in_flight = true;
        self.sockets.get_mut(info.id.0).unwrap().ever_active = true;
        Ok(())
    }

    pub fn import(
        &mut self,
        socket: OwnedSocket,
        kind: SocketKind,
        options: &SocketOptions,
    ) -> Result<SocketInfo, ImportError> {
        let handle: Socket = socket.into();
        let prepared = (|| {
            self.room_for_socket()?;
            let addresses = sys::validate_import(&handle, kind)?;
            if kind == SocketKind::Udp {
                self.room_for_operations(self.limits.max_pending_receives)?;
            }
            Ok::<_, io::Error>(addresses)
        })();
        let (local, peer) = match prepared {
            Ok(addresses) => addresses,
            Err(error) => {
                return Err(ImportError {
                    error,
                    socket: handle.into(),
                });
            }
        };
        let info = self.insert_socket(handle, kind, local, peer, options);
        let prepared = (|| {
            if kind == SocketKind::Udp {
                self.prepare_datagrams(info.id)?;
            }
            sys::configure(
                self.socket(info.id)?.handle.as_ref().unwrap(),
                kind,
                options,
                false,
            )?;
            // The final fallible ownership step: an RQ cannot be detached from
            // its socket. Later receive/commit failures belong to the accepted
            // runtime socket and are reported by its persistent receive token.
            self.ensure_rq(info.id)?;
            Ok::<_, io::Error>(())
        })();
        if let Err(error) = prepared {
            if kind == SocketKind::Udp {
                self.rollback_datagrams(info.id);
            }
            let record = self.sockets.remove(info.id.0).unwrap();
            return Err(ImportError {
                error,
                socket: record.handle.unwrap().into(),
            });
        }
        Ok(info)
    }

    pub fn take_idle_socket(&mut self, socket: SocketId) -> io::Result<OwnedSocket> {
        let record = self.socket(socket)?;
        if record.ever_active
            || record.rq != 0
            || record.iocp
            || record.receive.is_some()
            || record.accept.is_some()
            || record.send_head.is_some()
        {
            return Err(sys::invalid(
                "only a newly accepted socket with no IOCP/RIO association or data I/O may move workers",
            ));
        }
        Ok(self
            .sockets
            .remove(socket.0)
            .unwrap()
            .handle
            .unwrap()
            .into())
    }

    pub fn start_accept(&mut self, socket: SocketId, token: Token) -> io::Result<()> {
        self.room_for_operation(token)?;
        let record = self.socket(socket)?;
        if record.info.kind != SocketKind::TcpListener || record.accept.is_some() {
            return Err(sys::invalid(
                "accept requires a listener without another persistent accept",
            ));
        }
        let function: LPFN_ACCEPTEX = unsafe { sys::extension(record.raw()?, &WSAID_ACCEPTEX)? };
        if function.is_none() {
            return Err(sys::unsupported("Winsock provider has no AcceptEx"));
        }
        self.attach_iocp(socket)?;
        let key = self.insert_operation(Operation::new(
            token,
            socket,
            OperationKind::Accept {
                child: None,
                ready: None,
            },
        ));
        let record = self.sockets.get_mut(socket.0).unwrap();
        record.accept = Some(key);
        record.accept_ex = function;
        record.ever_active = true;
        Ok(())
    }

    pub fn start_recv(&mut self, socket: SocketId, token: Token) -> io::Result<()> {
        self.check_token(token)?;
        let record = self.socket(socket)?;
        if let Some(group) = &record.datagrams {
            if group.token.is_some() || group.stopping || record.read_shutdown {
                return Err(sys::invalid("UDP receive is already started or stopped"));
            }
            let mut next = record.receive;
            while let Some(key) = next {
                let operation = self.operations.get_mut(key).unwrap();
                operation.token = Some(token);
                let OperationKind::DatagramReceive {
                    next: following, ..
                } = &operation.kind
                else {
                    unreachable!()
                };
                next = *following;
            }
            let record = self.sockets.get_mut(socket.0).unwrap();
            record.datagrams.as_mut().unwrap().token = Some(token);
            record.ever_active = true;
            return Ok(());
        }
        self.room_for_operation(token)?;
        let record = self.socket(socket)?;
        if record.info.kind == SocketKind::TcpListener
            || record.receive.is_some()
            || record.read_shutdown
        {
            return Err(sys::invalid(
                "receive requires a readable TCP/UDP socket without another persistent receive",
            ));
        }
        // An explicit admission error is preferable to an active socket waiting
        // forever behind idle sockets that have pinned every receive buffer.
        let buffer = self
            .pool
            .try_acquire_at_least(record.options.receive_chunk)?;
        self.ensure_rq(socket)?;
        let key = self.insert_operation(Operation::new(
            token,
            socket,
            OperationKind::Receive {
                writable: Some(buffer),
                reserve: None,
                ready: None,
                eof: false,
            },
        ));
        let record = self.sockets.get_mut(socket.0).unwrap();
        record.receive = Some(key);
        record.ever_active = true;
        Ok(())
    }

    pub fn receive_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()> {
        let record = self
            .sockets
            .get(socket.0)
            .ok_or_else(|| sys::invalid("stale socket identifier"))?;
        if record.handle.is_none()
            && !record
                .datagrams
                .as_ref()
                .is_some_and(|group| group.stopping)
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "socket is closing",
            ));
        }
        if slots > self.limits.max_pending_receives {
            return Err(sys::invalid(
                "receive credits exceed the configured queue bound",
            ));
        }
        self.sockets.get_mut(socket.0).unwrap().receive_credits = slots;
        // RIO drops datagrams with no posted receive. Replenish and commit
        // here, not on the next worker poll, so bind/import and every consumer
        // dequeue return with all currently admitted lanes actually receptive.
        if self.sockets.get(socket.0).unwrap().datagrams.is_some() {
            self.rearm_datagrams(socket);
            self.commit_datagrams(socket);
        }
        Ok(())
    }

    pub fn accept_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()> {
        self.socket(socket)?;
        if slots > self.limits.max_pending_accepts {
            return Err(sys::invalid(
                "accept credits exceed the configured queue bound",
            ));
        }
        self.sockets.get_mut(socket.0).unwrap().accept_credits = slots;
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
        let prepared = (|| {
            self.room_for_operation(token)?;
            let record = self.socket(socket)?;
            if record.info.kind == SocketKind::TcpListener || record.write_shutdown {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "socket is not writable",
                ));
            }
            if segment_size.is_some() {
                return Err(sys::unsupported(
                    "UDP segmentation is not implemented by Windows RIO",
                ));
            }
            if data.segments().len() > self.limits.max_iovecs {
                return Err(sys::invalid(
                    "send vector exceeds the configured iovec limit",
                ));
            }
            if data.len() > self.limits.max_send_bytes.saturating_sub(self.send_bytes) {
                return Err(sys::exhausted(
                    "Windows in-flight send-byte budget exhausted",
                ));
            }
            if record.info.kind == SocketKind::TcpStream && destination.is_some() {
                return Err(sys::invalid(
                    "a TCP send cannot specify a datagram destination",
                ));
            }
            if record.info.kind == SocketKind::Udp
                && destination.is_none()
                && record.info.peer_addr.is_none()
            {
                return Err(sys::invalid("unconnected UDP send requires a destination"));
            }
            if let Some(destination) = destination {
                sys::unicast(destination)?;
            }
            let coalesced = if record.info.kind == SocketKind::Udp
                && data
                    .segments()
                    .iter()
                    .filter(|segment| !segment.is_empty())
                    .take(2)
                    .count()
                    == 2
            {
                let mut buffer = self.pool.try_acquire_at_least(data.len())?;
                for segment in data.segments() {
                    buffer.extend_from_slice(segment.as_slice())?;
                }
                Some(buffer.freeze())
            } else {
                None
            };
            self.ensure_rq(socket)?;
            Ok::<_, io::Error>(coalesced)
        })();
        let coalesced = match prepared {
            Ok(buffer) => buffer,
            Err(error) => {
                return Err(SendOutcome {
                    result: Err(error),
                    data,
                });
            }
        };
        self.send_bytes += data.len();
        let key = self.insert_operation(Operation::new(
            token,
            socket,
            OperationKind::Send {
                data: Some(data),
                coalesced,
                destination,
                segment: 0,
                offset: 0,
                transferred: 0,
                registration: None,
                result: None,
            },
        ));
        let record = self.sockets.get_mut(socket.0).unwrap();
        if let Some(tail) = record.send_tail {
            self.operations.get_mut(tail).unwrap().next_send = Some(key);
        } else {
            record.send_head = Some(key);
        }
        record.send_tail = Some(key);
        record.ever_active = true;
        Ok(())
    }

    pub fn splice(
        &mut self,
        _token: Token,
        _source: SocketId,
        _destination: SocketId,
        _bytes: usize,
    ) -> io::Result<()> {
        Err(sys::unsupported(
            "TCP splice is Linux-only; Windows RIO requires application-owned receive/send leases",
        ))
    }

    pub fn shutdown(&mut self, socket: SocketId, how: Shutdown) -> io::Result<()> {
        self.socket(socket)?
            .handle
            .as_ref()
            .unwrap()
            .shutdown(how)?;
        let record = self.sockets.get_mut(socket.0).unwrap();
        if matches!(how, Shutdown::Read | Shutdown::Both) {
            record.read_shutdown = true;
        }
        if matches!(how, Shutdown::Write | Shutdown::Both) {
            record.write_shutdown = true;
        }
        Ok(())
    }

    pub fn abort(&mut self, socket: SocketId) -> io::Result<()> {
        let record = self.socket(socket)?;
        if record.info.kind != SocketKind::TcpStream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "abortive close requires a TCP stream",
            ));
        }
        record
            .handle
            .as_ref()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?
            .set_linger(Some(Duration::ZERO))?;
        // Preserve RIO/OVERLAPPED storage until actual completion dequeue.
        self.close(socket)
    }

    pub fn close(&mut self, socket: SocketId) -> io::Result<()> {
        let record = self.sockets.get_mut(socket.0).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "stale socket identifier")
        })?;
        // closesocket cancels kernel requests, but none of their storage is freed
        // here. OVERLAPPED and RIO operations retire only after dequeue.
        drop(record.handle.take());
        record.receive_commit = false;
        record.send_commit = false;
        if let Some(group) = &mut record.datagrams {
            group.stopping = true;
            group.discard = true;
        }
        for (_, operation) in self.operations.iter_mut() {
            if operation.socket == socket {
                operation.cancelled = true;
            }
        }
        self.notifier.notify();
        Ok(())
    }

    pub fn cancel(&mut self, token: Token) -> io::Result<()> {
        let Some(key) = self
            .operations
            .iter()
            .find_map(|(key, op)| (op.token == Some(token)).then_some(key))
        else {
            return Ok(());
        };
        if matches!(
            self.operations.get(key).unwrap().kind,
            OperationKind::DatagramReceive { .. }
        ) {
            let socket = self.operations.get(key).unwrap().socket;
            self.stop_datagrams(socket, None);
            return Ok(());
        }
        let control = self.control(key);
        let operation = self.operations.get_mut(key).unwrap();
        operation.cancelled = true;
        let socket = operation.socket;
        if operation.in_flight {
            match operation.kind {
                OperationKind::Connect { .. } | OperationKind::Accept { .. } => {
                    let raw = self.sockets.get(socket.0).and_then(|s| s.raw().ok());
                    if let Some(raw) = raw {
                        let result = unsafe {
                            windows_sys::Win32::System::IO::CancelIoEx(
                                raw as _,
                                ptr::addr_of!((*control).overlapped),
                            )
                        };
                        if result == 0 {
                            let error = io::Error::last_os_error();
                            if error.raw_os_error()
                                != Some(windows_sys::Win32::Foundation::ERROR_NOT_FOUND as i32)
                            {
                                return Err(error);
                            }
                        }
                    }
                }
                OperationKind::Receive { .. } => {
                    // RIO has no request-specific cancellation primitive. SIO_FLUSH
                    // retires pending requests; unrelated sends report their real
                    // completion/error, never an invented success or a retry.
                    if let Some(raw) = self.sockets.get(socket.0).and_then(|s| s.raw().ok()) {
                        sys::flush(raw)?;
                    }
                }
                OperationKind::DatagramReceive { .. } => unreachable!(),
                OperationKind::Send { .. } => {
                    // Dropping a send future is not permission to free its data
                    // or to interrupt another logical send on the same stream.
                }
            }
        }
        self.notifier.notify();
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        self.operations.is_empty() && self.pending.is_empty()
    }

    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
        for (_, record) in self.sockets.iter_mut() {
            drop(record.handle.take());
            record.receive_commit = false;
            record.send_commit = false;
            if let Some(group) = &mut record.datagrams {
                group.stopping = true;
                group.discard = true;
            }
        }
        for (_, operation) in self.operations.iter_mut() {
            operation.cancelled = true;
        }
        self.notifier.notify();
    }
}
