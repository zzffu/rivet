//! Owner-thread io_uring networking for stable Linux 7.2.7 and later (excluding RC).

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
compile_error!("the Linux backend supports x86_64 and aarch64 only");

mod net;
#[cfg(any(
    feature = "fixed-files",
    feature = "registered-buffers",
    feature = "provided-buffers"
))]
mod resources;
pub(crate) mod ring;
pub(crate) mod uapi;
#[cfg(feature = "zc-tx")]
mod zc;
#[cfg(feature = "zc-rx")]
pub(crate) mod zcrx;

#[cfg(feature = "fixed-files")]
use self::resources::FixedFiles;
#[cfg(feature = "registered-buffers")]
use self::resources::RegisteredBuffers;
#[cfg(feature = "buffer-bundles")]
use self::resources::SendBundle;
#[cfg(feature = "provided-buffers")]
use self::resources::{ProvidedBuffers, ProvidedRange};
use self::{
    ring::{Mapping, Ring},
    uapi::*,
};
#[cfg(any(
    feature = "direct-descriptors",
    feature = "incremental-buffers",
    feature = "zc-rx",
    feature = "zc-tx-fixed"
))]
use crate::config::Policy;
use crate::{
    buffer::{BufferPool, SendPayload, WriteBuf},
    capability::{CapabilityReport, KernelVersion, ZcStats},
    config::{Optimization, RuntimeConfig},
    driver::{Arena, Event, Received, SendOutcome, SocketId, SocketInfo, SocketKind, Token},
    socket::{ImportError, OwnedSocket, SocketOptions},
};
#[cfg(feature = "uring-msg-ring")]
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    ffi::CStr,
    io, mem,
    net::{Shutdown, SocketAddr},
    os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const WAKE: u64 = u64::MAX;
#[cfg(feature = "direct-descriptors")]
const CONTROL: u64 = u64::MAX - 1;
#[cfg(all(feature = "zc-rx", feature = "zc-observe"))]
const ZCRX_EVENT: u64 = u64::MAX - 2;
const CANCEL: u64 = u64::MAX - 3;
#[cfg(feature = "uring-msg-ring")]
const MESSAGE_WAKE: u64 = u64::MAX - 4;

/// A duplicated target descriptor is cloned under the same lock as detach. Its
/// owning guard outlives the syscall, without locking across kernel entry.
pub struct Notifier {
    event: OwnedFd,
    pending: AtomicBool,
    closed: AtomicBool,
    #[cfg(feature = "uring-msg-ring")]
    target: Mutex<Option<Arc<OwnedFd>>>,
}
impl Notifier {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            event: unsafe { OwnedFd::from_raw_fd(fd) },
            pending: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            #[cfg(feature = "uring-msg-ring")]
            target: Mutex::new(None),
        })
    }
    pub fn notify(&self) {
        if self.closed.load(Ordering::Acquire) || self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(feature = "uring-msg-ring")]
        {
            let target = self.target.lock().clone();
            if let Some(fd) = target.as_ref() {
                let sqe = Sqe {
                    opcode: IORING_OP_MSG_RING,
                    fd: fd.as_raw_fd(),
                    off: MESSAGE_WAKE,
                    ..Sqe::default()
                };
                let result = unsafe {
                    libc::syscall(
                        libc::SYS_io_uring_register,
                        -1,
                        IORING_REGISTER_SEND_MSG_RING,
                        &sqe,
                        1u32,
                    )
                };
                if result >= 0 {
                    return;
                }
            }
        }
        // eventfd is also registered on the ring. It is a reliability side
        // channel if a MSG_RING target is closing or its CQ cannot accept a wake.
        let value = 1u64;
        loop {
            let result =
                unsafe { libc::write(self.event.as_raw_fd(), (&value as *const u64).cast(), 8) };
            if result >= 0 || io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
    pub fn reset(&self) {
        self.drain();
        self.pending.store(false, Ordering::Release);
        // Runtime performs a work recheck after reset and before sleeping.
    }
    fn drain(&self) {
        let mut value = 0u64;
        loop {
            let result =
                unsafe { libc::read(self.event.as_raw_fd(), (&mut value as *mut u64).cast(), 8) };
            if result >= 0 {
                continue;
            }
            if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        #[cfg(feature = "uring-msg-ring")]
        self.detach();
        // Arc lifetime, not close(), owns the eventfd: concurrent notifiers never
        // use a raw descriptor after close and accidental descriptor reuse.
    }
    #[cfg(feature = "uring-msg-ring")]
    fn detach(&self) {
        let target = self.target.lock().take();
        drop(target);
    }
    #[cfg(feature = "uring-msg-ring")]
    fn install(&self, ring: &Ring) -> io::Result<()> {
        let raw = unsafe { libc::fcntl(ring.fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let sqe = Sqe {
            opcode: IORING_OP_MSG_RING,
            fd: fd.as_raw_fd(),
            off: MESSAGE_WAKE,
            ..Sqe::default()
        };
        if unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                -1,
                IORING_REGISTER_SEND_MSG_RING,
                &sqe,
                1u32,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        *self.target.lock() = Some(Arc::new(fd));
        Ok(())
    }
}

pub struct Shared {
    #[cfg(feature = "zc-rx-shared")]
    zcrx: zcrx::ZcrxShared,
}
impl Shared {
    pub fn new(_workers: usize) -> Self {
        Self {
            #[cfg(feature = "zc-rx-shared")]
            zcrx: zcrx::ZcrxShared::new(),
        }
    }
}

struct SocketEntry {
    fd: Option<OwnedSocket>,
    #[cfg(feature = "fixed-files")]
    fixed: Option<u32>,
    kind: SocketKind,
    options: SocketOptions,
    active: usize,
    native_pending: usize,
    closing: bool,
    aborting: bool,
    receive: Option<u64>,
    accept: Option<u64>,
    sending: Option<u64>,
    receive_credits: usize,
    accept_credits: usize,
}
impl SocketEntry {
    fn native(&self) -> io::Result<(i32, u8)> {
        #[cfg(feature = "fixed-files")]
        if let Some(index) = self.fixed {
            return Ok((index as i32, IOSQE_FIXED_FILE));
        }
        Ok((
            self.fd
                .as_ref()
                .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?
                .as_raw_fd(),
            0,
        ))
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Idle,
    Connect,
    Accept,
    Receive,
    Send,
    #[cfg(feature = "tcp-splice")]
    Splice,
}
#[cfg(feature = "tcp-splice")]
#[derive(Clone, Copy, PartialEq)]
enum SpliceStage {
    Read,
    Write,
}
/// Raw-mapped storage, separate from borrowed operation metadata. The kernel
/// writes single-shot recvmsg output here while Operation is freely borrowed.
/// No Rust reference into this mapping exists during an outstanding request.
#[repr(C)]
struct NativeMessage {
    address: [u64; net::ADDRESS_BYTES / 8],
    message: UserMsgHdr,
    control: [u64; net::CONTROL_BYTES / 8],
}
struct Operation {
    generation: u32,
    kind: Kind,
    token: Token,
    socket: SocketId,
    #[cfg(feature = "tcp-splice")]
    destination: Option<SocketId>,
    submitted: bool,
    native_pending: bool,
    queued: bool,
    stopping: bool,
    cancel_sent: bool,
    pending: usize,
    retry_at: Option<Instant>,
    issued_sequence: u64,
    completed_sequence: u64,
    address_len: i32,
    iovecs: Vec<libc::iovec>,
    payload: Option<SendPayload>,
    receive: Option<WriteBuf>,
    provided: bool,
    multishot: bool,
    #[cfg(feature = "buffer-bundles")]
    bundle: Option<usize>,
    finished: bool,
    #[cfg(feature = "zc-tx")]
    zc: zc::ZcTx,
    use_zc: bool,
    sent: usize,
    send_error: Option<i32>,
    requested: usize,
    #[cfg(feature = "tcp-splice")]
    pipe: Option<(OwnedFd, OwnedFd)>,
    #[cfg(feature = "tcp-splice")]
    pipe_bytes: usize,
    #[cfg(feature = "tcp-splice")]
    pipe_sent: usize,
    #[cfg(feature = "tcp-splice")]
    splice_stage: SpliceStage,
    #[cfg(feature = "tcp-splice")]
    splice_wait: bool,
}
impl Operation {
    fn new(max_iovecs: usize, #[cfg(feature = "zc-tx")] zc_enabled: bool) -> Self {
        Self {
            generation: 1,
            kind: Kind::Idle,
            token: Token(0),
            socket: SocketId(0),
            #[cfg(feature = "tcp-splice")]
            destination: None,
            submitted: false,
            native_pending: false,
            queued: false,
            stopping: false,
            finished: false,
            cancel_sent: false,
            pending: 0,
            retry_at: None,
            issued_sequence: 0,
            completed_sequence: 0,
            address_len: 0,
            iovecs: Vec::with_capacity(max_iovecs),
            payload: None,
            receive: None,
            provided: false,
            multishot: false,
            #[cfg(feature = "buffer-bundles")]
            bundle: None,
            #[cfg(feature = "zc-tx")]
            zc: zc::ZcTx::new(if zc_enabled { max_iovecs } else { 0 }),
            use_zc: false,
            sent: 0,
            send_error: None,
            requested: 0,
            #[cfg(feature = "tcp-splice")]
            pipe: None,
            #[cfg(feature = "tcp-splice")]
            pipe_bytes: 0,
            #[cfg(feature = "tcp-splice")]
            pipe_sent: 0,
            #[cfg(feature = "tcp-splice")]
            splice_stage: SpliceStage::Read,
            #[cfg(feature = "tcp-splice")]
            splice_wait: false,
        }
    }
    fn key(&self, index: usize) -> u64 {
        (u64::from(self.generation) << 32) | index as u64
    }
}
#[derive(Clone, Copy)]
struct Pending {
    cqe: Cqe,
    sequence: u64,
    #[cfg(feature = "provided-buffers")]
    provided: Option<ProvidedRange>,
}

pub struct Driver {
    // Ring closes before registrations' memory and before the normal pool clone.
    ring: Ring,
    #[cfg(feature = "fixed-files")]
    fixed: Option<FixedFiles>,
    #[cfg(feature = "registered-buffers")]
    registered: Option<RegisteredBuffers>,
    #[cfg(feature = "provided-buffers")]
    provided: Option<ProvidedBuffers>,
    #[cfg(feature = "buffer-bundles")]
    bundles: Vec<SendBundle>,
    #[cfg(feature = "buffer-bundles")]
    free_bundles: Vec<usize>,
    #[cfg(feature = "zc-rx")]
    zcrx: Option<zcrx::Zcrx>,
    pool: BufferPool,
    notifier: Arc<Notifier>,
    #[cfg(feature = "zc-rx-shared")]
    _shared: Arc<Shared>,
    config: RuntimeConfig,
    report: CapabilityReport,
    sockets: Arena<SocketEntry>,
    operations: Box<[Operation]>,
    native_messages: Mapping,
    free_operations: Vec<u32>,
    tokens: HashMap<Token, u64>,
    ready: VecDeque<u64>,
    deferred: VecDeque<Pending>,
    deferred_limit: usize,
    blocked_ingest: Option<Cqe>,
    #[cfg(feature = "direct-descriptors")]
    control_inflight: Option<u8>,
    scratch: Vec<u64>,
    send_bytes: usize,
    stopping: bool,
    aborting_sockets: usize,
    shutdown_error: Option<io::Error>,
    wake_armed: bool,
    sq_deferred: bool,
    retry_deadline: Option<Instant>,
    #[cfg(all(feature = "zc-tx", feature = "zc-observe"))]
    stats: ZcStats,
}

impl Driver {
    pub fn new(
        config: &RuntimeConfig,
        worker: usize,
        pool: BufferPool,
        notifier: Arc<Notifier>,
        _shared: Arc<Shared>,
    ) -> io::Result<Self> {
        let config = config.normalized()?;
        let native_bytes = config
            .limits
            .max_operations
            .checked_mul(size_of::<NativeMessage>())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native operation storage size overflow",
                )
            })?;
        let native_messages = Mapping::anonymous(native_bytes)?;
        let mut uname: libc::utsname = unsafe { mem::zeroed() };
        if unsafe { libc::uname(&mut uname) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let release = unsafe { CStr::from_ptr(uname.release.as_ptr()) }
            .to_str()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 Linux release"))?;
        let kernel = KernelVersion::parse(release)?;
        kernel.require_supported()?;
        let mut report = CapabilityReport::new("linux-io_uring", worker);
        report.kernel = Some(kernel);
        let params = Params {
            flags: IORING_SETUP_CQSIZE
                | IORING_SETUP_SUBMIT_ALL
                | IORING_SETUP_SINGLE_ISSUER
                | IORING_SETUP_DEFER_TASKRUN
                | IORING_SETUP_TASKRUN_FLAG
                | IORING_SETUP_NO_SQARRAY,
            cq_entries: config.linux.cq_entries,
            ..Params::default()
        };
        #[cfg(any(
            feature = "zc-rx",
            feature = "registered-wait",
            feature = "mixed-cqe",
            feature = "sq-rewind",
            feature = "uring-sqpoll"
        ))]
        let mut params = params;
        #[cfg(feature = "zc-rx")]
        if requested(&config, Optimization::ZcRx) || requested(&config, Optimization::ZcRxNodev) {
            params.flags |= IORING_SETUP_CQE32;
        }
        // WAIT_ARG memory must be installed while no kernel waiters can exist.
        #[cfg(feature = "registered-wait")]
        if requested(&config, Optimization::RegisteredWait) {
            params.flags |= IORING_SETUP_R_DISABLED;
        }
        let ring = Ring::new(config.linux.sq_entries, params)?;
        #[cfg(any(
            feature = "registered-ring",
            feature = "registered-wait",
            feature = "mixed-cqe",
            feature = "sq-rewind",
            feature = "uring-sqpoll"
        ))]
        let mut ring = ring;
        #[cfg(any(feature = "mixed-cqe", feature = "sq-rewind", feature = "uring-sqpoll"))]
        for (feature, flag) in [
            #[cfg(feature = "mixed-cqe")]
            (Optimization::MixedCqe, IORING_SETUP_CQE_MIXED),
            #[cfg(feature = "sq-rewind")]
            (Optimization::SqRewind, IORING_SETUP_SQ_REWIND),
            #[cfg(feature = "uring-sqpoll")]
            (Optimization::SqPoll, IORING_SETUP_SQPOLL),
        ] {
            if !requested(&config, feature) {
                continue;
            }
            let mut attempt = params;
            attempt.flags |= flag;
            #[cfg(feature = "mixed-cqe")]
            if feature == Optimization::MixedCqe {
                attempt.flags &= !IORING_SETUP_CQE32;
            }
            #[cfg(feature = "uring-sqpoll")]
            if feature == Optimization::SqPoll {
                attempt.flags &= !(IORING_SETUP_DEFER_TASKRUN
                    | IORING_SETUP_TASKRUN_FLAG
                    | IORING_SETUP_COOP_TASKRUN);
                attempt.sq_thread_idle = config.linux.sqpoll_idle.as_millis().max(1) as u32;
                if let Some(cpu) = config.linux.sqpoll_cpu {
                    attempt.flags |= IORING_SETUP_SQ_AFF;
                    attempt.sq_thread_cpu = cpu;
                }
            }
            match Ring::new(config.linux.sq_entries, attempt) {
                Ok(replacement) => {
                    ring = replacement;
                    params = attempt;
                    report.decide(feature, config.policy(feature), Ok(()))?;
                }
                Err(error) => {
                    report.decide(feature, config.policy(feature), Err(error.to_string()))?;
                }
            }
        }
        let probe = ring.probe()?;
        for opcode in [
            IORING_OP_CONNECT,
            IORING_OP_ACCEPT,
            IORING_OP_SEND,
            IORING_OP_SENDMSG,
            IORING_OP_RECV,
            IORING_OP_RECVMSG,
            IORING_OP_ASYNC_CANCEL,
            IORING_OP_POLL_ADD,
        ] {
            if !supported(&probe, opcode) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("required io_uring opcode {opcode} unavailable"),
                ));
            }
        }
        #[cfg(feature = "fixed-files")]
        let mut fixed = None;
        #[cfg(feature = "fixed-files")]
        if requested(&config, Optimization::FixedFiles) {
            match FixedFiles::new(
                &ring,
                config.limits.max_sockets,
                requested(&config, Optimization::DirectDescriptors),
            ) {
                Ok(resource) => {
                    fixed = Some(resource);
                    report.decide(
                        Optimization::FixedFiles,
                        config.policy(Optimization::FixedFiles),
                        Ok(()),
                    )?;
                }
                #[cfg(feature = "direct-descriptors")]
                Err(error) if config.policy(Optimization::DirectDescriptors) == Policy::Auto => {
                    report.decide(
                        Optimization::DirectDescriptors,
                        Policy::Auto,
                        Err(error.to_string()),
                    )?;
                    match FixedFiles::new(&ring, config.limits.max_sockets, false) {
                        Ok(resource) => {
                            fixed = Some(resource);
                            report.decide(
                                Optimization::FixedFiles,
                                config.policy(Optimization::FixedFiles),
                                Ok(()),
                            )?;
                        }
                        Err(error) => {
                            report.decide(
                                Optimization::FixedFiles,
                                config.policy(Optimization::FixedFiles),
                                Err(error.to_string()),
                            )?;
                        }
                    }
                }
                Err(error) => {
                    report.decide(
                        Optimization::FixedFiles,
                        config.policy(Optimization::FixedFiles),
                        Err(error.to_string()),
                    )?;
                }
            }
        }
        #[cfg(feature = "direct-descriptors")]
        if requested(&config, Optimization::DirectDescriptors)
            && report.state(Optimization::DirectDescriptors).is_none()
        {
            decide(
                &mut report,
                &config,
                Optimization::DirectDescriptors,
                fixed.is_some()
                    && supported(&probe, IORING_OP_FIXED_FD_INSTALL)
                    && supported(&probe, IORING_OP_SOCKET),
                "direct descriptors require initialized fixed files, SOCKET and FIXED_FD_INSTALL",
            )?;
        }
        #[cfg(any(
            feature = "registered-ring",
            feature = "registered-wait",
            feature = "uring-napi"
        ))]
        for feature in [
            #[cfg(feature = "registered-ring")]
            Optimization::RegisteredRing,
            #[cfg(feature = "registered-wait")]
            Optimization::RegisteredWait,
            #[cfg(feature = "uring-napi")]
            Optimization::NapiBusyPoll,
        ] {
            if !requested(&config, feature) {
                continue;
            }
            let result = match feature {
                #[cfg(feature = "registered-ring")]
                Optimization::RegisteredRing => ring.register_ring(),
                #[cfg(feature = "registered-wait")]
                Optimization::RegisteredWait => ring.register_wait(),
                #[cfg(feature = "uring-napi")]
                Optimization::NapiBusyPoll => initialize_napi(&ring, &config),
                _ => unreachable!(),
            };
            report.decide(
                feature,
                config.policy(feature),
                result.map_err(|e| e.to_string()),
            )?;
        }
        // Auto registration failures still leave a disabled ring to activate.
        #[cfg(feature = "registered-wait")]
        ring.enable()?;
        #[cfg(feature = "provided-buffers")]
        let mut provided = None;
        #[cfg(feature = "provided-buffers")]
        if requested(&config, Optimization::ProvidedBuffers) {
            let incremental = requested(&config, Optimization::IncrementalBuffers);
            let result = ProvidedBuffers::new(
                &ring,
                config.limits.pool.bytes,
                config.limits.pool.block_size,
                incremental,
                notifier.clone(),
            );
            match result {
                Ok(resource) => {
                    provided = Some(resource);
                    report.decide(
                        Optimization::ProvidedBuffers,
                        config.policy(Optimization::ProvidedBuffers),
                        Ok(()),
                    )?;
                    if incremental {
                        report.decide(
                            Optimization::IncrementalBuffers,
                            config.policy(Optimization::IncrementalBuffers),
                            Ok(()),
                        )?;
                    }
                }
                #[cfg(feature = "incremental-buffers")]
                Err(error)
                    if incremental
                        && config.policy(Optimization::IncrementalBuffers) == Policy::Auto =>
                {
                    report.decide(
                        Optimization::IncrementalBuffers,
                        Policy::Auto,
                        Err(error.to_string()),
                    )?;
                    match ProvidedBuffers::new(
                        &ring,
                        config.limits.pool.bytes,
                        config.limits.pool.block_size,
                        false,
                        notifier.clone(),
                    ) {
                        Ok(resource) => {
                            provided = Some(resource);
                            report.decide(
                                Optimization::ProvidedBuffers,
                                config.policy(Optimization::ProvidedBuffers),
                                Ok(()),
                            )?;
                        }
                        Err(error) => {
                            report.decide(
                                Optimization::ProvidedBuffers,
                                config.policy(Optimization::ProvidedBuffers),
                                Err(error.to_string()),
                            )?;
                        }
                    }
                }
                Err(error) => {
                    report.decide(
                        Optimization::ProvidedBuffers,
                        config.policy(Optimization::ProvidedBuffers),
                        Err(error.to_string()),
                    )?;
                }
            }
        }
        #[cfg(feature = "buffer-bundles")]
        let mut bundles = Vec::new();
        #[cfg(feature = "buffer-bundles")]
        if requested(&config, Optimization::BufferBundles) {
            let result = if provided.is_none() || ring.features() & IORING_FEAT_RECVSEND_BUNDLE == 0
            {
                Err(unsupported(
                    "buffer bundles require initialized provided rings and RECVSEND_BUNDLE",
                ))
            } else {
                // Registration is cold and finite. The pool is shared by send
                // operation slots rather than registering one ring per packet.
                let count = (config.linux.sq_entries as usize)
                    .min(config.limits.max_operations)
                    .min(65530);
                (0..count).try_for_each(|index| {
                    SendBundle::new(&ring, 2 + index as u16, config.limits.max_iovecs)
                        .map(|bundle| bundles.push(bundle))
                })
            };
            if result.is_err() {
                for bundle in &bundles {
                    bundle.unregister(&ring)?;
                }
                bundles.clear();
            }
            report.decide(
                Optimization::BufferBundles,
                config.policy(Optimization::BufferBundles),
                result.map_err(|e| e.to_string()),
            )?;
        }
        #[cfg(feature = "multishot-accept")]
        if requested(&config, Optimization::MultishotAccept) {
            decide(
                &mut report,
                &config,
                Optimization::MultishotAccept,
                true,
                "multishot accept unavailable",
            )?;
        }
        #[cfg(feature = "multishot-recv")]
        if requested(&config, Optimization::MultishotRecv) {
            decide(
                &mut report,
                &config,
                Optimization::MultishotRecv,
                provided.is_some(),
                "multishot receive requires initialized provided buffers",
            )?;
        }
        #[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
        for (feature, option) in [
            #[cfg(feature = "udp-gso")]
            (Optimization::UdpGso, net::UDP_SEGMENT),
            #[cfg(feature = "udp-gro")]
            (Optimization::UdpGro, net::UDP_GRO),
        ] {
            if !requested(&config, feature) {
                continue;
            }
            let result = (|| {
                let socket = net::create(
                    "127.0.0.1:0".parse().unwrap(),
                    SocketKind::Udp,
                    &SocketOptions::udp(),
                    false,
                )?;
                net::set_int(
                    socket.as_raw_fd(),
                    libc::IPPROTO_UDP,
                    option,
                    if feature == Optimization::UdpGro {
                        1
                    } else {
                        0
                    },
                )
            })();
            report.decide(
                feature,
                config.policy(feature),
                result.map_err(|e| e.to_string()),
            )?;
        }
        #[cfg(feature = "tcp-splice")]
        if requested(&config, Optimization::TcpSplice) {
            let result = if supported(&probe, IORING_OP_SPLICE) {
                make_pipe(config.linux.splice_pipe_bytes).map(drop)
            } else {
                Err(unsupported("SPLICE unavailable"))
            };
            report.decide(
                Optimization::TcpSplice,
                config.policy(Optimization::TcpSplice),
                result.map_err(|e| e.to_string()),
            )?;
        }
        #[cfg(feature = "zc-tx")]
        if requested(&config, Optimization::ZcTx) {
            decide(
                &mut report,
                &config,
                Optimization::ZcTx,
                supported(&probe, IORING_OP_SEND_ZC) && supported(&probe, IORING_OP_SENDMSG_ZC),
                "SEND_ZC/SENDMSG_ZC unavailable",
            )?;
        }
        #[cfg(feature = "zc-rx")]
        let mut zcrx = None;
        #[cfg(feature = "zc-rx")]
        if requested(&config, Optimization::ZcRx) || requested(&config, Optimization::ZcRxNodev) {
            let primary = if requested(&config, Optimization::ZcRxNodev) {
                Optimization::ZcRxNodev
            } else {
                Optimization::ZcRx
            };
            match zcrx::Zcrx::new(
                &ring,
                &config,
                worker,
                #[cfg(feature = "zc-rx-shared")]
                &_shared.zcrx,
                #[cfg(feature = "zc-observe")]
                ZCRX_EVENT,
            ) {
                Ok(resource) => {
                    resource.set_notifier(&notifier)?;
                    report.receive_mode = resource.mode();
                    zcrx = Some(resource);
                    report.decide(primary, config.policy(primary), Ok(()))?;
                }
                Err(error) => {
                    let reason = error.to_string();
                    report.decide(primary, config.policy(primary), Err(reason.clone()))?;
                    if zcrx::requires_ring_close(&error) {
                        // IFQ has no unregister operation. Auto may use ordinary
                        // I/O only on a freshly created ring, never one still
                        // owning the failed NIC registration.
                        let mut fallback = config.clone();
                        let disable_observe = !report.enabled(Optimization::ZcTx);
                        for feature in [
                            primary,
                            Optimization::ZcRxLargeChunks,
                            Optimization::ZcRxShared,
                        ] {
                            fallback.optimizations.insert(feature, Policy::Off);
                        }
                        if disable_observe {
                            fallback
                                .optimizations
                                .insert(Optimization::ZcObserve, Policy::Off);
                        }
                        #[cfg(feature = "uring-msg-ring")]
                        notifier.detach();
                        drop(ring);
                        #[cfg(feature = "buffer-bundles")]
                        drop(bundles);
                        #[cfg(feature = "provided-buffers")]
                        drop(provided);
                        #[cfg(feature = "fixed-files")]
                        drop(fixed);
                        let mut driver = Self::new(&fallback, worker, pool, notifier, _shared)?;
                        for feature in [
                            primary,
                            Optimization::ZcRxLargeChunks,
                            Optimization::ZcRxShared,
                        ] {
                            driver.report.decide(
                                feature,
                                config.policy(feature),
                                Err(reason.clone()),
                            )?;
                        }
                        if disable_observe {
                            driver.report.decide(
                                Optimization::ZcObserve,
                                config.policy(Optimization::ZcObserve),
                                Err(reason),
                            )?;
                        }
                        return Ok(driver);
                    }
                }
            }
        }
        #[cfg(feature = "registered-buffers")]
        let mut registered = None;
        #[cfg(feature = "registered-buffers")]
        if requested(&config, Optimization::RegisteredBuffers) {
            let regions = pool.regions();
            #[cfg(all(
                feature = "zc-tx-fixed",
                any(feature = "provided-buffers", feature = "zc-rx")
            ))]
            let mut regions = regions;
            let extra = requested(&config, Optimization::ZcTxFixed);
            if extra {
                #[cfg(all(feature = "zc-tx-fixed", feature = "provided-buffers"))]
                if let Some(buffers) = &provided {
                    regions.push(buffers.memory_region(regions.len() as u32));
                }
                #[cfg(all(feature = "zc-tx-fixed", feature = "zc-rx"))]
                if let Some(rx) = &zcrx {
                    regions.push(rx.memory_region(regions.len() as u32));
                }
            }
            match RegisteredBuffers::new(&ring, regions) {
                Ok(resource) => {
                    registered = Some(resource);
                    report.decide(
                        Optimization::RegisteredBuffers,
                        config.policy(Optimization::RegisteredBuffers),
                        Ok(()),
                    )?;
                }
                #[cfg(feature = "zc-tx-fixed")]
                Err(error) if extra && config.policy(Optimization::ZcTxFixed) == Policy::Auto => {
                    report.decide(
                        Optimization::ZcTxFixed,
                        Policy::Auto,
                        Err(error.to_string()),
                    )?;
                    match RegisteredBuffers::new(&ring, pool.regions()) {
                        Ok(resource) => {
                            registered = Some(resource);
                            report.decide(
                                Optimization::RegisteredBuffers,
                                config.policy(Optimization::RegisteredBuffers),
                                Ok(()),
                            )?;
                        }
                        Err(error) => {
                            report.decide(
                                Optimization::RegisteredBuffers,
                                config.policy(Optimization::RegisteredBuffers),
                                Err(error.to_string()),
                            )?;
                        }
                    }
                }
                Err(error) => {
                    report.decide(
                        Optimization::RegisteredBuffers,
                        config.policy(Optimization::RegisteredBuffers),
                        Err(error.to_string()),
                    )?;
                }
            }
        }
        #[cfg(feature = "zc-tx-fixed")]
        if requested(&config, Optimization::ZcTxFixed)
            && report.state(Optimization::ZcTxFixed).is_none()
        {
            let operational = report.enabled(Optimization::ZcTx) && registered.is_some();
            decide(
                &mut report,
                &config,
                Optimization::ZcTxFixed,
                operational,
                "ZC transmit prerequisite resource unavailable",
            )?;
        }
        #[cfg(feature = "zc-tx-vectored")]
        if requested(&config, Optimization::ZcTxVectored) {
            let operational = report.enabled(Optimization::ZcTx);
            decide(
                &mut report,
                &config,
                Optimization::ZcTxVectored,
                operational,
                "ZC transmit prerequisite resource unavailable",
            )?;
        }
        #[cfg(any(feature = "zc-rx-large-chunks", feature = "zc-rx-shared"))]
        for feature in [Optimization::ZcRxLargeChunks, Optimization::ZcRxShared] {
            if requested(&config, feature) {
                let support = zcrx.as_ref().map_or_else(
                    || Err("ZCRX registration unavailable".to_owned()),
                    |rx| rx.supports(feature),
                );
                report.decide(feature, config.policy(feature), support)?;
            }
        }
        #[cfg(feature = "zc-observe")]
        if requested(&config, Optimization::ZcObserve) {
            let support = match () {
                _ if report.enabled(Optimization::ZcTx) => Ok(()),
                #[cfg(feature = "zc-rx")]
                _ if zcrx.is_some() => zcrx.as_ref().unwrap().supports(Optimization::ZcObserve),
                _ => Err("no operational ZC observation source".to_owned()),
            };
            report.decide(
                Optimization::ZcObserve,
                config.policy(Optimization::ZcObserve),
                support,
            )?;
        }
        #[cfg(feature = "uring-msg-ring")]
        if requested(&config, Optimization::MsgRing) {
            let support = if supported(&probe, IORING_OP_MSG_RING) {
                notifier.install(&ring)
            } else {
                Err(unsupported("MSG_RING unavailable"))
            };
            report.decide(
                Optimization::MsgRing,
                config.policy(Optimization::MsgRing),
                support.map_err(|error| error.to_string()),
            )?;
        }
        report.finish(&config)?;
        let max_operations = config.limits.max_operations;
        let provided_entries = 0;
        #[cfg(feature = "provided-buffers")]
        let provided_entries =
            provided_entries + provided.as_ref().map_or(0, ProvidedBuffers::entries);
        let deferred_limit =
            config.linux.cq_entries as usize * 2 + max_operations + provided_entries + 8;
        #[cfg(feature = "buffer-bundles")]
        let free_bundles = (0..bundles.len()).rev().collect();
        let mut driver = Self {
            ring,
            #[cfg(feature = "fixed-files")]
            fixed,
            #[cfg(feature = "registered-buffers")]
            registered,
            #[cfg(feature = "provided-buffers")]
            provided,
            #[cfg(feature = "buffer-bundles")]
            bundles,
            #[cfg(feature = "buffer-bundles")]
            free_bundles,
            #[cfg(feature = "zc-rx")]
            zcrx,
            pool,
            notifier,
            #[cfg(feature = "zc-rx-shared")]
            _shared,
            sockets: Arena::new(config.limits.max_sockets),
            operations: (0..max_operations)
                .map(|_| {
                    Operation::new(
                        config.limits.max_iovecs,
                        #[cfg(feature = "zc-tx")]
                        report.enabled(Optimization::ZcTx),
                    )
                })
                .collect(),
            native_messages,
            free_operations: (0..max_operations as u32).rev().collect(),
            tokens: HashMap::with_capacity(max_operations),
            ready: VecDeque::with_capacity(max_operations),
            deferred: VecDeque::with_capacity(deferred_limit),
            deferred_limit,
            blocked_ingest: None,
            #[cfg(feature = "direct-descriptors")]
            control_inflight: None,
            scratch: Vec::with_capacity(max_operations),
            config,
            report,
            send_bytes: 0,
            stopping: false,
            aborting_sockets: 0,
            shutdown_error: None,
            wake_armed: false,
            sq_deferred: false,
            retry_deadline: None,
            #[cfg(all(feature = "zc-tx", feature = "zc-observe"))]
            stats: ZcStats::default(),
        };
        driver.arm_wake()?;
        driver.ring.submit()?;
        Ok(driver)
    }
    pub fn capabilities(&self) -> &CapabilityReport {
        &self.report
    }
    pub fn zc_stats(&self) -> ZcStats {
        #[cfg(feature = "zc-observe")]
        if self.report.enabled(Optimization::ZcObserve) {
            let result = ZcStats::default();
            #[cfg(any(feature = "zc-tx", feature = "zc-rx"))]
            let mut result = result;
            #[cfg(feature = "zc-tx")]
            result.accumulate(self.stats);
            #[cfg(feature = "zc-rx")]
            if let Some(zcrx) = &self.zcrx {
                result.accumulate(zcrx.stats());
            }
            return result;
        }
        ZcStats::default()
    }
}

#[cfg(any(
    feature = "fixed-files",
    feature = "registered-buffers",
    feature = "registered-ring",
    feature = "registered-wait",
    feature = "provided-buffers",
    feature = "multishot-accept",
    feature = "sq-rewind",
    feature = "mixed-cqe",
    feature = "zc-tx",
    feature = "zc-rx",
    feature = "zc-observe",
    feature = "uring-sqpoll",
    feature = "uring-napi",
    feature = "uring-msg-ring",
    feature = "udp-gso",
    feature = "udp-gro",
    feature = "tcp-splice"
))]
fn requested(config: &RuntimeConfig, feature: Optimization) -> bool {
    feature.compiled() && config.requested(feature)
}
fn supported(probe: &Probe, opcode: u8) -> bool {
    probe.ops[..probe.ops_len as usize]
        .iter()
        .any(|op| op.op == opcode && op.flags & 1 != 0)
}
#[cfg(any(
    feature = "direct-descriptors",
    feature = "multishot-accept",
    feature = "multishot-recv",
    feature = "zc-tx"
))]
fn decide(
    report: &mut CapabilityReport,
    config: &RuntimeConfig,
    feature: Optimization,
    supported: bool,
    reason: &'static str,
) -> io::Result<()> {
    report.decide(
        feature,
        config.policy(feature),
        if supported {
            Ok(())
        } else {
            Err(reason.to_owned())
        },
    )?;
    Ok(())
}
fn unsupported(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}
fn resource_error() -> io::Error {
    io::Error::from(io::ErrorKind::WouldBlock)
}
#[cfg(feature = "uring-napi")]
fn initialize_napi(ring: &Ring, config: &RuntimeConfig) -> io::Result<()> {
    if config.linux.napi_busy_poll.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NAPI polling requires a nonzero bounded timeout",
        ));
    }
    let napi = Napi {
        busy_poll_to: config.linux.napi_busy_poll.as_micros().max(1) as u32,
        prefer_busy_poll: u8::from(config.linux.napi_prefer_busy_poll),
        op_param: u32::from(!config.linux.napi_ids.is_empty()),
        ..Napi::default()
    };
    unsafe {
        ring.register(IORING_REGISTER_NAPI, (&napi as *const Napi).cast(), 1)?;
    }
    for &id in &config.linux.napi_ids {
        let entry = Napi {
            opcode: 1,
            op_param: id,
            ..Napi::default()
        };
        if let Err(error) =
            unsafe { ring.register(IORING_REGISTER_NAPI, (&entry as *const Napi).cast(), 1) }
        {
            unsafe {
                let _ = ring.register(IORING_UNREGISTER_NAPI, std::ptr::null(), 1);
            }
            return Err(error);
        }
    }
    Ok(())
}
#[cfg(feature = "tcp-splice")]
fn make_pipe(bytes: usize) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut fds = [-1; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let pair = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    if bytes > i32::MAX as usize || bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid splice pipe capacity",
        ));
    }
    if unsafe { libc::fcntl(pair.0.as_raw_fd(), libc::F_SETPIPE_SZ, bytes as i32) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pair)
}

impl Driver {
    fn native_message(&self, index: usize) -> *mut NativeMessage {
        debug_assert!(index < self.operations.len());
        // Mapping owns raw, page-aligned bytes, not a Box/reference whose
        // pointee is reborrowed when Driver or Operation metadata is accessed.
        unsafe {
            self.native_messages
                .as_ptr()
                .cast::<NativeMessage>()
                .add(index)
        }
    }
    fn op(&self, key: u64) -> Option<&Operation> {
        self.operations
            .get(key as u32 as usize)
            .filter(|op| op.kind != Kind::Idle && op.generation == (key >> 32) as u32)
    }
    fn op_mut(&mut self, key: u64) -> Option<&mut Operation> {
        self.operations
            .get_mut(key as u32 as usize)
            .filter(|op| op.kind != Kind::Idle && op.generation == (key >> 32) as u32)
    }
    fn allocate(&mut self, token: Token, socket: SocketId, kind: Kind) -> io::Result<u64> {
        if self.stopping {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        if self.tokens.contains_key(&token) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "operation token is already live",
            ));
        }
        let index = self.free_operations.pop().ok_or_else(resource_error)? as usize;
        // Retirement waits for both the terminal native CQE and all deferred
        // publications before making this index available again.
        unsafe {
            self.native_message(index).write_bytes(0, 1);
        }
        let op = &mut self.operations[index];
        op.kind = kind;
        op.token = token;
        op.socket = socket;
        #[cfg(feature = "tcp-splice")]
        {
            op.destination = None;
        }
        op.address_len = 0;
        op.submitted = false;
        op.native_pending = false;
        op.queued = false;
        op.stopping = false;
        op.finished = false;
        op.cancel_sent = false;
        op.pending = 0;
        op.retry_at = None;
        op.issued_sequence = 0;
        op.completed_sequence = 0;
        op.payload = None;
        op.receive = None;
        op.provided = false;
        op.multishot = false;
        #[cfg(feature = "buffer-bundles")]
        {
            op.bundle = None;
        }
        op.use_zc = false;
        op.sent = 0;
        op.send_error = None;
        #[cfg(feature = "tcp-splice")]
        {
            op.pipe_bytes = 0;
            op.pipe_sent = 0;
            op.splice_stage = SpliceStage::Read;
            op.splice_wait = false;
        }
        op.iovecs.clear();
        let key = op.key(index);
        self.tokens.insert(token, key);
        if let Some(socket) = self.sockets.get_mut(socket.0) {
            socket.active += 1;
        }
        Ok(key)
    }
    fn schedule(&mut self, key: u64) {
        if let Some(op) = self.op_mut(key) {
            if op.queued {
                return;
            }
            op.queued = true;
            self.ready.push_back(key);
        }
    }
    fn retire(&mut self, key: u64) -> io::Result<()> {
        let index = key as u32 as usize;
        let Some(op) = self.op(key) else {
            return Ok(());
        };
        debug_assert!(!op.submitted && op.pending == 0);
        #[cfg(feature = "zc-tx")]
        debug_assert!(op.zc.is_idle());
        let socket = op.socket;
        #[cfg(not(feature = "tcp-splice"))]
        let destination = None;
        #[cfg(feature = "tcp-splice")]
        let destination = op.destination;
        let token = op.token;
        let op = &mut self.operations[index];
        if op.kind == Kind::Send {
            self.send_bytes = self.send_bytes.saturating_sub(op.requested);
        }
        op.payload = None;
        op.receive = None;
        #[cfg(feature = "buffer-bundles")]
        if let Some(bundle) = op.bundle.take() {
            self.free_bundles.push(bundle);
        }
        #[cfg(feature = "tcp-splice")]
        {
            op.pipe = None;
        }
        op.kind = Kind::Idle;
        self.tokens.remove(&token);
        if op.generation < u32::MAX - 1 {
            op.generation += 1;
            self.free_operations.push(index as u32);
        }
        for id in [Some(socket), destination].into_iter().flatten() {
            if let Some(entry) = self.sockets.get_mut(id.0) {
                entry.active -= 1;
                if entry.receive == Some(key) {
                    entry.receive = None;
                }
                if entry.accept == Some(key) {
                    entry.accept = None;
                }
                if entry.sending == Some(key) {
                    entry.sending = None;
                }
            }
            self.close_if_idle(id)?;
        }
        Ok(())
    }
    fn native(&self, socket: SocketId) -> io::Result<(i32, u8)> {
        let socket = self
            .sockets
            .get(socket.0)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
        socket.native()
    }
    fn check_socket(&self, socket: SocketId, kind: Option<SocketKind>) -> io::Result<&SocketEntry> {
        let entry = self
            .sockets
            .get(socket.0)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
        if entry.closing || self.stopping {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        if kind.is_some_and(|kind| entry.kind != kind) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "wrong socket kind",
            ));
        }
        Ok(entry)
    }
    fn add_socket(
        &mut self,
        fd: OwnedSocket,
        kind: SocketKind,
        options: &SocketOptions,
        _direct: Option<u32>,
    ) -> Result<SocketInfo, ImportError> {
        if self.stopping || self.sockets.available() == 0 {
            #[cfg(feature = "direct-descriptors")]
            if let (Some(table), Some(index)) = (&mut self.fixed, _direct) {
                let _ = table.remove(&self.ring, index);
            }
            return Err(ImportError {
                error: resource_error(),
                socket: fd,
            });
        }
        #[cfg(feature = "fixed-files")]
        let fixed = if let Some(index) = _direct {
            Some(index)
        } else if let Some(table) = &mut self.fixed {
            match table.insert(&self.ring, fd.as_raw_fd()) {
                Ok(index) => Some(index),
                Err(error) => return Err(ImportError { error, socket: fd }),
            }
        } else {
            None
        };
        let metadata = match net::info(&fd, SocketId(0), kind) {
            Ok(info) => info,
            Err(error) => {
                #[cfg(feature = "fixed-files")]
                if let (Some(table), Some(index)) = (&mut self.fixed, fixed) {
                    let _ = table.remove(&self.ring, index);
                }
                return Err(ImportError { error, socket: fd });
            }
        };
        let entry = SocketEntry {
            fd: Some(fd),
            #[cfg(feature = "fixed-files")]
            fixed,
            kind,
            options: options.clone(),
            active: 0,
            native_pending: 0,
            closing: false,
            aborting: false,
            receive: None,
            accept: None,
            sending: None,
            receive_credits: 0,
            accept_credits: 0,
        };
        let id = match self.sockets.insert(entry) {
            Ok(id) => SocketId(id),
            Err(entry) => {
                #[cfg(feature = "fixed-files")]
                if let (Some(table), Some(index)) = (&mut self.fixed, entry.fixed) {
                    let _ = table.remove(&self.ring, index);
                }
                return Err(ImportError {
                    error: resource_error(),
                    socket: entry.fd.unwrap(),
                });
            }
        };
        Ok(SocketInfo { id, ..metadata })
    }
    fn enqueue_completion(&mut self, cqe: Cqe) -> io::Result<()> {
        #[cfg(feature = "uring-msg-ring")]
        if cqe.user_data == MESSAGE_WAKE {
            self.notifier.drain();
            return Ok(());
        }
        if cqe.user_data == WAKE {
            if cqe.user_data == WAKE && cqe.flags & IORING_CQE_F_MORE == 0 {
                self.wake_armed = false;
            }
            self.notifier.drain();
            return Ok(());
        }
        if cqe.user_data == CANCEL {
            return Ok(());
        }
        #[cfg(feature = "direct-descriptors")]
        if cqe.user_data == CONTROL {
            if let Some(opcode) = self.control_inflight.take()
                && cqe.res >= 0
            {
                if opcode == IORING_OP_FIXED_FD_INSTALL {
                    drop(unsafe { OwnedFd::from_raw_fd(cqe.res) });
                } else if opcode == IORING_OP_SOCKET {
                    self.fixed
                        .as_mut()
                        .unwrap()
                        .remove(&self.ring, cqe.res as u32)?;
                }
            }
            return Ok(());
        }
        #[cfg(all(feature = "zc-rx", feature = "zc-observe"))]
        if let Some(zcrx) = &self.zcrx
            && zcrx.handle_event(&self.ring, &cqe)?
        {
            return Ok(());
        }
        if let Some(_op) = self.op(cqe.user_data) {
            let expansion = 1;
            #[cfg(feature = "buffer-bundles")]
            let expansion = {
                let mut expansion = if _op.kind == Kind::Receive
                    && _op.provided
                    && cqe.flags & IORING_CQE_F_BUFFER != 0
                    && self.report.enabled(Optimization::BufferBundles)
                    && self
                        .sockets
                        .get(_op.socket.0)
                        .is_some_and(|socket| socket.kind == SocketKind::TcpStream)
                {
                    self.provided.as_ref().unwrap().entries()
                } else {
                    expansion
                };
                if expansion > 1 && self.deferred_limit - self.deferred.len() < expansion {
                    let buffers = self.provided.as_ref().unwrap();
                    let mut bid = (cqe.flags >> IORING_CQE_BUFFER_SHIFT) as u16;
                    let mut remaining = cqe.res.max(0) as usize;
                    expansion = 1;
                    loop {
                        let bytes = buffers.remaining(bid)?;
                        if remaining <= bytes {
                            break;
                        }
                        remaining -= bytes;
                        bid = buffers.next_bid(bid);
                        expansion += 1;
                        if expansion > buffers.entries() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "receive bundle exceeds its registered group",
                            ));
                        }
                    }
                }
                expansion
            };
            if self.deferred_limit - self.deferred.len() < expansion {
                // The popped CQE still owns its complete native ranges. Keep it
                // intact; do not advance incremental offsets or lose a token.
                self.blocked_ingest = Some(cqe);
                return Err(resource_error());
            }
        }
        self.complete_native(&cqe);
        let Some(op) = self.op_mut(cqe.user_data) else {
            return Ok(());
        };
        if cqe.flags & IORING_CQE_F_MORE == 0 && cqe.flags & IORING_CQE_F_NOTIF == 0 {
            op.submitted = false;
            op.cancel_sent = false;
        }
        #[cfg(feature = "provided-buffers")]
        if op.kind == Kind::Receive && op.provided && cqe.flags & IORING_CQE_F_BUFFER != 0 {
            let socket = op.socket;
            let multishot = op.multishot;
            let socket = self.sockets.get(socket.0).unwrap();
            let udp = socket.kind == SocketKind::Udp;
            let bundled = cfg!(feature = "buffer-bundles")
                && !udp
                && self.report.enabled(Optimization::BufferBundles);
            let mut remaining = cqe.res.max(0) as usize;
            if udp && !multishot {
                remaining = remaining.min(socket.options.receive_chunk);
            }
            let mut bid = (cqe.flags >> IORING_CQE_BUFFER_SHIFT) as u16;
            loop {
                let buffers = self.provided.as_mut().unwrap();
                let available = buffers.remaining(bid)?;
                if !bundled && remaining > available && !(udp && !multishot) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "receive completion exceeds its provided buffer",
                    ));
                }
                let bytes = remaining.min(available);
                let last = !bundled || remaining <= available;
                let next = bid;
                #[cfg(feature = "buffer-bundles")]
                let next = buffers.next_bid(next);
                // Reserve in native CQ order, before application-credit stalls
                // can reorder delivery across sockets sharing an incremental BID.
                let range =
                    buffers.reserve(bid, bytes, last && cqe.flags & IORING_CQE_F_BUF_MORE != 0)?;
                self.retain_completion(cqe, Some(range))?;
                if last {
                    break;
                }
                remaining -= bytes;
                bid = next;
            }
            return Ok(());
        }
        self.retain_completion(
            cqe,
            #[cfg(feature = "provided-buffers")]
            None,
        )?;
        Ok(())
    }
    fn retain_completion(
        &mut self,
        cqe: Cqe,
        #[cfg(feature = "provided-buffers")] provided: Option<ProvidedRange>,
    ) -> io::Result<()> {
        if self.deferred.len() == self.deferred_limit {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "bounded completion retention exhausted",
            ));
        }
        let op = self.op_mut(cqe.user_data).unwrap();
        op.pending += 1;
        let sequence = op.issued_sequence;
        op.issued_sequence = op.issued_sequence.wrapping_add(1);
        self.deferred.push_back(Pending {
            cqe,
            sequence,
            #[cfg(feature = "provided-buffers")]
            provided,
        });
        Ok(())
    }
    #[cfg(feature = "direct-descriptors")]
    /// Only cold, non-network-waiting operations use synchronous SQEs. Existing
    /// network completions retain their original order and kernel ownership.
    fn control(&mut self, mut sqe: Sqe) -> io::Result<i32> {
        if self.control_inflight.is_some() || self.blocked_ingest.is_some() {
            return Err(resource_error());
        }
        sqe.user_data = CONTROL;
        while self.ring.available() == 0 {
            self.ring.submit()?;
            while let Some(cqe) = self.ring.pop() {
                self.enqueue_completion(cqe)?;
            }
        }
        self.ring.push(sqe)?;
        self.control_inflight = Some(sqe.opcode);
        loop {
            self.ring.wait(None)?;
            while let Some(cqe) = self.ring.pop() {
                if cqe.user_data == CONTROL {
                    self.control_inflight = None;
                    self.ring.flush_completions();
                    return if cqe.res < 0 {
                        Err(io::Error::from_raw_os_error(-cqe.res))
                    } else {
                        Ok(cqe.res)
                    };
                }
                self.enqueue_completion(cqe)?;
            }
        }
    }
    fn create(
        &mut self,
        addr: SocketAddr,
        kind: SocketKind,
        options: &SocketOptions,
    ) -> io::Result<(OwnedSocket, Option<u32>)> {
        if self.stopping {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        options.validate()?;
        net::validate_address(addr, false)?;
        #[cfg(feature = "direct-descriptors")]
        if self.report.enabled(Optimization::DirectDescriptors) {
            let udp = kind == SocketKind::Udp;
            let index = self.control(Sqe {
                opcode: IORING_OP_SOCKET,
                fd: if addr.is_ipv4() {
                    libc::AF_INET
                } else {
                    libc::AF_INET6
                },
                off: (if udp {
                    libc::SOCK_DGRAM
                } else {
                    libc::SOCK_STREAM
                } | libc::SOCK_NONBLOCK) as u64,
                len: if udp {
                    libc::IPPROTO_UDP
                } else {
                    libc::IPPROTO_TCP
                } as u32,
                file_index: IORING_FILE_INDEX_ALLOC,
                ..Sqe::default()
            })? as u32;
            let result = (|| {
                let raw = self.control(Sqe {
                    opcode: IORING_OP_FIXED_FD_INSTALL,
                    flags: IOSQE_FIXED_FILE,
                    fd: index as i32,
                    ..Sqe::default()
                })?;
                let fd = unsafe { OwnedFd::from_raw_fd(raw) };
                net::configure(
                    &fd,
                    kind,
                    options,
                    self.report.enabled(Optimization::UdpGro),
                )?;
                if let Some(hook) = &options.hook {
                    hook.configure(fd.as_fd())?;
                }
                if !udp {
                    crate::socket::reject_blocking_linger(&socket2::SockRef::from(&fd))?;
                }
                Ok((fd, Some(index)))
            })();
            if result.is_err() {
                self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
            }
            return result;
        }
        net::create(
            addr,
            kind,
            options,
            self.report.enabled(Optimization::UdpGro),
        )
        .map(|fd| (fd, None))
    }
    pub fn listen(&mut self, addr: SocketAddr, options: &SocketOptions) -> io::Result<SocketInfo> {
        let (fd, direct) = self.create(addr, SocketKind::TcpListener, options)?;
        let result = (|| {
            let socket = socket2::SockRef::from(&fd);
            socket.bind(&addr.into())?;
            socket.listen(options.backlog)
        })();
        if let Err(error) = result {
            #[cfg(feature = "direct-descriptors")]
            if let Some(index) = direct {
                self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
            }
            return Err(error);
        }
        self.add_socket(fd, SocketKind::TcpListener, options, direct)
            .map_err(|error| error.error)
    }
    pub fn bind_udp(
        &mut self,
        addr: SocketAddr,
        peer: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> io::Result<SocketInfo> {
        if let Some(peer) = peer {
            net::validate_address(peer, true)?;
        }
        let (fd, direct) = self.create(addr, SocketKind::Udp, options)?;
        let result = (|| {
            let socket = socket2::SockRef::from(&fd);
            socket.bind(&addr.into())?;
            if let Some(peer) = peer {
                socket.connect(&peer.into())?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            #[cfg(feature = "direct-descriptors")]
            if let Some(index) = direct {
                self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
            }
            return Err(error);
        }
        self.add_socket(fd, SocketKind::Udp, options, direct)
            .map_err(|error| error.error)
    }
    pub fn connect(
        &mut self,
        token: Token,
        addr: SocketAddr,
        local: Option<SocketAddr>,
        options: &SocketOptions,
    ) -> io::Result<()> {
        net::validate_address(addr, true)?;
        let (fd, direct) = self.create(addr, SocketKind::TcpStream, options)?;
        if let Some(local) = local
            && let Err(error) = socket2::SockRef::from(&fd).bind(&local.into())
        {
            #[cfg(feature = "direct-descriptors")]
            if let Some(index) = direct {
                self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
            }
            return Err(error);
        }
        let info = self
            .add_socket(fd, SocketKind::TcpStream, options, direct)
            .map_err(|error| error.error)?;
        let key = match self.allocate(token, info.id, Kind::Connect) {
            Ok(key) => key,
            Err(error) => {
                self.close(info.id)?;
                return Err(error);
            }
        };
        let native = unsafe { &mut *self.native_message(key as u32 as usize) };
        let op = self.op_mut(key).unwrap();
        (native.address, op.address_len) = net::encode(addr);
        self.schedule(key);
        Ok(())
    }
    pub fn import(
        &mut self,
        socket: OwnedSocket,
        kind: SocketKind,
        options: &SocketOptions,
    ) -> Result<SocketInfo, ImportError> {
        if self.stopping {
            return Err(ImportError {
                error: io::Error::from(io::ErrorKind::BrokenPipe),
                socket,
            });
        }
        if self.sockets.available() == 0 {
            return Err(ImportError {
                error: resource_error(),
                socket,
            });
        }
        let result = (|| {
            net::validate_import(&socket, kind)?;
            net::configure(
                &socket,
                kind,
                options,
                self.report.enabled(Optimization::UdpGro),
            )?;
            if let Some(hook) = &options.hook {
                hook.configure(socket.as_fd())?;
            }
            let sock = socket2::SockRef::from(&socket);
            if kind != SocketKind::Udp {
                crate::socket::reject_blocking_linger(&sock)?;
            }
            sock.set_nonblocking(true)
        })();
        if let Err(error) = result {
            return Err(ImportError { error, socket });
        }
        self.add_socket(socket, kind, options, None)
    }
    pub fn take_idle_socket(&mut self, socket: SocketId) -> io::Result<OwnedSocket> {
        let entry = self.check_socket(socket, None)?;
        if entry.active != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "socket has active or retained I/O",
            ));
        }
        #[cfg(feature = "fixed-files")]
        if let Some(index) = entry.fixed {
            self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
        }
        Ok(self.sockets.remove(socket.0).unwrap().fd.unwrap())
    }
    pub fn start_accept(&mut self, socket: SocketId, token: Token) -> io::Result<()> {
        if self
            .check_socket(socket, Some(SocketKind::TcpListener))?
            .accept
            .is_some()
        {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        let key = self.allocate(token, socket, Kind::Accept)?;
        self.operations[key as u32 as usize].multishot = cfg!(feature = "multishot-accept")
            && self.report.enabled(Optimization::MultishotAccept);
        self.sockets.get_mut(socket.0).unwrap().accept = Some(key);
        self.schedule(key);
        Ok(())
    }
    pub fn start_recv(&mut self, socket: SocketId, token: Token) -> io::Result<()> {
        let entry = self.check_socket(socket, None)?;
        if entry.kind == SocketKind::TcpListener {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot receive on a listener",
            ));
        }
        if entry.receive.is_some() {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        #[cfg(feature = "zc-rx")]
        let udp = entry.kind == SocketKind::Udp;
        let key = self.allocate(token, socket, Kind::Receive)?;
        let op = &mut self.operations[key as u32 as usize];
        #[cfg(feature = "zc-rx")]
        {
            op.use_zc = !udp && self.zcrx.is_some();
        }
        #[cfg(feature = "provided-buffers")]
        {
            op.provided = !op.use_zc && self.provided.is_some();
        }
        op.multishot = op.use_zc
            || (cfg!(feature = "multishot-recv")
                && op.provided
                && self.report.enabled(Optimization::MultishotRecv));
        self.sockets.get_mut(socket.0).unwrap().receive = Some(key);
        self.schedule(key);
        Ok(())
    }
    pub fn receive_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()> {
        let entry = self
            .sockets
            .get_mut(socket.0)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
        entry.receive_credits = slots.min(self.config.limits.max_pending_receives);
        if let Some(key) = entry.receive {
            self.schedule(key);
        }
        Ok(())
    }
    pub fn accept_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()> {
        let entry = self
            .sockets
            .get_mut(socket.0)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
        entry.accept_credits = slots.min(self.config.limits.max_pending_accepts);
        if let Some(key) = entry.accept {
            self.schedule(key);
        }
        Ok(())
    }
}

impl Driver {
    pub fn send(
        &mut self,
        socket: SocketId,
        token: Token,
        data: SendPayload,
        destination: Option<SocketAddr>,
        segment_size: Option<u16>,
    ) -> Result<(), SendOutcome> {
        let validation = (|| {
            let entry = self.check_socket(socket, None)?;
            if entry.kind == SocketKind::TcpListener {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot send on a listener",
                ));
            }
            if entry.kind == SocketKind::TcpStream && entry.sending.is_some() {
                return Err(resource_error());
            }
            if data.segments().len() > self.config.limits.max_iovecs
                || data.len() > i32::MAX as usize
                || data
                    .segments()
                    .iter()
                    .any(|segment| segment.len() > u32::MAX as usize)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "send exceeds configured vector or kernel byte limit",
                ));
            }
            if self
                .send_bytes
                .checked_add(data.len())
                .is_none_or(|bytes| bytes > self.config.limits.max_send_bytes)
            {
                return Err(resource_error());
            }
            if let Some(addr) = destination {
                net::validate_address(addr, true)?;
                if entry.kind != SocketKind::Udp {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "TCP sends cannot override peer address",
                    ));
                }
            }
            if let Some(size) = segment_size {
                if size == 0 || entry.kind != SocketKind::Udp {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "GSO requires UDP and a nonzero segment size",
                    ));
                }
                if !self.report.enabled(Optimization::UdpGso) {
                    return Err(unsupported("UDP GSO was not enabled"));
                }
            }
            Ok(entry.kind == SocketKind::Udp)
        })();
        let udp = match validation {
            Ok(udp) => udp,
            Err(error) => {
                return Err(SendOutcome {
                    result: Err(error),
                    data,
                });
            }
        };
        let key = match self.allocate(token, socket, Kind::Send) {
            Ok(key) => key,
            Err(error) => {
                return Err(SendOutcome {
                    result: Err(error),
                    data,
                });
            }
        };
        let index = key as u32 as usize;
        self.send_bytes += data.len();
        self.operations[index].requested = data.len();
        self.operations[index].payload = Some(data);
        let use_zc = cfg!(feature = "zc-tx")
            && self.report.enabled(Optimization::ZcTx)
            && self.operations[index].requested != 0
            && self.operations[index].requested >= self.config.linux.zc_send_threshold
            && (self.operations[index]
                .payload
                .as_ref()
                .unwrap()
                .segments()
                .len()
                <= 1
                || (cfg!(feature = "zc-tx-vectored")
                    && self.report.enabled(Optimization::ZcTxVectored)));
        self.operations[index].use_zc = use_zc;
        #[cfg(feature = "buffer-bundles")]
        if !use_zc
            && !udp
            && self.report.enabled(Optimization::BufferBundles)
            && self.operations[index].requested != 0
        {
            if let Some(bundle) = self.free_bundles.pop() {
                self.operations[index].bundle = Some(bundle);
            } else {
                let data = self.operations[index].payload.take().unwrap();
                self.send_bytes -= self.operations[index].requested;
                self.operations[index].requested = 0;
                let _ = self.retire(key);
                return Err(SendOutcome {
                    result: Err(resource_error()),
                    data,
                });
            }
        }
        let native = unsafe { &mut *self.native_message(index) };
        let op = &mut self.operations[index];
        if let Some(addr) = destination {
            (native.address, op.address_len) = net::encode(addr);
            native.message.msg_namelen = op.address_len;
        }
        #[cfg(feature = "udp-gso")]
        if let Some(segment) = segment_size {
            native.message.msg_controllen = net::put_gso(&mut native.control, segment);
        }
        for segment in op.payload.as_ref().unwrap().segments() {
            op.iovecs.push(libc::iovec {
                iov_base: segment.as_ptr() as *mut libc::c_void,
                iov_len: segment.len(),
            });
        }
        if !udp {
            self.sockets.get_mut(socket.0).unwrap().sending = Some(key);
        }
        self.schedule(key);
        Ok(())
    }
    #[cfg(not(feature = "tcp-splice"))]
    pub fn splice(
        &mut self,
        _token: Token,
        _source: SocketId,
        _destination: SocketId,
        _bytes: usize,
    ) -> io::Result<()> {
        Err(unsupported("TCP splice was not compiled"))
    }
    #[cfg(feature = "tcp-splice")]
    pub fn splice(
        &mut self,
        token: Token,
        source: SocketId,
        destination: SocketId,
        bytes: usize,
    ) -> io::Result<()> {
        if !self.report.enabled(Optimization::TcpSplice) {
            return Err(unsupported("TCP splice was not enabled"));
        }
        if source == destination {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "splice requires distinct sockets",
            ));
        }
        let source_entry = self.check_socket(source, Some(SocketKind::TcpStream))?;
        if source_entry.receive.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "source already has a receive operation",
            ));
        }
        if self
            .check_socket(destination, Some(SocketKind::TcpStream))?
            .sending
            .is_some()
        {
            return Err(resource_error());
        }
        let pipe = make_pipe(self.config.linux.splice_pipe_bytes)?;
        let key = self.allocate(token, source, Kind::Splice)?;
        let op = self.op_mut(key).unwrap();
        op.destination = Some(destination);
        op.requested = bytes;
        op.pipe = Some(pipe);
        self.sockets.get_mut(source.0).unwrap().receive = Some(key);
        let destination = self.sockets.get_mut(destination.0).unwrap();
        destination.active += 1;
        destination.sending = Some(key);
        self.schedule(key);
        Ok(())
    }
    pub fn shutdown(&mut self, socket: SocketId, how: Shutdown) -> io::Result<()> {
        let fd = self
            .check_socket(socket, None)?
            .fd
            .as_ref()
            .unwrap()
            .as_raw_fd();
        let how = match how {
            Shutdown::Read => libc::SHUT_RD,
            Shutdown::Write => libc::SHUT_WR,
            Shutdown::Both => libc::SHUT_RDWR,
        };
        if unsafe { libc::shutdown(fd, how) } < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    pub fn abort(&mut self, socket: SocketId) -> io::Result<()> {
        let entry = self.check_socket(socket, Some(SocketKind::TcpStream))?;
        // Direct sockets retain the native fd installed at creation/accept.
        // It refers to the same socket as the fixed table entry, so this
        // changes the real socket rather than merely closing one descriptor.
        let fd = entry
            .fd
            .as_ref()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
        net::abort_on_close(fd.as_raw_fd())?;
        self.sockets.get_mut(socket.0).unwrap().aborting = true;
        self.aborting_sockets += 1;
        self.close(socket)
    }
    fn close_if_idle(&mut self, socket: SocketId) -> io::Result<()> {
        let Some(entry) = self.sockets.get(socket.0) else {
            return Ok(());
        };
        if !entry.closing || entry.active != 0 {
            return Ok(());
        }
        #[cfg(feature = "fixed-files")]
        if let Some(index) = entry.fixed {
            self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
        }
        if entry.aborting {
            self.aborting_sockets -= 1;
        }
        self.sockets.remove(socket.0);
        Ok(())
    }
    pub fn close(&mut self, socket: SocketId) -> io::Result<()> {
        let entry = self
            .sockets
            .get_mut(socket.0)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
        entry.closing = true;
        let aborting = entry.aborting;
        let half_close = if entry.kind == SocketKind::TcpStream && !aborting {
            entry
                .fd
                .as_ref()
                .map_or(Ok(()), |fd| net::half_close(fd.as_raw_fd()))
        } else {
            Ok(())
        };
        self.scratch.clear();
        for (index, op) in self.operations.iter().enumerate() {
            let affects = op.socket == socket;
            #[cfg(feature = "tcp-splice")]
            let affects = affects || op.destination == Some(socket);
            if op.kind != Kind::Idle && (op.kind != Kind::Send || aborting) && affects {
                self.scratch.push(op.key(index));
            }
        }
        while let Some(key) = self.scratch.pop() {
            self.op_mut(key).unwrap().stopping = true;
            self.schedule(key);
        }
        self.close_if_idle(socket)?;
        self.release_aborted_sockets()?;
        half_close
    }
    pub fn cancel(&mut self, token: Token) -> io::Result<()> {
        let Some(&key) = self.tokens.get(&token) else {
            return Ok(());
        };
        self.op_mut(key).unwrap().stopping = true;
        self.schedule(key);
        Ok(())
    }
    pub fn begin_shutdown(&mut self) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        #[cfg(feature = "uring-msg-ring")]
        self.notifier.detach();
        for (_, socket) in self.sockets.iter_mut() {
            socket.closing = true;
            if socket.kind == SocketKind::TcpStream
                && let Some(fd) = &socket.fd
                && let Err(error) = net::abort_on_close(fd.as_raw_fd())
                && self.shutdown_error.is_none()
            {
                self.shutdown_error = Some(error);
            }
        }
        self.scratch.clear();
        for (index, op) in self.operations.iter_mut().enumerate() {
            if op.kind != Kind::Idle {
                op.stopping = true;
                self.scratch.push(op.key(index));
            }
        }
        while let Some(key) = self.scratch.pop() {
            self.schedule(key);
        }
        if let Err(error) = self.release_aborted_sockets()
            && self.shutdown_error.is_none()
        {
            self.shutdown_error = Some(error);
        }
    }
    pub fn is_idle(&self) -> bool {
        #[cfg(feature = "direct-descriptors")]
        if self.control_inflight.is_some() {
            return false;
        }
        self.tokens.is_empty() && self.deferred.is_empty() && self.blocked_ingest.is_none()
    }
    fn arm_wake(&mut self) -> io::Result<()> {
        if self.wake_armed || self.stopping {
            return Ok(());
        }
        if self.ring.available() == 0 {
            self.sq_deferred = true;
            return Ok(());
        }
        self.ring.push(Sqe {
            opcode: IORING_OP_POLL_ADD,
            fd: self.notifier.event.as_raw_fd(),
            op_flags: libc::POLLIN as u32,
            len: IORING_POLL_ADD_MULTI,
            user_data: WAKE,
            ..Sqe::default()
        })?;
        self.wake_armed = true;
        Ok(())
    }
    fn cancel_native(&mut self, key: u64) -> io::Result<()> {
        let Some(op) = self.op(key) else {
            return Ok(());
        };
        if !op.submitted || op.cancel_sent {
            return Ok(());
        }
        if self.ring.available() == 0 {
            self.sq_deferred = true;
            self.schedule(key);
            return Ok(());
        }
        self.ring.push(Sqe {
            opcode: IORING_OP_ASYNC_CANCEL,
            fd: -1,
            addr: key,
            user_data: CANCEL,
            ..Sqe::default()
        })?;
        self.op_mut(key).unwrap().cancel_sent = true;
        Ok(())
    }
    fn prepare(&mut self, key: u64) -> io::Result<Sqe> {
        let index = key as u32 as usize;
        let (fd, flags) = self.native(self.operations[index].socket)?;
        let kind = self.operations[index].kind;
        let entry = self.sockets.get(self.operations[index].socket.0).unwrap();
        let udp = entry.kind == SocketKind::Udp;
        let receive_chunk = entry.options.receive_chunk;
        let mut sqe = Sqe {
            fd,
            flags,
            user_data: key,
            ..Sqe::default()
        };
        let native_ptr = self.native_message(index);
        // drive_ready calls prepare only after native completion and deferred
        // delivery. Finish every native borrow before publishing the SQE.
        let native = unsafe { &mut *native_ptr };
        let op = &mut self.operations[index];
        debug_assert!(!op.submitted && !op.native_pending && op.pending == 0);
        match kind {
            Kind::Connect => {
                sqe.opcode = IORING_OP_CONNECT;
                sqe.addr = native.address.as_ptr() as u64;
                sqe.off = op.address_len as u64;
            }
            Kind::Accept => {
                sqe.opcode = IORING_OP_ACCEPT;
                sqe.op_flags = (libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC) as u32;
                #[cfg(feature = "direct-descriptors")]
                if self.report.enabled(Optimization::DirectDescriptors) {
                    sqe.file_index = IORING_FILE_INDEX_ALLOC;
                    sqe.op_flags &= !(libc::SOCK_CLOEXEC as u32);
                }
                #[cfg(feature = "multishot-accept")]
                if op.multishot {
                    sqe.ioprio = IORING_ACCEPT_MULTISHOT;
                }
            }
            Kind::Receive => {
                #[cfg(feature = "zc-rx")]
                if op.use_zc {
                    self.zcrx.as_ref().unwrap().prepare_recv(&mut sqe)?;
                    return Ok(sqe);
                }
                sqe.opcode = if udp {
                    IORING_OP_RECVMSG
                } else {
                    IORING_OP_RECV
                };
                match () {
                    #[cfg(feature = "provided-buffers")]
                    _ if op.provided => {
                        sqe.flags |= IOSQE_BUFFER_SELECT;
                        sqe.buf_index = self.provided.as_ref().unwrap().group;
                        #[cfg(feature = "multishot-recv")]
                        if op.multishot {
                            sqe.ioprio |= IORING_RECV_MULTISHOT;
                        }
                        #[cfg(feature = "buffer-bundles")]
                        if !udp && self.report.enabled(Optimization::BufferBundles) {
                            sqe.ioprio |= IORING_RECVSEND_BUNDLE;
                        }
                    }
                    _ => {
                        if op.receive.is_none() {
                            op.receive = Some(self.pool.try_acquire_at_least(receive_chunk)?);
                        }
                        let buffer = op.receive.as_mut().unwrap();
                        sqe.addr = buffer.as_mut_ptr() as u64;
                        sqe.len =
                            buffer.capacity().min(receive_chunk).min(i32::MAX as usize) as u32;
                        #[cfg(feature = "registered-buffers")]
                        if !udp
                            && let Some(index) = self
                                .registered
                                .as_ref()
                                .and_then(|r| r.index(buffer.as_ptr(), sqe.len as usize))
                        {
                            sqe.ioprio |= IORING_RECVSEND_FIXED_BUF;
                            sqe.buf_index = index;
                        }
                    }
                }
                if udp {
                    op.address_len = net::ADDRESS_BYTES as i32;
                    native.message = UserMsgHdr::default();
                    native.message.msg_name = native.address.as_mut_ptr().cast();
                    native.message.msg_namelen = op.address_len;
                    native.message.msg_control = native.control.as_mut_ptr().cast();
                    native.message.msg_controllen = net::CONTROL_BYTES;
                    op.iovecs.clear();
                    let capacity =
                        if cfg!(feature = "multishot-recv") && op.provided && op.multishot {
                            receive_chunk.saturating_add(
                                size_of::<RecvMsgOut>() + net::ADDRESS_BYTES + net::CONTROL_BYTES,
                            )
                        } else if op.provided {
                            receive_chunk
                        } else {
                            sqe.len as usize
                        };
                    op.iovecs.push(libc::iovec {
                        iov_base: sqe.addr as *mut libc::c_void,
                        iov_len: capacity,
                    });
                    native.message.msg_iov = op.iovecs.as_mut_ptr();
                    native.message.msg_iovlen = 1;
                    sqe.addr = std::ptr::from_mut(&mut native.message) as u64;
                    sqe.len = 1;
                    sqe.op_flags = libc::MSG_TRUNC as u32;
                } else if cfg!(feature = "provided-buffers") && op.provided {
                    // 7.2 multishot cap limits one shot without mistaking a
                    // logical restart for EOF. Bundles may span provided blocks.
                    sqe.len = if cfg!(feature = "buffer-bundles")
                        && self.report.enabled(Optimization::BufferBundles)
                    {
                        0
                    } else {
                        receive_chunk.min(i32::MAX as usize) as u32
                    };
                }
            }
            Kind::Send => {
                sqe.op_flags = libc::MSG_NOSIGNAL as u32;
                let data = op.payload.as_ref().unwrap();
                native.message.msg_name = if native.message.msg_namelen == 0 {
                    std::ptr::null_mut()
                } else {
                    native.address.as_mut_ptr().cast()
                };
                native.message.msg_control = if native.message.msg_controllen == 0 {
                    std::ptr::null_mut()
                } else {
                    native.control.as_mut_ptr().cast()
                };
                native.message.msg_iov = op.iovecs.as_mut_ptr();
                native.message.msg_iovlen = op.iovecs.len();
                #[cfg(feature = "zc-tx")]
                if op.use_zc {
                    #[cfg(feature = "zc-tx-fixed")]
                    let regions = self
                        .registered
                        .as_ref()
                        .map_or(&[][..], |r| r.regions.as_slice());
                    #[cfg(feature = "zc-tx-fixed")]
                    let fixed = self.report.enabled(Optimization::ZcTxFixed)
                        && self.registered.as_ref().is_some_and(|registered| {
                            let mut segments =
                                data.segments().iter().filter(|segment| !segment.is_empty());
                            let Some(first) = segments.next().and_then(|segment| {
                                registered.index(segment.as_ptr(), segment.len())
                            }) else {
                                return false;
                            };
                            segments.all(|segment| {
                                registered.index(segment.as_ptr(), segment.len()) == Some(first)
                            })
                        });
                    let options = zc::TxOptions {
                        #[cfg(feature = "zc-tx-fixed")]
                        fixed,
                        #[cfg(feature = "zc-tx-vectored")]
                        vectored: self.report.enabled(Optimization::ZcTxVectored),
                        #[cfg(feature = "zc-observe")]
                        observe: self.report.enabled(Optimization::ZcObserve),
                    };
                    let message = if udp
                        || native.message.msg_controllen != 0
                        || native.message.msg_namelen != 0
                    {
                        Some(&mut native.message)
                    } else {
                        None
                    };
                    op.zc.prepare(
                        &mut sqe,
                        data,
                        #[cfg(feature = "zc-tx-fixed")]
                        regions,
                        options,
                        message,
                    )?;
                    return Ok(sqe);
                }
                #[cfg(feature = "buffer-bundles")]
                if let Some(bundle) = op.bundle {
                    self.bundles[bundle].publish(data.segments());
                    sqe.opcode = IORING_OP_SEND;
                    sqe.flags |= IOSQE_BUFFER_SELECT;
                    sqe.ioprio = IORING_RECVSEND_BUNDLE;
                    sqe.buf_index = self.bundles[bundle].group;
                    return Ok(sqe);
                }
                if cfg!(feature = "udp-gso") && native.message.msg_controllen != 0 {
                    sqe.opcode = IORING_OP_SENDMSG;
                    sqe.addr = std::ptr::from_mut(&mut native.message) as u64;
                    sqe.len = 1;
                } else {
                    sqe.opcode = IORING_OP_SEND;
                    if native.message.msg_namelen != 0 {
                        sqe.off = native.address.as_ptr() as u64;
                        sqe.file_index = op.address_len as u32;
                    }
                    if data.is_empty() {
                        // Empty TCP sends and UDP datagrams use scalar SEND,
                        // without fixed buffers or zero-length vector entries.
                        sqe.addr = 0;
                        sqe.len = 0;
                    } else if data.segments().len() == 1 {
                        let segment = &data.segments()[0];
                        sqe.addr = segment.as_ptr() as u64;
                        sqe.len = segment.len() as u32;
                        #[cfg(feature = "registered-buffers")]
                        if let Some(index) = self
                            .registered
                            .as_ref()
                            .and_then(|r| r.index(segment.as_ptr(), segment.len()))
                        {
                            sqe.ioprio |= IORING_RECVSEND_FIXED_BUF;
                            sqe.buf_index = index;
                        }
                    } else {
                        sqe.ioprio = IORING_SEND_VECTORIZED;
                        sqe.addr = op.iovecs.as_ptr() as u64;
                        sqe.len = op.iovecs.len() as u32;
                    }
                }
            }
            #[cfg(feature = "tcp-splice")]
            Kind::Splice => {
                if op.splice_wait {
                    sqe.opcode = IORING_OP_POLL_ADD;
                    sqe.op_flags = libc::POLLIN as u32;
                    if op.splice_stage == SpliceStage::Write {
                        let destination = self.sockets.get(op.destination.unwrap().0).unwrap();
                        (sqe.fd, sqe.flags) = destination.native()?;
                        sqe.op_flags = libc::POLLOUT as u32;
                    }
                    return Ok(sqe);
                }
                sqe.opcode = IORING_OP_SPLICE;
                sqe.off = u64::MAX;
                sqe.addr = u64::MAX;
                sqe.op_flags = libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK;
                let pipe = op.pipe.as_ref().unwrap();
                if op.splice_stage == SpliceStage::Read {
                    sqe.fd = pipe.1.as_raw_fd();
                    sqe.flags = 0;
                    sqe.file_index = fd as u32;
                    #[cfg(feature = "fixed-files")]
                    if flags & IOSQE_FIXED_FILE != 0 {
                        sqe.op_flags |= SPLICE_F_FD_IN_FIXED;
                    }
                    sqe.len = (op.requested - op.sent)
                        .min(self.config.linux.splice_pipe_bytes)
                        .min(i32::MAX as usize) as u32;
                } else {
                    let destination = self.sockets.get(op.destination.unwrap().0).unwrap();
                    (sqe.fd, sqe.flags) = destination.native()?;
                    sqe.file_index = pipe.0.as_raw_fd() as u32;
                    sqe.len = (op.pipe_bytes - op.pipe_sent) as u32;
                }
            }
            Kind::Idle => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "inactive operation",
                ));
            }
        }
        Ok(sqe)
    }
}

impl Driver {
    fn drive_ready(&mut self, events: &mut Vec<Event>, limit: usize) -> io::Result<()> {
        let count = self.ready.len();
        for _ in 0..count {
            let Some(key) = self.ready.pop_front() else {
                break;
            };
            let Some(op) = self.op_mut(key) else {
                continue;
            };
            op.queued = false;
            let kind = op.kind;
            let stopping = op.stopping;
            let finished = op.finished;
            let submitted = op.submitted;
            let pending = op.pending;
            let socket = op.socket;
            let token = op.token;
            let credits = self.sockets.get(socket.0).map_or(0, |entry| match kind {
                Kind::Accept => entry.accept_credits,
                Kind::Receive => entry.receive_credits,
                _ => usize::MAX,
            });
            if submitted {
                if stopping
                    || finished
                    || ((kind == Kind::Accept || kind == Kind::Receive) && credits == 0)
                {
                    self.cancel_native(key)?;
                }
                continue;
            }
            if pending != 0 {
                continue;
            }
            if stopping || finished {
                #[cfg(feature = "zc-tx")]
                if !self.op(key).unwrap().zc.is_idle() {
                    continue;
                }
                let emits_event = matches!(kind, Kind::Accept | Kind::Receive) || !finished;
                if emits_event && events.len() == limit {
                    self.schedule(key);
                    continue;
                }
                match kind {
                    Kind::Accept | Kind::Receive => events.push(Event::Stopped {
                        token,
                        result: Ok(()),
                    }),
                    Kind::Connect if !finished => {
                        events.push(Event::Connected {
                            token,
                            result: Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                        });
                        if let Some(entry) = self.sockets.get_mut(socket.0) {
                            entry.closing = true;
                        }
                    }
                    Kind::Send if !finished => {
                        if let Some(data) = self.op_mut(key).unwrap().payload.take() {
                            events.push(Event::Sent {
                                token,
                                outcome: SendOutcome {
                                    result: Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                                    data,
                                },
                                memory_released: true,
                            });
                        }
                    }
                    #[cfg(feature = "tcp-splice")]
                    Kind::Splice if !finished => {
                        let bytes = self.op(key).unwrap().sent;
                        events.push(Event::Spliced {
                            token,
                            result: if bytes != 0 {
                                Ok(bytes)
                            } else {
                                Err(io::Error::from_raw_os_error(libc::ECANCELED))
                            },
                        });
                    }
                    _ => {}
                }
                self.retire(key)?;
                continue;
            }
            if let Some(deadline) = self.op(key).unwrap().retry_at {
                if Instant::now() < deadline {
                    self.retry_deadline = Some(
                        self.retry_deadline
                            .map_or(deadline, |old| old.min(deadline)),
                    );
                    self.schedule(key);
                    continue;
                }
                self.op_mut(key).unwrap().retry_at = None;
            }
            if credits == 0 {
                continue;
            }
            if self.ring.available() == 0 {
                self.sq_deferred = true;
                self.schedule(key);
                continue;
            }
            #[cfg(feature = "tcp-splice")]
            if kind == Kind::Splice && self.op(key).unwrap().requested == 0 {
                self.enqueue_completion(Cqe {
                    user_data: key,
                    ..Cqe::default()
                })?;
                continue;
            }
            match self.prepare(key) {
                Ok(sqe) => {
                    // We reserved capacity before prepare. Nothing between here
                    // and push can consume that owner-thread SQ reservation.
                    if let Err(error) = self.ring.push(sqe) {
                        #[cfg(feature = "zc-tx")]
                        if self.op(key).unwrap().use_zc && kind == Kind::Send {
                            unsafe {
                                self.op_mut(key).unwrap().zc.abandon_unsubmitted();
                            }
                        }
                        return Err(error);
                    }
                    self.mark_submitted(key);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.schedule(key);
                }
                Err(error) => {
                    self.enqueue_completion(Cqe {
                        user_data: key,
                        res: -error.raw_os_error().unwrap_or(libc::EIO),
                        ..Cqe::default()
                    })?;
                }
            }
        }
        Ok(())
    }
    fn accepted(&mut self, key: u64, cqe: &Cqe, events: &mut Vec<Event>) -> io::Result<bool> {
        let op = self.op(key).unwrap();
        let socket = op.socket;
        let token = op.token;
        let stopping = op.stopping;
        let direct = cfg!(feature = "direct-descriptors")
            && self.report.enabled(Optimization::DirectDescriptors);
        if cqe.res < 0 {
            let errno = -cqe.res;
            if !stopping
                && matches!(
                    errno,
                    libc::ECANCELED | libc::ENFILE | libc::EMFILE | libc::ENOBUFS | libc::ENOMEM
                )
            {
                if errno != libc::ECANCELED {
                    self.op_mut(key).unwrap().retry_at =
                        Some(Instant::now() + Duration::from_millis(1));
                }
                self.schedule(key);
                return Ok(true);
            }
            if !stopping {
                events.push(Event::Accepted {
                    token,
                    result: Err(io::Error::from_raw_os_error(errno)),
                });
                self.op_mut(key).unwrap().finished = true;
            }
            self.schedule(key);
            return Ok(true);
        }
        if stopping {
            match () {
                #[cfg(feature = "direct-descriptors")]
                _ if direct => {
                    self.fixed
                        .as_mut()
                        .unwrap()
                        .remove(&self.ring, cqe.res as u32)?;
                }
                _ => drop(unsafe { OwnedFd::from_raw_fd(cqe.res) }),
            }
            self.schedule(key);
            return Ok(true);
        }
        if self.sockets.get(socket.0).unwrap().accept_credits == 0 {
            self.schedule(key);
            return Ok(false);
        }
        let options = self.sockets.get(socket.0).unwrap().options.clone();
        let result = (|| {
            let raw = cqe.res;
            #[cfg(feature = "direct-descriptors")]
            let raw = if direct {
                self.control(Sqe {
                    opcode: IORING_OP_FIXED_FD_INSTALL,
                    fd: raw,
                    flags: IOSQE_FIXED_FILE,
                    ..Sqe::default()
                })?
            } else {
                raw
            };
            let fd = unsafe { OwnedFd::from_raw_fd(raw) };
            // Inherited transport options are explicitly normalized; the host
            // hook is not rerun on an already-established accepted connection.
            net::configure(&fd, SocketKind::TcpStream, &options, false)?;
            self.add_socket(
                fd,
                SocketKind::TcpStream,
                &options,
                direct.then_some(cqe.res as u32),
            )
            .map_err(|error| error.error)
        })();
        // add_socket owns cleanup once it is called. FIXED_FD_INSTALL/configure
        // failures before that point need to release the direct table reference.
        #[cfg(feature = "direct-descriptors")]
        if result.is_err() && direct {
            // A cleared slot returns EBADF on a second removal, with no fd reuse
            // because this synchronous owner has not allocated another slot.
            let _ = self
                .fixed
                .as_mut()
                .unwrap()
                .remove(&self.ring, cqe.res as u32);
        }
        self.sockets.get_mut(socket.0).unwrap().accept_credits -= 1;
        events.push(Event::Accepted { token, result });
        self.schedule(key);
        Ok(true)
    }
    fn received(
        &mut self,
        key: u64,
        pending: &mut Pending,
        events: &mut Vec<Event>,
    ) -> io::Result<bool> {
        let cqe = pending.cqe;
        let op = self.op(key).unwrap();
        let token = op.token;
        let socket = op.socket;
        let stopping = op.stopping;
        #[cfg(feature = "provided-buffers")]
        let provided = op.provided;
        #[cfg(feature = "provided-buffers")]
        let multishot = op.multishot;
        #[cfg(feature = "zc-rx")]
        let use_zc = op.use_zc;
        let udp = self.sockets.get(socket.0).unwrap().kind == SocketKind::Udp;
        if cqe.res < 0 {
            #[cfg(feature = "provided-buffers")]
            if let Some(range) = pending.provided {
                self.provided.as_ref().unwrap().discard(range);
            }
            let errno = -cqe.res;
            if !stopping
                && matches!(
                    errno,
                    libc::ECANCELED
                        | libc::ENOBUFS
                        | libc::ENOMEM
                        | libc::ENOSPC
                        | libc::EAGAIN
                        | libc::EINTR
                )
            {
                if errno != libc::ECANCELED {
                    self.op_mut(key).unwrap().retry_at =
                        Some(Instant::now() + Duration::from_millis(1));
                }
                self.schedule(key);
                return Ok(true);
            }
            if !stopping {
                events.push(Event::Received {
                    token,
                    result: Err(io::Error::from_raw_os_error(errno)),
                });
                self.op_mut(key).unwrap().finished = true;
            }
            self.op_mut(key).unwrap().receive = None;
            self.schedule(key);
            return Ok(true);
        }
        let eof = !udp && cqe.res == 0;
        if !stopping && !eof && self.sockets.get(socket.0).unwrap().receive_credits == 0 {
            self.schedule(key);
            return Ok(false);
        }
        let result = match () {
            #[cfg(feature = "zc-rx")]
            _ if use_zc && stopping => match self.zcrx.as_ref().unwrap().discard(&cqe) {
                Ok(()) => None,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(error),
            },
            #[cfg(feature = "zc-rx")]
            _ if use_zc => match self.zcrx.as_ref().unwrap().complete(&cqe, &self.pool) {
                Ok(Some(data)) => Some(Ok(Received {
                    data,
                    peer: None,
                    truncated: false,
                    original_len: None,
                    gro_segment_size: None,
                })),
                Ok(None) => None,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(error),
            },
            #[cfg(feature = "provided-buffers")]
            _ if pending.provided.is_some() => {
                let range = pending.provided.unwrap();
                let buffers = self.provided.as_ref().unwrap();
                if stopping || eof {
                    buffers.discard(range);
                    None
                } else {
                    let data = match buffers.lease(&self.pool, range) {
                        Ok(data) => data,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            return Ok(false);
                        }
                        Err(error) => return Err(error),
                    };
                    match () {
                        #[cfg(feature = "multishot-recv")]
                        _ if udp && multishot => Some(parse_multishot_datagram(
                            data,
                            self.sockets.get(socket.0).unwrap().options.receive_chunk,
                        )),
                        _ if udp => Some(self.completed_datagram(key, data, cqe.res as usize)),
                        _ => Some(Ok(Received {
                            data,
                            peer: None,
                            truncated: false,
                            original_len: None,
                            gro_segment_size: None,
                        })),
                    }
                }
            }
            #[cfg(feature = "provided-buffers")]
            _ if provided => {
                // TCP EOF needs no selected payload buffer, including multishot.
                // Multishot recvmsg still requires its result header for empty UDP.
                if cqe.res != 0 || (udp && multishot) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "receive data lacks a selected buffer",
                    ));
                }
                if udp && !stopping {
                    // A zero-length datagram can recycle the selected buffer before
                    // completing, so lack of F_BUFFER is not permission to drop it.
                    let empty = match self.pool.try_acquire() {
                        Ok(buffer) => buffer.freeze(),
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            return Ok(false);
                        }
                        Err(error) => return Err(error),
                    };
                    Some(self.completed_datagram(key, empty, 0))
                } else {
                    None
                }
            }
            _ => {
                let op = self.op_mut(key).unwrap();
                let mut buffer = op.receive.take().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "receive completion without its owned buffer",
                    )
                })?;
                let bytes = (cqe.res as usize)
                    .min(buffer.capacity())
                    .min(self.sockets.get(socket.0).unwrap().options.receive_chunk);
                unsafe {
                    buffer.set_initialized_len(bytes);
                }
                if stopping || eof {
                    None
                } else {
                    let data = buffer.freeze();
                    if udp {
                        Some(self.completed_datagram(key, data, cqe.res as usize))
                    } else {
                        Some(Ok(Received {
                            data,
                            peer: None,
                            truncated: false,
                            original_len: None,
                            gro_segment_size: None,
                        }))
                    }
                }
            }
        };
        if !stopping {
            if let Some(result) = result {
                if result.is_err() {
                    self.op_mut(key).unwrap().finished = true;
                }
                self.sockets.get_mut(socket.0).unwrap().receive_credits -= 1;
                events.push(Event::Received { token, result });
            } else if eof {
                events.push(Event::ReceiveEof { token });
                self.op_mut(key).unwrap().finished = true;
            }
        }
        self.schedule(key);
        Ok(true)
    }
    fn complete(&mut self, pending: &mut Pending, events: &mut Vec<Event>) -> io::Result<bool> {
        let cqe = pending.cqe;
        let key = cqe.user_data;
        let Some(op) = self.op(key) else {
            return Ok(true);
        };
        let token = op.token;
        let socket = op.socket;
        match op.kind {
            Kind::Accept => return self.accepted(key, &cqe, events),
            Kind::Receive => return self.received(key, pending, events),
            Kind::Connect => {
                let result = if self.op(key).unwrap().stopping {
                    Err(io::Error::from_raw_os_error(libc::ECANCELED))
                } else if cqe.res < 0 {
                    Err(io::Error::from_raw_os_error(-cqe.res))
                } else {
                    net::info(
                        self.sockets.get(socket.0).unwrap().fd.as_ref().unwrap(),
                        socket,
                        SocketKind::TcpStream,
                    )
                };
                if result.is_err() {
                    self.sockets.get_mut(socket.0).unwrap().closing = true;
                }
                events.push(Event::Connected { token, result });
                self.op_mut(key).unwrap().finished = true;
            }
            Kind::Send => {
                let index = key as u32 as usize;
                #[cfg(feature = "zc-tx")]
                if self.operations[index].use_zc && !self.operations[index].zc.is_idle() {
                    match self.operations[index].zc.process(
                        &cqe,
                        #[cfg(feature = "zc-observe")]
                        &mut self.stats,
                    )? {
                        zc::TxCompletion::Result {
                            result,
                            memory_released,
                        } => {
                            let data = self.operations[index].payload.take().unwrap();
                            events.push(Event::Sent {
                                token,
                                outcome: SendOutcome { result, data },
                                memory_released,
                            });
                            self.operations[index].finished = memory_released;
                            if memory_released {
                                self.operations[index].submitted = false;
                            }
                            // Stream ordering depends on the result, not on the
                            // later memory-release notification.
                            if let Some(entry) = self.sockets.get_mut(socket.0)
                                && entry.sending == Some(key)
                            {
                                entry.sending = None;
                            }
                        }
                        zc::TxCompletion::Released => {
                            events.push(Event::Released { token });
                            self.operations[index].finished = true;
                            self.operations[index].submitted = false;
                        }
                        zc::TxCompletion::Pending => {}
                    }
                    self.schedule(key);
                    return Ok(true);
                }
                {
                    let op = &mut self.operations[index];
                    if cqe.res >= 0 {
                        op.sent += cqe.res as usize;
                    } else {
                        op.send_error = Some(-cqe.res);
                    }
                    if cqe.flags & IORING_CQE_F_MORE != 0 {
                        return Ok(true);
                    }
                    #[cfg(feature = "buffer-bundles")]
                    if let Some(bundle) = op.bundle
                        && op.sent != op.requested
                    {
                        self.bundles[bundle].reset(&self.ring)?;
                    }
                    let result = match op.send_error {
                        Some(error) if op.sent == 0 => Err(io::Error::from_raw_os_error(error)),
                        _ => Ok(op.sent),
                    };
                    let data = op.payload.take().unwrap();
                    events.push(Event::Sent {
                        token,
                        outcome: SendOutcome { result, data },
                        memory_released: true,
                    });
                    op.finished = true;
                }
            }
            #[cfg(feature = "tcp-splice")]
            Kind::Splice => {
                let op = self.op_mut(key).unwrap();
                if op.splice_wait && cqe.res >= 0 {
                    op.splice_wait = false;
                } else if !op.stopping
                    && matches!(cqe.res, value if value == -libc::EAGAIN || value == -libc::EINTR)
                {
                    // SPLICE is issued by io-wq, not io_uring's socket fast-poll
                    // path. Explicit one-shot ring poll makes EAGAIN sleep while
                    // preserving the socket->pipe->socket transfer itself.
                    op.splice_wait = true;
                } else if cqe.res <= 0 {
                    let result = if op.sent != 0 || cqe.res == 0 {
                        Ok(op.sent)
                    } else {
                        Err(io::Error::from_raw_os_error(-cqe.res))
                    };
                    events.push(Event::Spliced { token, result });
                    op.finished = true;
                } else if op.splice_stage == SpliceStage::Read {
                    op.pipe_bytes = cqe.res as usize;
                    op.pipe_sent = 0;
                    op.splice_stage = SpliceStage::Write;
                } else {
                    op.pipe_sent += cqe.res as usize;
                    op.sent += cqe.res as usize;
                    if op.pipe_sent == op.pipe_bytes {
                        op.pipe_bytes = 0;
                        op.pipe_sent = 0;
                        if op.sent == op.requested {
                            events.push(Event::Spliced {
                                token,
                                result: Ok(op.sent),
                            });
                            op.finished = true;
                        } else {
                            op.splice_stage = SpliceStage::Read;
                        }
                    }
                }
            }
            Kind::Idle => {}
        }
        self.schedule(key);
        Ok(true)
    }
    fn drain_deferred(&mut self, events: &mut Vec<Event>, limit: usize) -> io::Result<()> {
        let count = self.deferred.len();
        for _ in 0..count {
            if events.len() == limit {
                break;
            }
            let Some(mut pending) = self.deferred.pop_front() else {
                break;
            };
            let key = pending.cqe.user_data;
            if self
                .op(key)
                .is_some_and(|op| op.completed_sequence != pending.sequence)
            {
                self.deferred.push_back(pending);
                continue;
            }
            let complete = match self.complete(&mut pending, events) {
                Ok(complete) => complete,
                Err(error) => {
                    self.deferred.push_front(pending);
                    return Err(error);
                }
            };
            if complete {
                if let Some(op) = self.op_mut(key) {
                    op.pending -= 1;
                    op.completed_sequence = op.completed_sequence.wrapping_add(1);
                }
                self.schedule(key);
            } else {
                self.deferred.push_back(pending);
            }
        }
        Ok(())
    }
    pub fn poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()> {
        let initial = events.len();
        let limit = initial.saturating_add(self.config.limits.completion_budget);
        self.retry_deadline = None;
        self.sq_deferred = false;
        self.pool.flush_recycles();
        #[cfg(feature = "provided-buffers")]
        if let Some(provided) = &mut self.provided {
            provided.flush();
        }
        #[cfg(feature = "zc-rx")]
        if let Some(zcrx) = &self.zcrx {
            zcrx.flush_refills(&self.ring)?;
        }
        self.drain_deferred(events, limit)?;
        if let Some(cqe) = self.blocked_ingest.take() {
            self.enqueue_completion(cqe)?;
        }
        self.drive_ready(events, limit)?;
        self.release_aborted_sockets()?;
        self.arm_wake()?;
        // A full SQ can hold only sleeping receives. Submit without waiting for
        // their CQEs while other submissions (including cancel/wake) are owed.
        // Credit, buffer and retry-timer stalls do not set sq_deferred.
        let effective_timeout = if self.sq_deferred || events.len() != initial {
            Some(Duration::ZERO)
        } else if let Some(deadline) = self.retry_deadline {
            let retry = deadline.saturating_duration_since(Instant::now());
            Some(timeout.map_or(retry, |timeout| timeout.min(retry)))
        } else {
            timeout
        };
        self.ring.wait(effective_timeout)?;
        let mut completed = 0usize;
        while completed < self.config.limits.completion_budget
            && self.deferred.len() < self.deferred_limit
        {
            let Some(cqe) = self.ring.pop() else {
                break;
            };
            self.enqueue_completion(cqe)?;
            completed += 1;
        }
        self.ring.flush_completions();
        self.drain_deferred(events, limit)?;
        self.drive_ready(events, limit)?;
        self.release_aborted_sockets()?;
        self.ring.submit()?;
        if let Some(error) = self.shutdown_error.take() {
            return Err(error);
        }
        Ok(())
    }
    fn completed_datagram(
        &self,
        key: u64,
        data: crate::buffer::ReadBuf,
        original: usize,
    ) -> io::Result<Received> {
        let op = self.op(key).unwrap();
        debug_assert!(!op.multishot && !op.submitted && !op.native_pending);
        // Single-shot terminal CQE acquisition precedes this read. A retained
        // CQE keeps pending > 0, preventing rearm until metadata is consumed.
        let native = unsafe { &*self.native_message(key as u32 as usize) };
        datagram(data, native, original)
    }
}

fn datagram(
    data: crate::buffer::ReadBuf,
    native: &NativeMessage,
    original: usize,
) -> io::Result<Received> {
    let message = &native.message;
    let address_bytes = unsafe {
        std::slice::from_raw_parts(
            native.address.as_ptr().cast::<u8>(),
            (message.msg_namelen as usize).min(net::ADDRESS_BYTES),
        )
    };
    let peer = if address_bytes.is_empty() {
        None
    } else {
        Some(net::decode(address_bytes)?)
    };
    let control_bytes = unsafe {
        std::slice::from_raw_parts(
            native.control.as_ptr().cast::<u8>(),
            message.msg_controllen.min(net::CONTROL_BYTES),
        )
    };
    let segment = net::gro_segment(control_bytes)?;
    let truncated = message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) as u32 != 0
        || original > data.len();
    Ok(Received {
        data,
        peer,
        truncated,
        original_len: Some(original),
        gro_segment_size: segment,
    })
}

#[cfg(feature = "multishot-recv")]
fn parse_multishot_datagram(
    data: crate::buffer::ReadBuf,
    receive_chunk: usize,
) -> io::Result<Received> {
    let header = size_of::<RecvMsgOut>();
    let name_space = net::ADDRESS_BYTES;
    let payload_offset = header + name_space + net::CONTROL_BYTES;
    if data.len() < payload_offset {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short multishot recvmsg completion",
        ));
    }
    let out = unsafe { data.as_ptr().cast::<RecvMsgOut>().read_unaligned() };
    let peer = if out.namelen == 0 {
        None
    } else {
        Some(net::decode(
            &data.as_slice()[header..header + (out.namelen as usize).min(name_space)],
        )?)
    };
    let controls = &data.as_slice()[header + name_space
        ..header + name_space + (out.controllen as usize).min(net::CONTROL_BYTES)];
    let segment = net::gro_segment(controls)?;
    // Later multishot attempts may use the entire provided buffer even when the
    // initial request was smaller. Preserve the socket's datagram receive cap.
    let length = (out.payloadlen as usize)
        .min(data.len() - payload_offset)
        .min(receive_chunk);
    let payload = data.slice(payload_offset..payload_offset + length);
    Ok(Received {
        data: payload,
        peer,
        original_len: Some(out.payloadlen as usize),
        truncated: out.flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) as u32 != 0
            || length < out.payloadlen as usize,
        gro_segment_size: segment,
    })
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.begin_shutdown();
        // Normal shutdown is already drained by Runtime. This path also makes a
        // partially constructed runtime or an unwinding owner safe: sync cancel
        // settles persistent requests, then notifications retain their guards
        // until their actual CQEs, never merely until the send result.
        let _ = self.ring.cancel_all();
        let mut events = Vec::with_capacity(self.config.limits.completion_budget);
        while !self.is_idle() {
            events.clear();
            if self
                .poll(Some(Duration::from_millis(10)), &mut events)
                .is_err()
            {
                // Retrying cancellation does not release ownership. In
                // particular it cannot turn a transient EINTR into a UAF.
                let _ = self.ring.cancel_all();
            }
        }
        #[cfg(feature = "zc-rx")]
        if let Some(zcrx) = &mut self.zcrx {
            let _ = zcrx.shutdown(&self.ring);
        }
        self.pool.flush_recycles();
        #[cfg(feature = "uring-msg-ring")]
        self.notifier.detach();
    }
}

impl Driver {
    fn mark_submitted(&mut self, key: u64) {
        let op = self.op_mut(key).unwrap();
        op.submitted = true;
        op.native_pending = true;
        let socket = op.socket;
        #[cfg(not(feature = "tcp-splice"))]
        let destination = None;
        #[cfg(feature = "tcp-splice")]
        let destination = op.destination;
        for id in [Some(socket), destination].into_iter().flatten() {
            self.sockets.get_mut(id.0).unwrap().native_pending += 1;
        }
    }
    fn complete_native(&mut self, cqe: &Cqe) {
        let Some(op) = self.op_mut(cqe.user_data) else {
            return;
        };
        if !op.native_pending || cqe.flags & IORING_CQE_F_NOTIF != 0 {
            return;
        }
        if cqe.flags & IORING_CQE_F_MORE != 0
            && !(cfg!(feature = "zc-tx") && op.kind == Kind::Send && op.use_zc)
        {
            return;
        }
        op.native_pending = false;
        let socket = op.socket;
        #[cfg(not(feature = "tcp-splice"))]
        let destination = None;
        #[cfg(feature = "tcp-splice")]
        let destination = op.destination;
        for id in [Some(socket), destination].into_iter().flatten() {
            self.sockets.get_mut(id.0).unwrap().native_pending -= 1;
        }
    }
    fn release_aborted_sockets(&mut self) -> io::Result<()> {
        if !self.stopping && self.aborting_sockets == 0 {
            return Ok(());
        }
        for (_, socket) in self.sockets.iter_mut() {
            // SPLICE resolves its input descriptor in io-wq. Wait for native
            // requests to retire before freeing descriptor numbers; a send's
            // separate ZC notification does not hold this barrier open.
            if socket.native_pending != 0 || (!self.stopping && !socket.aborting) {
                continue;
            }
            #[cfg(feature = "fixed-files")]
            if let Some(index) = socket.fixed {
                self.fixed.as_mut().unwrap().remove(&self.ring, index)?;
                socket.fixed = None;
            }
            socket.fd.take();
            if socket.aborting {
                socket.aborting = false;
                self.aborting_sockets -= 1;
            }
        }
        Ok(())
    }
}
