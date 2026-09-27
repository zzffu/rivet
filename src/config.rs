//! Startup configuration and legal optimization combinations.

use crate::{
    buffer::PoolConfig,
    driver::{Event, Received, SocketInfo},
};
use std::{alloc::Layout, collections::BTreeMap, io, time::Duration};

/// What to do when an explicitly selected optimization is unavailable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Policy {
    #[default]
    Off,
    Auto,
    RequireCapability,
}

macro_rules! optimizations {
    ($($name:ident => $feature:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        #[repr(u8)]
        pub enum Optimization { $($name),+ }
        impl Optimization {
            pub const ALL: &'static [Self] = &[$(Self::$name),+];
            pub const fn name(self) -> &'static str {
                match self { $(Self::$name => $feature),+ }
            }
            pub const fn compiled(self) -> bool {
                let platform = cfg!(target_os = "linux")
                    || (cfg!(target_os = "android") && matches!(self, Self::UdpGso | Self::UdpGro));
                platform && match self { $(Self::$name => cfg!(feature = $feature)),+ }
            }
            pub(crate) const fn bit(self) -> u64 { 1u64 << self as u8 }
        }
    };
}

optimizations! {
    FixedFiles => "fixed-files",
    DirectDescriptors => "direct-descriptors",
    RegisteredBuffers => "registered-buffers",
    RegisteredRing => "registered-ring",
    RegisteredWait => "registered-wait",
    ProvidedBuffers => "provided-buffers",
    IncrementalBuffers => "incremental-buffers",
    MultishotAccept => "multishot-accept",
    MultishotRecv => "multishot-recv",
    BufferBundles => "buffer-bundles",
    SqRewind => "sq-rewind",
    MixedCqe => "mixed-cqe",
    ZcTx => "zc-tx",
    ZcTxFixed => "zc-tx-fixed",
    ZcTxVectored => "zc-tx-vectored",
    ZcRx => "zc-rx",
    ZcRxLargeChunks => "zc-rx-large-chunks",
    ZcRxShared => "zc-rx-shared",
    ZcObserve => "zc-observe",
    ZcRxNodev => "zc-rx-nodev",
    SqPoll => "uring-sqpoll",
    NapiBusyPoll => "uring-napi",
    MsgRing => "uring-msg-ring",
    UdpGso => "udp-gso",
    UdpGro => "udp-gro",
    TcpSplice => "tcp-splice",
}

const CONFLICTS: &[(Optimization, Optimization)] = &[
    (Optimization::SqPoll, Optimization::ZcRx),
    (Optimization::SqPoll, Optimization::ZcRxNodev),
    (Optimization::SqPoll, Optimization::SqRewind),
    (Optimization::ZcRx, Optimization::ZcRxNodev),
];

impl std::fmt::Display for Optimization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Resource bounds configured independently for each worker.
///
/// Socket/operation slots and the pool are shared by that worker's sockets;
/// receive and accept queue bounds apply separately to each socket. Runtime
/// construction does not reserve every possible socket's traffic buffers.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_tasks: usize,
    pub max_sockets: usize,
    /// Capacity of each of the separate Core and native-driver operation arenas.
    /// A Windows UDP socket uses one Core receive operation but
    /// `max_pending_receives` native receive operations, before other I/O.
    pub max_operations: usize,
    /// Per-socket queued receive results, not a worker-wide receive count.
    ///
    /// Windows UDP also reserves this many native receive lanes before
    /// bind/import returns. Each lane initially needs one distinct lease and
    /// `max(receive_chunk, pool.block_size)` payload bytes from the shared pool.
    /// TCP does not post this many parallel native receives. See
    /// [`Self::windows_udp_receive_bytes`] for Windows payload planning.
    pub max_pending_receives: usize,
    /// Per-listener queued accepted connections.
    pub max_pending_accepts: usize,
    pub max_send_bytes: usize,
    pub max_iovecs: usize,
    pub task_budget: usize,
    pub completion_budget: usize,
    /// Shared worker-local payload arena and distinct-lease budget.
    ///
    /// Immutable aliases share a lease slot but prevent writable reuse. Keeping
    /// old receive allocations while rearming a full window requires additional
    /// storage/leases. This budget excludes native metadata and is not process RSS.
    pub pool: PoolConfig,
}

impl Limits {
    /// Initial payload bytes reserved by one Windows UDP receive window.
    ///
    /// Computes `max_pending_receives * max(receive_chunk, pool.block_size)`
    /// without allocating or creating a runtime. Available on every platform
    /// for offline Windows planning; other backends have different reservations.
    ///
    /// The result may exceed `pool.bytes`: this is an estimate, not admission.
    /// For multiple sockets on one worker, sum their estimates with checked
    /// arithmetic and separately budget one lease and native operation per lane,
    /// one Core receive operation per socket, and all other I/O. Retained old
    /// allocations need extra storage if the full window must remain armed.
    /// Free bytes alone do not guarantee a sufficiently large contiguous extent.
    ///
    /// Returns `InvalidInput` for zero window/block sizes, a receive size outside
    /// `1..=i32::MAX`, or multiplication overflow. It does not replace complete
    /// configuration validation, current-resource admission or native OS checks.
    pub fn windows_udp_receive_bytes(&self, receive_chunk: usize) -> io::Result<usize> {
        if self.max_pending_receives == 0
            || self.pool.block_size == 0
            || receive_chunk == 0
            || receive_chunk > i32::MAX as usize
        {
            return Err(invalid("invalid Windows UDP receive window or buffer size"));
        }
        self.max_pending_receives
            .checked_mul(receive_chunk.max(self.pool.block_size))
            .ok_or_else(|| invalid("Windows UDP receive window payload size overflow"))
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_tasks: 4096,
            max_sockets: 4096,
            max_operations: 8192,
            max_pending_receives: 64,
            max_pending_accepts: 64,
            max_send_bytes: 16 * 1024 * 1024,
            max_iovecs: 64,
            task_budget: 64,
            completion_budget: 256,
            pool: PoolConfig {
                bytes: 16 * 1024 * 1024,
                block_size: 16 * 1024,
                max_leases: 8192,
            },
        }
    }
}

/// A deployment-preconfigured NIC receive queue. Rivet never reconfigures it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RxQueue {
    pub interface_index: u32,
    pub queue_index: u32,
}

#[derive(Clone, Debug)]
pub struct ZcrxConfig {
    pub queues: Vec<RxQueue>,
    pub area_bytes: usize,
    pub refill_entries: u32,
    pub chunk_bytes: u32,
}

impl Default for ZcrxConfig {
    fn default() -> Self {
        Self {
            queues: Vec::new(),
            area_bytes: 16 * 1024 * 1024,
            refill_entries: 4096,
            chunk_bytes: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LinuxConfig {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub sqpoll_idle: Duration,
    pub sqpoll_cpu: Option<u32>,
    pub napi_busy_poll: Duration,
    pub napi_prefer_busy_poll: bool,
    pub napi_ids: Vec<u32>,
    pub zcrx: ZcrxConfig,
    pub zc_send_threshold: usize,
    pub splice_pipe_bytes: usize,
}

impl Default for LinuxConfig {
    fn default() -> Self {
        Self {
            sq_entries: 1024,
            cq_entries: 4096,
            sqpoll_idle: Duration::from_millis(1),
            sqpoll_cpu: None,
            napi_busy_poll: Duration::from_micros(25),
            napi_prefer_busy_poll: false,
            napi_ids: Vec::new(),
            zcrx: ZcrxConfig::default(),
            zc_send_threshold: 16 * 1024,
            splice_pipe_bytes: 64 * 1024,
        }
    }
}

/// Independent bounds for blocking closures, separate from async worker tasks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockingConfig {
    /// Maximum number of blocking threads, started only when work needs them.
    pub threads: usize,
    /// Maximum number of closures waiting to start; running work is separate.
    pub queue_capacity: usize,
}

impl Default for BlockingConfig {
    fn default() -> Self {
        Self {
            threads: 4,
            queue_capacity: 128,
        }
    }
}

/// Resource bounds and explicit optimization policies.
///
/// Linux supplements unspecified policies with compatible automatic candidates
/// during backend initialization. Windows and Android do not inherit those
/// defaults. Effective choices are reported by [`crate::Runtime::capabilities`],
/// not by configuration queries.
///
/// A zero-copy threshold is a configurable starting point, not a measured optimum.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub workers: usize,
    pub affinity: Option<Vec<usize>>,
    pub limits: Limits,
    pub blocking: BlockingConfig,
    /// Runtime-wide bound for non-socket native registrations.
    pub max_async_io: usize,
    pub linux: LinuxConfig,
    pub idle_spin: Duration,
    /// Explicit overrides: Off forbids a path, Auto permits fallback, and
    /// RequireCapability makes an unavailable path an initialization error.
    /// An absent entry is not an explicit Off and may inherit a Linux default.
    pub optimizations: BTreeMap<Optimization, Policy>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            affinity: None,
            limits: Limits::default(),
            blocking: BlockingConfig::default(),
            max_async_io: 256,
            linux: LinuxConfig::default(),
            idle_spin: Duration::ZERO,
            optimizations: BTreeMap::new(),
        }
    }
}

impl RuntimeConfig {
    pub fn single_thread() -> Self {
        Self {
            workers: 1,
            ..Self::default()
        }
    }

    /// Require an optimization; initialization fails if it cannot be activated.
    pub fn enable(mut self, optimization: Optimization) -> Self {
        self.optimizations
            .insert(optimization, Policy::RequireCapability);
        self
    }

    /// Override an optimization's policy, including any automatic Linux default.
    pub fn with_policy(mut self, optimization: Optimization, policy: Policy) -> Self {
        self.optimizations.insert(optimization, policy);
        self
    }

    /// Return the stored request policy, or Off when no entry is present.
    ///
    /// This does not resolve automatic Linux defaults or native capabilities.
    /// Use [`crate::Runtime::capabilities`] for the backend's actual decisions.
    pub fn policy(&self, optimization: Optimization) -> Policy {
        self.optimizations
            .get(&optimization)
            .copied()
            .unwrap_or(Policy::Off)
    }

    /// Whether the stored request is Auto or RequireCapability.
    ///
    /// This is not an enabled-capability query; absent Linux defaults are not
    /// represented until the backend prepares its local configuration.
    pub fn requested(&self, optimization: Optimization) -> bool {
        self.policy(optimization) != Policy::Off
    }

    /// Resolve explicit requests' dependencies and reject contradictions before
    /// OS resources exist. This does not insert automatic Linux defaults.
    pub fn normalized(&self) -> io::Result<Self> {
        let mut config = self.clone();
        config.validate_limits()?;
        for _ in 0..Optimization::ALL.len() {
            let mut changed = false;
            for &feature in Optimization::ALL {
                let policy = config.policy(feature);
                if policy == Policy::Off {
                    continue;
                }
                for &dependency in config.dependencies(feature) {
                    match config.optimizations.get(&dependency).copied() {
                        Some(Policy::Off) => {
                            return Err(invalid(format!(
                                "{} requires {}, explicitly disabled",
                                feature, dependency
                            )));
                        }
                        Some(Policy::Auto) if policy == Policy::RequireCapability => {
                            config.optimizations.insert(dependency, policy);
                            changed = true;
                        }
                        // An unavailable Auto path must not activate resources
                        // or a conflicting mode solely through implicit dependencies.
                        None if policy == Policy::RequireCapability || feature.compiled() => {
                            config.optimizations.insert(dependency, policy);
                            changed = true;
                        }
                        _ => {}
                    }
                }
            }
            if !changed {
                break;
            }
        }
        use Optimization::*;
        for &(left, right) in CONFLICTS {
            if config.requested(left) && config.requested(right) {
                return Err(invalid(format!(
                    "{} and {} cannot be requested together",
                    left, right
                )));
            }
        }
        if config.requested(ZcObserve)
            && ![ZcTx, ZcRx, ZcRxNodev].iter().any(|f| config.requested(*f))
        {
            return Err(invalid(
                "zc-observe requires an explicitly selected ZC or NODEV path",
            ));
        }
        let mut unavailable = None;
        for (&feature, &policy) in &config.optimizations {
            if policy == Policy::RequireCapability && !feature.compiled() {
                // Prefer the caller's strict request over a dependency that
                // normalization inserted or strengthened on its behalf.
                if self.policy(feature) == Policy::RequireCapability {
                    unavailable = Some(feature);
                    break;
                }
                unavailable.get_or_insert(feature);
            }
        }
        if let Some(feature) = unavailable {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                crate::capability::CapabilityError {
                    optimization: feature,
                    reason: format!(
                        "implementation is not compiled for {}; check target support and Cargo features",
                        std::env::consts::OS
                    ),
                },
            ));
        }
        Ok(config)
    }

    pub(crate) fn dependencies(&self, feature: Optimization) -> &'static [Optimization] {
        use Optimization::*;
        match feature {
            DirectDescriptors => &[FixedFiles],
            IncrementalBuffers | MultishotRecv | BufferBundles => &[ProvidedBuffers],
            ZcTxFixed | ZcTxVectored => &[ZcTx],
            ZcRxShared if self.requested(ZcRxNodev) => &[ZcRxNodev],
            ZcRxLargeChunks | ZcRxShared => &[ZcRx],
            _ => &[],
        }
    }

    /// Add only compatible candidates to an already normalized local copy.
    #[cfg(target_os = "linux")]
    pub(crate) fn apply_linux_defaults(&mut self) {
        use Optimization::*;
        // Dependencies precede their consumers; explicit Off entries remain
        // authoritative without another normalization pass.
        for feature in [
            FixedFiles,
            DirectDescriptors,
            RegisteredBuffers,
            RegisteredRing,
            RegisteredWait,
            ProvidedBuffers,
            IncrementalBuffers,
            MultishotAccept,
            MultishotRecv,
            BufferBundles,
            SqRewind,
            ZcTx,
            ZcTxFixed,
            ZcTxVectored,
            UdpGso,
            UdpGro,
            TcpSplice,
        ] {
            self.insert_linux_default(feature);
        }
        if self.workers > 1 {
            self.insert_linux_default(MsgRing);
        }

        let queues = self.linux.zcrx.queues.len();
        if queues != 0
            && (queues >= self.workers
                || (ZcRxShared.compiled()
                    && self.optimizations.get(&ZcRxShared) != Some(&Policy::Off)))
        {
            self.insert_linux_default(ZcRx);
        }
        if ZcRx.compiled() && self.requested(ZcRx) {
            if queues != 0 && queues < self.workers {
                self.insert_linux_default(ZcRxShared);
            }
            if ZcRxLargeChunks.compiled()
                && !self.optimizations.contains_key(&ZcRxLargeChunks)
                && self.linux.zcrx.chunk_bytes != 0
            {
                let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                if page > 0
                    && (page as usize).is_power_of_two()
                    && self.linux.zcrx.chunk_bytes as usize > page as usize
                {
                    self.insert_linux_default(ZcRxLargeChunks);
                }
            }
        }
        let receive = [ZcRx, ZcRxNodev]
            .into_iter()
            .any(|feature| feature.compiled() && self.requested(feature));
        if receive {
            self.insert_linux_default(MixedCqe);
        }
        if receive || (ZcTx.compiled() && self.requested(ZcTx)) {
            self.insert_linux_default(ZcObserve);
        }
    }

    #[cfg(target_os = "linux")]
    fn insert_linux_default(&mut self, feature: Optimization) {
        if !feature.compiled()
            || self.optimizations.contains_key(&feature)
            || self
                .dependencies(feature)
                .iter()
                .any(|&dependency| !dependency.compiled() || !self.requested(dependency))
            || CONFLICTS.iter().any(|&(left, right)| {
                (feature == left && self.requested(right))
                    || (feature == right && self.requested(left))
            })
        {
            return;
        }
        self.optimizations.insert(feature, Policy::Auto);
    }

    fn validate_limits(&self) -> io::Result<()> {
        let l = &self.limits;
        if self.workers == 0 || self.workers > u16::MAX as usize {
            return Err(invalid("workers must be in 1..=65535"));
        }
        if let Some(cpus) = &self.affinity
            && cpus.len() != self.workers
        {
            return Err(invalid("affinity must contain one CPU per worker"));
        }
        if self.blocking.threads == 0 || self.blocking.threads > u16::MAX as usize {
            return Err(invalid("blocking threads must be in 1..=65535"));
        }
        crate::io::Registry::validate_capacity(self.max_async_io)?;
        if [
            l.max_tasks,
            l.max_sockets,
            l.max_operations,
            l.max_pending_receives,
            l.max_pending_accepts,
            l.max_send_bytes,
            l.max_iovecs,
            l.task_budget,
            l.completion_budget,
            l.pool.bytes,
            l.pool.block_size,
            l.pool.max_leases,
            self.blocking.queue_capacity,
        ]
        .contains(&0)
        {
            return Err(invalid(
                "resource capacities and processing budgets must be nonzero",
            ));
        }
        if l.max_iovecs > 1024 || l.pool.block_size > l.pool.bytes {
            return Err(invalid("invalid iovec or pool bounds"));
        }
        if [l.max_tasks, l.max_operations, l.max_sockets]
            .iter()
            .any(|&limit| limit > u32::MAX as usize)
        {
            return Err(invalid(
                "task, socket and operation limits exceed token index capacity",
            ));
        }
        // Queue limits count elements, not bytes. Reject layouts that Vec and
        // VecDeque cannot address before constructing a worker or a socket.
        Layout::array::<io::Result<Received>>(l.max_pending_receives)
            .map_err(|_| invalid("receive queue capacity exceeds addressable memory"))?;
        Layout::array::<io::Result<SocketInfo>>(l.max_pending_accepts)
            .map_err(|_| invalid("accept queue capacity exceeds addressable memory"))?;
        Layout::array::<Event>(l.completion_budget)
            .map_err(|_| invalid("completion queue capacity exceeds addressable memory"))?;
        Layout::array::<Box<dyn FnOnce() + Send>>(self.blocking.queue_capacity)
            .map_err(|_| invalid("blocking queue capacity exceeds addressable memory"))?;
        Layout::array::<std::thread::JoinHandle<()>>(self.blocking.threads)
            .map_err(|_| invalid("blocking thread capacity exceeds addressable memory"))?;
        if self
            .blocking
            .threads
            .checked_add(self.blocking.queue_capacity)
            .is_none()
        {
            return Err(invalid("blocking admission size overflow"));
        }
        if l.max_tasks.checked_mul(2).is_none() || self.workers.checked_mul(l.pool.bytes).is_none()
        {
            return Err(invalid("resource size overflow"));
        }
        let linux = &self.linux;
        if !linux.sq_entries.is_power_of_two()
            || !linux.cq_entries.is_power_of_two()
            || linux.cq_entries < linux.sq_entries
            || linux.sq_entries > 32768
            || linux.cq_entries > 65536
        {
            return Err(invalid(
                "SQ/CQ entries must be bounded powers of two with CQ >= SQ",
            ));
        }
        if linux.zcrx.area_bytes == 0 || !linux.zcrx.refill_entries.is_power_of_two() {
            return Err(invalid("invalid ZCRX memory or refill size"));
        }
        if linux.sqpoll_idle.as_millis() == 0
            || linux.sqpoll_idle.as_millis() > u32::MAX as u128
            || linux.napi_busy_poll.as_micros() > u32::MAX as u128
            || (self.requested(Optimization::NapiBusyPoll) && linux.napi_busy_poll.as_micros() == 0)
        {
            return Err(invalid("polling duration is outside the kernel ABI range"));
        }
        Ok(())
    }
}

fn invalid(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{Optimization as O, Policy, RuntimeConfig, RxQueue};

    #[test]
    fn automatic_dependencies_respect_explicit_off() {
        for (dependency, children) in [
            (O::FixedFiles, &[O::DirectDescriptors][..]),
            (
                O::ProvidedBuffers,
                &[O::IncrementalBuffers, O::MultishotRecv, O::BufferBundles][..],
            ),
            (O::ZcTx, &[O::ZcTxFixed, O::ZcTxVectored, O::ZcObserve][..]),
        ] {
            let mut config = RuntimeConfig::single_thread()
                .with_policy(dependency, Policy::Off)
                .normalized()
                .unwrap();
            config.apply_linux_defaults();
            assert_eq!(config.policy(dependency), Policy::Off);
            for &child in children {
                assert!(!config.requested(child), "{child}");
            }
        }
    }

    #[test]
    fn automatic_ring_modes_yield_to_explicit_requests() {
        for (mode, suppressed) in [
            (O::SqPoll, &[O::SqRewind, O::ZcRx, O::MixedCqe][..]),
            (
                O::ZcRxNodev,
                &[O::ZcRx, O::ZcRxLargeChunks, O::ZcRxShared][..],
            ),
        ] {
            let mut config = RuntimeConfig::single_thread().with_policy(mode, Policy::Auto);
            config.linux.zcrx.queues.push(RxQueue {
                interface_index: 1,
                queue_index: 0,
            });
            let mut config = config.normalized().unwrap();
            config.apply_linux_defaults();
            assert_eq!(config.policy(mode), Policy::Auto);
            for &feature in suppressed {
                assert!(!config.requested(feature), "{feature}");
            }
        }
    }

    #[cfg(feature = "zc-tx-fixed")]
    #[test]
    fn automatic_fixed_zero_copy_keeps_ordinary_fixed_io_disabled() {
        let mut config = RuntimeConfig::single_thread()
            .with_policy(O::RegisteredBuffers, Policy::Off)
            .enable(O::ZcTx)
            .normalized()
            .unwrap();
        config.apply_linux_defaults();
        assert_eq!(config.policy(O::ZcTxFixed), Policy::Auto);
        assert_eq!(config.policy(O::RegisteredBuffers), Policy::Off);
        assert_eq!(config.policy(O::ZcTx), Policy::RequireCapability);
        let once = config.optimizations.clone();
        config.apply_linux_defaults();
        assert_eq!(config.optimizations, once);
    }

    #[cfg(feature = "zc-rx-shared")]
    #[test]
    fn automatic_hardware_receive_requires_usable_queue_topology() {
        let mut config = RuntimeConfig::single_thread();
        config.workers = 2;
        config.linux.zcrx.queues.push(RxQueue {
            interface_index: 1,
            queue_index: 0,
        });
        let mut disabled = config
            .clone()
            .with_policy(O::ZcRxShared, Policy::Off)
            .normalized()
            .unwrap();
        disabled.apply_linux_defaults();
        assert!(!disabled.requested(O::ZcRx));
        assert_eq!(disabled.policy(O::ZcRxShared), Policy::Off);

        let mut shared = config.normalized().unwrap();
        shared.apply_linux_defaults();
        assert_eq!(shared.policy(O::ZcRx), Policy::Auto);
        assert_eq!(shared.policy(O::ZcRxShared), Policy::Auto);

        config.linux.zcrx.queues.push(RxQueue {
            interface_index: 1,
            queue_index: 1,
        });
        let mut independent = config.normalized().unwrap();
        independent.apply_linux_defaults();
        assert_eq!(independent.policy(O::ZcRx), Policy::Auto);
        assert!(!independent.requested(O::ZcRxShared));
    }

    #[cfg(feature = "zc-rx-large-chunks")]
    #[test]
    fn automatic_large_chunks_follow_explicit_size_and_off_policy() {
        let page = u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap();
        let mut config = RuntimeConfig::single_thread();
        config.linux.zcrx.queues.push(RxQueue {
            interface_index: 1,
            queue_index: 0,
        });
        config.linux.zcrx.chunk_bytes = page;
        let mut ordinary = config.normalized().unwrap();
        ordinary.apply_linux_defaults();
        assert!(!ordinary.requested(O::ZcRxLargeChunks));

        config.linux.zcrx.chunk_bytes = page.checked_mul(2).unwrap();
        let mut large = config.normalized().unwrap();
        large.apply_linux_defaults();
        assert_eq!(large.policy(O::ZcRxLargeChunks), Policy::Auto);

        let mut disabled = config
            .with_policy(O::ZcRxLargeChunks, Policy::Off)
            .normalized()
            .unwrap();
        disabled.apply_linux_defaults();
        assert_eq!(disabled.policy(O::ZcRxLargeChunks), Policy::Off);
    }

    #[cfg(feature = "uring-msg-ring")]
    #[test]
    fn automatic_worker_notifications_respect_worker_count_and_off() {
        let mut config = RuntimeConfig::single_thread();
        config.apply_linux_defaults();
        assert!(!config.requested(O::MsgRing));
        config.workers = 2;
        config.apply_linux_defaults();
        assert_eq!(config.policy(O::MsgRing), Policy::Auto);

        let mut disabled = config.with_policy(O::MsgRing, Policy::Off);
        disabled.apply_linux_defaults();
        assert_eq!(disabled.policy(O::MsgRing), Policy::Off);
    }
}
