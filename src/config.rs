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
            pub const fn bit(self) -> u64 { 1u64 << self as u8 }
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

impl std::fmt::Display for Optimization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Per-worker resource bounds. These limits do not preallocate network traffic.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_tasks: usize,
    pub max_sockets: usize,
    pub max_operations: usize,
    pub max_pending_receives: usize,
    pub max_pending_accepts: usize,
    pub max_send_bytes: usize,
    pub max_iovecs: usize,
    pub task_budget: usize,
    pub completion_budget: usize,
    pub pool: PoolConfig,
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

    /// Request a capability strictly; compilation alone never enables it.
    pub fn enable(mut self, optimization: Optimization) -> Self {
        self.optimizations
            .insert(optimization, Policy::RequireCapability);
        self
    }

    pub fn with_policy(mut self, optimization: Optimization, policy: Policy) -> Self {
        self.optimizations.insert(optimization, policy);
        self
    }

    pub fn policy(&self, optimization: Optimization) -> Policy {
        self.optimizations
            .get(&optimization)
            .copied()
            .unwrap_or(Policy::Off)
    }

    pub fn requested(&self, optimization: Optimization) -> bool {
        self.policy(optimization) != Policy::Off
    }

    /// Resolve implicit dependencies and reject contradictions before OS resources exist.
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
        for (left, right) in [
            (SqPoll, ZcRx),
            (SqPoll, ZcRxNodev),
            (SqPoll, SqRewind),
            (ZcRx, ZcRxNodev),
        ] {
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
        for (&feature, &policy) in &config.optimizations {
            if policy == Policy::RequireCapability && !feature.compiled() {
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
        }
        Ok(config)
    }

    pub(crate) fn dependencies(&self, feature: Optimization) -> &'static [Optimization] {
        use Optimization::*;
        match feature {
            DirectDescriptors => &[FixedFiles],
            IncrementalBuffers | MultishotRecv | BufferBundles => &[ProvidedBuffers],
            ZcTxFixed => &[ZcTx, RegisteredBuffers],
            ZcTxVectored => &[ZcTx],
            ZcRxShared if self.requested(ZcRxNodev) => &[ZcRxNodev],
            ZcRxLargeChunks | ZcRxShared => &[ZcRx],
            _ => &[],
        }
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
