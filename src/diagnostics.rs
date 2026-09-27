//! Read-only, owner-local resource observations, not a monitoring runtime.
//!
//! Obtain these values from [`crate::BufferPool::usage`],
//! [`crate::runtime::resource_snapshot`] or
//! [`crate::UdpSocket::receive_snapshot`]. Collection does not advance I/O,
//! allocate payload or metadata, recycle leases, update credits or wake tasks.
//! It scans bounded owner-local state; take snapshots on demand rather than on
//! every packet. Values contain no resource owners and may be copied or sent to
//! another thread without making the observed runtime, socket or leases movable.
//!
//! Software state can lag native completion. Counts are neither packet-loss
//! measurements nor proof that every kernel reference has retired. Independent
//! workers do not share an atomic observation point. Optional backend fields use
//! `None` for an inapplicable model, not a fabricated zero.

/// Allocation accounting for one pool, independent of runtime lifetime.
///
/// Normal arena bytes count entire allocated extents, not initialized bytes or
/// visible slices. Aliases share one extent and one distinct lease slot. External
/// backing has its own byte budget but shares this pool's lease slots, including
/// slots retained while the external provider has not accepted their return.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolUsage {
    pub(crate) payload_capacity: usize,
    pub(crate) payload_available: usize,
    pub(crate) largest_free_extent: usize,
    pub(crate) lease_capacity: usize,
    pub(crate) leases_available: usize,
    pub(crate) pending_recycles: usize,
}

impl PoolUsage {
    /// Total bytes in the normal preallocated arena, excluding external backing.
    pub fn payload_capacity(&self) -> usize {
        self.payload_capacity
    }
    /// Bytes charged to normal allocations, regardless of visible slice length.
    pub fn payload_in_use(&self) -> usize {
        self.payload_capacity - self.payload_available
    }
    /// Sum of free normal extents; fragmentation may prevent a large allocation.
    pub fn payload_available(&self) -> usize {
        self.payload_available
    }
    /// Largest contiguous free extent. A free lease slot is also required.
    pub fn largest_free_extent(&self) -> usize {
        self.largest_free_extent
    }
    /// Capacity for distinct normal and external leases together.
    pub fn lease_capacity(&self) -> usize {
        self.lease_capacity
    }
    /// Occupied distinct lease slots, not the number of immutable aliases.
    pub fn leases_in_use(&self) -> usize {
        self.lease_capacity - self.leases_available
    }
    /// Metadata slots currently available for a new distinct lease.
    pub fn leases_available(&self) -> usize {
        self.leases_available
    }
    /// External return tokens currently queued for provider retry.
    ///
    /// This is not all live external leases, native outstanding I/O, or normal
    /// pool pressure. A return callback currently executing is not queued.
    pub fn pending_recycles(&self) -> usize {
        self.pending_recycles
    }
}

/// One current worker's logical I/O, normal pool and native-driver resources.
///
/// This is not a whole-runtime aggregate. Worker numbers are local to a runtime,
/// not persistent identifiers. Core and driver operations are separate domains;
/// do not add them together and compare the sum with one operation limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerResources {
    pub(crate) worker: usize,
    pub(crate) backend: &'static str,
    pub(crate) sockets: usize,
    pub(crate) socket_capacity: usize,
    pub(crate) available_socket_slots: usize,
    pub(crate) operations: usize,
    pub(crate) operation_capacity: usize,
    pub(crate) available_operation_slots: usize,
    pub(crate) queued_receives: usize,
    pub(crate) queued_accepts: usize,
    pub(crate) send_bytes: usize,
    pub(crate) send_byte_capacity: usize,
    pub(crate) pool: PoolUsage,
    pub(crate) driver: DriverResources,
}

impl WorkerResources {
    /// Index of the observed current worker within its runtime.
    pub fn worker(&self) -> usize {
        self.worker
    }
    /// Native backend name from the worker's capability report.
    pub fn backend(&self) -> &'static str {
        self.backend
    }
    /// Occupied Core socket records, excluding already-removed logical sockets.
    pub fn sockets(&self) -> usize {
        self.sockets
    }
    /// Configured Core socket capacity for this worker.
    pub fn socket_capacity(&self) -> usize {
        self.socket_capacity
    }
    /// Actual available Core slots, excluding permanently retired generations.
    pub fn available_socket_slots(&self) -> usize {
        self.available_socket_slots
    }
    /// Occupied logical Core operation records.
    pub fn operations(&self) -> usize {
        self.operations
    }
    /// Configured capacity of the Core operation arena.
    pub fn operation_capacity(&self) -> usize {
        self.operation_capacity
    }
    /// Actual available Core operation slots.
    pub fn available_operation_slots(&self) -> usize {
        self.available_operation_slots
    }
    /// Receive results queued across the worker's logical sockets, including errors.
    pub fn queued_receives(&self) -> usize {
        self.queued_receives
    }
    /// Accepted-connection results queued across listeners, including errors.
    pub fn queued_accepts(&self) -> usize {
        self.queued_accepts
    }
    /// Current Core send-byte admission charge.
    ///
    /// This is neither peer-acknowledged bytes nor all memory retained by the
    /// kernel after send completion. See [`Self::driver`] and [`Self::pool`].
    pub fn send_bytes(&self) -> usize {
        self.send_bytes
    }
    /// Configured limit for the Core send-byte admission charge.
    pub fn send_byte_capacity(&self) -> usize {
        self.send_byte_capacity
    }
    /// The current worker's normal pool, not every application-created pool.
    pub fn pool(&self) -> &PoolUsage {
        &self.pool
    }
    /// Native-driver bookkeeping, distinct from Core admission.
    pub fn driver(&self) -> &DriverResources {
        &self.driver
    }
}

/// Native-driver software bookkeeping at the same owner-local observation point.
///
/// Occupied records can survive logical socket closure and task joining. Slot
/// availability describes backend bookkeeping, not a promise that all other
/// budgets or native OS resources admit another operation. No observation calls
/// into the OS or harvests a completion to make these numbers more current.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverResources {
    pub(crate) sockets: usize,
    pub(crate) available_socket_slots: usize,
    pub(crate) operations: usize,
    pub(crate) available_operation_slots: usize,
    pub(crate) pending_completions: usize,
    pub(crate) closing_sockets: usize,
    pub(crate) native_outstanding: Option<usize>,
    pub(crate) retiring_native: Option<usize>,
    pub(crate) rio_receive_queue_slots: Option<usize>,
    pub(crate) udp_rearm_allocation_failures_total: Option<u64>,
}

impl DriverResources {
    /// Retained native socket records, including closing records.
    pub fn sockets(&self) -> usize {
        self.sockets
    }
    /// Socket slots available under the backend's existing reservations.
    pub fn available_socket_slots(&self) -> usize {
        self.available_socket_slots
    }
    /// Occupied backend operation records or readiness-operation charges.
    pub fn operations(&self) -> usize {
        self.operations
    }
    /// Available slots under the backend's operation/completion admission rules.
    pub fn available_operation_slots(&self) -> usize {
        self.available_operation_slots
    }
    /// Completion records/results retained in software, not unharvested OS queues.
    ///
    /// Windows counts retained semantic results; Linux counts retained completion
    /// records (including expanded bundle entries); Android counts queued events
    /// and its immediate delivery slot. This is not a portable packet count.
    pub fn pending_completions(&self) -> usize {
        self.pending_completions
    }
    /// Closed or closing native socket records not yet removed from the driver.
    pub fn closing_sockets(&self) -> usize {
        self.closing_sockets
    }
    /// Application operation records still awaiting native completion or release.
    ///
    /// Linux includes active zero-copy guards even after the data result. Windows
    /// includes deferred RIO submissions and unharvested RIO/overlapped completion.
    /// Internal wake, cancel and control requests are excluded, so zero does not
    /// prove the entire driver is idle. Android's synchronous readiness I/O is
    /// a different model and returns `None`.
    pub fn native_outstanding(&self) -> Option<usize> {
        self.native_outstanding
    }
    /// The outstanding subset whose operation/socket/driver is stopping.
    ///
    /// A cancelled task alone does not prove its I/O is stopping: submitted sends
    /// may still produce output. `None` has the same model meaning as
    /// [`Self::native_outstanding`].
    pub fn retiring_native(&self) -> Option<usize> {
        self.retiring_native
    }
    /// Windows RIO extra receive-queue reservations, excluding each queue's base slot.
    ///
    /// These survive until the queue's socket record is reaped and share the
    /// configured operation limit. `None` on other backends.
    pub fn rio_receive_queue_slots(&self) -> Option<usize> {
        self.rio_receive_queue_slots
    }
    /// Windows UDP replacement-allocation failures over this driver's lifetime.
    ///
    /// Saturates at `u64::MAX`, survives socket retirement, and counts actual
    /// `WouldBlock` attempts, not unique packets, pause episodes or lost packets.
    /// `None` on backends without this RIO observation.
    pub fn udp_rearm_allocation_failures_total(&self) -> Option<u64> {
        self.udp_rearm_allocation_failures_total
    }
}

/// Logical and backend receive state for one owned UDP socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveResources {
    pub(crate) worker: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) queued_results: usize,
    pub(crate) waiter_registered: bool,
    pub(crate) active: bool,
    pub(crate) credits_pending: bool,
    pub(crate) backend_publication_credits: usize,
    pub(crate) native_outstanding: Option<usize>,
    pub(crate) rio: Option<RioReceiveResources>,
}

impl ReceiveResources {
    /// Owner worker index within this socket's runtime.
    pub fn worker(&self) -> usize {
        self.worker
    }
    /// Maximum queued Core receive results for this socket.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }
    /// Queued data/error results, not bytes; an empty datagram still counts once.
    pub fn queued_results(&self) -> usize {
        self.queued_results
    }
    /// Unoccupied logical queue slots, not the native receive window.
    pub fn queue_available(&self) -> usize {
        self.queue_capacity - self.queued_results
    }
    /// Whether an application receive waiter is registered.
    pub fn waiter_registered(&self) -> bool {
        self.waiter_registered
    }
    /// Whether Core retains a persistent logical receive operation.
    ///
    /// This is not proof that a native receive is currently posted.
    pub fn active(&self) -> bool {
        self.active
    }
    /// Whether Core has a publication-capacity update waiting to be flushed.
    pub fn credits_pending(&self) -> bool {
        self.credits_pending
    }
    /// Publication credits currently held by the backend.
    ///
    /// This can differ from [`Self::queue_available`] while an update is pending.
    /// It neither counts available RIO lanes nor causes a credit update.
    pub fn backend_publication_credits(&self) -> usize {
        self.backend_publication_credits
    }
    /// This socket's receive operation records awaiting native completion.
    ///
    /// Windows counts in-flight lanes, including deferred submissions; Linux
    /// counts its pending native receive operation, not multishot datagrams.
    /// Android returns `None`. Already-completed OS work not yet harvested may
    /// still be counted; do not interpret this as instantaneous NIC capacity.
    pub fn native_outstanding(&self) -> Option<usize> {
        self.native_outstanding
    }
    /// Windows RIO window state; absent on other backends or after window retirement.
    pub fn rio(&self) -> Option<&RioReceiveResources> {
        self.rio.as_ref()
    }
}

/// Windows UDP lane state, distinct from Core queue occupancy and publication credits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RioReceiveResources {
    pub(crate) admitted_lanes: usize,
    pub(crate) ready_results: usize,
    pub(crate) idle_lanes: usize,
    pub(crate) last_pool_blocked_lanes: usize,
    pub(crate) rearm_allocation_failures_total: u64,
    pub(crate) commit_pending: bool,
    pub(crate) stopping: bool,
}

impl RioReceiveResources {
    /// Number of lanes admitted for this still-retained window.
    pub fn admitted_lanes(&self) -> usize {
        self.admitted_lanes
    }
    /// Harvested datagrams retained by the driver, not yet published to Core.
    pub fn ready_results(&self) -> usize {
        self.ready_results
    }
    /// Lanes available for a later rearm attempt; not necessarily pool-blocked.
    pub fn idle_lanes(&self) -> usize {
        self.idle_lanes
    }
    /// Lanes whose most recent storage acquisition failed with `WouldBlock`.
    ///
    /// A newly returned lease does not clear this history until the driver next
    /// obtains writable ownership. Querying never retries the allocation.
    pub fn last_pool_blocked_lanes(&self) -> usize {
        self.last_pool_blocked_lanes
    }
    /// Actual replacement-allocation failures during this window's lifetime.
    ///
    /// Saturates at `u64::MAX`; it is not a dropped-packet count.
    pub fn rearm_allocation_failures_total(&self) -> u64 {
        self.rearm_allocation_failures_total
    }
    /// At least one deferred receive submission still needs its RIO commit.
    pub fn commit_pending(&self) -> bool {
        self.commit_pending
    }
    /// This window is converging a stop/error/close rather than rearming.
    pub fn stopping(&self) -> bool {
        self.stopping
    }
}
