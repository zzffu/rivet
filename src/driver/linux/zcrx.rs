//! CPU-memory ZCRX ABI pinned to Linux v7.2.7, not an older liburing layout.
//!
//! References: include/uapi/linux/io_uring/zcrx.h, io_uring/zcrx.c and net.c
//! in the v7.2.7 stable tree. There is no per-instance unregister syscall:
//! closing a ring unregisters its imports. An exported instance keeps the area
//! alive until its last user reference; our leases additionally retain mappings.

use super::{
    Notifier,
    ring::{Mapping, Ring},
    uapi::*,
};
#[cfg(feature = "zc-tx-fixed")]
use crate::buffer::MemoryRegion;
#[cfg(feature = "zc-observe")]
use crate::capability::ZcStats;
use crate::{
    buffer::{BufferPool, ExternalMemory, ReadBuf, Recycle, ReturnToken},
    capability::ReceiveMode,
    config::{Optimization, Policy, RuntimeConfig},
};
#[cfg(feature = "zc-rx-shared")]
use parking_lot::Condvar;
use parking_lot::Mutex;
#[cfg(any(
    feature = "zc-rx-large-chunks",
    feature = "zc-rx-shared",
    feature = "zc-observe"
))]
use std::collections::BTreeMap;
#[cfg(feature = "zc-rx-shared")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(feature = "zc-observe")]
use std::{cell::Cell, sync::atomic::AtomicU64};
use std::{
    collections::VecDeque,
    fmt, io,
    marker::PhantomData,
    ptr::{self, NonNull},
    rc::Rc,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

const ZCRX_AREA_SHIFT: u32 = 48;
const ZCRX_AREA_MASK: u64 = !((1u64 << ZCRX_AREA_SHIFT) - 1);
#[cfg(feature = "zc-rx-shared")]
const ZCRX_REG_IMPORT: u32 = 1;
const ZCRX_REG_NODEV: u32 = 2;
#[cfg(feature = "zc-observe")]
const ZCRX_EVENT_ALLOC_FAIL: u32 = 0;
#[cfg(feature = "zc-observe")]
const ZCRX_EVENT_COPY: u32 = 1;
#[cfg(feature = "zc-observe")]
const ZCRX_EVENT_DESC_FLAG_STATS: u32 = 1;
const ZCRX_CTRL_FLUSH_RQ: u32 = 0;
#[cfg(feature = "zc-rx-shared")]
const ZCRX_CTRL_EXPORT: u32 = 1;
#[cfg(feature = "zc-observe")]
const ZCRX_CTRL_ARM_EVENT: u32 = 2;
const MAX_REFILL_ENTRIES: u32 = 1 << 15;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct ZcrxRqe {
    off: u64,
    len: u32,
    pad: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct ZcrxOffsets {
    head: u32,
    tail: u32,
    rqes: u32,
    resv2: u32,
    resv: [u64; 2],
}

#[repr(C)]
#[derive(Default)]
struct ZcrxAreaReg {
    addr: u64,
    len: u64,
    rq_area_token: u64,
    flags: u32,
    dmabuf_fd: u32,
    resv2: [u64; 2],
}

#[repr(C)]
#[derive(Default)]
struct ZcrxEventDesc {
    user_data: u64,
    type_mask: u32,
    flags: u32,
    stats_offset: u64,
    resv2: [u64; 9],
}

#[repr(C)]
#[derive(Default)]
struct ZcrxIfqReg {
    if_idx: u32,
    if_rxq: u32,
    rq_entries: u32,
    flags: u32,
    area_ptr: u64,
    region_ptr: u64,
    offsets: ZcrxOffsets,
    zcrx_id: u32,
    rx_buf_len: u32,
    event_desc: u64,
    resv: [u64; 2],
}

#[repr(C)]
#[derive(Default)]
struct ZcrxCtrl {
    zcrx_id: u32,
    op: u32,
    resv: [u64; 2],
    // All v7.2.7 control union alternatives have the same 48-byte size.
    data: [u32; 12],
}

const _: () = {
    assert!(size_of::<ZcrxRqe>() == 16);
    assert!(size_of::<ZcrxOffsets>() == 32);
    assert!(size_of::<ZcrxAreaReg>() == 48);
    assert!(size_of::<ZcrxEventDesc>() == 96);
    assert!(size_of::<ZcrxIfqReg>() == 96);
    assert!(size_of::<ZcrxCtrl>() == 72);
    assert!(std::mem::offset_of!(ZcrxIfqReg, offsets) == 32);
    assert!(std::mem::offset_of!(ZcrxIfqReg, event_desc) == 72);
};

#[cfg(feature = "zc-rx-shared")]
/// A typed, cloneable export owns the kernel export fd AND its CPU/refill memory.
/// A bare fd is insufficient: the import syscall does not return either mapping.
/// Clones use the same producer lock; never independently wrap the same refill
/// queue and assume its single-producer publication protocol is MPSC-safe.
pub(crate) struct SharedZcrxExport {
    storage: Arc<Storage>,
}

#[cfg(feature = "zc-rx-shared")]
impl fmt::Debug for SharedZcrxExport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedZcrxExport")
            .field("area_bytes", &self.storage.area.len())
            .field("refill_entries", &self.storage.entries)
            .field("chunk_bytes", &self.storage.chunk_bytes)
            .field("receive_mode", &self.storage.mode)
            .finish()
    }
}

#[cfg(feature = "zc-rx-shared")]
impl SharedZcrxExport {
    fn new(storage: Arc<Storage>) -> io::Result<Self> {
        {
            let mut lifecycle = storage.lifecycle.lock();
            if lifecycle.exported.is_none() {
                return Err(unsupported("ZCRX instance was shut down"));
            }
            lifecycle.users += 1;
        }
        Ok(Self { storage })
    }
}

#[cfg(feature = "zc-rx-shared")]
impl Clone for SharedZcrxExport {
    fn clone(&self) -> Self {
        self.storage.lifecycle.lock().users += 1;
        Self {
            storage: self.storage.clone(),
        }
    }
}

#[cfg(feature = "zc-rx-shared")]
impl Drop for SharedZcrxExport {
    fn drop(&mut self) {
        self.storage.detach();
    }
}

#[cfg(feature = "zc-rx-shared")]
/// Cold-start rendezvous for automatically selected workers sharing a queue.
/// It holds weak references, so shared configuration does not keep idle rings
/// or NIC queue registrations alive after the runtime and its leases disappear.
pub(crate) struct ZcrxShared {
    instances: Mutex<BTreeMap<(u32, u32), SharedState>>,
    ready: Condvar,
}

#[cfg(feature = "zc-rx-shared")]
impl ZcrxShared {
    pub fn new() -> Self {
        Self {
            instances: Mutex::new(BTreeMap::new()),
            ready: Condvar::new(),
        }
    }
}

#[cfg(feature = "zc-rx-shared")]
enum SharedState {
    Initializing,
    Ready(Weak<Storage>),
}

/// The kernel writes only unpublished area ranges. Published ranges are owned
/// by external leases; user code never obtains a safe mutable reference here.
struct CpuArea {
    ptr: NonNull<u8>,
    len: usize,
}
unsafe impl Send for CpuArea {}
unsafe impl Sync for CpuArea {}
unsafe impl ExternalMemory for CpuArea {
    fn as_ptr(&self) -> NonNull<u8> {
        self.ptr
    }
    fn len(&self) -> usize {
        self.len
    }
}
impl Drop for CpuArea {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

impl CpuArea {
    fn new(len: usize, chunk: usize, page: usize) -> io::Result<Self> {
        let alignment = page;
        #[cfg(feature = "zc-rx-large-chunks")]
        let (alignment, pmd_bytes) = {
            // PMD-sized THP differs with aarch64's 4K/16K/64K base pages.
            // Registration proves support for the exact requested DMA chunk.
            let pmd_bytes = page
                .checked_mul(page / size_of::<u64>())
                .ok_or_else(|| invalid("ZCRX huge-page alignment overflow"))?;
            (
                if chunk > page {
                    chunk.max(pmd_bytes)
                } else {
                    alignment
                },
                pmd_bytes,
            )
        };
        if len == 0 || !len.is_multiple_of(chunk) || !len.is_multiple_of(page) {
            return Err(invalid(
                "ZCRX area size must be a nonzero multiple of its chunk and page size",
            ));
        }
        let allocation = len
            .checked_add(alignment)
            .ok_or_else(|| invalid("ZCRX area size overflow"))?;
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                allocation,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let base = raw as usize;
        let aligned = (base + alignment - 1) & !(alignment - 1);
        let prefix = aligned - base;
        let suffix = allocation - prefix - len;
        unsafe {
            if prefix != 0 {
                libc::munmap(raw, prefix);
            }
            if suffix != 0 {
                libc::munmap((aligned + len) as *mut libc::c_void, suffix);
            }
        }
        let area = Self {
            ptr: NonNull::new(aligned as *mut u8).expect("mmap returned null"),
            len,
        };
        #[cfg(feature = "zc-rx-large-chunks")]
        if chunk > page {
            unsafe {
                libc::madvise(area.ptr.as_ptr().cast(), len, libc::MADV_HUGEPAGE);
            }
            // Fault privately, before the kernel pins or publishes any ranges.
            for offset in (0..len).step_by(page) {
                unsafe {
                    area.ptr.as_ptr().add(offset).write_volatile(0);
                }
            }
            // A failed collapse is not a smaller-chunk fallback: registration
            // below still requests the exact large rx_buf_len and must succeed.
            // This also permits mTHP/contiguous normal pages on a machine whose
            // PMD huge pages are unavailable or larger than the area budget.
            if len >= pmd_bytes {
                const MADV_COLLAPSE: i32 = 25;
                unsafe {
                    libc::madvise(area.ptr.as_ptr().cast(), len, MADV_COLLAPSE);
                }
            }
        }
        Ok(area)
    }
}

struct RefillMemory(Mapping);
// Unlike SQ/CQ mappings, this memory is explicitly shared: producers use Storage's
// mutex and release tail stores; the kernel uses acquire tail/release head.
unsafe impl Send for RefillMemory {}
unsafe impl Sync for RefillMemory {}
unsafe impl ExternalMemory for RefillMemory {
    fn as_ptr(&self) -> NonNull<u8> {
        NonNull::new(self.0.as_ptr()).expect("mapped refill memory")
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(feature = "zc-rx-shared")]
struct Lifecycle {
    exported: Option<OwnedFd>,
    // Counts live receiving backends and typed export handles, not user leases.
    users: usize,
}

struct Producer {
    pending: VecDeque<ReturnToken>,
    capacity: usize,
}

struct Storage {
    // Close the export before unmapping. Live imported rings additionally pin
    // these pages; Arc leases keep the userspace addresses mapped as needed.
    #[cfg(feature = "zc-rx-shared")]
    lifecycle: Mutex<Lifecycle>,
    area: Arc<dyn ExternalMemory>,
    refill: Arc<dyn ExternalMemory>,
    offsets: ZcrxOffsets,
    entries: u32,
    area_token: u64,
    chunk_bytes: u32,
    #[cfg(feature = "zc-rx-large-chunks")]
    large_chunks: bool,
    #[cfg(feature = "zc-rx-shared")]
    shared: bool,
    mode: ReceiveMode,
    #[cfg(feature = "zc-observe")]
    stats_offset: Option<usize>,
    #[cfg(feature = "zc-observe")]
    allocation_events: AtomicU64,
    retired: AtomicBool,
    producer: Mutex<Producer>,
    notifiers: Mutex<Vec<Weak<Notifier>>>,
    max_notifiers: usize,
}

impl Storage {
    fn detach(&self) {
        #[cfg(feature = "zc-rx-shared")]
        let exported = {
            let mut lifecycle = self.lifecycle.lock();
            lifecycle.users -= 1;
            if lifecycle.users != 0 {
                return;
            }
            // No new receives/imports are allowed. Last ring unregister scrubs
            // refs which were not returned. Late leases never touch dead rings.
            self.retired.store(true, Ordering::Release);
            lifecycle.exported.take()
        };
        // Closing the last kernel export can stop a NIC queue: no userspace
        // lock may be held across that teardown.
        #[cfg(feature = "zc-rx-shared")]
        drop(exported);
        #[cfg(not(feature = "zc-rx-shared"))]
        self.retired.store(true, Ordering::Release);
        self.producer.lock().pending.clear();
    }

    fn publish(&self, token: ReturnToken) -> bool {
        let tail = self.tail().load(Ordering::Relaxed);
        let head = self.head().load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.entries {
            return false;
        }
        let index = (tail & (self.entries - 1)) as usize;
        unsafe {
            self.refill
                .as_ptr()
                .as_ptr()
                .add(self.offsets.rqes as usize)
                .cast::<ZcrxRqe>()
                .add(index)
                .write(ZcrxRqe {
                    off: token.offset,
                    len: token.length,
                    pad: 0,
                });
        }
        self.tail().store(tail.wrapping_add(1), Ordering::Release);
        true
    }

    fn head(&self) -> &AtomicU32 {
        unsafe {
            &*self
                .refill
                .as_ptr()
                .as_ptr()
                .add(self.offsets.head as usize)
                .cast()
        }
    }
    fn tail(&self) -> &AtomicU32 {
        unsafe {
            &*self
                .refill
                .as_ptr()
                .as_ptr()
                .add(self.offsets.tail as usize)
                .cast()
        }
    }
    fn pending(&self) -> bool {
        self.head().load(Ordering::Acquire) != self.tail().load(Ordering::Acquire)
    }
    #[cfg(feature = "zc-observe")]
    fn shared_stats(&self) -> ZcStats {
        let Some(offset) = self.stats_offset else {
            return ZcStats::default();
        };
        // Kernel WRITE_ONCE u64 counters are naturally aligned on both supported
        // 64-bit architectures. Each field is a snapshot, not an atomic pair.
        let stats = unsafe {
            self.refill
                .as_ptr()
                .as_ptr()
                .add(offset)
                .cast::<AtomicU64>()
        };
        ZcStats {
            rx_copy_events: unsafe { &*stats }.load(Ordering::Relaxed),
            rx_copied_bytes: unsafe { &*stats.add(1) }.load(Ordering::Relaxed),
            rx_allocation_failures: self.allocation_events.load(Ordering::Relaxed),
            ..ZcStats::default()
        }
    }
}

/// Return callbacks never issue a syscall on a foreign SINGLE_ISSUER ring.
/// Queue publication is serialized and excess returns use bounded cold storage;
/// the next worker poll flushes through that worker's own imported ring/id.
struct RefillPort {
    id: u32,
    storage: Arc<Storage>,
    error: AtomicU32,
    #[cfg(feature = "zc-rx-shared")]
    live: AtomicBool,
}

impl RefillPort {
    fn check_error(&self) -> io::Result<()> {
        let error = self.error.swap(0, Ordering::AcqRel);
        if error != 0 {
            return Err(io::Error::from_raw_os_error(error as i32));
        }
        Ok(())
    }

    fn recycle_checked(&self, token: ReturnToken) -> io::Result<bool> {
        let raw_offset = token.offset & !ZCRX_AREA_MASK;
        if token.tag != (self.storage.area_token >> ZCRX_AREA_SHIFT) as u32
            || token.offset & ZCRX_AREA_MASK != self.storage.area_token
            || token.length == 0
            || raw_offset
                .checked_add(u64::from(token.length))
                .is_none_or(|end| end > self.storage.area.len() as u64)
        {
            return Err(invalid_data("invalid ZCRX return token"));
        }
        if self.storage.retired.load(Ordering::Acquire) {
            return Ok(true);
        }
        let mut producer = self.storage.producer.lock();
        if self.storage.retired.load(Ordering::Acquire) {
            return Ok(true);
        }
        if producer.pending.is_empty() && self.storage.publish(token) {
            return Ok(true);
        }
        if producer.pending.len() == producer.capacity {
            return Ok(false);
        }
        producer.pending.push_back(token);
        Ok(true)
    }
}

impl Recycle for RefillPort {
    fn recycle(&self, token: ReturnToken) -> bool {
        match self.recycle_checked(token) {
            Ok(accepted) => {
                // An escaped lease from a stopped worker cannot rely on that
                // worker's pool waker. Wake surviving importers to flush NODEV
                // and copy-fallback returns; no callback keeps a worker alive.
                #[cfg(feature = "zc-rx-shared")]
                if !self.live.load(Ordering::Acquire)
                    && !self.storage.retired.load(Ordering::Acquire)
                {
                    for notifier in self
                        .storage
                        .notifiers
                        .lock()
                        .iter()
                        .filter_map(Weak::upgrade)
                    {
                        notifier.notify();
                    }
                }
                accepted
            }
            Err(error) => {
                self.error.store(
                    error.raw_os_error().unwrap_or(libc::EIO) as u32,
                    Ordering::Release,
                );
                false
            }
        }
    }
}

pub(crate) struct Zcrx {
    port: Arc<RefillPort>,
    attached: bool,
    #[cfg(feature = "zc-observe")]
    events_owner: bool,
    #[cfg(feature = "zc-observe")]
    event_user_data: u64,
    #[cfg(feature = "zc-observe")]
    observe: bool,
    #[cfg(feature = "mixed-cqe")]
    mixed_cqe: bool,
    #[cfg(feature = "zc-observe")]
    local_stats: Cell<ZcStats>,
    #[cfg(any(
        feature = "zc-rx-large-chunks",
        feature = "zc-rx-shared",
        feature = "zc-observe"
    ))]
    fallbacks: BTreeMap<Optimization, String>,
    _local: PhantomData<Rc<()>>,
}

impl Zcrx {
    pub fn new(
        ring: &Ring,
        config: &RuntimeConfig,
        worker: usize,
        #[cfg(feature = "zc-rx-shared")] shared: &ZcrxShared,
        #[cfg(feature = "zc-observe")] event_user_data: u64,
    ) -> io::Result<Self> {
        const CHILDREN: [Optimization; 3] = [
            Optimization::ZcRxLargeChunks,
            Optimization::ZcObserve,
            Optimization::ZcRxShared,
        ];
        let automatic = CHILDREN
            .iter()
            .enumerate()
            .fold(0u8, |mask, (bit, &child)| {
                mask | if config.policy(child) == Policy::Auto {
                    1 << bit
                } else {
                    0
                }
            });
        let mut last_error = None;
        #[cfg(any(
            feature = "zc-rx-large-chunks",
            feature = "zc-rx-shared",
            feature = "zc-observe"
        ))]
        let mut first_failure = None;
        // Try minimum changes first, preserving independent requested children.
        // Every failed IFQ registration is rolled back by the kernel. A failure
        // after registration is different and requires whole-ring teardown.
        for disabled in [0u8, 1, 2, 4, 3, 5, 6, 7] {
            if disabled & !automatic != 0 {
                continue;
            }
            let mut effective;
            let candidate = if disabled == 0 {
                config
            } else {
                effective = config.clone();
                for (bit, &child) in CHILDREN.iter().enumerate() {
                    if disabled & (1 << bit) != 0 {
                        effective.optimizations.insert(child, Policy::Off);
                        if child == Optimization::ZcRxLargeChunks {
                            effective.linux.zcrx.chunk_bytes = 0;
                        }
                    }
                }
                &effective
            };
            match Self::new_effective(
                ring,
                candidate,
                worker,
                #[cfg(feature = "zc-rx-shared")]
                shared,
                #[cfg(feature = "zc-observe")]
                event_user_data,
            ) {
                Ok(instance) => {
                    #[cfg(any(
                        feature = "zc-rx-large-chunks",
                        feature = "zc-rx-shared",
                        feature = "zc-observe"
                    ))]
                    let mut instance = instance;
                    #[cfg(any(
                        feature = "zc-rx-large-chunks",
                        feature = "zc-rx-shared",
                        feature = "zc-observe"
                    ))]
                    for (bit, &child) in CHILDREN.iter().enumerate() {
                        if disabled & (1 << bit) != 0 {
                            instance.fallbacks.insert(
                                child,
                                format!(
                                    "Auto disabled {child}: {}",
                                    first_failure
                                        .as_deref()
                                        .unwrap_or("startup capability unavailable")
                                ),
                            );
                        }
                    }
                    return Ok(instance);
                }
                Err(error) => {
                    if requires_ring_close(&error) {
                        return Err(error);
                    }
                    #[cfg(any(
                        feature = "zc-rx-large-chunks",
                        feature = "zc-rx-shared",
                        feature = "zc-observe"
                    ))]
                    if first_failure.is_none() {
                        first_failure = Some(error.to_string());
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.expect("initial ZCRX candidate is always attempted"))
    }

    fn new_effective(
        ring: &Ring,
        config: &RuntimeConfig,
        worker: usize,
        #[cfg(feature = "zc-rx-shared")] shared: &ZcrxShared,
        #[cfg(feature = "zc-observe")] event_user_data: u64,
    ) -> io::Result<Self> {
        check_ring(ring)?;
        let nodev = config.requested(Optimization::ZcRxNodev);
        if !nodev && !config.requested(Optimization::ZcRx) {
            return Err(unsupported(
                "a compiled, explicitly selected ZCRX path is required",
            ));
        }
        if nodev && !cfg!(feature = "zc-rx-nodev") {
            return Err(unsupported("ZCRX NODEV validation was not compiled"));
        }
        let sharing = config.requested(Optimization::ZcRxShared);
        if sharing && !cfg!(feature = "zc-rx-shared") {
            return Err(unsupported("ZCRX shared instances were not compiled"));
        }
        if worker >= config.workers {
            return Err(invalid("ZCRX worker index exceeds worker count"));
        }
        let queues = &config.linux.zcrx.queues;
        let (if_index, queue_index) = if cfg!(feature = "zc-rx-nodev") && nodev {
            if !queues.is_empty() {
                return Err(invalid("NODEV validation cannot bind a hardware queue"));
            }
            (0, 0)
        } else {
            if queues.is_empty() {
                return Err(invalid(
                    "hardware ZCRX needs deployment-preconfigured receive queues",
                ));
            }
            if !sharing && queues.len() < config.workers {
                return Err(invalid(
                    "unshared ZCRX requires a distinct preconfigured queue for every worker",
                ));
            }
            let selected = queues[worker % queues.len()];
            if selected.interface_index == 0 || selected.queue_index == u32::MAX {
                return Err(invalid(
                    "invalid deployment-preconfigured ZCRX interface or queue",
                ));
            }
            if queues
                .iter()
                .enumerate()
                .any(|(index, queue)| queues[..index].contains(queue))
            {
                return Err(invalid(
                    "ZCRX configuration contains a duplicate receive queue",
                ));
            }
            (selected.interface_index, selected.queue_index)
        };
        #[cfg(feature = "zc-rx-shared")]
        if sharing {
            let key = (if_index, queue_index);
            loop {
                let mut instances = shared.instances.lock();
                match instances.get(&key) {
                    Some(SharedState::Initializing) => {
                        shared.ready.wait(&mut instances);
                        continue;
                    }
                    Some(SharedState::Ready(weak)) => {
                        if let Some(storage) = weak
                            .upgrade()
                            .filter(|storage| !storage.retired.load(Ordering::Acquire))
                        {
                            let export = SharedZcrxExport::new(storage)?;
                            drop(instances);
                            return Self::import(
                                ring,
                                &export,
                                #[cfg(feature = "zc-observe")]
                                event_user_data,
                                #[cfg(feature = "zc-observe")]
                                config.requested(Optimization::ZcObserve),
                            );
                        }
                    }
                    None => {}
                }
                instances.insert(key, SharedState::Initializing);
                drop(instances);
                let sharers = config
                    .workers
                    .div_ceil(if nodev { 1 } else { queues.len() });
                let result = Self::register(
                    ring,
                    config,
                    if_index,
                    queue_index,
                    nodev,
                    sharers,
                    #[cfg(feature = "zc-observe")]
                    event_user_data,
                );
                let mut instances = shared.instances.lock();
                match &result {
                    Ok(instance) if instance.port.storage.shared => {
                        instances.insert(
                            key,
                            SharedState::Ready(Arc::downgrade(&instance.port.storage)),
                        );
                    }
                    _ => {
                        instances.remove(&key);
                    }
                }
                drop(instances);
                shared.ready.notify_all();
                return result;
            }
        }
        Self::register(
            ring,
            config,
            if_index,
            queue_index,
            nodev,
            1,
            #[cfg(feature = "zc-observe")]
            event_user_data,
        )
    }

    fn register(
        ring: &Ring,
        config: &RuntimeConfig,
        if_index: u32,
        queue_index: u32,
        nodev: bool,
        sharers: usize,
        #[cfg(feature = "zc-observe")] event_user_data: u64,
    ) -> io::Result<Self> {
        let options = &config.linux.zcrx;
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 || !(page as usize).is_power_of_two() {
            return Err(unsupported("unsupported system page size for ZCRX"));
        }
        let page = page as usize;
        let large = config.requested(Optimization::ZcRxLargeChunks);
        if large && !cfg!(feature = "zc-rx-large-chunks") {
            return Err(unsupported("large ZCRX chunks were not compiled"));
        }
        let requested_chunk = if options.chunk_bytes != 0 {
            options.chunk_bytes as usize
        } else if cfg!(feature = "zc-rx-large-chunks") && large {
            page.checked_mul(16)
                .ok_or_else(|| invalid("ZCRX chunk size overflow"))?
        } else {
            page
        };
        if !requested_chunk.is_power_of_two()
            || requested_chunk < page
            || requested_chunk > u32::MAX as usize
        {
            return Err(invalid(
                "ZCRX chunk size must be a power of two at least one system page",
            ));
        }
        if large != (requested_chunk > page) {
            return Err(invalid(
                "large ZCRX chunk selection must match the configured chunk size",
            ));
        }
        if nodev && requested_chunk != page {
            return Err(unsupported(
                "Linux 7.2.7 NODEV only supports system-page-sized chunks",
            ));
        }
        if options.area_bytes > (1usize << 40) {
            return Err(invalid(
                "ZCRX CPU area exceeds the kernel's one-terabyte registration bound",
            ));
        }
        if !options.refill_entries.is_power_of_two() || options.refill_entries > MAX_REFILL_ENTRIES
        {
            return Err(invalid(
                "ZCRX refill entries must be a power of two in 1..=32768",
            ));
        }
        #[cfg(feature = "zc-observe")]
        let observe = config.requested(Optimization::ZcObserve) && cfg!(feature = "zc-observe");
        let area: Arc<dyn ExternalMemory> =
            Arc::new(CpuArea::new(options.area_bytes, requested_chunk, page)?);
        // Kernel returns the real offsets. A page reserved for its cache-line
        // aligned header avoids assuming x86's L1_CACHE_BYTES on aarch64.
        let entries_bytes = options.refill_entries as usize * size_of::<ZcrxRqe>();
        let queue_end = page
            .checked_add(entries_bytes)
            .ok_or_else(|| invalid("ZCRX refill size overflow"))?;
        let refill_bytes = align_up(queue_end, page)?;
        #[cfg(feature = "zc-observe")]
        let refill_bytes = if observe {
            align_up(
                queue_end
                    .checked_add(16)
                    .ok_or_else(|| invalid("ZCRX stats size overflow"))?,
                page,
            )?
        } else {
            refill_bytes
        };
        let refill: Arc<dyn ExternalMemory> =
            Arc::new(RefillMemory(Mapping::anonymous(refill_bytes)?));
        let pending_capacity = config
            .limits
            .pool
            .max_leases
            .checked_mul(sharers)
            .ok_or_else(|| invalid("ZCRX shared return budget overflow"))?;
        let mut pending = VecDeque::new();
        pending.try_reserve_exact(pending_capacity).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot reserve bounded ZCRX return metadata",
            )
        })?;
        let mut area_reg = ZcrxAreaReg {
            addr: area.as_ptr().as_ptr() as u64,
            len: area.len() as u64,
            ..ZcrxAreaReg::default()
        };
        let mut region = RegionDesc {
            user_addr: refill.as_ptr().as_ptr() as u64,
            size: refill.len() as u64,
            flags: IORING_MEM_REGION_TYPE_USER,
            ..RegionDesc::default()
        };
        #[cfg(feature = "zc-observe")]
        let mut events = ZcrxEventDesc {
            user_data: event_user_data,
            type_mask: (1 << ZCRX_EVENT_ALLOC_FAIL) | (1 << ZCRX_EVENT_COPY),
            flags: ZCRX_EVENT_DESC_FLAG_STATS,
            stats_offset: queue_end as u64,
            ..ZcrxEventDesc::default()
        };
        let mut registration = ZcrxIfqReg {
            if_idx: if_index,
            if_rxq: queue_index,
            rq_entries: options.refill_entries,
            flags: if cfg!(feature = "zc-rx-nodev") && nodev {
                ZCRX_REG_NODEV
            } else {
                0
            },
            area_ptr: ptr::from_mut(&mut area_reg) as u64,
            region_ptr: ptr::from_mut(&mut region) as u64,
            rx_buf_len: requested_chunk as u32,
            #[cfg(feature = "zc-observe")]
            event_desc: if observe {
                ptr::from_mut(&mut events) as u64
            } else {
                0
            },
            ..ZcrxIfqReg::default()
        };
        unsafe {
            ring.register(
                IORING_REGISTER_ZCRX_IFQ,
                ptr::from_mut(&mut registration).cast(),
                1,
            )?;
        }
        #[cfg(feature = "zc-observe")]
        let stats_offset = observe.then_some(queue_end);
        #[cfg(not(feature = "zc-observe"))]
        let stats_offset = None;
        validate_layout(&registration, refill.len(), stats_offset).map_err(post_registration)?;
        if registration.rx_buf_len != requested_chunk as u32
            || area_reg.rq_area_token & !ZCRX_AREA_MASK != 0
        {
            return Err(post_registration(invalid_data(
                "kernel returned an incompatible ZCRX chunk or area token",
            )));
        }
        #[cfg(any(
            feature = "zc-rx-large-chunks",
            feature = "zc-rx-shared",
            feature = "zc-observe"
        ))]
        let fallbacks = BTreeMap::new();
        #[cfg(feature = "zc-rx-shared")]
        let mut fallbacks = fallbacks;
        #[cfg(feature = "zc-rx-shared")]
        let mut sharing = config.requested(Optimization::ZcRxShared);
        #[cfg(feature = "zc-rx-shared")]
        let exported = if sharing {
            let mut export = ZcrxCtrl {
                zcrx_id: registration.zcrx_id,
                op: ZCRX_CTRL_EXPORT,
                ..ZcrxCtrl::default()
            };
            match unsafe {
                ring.register(
                    IORING_REGISTER_ZCRX_CTRL,
                    ptr::from_mut(&mut export).cast(),
                    0,
                )
            } {
                Ok(_) => {
                    if export.data[0] > i32::MAX as u32 {
                        return Err(post_registration(invalid_data(
                            "kernel returned an invalid ZCRX export fd",
                        )));
                    }
                    Some(unsafe { OwnedFd::from_raw_fd(export.data[0] as i32) })
                }
                Err(error)
                    if config.policy(Optimization::ZcRxShared) == Policy::Auto
                        && (nodev || options.queues.len() >= config.workers) =>
                {
                    // The IFQ itself is valid. With independent queues (or
                    // NODEV areas), sharing can be omitted without re-registering
                    // or changing the required receive path.
                    sharing = false;
                    fallbacks.insert(Optimization::ZcRxShared, error.to_string());
                    None
                }
                Err(error) => return Err(post_registration(error)),
            }
        } else {
            None
        };
        let storage = Arc::new(Storage {
            #[cfg(feature = "zc-rx-shared")]
            lifecycle: Mutex::new(Lifecycle { exported, users: 1 }),
            area,
            refill,
            offsets: registration.offsets,
            entries: registration.rq_entries,
            area_token: area_reg.rq_area_token,
            chunk_bytes: registration.rx_buf_len,
            #[cfg(feature = "zc-rx-large-chunks")]
            large_chunks: large,
            #[cfg(feature = "zc-rx-shared")]
            shared: sharing,
            mode: if cfg!(feature = "zc-rx-nodev") && nodev {
                ReceiveMode::NodevCopied
            } else {
                ReceiveMode::HardwareZeroCopy
            },
            #[cfg(feature = "zc-observe")]
            stats_offset,
            #[cfg(feature = "zc-observe")]
            allocation_events: AtomicU64::new(0),
            retired: AtomicBool::new(false),
            producer: Mutex::new(Producer {
                pending,
                capacity: pending_capacity,
            }),
            notifiers: Mutex::new(Vec::with_capacity(sharers)),
            max_notifiers: sharers,
        });
        Ok(Self {
            port: Arc::new(RefillPort {
                id: registration.zcrx_id,
                storage,
                error: AtomicU32::new(0),
                #[cfg(feature = "zc-rx-shared")]
                live: AtomicBool::new(true),
            }),
            attached: true,
            #[cfg(feature = "zc-observe")]
            events_owner: observe,
            #[cfg(feature = "zc-observe")]
            event_user_data,
            #[cfg(feature = "zc-observe")]
            observe,
            #[cfg(feature = "mixed-cqe")]
            mixed_cqe: ring.flags() & IORING_SETUP_CQE_MIXED != 0,
            #[cfg(feature = "zc-observe")]
            local_stats: Cell::new(ZcStats::default()),
            #[cfg(any(
                feature = "zc-rx-large-chunks",
                feature = "zc-rx-shared",
                feature = "zc-observe"
            ))]
            fallbacks,
            _local: PhantomData,
        })
    }

    #[cfg(feature = "zc-rx-shared")]
    pub fn import(
        ring: &Ring,
        export: &SharedZcrxExport,
        #[cfg(feature = "zc-observe")] event_user_data: u64,
        #[cfg(feature = "zc-observe")] observe: bool,
    ) -> io::Result<Self> {
        check_ring(ring)?;
        let storage = &export.storage;
        #[cfg(feature = "zc-observe")]
        if observe && storage.stats_offset.is_none() {
            return Err(unsupported(
                "the exported ZCRX instance has no copy-event/statistics registration",
            ));
        }
        // The typed export retains one lifecycle reference, keeping this fd
        // valid without holding a userspace lock across registration.
        let fd = storage
            .lifecycle
            .lock()
            .exported
            .as_ref()
            .ok_or_else(|| unsupported("cannot import a retired ZCRX instance"))?
            .as_raw_fd();
        // The import ABI requires all original area/region/rq fields be zero.
        // Returned offsets refer to the SAME refill queue, not a new SPSC ring.
        let mut registration = ZcrxIfqReg {
            if_idx: fd as u32,
            flags: ZCRX_REG_IMPORT,
            ..ZcrxIfqReg::default()
        };
        unsafe {
            ring.register(
                IORING_REGISTER_ZCRX_IFQ,
                ptr::from_mut(&mut registration).cast(),
                1,
            )?;
        }
        if registration.offsets.head != storage.offsets.head
            || registration.offsets.tail != storage.offsets.tail
            || registration.offsets.rqes != storage.offsets.rqes
        {
            return Err(post_registration(invalid_data(
                "ZCRX import changed the shared refill layout",
            )));
        }
        storage.lifecycle.lock().users += 1;
        Ok(Self {
            port: Arc::new(RefillPort {
                id: registration.zcrx_id,
                storage: storage.clone(),
                error: AtomicU32::new(0),
                live: AtomicBool::new(true),
            }),
            attached: true,
            #[cfg(feature = "zc-observe")]
            events_owner: false,
            #[cfg(feature = "zc-observe")]
            event_user_data,
            #[cfg(feature = "zc-observe")]
            observe,
            #[cfg(feature = "mixed-cqe")]
            mixed_cqe: ring.flags() & IORING_SETUP_CQE_MIXED != 0,
            #[cfg(feature = "zc-observe")]
            local_stats: Cell::new(ZcStats::default()),
            fallbacks: BTreeMap::new(),
            _local: PhantomData,
        })
    }

    #[cfg(any(
        feature = "zc-rx-large-chunks",
        feature = "zc-rx-shared",
        feature = "zc-observe"
    ))]
    /// Report only child paths actually initialized for this instance. In
    /// particular, an Auto child can be inactive while required RX stays active.
    pub fn supports(&self, optimization: Optimization) -> Result<(), String> {
        let active = match optimization {
            Optimization::ZcRx => self.mode() == ReceiveMode::HardwareZeroCopy,
            #[cfg(feature = "zc-rx-nodev")]
            Optimization::ZcRxNodev => self.mode() == ReceiveMode::NodevCopied,
            #[cfg(feature = "zc-rx-large-chunks")]
            Optimization::ZcRxLargeChunks => self.port.storage.large_chunks,
            #[cfg(feature = "zc-rx-shared")]
            Optimization::ZcRxShared => self.port.storage.shared,
            #[cfg(feature = "zc-observe")]
            Optimization::ZcObserve => self.observe,
            _ => false,
        };
        if active {
            return Ok(());
        }
        Err(self
            .fallbacks
            .get(&optimization)
            .cloned()
            .unwrap_or_else(|| {
                format!("{optimization} is not active in this initialized ZCRX instance")
            }))
    }

    pub fn set_notifier(&self, notifier: &Arc<Notifier>) -> io::Result<()> {
        let weak = Arc::downgrade(notifier);
        let mut notifiers = self.port.storage.notifiers.lock();
        notifiers.retain(|notifier| notifier.strong_count() != 0);
        if notifiers.iter().any(|existing| existing.ptr_eq(&weak)) {
            return Ok(());
        }
        if notifiers.len() == self.port.storage.max_notifiers {
            return Err(invalid(
                "ZCRX imports exceed the configured shared-worker budget",
            ));
        }
        notifiers.push(weak);
        Ok(())
    }

    pub fn mode(&self) -> ReceiveMode {
        self.port.storage.mode
    }

    #[cfg(feature = "zc-tx-fixed")]
    /// Include this stable area in the ring's registered send regions to permit
    /// fixed SEND_ZC of received leases without copying into the normal pool.
    /// The caller retains this Zcrx until the ring's buffer registration ends.
    pub fn memory_region(&self, id: u32) -> MemoryRegion {
        MemoryRegion {
            ptr: self.port.storage.area.as_ptr(),
            len: self.port.storage.area.len(),
            id,
        }
    }

    /// RECV_ZC is TCP-only in this kernel. The core chooses ordinary RECVMSG for
    /// UDP. Zero len means unlimited multishot, so its terminal zero is true EOF,
    /// not exhaustion of a finite byte budget.
    pub fn prepare_recv(&self, sqe: &mut Sqe) -> io::Result<()> {
        if !self.attached {
            return Err(unsupported("ZCRX is shutting down"));
        }
        if sqe.flags & (IOSQE_BUFFER_SELECT | IOSQE_CQE_SKIP_SUCCESS) != 0 {
            return Err(invalid(
                "ZCRX cannot use provided buffer selection or skip terminal CQEs",
            ));
        }
        sqe.opcode = IORING_OP_RECV_ZC;
        sqe.ioprio = (sqe.ioprio & IORING_RECVSEND_POLL_FIRST) | IORING_RECV_MULTISHOT;
        sqe.addr = 0;
        sqe.off = 0;
        sqe.addr3 = 0;
        sqe.len = 0;
        sqe.op_flags = 0;
        sqe.buf_index = 0;
        sqe.file_index = self.port.id;
        sqe.pad2 = 0;
        Ok(())
    }

    fn completion_token(&self, cqe: &Cqe) -> io::Result<Option<ReturnToken>> {
        if cqe.res < 0 {
            return Err(io::Error::from_raw_os_error(cqe.res.saturating_neg()));
        }
        if cqe.res == 0 {
            if cqe.flags & IORING_CQE_F_MORE != 0 {
                return Err(invalid_data(
                    "ZCRX returned an empty nonterminal data fragment",
                ));
            }
            return Ok(None);
        }
        let invalid_mixed = false;
        #[cfg(feature = "mixed-cqe")]
        let invalid_mixed = invalid_mixed || (self.mixed_cqe && cqe.flags & IORING_CQE_F_32 == 0);
        if cqe.flags & IORING_CQE_F_MORE == 0
            || cqe.flags & IORING_CQE_F_NOTIF != 0
            || invalid_mixed
            || cqe.extra[1] != 0
        {
            return Err(invalid_data("invalid extended ZCRX data completion"));
        }
        let storage = &self.port.storage;
        let offset = cqe.extra[0] & !ZCRX_AREA_MASK;
        let length = cqe.res as u64;
        if cqe.extra[0] & ZCRX_AREA_MASK != storage.area_token
            || offset
                .checked_add(length)
                .is_none_or(|end| end > storage.area.len() as u64)
            || (offset & (u64::from(storage.chunk_bytes) - 1)) + length
                > u64::from(storage.chunk_bytes)
        {
            return Err(invalid_data(
                "ZCRX completion is outside its registered area or chunk",
            ));
        }
        Ok(Some(ReturnToken {
            offset: cqe.extra[0],
            length: cqe.res as u32,
            tag: (storage.area_token >> ZCRX_AREA_SHIFT) as u32,
        }))
    }

    /// A failed lease acquisition leaves the CQE's one kernel user reference
    /// untouched. The caller must retain the exact CQE and retry; dropping it
    /// would lose TCP bytes and leak a kernel user reference.
    /// Supply each native data CQE until exactly one call to complete or discard
    /// accepts it; a successfully accepted CQE must never be submitted again.
    pub fn complete(&self, cqe: &Cqe, pool: &BufferPool) -> io::Result<Option<ReadBuf>> {
        let Some(token) = self.completion_token(cqe)? else {
            return Ok(None);
        };
        let storage = &self.port.storage;
        let offset = token.offset & !ZCRX_AREA_MASK;
        let length = token.length as usize;
        let recycler: Arc<dyn Recycle> = self.port.clone();
        let data = unsafe {
            pool.lease_external(
                storage.area.clone(),
                offset as usize,
                length,
                recycler,
                token,
            )?
        };
        #[cfg(feature = "zc-observe")]
        if self.observe {
            let mut stats = self.local_stats.get();
            stats.rx_completions = stats.rx_completions.saturating_add(1);
            stats.rx_bytes = stats.rx_bytes.saturating_add(length as u64);
            self.local_stats.set(stats);
        }
        Ok(Some(data))
    }

    /// Release a consumed CQE during socket/runtime shutdown without needing a
    /// spare lease slot. This is not cancellation of an application receive
    /// waiter: the core may call it only when it has chosen to close the logical
    /// receive operation. On WouldBlock, the caller still owns the exact CQE and
    /// must retain it until a refill flush permits retry.
    pub fn discard(&self, cqe: &Cqe) -> io::Result<()> {
        let Some(token) = self.completion_token(cqe)? else {
            return Ok(());
        };
        if self.port.recycle(token) {
            return Ok(());
        }
        self.port.check_error()?;
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "ZCRX return queue is full during shutdown",
        ))
    }

    pub fn flush_refills(&self, ring: &Ring) -> io::Result<()> {
        self.port.check_error()?;
        if !self.attached || self.port.storage.retired.load(Ordering::Acquire) {
            return Ok(());
        }
        let storage = &self.port.storage;
        let rounds = storage
            .producer
            .lock()
            .capacity
            .div_ceil(storage.entries as usize)
            + 1;
        // Serialize publication only. The kernel also serializes its consumers;
        // no producer lock is needed across the potentially sleeping syscall.
        for _ in 0..rounds {
            {
                let mut producer = storage.producer.lock();
                while let Some(token) = producer.pending.front().copied() {
                    if !storage.publish(token) {
                        break;
                    }
                    producer.pending.pop_front();
                }
            }
            if !storage.pending() {
                return Ok(());
            }
            let before = storage.head().load(Ordering::Acquire);
            let mut control = ZcrxCtrl {
                zcrx_id: self.port.id,
                op: ZCRX_CTRL_FLUSH_RQ,
                ..ZcrxCtrl::default()
            };
            unsafe {
                ring.register(
                    IORING_REGISTER_ZCRX_CTRL,
                    ptr::from_mut(&mut control).cast(),
                    0,
                )?;
            }
            if storage.producer.lock().pending.is_empty() {
                return Ok(());
            }
            if storage.head().load(Ordering::Acquire) == before {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "kernel made no progress flushing ZCRX returns",
                ));
            }
        }
        // Concurrent producers cannot turn one processing round into an
        // unbounded spin. Leave their exact tokens queued and schedule progress.
        for notifier in storage.notifiers.lock().iter().filter_map(Weak::upgrade) {
            notifier.notify();
        }
        Ok(())
    }

    #[cfg(feature = "zc-observe")]
    /// Copy/ALLOC_FAIL events are shared-instance, one-shot notifications.
    /// Re-arm exactly the type which fired; stats retain cumulative kernel data.
    pub fn handle_event(&self, ring: &Ring, cqe: &Cqe) -> io::Result<bool> {
        if cqe.user_data != self.event_user_data {
            return Ok(false);
        }
        if !self.events_owner {
            return Err(invalid_data("ZCRX event delivered to a non-owner ring"));
        }
        if cqe.flags & (IORING_CQE_F_MORE | IORING_CQE_F_NOTIF) != 0
            || !matches!(cqe.res as u32, ZCRX_EVENT_ALLOC_FAIL | ZCRX_EVENT_COPY)
        {
            return Err(invalid_data("invalid ZCRX shared event completion"));
        }
        if cqe.res as u32 == ZCRX_EVENT_ALLOC_FAIL {
            self.port
                .storage
                .allocation_events
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut control = ZcrxCtrl {
            zcrx_id: self.port.id,
            op: ZCRX_CTRL_ARM_EVENT,
            ..ZcrxCtrl::default()
        };
        control.data[0] = cqe.res as u32;
        unsafe {
            ring.register(
                IORING_REGISTER_ZCRX_CTRL,
                ptr::from_mut(&mut control).cast(),
                0,
            )?;
        }
        Ok(true)
    }

    #[cfg(feature = "zc-observe")]
    /// Receive CQE/byte counters are local to this Driver. Copy and allocation
    /// counters are one cumulative kernel-instance snapshot, including traffic
    /// from every importer when sharing is enabled. Never sum such snapshots
    /// from multiple workers attached to the same instance.
    pub fn stats(&self) -> ZcStats {
        if !self.observe {
            return ZcStats::default();
        }
        let mut stats = self.local_stats.get();
        stats.accumulate(self.port.storage.shared_stats());
        stats
    }

    /// Call only after receive requests are canceled/drained. The core must
    /// subsequently close the ring (the only ZCRX unregister ABI), then flush its
    /// pool's pending returns. Surviving read-only leases retain the CPU mapping.
    pub fn shutdown(&mut self, ring: &Ring) -> io::Result<()> {
        if !self.attached {
            return Ok(());
        }
        let result = self.flush_refills(ring);
        #[cfg(feature = "zc-rx-shared")]
        self.port.live.store(false, Ordering::Release);
        self.port.storage.detach();
        self.attached = false;
        result
    }
}

impl Drop for Zcrx {
    fn drop(&mut self) {
        if self.attached {
            #[cfg(feature = "zc-rx-shared")]
            self.port.live.store(false, Ordering::Release);
            self.port.storage.detach();
            self.attached = false;
        }
    }
}

#[derive(Debug)]
struct RegisteredInitializationError(io::Error);

impl fmt::Display for RegisteredInitializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ZCRX registration must be released by closing its ring: {}",
            self.0
        )
    }
}
impl std::error::Error for RegisteredInitializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

fn post_registration(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), RegisteredInitializationError(error))
}

/// A same-ring Auto fallback would retain a registered hardware queue. The
/// caller must destroy/rebuild the whole ring, or return initialization failure.
pub(crate) fn requires_ring_close(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<RegisteredInitializationError>())
}

fn check_ring(ring: &Ring) -> io::Result<()> {
    let required = IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER;
    if ring.flags() & required != required
        || ring.flags() & (IORING_SETUP_CQE32 | IORING_SETUP_CQE_MIXED) == 0
        || ring.flags() & IORING_SETUP_SQPOLL != 0
    {
        return Err(invalid(
            "ZCRX requires SINGLE_ISSUER, DEFER_TASKRUN and CQE32 or CQE_MIXED without SQPOLL",
        ));
    }
    Ok(())
}

fn align_up(value: usize, alignment: usize) -> io::Result<usize> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| invalid("ZCRX memory alignment overflow"))
}

fn validate_layout(reg: &ZcrxIfqReg, bytes: usize, stats_offset: Option<usize>) -> io::Result<()> {
    let head = reg.offsets.head as usize;
    let tail = reg.offsets.tail as usize;
    let rqes = reg.offsets.rqes as usize;
    let end = rqes
        .checked_add(reg.rq_entries as usize * size_of::<ZcrxRqe>())
        .ok_or_else(|| invalid_data("ZCRX returned an overflowing refill layout"))?;
    if !reg.rq_entries.is_power_of_two()
        || reg.rq_entries > MAX_REFILL_ENTRIES
        || !head.is_multiple_of(align_of::<u32>())
        || !tail.is_multiple_of(align_of::<u32>())
        || head == tail
        || head + 4 > rqes
        || tail + 4 > rqes
        || !rqes.is_multiple_of(align_of::<ZcrxRqe>())
        || end > bytes
        || stats_offset
            .is_some_and(|offset| offset < end || offset + 16 > bytes || !offset.is_multiple_of(8))
    {
        return Err(invalid_data(
            "kernel returned an invalid ZCRX refill layout",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn unsupported(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolConfig;

    // Exercise the real refill producer and lease code with anonymous memory;
    // no kernel capability is reported by this algorithm-only fixture.
    fn storage(entries: u32, pending_capacity: usize) -> Arc<Storage> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let area: Arc<dyn ExternalMemory> = Arc::new(CpuArea::new(page, page, page).unwrap());
        let refill: Arc<dyn ExternalMemory> = Arc::new(RefillMemory(
            Mapping::anonymous(
                align_up(64 + entries as usize * size_of::<ZcrxRqe>(), page).unwrap(),
            )
            .unwrap(),
        ));
        Arc::new(Storage {
            #[cfg(feature = "zc-rx-shared")]
            lifecycle: Mutex::new(Lifecycle {
                exported: None,
                users: 1,
            }),
            area,
            refill,
            offsets: ZcrxOffsets {
                head: 0,
                tail: 4,
                rqes: 64,
                ..ZcrxOffsets::default()
            },
            entries,
            area_token: 0,
            chunk_bytes: page as u32,
            #[cfg(feature = "zc-rx-large-chunks")]
            large_chunks: false,
            #[cfg(feature = "zc-rx-shared")]
            shared: false,
            mode: ReceiveMode::NodevCopied,
            #[cfg(feature = "zc-observe")]
            stats_offset: None,
            #[cfg(feature = "zc-observe")]
            allocation_events: AtomicU64::new(0),
            retired: AtomicBool::new(false),
            producer: Mutex::new(Producer {
                pending: VecDeque::with_capacity(pending_capacity),
                capacity: pending_capacity,
            }),
            notifiers: Mutex::new(Vec::with_capacity(8)),
            max_notifiers: 8,
        })
    }

    fn port(storage: Arc<Storage>) -> Arc<RefillPort> {
        Arc::new(RefillPort {
            id: 0,
            storage,
            error: AtomicU32::new(0),
            #[cfg(feature = "zc-rx-shared")]
            live: AtomicBool::new(true),
        })
    }

    fn instance(storage: Arc<Storage>) -> Zcrx {
        Zcrx {
            port: port(storage),
            attached: true,
            #[cfg(feature = "zc-observe")]
            events_owner: false,
            #[cfg(feature = "zc-observe")]
            event_user_data: u64::MAX - 2,
            #[cfg(feature = "zc-observe")]
            observe: true,
            #[cfg(feature = "mixed-cqe")]
            mixed_cqe: true,
            #[cfg(feature = "zc-observe")]
            local_stats: Cell::new(ZcStats::default()),
            #[cfg(any(
                feature = "zc-rx-large-chunks",
                feature = "zc-rx-shared",
                feature = "zc-observe"
            ))]
            fallbacks: BTreeMap::new(),
            _local: PhantomData,
        }
    }

    #[test]
    fn refill_wrap_preserves_exact_tokens_and_bounds_pending_returns() {
        let storage = storage(2, 1);
        storage.head().store(u32::MAX - 1, Ordering::Relaxed);
        storage.tail().store(u32::MAX - 1, Ordering::Relaxed);
        let port = port(storage.clone());
        let tokens = [
            ReturnToken {
                offset: 13,
                length: 3,
                tag: 0,
            },
            ReturnToken {
                offset: 41,
                length: 7,
                tag: 0,
            },
            ReturnToken {
                offset: 77,
                length: 9,
                tag: 0,
            },
            ReturnToken {
                offset: 103,
                length: 11,
                tag: 0,
            },
        ];
        assert!(port.recycle(tokens[0]));
        assert!(port.recycle(tokens[1]));
        assert_eq!(storage.tail().load(Ordering::Acquire), 0);
        assert!(port.recycle(tokens[2]));
        assert!(!port.recycle(tokens[3]));
        let rqes = unsafe { storage.refill.as_ptr().as_ptr().add(64).cast::<ZcrxRqe>() };
        let first = unsafe { rqes.read() };
        let second = unsafe { rqes.add(1).read() };
        assert_eq!((first.off, first.len, first.pad), (13, 3, 0));
        assert_eq!((second.off, second.len, second.pad), (41, 7, 0));
        assert_eq!(
            storage.producer.lock().pending.front().copied(),
            Some(tokens[2])
        );
        // Model the kernel consuming the published entries. The rejected token
        // remains the caller's and can then be accepted exactly once.
        storage.head().store(0, Ordering::Release);
        {
            let mut producer = storage.producer.lock();
            let token = producer.pending.pop_front().unwrap();
            assert!(storage.publish(token));
        }
        assert!(port.recycle(tokens[3]));
        let third = unsafe { rqes.read() };
        let fourth = unsafe { rqes.add(1).read() };
        assert_eq!((third.off, third.len), (77, 9));
        assert_eq!((fourth.off, fourth.len), (103, 11));
    }

    #[test]
    fn shared_refill_producers_do_not_overwrite_or_drop_each_others_returns() {
        let storage = storage(64, 512);
        let port = port(storage.clone());
        let threads = (0..8u64)
            .map(|worker| {
                let port = port.clone();
                std::thread::spawn(move || {
                    for index in worker * 64..(worker + 1) * 64 {
                        assert!(port.recycle(ReturnToken {
                            offset: index * 8,
                            length: (index % 8 + 1) as u32,
                            tag: 0
                        }));
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(storage.tail().load(Ordering::Acquire), 64);
        let mut returned = Vec::new();
        let rqes = unsafe { storage.refill.as_ptr().as_ptr().add(64).cast::<ZcrxRqe>() };
        for index in 0..64 {
            let entry = unsafe { rqes.add(index).read() };
            assert_eq!(entry.pad, 0);
            returned.push(ReturnToken {
                offset: entry.off,
                length: entry.len,
                tag: 0,
            });
        }
        returned.extend(storage.producer.lock().pending.iter().copied());
        returned.sort_by_key(|token| token.offset);
        let expected = (0..512u64)
            .map(|index| ReturnToken {
                offset: index * 8,
                length: (index % 8 + 1) as u32,
                tag: 0,
            })
            .collect::<Vec<_>>();
        assert_eq!(returned, expected);
    }

    #[test]
    fn lease_pressure_retains_completion_then_recycles_original_range_after_last_alias() {
        let storage = storage(4, 4);
        unsafe {
            ptr::copy_nonoverlapping(b"abcd".as_ptr(), storage.area.as_ptr().as_ptr().add(8), 4);
        }
        let instance = instance(storage.clone());
        let pool = BufferPool::new(PoolConfig {
            bytes: 8,
            block_size: 8,
            max_leases: 1,
        })
        .unwrap();
        let occupied = pool.try_acquire().unwrap();
        let cqe = Cqe {
            res: 4,
            flags: IORING_CQE_F_MORE | IORING_CQE_F_32,
            extra: [8, 0],
            ..Cqe::default()
        };
        assert_eq!(
            instance.complete(&cqe, &pool).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(storage.tail().load(Ordering::Acquire), 0);
        #[cfg(feature = "zc-observe")]
        assert_eq!(instance.stats().rx_completions, 0);
        drop(occupied);
        let data = instance.complete(&cqe, &pool).unwrap().unwrap();
        assert_eq!(data.as_slice(), b"abcd");
        let middle = data.slice(1..3);
        drop(data);
        assert_eq!(storage.tail().load(Ordering::Acquire), 0);
        assert_eq!(middle.as_slice(), b"bc");
        drop(middle);
        let entry = unsafe {
            storage
                .refill
                .as_ptr()
                .as_ptr()
                .add(64)
                .cast::<ZcrxRqe>()
                .read()
        };
        assert_eq!((entry.off, entry.len), (8, 4));
        assert_eq!(storage.tail().load(Ordering::Acquire), 1);
        #[cfg(feature = "zc-observe")]
        {
            assert_eq!(instance.stats().rx_completions, 1);
            assert_eq!(instance.stats().rx_bytes, 4);
        }
        assert_eq!(pool.pending_recycles(), 0);
    }

    #[test]
    fn user_lease_outlives_receiver_without_publishing_into_retired_refill() {
        let storage = storage(1, 1);
        unsafe {
            storage.area.as_ptr().as_ptr().add(4).write(42);
        }
        let instance = instance(storage.clone());
        let pool = BufferPool::new(PoolConfig {
            bytes: 8,
            block_size: 8,
            max_leases: 1,
        })
        .unwrap();
        let data = instance
            .complete(
                &Cqe {
                    res: 1,
                    flags: IORING_CQE_F_MORE | IORING_CQE_F_32,
                    extra: [4, 0],
                    ..Cqe::default()
                },
                &pool,
            )
            .unwrap()
            .unwrap();
        drop(instance);
        assert!(storage.retired.load(Ordering::Acquire));
        assert_eq!(data.as_slice(), &[42]);
        drop(data);
        pool.flush_recycles();
        assert_eq!(storage.tail().load(Ordering::Acquire), 0);
        assert_eq!(pool.pending_recycles(), 0);
        let mut writable = pool.try_acquire().unwrap();
        writable.extend_from_slice(b"new").unwrap();
        assert_eq!(writable.as_slice(), b"new");
    }

    #[test]
    fn shutdown_discard_needs_no_lease_and_retains_ownership_when_returns_fill() {
        let storage = storage(1, 1);
        let instance = instance(storage.clone());
        let pool = BufferPool::new(PoolConfig {
            bytes: 8,
            block_size: 8,
            max_leases: 1,
        })
        .unwrap();
        let occupied = pool.try_acquire().unwrap();
        let first = Cqe {
            res: 3,
            flags: IORING_CQE_F_MORE | IORING_CQE_F_32,
            extra: [5, 0],
            ..Cqe::default()
        };
        assert_eq!(
            instance.complete(&first, &pool).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        instance.discard(&first).unwrap();
        let entry = unsafe {
            storage
                .refill
                .as_ptr()
                .as_ptr()
                .add(64)
                .cast::<ZcrxRqe>()
                .read()
        };
        assert_eq!((entry.off, entry.len), (5, 3));
        let second = Cqe {
            extra: [15, 0],
            ..first
        };
        instance.discard(&second).unwrap();
        let third = Cqe {
            extra: [25, 0],
            ..first
        };
        assert_eq!(
            instance.discard(&third).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            storage.producer.lock().pending.front().copied(),
            Some(ReturnToken {
                offset: 15,
                length: 3,
                tag: 0
            })
        );
        assert_eq!(
            pool.try_acquire().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(pool.pending_recycles(), 0);
        drop(occupied);
    }
}
