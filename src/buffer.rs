//! Bounded, worker-local buffer ownership over stable CPU memory.
//!
//! A pool reserves its entire payload arena and lease metadata at construction.
//! Acquiring, freezing, cloning, slicing and returning normal buffers allocate no
//! storage. Immutable aliases share one lease slot; a slot cannot be reused while
//! any alias remains. External providers budget their own CPU areas, while their
//! outstanding leases share this pool's bounded metadata slots.
//!
//! Kernel references must be represented by an owning buffer guard. In particular,
//! obtaining a raw pointer does not permit dropping the guard before completion,
//! nor writing a range that has already been published as an immutable view.

use std::{
    alloc::{Layout, alloc, dealloc},
    cell::{Cell, RefCell, UnsafeCell},
    fmt, io,
    mem::MaybeUninit,
    ops::{Deref, Range},
    ptr::NonNull,
    rc::Rc,
    slice,
    sync::Arc,
    task::Waker,
};

/// Per-pool payload and distinct-lease limits.
///
/// `bytes` bounds the preallocated normal-buffer arena. `max_leases` bounds
/// distinct normal and external leases together, not immutable aliases of a
/// lease. External backing allocations have a separate provider-owned budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolConfig {
    pub bytes: usize,
    pub block_size: usize,
    pub max_leases: usize,
}

/// A stable registration range belonging to a pool.
///
/// Retain a clone of the pool until the OS has stopped referring to the region
/// and the registration has been removed. Registration does not grant writable
/// access: only an exclusive `WriteBuf` authorizes writes to its own range.
#[derive(Clone, Copy, Debug)]
pub struct MemoryRegion {
    pub ptr: NonNull<u8>,
    pub len: usize,
    pub id: u32,
}

/// An exact provider-owned return descriptor, independent of a view's length.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ReturnToken {
    pub offset: u64,
    pub length: u32,
    pub tag: u32,
}

/// Stable external CPU memory, including kernel-written receive areas.
///
/// # Safety
///
/// `as_ptr()` and `len()` must describe the same live allocation on every call.
/// The allocation must remain accessible at that address for this object's
/// lifetime, and its length must not exceed `isize::MAX`. A non-null, suitably
/// aligned dangling pointer is sufficient for an empty allocation. Storage that
/// may be written through shared ownership must use `UnsafeCell` (or an external
/// mapping with the equivalent interior-mutability contract), not immutable Rust
/// storage. Implementations must synchronize their own control data; `Send +
/// Sync` does not permit concurrent access to published payload bytes by writers.
/// Initialization and the disjointness of kernel-owned and published ranges are
/// obligations of the caller of `BufferPool::lease_external`.
pub unsafe trait ExternalMemory: Send + Sync {
    fn as_ptr(&self) -> NonNull<u8>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Accepts a returned receive region after its last immutable alias disappears.
///
/// `true` transfers the exact token back to the provider. `false` must have no
/// ownership-transfer side effect: the pool retains the token and backing and
/// retries through `BufferPool::flush_recycles`. Implementations must not unwind;
/// an unwind leaves ownership ambiguous, so the pool retains that backing rather
/// than risking a duplicate return or premature unmapping. Acceptance transfers
/// responsibility for any remaining kernel references to the provider.
pub trait Recycle: Send + Sync {
    fn recycle(&self, token: ReturnToken) -> bool;
}

/// A cheap local handle to a bounded stable-memory pool.
///
/// This type and all its leases are deliberately `!Send` and `!Sync`. Returning
/// an alias happens on the owning worker; the installed `Waker` may safely notify
/// that worker from the provider's own thread-safe notification machinery.
#[derive(Clone)]
pub struct BufferPool {
    inner: Rc<PoolInner>,
}

impl BufferPool {
    pub fn new(config: PoolConfig) -> io::Result<Self> {
        if config.block_size == 0 || config.bytes < config.block_size || config.max_leases == 0 {
            return Err(invalid(
                "buffer pool requires nonzero limits and at least one receive block",
            ));
        }
        let layout = Layout::from_size_align(config.bytes, 64)
            .map_err(|_| invalid("buffer pool payload size exceeds addressable memory"))?;
        Layout::array::<LeaseSlot>(config.max_leases)
            .map_err(|_| invalid("buffer pool lease count exceeds addressable memory"))?;
        let extent_capacity = config
            .max_leases
            .min(config.bytes / config.block_size)
            .checked_add(1)
            .ok_or_else(|| invalid("buffer pool extent count exceeds addressable memory"))?;

        let mut slots = Vec::new();
        slots
            .try_reserve_exact(config.max_leases)
            .map_err(|_| exhausted_memory())?;
        slots.resize_with(config.max_leases, LeaseSlot::new);
        let mut free_slots = Vec::new();
        free_slots
            .try_reserve_exact(config.max_leases)
            .map_err(|_| exhausted_memory())?;
        free_slots.extend((0..config.max_leases).rev());
        let mut free_extents = Vec::new();
        free_extents
            .try_reserve_exact(extent_capacity)
            .map_err(|_| exhausted_memory())?;
        free_extents.push(Extent {
            offset: 0,
            len: config.bytes,
        });

        // No bytes are initialized here. Every exposed byte must first be
        // initialized by WriteBuf or by an explicitly contracted kernel write.
        let ptr = NonNull::new(unsafe { alloc(layout) })
            .ok_or_else(exhausted_memory)?
            .cast::<UnsafeCell<MaybeUninit<u8>>>();
        Ok(Self {
            inner: Rc::new(PoolInner {
                storage: Storage { ptr, layout },
                config,
                slots: slots.into_boxed_slice(),
                allocator: RefCell::new(Allocator {
                    free_slots,
                    free_extents,
                }),
                recycle_waker: RefCell::new(None),
                pending_head: Cell::new(NO_SLOT),
                pending_tail: Cell::new(NO_SLOT),
                pending_count: Cell::new(0),
                recycling: Cell::new(false),
                ambiguous_return: Cell::new(false),
                pending_owner: RefCell::new(None),
            }),
        })
    }

    /// Observe normal payload storage, distinct lease slots and deferred returns.
    ///
    /// Free payload bytes do not imply that a lease slot or a sufficiently large
    /// contiguous range is available. External leases consume metadata, not the
    /// normal payload arena; aliases and slices share their original allocation.
    ///
    /// This query allocates nothing, invokes no provider or wake callbacks, and
    /// does not retry deferred returns. It remains available after its runtime
    /// has been dropped, as long as this pool handle is retained.
    pub fn usage(&self) -> crate::diagnostics::PoolUsage {
        // Allocator borrows end before provider and wake callbacks, so even a
        // callback reentering this infallible query can borrow it immutably.
        let allocator = self.inner.allocator.borrow();
        let mut payload_available = 0;
        let mut largest_free_extent = 0;
        for extent in &allocator.free_extents {
            payload_available += extent.len;
            largest_free_extent = largest_free_extent.max(extent.len);
        }
        crate::diagnostics::PoolUsage {
            payload_capacity: self.inner.config.bytes,
            payload_available,
            largest_free_extent,
            lease_capacity: self.inner.config.max_leases,
            leases_available: allocator.free_slots.len(),
            pending_recycles: self.inner.pending_count.get(),
        }
    }

    /// Acquire at least the configured receive block size.
    ///
    /// `WouldBlock` means the arena or its lease slots are currently exhausted.
    pub fn try_acquire(&self) -> io::Result<WriteBuf> {
        self.try_acquire_at_least(self.inner.config.block_size)
    }

    /// Acquire one contiguous range, never smaller than a receive block.
    ///
    /// Requests larger than the total arena return `InvalidInput`. Temporary
    /// exhaustion, including fragmentation, returns `WouldBlock`. Released
    /// adjacent ranges coalesce, so larger UDP and send buffers can use the same
    /// arena without a second allocation or a hidden budget overrun.
    pub fn try_acquire_at_least(&self, size: usize) -> io::Result<WriteBuf> {
        let size = size.max(self.inner.config.block_size);
        if size > self.inner.config.bytes {
            return Err(invalid("requested buffer exceeds the pool payload budget"));
        }
        let (index, extent) = {
            let mut allocator = self.inner.allocator.borrow_mut();
            let Some(&index) = allocator.free_slots.last() else {
                return Err(unavailable());
            };
            let Some(position) = allocator
                .free_extents
                .iter()
                .position(|extent| extent.len >= size)
            else {
                return Err(unavailable());
            };
            let extent = Extent {
                offset: allocator.free_extents[position].offset,
                len: size,
            };
            if allocator.free_extents[position].len == size {
                allocator.free_extents.remove(position);
            } else {
                allocator.free_extents[position].offset += size;
                allocator.free_extents[position].len -= size;
            }
            allocator.free_slots.pop();
            (index, extent)
        };
        let slot = &self.inner.slots[index];
        *slot.backing.borrow_mut() = Backing::Pooled(extent);
        slot.references.set(1);
        // The allocator lends disjoint subranges of the one stable allocation.
        let ptr = unsafe { self.inner.storage.ptr.cast::<u8>().add(extent.offset) };
        Ok(WriteBuf {
            lease: Lease {
                pool: Rc::clone(&self.inner),
                index,
            },
            ptr,
            capacity: size,
            initialized: 0,
        })
    }

    /// Replace the owning worker's recycle notification target.
    pub fn set_recycle_waker(&self, waker: Waker) {
        let old = self.inner.recycle_waker.borrow_mut().replace(waker);
        drop(old);
    }

    /// Number of returns still waiting for provider acceptance.
    pub fn pending_recycles(&self) -> usize {
        self.inner.pending_count.get()
    }

    /// Retry each currently deferred external return once, without allocating.
    ///
    /// Call after provider/refill progress and during owner-thread shutdown.
    /// A still-full provider keeps its slot and backing. Merely retrying a full
    /// queue does not resignal the waker, which would otherwise cause idle spin.
    /// A rejected return retains the pool itself until accepted: dropping the
    /// last public handle never discards an unaccepted token. Keep a pool handle
    /// and drain deferred returns before destroying the provider.
    pub fn flush_recycles(&self) {
        self.inner.flush_recycles();
    }

    /// Enumerate the stable normal-buffer arena for cold OS registration.
    ///
    /// Every normal lease is a subrange of region zero, including allocations
    /// larger than `block_size`. External provider areas are registered by their
    /// provider, not by this enumeration.
    pub fn regions(&self) -> Vec<MemoryRegion> {
        vec![MemoryRegion {
            ptr: self.inner.storage.ptr.cast(),
            len: self.inner.config.bytes,
            id: 0,
        }]
    }

    /// Publish an initialized range in a provider-owned CPU area.
    ///
    /// On error, no token is returned to `recycler`; ownership of the token
    /// remains with the caller. Successful publication consumes one reusable
    /// lease slot, retaining the supplied region and recycler until acceptance.
    /// Bounds errors are `InvalidInput`; unavailable metadata is `WouldBlock`.
    ///
    /// # Safety
    ///
    /// If the requested range is in bounds, all its bytes must be initialized
    /// and readable. No CPU or kernel writer may modify the published range
    /// until the recycler accepts the token after the last derived view drops.
    /// Writers may continue using *disjoint* ranges of the same allocation.
    /// The token must describe one valid provider reference, not a duplicate
    /// return. If independently published ranges overlap or share a recyclable
    /// chunk, the provider must track their independent references and any
    /// kernel-retained remainder before reusing that chunk. The recycler must
    /// retain whatever backing the OS still requires after accepting ownership.
    pub unsafe fn lease_external(
        &self,
        region: Arc<dyn ExternalMemory>,
        offset: usize,
        length: usize,
        recycler: Arc<dyn Recycle>,
        token: ReturnToken,
    ) -> io::Result<ReadBuf> {
        let region_len = region.len();
        if region_len > isize::MAX as usize
            || offset
                .checked_add(length)
                .is_none_or(|end| end > region_len)
        {
            return Err(invalid(
                "external buffer range exceeds its backing allocation",
            ));
        }
        let ptr = unsafe { region.as_ptr().add(offset) };
        let index = self
            .inner
            .allocator
            .borrow_mut()
            .free_slots
            .pop()
            .ok_or_else(unavailable)?;
        let slot = &self.inner.slots[index];
        *slot.backing.borrow_mut() = Backing::External(ExternalLease {
            region,
            recycler,
            token,
        });
        slot.references.set(1);
        Ok(SendBuf {
            lease: Lease {
                pool: Rc::clone(&self.inner),
                index,
            },
            ptr,
            len: length,
        })
    }
}

impl fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPool")
            .field("config", &self.inner.config)
            .field("pending_recycles", &self.inner.pending_count.get())
            .finish_non_exhaustive()
    }
}

/// Exclusive initialization access to one stable, worker-local allocation.
///
/// This value is not cloneable. A raw pointer handed to the OS requires keeping
/// this guard alive until the write has completed. While the OS is writing,
/// neither Rust reads nor writes may overlap that kernel-owned range.
pub struct WriteBuf {
    lease: Lease,
    ptr: NonNull<u8>,
    capacity: usize,
    initialized: usize,
}

impl WriteBuf {
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn initialized_len(&self) -> usize {
        self.initialized
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn as_slice(&self) -> &[u8] {
        // Only the prefix established by initialization is exposed as bytes.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.initialized) }
    }

    /// Mutably borrow only the already-initialized prefix.
    ///
    /// The view has the same length as [`Self::as_slice`]. Mutating it does not
    /// change the initialized length or expose spare capacity. Use
    /// [`Self::spare_capacity_mut`] to initialize additional bytes.
    ///
    /// A published buffer must first pass [`SendBuf::try_into_write`]; immutable
    /// aliases, outstanding kernel guards and external memory cannot bypass
    /// that ownership check.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // The exclusive guard owns this initialized prefix. Its mutable borrow
        // prevents freezing or accessing the buffer while the view is live.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.initialized) }
    }

    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe {
            slice::from_raw_parts_mut(
                self.ptr.as_ptr().add(self.initialized).cast(),
                self.capacity - self.initialized,
            )
        }
    }

    /// Forget initialization without changing or reallocating the storage.
    pub fn clear(&mut self) {
        self.initialized = 0;
    }

    /// Append bytes, returning `InvalidInput` without mutation if they do not fit.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > self.capacity - self.initialized {
            return Err(invalid("appended bytes exceed buffer capacity"));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.ptr.as_ptr().add(self.initialized),
                bytes.len(),
            );
        }
        self.initialized += bytes.len();
        Ok(())
    }

    /// Set the prefix that may be exposed as initialized bytes.
    ///
    /// # Safety
    ///
    /// Every byte of `0..length` must have been initialized. Any kernel or other
    /// raw-pointer writes to that prefix must have completed before it is read
    /// or frozen. Keeping a stale kernel write alive past this guard's release
    /// is forbidden even if the initialized prefix is empty.
    ///
    /// # Panics
    ///
    /// Panics if `length` exceeds this buffer's capacity.
    pub unsafe fn set_initialized_len(&mut self, length: usize) {
        assert!(
            length <= self.capacity,
            "initialized length exceeds buffer capacity"
        );
        self.initialized = length;
    }

    /// Publish the initialized prefix as immutable owned memory.
    pub fn freeze(self) -> SendBuf {
        SendBuf {
            lease: self.lease,
            ptr: self.ptr,
            len: self.initialized,
        }
    }
}

impl fmt::Debug for WriteBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBuf")
            .field("capacity", &self.capacity)
            .field("initialized_len", &self.initialized)
            .finish_non_exhaustive()
    }
}

/// An owning immutable view over initialized stable memory.
///
/// Cloning and slicing increment a local reference count in a reusable slot;
/// they copy neither payload nor a reference-count allocation. Writable
/// ownership can be recovered only from a unique normal-arena lease.
pub struct SendBuf {
    lease: Lease,
    ptr: NonNull<u8>,
    len: usize,
}

/// Receive views obey precisely the same immutable ownership as send views.
pub type ReadBuf = SendBuf;

impl SendBuf {
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Create an owning subrange without copying bytes.
    ///
    /// Panics for a reversed range or an endpoint outside this initialized view.
    /// Even an empty derived view keeps its backing lease alive.
    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.start <= range.end && range.end <= self.len,
            "buffer slice is out of bounds"
        );
        Self {
            lease: self.lease.clone(),
            ptr: unsafe { self.ptr.add(range.start) },
            len: range.end - range.start,
        }
    }

    /// Recover writable ownership only when this is the last normal-pool view.
    ///
    /// External backing cannot be proven exclusive from its local slot count,
    /// so external views always return `Err(self)`. A sliced normal view keeps
    /// its initialized bytes and gains spare capacity only from its own start
    /// through the original allocation's end; trimmed bytes are not exposed.
    /// A kernel still reading the data must hold its own immutable lease guard,
    /// which prevents this conversion until its memory-release completion.
    pub fn try_into_write(self) -> Result<WriteBuf, Self> {
        let capacity = {
            let slot = &self.lease.pool.slots[self.lease.index];
            if slot.references.get() != 1 {
                return Err(self);
            }
            let backing = slot.backing.borrow();
            match &*backing {
                Backing::Pooled(extent) => {
                    let arena = self.lease.pool.storage.ptr.as_ptr().cast::<u8>();
                    // Both pointers belong to the same arena, including the
                    // one-past pointer of an empty range at its end.
                    let start = unsafe { self.ptr.as_ptr().offset_from(arena) as usize };
                    Some(extent.offset + extent.len - start)
                }
                Backing::External(_) | Backing::Vacant => None,
            }
        };
        let Some(capacity) = capacity else {
            return Err(self);
        };
        Ok(WriteBuf {
            lease: self.lease,
            ptr: self.ptr,
            capacity,
            initialized: self.len,
        })
    }

    fn advance(&mut self, bytes: usize) {
        debug_assert!(bytes <= self.len);
        self.ptr = unsafe { self.ptr.add(bytes) };
        self.len -= bytes;
    }
}

impl Clone for SendBuf {
    fn clone(&self) -> Self {
        Self {
            lease: self.lease.clone(),
            ptr: self.ptr,
            len: self.len,
        }
    }
}

impl Deref for SendBuf {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for SendBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl fmt::Debug for SendBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendBuf")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// Owned single-range or scatter/gather send data.
///
/// Drivers can retain cloned segment guards in reusable operation storage until
/// the kernel releases its memory references; the payload itself need not clone
/// an iovec vector for every operation.
#[derive(Debug)]
pub enum SendPayload {
    Single(SendBuf),
    Vectored(Vec<SendBuf>),
}

impl SendPayload {
    /// Total byte length, panicking if the caller's segments overflow `usize`.
    pub fn len(&self) -> usize {
        self.segments()
            .iter()
            .try_fold(0usize, |len, segment| len.checked_add(segment.len()))
            .expect("send payload length exceeds usize")
    }

    pub fn is_empty(&self) -> bool {
        self.segments().iter().all(SendBuf::is_empty)
    }

    pub fn segments(&self) -> &[SendBuf] {
        match self {
            Self::Single(buffer) => slice::from_ref(buffer),
            Self::Vectored(buffers) => buffers.as_slice(),
        }
    }

    /// Keep only bytes not accepted by a short send.
    ///
    /// Completed segment guards are released, a partially consumed segment is
    /// narrowed in place, and a vectored payload reuses its original vector.
    /// No payload bytes or new lease metadata are allocated or copied.
    /// Panics if `bytes` exceeds the total payload length.
    pub fn remaining(self, bytes: usize) -> Self {
        assert!(
            bytes <= self.len(),
            "accepted bytes exceed send payload length"
        );
        match self {
            Self::Single(mut buffer) => {
                if bytes == buffer.len() {
                    Self::Vectored(Vec::new())
                } else {
                    buffer.advance(bytes);
                    Self::Single(buffer)
                }
            }
            Self::Vectored(mut buffers) => {
                let mut left = bytes;
                let mut completed = 0;
                for buffer in &mut buffers {
                    if left < buffer.len() {
                        buffer.advance(left);
                        break;
                    }
                    left -= buffer.len();
                    completed += 1;
                }
                drop(buffers.drain(..completed));
                Self::Vectored(buffers)
            }
        }
    }
}

impl From<SendBuf> for SendPayload {
    fn from(buffer: SendBuf) -> Self {
        Self::Single(buffer)
    }
}

impl From<Vec<SendBuf>> for SendPayload {
    fn from(buffers: Vec<SendBuf>) -> Self {
        Self::Vectored(buffers)
    }
}

const NO_SLOT: usize = usize::MAX;

struct Storage {
    ptr: NonNull<UnsafeCell<MaybeUninit<u8>>>,
    layout: Layout,
}

impl Drop for Storage {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_ptr().cast(), self.layout);
        }
    }
}

#[derive(Clone, Copy)]
struct Extent {
    offset: usize,
    len: usize,
}

struct Allocator {
    free_slots: Vec<usize>,
    // Sorted, non-overlapping, maximally coalesced. Capacity is reserved at new.
    free_extents: Vec<Extent>,
}

impl Allocator {
    fn release_extent(&mut self, extent: Extent) {
        let next = self
            .free_extents
            .partition_point(|free| free.offset < extent.offset);
        if next != 0
            && self.free_extents[next - 1].offset + self.free_extents[next - 1].len == extent.offset
        {
            self.free_extents[next - 1].len += extent.len;
            if next < self.free_extents.len()
                && self.free_extents[next - 1].offset + self.free_extents[next - 1].len
                    == self.free_extents[next].offset
            {
                let following = self.free_extents.remove(next);
                self.free_extents[next - 1].len += following.len;
            }
        } else if next < self.free_extents.len()
            && extent.offset + extent.len == self.free_extents[next].offset
        {
            self.free_extents[next].offset = extent.offset;
            self.free_extents[next].len += extent.len;
        } else {
            self.free_extents.insert(next, extent);
        }
    }
}

struct ExternalLease {
    // Kept even when there are no application aliases and the refill queue is full.
    region: Arc<dyn ExternalMemory>,
    recycler: Arc<dyn Recycle>,
    token: ReturnToken,
}

enum Backing {
    Vacant,
    Pooled(Extent),
    External(ExternalLease),
}

struct LeaseSlot {
    references: Cell<usize>,
    backing: RefCell<Backing>,
    pending_next: Cell<usize>,
}

impl LeaseSlot {
    fn new() -> Self {
        Self {
            references: Cell::new(0),
            backing: RefCell::new(Backing::Vacant),
            pending_next: Cell::new(NO_SLOT),
        }
    }
}

struct PoolInner {
    storage: Storage,
    config: PoolConfig,
    slots: Box<[LeaseSlot]>,
    allocator: RefCell<Allocator>,
    recycle_waker: RefCell<Option<Waker>>,
    pending_head: Cell<usize>,
    pending_tail: Cell<usize>,
    pending_count: Cell<usize>,
    recycling: Cell<bool>,
    ambiguous_return: Cell<bool>,
    // A bounded self-reference prevents last-handle drop from losing a token.
    // Normal shutdown drains pending returns and removes this reference.
    pending_owner: RefCell<Option<Rc<PoolInner>>>,
}

impl PoolInner {
    fn release(self: &Rc<Self>, index: usize) {
        let is_external = matches!(*self.slots[index].backing.borrow(), Backing::External(_));
        if !is_external {
            let Backing::Pooled(extent) = self.slots[index].backing.replace(Backing::Vacant) else {
                unreachable!("a live lease must own storage");
            };
            {
                let mut allocator = self.allocator.borrow_mut();
                allocator.release_extent(extent);
                allocator.free_slots.push(index);
            }
            self.wake_recycle();
            return;
        }

        self.retain_pending_owner();
        if self.recycling.get() {
            // A provider callback can release another local lease reentrantly.
            self.enqueue(index);
        } else {
            let guard = RecyclingGuard::new(self);
            if !self.try_return(index) {
                self.enqueue(index);
            }
            drop(guard);
            self.release_pending_owner_if_drained();
        }
        // A newly deferred token also wakes the owner to drive provider progress.
        self.wake_recycle();
    }

    fn enqueue(&self, index: usize) {
        self.slots[index].pending_next.set(NO_SLOT);
        let tail = self.pending_tail.replace(index);
        if tail == NO_SLOT {
            self.pending_head.set(index);
        } else {
            self.slots[tail].pending_next.set(index);
        }
        self.pending_count.set(self.pending_count.get() + 1);
    }

    fn dequeue(&self) -> Option<usize> {
        let index = self.pending_head.get();
        if index == NO_SLOT {
            return None;
        }
        let next = self.slots[index].pending_next.replace(NO_SLOT);
        self.pending_head.set(next);
        if next == NO_SLOT {
            self.pending_tail.set(NO_SLOT);
        }
        self.pending_count.set(self.pending_count.get() - 1);
        Some(index)
    }

    fn try_return(&self, index: usize) -> bool {
        let mut attempt = ReturnAttempt {
            pool: self,
            completed: false,
        };
        let accepted = {
            let backing = self.slots[index].backing.borrow();
            let Backing::External(external) = &*backing else {
                unreachable!("pending return must own external storage");
            };
            external.recycler.recycle(external.token)
        };
        attempt.completed = true;
        if accepted {
            let Backing::External(ExternalLease {
                region, recycler, ..
            }) = self.slots[index].backing.replace(Backing::Vacant)
            else {
                unreachable!("accepted return must own external storage");
            };
            self.allocator.borrow_mut().free_slots.push(index);
            // A provider destructor may still consult the CPU area.
            drop(recycler);
            drop(region);
        }
        accepted
    }

    fn flush_recycles(self: &Rc<Self>) {
        if self.recycling.get() || self.pending_count.get() == 0 {
            return;
        }
        let guard = RecyclingGuard::new(self);
        let attempts = self.pending_count.get();
        let mut freed = false;
        for _ in 0..attempts {
            let Some(index) = self.dequeue() else {
                break;
            };
            if self.try_return(index) {
                freed = true;
            } else {
                self.enqueue(index);
            }
        }
        drop(guard);
        self.release_pending_owner_if_drained();
        if freed {
            self.wake_recycle();
        }
    }

    fn retain_pending_owner(self: &Rc<Self>) {
        let mut owner = self.pending_owner.borrow_mut();
        if owner.is_none() {
            *owner = Some(Rc::clone(self));
        }
    }

    fn release_pending_owner_if_drained(&self) {
        if self.pending_count.get() == 0 && !self.ambiguous_return.get() {
            let owner = self.pending_owner.borrow_mut().take();
            drop(owner);
        }
    }

    fn wake_recycle(&self) {
        // No RefCell borrow or allocator mutation spans arbitrary wake code.
        // A reentrant replacement wins; a nested release is already signaled.
        let waker = self.recycle_waker.borrow_mut().take();
        if let Some(waker) = waker {
            waker.wake_by_ref();
            let mut current = self.recycle_waker.borrow_mut();
            if current.is_none() {
                *current = Some(waker);
            } else {
                drop(current);
                drop(waker);
            }
        }
    }
}

struct RecyclingGuard<'a> {
    pool: &'a PoolInner,
}

impl<'a> RecyclingGuard<'a> {
    fn new(pool: &'a PoolInner) -> Self {
        let was_recycling = pool.recycling.replace(true);
        debug_assert!(!was_recycling);
        Self { pool }
    }
}

impl Drop for RecyclingGuard<'_> {
    fn drop(&mut self) {
        self.pool.recycling.set(false);
    }
}

struct ReturnAttempt<'a> {
    pool: &'a PoolInner,
    completed: bool,
}

impl Drop for ReturnAttempt<'_> {
    fn drop(&mut self) {
        if !self.completed {
            // A panicking provider may already have consumed the token. Retain
            // its backing permanently rather than retrying an ambiguous return.
            self.pool.ambiguous_return.set(true);
        }
    }
}

struct Lease {
    pool: Rc<PoolInner>,
    index: usize,
}

impl Clone for Lease {
    fn clone(&self) -> Self {
        let references = &self.pool.slots[self.index].references;
        let Some(next) = references.get().checked_add(1) else {
            // Wrapping a reference count could expose writable aliased memory.
            std::process::abort();
        };
        references.set(next);
        Self {
            pool: Rc::clone(&self.pool),
            index: self.index,
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let references = &self.pool.slots[self.index].references;
        let remaining = references.get() - 1;
        references.set(remaining);
        if remaining == 0 {
            self.pool.release(self.index);
        } else if remaining == 1 {
            // A backend may retain one guard to rearm the same allocation.
            self.pool.wake_recycle();
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn unavailable() -> io::Error {
    io::ErrorKind::WouldBlock.into()
}
fn exhausted_memory() -> io::Error {
    io::ErrorKind::OutOfMemory.into()
}
