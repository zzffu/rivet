use parking_lot::Mutex;
use rivet::buffer::{
    BufferPool, ExternalMemory, PoolConfig, Recycle, ReturnToken, SendBuf, SendPayload,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    collections::VecDeque,
    io::ErrorKind,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
};

struct CountingAllocator;

thread_local! {
    #[cfg_attr(
        target_os = "android",
        allow(
            clippy::missing_const_for_thread_local,
            reason = "Already const; Android std TLS false positive (rust-lang/rust-clippy#13422)."
        )
    )]
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Some(count) = counter.get() {
            counter.set(Some(count + 1));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocations_during(action: impl FnOnce()) -> usize {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|counter| counter.set(None));
        }
    }
    ALLOCATIONS.with(|counter| counter.set(Some(0)));
    let _reset = Reset;
    action();
    ALLOCATIONS.with(|counter| counter.get().unwrap())
}

fn pool(bytes: usize, block_size: usize, max_leases: usize) -> BufferPool {
    BufferPool::new(PoolConfig {
        bytes,
        block_size,
        max_leases,
    })
    .unwrap()
}

fn filled(pool: &BufferPool, bytes: &[u8]) -> SendBuf {
    let mut buffer = pool.try_acquire_at_least(bytes.len()).unwrap();
    buffer.extend_from_slice(bytes).unwrap();
    buffer.freeze()
}

#[test]
fn only_initialized_bytes_are_published_and_failed_append_is_atomic() {
    let pool = pool(8, 8, 1);
    let mut buffer = pool.try_acquire().unwrap();
    assert_eq!(buffer.initialized_len(), 0);
    assert_eq!(buffer.as_slice(), b"");
    assert_eq!(buffer.as_mut_slice(), b"");

    for (destination, byte) in buffer.spare_capacity_mut().iter_mut().zip(*b"abc") {
        destination.write(byte);
    }
    unsafe {
        buffer.set_initialized_len(3);
    }
    buffer.extend_from_slice(b"de").unwrap();
    assert_eq!(buffer.as_slice(), b"abcde");
    buffer.as_mut_slice()[1..4].make_ascii_uppercase();
    assert_eq!(buffer.as_slice(), b"aBCDe");
    assert!(buffer.as_mut_slice().get_mut(5).is_none());
    assert_eq!(
        buffer.extend_from_slice(b"toolong").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(buffer.as_slice(), b"aBCDe");
    assert_eq!(buffer.spare_capacity_mut().len(), 3);

    buffer.clear();
    assert_eq!(buffer.as_slice(), b"");
    assert_eq!(buffer.as_mut_slice(), b"");
    buffer.extend_from_slice(b"xy").unwrap();
    let published = buffer.freeze();
    assert_eq!(published.as_slice(), b"xy");
    assert!(catch_unwind(AssertUnwindSafe(|| published.slice(0..3))).is_err());
    assert_eq!(published.as_slice(), b"xy");
    drop(published);

    let reused = pool.try_acquire().unwrap();
    assert_eq!(reused.initialized_len(), 0);
    assert_eq!(reused.as_slice(), b"");
}

#[test]
fn usage_distinguishes_free_payload_from_exhausted_lease_metadata() {
    let pool = pool(32, 8, 1);
    let empty = pool.usage();
    assert_eq!(empty.payload_capacity(), 32);
    assert_eq!(empty.payload_available(), 32);
    assert_eq!(empty.payload_in_use(), 0);
    assert_eq!(empty.largest_free_extent(), 32);
    assert_eq!(empty.lease_capacity(), 1);
    assert_eq!(empty.leases_available(), 1);
    assert_eq!(empty.leases_in_use(), 0);
    assert_eq!(empty.pending_recycles(), 0);

    let lease = pool.try_acquire().unwrap();
    let occupied = pool.usage();
    assert_eq!(occupied.payload_available(), 24);
    assert_eq!(occupied.payload_in_use(), 8);
    assert_eq!(occupied.largest_free_extent(), 24);
    assert_eq!(occupied.leases_available(), 0);
    assert_eq!(occupied.leases_in_use(), 1);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    let allocations = allocations_during(|| {
        for _ in 0..128 {
            assert_eq!(pool.usage(), occupied);
        }
    });
    assert_eq!(allocations, 0, "pool usage must not allocate");

    drop(lease);
    assert_eq!(pool.usage(), empty);
}

#[test]
fn every_alias_including_empty_ranges_keeps_memory_unwritable() {
    let pool = pool(8, 8, 1);
    let empty_usage = pool.usage();
    let original = filled(&pool, b"abcdef");
    let occupied_usage = pool.usage();
    assert_eq!(occupied_usage.payload_in_use(), 8);
    assert_eq!(occupied_usage.leases_in_use(), 1);
    let clone = original.clone();
    let middle = original.slice(2..5);
    let empty = middle.slice(1..1);
    assert_eq!(pool.usage(), occupied_usage);
    drop(original);
    drop(clone);
    assert_eq!(middle.as_slice(), b"cde");
    assert_eq!(pool.usage(), occupied_usage);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(middle);
    assert!(empty.is_empty());
    assert_eq!(pool.usage(), occupied_usage);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(empty);
    assert_eq!(pool.usage(), empty_usage);
    let mut recovered = pool.try_acquire().unwrap();
    recovered.extend_from_slice(b"reused!").unwrap();
    assert_eq!(recovered.freeze().as_slice(), b"reused!");
}

#[test]
fn unique_normal_views_recover_only_their_own_initialized_range() {
    let pool = pool(8, 8, 1);
    let original = filled(&pool, b"abcdef");
    let kernel_guard = original.clone();
    let original = original.try_into_write().unwrap_err();
    assert_eq!(original.as_slice(), b"abcdef");
    drop(kernel_guard);

    let middle = original.slice(2..4);
    drop(original);
    let mut writable = middle.try_into_write().unwrap();
    assert_eq!(writable.as_slice(), b"cd");
    writable.as_mut_slice().make_ascii_uppercase();
    assert_eq!(writable.capacity(), 6);
    writable.extend_from_slice(b"XY").unwrap();
    assert_eq!(writable.as_slice(), b"CDXY");
    writable.clear();
    writable.extend_from_slice(b"reuse!").unwrap();
    assert_eq!(writable.as_slice(), b"reuse!");
    drop(writable);

    let restored = pool.try_acquire_at_least(8).unwrap();
    assert_eq!(restored.capacity(), 8);
    assert_eq!(restored.initialized_len(), 0);
}

#[test]
fn empty_end_slice_can_recover_an_empty_mutable_view() {
    let pool = pool(8, 8, 1);
    let original = filled(&pool, b"12345678");
    let end = original.slice(8..8);
    drop(original);
    let mut writable = end.try_into_write().unwrap();
    assert_eq!(writable.as_mut_slice(), b"");
    assert_eq!(
        writable.extend_from_slice(b"x").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(writable.freeze().as_slice(), b"");
}

#[test]
fn variable_size_allocations_obey_budget_and_coalesce_after_last_alias() {
    let pool = pool(24, 4, 6);
    let registered = pool.regions();
    let first = pool.try_acquire().unwrap();
    let mut middle = pool.try_acquire_at_least(13).unwrap();
    middle.extend_from_slice(b"large-payload").unwrap();
    let last = pool.try_acquire_at_least(7).unwrap();
    let full = pool.usage();
    assert_eq!(full.payload_available(), 0);
    assert_eq!(full.largest_free_extent(), 0);
    assert_eq!(full.leases_in_use(), 3);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(
        pool.try_acquire_at_least(25).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    let middle = middle.freeze();
    let retained = middle.slice(6..13);
    drop(first);
    drop(last);
    drop(middle);
    assert_eq!(retained.as_slice(), b"payload");
    let fragmented = pool.usage();
    assert_eq!(fragmented.payload_available(), 11);
    assert_eq!(fragmented.payload_in_use(), 13);
    assert_eq!(fragmented.largest_free_extent(), 7);
    assert_eq!(fragmented.leases_available(), 5);
    assert_eq!(
        pool.try_acquire_at_least(8).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(retained);
    let coalesced = pool.usage();
    assert_eq!(coalesced.payload_available(), 24);
    assert_eq!(coalesced.largest_free_extent(), 24);
    assert_eq!(coalesced.leases_available(), 6);

    let whole_arena = pool.try_acquire_at_least(24).unwrap();
    assert_eq!(whole_arena.capacity(), 24);
    let start = whole_arena.as_ptr() as usize;
    let end = start + whole_arena.capacity();
    assert!(registered.iter().any(|region| {
        let registered_start = region.ptr.as_ptr() as usize;
        start >= registered_start && end <= registered_start + region.len
    }));
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(whole_arena);
    assert_eq!(pool.try_acquire_at_least(0).unwrap().capacity(), 4);
}

#[test]
fn short_send_remainders_preserve_order_and_release_completed_segments() {
    let pool = pool(16, 4, 4);
    let payload = SendPayload::Vectored(vec![
        filled(&pool, b"ab"),
        filled(&pool, b""),
        filled(&pool, b"cdef"),
        filled(&pool, b"gh"),
    ]);
    assert_eq!(payload.len(), 8);
    let payload = payload.remaining(3);
    let bytes: Vec<_> = payload
        .segments()
        .iter()
        .flat_map(|segment| segment.as_slice())
        .copied()
        .collect();
    assert_eq!(bytes, b"defgh");
    assert_eq!(payload.len(), 5);
    let released_prefix = pool.try_acquire_at_least(8).unwrap();
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(released_prefix);

    let payload = payload.remaining(3);
    assert_eq!(payload.segments()[0].as_slice(), b"gh");
    let payload = payload.remaining(2);
    assert!(payload.is_empty());
    let recovered = pool.try_acquire_at_least(16).unwrap();
    assert_eq!(recovered.capacity(), 16);
    drop(recovered);

    let single = SendPayload::Single(filled(&pool, b"data"));
    let single = single.remaining(1);
    assert_eq!(single.segments()[0].as_slice(), b"ata");
    assert!(single.remaining(3).is_empty());
    assert_eq!(pool.try_acquire_at_least(16).unwrap().capacity(), 16);
}

struct CpuArea {
    bytes: Box<[u8]>,
    drops: Arc<AtomicUsize>,
}

impl CpuArea {
    fn new(bytes: &[u8], drops: &Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            bytes: bytes.into(),
            drops: Arc::clone(drops),
        })
    }
}

// This test provider never writes the allocation after creation. Its boxed
// payload has a stable address and is retained until every published view ends.
unsafe impl ExternalMemory for CpuArea {
    fn as_ptr(&self) -> NonNull<u8> {
        NonNull::new(self.bytes.as_ptr().cast_mut()).unwrap()
    }
    fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl Drop for CpuArea {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct RefillQueue {
    capacity: usize,
    queued: Mutex<VecDeque<ReturnToken>>,
    accepted: Mutex<Vec<ReturnToken>>,
    retired: AtomicBool,
    attempts: AtomicUsize,
}

impl RefillQueue {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            queued: Mutex::new(VecDeque::new()),
            accepted: Mutex::new(Vec::new()),
            retired: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
        })
    }
    fn consume(&self) -> Option<ReturnToken> {
        self.queued.lock().pop_front()
    }
    fn accepted(&self) -> Vec<ReturnToken> {
        self.accepted.lock().clone()
    }
}

impl Recycle for RefillQueue {
    fn recycle(&self, token: ReturnToken) -> bool {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if !self.retired.load(Ordering::SeqCst) {
            let mut queued = self.queued.lock();
            if queued.len() == self.capacity {
                return false;
            }
            queued.push_back(token);
        }
        self.accepted.lock().push(token);
        true
    }
}

#[test]
fn external_memory_returns_exact_token_only_after_every_derived_view() {
    let pool = pool(8, 8, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"abcdefgh", &drops);
    let recycler = RefillQueue::new(1);
    let token = ReturnToken {
        offset: 8192,
        length: 4096,
        tag: 17,
    };
    let original = unsafe {
        pool.lease_external(memory.clone(), 2, 4, recycler.clone(), token)
            .unwrap()
    };
    let view = original.slice(1..3);
    let clone = view.clone();
    drop(memory);
    drop(original);
    drop(view);
    assert_eq!(clone.as_slice(), b"de");
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(recycler.accepted(), vec![]);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );

    // One local alias does not prove an external provider's allocation unique.
    let clone = clone.try_into_write().unwrap_err();
    assert_eq!(clone.as_slice(), b"de");
    drop(clone);
    assert_eq!(recycler.consume(), Some(token));
    assert_eq!(recycler.accepted(), vec![token]);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.try_acquire().unwrap().capacity(), 8);
}

#[test]
fn full_refill_queues_keep_every_token_and_backing_until_retry_accepts() {
    let pool = pool(8, 8, 2);
    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"abcdef", &drops);
    let recycler = RefillQueue::new(1);
    let a = ReturnToken {
        offset: 0,
        length: 2,
        tag: 10,
    };
    let b = ReturnToken {
        offset: 2,
        length: 2,
        tag: 11,
    };
    let c = ReturnToken {
        offset: 4,
        length: 2,
        tag: 12,
    };
    let first = unsafe {
        pool.lease_external(memory.clone(), 0, 2, recycler.clone(), a)
            .unwrap()
    };
    let second = unsafe {
        pool.lease_external(memory.clone(), 2, 2, recycler.clone(), b)
            .unwrap()
    };
    drop(first);
    drop(second);
    let third = unsafe {
        pool.lease_external(memory.clone(), 4, 2, recycler.clone(), c)
            .unwrap()
    };
    drop(third);
    drop(memory);

    assert_eq!(pool.pending_recycles(), 2);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    pool.flush_recycles();
    pool.flush_recycles();
    assert_eq!(pool.pending_recycles(), 2);
    assert_eq!(recycler.accepted(), vec![a]);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    assert_eq!(recycler.consume(), Some(a));
    pool.flush_recycles();
    assert_eq!(pool.pending_recycles(), 1);
    assert_eq!(recycler.accepted(), vec![a, b]);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let reclaimed_slot = pool.try_acquire().unwrap();
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(reclaimed_slot);

    assert_eq!(recycler.consume(), Some(b));
    pool.flush_recycles();
    assert_eq!(pool.pending_recycles(), 0);
    assert_eq!(recycler.consume(), Some(c));
    assert_eq!(recycler.accepted(), vec![a, b, c]);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.try_acquire().unwrap().capacity(), 8);
}

#[test]
fn external_rejection_preserves_caller_return_ownership() {
    let pool = pool(16, 8, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"abcd", &drops);
    let recycler = RefillQueue::new(1);
    let token = ReturnToken {
        offset: 0,
        length: 4,
        tag: 21,
    };
    let occupied = pool.try_acquire().unwrap();
    let result = unsafe { pool.lease_external(memory.clone(), 0, 4, recycler.clone(), token) };
    assert_eq!(result.unwrap_err().kind(), ErrorKind::WouldBlock);
    drop(occupied);
    let result =
        unsafe { pool.lease_external(memory.clone(), usize::MAX, 2, recycler.clone(), token) };
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(recycler.accepted(), vec![]);
    assert_eq!(pool.pending_recycles(), 0);

    let retried = unsafe {
        pool.lease_external(memory.clone(), 0, 4, recycler.clone(), token)
            .unwrap()
    };
    assert_eq!(retried.as_slice(), b"abcd");
    drop(retried);
    assert_eq!(recycler.accepted(), vec![token]);
}

#[test]
fn external_alias_can_outlive_pool_handle_and_provider_retirement() {
    let pool = pool(8, 8, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"alive", &drops);
    let recycler = RefillQueue::new(0);
    let token = ReturnToken {
        offset: 0,
        length: 5,
        tag: 31,
    };
    let view = unsafe {
        pool.lease_external(memory.clone(), 0, 5, recycler.clone(), token)
            .unwrap()
    };
    drop(memory);
    drop(pool);
    recycler.retired.store(true, Ordering::SeqCst);
    assert_eq!(view.as_slice(), b"alive");
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(view);
    assert_eq!(recycler.accepted(), vec![token]);
    assert_eq!(recycler.consume(), None);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn usage_does_not_retry_pending_returns_or_notify_the_owner() {
    let pool = pool(8, 8, 1);
    let notifications = Arc::new(WakeCount(AtomicUsize::new(0)));
    pool.set_recycle_waker(Waker::from(notifications.clone()));
    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"external", &drops);
    let recycler = RefillQueue::new(0);
    let token = ReturnToken {
        offset: 0,
        length: 8,
        tag: 40,
    };
    let external = unsafe {
        pool.lease_external(memory, 0, 8, recycler.clone(), token)
            .unwrap()
    };
    let live = pool.usage();
    assert_eq!(live.payload_available(), 8);
    assert_eq!(live.payload_in_use(), 0);
    assert_eq!(live.leases_in_use(), 1);
    assert_eq!(live.pending_recycles(), 0);
    drop(external);

    let pending = pool.usage();
    assert_eq!(pending.payload_available(), 8);
    assert_eq!(pending.payload_in_use(), 0);
    assert_eq!(pending.largest_free_extent(), 8);
    assert_eq!(pending.leases_available(), 0);
    assert_eq!(pending.pending_recycles(), 1);
    let notifications_before = notifications.0.load(Ordering::SeqCst);
    let attempts_before = recycler.attempts.load(Ordering::SeqCst);
    assert_eq!(attempts_before, 1);

    // The provider can now accept the return; observation must not retry it.
    recycler.retired.store(true, Ordering::SeqCst);
    let allocations = allocations_during(|| {
        for _ in 0..128 {
            assert_eq!(pool.usage(), pending);
        }
    });
    assert_eq!(allocations, 0, "pending-return usage must not allocate");
    assert_eq!(recycler.attempts.load(Ordering::SeqCst), attempts_before);
    assert_eq!(notifications.0.load(Ordering::SeqCst), notifications_before);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );

    pool.flush_recycles();
    assert_eq!(recycler.accepted(), vec![token]);
    assert_eq!(
        recycler.attempts.load(Ordering::SeqCst),
        attempts_before + 1
    );
    assert!(notifications.0.load(Ordering::SeqCst) > notifications_before);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.usage().pending_recycles(), 0);
    assert_eq!(pool.usage().leases_available(), 1);
    assert_eq!(pool.usage().payload_available(), 8);
}

#[test]
fn usage_remains_available_inside_recycle_and_wake_callbacks() {
    use std::cell::RefCell;

    thread_local! {
        #[cfg_attr(
            target_os = "android",
            allow(
                clippy::missing_const_for_thread_local,
                reason = "Already const; Android std TLS false positive (rust-lang/rust-clippy#13422)."
            )
        )]
        static OBSERVED_POOL: RefCell<Option<BufferPool>> = const { RefCell::new(None) };
    }
    #[derive(Default)]
    struct Observer(Mutex<Option<rivet::diagnostics::PoolUsage>>);
    impl Observer {
        fn observe(&self) {
            OBSERVED_POOL.with(|pool| {
                *self.0.lock() = Some(pool.borrow().as_ref().unwrap().usage());
            });
        }
    }
    impl Recycle for Observer {
        fn recycle(&self, _: ReturnToken) -> bool {
            self.observe();
            true
        }
    }
    impl Wake for Observer {
        fn wake(self: Arc<Self>) {
            self.observe();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
        }
    }

    let pool = pool(8, 8, 1);
    let empty = pool.usage();
    OBSERVED_POOL.with(|observed| *observed.borrow_mut() = Some(pool.clone()));
    let notifications = Arc::new(Observer::default());
    pool.set_recycle_waker(Waker::from(notifications.clone()));
    drop(pool.try_acquire().unwrap());
    assert_eq!(*notifications.0.lock(), Some(empty));

    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"external", &drops);
    let recycler = Arc::new(Observer::default());
    let external = unsafe {
        pool.lease_external(
            memory,
            0,
            8,
            recycler.clone(),
            ReturnToken {
                offset: 0,
                length: 8,
                tag: 42,
            },
        )
        .unwrap()
    };
    let occupied = pool.usage();
    drop(external);
    assert_eq!(*recycler.0.lock(), Some(occupied));
    assert_eq!(*notifications.0.lock(), Some(empty));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    OBSERVED_POOL.with(|observed| observed.borrow_mut().take());
}

#[test]
fn alias_uniqueness_and_pending_returns_wake_the_owner_without_idle_spin() {
    let pool = pool(8, 8, 1);
    let notifications = Arc::new(WakeCount(AtomicUsize::new(0)));
    pool.set_recycle_waker(Waker::from(notifications.clone()));
    let reserved = filled(&pool, b"data");
    let application = reserved.clone();
    let before = notifications.0.load(Ordering::SeqCst);
    drop(application);
    assert!(notifications.0.load(Ordering::SeqCst) > before);
    let writable = reserved.try_into_write().unwrap();
    let before = notifications.0.load(Ordering::SeqCst);
    drop(writable);
    assert!(notifications.0.load(Ordering::SeqCst) > before);

    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"x", &drops);
    let recycler = RefillQueue::new(0);
    let token = ReturnToken {
        offset: 0,
        length: 1,
        tag: 41,
    };
    let external = unsafe {
        pool.lease_external(memory, 0, 1, recycler.clone(), token)
            .unwrap()
    };
    let external_reserve = external.clone();
    let before = notifications.0.load(Ordering::SeqCst);
    drop(external);
    assert!(notifications.0.load(Ordering::SeqCst) > before);
    let before = notifications.0.load(Ordering::SeqCst);
    drop(external_reserve);
    assert!(notifications.0.load(Ordering::SeqCst) > before);
    let before = notifications.0.load(Ordering::SeqCst);
    pool.flush_recycles();
    assert_eq!(notifications.0.load(Ordering::SeqCst), before);
    recycler.retired.store(true, Ordering::SeqCst);
    pool.flush_recycles();
    assert!(notifications.0.load(Ordering::SeqCst) > before);
    assert_eq!(pool.pending_recycles(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn impossible_pool_limits_fail_before_acquisition() {
    for config in [
        PoolConfig {
            bytes: 0,
            block_size: 1,
            max_leases: 1,
        },
        PoolConfig {
            bytes: 8,
            block_size: 0,
            max_leases: 1,
        },
        PoolConfig {
            bytes: 8,
            block_size: 9,
            max_leases: 1,
        },
        PoolConfig {
            bytes: 8,
            block_size: 1,
            max_leases: 0,
        },
        PoolConfig {
            bytes: usize::MAX,
            block_size: 1,
            max_leases: 1,
        },
        PoolConfig {
            bytes: 8,
            block_size: 1,
            max_leases: usize::MAX,
        },
    ] {
        assert_eq!(
            BufferPool::new(config).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

struct CountReturns(AtomicUsize);

impl Recycle for CountReturns {
    fn recycle(&self, _: ReturnToken) -> bool {
        self.0.fetch_add(1, Ordering::SeqCst);
        true
    }
}

#[test]
fn repeated_normal_and_external_leases_reuse_preallocated_storage() {
    let pool = pool(8, 8, 1);
    let drops = Arc::new(AtomicUsize::new(0));
    let memory = CpuArea::new(b"external", &drops);
    let recycler = Arc::new(CountReturns(AtomicUsize::new(0)));
    let allocations = allocations_during(|| {
        for tag in 0..128 {
            let mut writable = pool.try_acquire().unwrap();
            writable.extend_from_slice(b"payload").unwrap();
            let published = writable.freeze();
            let alias = published.slice(1..4);
            assert_eq!(alias.as_slice(), b"ayl");
            drop(alias);
            let mut writable = published.try_into_write().unwrap();
            writable.clear();
            writable.extend_from_slice(b"reuse").unwrap();
            assert_eq!(writable.as_slice(), b"reuse");
            drop(writable);

            let external = unsafe {
                pool.lease_external(
                    memory.clone(),
                    0,
                    8,
                    recycler.clone(),
                    ReturnToken {
                        offset: 0,
                        length: 8,
                        tag,
                    },
                )
                .unwrap()
            };
            let alias = external.clone();
            drop(external);
            assert_eq!(alias.as_slice(), b"external");
            drop(alias);
            pool.flush_recycles();
        }
    });
    assert_eq!(
        allocations, 0,
        "warm leases must not allocate payload or control blocks"
    );
    assert_eq!(recycler.0.load(Ordering::SeqCst), 128);
}

#[test]
fn successful_pool_worker_and_udp_observation_allocates_nothing() {
    use futures_lite::future::poll_once;
    use rivet::{Optimization, Policy, Runtime, RuntimeConfig, SocketOptions, UdpSocket, runtime};
    use std::time::Duration;

    let mut config = RuntimeConfig::single_thread()
        .with_policy(Optimization::ProvidedBuffers, Policy::Off)
        .with_policy(Optimization::RegisteredBuffers, Policy::Off);
    config.limits.max_tasks = 4;
    config.limits.max_sockets = 1;
    config.limits.max_operations = 8;
    config.limits.max_pending_receives = 2;
    config.limits.max_pending_accepts = 2;
    config.limits.pool.bytes = 64 * 1024;
    config.limits.pool.block_size = 1024;
    config.limits.pool.max_leases = 64;
    let mut runtime = Runtime::new(config).unwrap();
    let pool = runtime.buffer_pool();
    let retained = filled(&pool, b"alive after runtime");
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();

    runtime.block_on(async {
        let socket = UdpSocket::bind_with_options(
            "127.0.0.1:0".parse().unwrap(),
            SocketOptions {
                receive_chunk: 1024,
                ..SocketOptions::udp()
            },
        )
        .unwrap();
        let mut receive = socket.recv();
        assert!(poll_once(&mut receive).await.is_none());
        let waiting = socket.receive_snapshot().unwrap();
        assert!(waiting.active());
        assert!(waiting.waiter_registered());
        assert_eq!(waiting.queue_capacity(), 2);
        assert_eq!(waiting.queued_results(), 0);
        assert_eq!(waiting.queue_available(), 2);
        let admitted = runtime::resource_snapshot().unwrap();
        assert_eq!(admitted.sockets(), 1);
        assert_eq!(admitted.socket_capacity(), 1);
        assert_eq!(admitted.available_socket_slots(), 0);
        assert_eq!(admitted.operations(), 1);
        assert_eq!(admitted.operation_capacity(), 8);
        assert_eq!(admitted.available_operation_slots(), 7);
        assert_eq!(
            UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .unwrap_err()
                .kind(),
            ErrorKind::WouldBlock
        );

        let bytes = b"queued observation";
        assert_eq!(
            sender.send_to(bytes, socket.local_addr()).unwrap(),
            bytes.len()
        );
        rivet::time::timeout(Duration::from_secs(5), async {
            while socket.receive_snapshot().unwrap().queued_results() != 1 {
                runtime::yield_now().await;
            }
        })
        .await
        .expect("native UDP result must reach the bounded receive queue");

        let expected_pool = pool.usage();
        let expected_worker = runtime::resource_snapshot().unwrap();
        let expected_receive = socket.receive_snapshot().unwrap();
        assert_eq!(expected_worker.sockets(), 1);
        assert_eq!(expected_worker.available_socket_slots(), 0);
        assert_eq!(expected_worker.driver().sockets(), 1);
        assert_eq!(expected_worker.queued_receives(), 1);
        assert_eq!(expected_worker.pool(), &expected_pool);
        assert_eq!(expected_receive.queued_results(), 1);
        assert_eq!(expected_receive.queue_available(), 1);
        assert!(expected_receive.waiter_registered());
        let allocations = allocations_during(|| {
            for _ in 0..128 {
                assert_eq!(pool.usage(), expected_pool);
                assert_eq!(runtime::resource_snapshot().unwrap(), expected_worker);
                assert_eq!(socket.receive_snapshot().unwrap(), expected_receive);
            }
        });
        assert_eq!(
            allocations, 0,
            "successful resource queries must not allocate"
        );

        let received = receive.await.unwrap();
        assert_eq!(received.data.as_slice(), bytes);
        assert_eq!(received.peer, Some(sender.local_addr().unwrap()));
    });

    drop(runtime);
    let after_runtime = pool.usage();
    assert_eq!(after_runtime.payload_capacity(), 64 * 1024);
    assert_eq!(after_runtime.payload_in_use(), 1024);
    assert_eq!(after_runtime.payload_available(), 63 * 1024);
    assert_eq!(after_runtime.leases_in_use(), 1);
    assert_eq!(after_runtime.leases_available(), 63);
    assert_eq!(after_runtime.pending_recycles(), 0);
    assert_eq!(retained.as_slice(), b"alive after runtime");
    let allocations = allocations_during(|| {
        for _ in 0..128 {
            assert_eq!(pool.usage(), after_runtime);
        }
    });
    assert_eq!(
        allocations, 0,
        "retained pool observation must not allocate"
    );
    drop(retained);
    let released = pool.usage();
    assert_eq!(released.payload_available(), released.payload_capacity());
    assert_eq!(released.largest_free_extent(), released.payload_capacity());
    assert_eq!(released.leases_available(), released.lease_capacity());
}
