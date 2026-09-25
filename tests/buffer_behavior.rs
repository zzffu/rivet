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

    for (destination, byte) in buffer.spare_capacity_mut().iter_mut().zip(*b"abc") {
        destination.write(byte);
    }
    unsafe {
        buffer.set_initialized_len(3);
    }
    buffer.extend_from_slice(b"de").unwrap();
    assert_eq!(buffer.as_slice(), b"abcde");
    assert_eq!(
        buffer.extend_from_slice(b"toolong").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(buffer.as_slice(), b"abcde");
    assert_eq!(buffer.spare_capacity_mut().len(), 3);

    buffer.clear();
    assert_eq!(buffer.as_slice(), b"");
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
fn every_alias_including_empty_ranges_keeps_memory_unwritable() {
    let pool = pool(8, 8, 1);
    let original = filled(&pool, b"abcdef");
    let clone = original.clone();
    let middle = original.slice(2..5);
    let empty = middle.slice(1..1);
    drop(original);
    drop(clone);
    assert_eq!(middle.as_slice(), b"cde");
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(middle);
    assert!(empty.is_empty());
    assert_eq!(
        pool.try_acquire().unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(empty);
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
    assert_eq!(writable.capacity(), 6);
    writable.extend_from_slice(b"XY").unwrap();
    assert_eq!(writable.as_slice(), b"cdXY");
    writable.clear();
    writable.extend_from_slice(b"reuse!").unwrap();
    assert_eq!(writable.as_slice(), b"reuse!");
    drop(writable);

    let restored = pool.try_acquire_at_least(8).unwrap();
    assert_eq!(restored.capacity(), 8);
    assert_eq!(restored.initialized_len(), 0);
}

#[test]
fn variable_size_allocations_obey_budget_and_coalesce_after_last_alias() {
    let pool = pool(24, 4, 6);
    let registered = pool.regions();
    let first = pool.try_acquire().unwrap();
    let mut middle = pool.try_acquire_at_least(13).unwrap();
    middle.extend_from_slice(b"large-payload").unwrap();
    let last = pool.try_acquire_at_least(7).unwrap();
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
    assert_eq!(
        pool.try_acquire_at_least(13).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    drop(retained);

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
}

impl RefillQueue {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            queued: Mutex::new(VecDeque::new()),
            accepted: Mutex::new(Vec::new()),
            retired: AtomicBool::new(false),
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
