#[cfg(feature = "provided-buffers")]
use super::{Notifier, ring::Mapping};
use super::{ring::Ring, uapi::*};
#[cfg(any(feature = "registered-buffers", feature = "zc-tx-fixed"))]
use crate::buffer::MemoryRegion;
#[cfg(feature = "provided-buffers")]
use crate::buffer::{BufferPool, ExternalMemory, ReadBuf, Recycle, ReturnToken};
#[cfg(feature = "provided-buffers")]
use crossbeam_queue::ArrayQueue;
use std::io;
#[cfg(feature = "fixed-files")]
use std::os::fd::RawFd;
#[cfg(feature = "provided-buffers")]
use std::{
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[cfg(feature = "fixed-files")]
pub struct FixedFiles {
    free: Vec<u32>,
    ordinary_slots: u32,
}
#[cfg(feature = "fixed-files")]
impl FixedFiles {
    pub fn new(ring: &Ring, capacity: usize, direct: bool) -> io::Result<Self> {
        let ordinary_slots =
            u32::try_from(capacity).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let total = ordinary_slots
            .checked_mul(if cfg!(feature = "direct-descriptors") && direct {
                2
            } else {
                1
            })
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        let reg = ResourceRegister {
            nr: total,
            flags: IORING_RSRC_REGISTER_SPARSE,
            ..ResourceRegister::default()
        };
        unsafe {
            ring.register(
                IORING_REGISTER_FILES2,
                (&reg as *const ResourceRegister).cast(),
                size_of::<ResourceRegister>() as u32,
            )?;
        }
        #[cfg(feature = "direct-descriptors")]
        if direct {
            // Kernel-allocated direct descriptors never collide with userspace's fixed slots.
            let range = FileRange {
                off: ordinary_slots,
                len: ordinary_slots,
                resv: 0,
            };
            if let Err(error) = unsafe {
                ring.register(
                    IORING_REGISTER_FILE_ALLOC_RANGE,
                    (&range as *const FileRange).cast(),
                    0,
                )
            } {
                unsafe {
                    let _ = ring.register(IORING_UNREGISTER_FILES, std::ptr::null(), 0);
                }
                return Err(error);
            }
        }
        Ok(Self {
            free: (0..ordinary_slots).rev().collect(),
            ordinary_slots,
        })
    }
    pub fn insert(&mut self, ring: &Ring, fd: RawFd) -> io::Result<u32> {
        let index = self
            .free
            .pop()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENFILE))?;
        if let Err(error) = self.update(ring, index, fd) {
            self.free.push(index);
            return Err(error);
        }
        Ok(index)
    }
    fn update(&self, ring: &Ring, index: u32, fd: RawFd) -> io::Result<()> {
        let update = ResourceUpdate {
            offset: index,
            data: &fd as *const RawFd as u64,
            ..ResourceUpdate::default()
        };
        unsafe {
            ring.register(
                IORING_REGISTER_FILES_UPDATE,
                (&update as *const ResourceUpdate).cast(),
                1,
            )?;
        }
        Ok(())
    }
    pub fn remove(&mut self, ring: &Ring, index: u32) -> io::Result<()> {
        self.update(ring, index, -1)?;
        if index < self.ordinary_slots {
            self.free.push(index);
        }
        Ok(())
    }
}

#[cfg(feature = "registered-buffers")]
pub struct RegisteredBuffers {
    pub regions: Vec<MemoryRegion>,
}
#[cfg(feature = "registered-buffers")]
impl RegisteredBuffers {
    pub fn new(ring: &Ring, mut regions: Vec<MemoryRegion>) -> io::Result<Self> {
        for (index, region) in regions.iter_mut().enumerate() {
            region.id = index as u32;
        }
        if regions.len() > u16::MAX as usize + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "too many registered buffer regions",
            ));
        }
        let iovecs: Vec<libc::iovec> = regions
            .iter()
            .map(|region| libc::iovec {
                iov_base: region.ptr.as_ptr().cast(),
                iov_len: region.len,
            })
            .collect();
        unsafe {
            ring.register(
                IORING_REGISTER_BUFFERS,
                iovecs.as_ptr().cast(),
                iovecs.len() as u32,
            )?;
        }
        Ok(Self { regions })
    }
    pub fn index(&self, ptr: *const u8, length: usize) -> Option<u16> {
        let start = ptr as usize;
        let end = start.checked_add(length)?;
        self.regions.iter().enumerate().find_map(|(index, region)| {
            let base = region.ptr.as_ptr() as usize;
            (start >= base && end <= base + region.len).then_some(index as u16)
        })
    }
}

#[cfg(feature = "provided-buffers")]
struct Memory(Mapping);
// The allocator never creates references over kernel-writable bytes. Published
// immutable leases cover only consumed prefixes; suffixes remain kernel owned.
#[cfg(feature = "provided-buffers")]
unsafe impl Send for Memory {}
#[cfg(feature = "provided-buffers")]
unsafe impl Sync for Memory {}
#[cfg(feature = "provided-buffers")]
unsafe impl ExternalMemory for Memory {
    fn as_ptr(&self) -> NonNull<u8> {
        NonNull::new(self.0.as_ptr()).unwrap()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
#[cfg(feature = "provided-buffers")]
const KERNEL: usize = 1usize << (usize::BITS - 1);
#[cfg(feature = "provided-buffers")]
struct Returns {
    state: Box<[AtomicUsize]>,
    ready: ArrayQueue<u16>,
    notifier: Arc<Notifier>,
}
#[cfg(feature = "provided-buffers")]
impl Returns {
    fn returned(&self, bid: u16) {
        // A buffer transitions to ready only once: the kernel or its last lease
        // clears the last reference. Queue capacity equals the buffer count.
        assert!(
            self.ready.push(bid).is_ok(),
            "duplicate provided buffer return"
        );
        self.notifier.notify();
    }
    fn kernel_done(&self, bid: u16) {
        let old = self.state[bid as usize].fetch_and(!KERNEL, Ordering::AcqRel);
        if old == KERNEL {
            self.returned(bid);
        }
    }
}
#[cfg(feature = "provided-buffers")]
impl Recycle for Returns {
    fn recycle(&self, token: ReturnToken) -> bool {
        let old = self.state[token.tag as usize].fetch_sub(1, Ordering::AcqRel);
        assert!(old & !KERNEL != 0, "provided buffer reference underflow");
        if old == 1 {
            self.returned(token.tag as u16);
        }
        true
    }
}

#[cfg(feature = "provided-buffers")]
/// One owner publishes descriptors; application releases only enqueue IDs.
/// Incremental ranges are never republished while either kernel suffix or any
/// application prefix remains outstanding.
pub struct ProvidedBuffers {
    memory: Arc<Memory>,
    returns: Arc<Returns>,
    descriptors: Mapping,
    #[cfg(feature = "buffer-bundles")]
    positions: Box<[u16]>,
    #[cfg(feature = "buffer-bundles")]
    order: Box<[u16]>,
    #[cfg(feature = "incremental-buffers")]
    consumed: Box<[usize]>,
    tail: u16,
    entries: u16,
    pub block_bytes: usize,
    pub group: u16,
}

#[cfg(feature = "provided-buffers")]
/// One kernel completion's immutable range. Its retained reference exists
/// before application credit or lease metadata is available.
#[derive(Clone, Copy)]
pub struct ProvidedRange {
    token: ReturnToken,
}
#[cfg(feature = "provided-buffers")]
impl ProvidedBuffers {
    pub fn new(
        ring: &Ring,
        bytes: usize,
        requested_block: usize,
        incremental: bool,
        notifier: Arc<Notifier>,
    ) -> io::Result<Self> {
        // recvmsg needs space for its result header, source address and cmsgs.
        let block_bytes = requested_block
            .max(65536 + 16 + size_of::<libc::sockaddr_storage>() + super::net::CONTROL_BYTES);
        if block_bytes > i32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "provided buffer size exceeds the network CQE byte range",
            ));
        }
        let available = (bytes / block_bytes).min(32768);
        if available == 0 {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "provided-buffer budget cannot hold one UDP receive block",
            ));
        }
        let entries = 1usize << available.ilog2();
        let memory = Arc::new(Memory(Mapping::anonymous(entries * block_bytes)?));
        let descriptors = Mapping::anonymous(entries * size_of::<Buf>())?;
        let returns = Arc::new(Returns {
            state: (0..entries).map(|_| AtomicUsize::new(0)).collect(),
            ready: ArrayQueue::new(entries),
            notifier,
        });
        let incremental = cfg!(feature = "incremental-buffers") && incremental;
        let reg = BufReg {
            ring_addr: descriptors.as_ptr() as u64,
            ring_entries: entries as u32,
            bgid: 1,
            flags: if incremental { IOU_PBUF_RING_INC } else { 0 },
            min_left: if incremental {
                (16 + size_of::<libc::sockaddr_storage>() + super::net::CONTROL_BYTES + 1) as u32
            } else {
                0
            },
            ..BufReg::default()
        };
        unsafe {
            ring.register(IORING_REGISTER_PBUF_RING, (&reg as *const BufReg).cast(), 1)?;
        }
        let mut buffers = Self {
            memory,
            returns,
            descriptors,
            #[cfg(feature = "buffer-bundles")]
            positions: vec![0; entries].into_boxed_slice(),
            #[cfg(feature = "buffer-bundles")]
            order: vec![0; entries].into_boxed_slice(),
            #[cfg(feature = "incremental-buffers")]
            consumed: vec![0; entries].into_boxed_slice(),
            tail: 0,
            entries: entries as u16,
            block_bytes,
            group: 1,
        };
        for bid in 0..entries {
            buffers.publish(bid as u16);
        }
        buffers.publish_tail();
        Ok(buffers)
    }
    fn publish(&mut self, bid: u16) {
        let index = self.tail & (self.entries - 1);
        #[cfg(feature = "buffer-bundles")]
        {
            self.positions[bid as usize] = self.tail;
            self.order[index as usize] = bid;
        }
        #[cfg(feature = "incremental-buffers")]
        {
            self.consumed[bid as usize] = 0;
        }
        self.returns.state[bid as usize].store(KERNEL, Ordering::Release);
        let desc = unsafe { self.descriptors.as_ptr().cast::<Buf>().add(index as usize) };
        // Entry zero's resv aliases tail; never overwrite it while publishing.
        unsafe {
            std::ptr::addr_of_mut!((*desc).addr)
                .write(self.memory.0.as_ptr().add(bid as usize * self.block_bytes) as u64);
            std::ptr::addr_of_mut!((*desc).len).write(self.block_bytes as u32);
            std::ptr::addr_of_mut!((*desc).bid).write(bid);
        }
        self.tail = self.tail.wrapping_add(1);
    }
    fn publish_tail(&self) {
        let tail = unsafe {
            &*self
                .descriptors
                .as_ptr()
                .add(14)
                .cast::<std::sync::atomic::AtomicU16>()
        };
        tail.store(self.tail, Ordering::Release);
    }
    pub fn flush(&mut self) -> usize {
        let mut count = 0;
        while let Some(bid) = self.returns.ready.pop() {
            self.publish(bid);
            count += 1;
        }
        if count != 0 {
            self.publish_tail();
        }
        count
    }
    pub fn entries(&self) -> usize {
        self.entries as usize
    }
    #[cfg(feature = "zc-tx-fixed")]
    pub fn memory_region(&self, id: u32) -> MemoryRegion {
        MemoryRegion {
            ptr: self.memory.as_ptr(),
            len: self.memory.len(),
            id,
        }
    }
    pub fn remaining(&self, bid: u16) -> io::Result<usize> {
        if bid >= self.entries {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown provided buffer ID",
            ));
        }
        let remaining = self.block_bytes;
        #[cfg(feature = "incremental-buffers")]
        let remaining = remaining - self.consumed[bid as usize];
        Ok(remaining)
    }
    #[cfg(feature = "buffer-bundles")]
    pub fn next_bid(&self, bid: u16) -> u16 {
        let next = self.positions[bid as usize].wrapping_add(1) & (self.entries - 1);
        self.order[next as usize]
    }
    pub fn reserve(&mut self, bid: u16, bytes: usize, more: bool) -> io::Result<ProvidedRange> {
        if bytes > self.remaining(bid)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "provided buffer completion exceeds available range",
            ));
        }
        let offset = bid as usize * self.block_bytes;
        #[cfg(feature = "incremental-buffers")]
        let offset = offset + self.consumed[bid as usize];
        self.returns.state[bid as usize].fetch_add(1, Ordering::AcqRel);
        #[cfg(feature = "incremental-buffers")]
        {
            self.consumed[bid as usize] += bytes;
        }
        if !(cfg!(feature = "incremental-buffers") && more) {
            self.returns.kernel_done(bid);
        }
        Ok(ProvidedRange {
            token: ReturnToken {
                offset: offset as u64,
                length: bytes as u32,
                tag: bid as u32,
            },
        })
    }
    pub fn lease(&self, pool: &BufferPool, range: ProvidedRange) -> io::Result<ReadBuf> {
        // On failure the raw completion still owns the reserved reference.
        unsafe {
            pool.lease_external(
                self.memory.clone(),
                range.token.offset as usize,
                range.token.length as usize,
                self.returns.clone(),
                range.token,
            )
        }
    }
    pub fn discard(&self, range: ProvidedRange) {
        self.returns.recycle(range.token);
    }
}

#[cfg(feature = "buffer-bundles")]
/// Immutable send leases are published without copying into a dedicated group.
/// One group per operation prevents bundling data from unrelated TCP streams.
pub struct SendBundle {
    descriptors: Mapping,
    pub group: u16,
    entries: u16,
    tail: u16,
}
#[cfg(feature = "buffer-bundles")]
impl SendBundle {
    pub fn new(ring: &Ring, group: u16, max_iovecs: usize) -> io::Result<Self> {
        let entries = max_iovecs.next_power_of_two().min(32768) as u16;
        let descriptors = Mapping::anonymous(entries as usize * size_of::<Buf>())?;
        let reg = BufReg {
            ring_addr: descriptors.as_ptr() as u64,
            ring_entries: entries as u32,
            bgid: group,
            ..BufReg::default()
        };
        unsafe {
            ring.register(IORING_REGISTER_PBUF_RING, (&reg as *const BufReg).cast(), 1)?;
        }
        Ok(Self {
            descriptors,
            group,
            entries,
            tail: 0,
        })
    }
    pub fn publish(&mut self, segments: &[crate::buffer::SendBuf]) {
        for (bid, segment) in segments.iter().enumerate() {
            if segment.is_empty() {
                continue;
            }
            let index = self.tail & (self.entries - 1);
            let desc = unsafe { self.descriptors.as_ptr().cast::<Buf>().add(index as usize) };
            unsafe {
                std::ptr::addr_of_mut!((*desc).addr).write(segment.as_ptr() as u64);
                std::ptr::addr_of_mut!((*desc).len).write(segment.len() as u32);
                std::ptr::addr_of_mut!((*desc).bid).write(bid as u16);
            }
            self.tail = self.tail.wrapping_add(1);
        }
        unsafe {
            &*self
                .descriptors
                .as_ptr()
                .add(14)
                .cast::<std::sync::atomic::AtomicU16>()
        }
        .store(self.tail, Ordering::Release);
    }
    pub fn unregister(&self, ring: &Ring) -> io::Result<()> {
        let reg = BufReg {
            bgid: self.group,
            ..BufReg::default()
        };
        unsafe {
            ring.register(
                IORING_UNREGISTER_PBUF_RING,
                (&reg as *const BufReg).cast(),
                1,
            )?;
        }
        Ok(())
    }
    pub fn reset(&mut self, ring: &Ring) -> io::Result<()> {
        // A short/error send may leave descriptors unconsumed. Unregister/re-register
        // clears that queue before any new payload can occupy this operation slot.
        let reg = BufReg {
            bgid: self.group,
            ..BufReg::default()
        };
        unsafe {
            ring.register(
                IORING_UNREGISTER_PBUF_RING,
                (&reg as *const BufReg).cast(),
                1,
            )?;
        }
        self.tail = 0;
        unsafe {
            &*self
                .descriptors
                .as_ptr()
                .add(14)
                .cast::<std::sync::atomic::AtomicU16>()
        }
        .store(0, Ordering::Release);
        let reg = BufReg {
            ring_addr: self.descriptors.as_ptr() as u64,
            ring_entries: self.entries as u32,
            bgid: self.group,
            ..BufReg::default()
        };
        unsafe {
            ring.register(IORING_REGISTER_PBUF_RING, (&reg as *const BufReg).cast(), 1)?;
        }
        Ok(())
    }
}
