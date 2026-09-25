use super::uapi::*;
use std::{
    io,
    marker::PhantomData,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr::{self, NonNull},
    rc::Rc,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

/// An mmap remains stable; payload mappings may be shared, SQ/CQ mappings may not.
pub struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}
impl Mapping {
    pub fn anonymous(len: usize) -> io::Result<Self> {
        Self::map(-1, len, 0, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS)
    }
    pub fn shared(fd: RawFd, len: usize, offset: i64) -> io::Result<Self> {
        Self::map(fd, len, offset, libc::MAP_SHARED)
    }
    fn map(fd: RawFd, len: usize, offset: i64, flags: i32) -> io::Result<Self> {
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty mapping"));
        }
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd,
                offset as libc::off_t,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(raw.cast()).expect("mmap returned null"),
            len,
        })
    }
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
    #[cfg(any(feature = "provided-buffers", feature = "zc-rx"))]
    pub fn len(&self) -> usize {
        self.len
    }
}
impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

pub struct Ring {
    fd: OwnedFd,
    params: Params,
    sq: Mapping,
    cq: Option<Mapping>,
    sqes: Mapping,
    sq_tail: u32,
    published: u32,
    cq_head: u32,
    cq_tail: u32,
    #[cfg(feature = "sq-rewind")]
    rewind_pending: u32,
    #[cfg(feature = "registered-ring")]
    registered_index: Option<u32>,
    #[cfg(feature = "registered-wait")]
    wait_region: Option<Mapping>,
    _local: PhantomData<Rc<()>>,
}

impl Ring {
    pub fn new(entries: u32, mut params: Params) -> io::Result<Self> {
        let raw = unsafe { libc::syscall(libc::SYS_io_uring_setup, entries, &mut params) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw as RawFd) };
        if params.features & (IORING_FEAT_NODROP | IORING_FEAT_EXT_ARG)
            != IORING_FEAT_NODROP | IORING_FEAT_EXT_ARG
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring requires NODROP and EXT_ARG",
            ));
        }
        let stride = if cfg!(feature = "zc-rx") && params.flags & IORING_SETUP_CQE32 != 0 {
            32
        } else {
            16
        };
        let sq_len = (params.sq_off.array as usize
            + if params.flags & IORING_SETUP_NO_SQARRAY == 0 {
                params.sq_entries as usize * 4
            } else {
                0
            })
        .max(params.sq_off.flags as usize + 4)
        .max(params.sq_off.dropped as usize + 4);
        let cq_len = params.cq_off.cqes as usize + params.cq_entries as usize * stride;
        let single = params.features & IORING_FEAT_SINGLE_MMAP != 0;
        let sq = Mapping::shared(
            fd.as_raw_fd(),
            if single { sq_len.max(cq_len) } else { sq_len },
            IORING_OFF_SQ_RING,
        )?;
        let cq = if single {
            None
        } else {
            Some(Mapping::shared(fd.as_raw_fd(), cq_len, IORING_OFF_CQ_RING)?)
        };
        let sqes = Mapping::shared(
            fd.as_raw_fd(),
            params.sq_entries as usize * size_of::<Sqe>(),
            IORING_OFF_SQES,
        )?;
        if params.flags & IORING_SETUP_NO_SQARRAY == 0 {
            for i in 0..params.sq_entries {
                unsafe {
                    sq.as_ptr()
                        .add(params.sq_off.array as usize)
                        .cast::<u32>()
                        .add(i as usize)
                        .write(i);
                }
            }
        }
        Ok(Self {
            fd,
            params,
            sq,
            cq,
            sqes,
            sq_tail: 0,
            published: 0,
            cq_head: 0,
            cq_tail: 0,
            #[cfg(feature = "sq-rewind")]
            rewind_pending: 0,
            #[cfg(feature = "registered-ring")]
            registered_index: None,
            #[cfg(feature = "registered-wait")]
            wait_region: None,
            _local: PhantomData,
        })
    }
    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
    #[cfg(feature = "buffer-bundles")]
    pub fn features(&self) -> u32 {
        self.params.features
    }
    #[cfg(feature = "zc-rx")]
    pub fn flags(&self) -> u32 {
        self.params.flags
    }
    fn sq_atomic(&self, offset: u32) -> &AtomicU32 {
        unsafe { &*self.sq.as_ptr().add(offset as usize).cast() }
    }
    fn cq_base(&self) -> *mut u8 {
        self.cq.as_ref().unwrap_or(&self.sq).as_ptr()
    }
    fn cq_atomic(&self, offset: u32) -> &AtomicU32 {
        unsafe { &*self.cq_base().add(offset as usize).cast() }
    }
    pub fn available(&self) -> u32 {
        #[cfg(feature = "sq-rewind")]
        if self.params.flags & IORING_SETUP_SQ_REWIND != 0 {
            return self.params.sq_entries - self.rewind_pending;
        }
        self.params.sq_entries.saturating_sub(
            self.sq_tail.wrapping_sub(
                self.sq_atomic(self.params.sq_off.head)
                    .load(Ordering::Acquire),
            ),
        )
    }
    pub fn push(&mut self, sqe: Sqe) -> io::Result<()> {
        if self.available() == 0 {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        #[cfg(feature = "sq-rewind")]
        if self.params.flags & IORING_SETUP_SQ_REWIND != 0 {
            unsafe {
                self.sqes
                    .as_ptr()
                    .cast::<Sqe>()
                    .add(self.rewind_pending as usize)
                    .write(sqe);
            }
            self.rewind_pending += 1;
            return Ok(());
        }
        let index = self.sq_tail & (self.params.sq_entries - 1);
        unsafe {
            self.sqes
                .as_ptr()
                .cast::<Sqe>()
                .add(index as usize)
                .write(sqe);
        }
        self.sq_tail = self.sq_tail.wrapping_add(1);
        Ok(())
    }
    pub fn submit(&mut self) -> io::Result<usize> {
        self.enter(None, false)
    }
    pub fn wait(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.enter(timeout, true)
    }
    pub fn has_completions(&self) -> bool {
        self.cq_atomic(self.params.cq_off.tail)
            .load(Ordering::Acquire)
            != self.cq_head
    }
    fn enter(&mut self, timeout: Option<Duration>, wait: bool) -> io::Result<usize> {
        self.flush_completions();
        let sqpoll = cfg!(feature = "uring-sqpoll") && self.params.flags & IORING_SETUP_SQPOLL != 0;
        let submitted = match () {
            #[cfg(feature = "sq-rewind")]
            _ if self.params.flags & IORING_SETUP_SQ_REWIND != 0 => self.rewind_pending,
            _ => {
                if self.published != self.sq_tail {
                    self.sq_atomic(self.params.sq_off.tail)
                        .store(self.sq_tail, Ordering::Release);
                    self.published = self.sq_tail;
                }
                self.sq_tail.wrapping_sub(
                    self.sq_atomic(self.params.sq_off.head)
                        .load(Ordering::Acquire),
                )
            }
        };
        // SQPOLL's sleep handshake pairs its full barrier with this one after publishing tail.
        if sqpoll {
            std::sync::atomic::fence(Ordering::SeqCst);
        }
        let sq_flags = self
            .sq_atomic(self.params.sq_off.flags)
            .load(Ordering::Acquire);
        let nonblocking = timeout.is_some_and(|t| t.is_zero());
        let mut flags = if wait || sq_flags & (IORING_SQ_CQ_OVERFLOW | IORING_SQ_TASKRUN) != 0 {
            IORING_ENTER_GETEVENTS
        } else {
            0
        };
        if sqpoll && sq_flags & IORING_SQ_NEED_WAKEUP != 0 {
            flags |= IORING_ENTER_SQ_WAKEUP;
        }
        if sqpoll && flags == 0 {
            return Ok(submitted as usize);
        }
        if !sqpoll && submitted == 0 && flags == 0 {
            return Ok(0);
        }
        let minimum = u32::from(wait && !nonblocking && !self.has_completions());
        let ts = timeout.map(|t| Timespec {
            sec: t.as_secs().min(i64::MAX as u64) as i64,
            nsec: t.subsec_nanos() as i64,
        });
        let arg = GetEventsArg {
            ts: ts.as_ref().map_or(0, |t| t as *const _ as u64),
            ..GetEventsArg::default()
        };
        let (arg_ptr, arg_len) = match () {
            #[cfg(feature = "registered-wait")]
            _ if self.wait_region.is_some() => {
                let reg = unsafe {
                    &mut *self
                        .wait_region
                        .as_ref()
                        .unwrap()
                        .as_ptr()
                        .cast::<RegWait>()
                };
                reg.ts = ts.unwrap_or_default();
                reg.flags = if timeout.is_some() {
                    IORING_REG_WAIT_TS
                } else {
                    0
                };
                flags |= IORING_ENTER_EXT_ARG | IORING_ENTER_EXT_ARG_REG;
                (ptr::null::<libc::c_void>(), size_of::<RegWait>())
            }
            _ => {
                flags |= IORING_ENTER_EXT_ARG;
                (
                    &arg as *const _ as *const libc::c_void,
                    size_of::<GetEventsArg>(),
                )
            }
        };
        let fd = self.fd();
        #[cfg(feature = "registered-ring")]
        let fd = if let Some(index) = self.registered_index {
            flags |= IORING_ENTER_REGISTERED_RING;
            index as i32
        } else {
            fd
        };
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                fd,
                if sqpoll { 0 } else { submitted },
                minimum,
                flags,
                arg_ptr,
                arg_len,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(libc::ETIME | libc::EINTR)) {
                return Ok(0);
            }
            return Err(error);
        }
        #[cfg(feature = "sq-rewind")]
        if self.params.flags & IORING_SETUP_SQ_REWIND != 0 {
            let consumed = (result as u32).min(self.rewind_pending);
            self.rewind_pending -= consumed;
            if self.rewind_pending != 0 {
                unsafe {
                    ptr::copy(
                        self.sqes.as_ptr().cast::<Sqe>().add(consumed as usize),
                        self.sqes.as_ptr().cast::<Sqe>(),
                        self.rewind_pending as usize,
                    );
                }
            }
            // REWIND must never publish SQ head/tail, including partial submits.
        }
        Ok(result as usize)
    }
    pub fn pop(&mut self) -> Option<Cqe> {
        loop {
            if self.cq_head == self.cq_tail {
                self.cq_tail = self
                    .cq_atomic(self.params.cq_off.tail)
                    .load(Ordering::Acquire);
                if self.cq_head == self.cq_tail {
                    return None;
                }
            }
            let wide = cfg!(feature = "zc-rx") && self.params.flags & IORING_SETUP_CQE32 != 0;
            let index = self.cq_head & (self.params.cq_entries - 1);
            let ptr = unsafe {
                self.cq_base().add(
                    self.params.cq_off.cqes as usize + index as usize * if wide { 32 } else { 16 },
                )
            };
            let cqe = unsafe { ptr.cast::<Cqe16>().read() };
            let mixed_wide = cfg!(feature = "mixed-cqe")
                && !wide
                && self.params.flags & IORING_SETUP_CQE_MIXED != 0
                && cqe.flags & IORING_CQE_F_32 != 0;
            let extra = if wide || mixed_wide {
                unsafe { ptr.add(16).cast::<[u64; 2]>().read() }
            } else {
                [0; 2]
            };
            self.cq_head = self.cq_head.wrapping_add(if mixed_wide { 2 } else { 1 });
            if cfg!(feature = "mixed-cqe") && cqe.flags & IORING_CQE_F_SKIP != 0 {
                continue;
            }
            return Some(Cqe {
                user_data: cqe.user_data,
                res: cqe.res,
                flags: cqe.flags,
                extra,
            });
        }
    }
    /// Publish one CQ head update for a whole completion batch.
    pub fn flush_completions(&self) {
        self.cq_atomic(self.params.cq_off.head)
            .store(self.cq_head, Ordering::Release);
    }
    /// The pointed-to registration structure must match the selected UAPI command.
    pub unsafe fn register(&self, op: u32, arg: *const libc::c_void, nr: u32) -> io::Result<i32> {
        let result = unsafe { libc::syscall(libc::SYS_io_uring_register, self.fd(), op, arg, nr) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as i32)
        }
    }
    pub fn probe(&self) -> io::Result<Probe> {
        let mut probe = Probe::default();
        unsafe {
            self.register(
                IORING_REGISTER_PROBE,
                (&mut probe as *mut Probe).cast(),
                256,
            )?;
        }
        Ok(probe)
    }
    #[cfg(feature = "registered-ring")]
    pub fn register_ring(&mut self) -> io::Result<()> {
        let mut update = ResourceUpdate {
            offset: u32::MAX,
            data: self.fd() as u64,
            ..ResourceUpdate::default()
        };
        unsafe {
            self.register(
                IORING_REGISTER_RING_FDS,
                (&mut update as *mut ResourceUpdate).cast(),
                1,
            )?;
        }
        self.registered_index = Some(update.offset);
        Ok(())
    }
    #[cfg(feature = "registered-wait")]
    pub fn register_wait(&mut self) -> io::Result<()> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let region = Mapping::anonymous(page)?;
        let mut desc = RegionDesc {
            user_addr: region.as_ptr() as u64,
            size: page as u64,
            flags: IORING_MEM_REGION_TYPE_USER,
            ..RegionDesc::default()
        };
        let reg = MemRegionReg {
            region_uptr: &mut desc as *mut _ as u64,
            flags: IORING_MEM_REGION_REG_WAIT_ARG,
            ..MemRegionReg::default()
        };
        unsafe {
            self.register(
                IORING_REGISTER_MEM_REGION,
                (&reg as *const MemRegionReg).cast(),
                1,
            )?;
        }
        self.wait_region = Some(region);
        Ok(())
    }
    #[cfg(feature = "registered-wait")]
    pub fn enable(&mut self) -> io::Result<()> {
        if self.params.flags & IORING_SETUP_R_DISABLED != 0 {
            unsafe {
                self.register(IORING_REGISTER_ENABLE_RINGS, ptr::null(), 0)?;
            }
            self.params.flags &= !IORING_SETUP_R_DISABLED;
        }
        Ok(())
    }
    pub fn cancel_all(&mut self) -> io::Result<()> {
        self.submit()?;
        let cancel = SyncCancel {
            fd: -1,
            flags: IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL,
            timeout: Timespec { sec: -1, nsec: -1 },
            ..SyncCancel::default()
        };
        match unsafe {
            self.register(
                IORING_REGISTER_SYNC_CANCEL,
                (&cancel as *const SyncCancel).cast(),
                1,
            )
        } {
            Err(e) if e.raw_os_error() != Some(libc::ENOENT) => Err(e),
            _ => Ok(()),
        }
    }
}
#[cfg(feature = "registered-ring")]
impl Drop for Ring {
    fn drop(&mut self) {
        if let Some(index) = self.registered_index.take() {
            let update = ResourceUpdate {
                offset: index,
                ..ResourceUpdate::default()
            };
            unsafe {
                let _ = self.register(
                    IORING_UNREGISTER_RING_FDS,
                    (&update as *const ResourceUpdate).cast(),
                    1,
                );
            }
        }
        // Driver has drained every memory-bearing request before this point.
        // fd is declared first so close precedes unmapping registered wait memory.
    }
}
