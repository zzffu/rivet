use super::{
    native::Native,
    notify::{Notifier, RIO_KEY},
    sys,
};
use crate::buffer::BufferPool;
use std::{
    io,
    mem::{offset_of, size_of},
    ptr,
};
use windows_sys::Win32::{Networking::WinSock::*, System::IO::OVERLAPPED};

const INVALID_BUFFER: RIO_BUFFERID = u32::MAX as RIO_BUFFERID;

#[repr(C)]
#[derive(Default)]
pub(super) struct Metadata {
    pub address: SOCKADDR_STORAGE,
    pub flags: u32,
}

struct Region {
    base: usize,
    len: usize,
    id: RIO_BUFFERID,
}

struct SendRegion {
    base: usize,
    len: usize,
    id: RIO_BUFFERID,
    active: bool,
    pooled: bool,
}

/// Each worker is the sole RQ/CQ accessor. Every registration outlives its last
/// dequeued completion, not merely the result future that initiated the I/O.
pub(super) struct Rio {
    pub table: RIO_EXTENSION_FUNCTION_TABLE,
    pub cq: RIO_CQ,
    pub notification: Native<OVERLAPPED>,
    pub armed: bool,
    regions: Vec<Region>,
    metadata: Native<[Metadata]>,
    metadata_ids: Box<[RIO_BUFFERID]>,
    send_regions: Vec<SendRegion>,
    send_region_limit: usize,
    _pool: BufferPool,
    _winsock: sys::Winsock,
}

impl Rio {
    pub fn new(
        pool: BufferPool,
        notifier: &Notifier,
        sockets: usize,
        operations: usize,
    ) -> io::Result<Self> {
        let winsock = sys::Winsock::new()?;
        let probe = sys::new_socket(
            "127.0.0.1:0".parse().unwrap(),
            crate::driver::SocketKind::TcpStream,
        )?;
        use std::os::windows::io::AsRawSocket;
        let table = sys::rio_table(probe.as_raw_socket() as _)?;
        // Each socket reserves one send and at least one receive CQ entry.
        // Additional UDP lanes are charged to the global operation arena.
        let capacity = sockets
            .checked_mul(2)
            .and_then(|base| base.checked_add(operations))
            .filter(|n| *n <= RIO_MAX_CQ_SIZE as usize)
            .ok_or_else(|| {
                sys::invalid(
                    "Windows RIO socket/operation quotas exceed the completion queue limit",
                )
            })?;
        operations
            .checked_mul(size_of::<Metadata>())
            .ok_or_else(|| sys::invalid("Windows RIO operation metadata size overflow"))?;
        let mut rio = Self {
            table,
            cq: 0,
            notification: Native::new(Box::new(OVERLAPPED::default())),
            armed: false,
            regions: Vec::new(),
            metadata: Native::new(
                (0..operations)
                    .map(|_| Metadata::default())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            metadata_ids: vec![INVALID_BUFFER; operations].into_boxed_slice(),
            send_regions: Vec::with_capacity(operations),
            send_region_limit: operations,
            _pool: pool.clone(),
            _winsock: winsock,
        };
        let notification = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: notifier.handle(),
                    CompletionKey: RIO_KEY as *mut _,
                    Overlapped: rio.notification.as_ptr().cast(),
                },
            },
        };
        rio.cq = unsafe { table.RIOCreateCompletionQueue.unwrap()(capacity as u32, &notification) };
        if rio.cq == 0 {
            return Err(sys::wsa_error());
        }
        for region in pool.regions() {
            if region.len > u32::MAX as usize {
                return Err(sys::invalid(
                    "a Windows RIO pool region cannot exceed 4 GiB - 1",
                ));
            }
            let id =
                unsafe { table.RIORegisterBuffer.unwrap()(region.ptr.as_ptr(), region.len as u32) };
            if id == INVALID_BUFFER {
                return Err(sys::wsa_error());
            }
            rio.regions.push(Region {
                base: region.ptr.as_ptr() as usize,
                len: region.len,
                id,
            });
        }
        Ok(rio)
    }

    pub fn buffer(&self, ptr: *const u8, length: usize) -> Option<RIO_BUF> {
        let base = ptr as usize;
        self.regions.iter().find_map(|region| {
            let offset = base.checked_sub(region.base)?;
            if offset <= region.len && length <= region.len - offset {
                Some(RIO_BUF {
                    BufferId: region.id,
                    Offset: offset as u32,
                    Length: length as u32,
                })
            } else {
                None
            }
        })
    }

    fn register(&self, ptr: *const u8, length: usize) -> io::Result<RIO_BUFFERID> {
        let length =
            u32::try_from(length).map_err(|_| sys::invalid("RIO send region exceeds 4 GiB - 1"))?;
        let id = unsafe { self.table.RIORegisterBuffer.unwrap()(ptr, length) };
        if id == INVALID_BUFFER {
            Err(sys::wsa_error())
        } else {
            Ok(id)
        }
    }

    fn deregister(&self, id: RIO_BUFFERID) {
        unsafe {
            self.table.RIODeregisterBuffer.unwrap()(id);
        }
    }

    /// RIOSend's documented exclusivity covers the entire registration, not
    /// just its RIO_BUF view. Use exact-range send registrations, never the
    /// arena-wide receive registration. Overlapping immutable aliases serialize
    /// without copying. Cached pool registrations have stable backing for the
    /// lifetime of this worker; external registrations retire with their guard.
    pub fn acquire_send(
        &mut self,
        pointer: *const u8,
        length: usize,
    ) -> io::Result<Option<(usize, RIO_BUF)>> {
        let base = pointer as usize;
        let end = base
            .checked_add(length)
            .ok_or_else(|| sys::invalid("send address range overflow"))?;
        let mut cached = None;
        let mut reusable = None;
        for (index, region) in self.send_regions.iter().enumerate() {
            if region.active {
                if base < region.base + region.len && region.base < end {
                    return Ok(None);
                }
            } else {
                reusable = Some(index);
                if region.id != INVALID_BUFFER && region.base == base && region.len == length {
                    cached = Some(index);
                }
            }
        }
        let index = if let Some(index) = cached {
            index
        } else {
            let pooled = self.buffer(pointer, length).is_some();
            let id = self.register(pointer, length)?;
            let region = SendRegion {
                base,
                len: length,
                id,
                active: false,
                pooled,
            };
            if self.send_regions.len() < self.send_region_limit {
                let index = self.send_regions.len();
                self.send_regions.push(region);
                index
            } else if let Some(index) = reusable {
                let old = self.send_regions[index].id;
                if old != INVALID_BUFFER {
                    self.deregister(old);
                }
                self.send_regions[index] = region;
                index
            } else {
                self.deregister(id);
                return Err(sys::exhausted("RIO send registration budget exhausted"));
            }
        };
        let region = &mut self.send_regions[index];
        region.active = true;
        Ok(Some((
            index,
            RIO_BUF {
                BufferId: region.id,
                Offset: 0,
                Length: length as u32,
            },
        )))
    }

    pub fn release_send(&mut self, index: usize) {
        let region = &mut self.send_regions[index];
        debug_assert!(region.active);
        region.active = false;
        if !region.pooled {
            let id = std::mem::replace(&mut region.id, INVALID_BUFFER);
            self.deregister(id);
        }
    }

    pub fn prepare_metadata(&mut self, key: u64) -> io::Result<()> {
        let index = key as u32 as usize;
        if self.metadata_ids[index] == INVALID_BUFFER {
            self.metadata_ids[index] =
                self.register(self.metadata_pointer(key).cast(), size_of::<Metadata>())?;
        }
        Ok(())
    }

    fn metadata_pointer(&self, key: u64) -> *mut Metadata {
        let index = key as u32 as usize;
        debug_assert!(index < self.metadata_ids.len());
        unsafe { self.metadata.as_ptr().cast::<Metadata>().add(index) }
    }

    /// The slot must not have a submitted native request using its metadata.
    pub unsafe fn metadata_mut(&mut self, key: u64) -> &mut Metadata {
        unsafe { &mut *self.metadata_pointer(key) }
    }

    /// The slot's native completion must have been dequeued before borrowing.
    pub unsafe fn metadata(&self, key: u64) -> &Metadata {
        unsafe { &*self.metadata_pointer(key) }
    }

    fn meta_buffer(&self, key: u64, offset: usize, length: usize) -> RIO_BUF {
        let id = self.metadata_ids[key as u32 as usize];
        debug_assert_ne!(id, INVALID_BUFFER);
        RIO_BUF {
            BufferId: id,
            Offset: offset as u32,
            Length: length as u32,
        }
    }

    pub fn address_buffer(&self, key: u64) -> RIO_BUF {
        self.meta_buffer(
            key,
            offset_of!(Metadata, address),
            size_of::<SOCKADDR_STORAGE>(),
        )
    }

    pub fn flags_buffer(&self, key: u64) -> RIO_BUF {
        self.meta_buffer(key, offset_of!(Metadata, flags), size_of::<u32>())
    }

    pub fn create_queue(&self, socket: SOCKET, key: u64, receives: usize) -> io::Result<RIO_RQ> {
        let receives = u32::try_from(receives)
            .map_err(|_| sys::invalid("RIO receive window exceeds native capacity"))?;
        // TCP remains single-shot; UDP reserves its complete admitted window.
        // Logical vector sends still advance one native send at a time.
        let rq = unsafe {
            self.table.RIOCreateRequestQueue.unwrap()(
                socket,
                receives,
                1,
                1,
                1,
                self.cq,
                self.cq,
                key as usize as *const _,
            )
        };
        if rq == 0 {
            Err(sys::wsa_error())
        } else {
            Ok(rq)
        }
    }

    pub fn arm(&mut self) -> io::Result<()> {
        if self.armed {
            return Ok(());
        }
        let error = unsafe { self.table.RIONotify.unwrap()(self.cq) };
        if error != 0 {
            return Err(io::Error::from_raw_os_error(error));
        }
        self.armed = true;
        Ok(())
    }

    pub fn commit(&self, rq: RIO_RQ, receive: bool) -> io::Result<()> {
        let function = if receive {
            self.table.RIOReceive.unwrap()
        } else {
            self.table.RIOSend.unwrap()
        };
        if unsafe { function(rq, ptr::null(), 0, RIO_MSG_COMMIT_ONLY, ptr::null()) } == 0 {
            Err(sys::wsa_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for Rio {
    fn drop(&mut self) {
        // Driver has closed every RQ and retired every kernel operation first.
        if self.cq != 0 {
            unsafe {
                self.table.RIOCloseCompletionQueue.unwrap()(self.cq);
            }
        }
        for region in &self.regions {
            self.deregister(region.id);
        }
        for &id in &self.metadata_ids {
            if id != INVALID_BUFFER {
                self.deregister(id);
            }
        }
        for region in &self.send_regions {
            debug_assert!(!region.active);
            if region.id != INVALID_BUFFER {
                self.deregister(region.id);
            }
        }
    }
}
