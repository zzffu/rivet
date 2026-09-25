//! Linux v7.2.7 networking ABI: io_uring UAPI and native socket message layouts.
//! SPDX-License-Identifier: MIT
//! Original UAPI copyright (C) 2019 Jens Axboe and Christoph Hellwig.
//! Reference: <https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/tree/include/uapi/linux/io_uring.h?h=v7.2.7>.
//! Message ABI: <https://github.com/gregkh/linux/blob/v7.2.7/include/linux/socket.h>.

#![allow(dead_code)]

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Sqe {
    pub opcode: u8,
    pub flags: u8,
    pub ioprio: u16,
    pub fd: i32,
    pub off: u64,
    pub addr: u64,
    pub len: u32,
    pub op_flags: u32,
    pub user_data: u64,
    pub buf_index: u16,
    pub personality: u16,
    pub file_index: u32,
    pub addr3: u64,
    pub pad2: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Cqe {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
    pub extra: [u64; 2],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Cqe16 {
    pub user_data: u64,
    pub res: i32,
    pub flags: u32,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct SqOffsets {
    pub head: u32,
    pub tail: u32,
    pub ring_mask: u32,
    pub ring_entries: u32,
    pub flags: u32,
    pub dropped: u32,
    pub array: u32,
    pub resv1: u32,
    pub user_addr: u64,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct CqOffsets {
    pub head: u32,
    pub tail: u32,
    pub ring_mask: u32,
    pub ring_entries: u32,
    pub overflow: u32,
    pub cqes: u32,
    pub flags: u32,
    pub resv1: u32,
    pub user_addr: u64,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Params {
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub flags: u32,
    pub sq_thread_cpu: u32,
    pub sq_thread_idle: u32,
    pub features: u32,
    pub wq_fd: u32,
    pub resv: [u32; 3],
    pub sq_off: SqOffsets,
    pub cq_off: CqOffsets,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: i64,
}
#[repr(C)]
#[derive(Default)]
pub struct GetEventsArg {
    pub sigmask: u64,
    pub sigmask_sz: u32,
    pub min_wait_usec: u32,
    pub ts: u64,
}
#[repr(C)]
#[derive(Default)]
pub struct RegWait {
    pub ts: Timespec,
    pub min_wait_usec: u32,
    pub flags: u32,
    pub sigmask: u64,
    pub sigmask_sz: u32,
    pub pad: [u32; 3],
    pub pad2: [u64; 2],
}
#[repr(C)]
#[derive(Default)]
pub struct ResourceRegister {
    pub nr: u32,
    pub flags: u32,
    pub resv2: u64,
    pub data: u64,
    pub tags: u64,
}
#[repr(C)]
#[derive(Default)]
pub struct ResourceUpdate {
    pub offset: u32,
    pub resv: u32,
    pub data: u64,
}
#[repr(C)]
#[derive(Default)]
pub struct FileRange {
    pub off: u32,
    pub len: u32,
    pub resv: u64,
}
#[repr(C)]
#[derive(Default)]
pub struct RegionDesc {
    pub user_addr: u64,
    pub size: u64,
    pub flags: u32,
    pub id: u32,
    pub mmap_offset: u64,
    pub resv: [u64; 4],
}
#[repr(C)]
#[derive(Default)]
pub struct MemRegionReg {
    pub region_uptr: u64,
    pub flags: u64,
    pub resv: [u64; 2],
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ProbeOp {
    pub op: u8,
    pub resv: u8,
    pub flags: u16,
    pub resv2: u32,
}
#[repr(C)]
pub struct Probe {
    pub last_op: u8,
    pub ops_len: u8,
    pub resv: u16,
    pub resv2: [u32; 3],
    pub ops: [ProbeOp; 256],
}
impl Default for Probe {
    fn default() -> Self {
        Self {
            last_op: 0,
            ops_len: 0,
            resv: 0,
            resv2: [0; 3],
            ops: [ProbeOp::default(); 256],
        }
    }
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct Buf {
    pub addr: u64,
    pub len: u32,
    pub bid: u16,
    pub resv: u16,
}
#[repr(C)]
#[derive(Default)]
pub struct BufReg {
    pub ring_addr: u64,
    pub ring_entries: u32,
    pub bgid: u16,
    pub flags: u16,
    pub min_left: u32,
    pub resv: [u32; 5],
}
#[repr(C)]
#[derive(Default)]
pub struct Napi {
    pub busy_poll_to: u32,
    pub prefer_busy_poll: u8,
    pub opcode: u8,
    pub pad: [u8; 2],
    pub op_param: u32,
    pub resv: u32,
}
#[repr(C)]
#[derive(Default)]
pub struct SyncCancel {
    pub addr: u64,
    pub fd: i32,
    pub flags: u32,
    pub timeout: Timespec,
    pub opcode: u8,
    pub pad: [u8; 7],
    pub pad2: [u64; 3],
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct RecvMsgOut {
    pub namelen: u32,
    pub controllen: u32,
    pub payloadlen: u32,
    pub flags: u32,
}

/// Native 64-bit Linux `user_msghdr`, not libc's POSIX-facing `msghdr`.
/// musl uses 32-bit iov/control lengths with padding where the raw kernel ABI
/// requires size_t. Explicit padding keeps every submitted byte initialized.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct UserMsgHdr {
    pub msg_name: *mut libc::c_void,
    pub msg_namelen: i32,
    pub name_pad: u32,
    pub msg_iov: *mut libc::iovec,
    pub msg_iovlen: usize,
    pub msg_control: *mut libc::c_void,
    pub msg_controllen: usize,
    pub msg_flags: u32,
    pub flags_pad: u32,
}
impl Default for UserMsgHdr {
    fn default() -> Self {
        Self {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            name_pad: 0,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
            flags_pad: 0,
        }
    }
}

/// Native Linux ancillary header, including the full kernel size_t length.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CmsgHdr {
    pub cmsg_len: usize,
    pub cmsg_level: i32,
    pub cmsg_type: i32,
}

pub const IOSQE_FIXED_FILE: u8 = 1;
pub const IOSQE_IO_LINK: u8 = 1 << 2;
pub const IOSQE_BUFFER_SELECT: u8 = 1 << 5;
pub const IOSQE_CQE_SKIP_SUCCESS: u8 = 1 << 6;
pub const IORING_SETUP_SQPOLL: u32 = 1 << 1;
pub const IORING_SETUP_SQ_AFF: u32 = 1 << 2;
pub const IORING_SETUP_CQSIZE: u32 = 1 << 3;
pub const IORING_SETUP_R_DISABLED: u32 = 1 << 6;
pub const IORING_SETUP_SUBMIT_ALL: u32 = 1 << 7;
pub const IORING_SETUP_COOP_TASKRUN: u32 = 1 << 8;
pub const IORING_SETUP_TASKRUN_FLAG: u32 = 1 << 9;
pub const IORING_SETUP_CQE32: u32 = 1 << 11;
pub const IORING_SETUP_SINGLE_ISSUER: u32 = 1 << 12;
pub const IORING_SETUP_DEFER_TASKRUN: u32 = 1 << 13;
pub const IORING_SETUP_NO_SQARRAY: u32 = 1 << 16;
pub const IORING_SETUP_CQE_MIXED: u32 = 1 << 18;
pub const IORING_SETUP_SQ_REWIND: u32 = 1 << 20;
pub const IORING_OP_NOP: u8 = 0;
pub const IORING_OP_POLL_ADD: u8 = 6;
pub const IORING_OP_SENDMSG: u8 = 9;
pub const IORING_OP_RECVMSG: u8 = 10;
pub const IORING_OP_ACCEPT: u8 = 13;
pub const IORING_OP_ASYNC_CANCEL: u8 = 14;
pub const IORING_OP_CONNECT: u8 = 16;
pub const IORING_OP_CLOSE: u8 = 19;
pub const IORING_OP_SEND: u8 = 26;
pub const IORING_OP_RECV: u8 = 27;
pub const IORING_OP_SPLICE: u8 = 30;
pub const IORING_OP_SHUTDOWN: u8 = 34;
pub const IORING_OP_MSG_RING: u8 = 40;
pub const IORING_OP_SOCKET: u8 = 45;
pub const IORING_OP_SEND_ZC: u8 = 47;
pub const IORING_OP_SENDMSG_ZC: u8 = 48;
pub const IORING_OP_FIXED_FD_INSTALL: u8 = 54;
pub const IORING_OP_BIND: u8 = 56;
pub const IORING_OP_LISTEN: u8 = 57;
pub const IORING_OP_RECV_ZC: u8 = 58;
pub const IORING_FILE_INDEX_ALLOC: u32 = u32::MAX;
pub const IORING_ACCEPT_MULTISHOT: u16 = 1;
pub const IORING_RECVSEND_POLL_FIRST: u16 = 1;
pub const IORING_RECV_MULTISHOT: u16 = 1 << 1;
pub const IORING_RECVSEND_FIXED_BUF: u16 = 1 << 2;
pub const IORING_SEND_ZC_REPORT_USAGE: u16 = 1 << 3;
pub const IORING_RECVSEND_BUNDLE: u16 = 1 << 4;
pub const IORING_SEND_VECTORIZED: u16 = 1 << 5;
pub const IORING_NOTIF_USAGE_ZC_COPIED: u32 = 1 << 31;
pub const IORING_ASYNC_CANCEL_ALL: u32 = 1;
pub const IORING_ASYNC_CANCEL_ANY: u32 = 1 << 2;
pub const IORING_POLL_ADD_MULTI: u32 = 1;
pub const SPLICE_F_FD_IN_FIXED: u32 = 1 << 31;
pub const IORING_CQE_F_BUFFER: u32 = 1;
pub const IORING_CQE_F_MORE: u32 = 1 << 1;
pub const IORING_CQE_F_SOCK_NONEMPTY: u32 = 1 << 2;
pub const IORING_CQE_F_NOTIF: u32 = 1 << 3;
pub const IORING_CQE_F_BUF_MORE: u32 = 1 << 4;
pub const IORING_CQE_F_SKIP: u32 = 1 << 5;
pub const IORING_CQE_F_32: u32 = 1 << 15;
pub const IORING_CQE_BUFFER_SHIFT: u32 = 16;
pub const IORING_OFF_SQ_RING: i64 = 0;
pub const IORING_OFF_CQ_RING: i64 = 0x8000000;
pub const IORING_OFF_SQES: i64 = 0x10000000;
pub const IORING_SQ_NEED_WAKEUP: u32 = 1;
pub const IORING_SQ_CQ_OVERFLOW: u32 = 1 << 1;
pub const IORING_SQ_TASKRUN: u32 = 1 << 2;
pub const IORING_ENTER_GETEVENTS: u32 = 1;
pub const IORING_ENTER_SQ_WAKEUP: u32 = 1 << 1;
pub const IORING_ENTER_EXT_ARG: u32 = 1 << 3;
pub const IORING_ENTER_REGISTERED_RING: u32 = 1 << 4;
pub const IORING_ENTER_EXT_ARG_REG: u32 = 1 << 6;
pub const IORING_FEAT_SINGLE_MMAP: u32 = 1;
pub const IORING_FEAT_NODROP: u32 = 1 << 1;
pub const IORING_FEAT_EXT_ARG: u32 = 1 << 8;
pub const IORING_FEAT_RECVSEND_BUNDLE: u32 = 1 << 14;
pub const IORING_REGISTER_BUFFERS: u32 = 0;
pub const IORING_UNREGISTER_BUFFERS: u32 = 1;
pub const IORING_REGISTER_FILES: u32 = 2;
pub const IORING_UNREGISTER_FILES: u32 = 3;
pub const IORING_REGISTER_FILES_UPDATE: u32 = 6;
pub const IORING_REGISTER_PROBE: u32 = 8;
pub const IORING_REGISTER_ENABLE_RINGS: u32 = 12;
pub const IORING_REGISTER_FILES2: u32 = 13;
pub const IORING_REGISTER_RING_FDS: u32 = 20;
pub const IORING_UNREGISTER_RING_FDS: u32 = 21;
pub const IORING_REGISTER_PBUF_RING: u32 = 22;
pub const IORING_UNREGISTER_PBUF_RING: u32 = 23;
pub const IORING_REGISTER_SYNC_CANCEL: u32 = 24;
pub const IORING_REGISTER_FILE_ALLOC_RANGE: u32 = 25;
pub const IORING_REGISTER_NAPI: u32 = 27;
pub const IORING_UNREGISTER_NAPI: u32 = 28;
pub const IORING_REGISTER_SEND_MSG_RING: u32 = 31;
pub const IORING_REGISTER_ZCRX_IFQ: u32 = 32;
pub const IORING_REGISTER_MEM_REGION: u32 = 34;
pub const IORING_REGISTER_ZCRX_CTRL: u32 = 36;
pub const IORING_RSRC_REGISTER_SPARSE: u32 = 1;
pub const IORING_MEM_REGION_TYPE_USER: u32 = 1;
pub const IORING_MEM_REGION_REG_WAIT_ARG: u64 = 1;
pub const IORING_REG_WAIT_TS: u32 = 1;
pub const IOU_PBUF_RING_INC: u16 = 2;

const _: () = {
    assert!(size_of::<Sqe>() == 64);
    assert!(size_of::<Params>() == 120);
    assert!(size_of::<Cqe16>() == 16);
    assert!(size_of::<BufReg>() == 40);
    assert!(size_of::<RegWait>() == 64);
    assert!(size_of::<SyncCancel>() == 64);
    assert!(size_of::<RegionDesc>() == 64);
    assert!(size_of::<UserMsgHdr>() == 56);
    assert!(std::mem::align_of::<UserMsgHdr>() == 8);
    assert!(std::mem::offset_of!(UserMsgHdr, msg_namelen) == 8);
    assert!(std::mem::offset_of!(UserMsgHdr, msg_iov) == 16);
    assert!(std::mem::offset_of!(UserMsgHdr, msg_iovlen) == 24);
    assert!(std::mem::offset_of!(UserMsgHdr, msg_control) == 32);
    assert!(std::mem::offset_of!(UserMsgHdr, msg_controllen) == 40);
    assert!(std::mem::offset_of!(UserMsgHdr, msg_flags) == 48);
    assert!(size_of::<CmsgHdr>() == 16);
    assert!(std::mem::offset_of!(CmsgHdr, cmsg_level) == 8);
    assert!(std::mem::offset_of!(CmsgHdr, cmsg_type) == 12);
};
