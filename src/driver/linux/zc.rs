//! Linux 7.2.7 SEND_ZC/SENDMSG_ZC preparation and memory-release state.
//!
//! The send result does not release its immutable kernel guards when F_MORE is
//! present. A usage notification is a bit field, even though CQE.res is signed.

#[cfg(feature = "zc-tx-fixed")]
use super::uapi::IORING_RECVSEND_FIXED_BUF as RECVSEND_FIXED_BUF;
#[cfg(feature = "zc-tx-vectored")]
use super::uapi::IORING_SEND_VECTORIZED as SEND_VECTORIZED;
use super::uapi::{
    Cqe, IORING_CQE_F_MORE as CQE_F_MORE, IORING_CQE_F_NOTIF as CQE_F_NOTIF,
    IORING_OP_SEND_ZC as OP_SEND_ZC, IORING_OP_SENDMSG_ZC as OP_SENDMSG_ZC,
    IORING_RECVSEND_POLL_FIRST as RECVSEND_POLL_FIRST, IOSQE_BUFFER_SELECT, IOSQE_CQE_SKIP_SUCCESS,
    Sqe, UserMsgHdr,
};
#[cfg(feature = "zc-observe")]
use super::uapi::{
    IORING_NOTIF_USAGE_ZC_COPIED as NOTIF_USAGE_ZC_COPIED,
    IORING_SEND_ZC_REPORT_USAGE as SEND_ZC_REPORT_USAGE,
};
#[cfg(feature = "zc-tx-fixed")]
use crate::buffer::MemoryRegion;
use crate::buffer::{SendBuf, SendPayload};
#[cfg(feature = "zc-observe")]
use crate::capability::ZcStats;
use std::{io, ptr};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TxOptions {
    #[cfg(feature = "zc-tx-fixed")]
    pub fixed: bool,
    #[cfg(feature = "zc-tx-vectored")]
    pub vectored: bool,
    #[cfg(feature = "zc-observe")]
    pub observe: bool,
}

#[derive(Debug)]
pub(crate) enum TxCompletion {
    Result {
        result: io::Result<usize>,
        memory_released: bool,
    },
    Released,
    /// A notification overtook the result. Do not retire the operation yet.
    Pending,
}

/// One instance belongs to a stable, reusable operation slot. Both vectors are
/// allocated at worker initialization, not when a packet is submitted.
pub(crate) struct ZcTx {
    guards: Vec<SendBuf>,
    iovecs: Vec<libc::iovec>,
    max_iovecs: usize,
    active: bool,
    result_seen: bool,
    notification_seen: bool,
    #[cfg(feature = "zc-observe")]
    observation_started: bool,
    #[cfg(feature = "zc-observe")]
    observe: bool,
    #[cfg(feature = "zc-observe")]
    copy_marked: bool,
    requested: usize,
    #[cfg(feature = "zc-observe")]
    accepted: usize,
}

impl ZcTx {
    pub fn new(max_iovecs: usize) -> Self {
        let max_iovecs = if cfg!(feature = "zc-tx-vectored") {
            max_iovecs
        } else {
            max_iovecs.min(1)
        };
        Self {
            guards: Vec::with_capacity(max_iovecs),
            iovecs: Vec::with_capacity(max_iovecs),
            max_iovecs,
            active: false,
            result_seen: false,
            notification_seen: false,
            #[cfg(feature = "zc-observe")]
            observation_started: false,
            #[cfg(feature = "zc-observe")]
            observe: false,
            #[cfg(feature = "zc-observe")]
            copy_marked: false,
            requested: 0,
            #[cfg(feature = "zc-observe")]
            accepted: 0,
        }
    }

    /// Populate an SQE whose fd, user_data and fixed-file flag are already set.
    /// `message`, when supplied, must occupy stable storage until the result
    /// CQE. Its destination and ancillary data are preserved; iovecs are ours.
    /// All nonempty fixed vectors must be within a *single* registered region:
    /// the UAPI has one buf_index for the operation, not an index per iovec.
    pub fn prepare(
        &mut self,
        sqe: &mut Sqe,
        data: &SendPayload,
        #[cfg(feature = "zc-tx-fixed")] regions: &[MemoryRegion],
        _options: TxOptions,
        message: Option<&mut UserMsgHdr>,
    ) -> io::Result<()> {
        if self.active {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "zero-copy operation still owns kernel memory",
            ));
        }
        if sqe.flags & (IOSQE_BUFFER_SELECT | IOSQE_CQE_SKIP_SUCCESS) != 0 {
            return Err(invalid(
                "zero-copy sends cannot select provided buffers or skip result CQEs",
            ));
        }
        let count = data.segments().len();
        if count > self.max_iovecs || count > libc::UIO_MAXIOV as usize {
            return Err(invalid("zero-copy send exceeds the configured iovec limit"));
        }
        let vectored = false;
        #[cfg(feature = "zc-tx-vectored")]
        let vectored = vectored || _options.vectored;
        if count != 1 && !vectored {
            return Err(invalid("vectored zero-copy send was not enabled"));
        }
        let length = data.len();
        if length > i32::MAX as usize {
            return Err(invalid("zero-copy send exceeds the kernel result range"));
        }
        #[cfg(feature = "zc-tx-fixed")]
        let fixed_index = if _options.fixed && length != 0 {
            Some(fixed_region(data, regions)?)
        } else {
            None
        };

        self.guards.clear();
        self.iovecs.clear();
        for segment in data.segments() {
            // Linux 7.2.7 io_vec_fill_bvec rejects zero-length fixed entries.
            // Empty leases reference no transmitted bytes and need no guard.
            if segment.is_empty() {
                continue;
            }
            self.iovecs.push(libc::iovec {
                iov_base: segment.as_ptr() as *mut libc::c_void,
                iov_len: segment.len(),
            });
            self.guards.push(segment.clone());
        }
        let count = self.iovecs.len();
        sqe.ioprio &= RECVSEND_POLL_FIRST;
        sqe.buf_index = 0;
        #[cfg(feature = "zc-tx-fixed")]
        if let Some(index) = fixed_index {
            sqe.ioprio |= RECVSEND_FIXED_BUF;
            sqe.buf_index = index;
        }
        #[cfg(feature = "zc-observe")]
        if _options.observe {
            sqe.ioprio |= SEND_ZC_REPORT_USAGE;
        }
        sqe.op_flags |= libc::MSG_NOSIGNAL as u32;
        // Keep result and notification under the same generation-bearing key.
        sqe.addr3 = 0;
        sqe.pad2 = 0;
        match message {
            Some(message) => {
                message.msg_iov = self.iovecs.as_mut_ptr();
                message.msg_iovlen = count;
                sqe.opcode = OP_SENDMSG_ZC;
                sqe.addr = ptr::from_mut(message) as u64;
                sqe.len = 1;
                sqe.off = 0;
                sqe.file_index = 0;
            }
            #[cfg(feature = "zc-tx-vectored")]
            None if length != 0 && (count != 1 || vectored) => {
                sqe.opcode = OP_SEND_ZC;
                sqe.ioprio |= SEND_VECTORIZED;
                sqe.addr = self.iovecs.as_ptr() as u64;
                sqe.len = count as u32;
            }
            None => {
                sqe.opcode = OP_SEND_ZC;
                // A zero-byte TCP send must not enter the fixed-vector importer.
                sqe.addr = self.iovecs.first().map_or(0, |iov| iov.iov_base as u64);
                sqe.len = length as u32;
            }
        }
        self.active = true;
        self.result_seen = false;
        self.notification_seen = false;
        #[cfg(feature = "zc-observe")]
        {
            self.observation_started = false;
            self.observe = _options.observe;
            self.copy_marked = false;
            self.accepted = 0;
        }
        self.requested = length;
        Ok(())
    }

    pub fn is_idle(&self) -> bool {
        !self.active
    }

    /// # Safety
    /// The prepared SQE must not be visible to the kernel, including a queued
    /// SQPOLL submission. Cancellation of an already submitted send is not proof.
    pub unsafe fn abandon_unsubmitted(&mut self) {
        self.guards.clear();
        self.iovecs.clear();
        self.active = false;
    }

    pub fn process(
        &mut self,
        cqe: &Cqe,
        #[cfg(feature = "zc-observe")] stats: &mut ZcStats,
    ) -> io::Result<TxCompletion> {
        if !self.active {
            return Err(invalid_completion(
                "completion for an inactive zero-copy send",
            ));
        }
        #[cfg(feature = "zc-observe")]
        if self.observe && !self.observation_started {
            stats.tx_requests = stats.tx_requests.saturating_add(1);
            self.observation_started = true;
        }
        if cqe.flags & CQE_F_NOTIF != 0 {
            if self.notification_seen || cqe.flags & CQE_F_MORE != 0 {
                return Err(invalid_completion(
                    "duplicate or nonterminal zero-copy notification",
                ));
            }
            self.notification_seen = true;
            #[cfg(feature = "zc-observe")]
            {
                // res is NOT a negative errno in a usage notification.
                self.copy_marked = self.observe && (cqe.res as u32 & NOTIF_USAGE_ZC_COPIED != 0);
                if self.observe {
                    stats.tx_notifications = stats.tx_notifications.saturating_add(1);
                    if self.copy_marked {
                        stats.tx_copied_notifications =
                            stats.tx_copied_notifications.saturating_add(1);
                        if self.result_seen {
                            stats.tx_copy_marked_bytes = stats
                                .tx_copy_marked_bytes
                                .saturating_add(self.accepted as u64);
                        }
                    }
                }
            }
            self.guards.clear();
            if self.result_seen {
                self.active = false;
                return Ok(TxCompletion::Released);
            }
            return Ok(TxCompletion::Pending);
        }
        if self.result_seen || (cqe.res >= 0 && cqe.res as usize > self.requested) {
            return Err(invalid_completion(
                "duplicate or oversized zero-copy send result",
            ));
        }
        let more = cqe.flags & CQE_F_MORE != 0;
        if self.notification_seen && !more {
            return Err(invalid_completion(
                "zero-copy notification without a promised notification lifetime",
            ));
        }
        self.result_seen = true;
        #[cfg(feature = "zc-observe")]
        {
            self.accepted = if cqe.res >= 0 { cqe.res as usize } else { 0 };
            if self.copy_marked {
                stats.tx_copy_marked_bytes = stats
                    .tx_copy_marked_bytes
                    .saturating_add(self.accepted as u64);
            }
        }
        let memory_released = !more || self.notification_seen;
        if memory_released {
            self.guards.clear();
            self.active = false;
        }
        let result = if cqe.res >= 0 {
            Ok(cqe.res as usize)
        } else {
            Err(io::Error::from_raw_os_error(cqe.res.saturating_neg()))
        };
        Ok(TxCompletion::Result {
            result,
            memory_released,
        })
    }
}

#[cfg(feature = "zc-tx-fixed")]
fn fixed_region(data: &SendPayload, regions: &[MemoryRegion]) -> io::Result<u16> {
    for region in regions {
        let start = region.ptr.as_ptr() as usize;
        let Some(end) = start.checked_add(region.len) else {
            continue;
        };
        if data
            .segments()
            .iter()
            .filter(|segment| !segment.is_empty())
            .all(|segment| {
                let base = segment.as_ptr() as usize;
                base >= start
                    && base
                        .checked_add(segment.len())
                        .is_some_and(|limit| limit <= end)
            })
        {
            return u16::try_from(region.id)
                .map_err(|_| invalid("registered send buffer index exceeds u16"));
        }
    }
    Err(invalid(
        "fixed zero-copy vectors must belong to one registered memory region",
    ))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn invalid_completion(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferPool, PoolConfig};

    fn pool_and_payload() -> (BufferPool, SendPayload) {
        let pool = BufferPool::new(PoolConfig {
            bytes: 8,
            block_size: 8,
            max_leases: 1,
        })
        .unwrap();
        let mut write = pool.try_acquire().unwrap();
        write.extend_from_slice(b"abcdefgh").unwrap();
        (pool, SendPayload::Single(write.freeze()))
    }

    #[cfg(feature = "zc-observe")]
    #[test]
    fn short_send_keeps_full_kernel_guard_and_counts_only_accepted_copy_marked_bytes() {
        let (pool, data) = pool_and_payload();
        let mut tx = ZcTx::new(4);
        tx.prepare(
            &mut Sqe::default(),
            &data,
            #[cfg(feature = "zc-tx-fixed")]
            &[],
            TxOptions {
                observe: true,
                ..TxOptions::default()
            },
            None,
        )
        .unwrap();
        let mut stats = ZcStats::default();
        let result = tx
            .process(
                &Cqe {
                    res: 3,
                    flags: CQE_F_MORE,
                    ..Cqe::default()
                },
                &mut stats,
            )
            .unwrap();
        match result {
            TxCompletion::Result {
                result,
                memory_released,
            } => {
                assert_eq!(result.unwrap(), 3);
                assert!(!memory_released);
            }
            _ => panic!("expected a send result"),
        }
        let remaining = data.remaining(3);
        assert_eq!(remaining.segments()[0].as_slice(), b"defgh");
        drop(remaining);
        assert_eq!(
            pool.try_acquire().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(matches!(
            tx.process(
                &Cqe {
                    res: NOTIF_USAGE_ZC_COPIED as i32,
                    flags: CQE_F_NOTIF,
                    ..Cqe::default()
                },
                &mut stats
            )
            .unwrap(),
            TxCompletion::Released
        ));
        assert!(tx.is_idle());
        let mut recycled = pool.try_acquire().unwrap();
        recycled.extend_from_slice(b"newbytes").unwrap();
        assert_eq!(recycled.as_slice(), b"newbytes");
        assert_eq!(stats.tx_requests, 1);
        assert_eq!(stats.tx_notifications, 1);
        assert_eq!(stats.tx_copied_notifications, 1);
        assert_eq!(stats.tx_copy_marked_bytes, 3);
    }

    #[test]
    fn failed_preparation_result_without_more_needs_no_notification() {
        let (pool, data) = pool_and_payload();
        let mut tx = ZcTx::new(1);
        tx.prepare(
            &mut Sqe::default(),
            &data,
            #[cfg(feature = "zc-tx-fixed")]
            &[],
            TxOptions::default(),
            None,
        )
        .unwrap();
        #[cfg(feature = "zc-observe")]
        let mut stats = ZcStats::default();
        let result = tx
            .process(
                &Cqe {
                    res: -libc::EOPNOTSUPP,
                    ..Cqe::default()
                },
                #[cfg(feature = "zc-observe")]
                &mut stats,
            )
            .unwrap();
        match result {
            TxCompletion::Result {
                result,
                memory_released,
            } => {
                assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
                assert!(memory_released);
            }
            _ => panic!("expected an error result"),
        }
        drop(data);
        assert!(tx.is_idle());
        assert_eq!(pool.try_acquire().unwrap().capacity(), 8);
        #[cfg(feature = "zc-observe")]
        assert_eq!(stats, ZcStats::default());
    }

    #[cfg(feature = "zc-observe")]
    #[test]
    fn canceled_send_with_more_still_owns_memory_until_notification() {
        let (pool, data) = pool_and_payload();
        let mut tx = ZcTx::new(1);
        tx.prepare(
            &mut Sqe::default(),
            &data,
            #[cfg(feature = "zc-tx-fixed")]
            &[],
            TxOptions {
                observe: true,
                ..TxOptions::default()
            },
            None,
        )
        .unwrap();
        let mut stats = ZcStats::default();
        match tx
            .process(
                &Cqe {
                    res: -libc::ECANCELED,
                    flags: CQE_F_MORE,
                    ..Cqe::default()
                },
                &mut stats,
            )
            .unwrap()
        {
            TxCompletion::Result {
                result,
                memory_released,
            } => {
                assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
                assert!(!memory_released);
            }
            _ => panic!("expected a canceled result"),
        }
        drop(data);
        assert_eq!(
            pool.try_acquire().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(matches!(
            tx.process(
                &Cqe {
                    res: NOTIF_USAGE_ZC_COPIED as i32,
                    flags: CQE_F_NOTIF,
                    ..Cqe::default()
                },
                &mut stats
            )
            .unwrap(),
            TxCompletion::Released
        ));
        assert_eq!(stats.tx_copy_marked_bytes, 0);
        assert_eq!(stats.tx_copied_notifications, 1);
        assert_eq!(pool.try_acquire().unwrap().capacity(), 8);
    }

    #[cfg(feature = "zc-observe")]
    #[test]
    fn early_notification_does_not_retire_or_double_count_the_result() {
        let (_, data) = pool_and_payload();
        let mut tx = ZcTx::new(1);
        tx.prepare(
            &mut Sqe::default(),
            &data,
            #[cfg(feature = "zc-tx-fixed")]
            &[],
            TxOptions {
                observe: true,
                ..TxOptions::default()
            },
            None,
        )
        .unwrap();
        let mut stats = ZcStats::default();
        assert!(matches!(
            tx.process(
                &Cqe {
                    res: NOTIF_USAGE_ZC_COPIED as i32,
                    flags: CQE_F_NOTIF,
                    ..Cqe::default()
                },
                &mut stats
            )
            .unwrap(),
            TxCompletion::Pending
        ));
        assert!(!tx.is_idle());
        assert_eq!(stats.tx_copy_marked_bytes, 0);
        match tx
            .process(
                &Cqe {
                    res: 5,
                    flags: CQE_F_MORE,
                    ..Cqe::default()
                },
                &mut stats,
            )
            .unwrap()
        {
            TxCompletion::Result {
                result,
                memory_released,
            } => {
                assert_eq!(result.unwrap(), 5);
                assert!(memory_released);
            }
            _ => panic!("expected a released result"),
        }
        assert_eq!(stats.tx_copy_marked_bytes, 5);
        assert_eq!(stats.tx_notifications, 1);
        assert!(tx.is_idle());
    }

    #[cfg(all(feature = "zc-tx-fixed", feature = "zc-tx-vectored"))]
    #[test]
    fn fixed_vectors_cannot_cross_registration_boundaries() {
        let (pool, data) = pool_and_payload();
        let (other_pool, other) = pool_and_payload();
        let mut regions = pool.regions();
        let mut other_region = other_pool.regions()[0];
        other_region.id = 1;
        regions.push(other_region);
        let SendPayload::Single(first) = data else {
            unreachable!()
        };
        let SendPayload::Single(second) = other else {
            unreachable!()
        };
        let data = SendPayload::Vectored(vec![first, second]);
        let mut tx = ZcTx::new(2);
        let options = TxOptions {
            fixed: true,
            vectored: true,
            ..TxOptions::default()
        };
        assert_eq!(
            tx.prepare(&mut Sqe::default(), &data, &regions, options, None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(tx.is_idle());
        let same = SendPayload::Vectored(vec![
            data.segments()[0].slice(0..4),
            data.segments()[0].slice(4..8),
        ]);
        tx.prepare(&mut Sqe::default(), &same, &regions, options, None)
            .unwrap();
        drop(same);
        drop(data);
        assert_eq!(
            pool.try_acquire().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        #[cfg(feature = "zc-observe")]
        let mut stats = ZcStats::default();
        match tx
            .process(
                &Cqe {
                    res: 8,
                    ..Cqe::default()
                },
                #[cfg(feature = "zc-observe")]
                &mut stats,
            )
            .unwrap()
        {
            TxCompletion::Result {
                result,
                memory_released,
            } => {
                assert_eq!(result.unwrap(), 8);
                assert!(memory_released);
            }
            _ => panic!("expected a send result"),
        }
        assert_eq!(pool.try_acquire().unwrap().capacity(), 8);
    }
}
