use super::*;

impl Driver {
    pub(super) fn rearm_datagrams(&mut self, socket: SocketId) {
        let record = self.sockets.get(socket.0).unwrap();
        let group = record.datagrams.as_ref().unwrap();
        if group.token.is_none() || group.stopping {
            return;
        }
        let available = group.idle.len();
        for _ in 0..available {
            let record = self.sockets.get_mut(socket.0).unwrap();
            let group = record.datagrams.as_mut().unwrap();
            if group.pending >= record.receive_credits {
                break;
            }
            let key = group.idle.pop_front().unwrap();
            if let Err(error) = self.submit_datagram(key) {
                self.stop_datagrams(socket, Some(error));
                break;
            }
            if !self.operations.get(key).unwrap().in_flight {
                // Pool pressure can pause a lane, but another idle lane may
                // already have a uniquely owned reserve it can reuse.
                self.sockets
                    .get_mut(socket.0)
                    .unwrap()
                    .datagrams
                    .as_mut()
                    .unwrap()
                    .idle
                    .push_back(key);
            }
        }
    }

    fn submit_datagram(&mut self, key: u64) -> io::Result<()> {
        let operation = self.operations.get_mut(key).unwrap();
        let record = self.sockets.get_mut(operation.socket.0).unwrap();
        let OperationKind::DatagramReceive {
            writable, reserve, ..
        } = &mut operation.kind
        else {
            unreachable!()
        };
        if writable.is_none() {
            if let Some(buffer) = reserve.take() {
                match buffer.try_into_write() {
                    Ok(buffer) => *writable = Some(buffer),
                    Err(buffer) => *reserve = Some(buffer),
                }
            }
            if writable.is_none() {
                match self.pool.try_acquire_at_least(record.options.receive_chunk) {
                    Ok(buffer) => {
                        *writable = Some(buffer);
                        drop(reserve.take());
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
        }
        let buffer = writable.as_mut().unwrap();
        buffer.clear();
        operation.data_buf = self
            .rio
            .buffer(buffer.as_ptr(), record.options.receive_chunk)
            .ok_or_else(|| sys::invalid("receive buffer is outside the registered worker pool"))?;
        // Only this lane's dequeued completion allows its control and buffer to
        // be reused. Native-writable metadata is outside the operation arena.
        unsafe {
            *self.rio.metadata_mut(key) = rio::Metadata::default();
        }
        operation.address_buf = self.rio.address_buffer(key);
        operation.flags_buf = self.rio.flags_buffer(key);
        let submitted = unsafe {
            self.rio.table.RIOReceiveEx.unwrap()(
                record.rq,
                &operation.data_buf,
                1,
                ptr::null(),
                &operation.address_buf,
                ptr::null(),
                &operation.flags_buf,
                RIO_MSG_DEFER,
                key as usize as *const _,
            )
        };
        if submitted == 0 {
            return Err(sys::wsa_error());
        }
        operation.in_flight = true;
        record.datagrams.as_mut().unwrap().pending += 1;
        record.receive_commit = true;
        Ok(())
    }

    pub(super) fn commit_datagrams(&mut self, socket: SocketId) {
        let record = self.sockets.get_mut(socket.0).unwrap();
        if !record.receive_commit {
            return;
        }
        match self.rio.commit(record.rq, true) {
            Ok(()) => record.receive_commit = false,
            Err(error) => self.stop_datagrams(socket, Some(error)),
        }
    }

    pub(super) fn stop_datagrams(&mut self, socket: SocketId, error: Option<io::Error>) {
        let record = self.sockets.get_mut(socket.0).unwrap();
        let group = record.datagrams.as_mut().unwrap();
        if group.error.is_none() {
            group.error = error;
        }
        if group.stopping {
            return;
        }
        group.stopping = true;
        // A native failure after RQ creation cannot be returned as ImportError:
        // the handle is runtime-owned. Close on failure, but preserve completed
        // datagrams and defer the terminal error until every native lane retires.
        let cancellation = if group.error.is_some() {
            Err(())
        } else if group.pending == group.ready.len() {
            Ok(())
        } else if let Ok(raw) = record.raw() {
            let result = if record.receive_commit {
                self.rio
                    .commit(record.rq, true)
                    .and_then(|_| sys::flush(raw))
            } else {
                sys::flush(raw)
            };
            match result {
                Ok(()) => Ok(()),
                Err(error) => {
                    record.datagrams.as_mut().unwrap().error = Some(error);
                    Err(())
                }
            }
        } else {
            Ok(())
        };
        record.receive_commit = false;
        if cancellation.is_err() {
            drop(record.handle.take());
            record.send_commit = false;
        }
        for (_, operation) in self.operations.iter_mut() {
            if operation.socket == socket
                && (cancellation.is_err()
                    || matches!(operation.kind, OperationKind::DatagramReceive { .. }))
            {
                operation.cancelled = true;
            }
        }
        self.notifier.notify();
    }

    pub(super) fn complete_datagram(&mut self, key: u64, completion: RIORESULT) {
        let operation = self.operations.get_mut(key).unwrap();
        let socket = operation.socket;
        let record = self.sockets.get_mut(socket.0).unwrap();
        let group = record.datagrams.as_mut().unwrap();
        if group.discard || (completion.Status != 0 && completion.Status != WSAEMSGSIZE) {
            group.pending -= 1;
            if !group.stopping {
                self.stop_datagrams(
                    socket,
                    Some(io::Error::from_raw_os_error(completion.Status)),
                );
            }
            return;
        }
        // Native completion, including a zero-byte datagram, releases this
        // lane's write references but does not consume its publication credit.
        let metadata = unsafe { self.rio.metadata(key) };
        let peer = match sys::read_address(&metadata.address) {
            Ok(peer) => peer,
            Err(error) => {
                group.pending -= 1;
                self.stop_datagrams(socket, Some(error));
                return;
            }
        };
        let truncated = completion.Status == WSAEMSGSIZE
            || metadata.flags & (MSG_TRUNC | MSG_PARTIAL) != 0
            || completion.BytesTransferred > operation.data_buf.Length;
        let length = completion.BytesTransferred.min(operation.data_buf.Length) as usize;
        let OperationKind::DatagramReceive {
            writable,
            reserve,
            ready,
            ..
        } = &mut operation.kind
        else {
            unreachable!()
        };
        let mut buffer = writable.take().unwrap();
        unsafe {
            buffer.set_initialized_len(length);
        }
        let data = buffer.freeze();
        *reserve = Some(data.clone());
        *ready = Some(Received {
            data,
            peer: Some(peer),
            truncated,
            original_len: if truncated {
                (completion.BytesTransferred > operation.data_buf.Length)
                    .then_some(completion.BytesTransferred as usize)
            } else {
                Some(length)
            },
            gro_segment_size: None,
        });
        // Preserve native dequeue order rather than scanning lane/arena order.
        // One retained entry per admitted lane makes this FIFO allocation-free.
        debug_assert!(group.ready.len() < group.lanes);
        group.ready.push_back(key);
    }

    pub(super) fn service_datagrams(
        &mut self,
        socket: SocketId,
        events: &mut Vec<Event>,
        budget: &mut usize,
    ) {
        let record = self.sockets.get_mut(socket.0).unwrap();
        let group = record.datagrams.as_mut().unwrap();
        while !group.ready.is_empty()
            && (group.discard || (record.receive_credits != 0 && *budget != 0))
        {
            let key = group.ready.pop_front().unwrap();
            let operation = self.operations.get_mut(key).unwrap();
            let OperationKind::DatagramReceive { ready, .. } = &mut operation.kind else {
                unreachable!()
            };
            let received = ready.take().unwrap();
            group.pending -= 1;
            group.idle.push_back(key);
            if !group.discard {
                events.push(Event::Received {
                    token: group.token.unwrap(),
                    result: Ok(received),
                });
                record.receive_credits -= 1;
                *budget -= 1;
            }
        }
        if !group.stopping {
            self.rearm_datagrams(socket);
            return;
        }
        if !group.ready.is_empty() {
            return;
        }
        if group.token.is_some() && *budget == 0 {
            return;
        }
        let mut next = record.receive;
        while let Some(key) = next {
            let operation = self.operations.get(key).unwrap();
            if operation.in_flight {
                return;
            }
            let OperationKind::DatagramReceive {
                next: following, ..
            } = &operation.kind
            else {
                unreachable!()
            };
            next = *following;
        }
        // This is the single logical stop barrier for the entire native window.
        // No token, metadata slot or RQ-owned buffer is reusable before it.
        let group = record.datagrams.take().unwrap();
        debug_assert_eq!(group.pending, 0);
        record.read_shutdown = true;
        next = record.receive.take();
        while let Some(key) = next {
            let operation = self.operations.remove(key).unwrap();
            let OperationKind::DatagramReceive {
                next: following, ..
            } = operation.kind
            else {
                unreachable!()
            };
            next = following;
            record.operation_refs -= 1;
        }
        if let Some(token) = group.token {
            events.push(Event::Stopped {
                token,
                result: group.error.map_or(Ok(()), Err),
            });
            *budget -= 1;
        }
    }
}
