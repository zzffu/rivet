use super::*;

fn cancelled() -> io::Error {
    io::Error::from_raw_os_error(WSA_OPERATION_ABORTED)
}

impl Driver {
    pub(super) fn service(&mut self, events: &mut Vec<Event>, budget: &mut usize) {
        self.publish_pending(events, budget);
        self.keys.clear();
        self.keys.extend(self.operations.iter().map(|(key, _)| key));
        for index in 0..self.keys.len() {
            let key = self.keys[index];
            let Some(op) = self.operations.get(key) else {
                continue;
            };
            if op.in_flight {
                continue;
            }
            let socket = op.socket;
            let open = self
                .sockets
                .get(socket.0)
                .is_some_and(|s| s.handle.is_some());
            match &op.kind {
                OperationKind::Connect { .. } => {
                    // ConnectEx is always submitted by connect, not by service.
                    if op.cancelled {
                        self.finish_connect(key, Err(cancelled()));
                    }
                }
                OperationKind::Send { result, .. } => {
                    if self.sockets.get(socket.0).and_then(|s| s.send_head) != Some(key) {
                        continue;
                    }
                    if result.is_some() || op.cancelled || !open {
                        self.retire(key);
                    } else if let Err(error) = self.submit_send(key) {
                        if let OperationKind::Send {
                            result,
                            transferred,
                            ..
                        } = &mut self.operations.get_mut(key).unwrap().kind
                        {
                            *result = Some(if *transferred != 0 {
                                Ok(*transferred)
                            } else {
                                Err(error)
                            });
                        }
                        self.retire(key);
                    }
                }
                OperationKind::Receive { .. } => self.service_receive(key, open, events, budget),
                OperationKind::Accept { .. } => self.service_accept(key, open, events, budget),
                OperationKind::DatagramReceive { .. } => {}
            }
        }
        self.socket_keys.clear();
        self.socket_keys.extend(
            self.sockets
                .iter()
                .filter_map(|(key, record)| record.datagrams.as_ref().map(|_| key)),
        );
        for index in 0..self.socket_keys.len() {
            self.service_datagrams(SocketId(self.socket_keys[index]), events, budget);
        }
        self.publish_pending(events, budget);
        self.reap_closed_sockets();
    }

    fn publish_pending(&mut self, events: &mut Vec<Event>, budget: &mut usize) {
        while *budget != 0 {
            let Some(event) = self.pending.pop_front() else {
                break;
            };
            events.push(event);
            *budget -= 1;
        }
    }

    fn service_receive(
        &mut self,
        key: u64,
        open: bool,
        events: &mut Vec<Event>,
        budget: &mut usize,
    ) {
        let op = self.operations.get_mut(key).unwrap();
        let socket = op.socket;
        let OperationKind::Receive { ready, eof, .. } = &mut op.kind else {
            unreachable!()
        };
        if !open {
            drop(ready.take());
            self.retire(key);
            return;
        }
        let record = self.sockets.get_mut(socket.0).unwrap();
        if ready.is_some() && record.receive_credits != 0 && *budget != 0 {
            let result = ready.take().unwrap();
            if result.is_err() {
                op.cancelled = true;
            }
            events.push(Event::Received {
                token: op.token.unwrap(),
                result,
            });
            record.receive_credits -= 1;
            *budget -= 1;
        }
        if ready.is_some() {
            return;
        }
        if *eof && *budget != 0 {
            events.push(Event::ReceiveEof {
                token: op.token.unwrap(),
            });
            *budget -= 1;
            op.cancelled = true;
            *eof = false;
        }
        if *eof {
            return;
        }
        if op.cancelled {
            self.retire(key);
            return;
        }
        if record.read_shutdown {
            if let OperationKind::Receive { eof, .. } =
                &mut self.operations.get_mut(key).unwrap().kind
            {
                *eof = true;
            }
            return;
        }
        if record.receive_credits == 0 {
            return;
        }
        if let Err(error) = self.submit_receive(key)
            && let OperationKind::Receive { ready, .. } =
                &mut self.operations.get_mut(key).unwrap().kind
        {
            *ready = Some(Err(error));
        }
    }

    fn service_accept(
        &mut self,
        key: u64,
        open: bool,
        events: &mut Vec<Event>,
        budget: &mut usize,
    ) {
        let op = self.operations.get_mut(key).unwrap();
        let socket = op.socket;
        let OperationKind::Accept { ready, .. } = &mut op.kind else {
            unreachable!()
        };
        if !open {
            self.retire(key);
            return;
        }
        let record = self.sockets.get_mut(socket.0).unwrap();
        if ready.is_some() && record.accept_credits != 0 && *budget != 0 {
            let result = ready.take().unwrap();
            if result.is_err() {
                op.cancelled = true;
            }
            events.push(Event::Accepted {
                token: op.token.unwrap(),
                result,
            });
            record.accept_credits -= 1;
            *budget -= 1;
        }
        if ready.is_some() {
            return;
        }
        if op.cancelled {
            self.retire(key);
            return;
        }
        if record.accept_credits == 0 {
            return;
        }
        if self.sockets.available() <= self.accept_reservations {
            return;
        }
        if let Err(error) = self.submit_accept(key)
            && let OperationKind::Accept { ready, .. } =
                &mut self.operations.get_mut(key).unwrap().kind
        {
            *ready = Some(Err(error));
        }
    }

    fn submit_receive(&mut self, key: u64) -> io::Result<()> {
        let op = self.operations.get_mut(key).unwrap();
        let record = self.sockets.get_mut(op.socket.0).unwrap();
        let OperationKind::Receive {
            writable, reserve, ..
        } = &mut op.kind
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
                        // The application may retain the preceding immutable
                        // receive while reading the next one. Only the newest
                        // reserve is kept; older storage returns normally.
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
        let length = buffer.capacity().min(record.options.receive_chunk);
        op.data_buf = self
            .rio
            .buffer(buffer.as_ptr(), length)
            .ok_or_else(|| sys::invalid("receive buffer is outside the registered worker pool"))?;
        // This shot has not been submitted; the previous shot already retired.
        unsafe {
            *self.rio.metadata_mut(key) = rio::Metadata::default();
        }
        let result = unsafe {
            self.rio.table.RIOReceive.unwrap()(
                record.rq,
                &op.data_buf,
                1,
                RIO_MSG_DEFER,
                key as usize as *const _,
            )
        };
        if result == 0 {
            return Err(sys::wsa_error());
        }
        op.in_flight = true;
        record.receive_commit = true;
        Ok(())
    }

    fn submit_send(&mut self, key: u64) -> io::Result<()> {
        let op = self.operations.get_mut(key).unwrap();
        let record = self.sockets.get_mut(op.socket.0).unwrap();
        let OperationKind::Send {
            data,
            coalesced,
            destination,
            segment,
            offset,
            registration,
            result,
            ..
        } = &mut op.kind
        else {
            unreachable!()
        };
        let data = data.as_ref().unwrap();
        if data.is_empty() && record.info.kind == SocketKind::TcpStream {
            *result = Some(Ok(0));
            return Ok(());
        }
        let (pointer, length) = if let Some(buffer) = coalesced {
            (buffer.as_ptr(), buffer.len())
        } else if data.is_empty() {
            (ptr::null(), 0)
        } else {
            let segments = data.segments();
            while *segment < segments.len() && *offset == segments[*segment].len() {
                *segment += 1;
                *offset = 0;
            }
            let buffer = &segments[*segment];
            (
                unsafe { buffer.as_ptr().add(*offset) },
                (buffer.len() - *offset).min(u32::MAX as usize),
            )
        };
        if let Some(address) = destination {
            self.rio.prepare_metadata(key)?;
            op.address_buf = self.rio.address_buffer(key);
            unsafe {
                sys::write_address(&mut self.rio.metadata_mut(key).address, *address);
            }
        }
        op.data_buf = if length == 0 {
            RIO_BUF::default()
        } else {
            let Some((index, buffer)) = self.rio.acquire_send(pointer, length)? else {
                return Ok(());
            };
            *registration = Some(index);
            buffer
        };
        let data_pointer = if length == 0 {
            ptr::null()
        } else {
            &op.data_buf as *const RIO_BUF
        };
        let data_count = u32::from(length != 0);
        let submitted = unsafe {
            if destination.is_some() {
                self.rio.table.RIOSendEx.unwrap()(
                    record.rq,
                    data_pointer,
                    data_count,
                    ptr::null(),
                    &op.address_buf,
                    ptr::null(),
                    ptr::null(),
                    RIO_MSG_DEFER,
                    key as usize as *const _,
                )
            } else {
                self.rio.table.RIOSend.unwrap()(
                    record.rq,
                    data_pointer,
                    data_count,
                    RIO_MSG_DEFER,
                    key as usize as *const _,
                )
            }
        };
        if submitted == 0 {
            let error = sys::wsa_error();
            if let Some(index) = registration.take() {
                self.rio.release_send(index);
            }
            return Err(error);
        }
        op.in_flight = true;
        record.send_commit = true;
        Ok(())
    }

    fn submit_accept(&mut self, key: u64) -> io::Result<()> {
        let socket = self.operations.get(key).unwrap().socket;
        let record = self.socket(socket)?;
        let child = sys::new_socket(record.info.local_addr, SocketKind::TcpStream)?;
        sys::configure(&child, SocketKind::TcpStream, &record.options, true)?;
        let listener = record.raw()?;
        let function = record.accept_ex.unwrap();
        let control = self.control(key);
        let raw_child = child.as_raw_socket() as SOCKET;
        let op = self.operations.get_mut(key).unwrap();
        // Reuse is allowed only after the preceding IOCP completion. Neither
        // OVERLAPPED nor the AcceptEx output is part of the borrowed operation.
        unsafe {
            ptr::addr_of_mut!((*control).overlapped).write(OVERLAPPED::default());
        }
        let OperationKind::Accept { child: target, .. } = &mut op.kind else {
            unreachable!()
        };
        *target = Some(child);
        let mut bytes = 0;
        let result = unsafe {
            function(
                listener,
                raw_child,
                ptr::addr_of_mut!((*control).addresses).cast(),
                0,
                ACCEPT_ADDRESS_BYTES as u32,
                ACCEPT_ADDRESS_BYTES as u32,
                &mut bytes,
                ptr::addr_of_mut!((*control).overlapped),
            )
        };
        if result == 0 && unsafe { WSAGetLastError() } != WSA_IO_PENDING {
            let error = sys::wsa_error();
            drop(target.take());
            return Err(error);
        }
        op.in_flight = true;
        self.accept_reservations += 1;
        Ok(())
    }

    pub(super) fn commit_deferred(&mut self) -> io::Result<()> {
        self.socket_keys.clear();
        self.socket_keys
            .extend(self.sockets.iter().filter_map(|(key, s)| {
                (s.handle.is_some() && (s.receive_commit || s.send_commit)).then_some(key)
            }));
        for index in 0..self.socket_keys.len() {
            let key = self.socket_keys[index];
            if self.sockets.get(key).unwrap().datagrams.is_some() {
                self.commit_datagrams(SocketId(key));
            }
            let record = self.sockets.get_mut(key).unwrap();
            let result = (|| {
                if record.receive_commit {
                    self.rio.commit(record.rq, true)?;
                    record.receive_commit = false;
                }
                if record.send_commit {
                    self.rio.commit(record.rq, false)?;
                    record.send_commit = false;
                }
                Ok::<_, io::Error>(())
            })();
            if let Err(error) = result {
                let _ = self.close(SocketId(key));
                return Err(error);
            }
        }
        Ok(())
    }

    pub(super) fn complete_rio(&mut self, completion: RIORESULT) -> io::Result<()> {
        let key = completion.RequestContext;
        let op = self.operations.get_mut(key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "RIO completion references a retired operation",
            )
        })?;
        if !op.in_flight || op.socket.0 != completion.SocketContext {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RIO completion has an invalid socket generation or state",
            ));
        }
        op.in_flight = false;
        if matches!(op.kind, OperationKind::DatagramReceive { .. }) {
            self.complete_datagram(key, completion);
            return Ok(());
        }
        let record = self.sockets.get(op.socket.0).unwrap();
        let udp = record.info.kind == SocketKind::Udp;
        match &mut op.kind {
            OperationKind::Receive {
                writable,
                reserve,
                ready,
                eof,
            } => {
                if completion.Status != 0 {
                    if !op.cancelled && record.handle.is_some() {
                        *ready = Some(Err(io::Error::from_raw_os_error(completion.Status)));
                    }
                    return Ok(());
                }
                if completion.BytesTransferred == 0 {
                    *eof = true;
                    return Ok(());
                }
                if completion.BytesTransferred > op.data_buf.Length {
                    *ready = Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "RIO TCP completion exceeded its receive buffer",
                    )));
                    return Ok(());
                }
                let length = completion.BytesTransferred.min(op.data_buf.Length) as usize;
                let mut buffer = writable.take().unwrap();
                unsafe {
                    buffer.set_initialized_len(length);
                }
                let data = buffer.freeze();
                *reserve = Some(data.clone());
                let peer = None;
                *ready = Some(Ok(Received {
                    data,
                    peer,
                    truncated: false,
                    original_len: Some(length),
                    gro_segment_size: None,
                }));
            }
            OperationKind::Send {
                data,
                offset,
                transferred,
                registration,
                result,
                ..
            } => {
                if let Some(index) = registration.take() {
                    self.rio.release_send(index);
                }
                if completion.Status != 0 {
                    // A prefix completed by earlier RIO requests is observable.
                    // Returning that prefix lets send_all retry only the suffix.
                    *result = Some(if *transferred != 0 {
                        Ok(*transferred)
                    } else {
                        Err(io::Error::from_raw_os_error(completion.Status))
                    });
                } else if completion.BytesTransferred > op.data_buf.Length {
                    *result = Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "RIO send completion exceeded the submitted segment",
                    )));
                } else if udp {
                    *result = Some(
                        if completion.BytesTransferred as usize == data.as_ref().unwrap().len() {
                            Ok(completion.BytesTransferred as usize)
                        } else {
                            Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "RIO returned a partial UDP datagram send",
                            ))
                        },
                    );
                } else if completion.BytesTransferred == 0 {
                    *result = Some(if *transferred != 0 {
                        Ok(*transferred)
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "RIO TCP send made no progress",
                        ))
                    });
                } else {
                    *offset += completion.BytesTransferred as usize;
                    *transferred += completion.BytesTransferred as usize;
                    if *transferred == data.as_ref().unwrap().len() {
                        *result = Some(Ok(*transferred));
                    }
                    // Otherwise submit_send resumes this very segment or the
                    // next one; no later logical send can pass the socket head.
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-RIO operation in RIO completion queue",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn complete_overlapped(
        &mut self,
        pointer: *mut OVERLAPPED,
        status: i32,
    ) -> io::Result<()> {
        let base = self.controls.as_ptr().cast::<NativeControl>() as usize;
        let offset = (pointer as usize)
            .checked_sub(base)
            .filter(|offset| {
                offset % size_of::<NativeControl>() == 0
                    && offset / size_of::<NativeControl>() < self.limits.max_operations
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "socket IOCP completion has an unknown OVERLAPPED",
                )
            })?;
        // IOCP has retired the native write. Validate the address before reading
        // the generation; no reference to the rest of the control is created.
        let control = unsafe {
            self.controls
                .as_ptr()
                .cast::<NativeControl>()
                .add(offset / size_of::<NativeControl>())
        };
        let key = unsafe { ptr::addr_of!((*control).key).read() };
        let op = self.operations.get_mut(key).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "IOCP completion references a retired operation",
            )
        })?;
        if !op.in_flight {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IOCP completion has an invalid operation state",
            ));
        }
        op.in_flight = false;
        let result = if status == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status))
        };
        if matches!(op.kind, OperationKind::Connect { .. }) {
            self.finish_connect(key, result);
        } else if matches!(op.kind, OperationKind::Accept { .. }) {
            self.finish_accept(key, result);
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RIO data operation unexpectedly completed on IOCP",
            ));
        }
        Ok(())
    }

    fn finish_connect(&mut self, key: u64, completion: io::Result<()>) {
        let op = self.operations.remove(key).unwrap();
        self.sockets.get_mut(op.socket.0).unwrap().operation_refs -= 1;
        let result = (|| {
            completion?;
            if op.cancelled {
                return Err(cancelled());
            }
            let record = self.sockets.get_mut(op.socket.0).unwrap();
            let raw = record.raw()?;
            sys::set_context(raw, SO_UPDATE_CONNECT_CONTEXT, None)?;
            let socket = record.handle.as_ref().unwrap();
            record.info.local_addr = sys::address(socket.local_addr()?)?;
            record.info.peer_addr = Some(sys::address(socket.peer_addr()?)?);
            Ok(record.info.clone())
        })();
        if result.is_err() {
            let _ = self.close(op.socket);
        }
        self.pending.push_back(Event::Connected {
            token: op.token.unwrap(),
            result,
        });
    }

    fn finish_accept(&mut self, key: u64, completion: io::Result<()>) {
        let op = self.operations.get_mut(key).unwrap();
        let socket = op.socket;
        let is_cancelled = op.cancelled;
        let OperationKind::Accept { child, .. } = &mut op.kind else {
            unreachable!()
        };
        let child = child.take().unwrap();
        self.accept_reservations -= 1;
        let result = (|| {
            completion?;
            if is_cancelled {
                return Err(cancelled());
            }
            let record = self.socket(socket)?;
            sys::set_context(
                child.as_raw_socket() as _,
                SO_UPDATE_ACCEPT_CONTEXT,
                Some(record.raw()?),
            )?;
            let local = sys::address(child.local_addr()?)?;
            let peer = sys::address(child.peer_addr()?)?;
            let options = record.options.clone();
            Ok(self.insert_socket(child, SocketKind::TcpStream, local, Some(peer), &options))
        })();
        if !is_cancelled
            && let OperationKind::Accept { ready, .. } =
                &mut self.operations.get_mut(key).unwrap().kind
        {
            *ready = Some(result);
        }
    }

    fn retire(&mut self, key: u64) {
        let op = self.operations.remove(key).unwrap();
        debug_assert!(!op.in_flight);
        self.sockets.get_mut(op.socket.0).unwrap().operation_refs -= 1;
        match op.kind {
            OperationKind::Receive { .. } => {
                if let Some(record) = self.sockets.get_mut(op.socket.0) {
                    record.receive = None;
                }
                self.pending.push_back(Event::Stopped {
                    token: op.token.unwrap(),
                    result: Ok(()),
                });
            }
            OperationKind::Accept { ready, child, .. } => {
                debug_assert!(child.is_none());
                if let Some(Ok(info)) = ready {
                    let _ = self.close(info.id);
                }
                if let Some(record) = self.sockets.get_mut(op.socket.0) {
                    record.accept = None;
                }
                self.pending.push_back(Event::Stopped {
                    token: op.token.unwrap(),
                    result: Ok(()),
                });
            }
            OperationKind::Send {
                data,
                registration,
                result,
                transferred,
                ..
            } => {
                if let Some(index) = registration {
                    self.rio.release_send(index);
                }
                let data = data.unwrap();
                self.send_bytes -= data.len();
                if let Some(record) = self.sockets.get_mut(op.socket.0) {
                    debug_assert_eq!(record.send_head, Some(key));
                    record.send_head = op.next_send;
                    if record.send_head.is_none() {
                        record.send_tail = None;
                    }
                }
                let result = result.unwrap_or_else(|| {
                    if transferred != 0 {
                        Ok(transferred)
                    } else {
                        Err(cancelled())
                    }
                });
                self.pending.push_back(Event::Sent {
                    token: op.token.unwrap(),
                    outcome: SendOutcome { result, data },
                    memory_released: true,
                });
            }
            OperationKind::Connect { .. } | OperationKind::DatagramReceive { .. } => unreachable!(),
        }
    }

    fn reap_closed_sockets(&mut self) {
        self.socket_keys.clear();
        self.socket_keys
            .extend(self.sockets.iter().filter_map(|(key, record)| {
                (record.handle.is_none() && record.operation_refs == 0).then_some(key)
            }));
        for &key in &self.socket_keys {
            let record = self.sockets.remove(key).unwrap();
            if record.rq != 0 && record.info.kind == SocketKind::Udp {
                self.datagram_queue_slots -= self.limits.max_pending_receives - 1;
            }
        }
    }
}
