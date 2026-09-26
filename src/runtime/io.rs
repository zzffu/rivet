use crate::{
    buffer::SendPayload,
    config::Limits,
    driver::{Arena, Driver, Event, Received, SendOutcome, SocketInfo, SocketKind, Token},
};
use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    task::{Context, Poll, Waker},
};

fn capacity(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, what)
}
fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "socket has closed")
}
fn update_waker(slot: &mut Option<Waker>, waker: &Waker) {
    if !slot.as_ref().is_some_and(|old| old.will_wake(waker)) {
        *slot = Some(waker.clone());
    }
}
fn split_gro(received: &mut Received) -> Option<Received> {
    let segment = usize::from(received.gro_segment_size.filter(|&size| size != 0)?);
    let original = received
        .original_len
        .unwrap_or(received.data.len())
        .max(received.data.len());
    if original <= segment {
        return None;
    }
    let initialized = received.data.len().min(segment);
    let remainder = Received {
        data: received.data.slice(initialized..received.data.len()),
        peer: received.peer,
        truncated: received.truncated,
        original_len: received
            .original_len
            .map(|length| length.saturating_sub(segment)),
        gro_segment_size: received.gro_segment_size,
    };
    received.data = received.data.slice(0..initialized);
    received.original_len = Some(segment);
    received.truncated = initialized < segment;
    Some(remainder)
}
struct Waiter {
    id: u64,
    waker: Waker,
}
pub(crate) struct SocketState {
    pub info: SocketInfo,
    receives: VecDeque<io::Result<Received>>,
    accepts: VecDeque<io::Result<SocketInfo>>,
    receive_token: Option<Token>,
    accept_token: Option<Token>,
    receive_waiter: Option<Waiter>,
    accept_waiter: Option<Waiter>,
    eof: bool,
    receive_error: Option<io::Error>,
    accept_error: Option<io::Error>,
    send_head: Option<Token>,
    send_tail: Option<Token>,
    send_group: Option<u64>,
    splice_read: bool,
    splice_write: bool,
    credits_dirty: bool,
}
struct Connect {
    result: Option<io::Result<SocketInfo>>,
    waker: Option<Waker>,
    abandoned: bool,
}
pub(crate) struct SendRequest {
    pub data: SendPayload,
    pub destination: Option<SocketAddr>,
    pub segment_size: Option<u16>,
    pub group: Option<u64>,
}
struct Send {
    socket: u64,
    payload: Option<SendPayload>,
    destination: Option<SocketAddr>,
    segment_size: Option<u16>,
    result: Option<SendOutcome>,
    waker: Option<Waker>,
    abandoned: bool,
    submitted: bool,
    sent: bool,
    released: bool,
    bytes: usize,
    previous: Option<Token>,
    next: Option<Token>,
    group: Option<u64>,
}
struct Splice {
    #[cfg(all(target_os = "linux", feature = "tcp-splice"))]
    source: u64,
    #[cfg(all(target_os = "linux", feature = "tcp-splice"))]
    destination: u64,
    result: Option<io::Result<usize>>,
    waker: Option<Waker>,
    abandoned: bool,
}
enum Operation {
    Connect(Connect),
    Receive(u64),
    Accept(u64),
    Send(Send),
    Splice(Splice),
}
pub(crate) struct IoState {
    sockets: Arena<SocketState>,
    operations: Arena<Operation>,
    limits: Limits,
    send_bytes: usize,
    waiter_id: u64,
    closing: bool,
    dirty_credits: Vec<u64>,
}
impl IoState {
    pub fn new(limits: &Limits) -> Self {
        Self {
            sockets: Arena::new(limits.max_sockets),
            operations: Arena::new(limits.max_operations),
            limits: limits.clone(),
            send_bytes: 0,
            waiter_id: 0,
            closing: false,
            dirty_credits: Vec::with_capacity(limits.max_sockets),
        }
    }
    pub fn can_open(&self) -> io::Result<()> {
        if self.closing {
            return Err(closed());
        }
        if self.sockets.available() == 0 {
            return Err(capacity("socket admission limit reached"));
        }
        Ok(())
    }
    pub fn can_open_kind(&self, kind: SocketKind) -> io::Result<()> {
        self.can_open()?;
        // RIO must own a receive operation before application traffic starts.
        // Other backends retain datagrams in their ordinary kernel socket queue.
        if cfg!(target_os = "windows")
            && kind == SocketKind::Udp
            && self.operations.available() == 0
        {
            return Err(capacity("UDP receive operation admission limit reached"));
        }
        Ok(())
    }

    #[cfg(target_os = "windows")]
    pub fn prime_udp(&mut self, driver: &mut Driver, key: u64) {
        let socket = self.sockets.get(key).unwrap();
        if socket.info.kind != SocketKind::Udp {
            return;
        }
        let native = socket.info.id;
        let token = self
            .allocate(Operation::Receive(key))
            .expect("preflighted UDP receive operation");
        if let Err(error) = driver.start_recv(native, token) {
            self.operations.remove(token.0);
            self.sockets.get_mut(key).unwrap().receive_error = Some(error);
            let _ = driver.close(native);
            return;
        }
        self.sockets.get_mut(key).unwrap().receive_token = Some(token);
        self.mark_credits(key);
        self.flush_capacities(driver);
    }
    pub fn register(&mut self, info: SocketInfo) -> io::Result<u64> {
        self.can_open()?;
        let listener = info.kind == SocketKind::TcpListener;
        let state = SocketState {
            info,
            receives: VecDeque::with_capacity(if listener {
                0
            } else {
                self.limits.max_pending_receives
            }),
            accepts: VecDeque::with_capacity(if listener {
                self.limits.max_pending_accepts
            } else {
                0
            }),
            receive_token: None,
            accept_token: None,
            receive_waiter: None,
            accept_waiter: None,
            eof: false,
            receive_error: None,
            accept_error: None,
            send_head: None,
            send_tail: None,
            send_group: None,
            splice_read: false,
            splice_write: false,
            credits_dirty: false,
        };
        self.sockets
            .insert(state)
            .map_err(|_| capacity("socket admission limit reached"))
    }
    pub fn is_idle_socket(&self, key: u64) -> io::Result<()> {
        let socket = self.sockets.get(key).ok_or_else(closed)?;
        if socket.receive_token.is_some()
            || socket.accept_token.is_some()
            || socket.send_head.is_some()
            || socket.splice_read
            || socket.splice_write
            || !socket.receives.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only a new idle socket may be dispatched",
            ));
        }
        Ok(())
    }
    pub fn forget_idle(&mut self, key: u64) {
        self.sockets.remove(key);
        if let Some(index) = self.dirty_credits.iter().position(|&dirty| dirty == key) {
            self.dirty_credits.swap_remove(index);
        }
    }
    pub fn restore_receive_error(&mut self, driver: &mut Driver, key: u64, error: io::Error) {
        if let Some(socket) = self.sockets.get_mut(key) {
            socket.receives.push_front(Err(error));
        }
        self.mark_credits(key);
        self.flush_capacities(driver);
    }
    pub fn new_send_group(&mut self) -> io::Result<u64> {
        self.next_waiter()
    }
    pub fn finish_send_group(&mut self, driver: &mut Driver, key: u64, group: u64) {
        if let Some(socket) = self.sockets.get_mut(key)
            && socket.send_group == Some(group)
        {
            socket.send_group = None;
        }
        self.pump_send(driver, key);
    }
    fn allocate(&mut self, operation: Operation) -> io::Result<Token> {
        if self.closing {
            return Err(closed());
        }
        self.operations
            .insert(operation)
            .map(Token)
            .map_err(|_| capacity("operation admission limit reached"))
    }
    pub fn connect(
        &mut self,
        driver: &mut Driver,
        address: SocketAddr,
        local: Option<SocketAddr>,
        options: &crate::socket::SocketOptions,
        waker: &Waker,
    ) -> io::Result<Token> {
        self.can_open()?;
        let token = self.allocate(Operation::Connect(Connect {
            result: None,
            waker: Some(waker.clone()),
            abandoned: false,
        }))?;
        if let Err(error) = driver.connect(token, address, local, options) {
            self.operations.remove(token.0);
            return Err(error);
        }
        Ok(token)
    }
    pub fn poll_connect(&mut self, token: Token, cx: &Context<'_>) -> Poll<io::Result<SocketInfo>> {
        let Some(Operation::Connect(operation)) = self.operations.get_mut(token.0) else {
            return Poll::Ready(Err(closed()));
        };
        if let Some(result) = operation.result.take() {
            self.operations.remove(token.0);
            return Poll::Ready(result);
        }
        update_waker(&mut operation.waker, cx.waker());
        Poll::Pending
    }
    pub fn abandon_connect(&mut self, driver: &mut Driver, token: Token) {
        let Some(Operation::Connect(operation)) = self.operations.get_mut(token.0) else {
            return;
        };
        operation.abandoned = true;
        operation.waker = None;
        if let Some(result) = operation.result.take() {
            if let Ok(info) = result {
                let _ = driver.close(info.id);
            }
            self.operations.remove(token.0);
        } else {
            let _ = driver.cancel(token);
        }
    }
    fn next_waiter(&mut self) -> io::Result<u64> {
        self.waiter_id = self
            .waiter_id
            .checked_add(1)
            .ok_or_else(|| capacity("logical sequence identifier space exhausted"))?;
        Ok(self.waiter_id)
    }
    fn install_waiter(slot: &mut Option<Waiter>, id: u64, cx: &Context<'_>) -> io::Result<()> {
        if let Some(waiter) = slot {
            if waiter.id != id {
                return Err(capacity(
                    "another receive/accept waiter already owns this lane",
                ));
            }
            if !waiter.waker.will_wake(cx.waker()) {
                waiter.waker = cx.waker().clone();
            }
        } else {
            *slot = Some(Waiter {
                id,
                waker: cx.waker().clone(),
            });
        }
        Ok(())
    }
    pub fn cancel_waiter(&mut self, key: u64, id: u64, accept: bool) {
        if let Some(socket) = self.sockets.get_mut(key) {
            let slot = if accept {
                &mut socket.accept_waiter
            } else {
                &mut socket.receive_waiter
            };
            if slot.as_ref().is_some_and(|w| w.id == id) {
                *slot = None;
            }
        }
    }
    pub fn poll_receive(
        &mut self,
        driver: &mut Driver,
        key: u64,
        waiter: &mut Option<u64>,
        cx: &Context<'_>,
    ) -> Poll<io::Result<Option<Received>>> {
        let id = match *waiter {
            Some(id) => id,
            None => match self.next_waiter() {
                Ok(id) => {
                    *waiter = Some(id);
                    id
                }
                Err(error) => return Poll::Ready(Err(error)),
            },
        };
        let Some(socket) = self.sockets.get_mut(key) else {
            return Poll::Ready(Err(closed()));
        };
        if socket.splice_read {
            return Poll::Ready(Err(capacity("receive lane belongs to splice")));
        }
        if let Err(error) = Self::install_waiter(&mut socket.receive_waiter, id, cx) {
            return Poll::Ready(Err(error));
        }
        if let Some(mut result) = socket.receives.pop_front() {
            // GRO is a transport aggregate, not one oversized UDP datagram.
            // Retain the unread suffix in its existing queue entry and return
            // exactly one original segment, including a short final segment.
            if let Ok(received) = &mut result
                && let Some(remainder) = split_gro(received)
            {
                socket.receives.push_front(Ok(remainder));
            }
            socket.receive_waiter = None;
            *waiter = None;
            self.mark_credits(key);
            self.flush_capacities(driver);
            return Poll::Ready(result.map(Some));
        }
        if let Some(error) = socket.receive_error.take() {
            socket.receive_waiter = None;
            *waiter = None;
            return Poll::Ready(Err(error));
        }
        if socket.eof {
            socket.receive_waiter = None;
            *waiter = None;
            return Poll::Ready(Ok(None));
        }
        if socket.receive_token.is_none() {
            let native = socket.info.id;
            let token = match self.allocate(Operation::Receive(key)) {
                Ok(token) => token,
                Err(error) => {
                    self.cancel_waiter(key, id, false);
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            };
            if let Err(error) = driver.start_recv(native, token) {
                self.operations.remove(token.0);
                self.cancel_waiter(key, id, false);
                *waiter = None;
                return Poll::Ready(Err(error));
            }
            self.sockets.get_mut(key).unwrap().receive_token = Some(token);
            self.mark_credits(key);
            self.flush_capacities(driver);
        }
        Poll::Pending
    }
    pub fn poll_accept(
        &mut self,
        driver: &mut Driver,
        key: u64,
        waiter: &mut Option<u64>,
        cx: &Context<'_>,
    ) -> Poll<io::Result<SocketInfo>> {
        let id = match *waiter {
            Some(id) => id,
            None => match self.next_waiter() {
                Ok(id) => {
                    *waiter = Some(id);
                    id
                }
                Err(error) => return Poll::Ready(Err(error)),
            },
        };
        let Some(socket) = self.sockets.get_mut(key) else {
            return Poll::Ready(Err(closed()));
        };
        if let Err(error) = Self::install_waiter(&mut socket.accept_waiter, id, cx) {
            return Poll::Ready(Err(error));
        }
        if let Some(result) = socket.accepts.pop_front() {
            socket.accept_waiter = None;
            *waiter = None;
            self.mark_credits(key);
            self.flush_capacities(driver);
            return Poll::Ready(result);
        }
        if let Some(error) = socket.accept_error.take() {
            socket.accept_waiter = None;
            *waiter = None;
            return Poll::Ready(Err(error));
        }
        if socket.accept_token.is_none() {
            let native = socket.info.id;
            let token = match self.allocate(Operation::Accept(key)) {
                Ok(token) => token,
                Err(error) => {
                    self.cancel_waiter(key, id, true);
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            };
            if let Err(error) = driver.start_accept(native, token) {
                self.operations.remove(token.0);
                self.cancel_waiter(key, id, true);
                *waiter = None;
                return Poll::Ready(Err(error));
            }
            self.sockets.get_mut(key).unwrap().accept_token = Some(token);
            self.mark_credits(key);
            self.flush_capacities(driver);
        }
        Poll::Pending
    }
    fn mark_credits(&mut self, key: u64) {
        if let Some(socket) = self.sockets.get_mut(key)
            && !socket.credits_dirty
        {
            socket.credits_dirty = true;
            self.dirty_credits.push(key);
        }
    }
    /// Called at initial admission, after a complete native event batch, or a consumer dequeue.
    /// Otherwise absolute credits could accidentally republish occupied slots.
    pub fn flush_capacities(&mut self, driver: &mut Driver) {
        while let Some(key) = self.dirty_credits.pop() {
            let Some(socket) = self.sockets.get_mut(key) else {
                continue;
            };
            socket.credits_dirty = false;
            if socket.receive_token.is_some()
                && let Err(error) = driver.receive_capacity(
                    socket.info.id,
                    self.limits.max_pending_receives - socket.receives.len(),
                )
            {
                socket.receive_error = Some(error);
                if let Some(waiter) = &socket.receive_waiter {
                    waiter.waker.wake_by_ref();
                }
            }
            if socket.accept_token.is_some()
                && let Err(error) = driver.accept_capacity(
                    socket.info.id,
                    self.limits.max_pending_accepts - socket.accepts.len(),
                )
            {
                socket.accept_error = Some(error);
                if let Some(waiter) = &socket.accept_waiter {
                    waiter.waker.wake_by_ref();
                }
            }
        }
    }
    pub fn send(
        &mut self,
        driver: &mut Driver,
        key: u64,
        request: SendRequest,
        waker: &Waker,
    ) -> Result<Token, SendOutcome> {
        let SendRequest {
            data: payload,
            destination,
            segment_size,
            group,
        } = request;
        let reject = |data, error| {
            Err(SendOutcome {
                result: Err(error),
                data,
            })
        };
        let Some(socket) = self.sockets.get(key) else {
            return reject(payload, closed());
        };
        if self.closing {
            return reject(payload, closed());
        }
        if socket.splice_write {
            return reject(payload, capacity("send lane belongs to splice"));
        }
        if payload.segments().len() > self.limits.max_iovecs {
            return reject(
                payload,
                io::Error::new(io::ErrorKind::InvalidInput, "payload exceeds iovec limit"),
            );
        }
        let bytes = payload.len();
        if bytes > self.limits.max_send_bytes.saturating_sub(self.send_bytes) {
            return reject(payload, capacity("in-flight send-byte limit reached"));
        }
        let udp = socket.info.kind == SocketKind::Udp;
        let native = socket.info.id;
        let priority = group.is_some() && socket.send_group == group;
        let previous = if udp || priority {
            None
        } else {
            socket.send_tail
        };
        let next = if priority { socket.send_head } else { None };
        let operation = Send {
            socket: key,
            payload: Some(payload),
            destination,
            segment_size,
            result: None,
            waker: Some(waker.clone()),
            abandoned: false,
            submitted: false,
            sent: false,
            released: false,
            bytes,
            previous,
            next,
            group,
        };
        let token = match self.operations.insert(Operation::Send(operation)) {
            Ok(token) => Token(token),
            Err(Operation::Send(mut operation)) => {
                return reject(
                    operation.payload.take().unwrap(),
                    capacity("operation admission limit reached"),
                );
            }
            Err(_) => unreachable!(),
        };
        self.send_bytes += bytes;
        if udp {
            let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) else {
                unreachable!()
            };
            operation.submitted = true;
            let data = operation.payload.take().unwrap();
            if let Err(outcome) = driver.send(native, token, data, destination, segment_size) {
                self.complete_send(token, outcome, true);
            }
            return Ok(token);
        }
        if let Some(previous) = previous
            && let Some(Operation::Send(operation)) = self.operations.get_mut(previous.0)
        {
            operation.next = Some(token);
        }
        if let Some(next) = next
            && let Some(Operation::Send(operation)) = self.operations.get_mut(next.0)
        {
            operation.previous = Some(token);
        }
        let socket = self.sockets.get_mut(key).unwrap();
        if socket.send_head.is_none() || priority {
            socket.send_head = Some(token);
        }
        if socket.send_tail.is_none() || !priority {
            socket.send_tail = Some(token);
        }
        self.pump_send(driver, key);
        Ok(token)
    }
    fn pump_send(&mut self, driver: &mut Driver, key: u64) {
        loop {
            let Some(socket) = self.sockets.get(key) else {
                return;
            };
            let Some(token) = socket.send_head else {
                return;
            };
            let native = socket.info.id;
            let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) else {
                return;
            };
            if operation.submitted {
                return;
            }
            if socket.send_group.is_some() && socket.send_group != operation.group {
                return;
            }
            if operation.group.is_some() {
                self.sockets.get_mut(key).unwrap().send_group = operation.group;
            }
            operation.submitted = true;
            let payload = operation.payload.take().unwrap();
            match driver.send(
                native,
                token,
                payload,
                operation.destination,
                operation.segment_size,
            ) {
                Ok(()) => return,
                Err(outcome) => {
                    self.complete_send(token, outcome, true);
                }
            }
        }
    }
    fn unlink_send(&mut self, token: Token) {
        let Some(Operation::Send(operation)) = self.operations.get(token.0) else {
            return;
        };
        let (key, previous, next) = (operation.socket, operation.previous, operation.next);
        if let Some(previous) = previous
            && let Some(Operation::Send(op)) = self.operations.get_mut(previous.0)
        {
            op.next = next;
        }
        if let Some(next) = next
            && let Some(Operation::Send(op)) = self.operations.get_mut(next.0)
        {
            op.previous = previous;
        }
        if let Some(socket) = self.sockets.get_mut(key) {
            if socket.send_head == Some(token) {
                socket.send_head = next;
            }
            if socket.send_tail == Some(token) {
                socket.send_tail = previous;
            }
        }
        if let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) {
            operation.previous = None;
            operation.next = None;
        }
    }
    fn complete_send(
        &mut self,
        token: Token,
        outcome: SendOutcome,
        memory_released: bool,
    ) -> Option<u64> {
        self.unlink_send(token);
        let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) else {
            return None;
        };
        let key = operation.socket;
        operation.sent = true;
        if memory_released && !operation.released {
            operation.released = true;
            self.send_bytes -= operation.bytes;
        }
        if !operation.abandoned {
            operation.result = Some(outcome);
        }
        if let Some(waker) = operation.waker.take() {
            waker.wake();
        }
        if operation.abandoned && operation.released {
            self.operations.remove(token.0);
        }
        Some(key)
    }
    pub fn poll_send(&mut self, token: Token, cx: &Context<'_>) -> Poll<SendOutcome> {
        let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) else {
            panic!("send operation retired before its consumer");
        };
        if let Some(outcome) = operation.result.take() {
            operation.abandoned = true;
            if operation.released {
                self.operations.remove(token.0);
            }
            return Poll::Ready(outcome);
        }
        update_waker(&mut operation.waker, cx.waker());
        Poll::Pending
    }
    pub fn abandon_send(&mut self, driver: &mut Driver, token: Token) {
        let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) else {
            return;
        };
        let key = operation.socket;
        if !operation.submitted && !operation.sent {
            self.unlink_send(token);
            if let Some(Operation::Send(operation)) = self.operations.remove(token.0) {
                self.send_bytes -= operation.bytes;
            }
            self.pump_send(driver, key);
            return;
        }
        operation.abandoned = true;
        operation.waker = None;
        operation.result = None;
        if operation.sent && operation.released {
            self.operations.remove(token.0);
        }
    }
    pub fn splice(
        &mut self,
        driver: &mut Driver,
        source: u64,
        destination: u64,
        bytes: usize,
        waker: &Waker,
    ) -> io::Result<Token> {
        if source == destination || bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "splice requires distinct sockets and a positive byte limit",
            ));
        }
        let source_socket = self.sockets.get(source).ok_or_else(closed)?;
        let destination_socket = self.sockets.get(destination).ok_or_else(closed)?;
        if source_socket.receive_token.is_some()
            || !source_socket.receives.is_empty()
            || source_socket.splice_read
            || destination_socket.send_head.is_some()
            || destination_socket.send_group.is_some()
            || destination_socket.splice_write
        {
            return Err(capacity(
                "splice requires unused receive and idle send lanes",
            ));
        }
        let native_source = source_socket.info.id;
        let native_destination = destination_socket.info.id;
        let token = self.allocate(Operation::Splice(Splice {
            #[cfg(all(target_os = "linux", feature = "tcp-splice"))]
            source,
            #[cfg(all(target_os = "linux", feature = "tcp-splice"))]
            destination,
            result: None,
            waker: Some(waker.clone()),
            abandoned: false,
        }))?;
        if let Err(error) = driver.splice(token, native_source, native_destination, bytes) {
            self.operations.remove(token.0);
            return Err(error);
        }
        self.sockets.get_mut(source).unwrap().splice_read = true;
        self.sockets.get_mut(destination).unwrap().splice_write = true;
        Ok(token)
    }
    pub fn poll_splice(&mut self, token: Token, cx: &Context<'_>) -> Poll<io::Result<usize>> {
        let Some(Operation::Splice(operation)) = self.operations.get_mut(token.0) else {
            return Poll::Ready(Err(closed()));
        };
        if let Some(result) = operation.result.take() {
            self.operations.remove(token.0);
            return Poll::Ready(result);
        }
        update_waker(&mut operation.waker, cx.waker());
        Poll::Pending
    }
    pub fn abandon_splice(&mut self, driver: &mut Driver, token: Token) {
        let Some(Operation::Splice(operation)) = self.operations.get_mut(token.0) else {
            return;
        };
        operation.abandoned = true;
        operation.waker = None;
        if operation.result.is_some() {
            self.operations.remove(token.0);
        } else {
            let _ = driver.cancel(token);
        }
    }
    pub fn close_socket(&mut self, driver: &mut Driver, key: u64) {
        let Some(socket) = self.sockets.get(key) else {
            return;
        };
        let native = socket.info.id;
        let mut token = socket.send_head;
        while let Some(current) = token {
            let Some(Operation::Send(operation)) = self.operations.get_mut(current.0) else {
                break;
            };
            token = operation.next;
            if !operation.submitted {
                let data = operation.payload.take().unwrap();
                self.complete_send(
                    current,
                    SendOutcome {
                        result: Err(closed()),
                        data,
                    },
                    true,
                );
            }
        }
        if let Some(mut socket) = self.sockets.remove(key) {
            for info in socket.accepts.drain(..).flatten() {
                let _ = driver.close(info.id);
            }
            if let Some(waiter) = socket.receive_waiter {
                waiter.waker.wake();
            }
            if let Some(waiter) = socket.accept_waiter {
                waiter.waker.wake();
            }
        }
        if let Some(index) = self.dirty_credits.iter().position(|&dirty| dirty == key) {
            self.dirty_credits.swap_remove(index);
        }
        let _ = driver.close(native);
    }
    pub fn event(&mut self, driver: &mut Driver, event: Event) {
        match event {
            Event::Connected { token, mut result } => {
                if self.closing {
                    if let Ok(info) = result {
                        let _ = driver.close(info.id);
                    }
                    result = Err(closed());
                }
                let Some(Operation::Connect(operation)) = self.operations.get_mut(token.0) else {
                    if let Ok(info) = result {
                        let _ = driver.close(info.id);
                    }
                    return;
                };
                if operation.abandoned {
                    if let Ok(info) = result {
                        let _ = driver.close(info.id);
                    }
                    self.operations.remove(token.0);
                } else {
                    operation.result = Some(result);
                    if let Some(waker) = operation.waker.take() {
                        waker.wake();
                    }
                }
            }
            Event::Accepted { token, result } => {
                let key = match self.operations.get(token.0) {
                    Some(Operation::Accept(key)) => *key,
                    _ => {
                        if let Ok(info) = result {
                            let _ = driver.close(info.id);
                        }
                        return;
                    }
                };
                if let Some(socket) = self.sockets.get_mut(key) {
                    assert!(
                        socket.accepts.len() < self.limits.max_pending_accepts,
                        "backend exceeded accept credits"
                    );
                    socket.accepts.push_back(result);
                    if let Some(waiter) = &socket.accept_waiter {
                        waiter.waker.wake_by_ref();
                    }
                } else if let Ok(info) = result {
                    let _ = driver.close(info.id);
                }
                self.mark_credits(key);
            }
            Event::Received { token, result } => {
                let key = match self.operations.get(token.0) {
                    Some(Operation::Receive(key)) => *key,
                    _ => return,
                };
                if let Some(socket) = self.sockets.get_mut(key) {
                    assert!(
                        socket.receives.len() < self.limits.max_pending_receives,
                        "backend exceeded receive credits"
                    );
                    socket.receives.push_back(result);
                    if let Some(waiter) = &socket.receive_waiter {
                        waiter.waker.wake_by_ref();
                    }
                }
                self.mark_credits(key);
            }
            Event::ReceiveEof { token } => {
                if let Some(Operation::Receive(key)) = self.operations.get(token.0)
                    && let Some(socket) = self.sockets.get_mut(*key)
                {
                    socket.eof = true;
                    if let Some(waiter) = &socket.receive_waiter {
                        waiter.waker.wake_by_ref();
                    }
                }
            }
            Event::Sent {
                token,
                outcome,
                memory_released,
            } => {
                if let Some(key) = self.complete_send(token, outcome, memory_released) {
                    self.pump_send(driver, key);
                }
            }
            #[cfg(all(target_os = "linux", feature = "zc-tx"))]
            Event::Released { token } => {
                if let Some(Operation::Send(operation)) = self.operations.get_mut(token.0) {
                    if !operation.released {
                        operation.released = true;
                        self.send_bytes -= operation.bytes;
                    }
                    if operation.sent && operation.abandoned {
                        self.operations.remove(token.0);
                    }
                }
            }
            #[cfg(all(target_os = "linux", feature = "tcp-splice"))]
            Event::Spliced { token, result } => {
                if let Some(Operation::Splice(operation)) = self.operations.get_mut(token.0) {
                    if let Some(socket) = self.sockets.get_mut(operation.source) {
                        socket.splice_read = false;
                    }
                    if let Some(socket) = self.sockets.get_mut(operation.destination) {
                        socket.splice_write = false;
                    }
                    if operation.abandoned {
                        self.operations.remove(token.0);
                    } else {
                        operation.result = Some(result);
                        if let Some(waker) = operation.waker.take() {
                            waker.wake();
                        }
                    }
                }
            }
            Event::Stopped { token, result } => match self.operations.get(token.0) {
                Some(Operation::Receive(key)) => {
                    if let Some(socket) = self.sockets.get_mut(*key) {
                        socket.receive_token = None;
                        if let Err(error) = result {
                            socket.receive_error = Some(error);
                        }
                        if let Some(waiter) = &socket.receive_waiter {
                            waiter.waker.wake_by_ref();
                        }
                    }
                    self.operations.remove(token.0);
                }
                Some(Operation::Accept(key)) => {
                    if let Some(socket) = self.sockets.get_mut(*key) {
                        socket.accept_token = None;
                        if let Err(error) = result {
                            socket.accept_error = Some(error);
                        }
                        if let Some(waiter) = &socket.accept_waiter {
                            waiter.waker.wake_by_ref();
                        }
                    }
                    self.operations.remove(token.0);
                }
                _ => {}
            },
        }
    }
    pub fn begin_shutdown(&mut self, driver: &mut Driver) {
        self.closing = true;
        loop {
            let key = self.sockets.iter().next().map(|(key, _)| key);
            let Some(key) = key else {
                break;
            };
            self.close_socket(driver, key);
        }
        for (key, operation) in self.operations.iter_mut() {
            if let Operation::Connect(operation) = operation
                && let Some(result) = operation.result.take()
            {
                if let Ok(info) = result {
                    let _ = driver.close(info.id);
                }
                operation.result = Some(Err(closed()));
            }
            let _ = driver.cancel(Token(key));
        }
    }
    pub fn finish_shutdown(&mut self) {
        // Live operation futures may outlive Runtime. Keep their completed
        // results (including immutable send ownership), but no kernel refs.
        loop {
            let key = self.operations.iter().find_map(|(key, operation)| {
                let retire = match operation {
                    Operation::Receive(_) | Operation::Accept(_) => true,
                    Operation::Connect(operation) => operation.abandoned,
                    Operation::Send(operation) => operation.abandoned,
                    Operation::Splice(operation) => operation.abandoned,
                };
                retire.then_some(key)
            });
            let Some(key) = key else {
                break;
            };
            self.operations.remove(key);
        }
        self.send_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::split_gro;
    use crate::{
        buffer::{BufferPool, PoolConfig},
        driver::Received,
    };

    #[test]
    fn truncated_gro_keeps_known_datagram_lengths_without_fabricating_bytes() {
        let pool = BufferPool::new(PoolConfig {
            bytes: 64,
            block_size: 16,
            max_leases: 8,
        })
        .unwrap();
        let mut data = pool.try_acquire().unwrap();
        data.extend_from_slice(b"abcdXY").unwrap();
        let mut first = Received {
            data: data.freeze(),
            peer: Some("127.0.0.1:1234".parse().unwrap()),
            truncated: true,
            original_len: Some(11),
            gro_segment_size: Some(4),
        };
        let mut second = split_gro(&mut first).unwrap();
        let mut third = split_gro(&mut second).unwrap();
        assert!(split_gro(&mut third).is_none());
        assert_eq!(first.data.as_slice(), b"abcd");
        assert_eq!(first.original_len, Some(4));
        assert!(!first.truncated);
        assert_eq!(second.data.as_slice(), b"XY");
        assert_eq!(second.original_len, Some(4));
        assert!(second.truncated);
        assert_eq!(third.data.as_slice(), b"");
        assert_eq!(third.original_len, Some(3));
        assert!(third.truncated);
        assert_eq!(second.peer, first.peer);
        assert_eq!(third.peer, first.peer);
    }
}
