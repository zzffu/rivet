use super::*;
use std::{io::Read, time::Instant};

fn driver(completion_budget: usize) -> Driver {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_sockets = 32;
    config.limits.max_operations = 128;
    config.limits.completion_budget = completion_budget;
    config.limits.max_pending_receives = 4;
    config.limits.pool.bytes = 256 * 1024;
    config.limits.pool.block_size = 1024;
    config.limits.pool.max_leases = 128;
    let pool = BufferPool::new(config.limits.pool).unwrap();
    Driver::new(
        &config,
        0,
        pool,
        Arc::new(Notifier::new().unwrap()),
        Arc::new(Shared::new(1)),
    )
    .unwrap()
}

fn payload(driver: &Driver, bytes: &[u8]) -> SendBuf {
    let mut buffer = driver.pool.try_acquire().unwrap();
    buffer.extend_from_slice(bytes).unwrap();
    buffer.freeze()
}

#[test]
fn one_completion_budget_services_overlapped_before_rio_backlog_drains() {
    const BACKLOG: usize = 16;
    let mut driver = driver(1);
    let options = SocketOptions::default();
    let listener = driver
        .listen("127.0.0.1:0".parse().unwrap(), &options)
        .unwrap();
    driver.start_accept(listener.id, Token(1)).unwrap();
    driver.accept_capacity(listener.id, 1).unwrap();
    let outside_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    driver
        .connect(
            Token(2),
            outside_listener.local_addr().unwrap(),
            None,
            &options,
        )
        .unwrap();
    let mut events = Vec::new();
    driver.service(&mut events, &mut 0);
    // Both overlapped operations have real peers before the RIO backlog is
    // filled. No synthetic completion packets or alternate I/O path are used.
    let _incoming_peer = std::net::TcpStream::connect(listener.local_addr).unwrap();
    let (_outgoing_peer, _) = outside_listener.accept().unwrap();
    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    receiver
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut udp_options = SocketOptions::udp();
    udp_options.receive_chunk = 1024;
    for sequence in 0..BACKLOG {
        let sender = driver
            .bind_udp(
                "127.0.0.1:0".parse().unwrap(),
                Some(receiver.local_addr().unwrap()),
                &udp_options,
            )
            .unwrap();
        let data = payload(&driver, &[sequence as u8]);
        driver
            .send(
                sender.id,
                Token(100 + sequence as u64),
                SendPayload::Single(data),
                None,
                None,
            )
            .unwrap();
    }
    driver.service(&mut events, &mut 0);
    driver.commit_deferred().unwrap();
    let mut delivered = [false; BACKLOG];
    for _ in 0..BACKLOG {
        let mut byte = [0];
        assert_eq!(receiver.recv(&mut byte).unwrap(), 1);
        let sequence = byte[0] as usize;
        assert!(!delivered[sequence]);
        delivered[sequence] = true;
    }
    let mut sends = 0;
    let mut accepted = None;
    let mut connected = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    while sends < BACKLOG || accepted.is_none() || connected.is_none() {
        assert!(
            Instant::now() < deadline,
            "native completion scheduling stalled"
        );
        driver
            .poll(Some(Duration::from_millis(10)), &mut events)
            .unwrap();
        assert!(
            events.len() <= 1,
            "one poll exceeded its publication budget"
        );
        for event in events.drain(..) {
            match event {
                Event::Accepted {
                    token: Token(1),
                    result,
                } => {
                    result.unwrap();
                    accepted = Some(sends);
                    driver.accept_capacity(listener.id, 0).unwrap();
                }
                Event::Connected {
                    token: Token(2),
                    result,
                } => {
                    result.unwrap();
                    connected = Some(sends);
                }
                Event::Sent { outcome, .. } => {
                    assert_eq!(outcome.result.unwrap(), 1);
                    sends += 1;
                }
                _ => panic!("unexpected native completion"),
            }
        }
    }
    assert!(
        accepted.unwrap() < BACKLOG,
        "RIO starved AcceptEx until the backlog emptied"
    );
    assert!(
        connected.unwrap() < BACKLOG,
        "RIO starved ConnectEx until the backlog emptied"
    );
}

unsafe extern "system" fn reject_send(
    _queue: RIO_RQ,
    _data: *const RIO_BUF,
    _count: u32,
    _flags: u32,
    _context: *const core::ffi::c_void,
) -> i32 {
    unsafe {
        WSASetLastError(WSAENOBUFS);
    }
    0
}

#[test]
fn synchronous_continuation_failure_returns_accepted_prefix_and_retry_sends_only_suffix() {
    let mut driver = driver(8);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    driver
        .connect(
            Token(1),
            listener.local_addr().unwrap(),
            None,
            &SocketOptions::default(),
        )
        .unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    let socket = loop {
        assert!(Instant::now() < deadline, "ConnectEx did not complete");
        driver
            .poll(Some(Duration::from_millis(10)), &mut events)
            .unwrap();
        if let Some(info) = events.drain(..).find_map(|event| match event {
            Event::Connected {
                token: Token(1),
                result,
            } => Some(result.unwrap()),
            _ => None,
        }) {
            break info.id;
        }
    };
    let data = SendPayload::Vectored(vec![
        payload(&driver, b"prefix"),
        payload(&driver, b"suffix"),
    ]);
    driver.send(socket, Token(2), data, None, None).unwrap();
    driver.service(&mut events, &mut 8);
    driver.commit_deferred().unwrap();
    let key = driver.sockets.get(socket.0).unwrap().send_head.unwrap();
    // Retire the first real native request without automatically submitting its
    // continuation; the peer independently proves that prefix reached the wire.
    while driver.operations.get(key).unwrap().in_flight {
        assert!(Instant::now() < deadline, "first RIO send did not complete");
        driver.drain_rio(&mut 1).unwrap();
        std::thread::yield_now();
    }
    let mut prefix = [0; 6];
    peer.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"prefix");
    // Native resource exhaustion is nondeterministic. Inject only this syscall
    // failure; the preceding send, release, suffix retry and peer I/O are real.
    let native_send = driver.rio.table.RIOSend.replace(reject_send);
    driver.service(&mut events, &mut 8);
    driver.rio.table.RIOSend = native_send;
    let outcome = match events.pop().unwrap() {
        Event::Sent {
            token: Token(2),
            outcome,
            memory_released,
        } => {
            assert!(memory_released);
            outcome
        }
        _ => panic!("send continuation did not publish its result"),
    };
    let accepted = outcome.result.unwrap();
    assert_eq!(accepted, 6);
    let remaining = outcome.data.remaining(accepted);
    assert_eq!(remaining.segments()[0].as_slice(), b"suffix");
    driver
        .send(socket, Token(3), remaining, None, None)
        .unwrap();
    loop {
        assert!(Instant::now() < deadline, "suffix retry did not complete");
        driver
            .poll(Some(Duration::from_millis(10)), &mut events)
            .unwrap();
        if let Some(outcome) = events.drain(..).find_map(|event| match event {
            Event::Sent {
                token: Token(3),
                outcome,
                ..
            } => Some(outcome),
            _ => None,
        }) {
            assert_eq!(outcome.result.unwrap(), 6);
            break;
        }
    }
    driver.shutdown(socket, Shutdown::Write).unwrap();
    let mut suffix = Vec::new();
    peer.read_to_end(&mut suffix).unwrap();
    assert_eq!(
        suffix, b"suffix",
        "retry duplicated an already accepted prefix"
    );
}

#[test]
fn udp_zero_credits_retain_native_burst_and_cancel_stops_only_after_publication() {
    let mut driver = driver(8);
    let mut options = SocketOptions::udp();
    options.receive_chunk = 1024;
    let receiver = driver
        .bind_udp("127.0.0.1:0".parse().unwrap(), None, &options)
        .unwrap();
    driver.start_recv(receiver.id, Token(10)).unwrap();
    driver.receive_capacity(receiver.id, 4).unwrap();
    driver.receive_capacity(receiver.id, 0).unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    for sequence in 0u8..4 {
        sender.send_to(&[sequence], receiver.local_addr).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while driver
        .sockets
        .get(receiver.id.0)
        .unwrap()
        .datagrams
        .as_ref()
        .unwrap()
        .ready
        .len()
        != 4
    {
        assert!(
            Instant::now() < deadline,
            "posted receive window did not retain the native burst"
        );
        driver.drain_rio(&mut 4).unwrap();
        std::thread::yield_now();
    }
    let receive = driver.receive_snapshot(receiver.id).unwrap();
    assert_eq!(receive.publication_credits, 0);
    assert_eq!(receive.native_outstanding, Some(0));
    let rio = receive.rio.unwrap();
    assert_eq!(rio.admitted_lanes, 4);
    assert_eq!(rio.ready_results, 4);
    assert_eq!(rio.idle_lanes, 0);
    let resources = driver.resource_snapshot();
    assert_eq!(resources.pending_completions, 4);
    assert_eq!(resources.native_outstanding, Some(0));
    driver.cancel(Token(10)).unwrap();
    let mut events = Vec::new();
    driver.poll(Some(Duration::ZERO), &mut events).unwrap();
    assert!(
        events.is_empty(),
        "zero credits published data or discarded it to publish Stopped"
    );
    let receive = driver.receive_snapshot(receiver.id).unwrap();
    assert_eq!(receive.native_outstanding, Some(0));
    assert_eq!(receive.rio.unwrap().ready_results, 4);
    assert!(receive.rio.unwrap().stopping);
    assert_eq!(driver.resource_snapshot().pending_completions, 4);
    let mut held = Vec::new();
    let mut stopped = 0;
    for window in 0..2 {
        driver.receive_capacity(receiver.id, 2).unwrap();
        driver.receive_capacity(receiver.id, 2).unwrap(); // Absolute, not additive.
        driver.poll(Some(Duration::ZERO), &mut events).unwrap();
        for event in events.drain(..) {
            match event {
                Event::Received {
                    token: Token(10),
                    result,
                } => {
                    let packet = result.unwrap();
                    assert_eq!(packet.data.as_slice(), &[held.len() as u8]);
                    assert_eq!(packet.peer, Some(sender.local_addr().unwrap()));
                    held.push(packet.data);
                }
                Event::Stopped {
                    token: Token(10),
                    result,
                } => {
                    result.unwrap();
                    assert_eq!(held.len(), 4, "Stopped overtook a retained completion");
                    assert!(
                        driver.operations.is_empty(),
                        "Stopped overtook native lane retirement"
                    );
                    stopped += 1;
                }
                _ => panic!("unexpected datagram event"),
            }
        }
        assert_eq!(held.len(), (window + 1) * 2);
        assert_eq!(stopped, window);
    }
    let receive = driver.receive_snapshot(receiver.id).unwrap();
    assert_eq!(receive.native_outstanding, Some(0));
    assert!(
        receive.rio.is_none(),
        "retired lanes must not survive as an empty RIO group"
    );
    let resources = driver.resource_snapshot();
    assert_eq!(resources.operations, 0);
    assert_eq!(resources.pending_completions, 0);
    assert_eq!(resources.retiring_native, Some(0));
    assert_eq!(resources.rio_receive_queue_slots, Some(3));
    driver.close(receiver.id).unwrap();
    assert_eq!(
        driver.receive_snapshot(receiver.id).err().unwrap().kind(),
        io::ErrorKind::NotConnected
    );
    driver.poll(Some(Duration::ZERO), &mut events).unwrap();
    assert!(
        events.is_empty(),
        "closing an already stopped window duplicated Stopped"
    );
    drop(driver);
    for (sequence, data) in held.iter().enumerate() {
        assert_eq!(data.as_slice(), &[sequence as u8]);
    }
}

unsafe extern "system" fn reject_receive(
    _queue: RIO_RQ,
    _data: *const RIO_BUF,
    _count: u32,
    _local: *const RIO_BUF,
    _remote: *const RIO_BUF,
    _control: *const RIO_BUF,
    _flags: *const RIO_BUF,
    _native_flags: u32,
    _context: *const core::ffi::c_void,
) -> i32 {
    unsafe {
        WSASetLastError(WSAENOBUFS);
    }
    0
}

#[test]
fn udp_post_failure_after_import_preserves_data_and_reports_error_after_native_convergence() {
    let mut driver = driver(8);
    let mut options = SocketOptions::udp();
    options.receive_chunk = 1024;
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let original = sys::new_socket(address, SocketKind::Udp).unwrap();
    original.bind(&SockAddr::from(address)).unwrap();
    let receiver = driver
        .import(original.into(), SocketKind::Udp, &options)
        .unwrap();
    driver.start_recv(receiver.id, Token(20)).unwrap();
    driver.receive_capacity(receiver.id, 2).unwrap();
    let sender = std::net::UdpSocket::bind(address).unwrap();
    sender
        .send_to(b"completed before failure", receiver.local_addr)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while driver
        .sockets
        .get(receiver.id.0)
        .unwrap()
        .datagrams
        .as_ref()
        .unwrap()
        .ready
        .is_empty()
    {
        assert!(
            Instant::now() < deadline,
            "native datagram did not complete"
        );
        driver.drain_rio(&mut 4).unwrap();
        std::thread::yield_now();
    }
    let receive = driver.receive_snapshot(receiver.id).unwrap();
    assert_eq!(receive.native_outstanding, Some(1));
    assert_eq!(receive.rio.unwrap().ready_results, 1);
    assert_eq!(driver.resource_snapshot().pending_completions, 1);
    // Two requests were genuinely posted: one now retains data and one remains
    // native-owned. Fail only a later submission, never its real completion.
    let native_receive = driver.rio.table.RIOReceiveEx.replace(reject_receive);
    driver.receive_capacity(receiver.id, 4).unwrap();
    driver.rio.table.RIOReceiveEx = native_receive;
    let resources = driver.resource_snapshot();
    assert_eq!(resources.native_outstanding, Some(1));
    assert_eq!(resources.retiring_native, Some(1));
    assert_eq!(resources.closing_sockets, 1);
    assert_eq!(resources.pending_completions, 2);
    assert_eq!(resources.udp_rearm_allocation_failures_total, Some(0));
    assert_eq!(
        driver.receive_snapshot(receiver.id).err().unwrap().kind(),
        io::ErrorKind::NotConnected
    );
    let mut events = Vec::new();
    let mut retained = None;
    let mut stopped = 0;
    while !driver.is_idle() {
        assert!(
            Instant::now() < deadline,
            "failed UDP window did not converge"
        );
        driver
            .poll(Some(Duration::from_millis(10)), &mut events)
            .unwrap();
        for event in events.drain(..) {
            match event {
                Event::Received {
                    token: Token(20),
                    result,
                } => {
                    let packet = result.unwrap();
                    assert_eq!(packet.data.as_slice(), b"completed before failure");
                    assert_eq!(packet.peer, Some(sender.local_addr().unwrap()));
                    assert!(retained.replace(packet.data).is_none());
                }
                Event::Stopped {
                    token: Token(20),
                    result,
                } => {
                    assert_eq!(result.unwrap_err().raw_os_error(), Some(WSAENOBUFS));
                    assert!(
                        retained.is_some(),
                        "terminal error discarded already consumed data"
                    );
                    assert!(
                        driver.operations.is_empty(),
                        "terminal error preceded native cancellation"
                    );
                    stopped += 1;
                }
                _ => panic!("unexpected datagram event"),
            }
        }
    }
    assert_eq!(stopped, 1);
    let resources = driver.resource_snapshot();
    assert_eq!(resources.native_outstanding, Some(0));
    assert_eq!(resources.retiring_native, Some(0));
    assert_eq!(resources.pending_completions, 0);
    assert_eq!(resources.closing_sockets, 0);
    assert_eq!(resources.sockets, 0);
    assert_eq!(resources.rio_receive_queue_slots, Some(0));
    driver.poll(Some(Duration::ZERO), &mut events).unwrap();
    assert!(events.is_empty());
    drop(driver);
    assert_eq!(retained.unwrap().as_slice(), b"completed before failure");
}

#[test]
fn udp_rearm_snapshots_count_failed_attempts_and_keep_driver_history_after_close() {
    let mut driver = driver(8);
    let mut options = SocketOptions::udp();
    options.receive_chunk = 1024;
    let receiver = driver
        .bind_udp("127.0.0.1:0".parse().unwrap(), None, &options)
        .unwrap();
    driver.start_recv(receiver.id, Token(30)).unwrap();
    driver.receive_capacity(receiver.id, 4).unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut spare = Vec::new();
    loop {
        match driver.pool.try_acquire() {
            Ok(buffer) => spare.push(buffer),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("unexpected pool allocation error: {error}"),
        }
    }
    let mut failures = 0;
    let mut events = Vec::new();
    for reuse_reserve in [true, false] {
        sender.send_to(b"held", receiver.local_addr).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while driver
            .receive_snapshot(receiver.id)
            .unwrap()
            .rio
            .unwrap()
            .ready_results
            == 0
        {
            assert!(Instant::now() < deadline, "native receive did not complete");
            driver.drain_rio(&mut 4).unwrap();
            std::thread::yield_now();
        }
        driver.service_datagrams(receiver.id, &mut events, &mut 1);
        let Event::Received {
            token: Token(30),
            result,
        } = events.pop().unwrap()
        else {
            panic!("expected a retained datagram");
        };
        let mut held = Some(result.unwrap().data);
        assert_eq!(held.as_ref().unwrap().as_slice(), b"held");
        for _ in 0..2 {
            driver.receive_capacity(receiver.id, 4).unwrap();
            failures += 1;
            let receive = driver.receive_snapshot(receiver.id).unwrap();
            assert_eq!(receive.publication_credits, 4);
            assert_eq!(receive.native_outstanding, Some(3));
            let rio = receive.rio.unwrap();
            assert_eq!(rio.idle_lanes, 1);
            assert_eq!(rio.ready_results, 0);
            assert_eq!(rio.last_pool_blocked_lanes, 1);
            assert_eq!(rio.rearm_allocation_failures_total, failures);
            let resources = driver.resource_snapshot();
            assert_eq!(
                resources.udp_rearm_allocation_failures_total,
                Some(failures)
            );
            assert_eq!(resources.native_outstanding, Some(3));
            assert_eq!(resources, driver.resource_snapshot());
            assert_eq!(Some(rio), driver.receive_snapshot(receiver.id).unwrap().rio);
        }
        if reuse_reserve {
            drop(held.take());
        } else {
            drop(spare.pop().unwrap());
        }
        driver.rearm_datagrams(receiver.id);
        let receive = driver.receive_snapshot(receiver.id).unwrap();
        assert_eq!(receive.native_outstanding, Some(4));
        let rio = receive.rio.unwrap();
        assert_eq!(rio.last_pool_blocked_lanes, 0);
        assert_eq!(rio.idle_lanes, 0);
        assert_eq!(rio.rearm_allocation_failures_total, failures);
        assert!(rio.commit_pending);
        assert_eq!(driver.resource_snapshot().native_outstanding, Some(4));
        assert_eq!(Some(rio), driver.receive_snapshot(receiver.id).unwrap().rio);
        driver.commit_datagrams(receiver.id);
        assert!(
            !driver
                .receive_snapshot(receiver.id)
                .unwrap()
                .rio
                .unwrap()
                .commit_pending
        );
        if let Some(data) = held {
            assert_eq!(data.as_slice(), b"held");
        }
    }
    driver.close(receiver.id).unwrap();
    let resources = driver.resource_snapshot();
    assert_eq!(resources.native_outstanding, Some(4));
    assert_eq!(resources.retiring_native, Some(4));
    assert_eq!(resources.closing_sockets, 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !driver.is_idle() {
        assert!(
            Instant::now() < deadline,
            "closed receive window did not retire"
        );
        driver
            .poll(Some(Duration::from_millis(10)), &mut events)
            .unwrap();
        events.clear();
    }
    let resources = driver.resource_snapshot();
    assert_eq!(resources.sockets, 0);
    assert_eq!(resources.native_outstanding, Some(0));
    assert_eq!(
        resources.udp_rearm_allocation_failures_total,
        Some(failures)
    );
    let replacement = driver
        .bind_udp("127.0.0.1:0".parse().unwrap(), None, &options)
        .unwrap();
    assert_eq!(
        driver
            .receive_snapshot(replacement.id)
            .unwrap()
            .rio
            .unwrap()
            .rearm_allocation_failures_total,
        0
    );
    assert_eq!(
        driver
            .resource_snapshot()
            .udp_rearm_allocation_failures_total,
        Some(failures)
    );
    assert_eq!(
        driver.receive_snapshot(receiver.id).err().unwrap().kind(),
        io::ErrorKind::NotConnected
    );
    // Leave this replacement window unprimed. There are admitted records but
    // no native requests left to generate a completion; Drop must notice when
    // software service makes shutdown idle rather than wait forever on IOCP.
    assert_eq!(driver.resource_snapshot().native_outstanding, Some(0));
    assert!(!driver.is_idle());
}
