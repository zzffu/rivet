use rivet::{
    Optimization, Policy, Runtime, RuntimeConfig, SendPayload, SocketOptions, TcpListener,
    TcpStream, UdpSocket,
};
use std::{
    future::{Future, poll_fn},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr},
    os::fd::{AsRawFd, BorrowedFd, OwnedFd},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

#[path = "../../../examples/support/services.rs"]
mod runtime_services;

pub struct CaseResult {
    pub name: &'static str,
    pub status: &'static str,
    pub detail: String,
}

pub fn config() -> RuntimeConfig {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 64;
    config.limits.max_sockets = 64;
    config.limits.max_operations = 128;
    config.limits.max_pending_receives = 1;
    config.limits.max_pending_accepts = 1;
    config.limits.completion_budget = 8;
    config.limits.max_send_bytes = 1024 * 1024;
    config.limits.pool.bytes = 2 * 1024 * 1024;
    config.limits.pool.max_leases = 256;
    config
}

fn check(condition: bool, detail: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(detail))
    }
}

fn payload(bytes: &[u8]) -> io::Result<SendPayload> {
    let pool = rivet::runtime::buffer_pool()?;
    let mut buffer = pool.try_acquire_at_least(bytes.len().max(1))?;
    buffer.extend_from_slice(bytes)?;
    Ok(SendPayload::Single(buffer.freeze()))
}

async fn join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut result_a = None;
    let mut result_b = None;
    poll_fn(|cx| {
        if result_a.is_none()
            && let Poll::Ready(value) = a.as_mut().poll(cx)
        {
            result_a = Some(value);
        }
        if result_b.is_none()
            && let Poll::Ready(value) = b.as_mut().poll(cx)
        {
            result_b = Some(value);
        }
        if result_a.is_some() && result_b.is_some() {
            Poll::Ready((result_a.take().unwrap(), result_b.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}

async fn pair(ip: IpAddr) -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind(SocketAddr::new(ip, 0))?;
    let (client, server) = join(TcpStream::connect(listener.local_addr()), listener.accept()).await;
    Ok((client?, server?))
}

async fn receive_exact(stream: &TcpStream, expected: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < expected.len() {
        let bytes = stream.recv().await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "TCP ended before all bytes arrived",
            )
        })?;
        check(
            !bytes.is_empty() && offset + bytes.len() <= expected.len(),
            "TCP receive length mismatch",
        )?;
        check(
            bytes.as_slice() == &expected[offset..offset + bytes.len()],
            "TCP data-integrity mismatch",
        )?;
        offset += bytes.len();
    }
    Ok(())
}

pub async fn tcp_full_duplex(ip: IpAddr) -> io::Result<String> {
    let (client, server) = pair(ip).await?;
    let a: Vec<u8> = (0..256 * 1024)
        .map(|n| ((n * 31 + n / 7) % 251) as u8)
        .collect();
    let b: Vec<u8> = (0..256 * 1024)
        .map(|n| ((n * 17 + n / 11) % 253) as u8)
        .collect();
    let send_a = payload(&a)?;
    let send_b = payload(&b)?;
    let client_side = async {
        let (sent, received) = join(client.send_all(send_a), receive_exact(&client, &b)).await;
        check(sent.result? == a.len(), "client short send_all")?;
        received
    };
    let server_side = async {
        let (sent, received) = join(server.send_all(send_b), receive_exact(&server, &a)).await;
        check(sent.result? == b.len(), "server short send_all")?;
        received
    };
    let (left, right) = join(client_side, server_side).await;
    left?;
    right?;
    client.shutdown(Shutdown::Write)?;
    check(
        server.recv().await?.is_none(),
        "write half-close did not produce peer EOF",
    )?;
    let response = b"read half remains usable after write shutdown";
    let sent = server.send_all(payload(response)?).await;
    check(
        sent.result? == response.len(),
        "response after half-close was short",
    )?;
    receive_exact(&client, response).await?;
    server.shutdown(Shutdown::Write)?;
    check(client.recv().await?.is_none(), "opposite TCP EOF missing")?;
    Ok("524288 bidirectional bytes verified; ordered receive, independent send/receive, and both half-closes".to_owned())
}

pub async fn udp_datagrams(ip: IpAddr) -> io::Result<String> {
    let mut options = SocketOptions::udp();
    options.receive_chunk = 4;
    let receiver = UdpSocket::bind_with_options(SocketAddr::new(ip, 0), options)?;
    let sender = UdpSocket::bind(SocketAddr::new(ip, 0))?;
    let sent = sender.send_to(payload(&[])?, receiver.local_addr()).await;
    check(
        sent.result? == 0 && sent.data.is_empty(),
        "empty UDP send was not preserved",
    )?;
    let empty = receiver.recv().await?;
    check(
        empty.data.is_empty() && !empty.truncated && empty.original_len == Some(0),
        "empty UDP datagram was lost or treated as EOF",
    )?;
    check(
        empty.peer == Some(sender.local_addr()),
        "empty UDP source address mismatch",
    )?;
    let bytes = b"truncated datagram contents";
    let sent = sender.send_to(payload(bytes)?, receiver.local_addr()).await;
    check(
        sent.result? == bytes.len() && sent.data.segments()[0].as_slice() == bytes,
        "send ownership or UDP result mismatch",
    )?;
    let truncated = receiver.recv().await?;
    check(
        truncated.data.as_slice() == &bytes[..4]
            && truncated.truncated
            && truncated.original_len == Some(bytes.len()),
        "UDP truncation metadata mismatch",
    )?;
    check(
        truncated.peer == Some(sender.local_addr()),
        "UDP sender address mismatch",
    )?;
    drop(truncated);
    drop(empty);
    check(
        matches!(
            rivet::time::timeout(Duration::from_millis(15), receiver.recv()).await,
            Err(rivet::time::TimeoutError::Elapsed)
        ),
        "receive timeout did not expire",
    )?;
    let sent = sender
        .send_to(payload(b"next")?, receiver.local_addr())
        .await;
    check(sent.result? == 4, "post-timeout send failed")?;
    let next = receiver.recv().await?;
    check(
        next.data.as_slice() == b"next" && !next.truncated,
        "receive cancellation broke the next datagram",
    )?;
    Ok(
        "empty/source/truncation/original length/owned send and timeout cancellation verified"
            .to_owned(),
    )
}

pub async fn queued_tcp_cancellation() -> io::Result<String> {
    let (client, server) = pair(Ipv4Addr::LOCALHOST.into()).await?;
    let bytes = b"bytes queued while a receive waiter is abandoned";
    {
        let mut abandoned = pin!(server.recv());
        poll_fn(|cx| match abandoned.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(Ok(())),
            Poll::Ready(_) => Poll::Ready(Err(io::Error::other(
                "empty TCP stream unexpectedly completed a receive",
            ))),
        })
        .await?;
        let sent = client.send_all(payload(bytes)?).await;
        check(
            sent.result? == bytes.len(),
            "queued cancellation setup send failed",
        )?;
        rivet::time::sleep(Duration::from_millis(10)).await?;
        // The waiter is dropped only after the runtime had time to queue bytes.
        // Dropping it must not authorize dropping those already consumed bytes.
    }
    receive_exact(&server, bytes).await?;
    Ok(
        "dropping a pending waiter preserved TCP bytes already queued by the persistent receive"
            .to_owned(),
    )
}

pub async fn edge_credit_rearm() -> io::Result<String> {
    let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let sender = UdpSocket::bind_connected(
        (Ipv4Addr::LOCALHOST, 0).into(),
        receiver.local_addr(),
        SocketOptions::udp(),
    )?;
    for sequence in 0u32..32 {
        let sent = sender.send(payload(&sequence.to_be_bytes())?).await;
        check(sent.result? == 4, "connected UDP send failed")?;
    }
    for sequence in 0u32..32 {
        let received = receiver.recv().await?;
        check(
            received.data.as_slice() == sequence.to_be_bytes() && !received.truncated,
            "ET receive stalled, duplicated, or lost a datagram across one-slot credits",
        )?;
    }
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let mut clients = Vec::new();
    for _ in 0..6 {
        clients.push(TcpStream::connect(listener.local_addr()).await?);
    }
    let mut accepted = Vec::new();
    for _ in 0..6 {
        accepted.push(listener.accept().await?);
    }
    for (client, accepted) in clients.iter().zip(&accepted) {
        let sent = client.send_all(payload(b"accepted")?).await;
        check(sent.result? == 8, "accepted client send failed")?;
        receive_exact(accepted, b"accepted").await?;
    }
    Ok("32 connected datagrams and 6 accepts resumed across single-slot ET credits".to_owned())
}

pub async fn import_ownership() -> io::Result<String> {
    let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let external = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    let original = external.as_raw_fd();
    let socket: OwnedFd = external.into();
    let failure = match TcpStream::import(socket, SocketOptions::default()) {
        Ok(_) => {
            return Err(io::Error::other(
                "wrong-kind native import unexpectedly succeeded",
            ));
        }
        Err(error) => error,
    };
    check(
        failure.socket.as_raw_fd() == original,
        "failed import did not return the original owning fd",
    )?;
    let returned = std::net::UdpSocket::from(failure.socket);
    check(
        returned.send_to(b"returned", receiver.local_addr())? == 8,
        "returned import socket cannot send",
    )?;
    check(
        receiver.recv().await?.data.as_slice() == b"returned",
        "returned import socket data mismatch",
    )?;
    let imported =
        UdpSocket::import(returned.into(), SocketOptions::udp()).map_err(|error| error.error)?;
    let sent = imported
        .send_to(payload(b"imported")?, receiver.local_addr())
        .await;
    check(sent.result? == 8, "successful native import send failed")?;
    check(
        receiver.recv().await?.data.as_slice() == b"imported",
        "successful import data mismatch",
    )?;
    Ok("wrong-kind import returned the exact usable owning fd; later successful import transferred ownership".to_owned())
}

fn descriptor_flags(fd: i32) -> io::Result<(i32, i32)> {
    let status = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if status < 0 {
        return Err(io::Error::last_os_error());
    }
    let descriptor = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((status, descriptor))
}

fn set_linger(fd: i32, seconds: Option<i32>) -> io::Result<()> {
    let value = libc::linger {
        l_onoff: i32::from(seconds.is_some()),
        l_linger: seconds.unwrap_or(0),
    };
    if unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&value as *const libc::linger).cast(),
            std::mem::size_of_val(&value) as _,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn socket_linger(fd: i32) -> io::Result<(i32, i32)> {
    let mut value = libc::linger {
        l_onoff: 0,
        l_linger: 0,
    };
    let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&mut value as *mut libc::linger).cast(),
            &mut length,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    check(
        length as usize == std::mem::size_of_val(&value),
        "SO_LINGER returned an invalid length",
    )?;
    Ok((value.l_onoff, value.l_linger))
}

pub async fn inherited_tcp_linger() -> io::Result<String> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let external = std::net::TcpStream::connect(listener.local_addr())?;
    let raw = external.as_raw_fd();
    set_linger(raw, Some(1))?;
    let flags = descriptor_flags(raw)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let options = SocketOptions {
        hook: Some(Arc::new(move |socket: BorrowedFd<'_>| {
            recorded.fetch_add(1, Ordering::SeqCst);
            set_linger(socket.as_raw_fd(), None)
        })),
        ..SocketOptions::default()
    };
    let failure = match TcpStream::import(external.into(), options) {
        Ok(_) => {
            return Err(io::Error::other(
                "TCP import accepted inherited positive linger",
            ));
        }
        Err(error) => error,
    };
    check(
        failure.error.kind() == io::ErrorKind::InvalidInput && failure.socket.as_raw_fd() == raw,
        "linger rejection did not return the original TCP fd",
    )?;
    check(
        descriptor_flags(raw)? == flags && socket_linger(raw)? == (1, 1),
        "linger rejection mutated native mode or replaced the caller's linger",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 0,
        "inherited positive linger was checked only after the setup hook",
    )?;
    set_linger(raw, None)?;
    let imported =
        TcpStream::import(failure.socket, SocketOptions::default()).map_err(|error| error.error)?;
    let accepted = listener.accept().await?;
    let sent = imported.send_all(payload(b"disabled-linger")?).await;
    check(
        sent.result? == 15,
        "returned TCP fd could not be imported with disabled linger",
    )?;
    receive_exact(&accepted, b"disabled-linger").await?;

    let external = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let raw = external.as_raw_fd();
    set_linger(raw, Some(1))?;
    let flags = descriptor_flags(raw)?;
    let failure = match TcpListener::import(external.into(), SocketOptions::default()) {
        Ok(_) => {
            return Err(io::Error::other(
                "listener import accepted inherited positive linger",
            ));
        }
        Err(error) => error,
    };
    check(
        failure.error.kind() == io::ErrorKind::InvalidInput && failure.socket.as_raw_fd() == raw,
        "listener linger rejection lost owning fd",
    )?;
    check(
        descriptor_flags(raw)? == flags && socket_linger(raw)? == (1, 1),
        "listener linger rejection changed its native settings",
    )?;
    set_linger(raw, Some(0))?;
    let imported = TcpListener::import(failure.socket, SocketOptions::default())
        .map_err(|error| error.error)?;
    let (client, accepted) =
        join(TcpStream::connect(imported.local_addr()), imported.accept()).await;
    let client = client?;
    let accepted = accepted?;
    let sent = client.send_all(payload(b"zero-linger")?).await;
    check(
        sent.result? == 11,
        "zero-linger listener did not accept usable TCP",
    )?;
    receive_exact(&accepted, b"zero-linger").await?;
    Ok("positive stream/listener linger rejected before hooks or fd-mode changes; original ownership and linger retained; explicitly disabled and zero linger remained usable".to_owned())
}

pub async fn hook_tcp_linger() -> io::Result<String> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let options = SocketOptions {
        hook: Some(Arc::new(move |socket: BorrowedFd<'_>| {
            recorded.fetch_add(1, Ordering::SeqCst);
            set_linger(socket.as_raw_fd(), Some(1))
        })),
        ..SocketOptions::default()
    };
    check(
        matches!(TcpStream::connect_with_options(listener.local_addr(), options.clone()).await,
        Err(error) if error.kind() == io::ErrorKind::InvalidInput),
        "connect accepted hook-configured positive linger",
    )?;
    check(
        matches!(TcpListener::bind_with_options((Ipv4Addr::LOCALHOST, 0).into(), options.clone()),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput),
        "listener accepted hook-configured positive linger",
    )?;
    check(
        matches!(
            rivet::time::timeout(Duration::from_millis(25), listener.accept()).await,
            Err(rivet::time::TimeoutError::Elapsed)
        ),
        "linger-denied connect reached the peer",
    )?;
    let external = std::net::TcpStream::connect(listener.local_addr())?;
    let raw = external.as_raw_fd();
    let flags = descriptor_flags(raw)?;
    let failure = match TcpStream::import(external.into(), options) {
        Ok(_) => {
            return Err(io::Error::other(
                "import accepted hook-configured positive linger",
            ));
        }
        Err(error) => error,
    };
    check(
        failure.error.kind() == io::ErrorKind::InvalidInput && failure.socket.as_raw_fd() == raw,
        "post-hook linger rejection lost native import ownership",
    )?;
    check(
        descriptor_flags(raw)? == flags && socket_linger(raw)? == (1, 1),
        "post-hook linger rejection changed fd mode or silently substituted linger",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 3,
        "a TCP setup path did not execute its configuration hook",
    )?;
    set_linger(raw, None)?;
    let imported =
        TcpStream::import(failure.socket, SocketOptions::default()).map_err(|error| error.error)?;
    let accepted = listener.accept().await?;
    let sent = imported.send_all(payload(b"returned-after-hook")?).await;
    check(
        sent.result? == 19,
        "post-hook rejection returned an unusable TCP fd",
    )?;
    receive_exact(&accepted, b"returned-after-hook").await?;
    Ok("positive linger introduced by TCP connect/listen/import hooks was rejected before establishment or fd-mode changes; rejected import retained ownership".to_owned())
}

pub async fn connected_import_network_rejection(network: u64) -> io::Result<String> {
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let options = SocketOptions {
        android_network: Some(network),
        hook: Some(Arc::new(move |_: BorrowedFd<'_>| {
            recorded.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "connected Network import must reject before this hook",
            ))
        })),
        ..SocketOptions::default()
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let external = std::net::TcpStream::connect(listener.local_addr())?;
    let raw = external.as_raw_fd();
    let flags = descriptor_flags(raw)?;
    let failure = match TcpStream::import(external.into(), options.clone()) {
        Ok(_) => {
            return Err(io::Error::other(
                "connected TCP import accepted a late Network binding request",
            ));
        }
        Err(error) => error,
    };
    check(
        failure.error.kind() == io::ErrorKind::InvalidInput && failure.socket.as_raw_fd() == raw,
        "late TCP Network rejection lost native ownership or reached setup",
    )?;
    check(
        descriptor_flags(raw)? == flags,
        "late TCP Network rejection changed fd flags",
    )?;
    let returned = std::net::TcpStream::from(failure.socket);
    check(
        !returned.nodelay()? && returned.peer_addr()? == listener.local_addr(),
        "late TCP Network rejection changed the original connection",
    )?;
    let imported = TcpStream::import(returned.into(), SocketOptions::default())
        .map_err(|error| error.error)?;
    let accepted = listener.accept().await?;
    let sent = imported.send_all(payload(b"existing-tcp-route")?).await;
    check(
        sent.result? == 18,
        "connected TCP import without rebinding failed",
    )?;
    receive_exact(&accepted, b"existing-tcp-route").await?;

    let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let external = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    external.connect(receiver.local_addr())?;
    external.set_broadcast(true)?;
    let raw = external.as_raw_fd();
    let flags = descriptor_flags(raw)?;
    let failure = match UdpSocket::import(external.into(), options) {
        Ok(_) => {
            return Err(io::Error::other(
                "connected UDP import accepted a late Network binding request",
            ));
        }
        Err(error) => error,
    };
    check(
        failure.error.kind() == io::ErrorKind::InvalidInput && failure.socket.as_raw_fd() == raw,
        "late UDP Network rejection lost native ownership or reached setup",
    )?;
    check(
        descriptor_flags(raw)? == flags && calls.load(Ordering::SeqCst) == 0,
        "connected Network import mutated fd mode or invoked a hook",
    )?;
    let returned = std::net::UdpSocket::from(failure.socket);
    check(
        returned.broadcast()? && returned.peer_addr()? == receiver.local_addr(),
        "late UDP Network rejection changed native socket options or peer",
    )?;
    check(
        returned.send(b"returned-udp")? == 12,
        "late Network rejection returned an unusable UDP fd",
    )?;
    check(
        receiver.recv().await?.data.as_slice() == b"returned-udp",
        "returned connected UDP changed its data route",
    )?;
    let imported =
        UdpSocket::import(returned.into(), SocketOptions::udp()).map_err(|error| error.error)?;
    let sent = imported.send(payload(b"existing-udp-route")?).await;
    check(
        sent.result? == 18,
        "connected UDP import without rebinding failed",
    )?;
    check(
        receiver.recv().await?.data.as_slice() == b"existing-udp-route",
        "connected UDP import without rebinding lost data",
    )?;
    Ok("connected TCP/UDP rejected explicit Network requests before hooks/options/mode changes, returned the same usable fds, and imported successfully with no new binding request".to_owned())
}

pub async fn network_and_protection_errors() -> io::Result<String> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0).into())?;
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let denied = SocketOptions {
        hook: Some(Arc::new(move |socket: BorrowedFd<'_>| {
            if unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_GETFD) } < 0 {
                return Err(io::Error::last_os_error());
            }
            recorded.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "deliberate host protection denial",
            ))
        })),
        ..SocketOptions::default()
    };
    let error = match TcpStream::connect_with_options(listener.local_addr(), denied).await {
        Ok(_) => return Err(io::Error::other("host protection denial was ignored")),
        Err(error) => error,
    };
    check(
        error.kind() == io::ErrorKind::PermissionDenied && calls.load(Ordering::SeqCst) == 1,
        "host protection error was not propagated exactly once",
    )?;
    let mut invalid_network = SocketOptions {
        android_network: Some(u64::MAX),
        ..SocketOptions::default()
    };
    check(
        TcpStream::connect_with_options(listener.local_addr(), invalid_network.clone())
            .await
            .is_err(),
        "invalid Android Network was silently ignored",
    )?;
    invalid_network.nodelay = false;
    check(
        UdpSocket::bind_with_options((Ipv4Addr::LOCALHOST, 0).into(), invalid_network).is_err(),
        "UDP proceeded after Android Network binding failed",
    )?;
    check(
        matches!(
            rivet::time::timeout(Duration::from_millis(25), listener.accept()).await,
            Err(rivet::time::TimeoutError::Elapsed)
        ),
        "a denied establishment still reached the TCP listener",
    )?;
    let owned: OwnedFd = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?.into();
    let raw = owned.as_raw_fd();
    let mut denied_import = SocketOptions::udp();
    denied_import.hook = Some(Arc::new(|_: BorrowedFd<'_>| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "deliberate import protection denial",
        ))
    }));
    let failure = match UdpSocket::import(owned, denied_import) {
        Ok(_) => return Err(io::Error::other("import protection denial was ignored")),
        Err(error) => error,
    };
    check(
        failure.error.kind() == io::ErrorKind::PermissionDenied
            && failure.socket.as_raw_fd() == raw,
        "protection failure lost native import ownership",
    )?;
    let returned = std::net::UdpSocket::from(failure.socket);
    check(
        returned.local_addr()?.port() != 0,
        "returned protected-import socket is unusable",
    )?;
    Ok("real libandroid rejected an invalid Network; deliberate host callback denials prevented TCP/UDP setup and preserved import ownership; no VPN was created or changed".to_owned())
}

pub async fn network_binding(network: u64) -> io::Result<String> {
    let mut options = SocketOptions::udp();
    options.android_network = Some(network);
    let socket = UdpSocket::bind_with_options((Ipv4Addr::UNSPECIFIED, 0).into(), options.clone())?;
    check(
        socket.local_addr().port() != 0,
        "Network-bound socket was not bound",
    )?;
    let external = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    let local = external.local_addr()?;
    let imported = UdpSocket::import(external.into(), options).map_err(|error| error.error)?;
    check(
        imported.local_addr() == local && imported.peer_addr().is_none(),
        "Network binding changed an unconnected UDP import's address or peer",
    )?;
    let external = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    let local = external.local_addr()?;
    let options = SocketOptions {
        android_network: Some(network),
        ..SocketOptions::default()
    };
    let imported = TcpListener::import(external.into(), options).map_err(|error| error.error)?;
    check(
        imported.local_addr() == local,
        "Network binding changed an unconnected TCP listener import's address",
    )?;
    Ok(format!(
        "real android_setsocknetwork accepted active Network {network} for new UDP and unconnected UDP/listener imports; no process-wide binding was changed and no remote route or VPN protection was claimed"
    ))
}

pub fn lease_outlives_runtime() -> io::Result<String> {
    let mut runtime = Runtime::new(config())?;
    let lease = runtime
        .block_on(rivet::time::timeout(Duration::from_secs(8), async {
            let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
            let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
            let outcome = sender
                .send_to(
                    payload(b"lease survives owner shutdown")?,
                    receiver.local_addr(),
                )
                .await;
            check(outcome.result? == 29, "ownership setup send failed")?;
            Ok::<_, io::Error>(receiver.recv().await?.data)
        }))
        .map_err(|error| io::Error::other(error.to_string()))??;
    let alias = lease.slice(6..14);
    drop(runtime);
    check(
        lease.as_slice() == b"lease survives owner shutdown" && alias.as_slice() == b"survives",
        "runtime shutdown invalidated a live immutable receive lease",
    )?;
    drop(lease);
    check(
        alias.as_slice() == b"survives",
        "derived lease lost its backing memory",
    )?;
    Ok("owned receive data and a derived immutable alias remained valid after Runtime and sockets were destroyed".to_owned())
}

pub fn pool_pressure_recovery() -> io::Result<String> {
    let mut limits = config();
    limits.limits.pool.bytes = 4 * limits.limits.pool.block_size;
    let mut runtime = Runtime::new(limits)?;
    runtime
        .block_on(rivet::time::timeout(Duration::from_secs(8), async {
            let mut options = SocketOptions::udp();
            options.receive_chunk = 16 * 1024;
            let receiver = UdpSocket::bind_with_options((Ipv4Addr::LOCALHOST, 0).into(), options)?;
            let external = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
            let pool = rivet::runtime::buffer_pool()?;
            let mut held = Vec::new();
            loop {
                match pool.try_acquire() {
                    Ok(buffer) => held.push(buffer),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error),
                }
            }
            check(!held.is_empty(), "pool was not usable before pressure")?;
            let mut waiting = pin!(receiver.recv());
            poll_fn(|cx| match waiting.as_mut().poll(cx) {
                Poll::Pending => Poll::Ready(Ok(())),
                Poll::Ready(_) => Poll::Ready(Err(io::Error::other(
                    "receive completed without input under pool pressure",
                ))),
            })
            .await?;
            check(
                external.send_to(b"recycle-wake", receiver.local_addr())? == 12,
                "pool-pressure setup send failed",
            )?;
            rivet::time::sleep(Duration::from_millis(10)).await?;
            drop(held.pop());
            let received = waiting.await?;
            check(
                received.data.as_slice() == b"recycle-wake",
                "pool recycle did not resume an already-consumed ET edge",
            )?;
            Ok::<_, io::Error>(())
        }))
        .map_err(|error| io::Error::other(error.to_string()))??;
    Ok("finite pool exhaustion paused the readable socket without consuming its datagram; releasing one lease resumed it without a new packet".to_owned())
}

pub async fn cross_thread_wake() -> io::Result<String> {
    use std::sync::{OnceLock, atomic::AtomicBool};
    let ready = Arc::new(AtomicBool::new(false));
    let waker = Arc::new(OnceLock::<std::task::Waker>::new());
    let producer_ready = ready.clone();
    let producer_waker = waker.clone();
    let producer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        producer_ready.store(true, Ordering::Release);
        if let Some(waker) = producer_waker.get() {
            for _ in 0..256 {
                waker.wake_by_ref();
            }
        }
    });
    poll_fn(|cx| {
        waker.get_or_init(|| cx.waker().clone());
        if ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    producer
        .join()
        .map_err(|_| io::Error::other("waker producer panicked"))?;
    Ok(
        "an idle owner resumed after a cross-thread burst of 256 standard Waker notifications"
            .to_owned(),
    )
}

pub fn capability_policy() -> io::Result<String> {
    let strict = Runtime::new(config().enable(Optimization::FixedFiles));
    check(
        matches!(&strict, Err(error) if error.kind() == io::ErrorKind::Unsupported),
        "Android accepted a Linux-only required capability",
    )?;
    let auto = Runtime::new(config().with_policy(Optimization::FixedFiles, Policy::Auto))?;
    let state = auto.capabilities()[0]
        .state(Optimization::FixedFiles)
        .ok_or_else(|| io::Error::other("missing capability state"))?;
    check(
        !state.enabled && !state.supported && state.reason.is_some(),
        "Auto Linux-only capability was not reported unavailable",
    )?;
    let off = Runtime::new(config())?;
    let auto = Runtime::new(
        config()
            .with_policy(Optimization::UdpGso, Policy::Auto)
            .with_policy(Optimization::UdpGro, Policy::Auto),
    )?;
    let report = &auto.capabilities()[0];
    check(
        report.backend == "android-epoll",
        "Auto offload changed the Android backend",
    )?;
    let mut detail =
        "Linux-only RequireCapability fails and Auto reports unavailable on epoll".to_owned();
    for optimization in [Optimization::UdpGso, Optimization::UdpGro] {
        let default = off.capabilities()[0]
            .state(optimization)
            .ok_or_else(|| io::Error::other("missing default UDP offload state"))?;
        check(
            default.policy == Policy::Off && !default.enabled,
            "UDP offload was implicitly enabled",
        )?;
        let state = report
            .state(optimization)
            .ok_or_else(|| io::Error::other("missing Auto UDP offload state"))?;
        let required = Runtime::new(config().enable(optimization));
        if state.enabled {
            check(
                state.compiled && state.supported && state.reason.is_none(),
                "Auto enabled UDP offload without native capability evidence",
            )?;
            let required = required?;
            check(
                required.capabilities()[0].enabled(optimization),
                "required UDP offload was silently disabled",
            )?;
            detail.push_str(&format!(
                "; {}: Off stayed disabled, Auto and RequireCapability enabled the native path",
                optimization.name()
            ));
        } else {
            check(
                !state.supported && state.reason.is_some(),
                "Auto did not explain unavailable UDP offload",
            )?;
            let error = match required {
                Err(error) => error,
                Ok(_) => {
                    return Err(io::Error::other(
                        "RequireCapability accepted unavailable UDP offload",
                    ));
                }
            };
            check(
                error.kind() == io::ErrorKind::Unsupported,
                "unavailable UDP offload did not fail explicitly as unsupported",
            )?;
            detail.push_str(&format!("; {}: Off stayed disabled, Auto unavailable ({}), RequireCapability failed ({error})",
                optimization.name(), state.reason.as_deref().unwrap()));
        }
    }
    Ok(detail)
}

pub fn udp_offload() -> io::Result<Option<String>> {
    let requested = config()
        .with_policy(Optimization::UdpGso, Policy::Auto)
        .with_policy(Optimization::UdpGro, Policy::Auto);
    let mut runtime = Runtime::new(requested)?;
    let report = &runtime.capabilities()[0];
    if !report.enabled(Optimization::UdpGso) {
        return Ok(None);
    }
    let gro = report.enabled(Optimization::UdpGro);
    runtime
        .block_on(rivet::time::timeout(Duration::from_secs(8), async {
            let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
            let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0).into())?;
            let bytes = b"abcdefghijklmnopqr";
            let sent = sender
                .send_segments(payload(bytes)?, 4, Some(receiver.local_addr()))
                .await;
            check(sent.result? == bytes.len(), "UDP GSO send failed")?;
            for segment in bytes.chunks(4) {
                let received = receiver.recv().await?;
                check(
                    received.data.as_slice() == segment
                        && received.peer == Some(sender.local_addr())
                        && !received.truncated,
                    "GSO/GRO lost datagram boundaries, address, or last short segment",
                )?;
            }
            Ok::<_, io::Error>(())
        }))
        .map_err(|error| io::Error::other(error.to_string()))??;
    Ok(Some(format!(
        "real UDP_SEGMENT send reconstructed five datagrams including the final short segment; UDP_GRO enabled={gro}"
    )))
}

fn record(name: &'static str, result: io::Result<String>, cases: &mut Vec<CaseResult>) {
    match result {
        Ok(detail) => cases.push(CaseResult {
            name,
            status: "passed",
            detail,
        }),
        Err(error) => cases.push(CaseResult {
            name,
            status: "failed",
            detail: error.to_string(),
        }),
    }
}

fn record_async(
    name: &'static str,
    future: impl Future<Output = io::Result<String>>,
    cases: &mut Vec<CaseResult>,
) {
    let result = Runtime::new(config()).and_then(|mut runtime| {
        runtime
            .block_on(rivet::time::timeout(Duration::from_secs(8), future))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "native smoke case exceeded 8 seconds",
                )
            })?
    });
    record(name, result, cases);
}

pub fn suite(network: u64) -> Vec<CaseResult> {
    let mut cases = Vec::new();
    record(
        "runtime_host_services",
        runtime_services::run(config()),
        &mut cases,
    );
    record_async(
        "tcp_ipv4_full_duplex_half_close",
        tcp_full_duplex(Ipv4Addr::LOCALHOST.into()),
        &mut cases,
    );
    record_async(
        "tcp_ipv6_full_duplex_half_close",
        tcp_full_duplex(Ipv6Addr::LOCALHOST.into()),
        &mut cases,
    );
    record_async(
        "udp_ipv4_datagram_semantics",
        udp_datagrams(Ipv4Addr::LOCALHOST.into()),
        &mut cases,
    );
    record_async(
        "udp_ipv6_datagram_semantics",
        udp_datagrams(Ipv6Addr::LOCALHOST.into()),
        &mut cases,
    );
    record_async(
        "cancel_waiter_preserves_queued_tcp",
        queued_tcp_cancellation(),
        &mut cases,
    );
    record_async(
        "edge_triggered_credit_rearm",
        edge_credit_rearm(),
        &mut cases,
    );
    record_async(
        "coalesced_cross_thread_idle_wake",
        cross_thread_wake(),
        &mut cases,
    );
    record(
        "pool_pressure_recycle_resumes_et_read",
        pool_pressure_recovery(),
        &mut cases,
    );
    record_async("native_import_ownership", import_ownership(), &mut cases);
    record_async(
        "inherited_tcp_linger_rejected_without_mutation",
        inherited_tcp_linger(),
        &mut cases,
    );
    record_async(
        "hook_tcp_linger_rejected_before_establishment",
        hook_tcp_linger(),
        &mut cases,
    );
    // An explicit request must be rejected before any native binding even when
    // there is no active Network. This case never claims binding succeeded.
    record_async(
        "connected_import_rejects_late_network_binding",
        connected_import_network_rejection(if network == 0 { u64::MAX } else { network }),
        &mut cases,
    );
    record_async(
        "network_and_host_protection_errors",
        network_and_protection_errors(),
        &mut cases,
    );
    match udp_offload() {
        Ok(Some(detail)) => cases.push(CaseResult {
            name: "optional_udp_gso_gro",
            status: "passed",
            detail,
        }),
        Ok(None) => cases.push(CaseResult {
            name: "optional_udp_gso_gro",
            status: "skipped",
            detail: "explicit Auto could not enable GSO; offload success was not claimed"
                .to_owned(),
        }),
        Err(error) => record("optional_udp_gso_gro", Err(error), &mut cases),
    }
    if network == 0 {
        cases.push(CaseResult {
            name: "active_android_network_binding",
            status: "skipped",
            detail:
                "ConnectivityManager reported no active Network; successful binding was not claimed"
                    .to_owned(),
        });
    } else {
        record_async(
            "active_android_network_binding",
            network_binding(network),
            &mut cases,
        );
    }
    record(
        "receive_lease_outlives_runtime",
        lease_outlives_runtime(),
        &mut cases,
    );
    record(
        "strict_and_auto_capability_policy",
        capability_policy(),
        &mut cases,
    );
    cases
}

pub fn json(network: u64) -> String {
    let uid = unsafe { libc::getuid() };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    unsafe extern "C" {
        fn android_get_device_api_level() -> i32;
    }
    let api_level = unsafe { android_get_device_api_level() };
    let mut cases = suite(network);
    if uid < 10000 {
        cases.push(CaseResult {
            name: "ordinary_application_uid",
            status: "failed",
            detail: format!("UID {uid} is not an ordinary Android application UID"),
        });
    }
    let passed = cases.iter().all(|case| case.status != "failed");
    let mut json = format!(
        "{{\"status\":\"{}\",\"uid\":{uid},\"api_level\":{api_level},\"page_size\":{page_size},\"active_network\":{network},\"backend\":\"android-epoll\",\"cases\":[",
        if passed { "passed" } else { "failed" }
    );
    for (index, case) in cases.iter().enumerate() {
        if index != 0 {
            json.push(',');
        }
        json.push_str("{\"name\":");
        quote(case.name, &mut json);
        json.push_str(",\"status\":");
        quote(case.status, &mut json);
        json.push_str(",\"detail\":");
        quote(&case.detail, &mut json);
        json.push('}');
    }
    json.push_str("]}");
    json
}

fn quote(text: &str, output: &mut String) {
    use std::fmt::Write;
    output.push('"');
    for value in text.chars() {
        match value {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            ' '..='~' => output.push(value),
            value => {
                for unit in value.encode_utf16(&mut [0; 2]) {
                    write!(output, "\\u{unit:04x}").unwrap();
                }
            }
        }
    }
    output.push('"');
}
