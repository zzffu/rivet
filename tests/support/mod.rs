pub fn family_count() -> usize {
    if std::env::var_os("RIVET_VERIFY_IPV4_ONLY").as_deref() == Some(std::ffi::OsStr::new("1")) {
        eprintln!("IPv6 not exercised: RIVET_VERIFY_IPV4_ONLY=1");
        1
    } else {
        2
    }
}

#[cfg(windows)]
pub fn registered_udp() -> socket2::Socket {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM.registered_io(),
        Some(socket2::Protocol::UDP),
    )
    .unwrap();
    socket
        .bind(&socket2::SockAddr::from(
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
        ))
        .unwrap();
    socket
}
