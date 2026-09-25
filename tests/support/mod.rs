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
    use std::os::windows::io::FromRawSocket;
    use windows_sys::Win32::Networking::WinSock::*;
    let raw = unsafe {
        WSASocketW(
            AF_INET as i32,
            SOCK_DGRAM,
            IPPROTO_UDP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    assert_ne!(raw, INVALID_SOCKET);
    let socket = unsafe { socket2::Socket::from_raw_socket(raw as _) };
    socket
        .bind(&socket2::SockAddr::from(
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
        ))
        .unwrap();
    socket
}
