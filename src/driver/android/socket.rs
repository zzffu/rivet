use super::super::SocketKind;
use crate::socket::{OwnedSocket, SocketOptions};
use std::{
    io, mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::{AsFd, AsRawFd, FromRawFd, RawFd},
};

#[link(name = "android")]
unsafe extern "C" {
    fn android_setsocknetwork(network: u64, fd: libc::c_int) -> libc::c_int;
}

pub(super) fn require_api() -> io::Result<()> {
    // Match the NDK's pre-29 inline implementation without importing its API 29 symbol.
    let mut value = [0u8; libc::PROP_VALUE_MAX as usize];
    // SAFETY: the property name is NUL-terminated and value has the required capacity.
    let length = unsafe {
        libc::__system_property_get(c"ro.build.version.sdk".as_ptr(), value.as_mut_ptr().cast())
    };
    let api_level = value
        .get(..length as usize)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse::<u32>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid Android API level"))?;
    if api_level < 23 {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Android API 23 or later is required",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn create(address: SocketAddr, kind: SocketKind) -> io::Result<OwnedSocket> {
    let domain = if address.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    let (ty, protocol) = match kind {
        SocketKind::Udp => (libc::SOCK_DGRAM, libc::IPPROTO_UDP),
        _ => (libc::SOCK_STREAM, libc::IPPROTO_TCP),
    };
    let fd = unsafe {
        libc::socket(
            domain,
            ty | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            protocol,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedSocket::from_raw_fd(fd) })
}

pub(super) fn set_int(fd: RawFd, level: i32, name: i32, value: i32) -> io::Result<()> {
    cvt(unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (&value as *const i32).cast(),
            mem::size_of_val(&value) as _,
        )
    })
    .map(|_| ())
}

pub(super) fn get_int(fd: RawFd, level: i32, name: i32) -> io::Result<i32> {
    let mut value: i32 = 0;
    let mut length = mem::size_of_val(&value) as libc::socklen_t;
    cvt(unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            (&mut value as *mut i32).cast(),
            &mut length,
        )
    })?;
    if length as usize != mem::size_of_val(&value) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid integer socket option length",
        ));
    }
    Ok(value)
}

pub(super) fn configure(
    socket: &OwnedSocket,
    kind: SocketKind,
    ipv6: bool,
    options: &SocketOptions,
    imported: bool,
) -> io::Result<()> {
    options.validate()?;
    let fd = socket.as_raw_fd();
    // Host protection precedes binding as well as every possible connect/send.
    // Both errors are propagated; neither is advisory and neither is synthesized.
    if let Some(hook) = &options.hook {
        hook.configure(socket.as_fd())?;
    }
    if kind != SocketKind::Udp {
        crate::socket::reject_blocking_linger(&socket2::SockRef::from(socket))?;
    }
    if let Some(network) = options.android_network {
        cvt(unsafe { android_setsocknetwork(network, fd) })?;
    }
    set_int(
        fd,
        libc::SOL_SOCKET,
        libc::SO_REUSEADDR,
        i32::from(options.reuse_address),
    )?;
    set_int(
        fd,
        libc::SOL_SOCKET,
        libc::SO_REUSEPORT,
        i32::from(options.reuse_port),
    )?;
    if kind == SocketKind::Udp {
        set_int(fd, libc::SOL_SOCKET, libc::SO_BROADCAST, 0)?;
    }
    if kind != SocketKind::Udp {
        set_int(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            i32::from(options.nodelay),
        )?;
        set_int(
            fd,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            i32::from(options.keepalive),
        )?;
    }
    if let Some(bytes) = options.receive_buffer_bytes {
        set_int(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, checked_size(bytes)?)?;
    }
    if let Some(bytes) = options.send_buffer_bytes {
        set_int(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, checked_size(bytes)?)?;
    }
    if ipv6 && let Some(only_v6) = options.only_v6 {
        if imported {
            if (get_int(fd, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY)? != 0) != only_v6 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "imported IPv6 socket has a different IPV6_V6ONLY setting",
                ));
            }
        } else {
            set_int(
                fd,
                libc::IPPROTO_IPV6,
                libc::IPV6_V6ONLY,
                i32::from(only_v6),
            )?;
        }
    }
    Ok(())
}

pub(super) fn validate_import(
    fd: RawFd,
    kind: SocketKind,
) -> io::Result<(SocketAddr, Option<SocketAddr>)> {
    let ty = get_int(fd, libc::SOL_SOCKET, libc::SO_TYPE)?;
    let protocol = get_int(fd, libc::SOL_SOCKET, libc::SO_PROTOCOL)?;
    let (expected_type, expected_protocol) = if kind == SocketKind::Udp {
        (libc::SOCK_DGRAM, libc::IPPROTO_UDP)
    } else {
        (libc::SOCK_STREAM, libc::IPPROTO_TCP)
    };
    if ty != expected_type || protocol != expected_protocol {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "imported socket has the wrong type or protocol",
        ));
    }
    let local = local_addr(fd)?;
    let peer = peer_addr(fd)?;
    let listening = get_int(fd, libc::SOL_SOCKET, libc::SO_ACCEPTCONN)? != 0;
    match kind {
        SocketKind::TcpListener if !listening => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "imported TCP listener is not listening",
            ));
        }
        SocketKind::TcpStream if listening || peer.is_none() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "imported TCP stream is not connected",
            ));
        }
        SocketKind::Udp if local.port() == 0 => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "imported UDP socket must be bound",
            ));
        }
        _ => {}
    }
    validate_unicast(local)?;
    if let Some(peer) = peer {
        validate_unicast(peer)?;
    }
    Ok((local, peer))
}

pub(super) fn validate_unicast(address: SocketAddr) -> io::Result<()> {
    let mapped = match address {
        SocketAddr::V4(address) => Some(*address.ip()),
        SocketAddr::V6(address) => address.ip().to_ipv4_mapped(),
    };
    if address.ip().is_multicast()
        || mapped.is_some_and(|ip| ip.is_multicast() || ip == Ipv4Addr::BROADCAST)
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "multicast and broadcast are outside the networking contract",
        ));
    }
    Ok(())
}

pub(super) fn bind(fd: RawFd, address: SocketAddr) -> io::Result<()> {
    validate_unicast(address)?;
    let address = socket2::SockAddr::from(address);
    cvt(unsafe { libc::bind(fd, address.as_ptr().cast(), address.len()) }).map(|_| ())
}

pub(super) fn connect(fd: RawFd, address: SocketAddr) -> io::Result<bool> {
    validate_unicast(address)?;
    let address = socket2::SockAddr::from(address);
    if unsafe { libc::connect(fd, address.as_ptr().cast(), address.len()) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EINPROGRESS | libc::EALREADY | libc::EINTR) => Ok(false),
        Some(libc::EISCONN) => Ok(true),
        _ => Err(error),
    }
}

pub(super) fn local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    address(fd, false).map(Option::unwrap)
}
pub(super) fn peer_addr(fd: RawFd) -> io::Result<Option<SocketAddr>> {
    address(fd, true)
}

fn address(fd: RawFd, peer: bool) -> io::Result<Option<SocketAddr>> {
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut length = mem::size_of_val(&storage) as libc::socklen_t;
    let result = unsafe {
        if peer {
            libc::getpeername(
                fd,
                (&mut storage as *mut libc::sockaddr_storage).cast(),
                &mut length,
            )
        } else {
            libc::getsockname(
                fd,
                (&mut storage as *mut libc::sockaddr_storage).cast(),
                &mut length,
            )
        }
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        if peer && error.raw_os_error() == Some(libc::ENOTCONN) {
            return Ok(None);
        }
        return Err(error);
    }
    decode(&storage, length).map(Some)
}

pub(super) fn decode(
    storage: &libc::sockaddr_storage,
    length: libc::socklen_t,
) -> io::Result<SocketAddr> {
    match i32::from(storage.ss_family) {
        libc::AF_INET if length as usize >= mem::size_of::<libc::sockaddr_in>() => {
            let addr =
                unsafe { &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(addr.sin_port),
            )))
        }
        libc::AF_INET6 if length as usize >= mem::size_of::<libc::sockaddr_in6>() => {
            let addr = unsafe {
                &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>()
            };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(addr.sin6_addr.s6_addr),
                u16::from_be(addr.sin6_port),
                u32::from_be(addr.sin6_flowinfo),
                addr.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "socket address is not a complete IPv4/IPv6 address",
        )),
    }
}

pub(super) fn cvt(result: i32) -> io::Result<i32> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn checked_size(bytes: usize) -> io::Result<i32> {
    i32::try_from(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket buffer exceeds the native integer range",
        )
    })
}
