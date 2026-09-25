#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
use super::uapi::CmsgHdr;
use crate::{
    driver::{SocketId, SocketInfo, SocketKind},
    socket::{OwnedSocket, SocketOptions, reject_blocking_linger},
};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    os::fd::{AsFd, AsRawFd, RawFd},
};

pub const CONTROL_BYTES: usize = 128;
pub const ADDRESS_BYTES: usize = 128;
#[cfg(feature = "udp-gso")]
pub const UDP_SEGMENT: i32 = 103;
#[cfg(feature = "udp-gro")]
pub const UDP_GRO: i32 = 104;

pub fn validate_address(addr: SocketAddr, destination: bool) -> io::Result<()> {
    if addr.ip().is_multicast() || addr.ip() == IpAddr::V4(Ipv4Addr::BROADCAST) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "multicast and broadcast are outside the runtime contract",
        ));
    }
    if destination && (addr.ip().is_unspecified() || addr.port() == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination must specify an address and port",
        ));
    }
    Ok(())
}

pub fn create(
    addr: SocketAddr,
    kind: SocketKind,
    options: &SocketOptions,
    gro: bool,
) -> io::Result<OwnedSocket> {
    options.validate()?;
    validate_address(addr, false)?;
    let udp = kind == SocketKind::Udp;
    let socket = Socket::new(
        Domain::for_address(addr),
        if udp { Type::DGRAM } else { Type::STREAM },
        Some(if udp { Protocol::UDP } else { Protocol::TCP }),
    )?;
    socket.set_nonblocking(true)?;
    configure(&socket, kind, options, gro)?;
    if let Some(hook) = &options.hook {
        hook.configure(socket.as_fd())?;
    }
    if !udp {
        reject_blocking_linger(&socket)?;
    }
    Ok(socket.into())
}

pub fn configure(
    socket: &impl AsFd,
    kind: SocketKind,
    options: &SocketOptions,
    _gro: bool,
) -> io::Result<()> {
    options.validate()?;
    let sock = SockRef::from(socket);
    if kind != SocketKind::Udp {
        reject_blocking_linger(&sock)?;
        sock.set_tcp_nodelay(options.nodelay)?;
        sock.set_keepalive(options.keepalive)?;
    }
    sock.set_reuse_address(options.reuse_address)?;
    sock.set_reuse_port(options.reuse_port)?;
    if let Some(v6) = options.only_v6
        && sock.only_v6()? != v6
    {
        sock.set_only_v6(v6)?;
    }
    if let Some(bytes) = options.receive_buffer_bytes {
        sock.set_recv_buffer_size(bytes)?;
    }
    if let Some(bytes) = options.send_buffer_bytes {
        sock.set_send_buffer_size(bytes)?;
    }
    #[cfg(feature = "udp-gro")]
    if kind == SocketKind::Udp && _gro {
        set_int(sock.as_raw_fd(), libc::IPPROTO_UDP, UDP_GRO, 1)?;
    }
    Ok(())
}

#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
pub fn set_int(fd: RawFd, level: i32, option: i32, value: i32) -> io::Result<()> {
    let result = unsafe {
        libc::setsockopt(
            fd,
            level,
            option,
            (&value as *const i32).cast(),
            size_of::<i32>() as libc::socklen_t,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn get_int(fd: RawFd, level: i32, option: i32) -> io::Result<i32> {
    let mut value = 0i32;
    let mut length = size_of::<i32>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            level,
            option,
            (&mut value as *mut i32).cast(),
            &mut length,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

pub fn info(socket: &OwnedSocket, id: SocketId, kind: SocketKind) -> io::Result<SocketInfo> {
    let sock = SockRef::from(socket);
    let local_addr = sock.local_addr()?.as_socket().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "only IPv4/IPv6 sockets are supported",
        )
    })?;
    let peer_addr = match sock.peer_addr() {
        Ok(addr) => Some(
            addr.as_socket()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-IP peer"))?,
        ),
        Err(e) if e.raw_os_error() == Some(libc::ENOTCONN) => None,
        Err(e) => return Err(e),
    };
    validate_address(local_addr, false)?;
    if let Some(peer) = peer_addr {
        validate_address(peer, true)?;
    }
    Ok(SocketInfo {
        id,
        kind,
        local_addr,
        peer_addr,
    })
}

pub fn validate_import(socket: &OwnedSocket, kind: SocketKind) -> io::Result<()> {
    let raw = socket.as_raw_fd();
    let actual = get_int(raw, libc::SOL_SOCKET, libc::SO_TYPE)?;
    if actual
        != if kind == SocketKind::Udp {
            libc::SOCK_DGRAM
        } else {
            libc::SOCK_STREAM
        }
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket type does not match import kind",
        ));
    }
    let listening = get_int(raw, libc::SOL_SOCKET, libc::SO_ACCEPTCONN)? != 0;
    if listening != (kind == SocketKind::TcpListener) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket listening state does not match import kind",
        ));
    }
    if kind == SocketKind::TcpStream && SockRef::from(socket).peer_addr().is_err() {
        return Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "TCP imports must already be connected",
        ));
    }
    let protocol = get_int(raw, libc::SOL_SOCKET, libc::SO_PROTOCOL)?;
    if protocol
        != if kind == SocketKind::Udp {
            libc::IPPROTO_UDP
        } else {
            libc::IPPROTO_TCP
        }
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only native TCP and UDP sockets may be imported",
        ));
    }
    let _ = info(socket, SocketId(0), kind)?;
    Ok(())
}

// These native address types have no implicit padding on supported Linux ABIs;
// copying them into the zeroed byte storage initializes every submitted byte.
const _: () = {
    assert!(size_of::<libc::sockaddr_in>() == 16);
    assert!(std::mem::offset_of!(libc::sockaddr_in, sin_port) == 2);
    assert!(std::mem::offset_of!(libc::sockaddr_in, sin_addr) == 4);
    assert!(std::mem::offset_of!(libc::sockaddr_in, sin_zero) == 8);
    assert!(size_of::<libc::sockaddr_in6>() == 28);
    assert!(std::mem::offset_of!(libc::sockaddr_in6, sin6_port) == 2);
    assert!(std::mem::offset_of!(libc::sockaddr_in6, sin6_flowinfo) == 4);
    assert!(std::mem::offset_of!(libc::sockaddr_in6, sin6_addr) == 8);
    assert!(std::mem::offset_of!(libc::sockaddr_in6, sin6_scope_id) == 24);
};

pub fn encode(addr: SocketAddr) -> ([u64; ADDRESS_BYTES / 8], i32) {
    // Store the complete representation as initialized bytes, rather than
    // copying libc sockaddr_storage's opaque Rust Padding fields.
    let mut storage = [0; ADDRESS_BYTES / 8];
    let ptr = storage.as_mut_ptr();
    let length = match addr {
        SocketAddr::V4(addr) => {
            let native = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: addr.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(addr.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            unsafe {
                ptr.cast::<libc::sockaddr_in>().write(native);
            }
            size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(addr) => {
            let native = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: addr.port().to_be(),
                sin6_flowinfo: addr.flowinfo().to_be(),
                sin6_addr: libc::in6_addr {
                    s6_addr: addr.ip().octets(),
                },
                sin6_scope_id: addr.scope_id(),
            };
            unsafe {
                ptr.cast::<libc::sockaddr_in6>().write(native);
            }
            size_of::<libc::sockaddr_in6>()
        }
    };
    (storage, length as i32)
}

/// Accepts unaligned sockaddr data embedded inside a multishot recvmsg buffer.
pub fn decode(bytes: &[u8]) -> io::Result<SocketAddr> {
    if bytes.len() < size_of::<libc::sa_family_t>() {
        return Err(invalid("short socket address"));
    }
    let family = unsafe { bytes.as_ptr().cast::<libc::sa_family_t>().read_unaligned() } as i32;
    match family {
        libc::AF_INET if bytes.len() >= size_of::<libc::sockaddr_in>() => {
            let addr = unsafe { bytes.as_ptr().cast::<libc::sockaddr_in>().read_unaligned() };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(addr.sin_port),
            )))
        }
        libc::AF_INET6 if bytes.len() >= size_of::<libc::sockaddr_in6>() => {
            let addr = unsafe { bytes.as_ptr().cast::<libc::sockaddr_in6>().read_unaligned() };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(addr.sin6_addr.s6_addr),
                u16::from_be(addr.sin6_port),
                u32::from_be(addr.sin6_flowinfo),
                addr.sin6_scope_id,
            )))
        }
        _ => Err(invalid("unsupported or short socket address")),
    }
}

#[cfg(feature = "udp-gro")]
pub fn gro_segment(control: &[u8]) -> io::Result<Option<u16>> {
    let align = size_of::<usize>();
    let header = size_of::<CmsgHdr>();
    let mut offset = 0usize;
    while control.len().saturating_sub(offset) >= header {
        let cmsg = unsafe {
            control
                .as_ptr()
                .add(offset)
                .cast::<CmsgHdr>()
                .read_unaligned()
        };
        if cmsg.cmsg_len < header || cmsg.cmsg_len > control.len() - offset {
            return Err(invalid("malformed ancillary message"));
        }
        if cmsg.cmsg_level == libc::IPPROTO_UDP && cmsg.cmsg_type == UDP_GRO {
            // Linux UDP_GRO emits an int, unlike the uint16_t UDP_SEGMENT send cmsg.
            if cmsg.cmsg_len < header + size_of::<i32>() {
                return Err(invalid("short UDP_GRO control message"));
            }
            let value = unsafe {
                control
                    .as_ptr()
                    .add(offset + header)
                    .cast::<i32>()
                    .read_unaligned()
            };
            return if (1..=u16::MAX as i32).contains(&value) {
                Ok(Some(value as u16))
            } else {
                Err(invalid("invalid UDP_GRO segment size"))
            };
        }
        let step = (cmsg.cmsg_len + align - 1) & !(align - 1);
        offset = offset
            .checked_add(step)
            .ok_or_else(|| invalid("ancillary offset overflow"))?;
    }
    Ok(None)
}

#[cfg(feature = "udp-gso")]
pub fn put_gso(control: &mut [u64; CONTROL_BYTES / 8], segment: u16) -> usize {
    control.fill(0);
    let ptr = control.as_mut_ptr().cast::<u8>();
    let header = size_of::<CmsgHdr>();
    unsafe {
        ptr.cast::<CmsgHdr>().write(CmsgHdr {
            cmsg_len: header + 2,
            cmsg_level: libc::IPPROTO_UDP,
            cmsg_type: UDP_SEGMENT,
        });
        ptr.add(header).cast::<u16>().write(segment);
    }
    (header + 2 + size_of::<usize>() - 1) & !(size_of::<usize>() - 1)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(super) fn half_close(fd: RawFd) -> io::Result<()> {
    if unsafe { libc::shutdown(fd, libc::SHUT_WR) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    // A failed/in-progress connect or a peer-reset socket has no write half.
    if error.raw_os_error() == Some(libc::ENOTCONN) {
        Ok(())
    } else {
        Err(error)
    }
}

pub(super) fn abort_on_close(fd: RawFd) -> io::Result<()> {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    if unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&linger as *const libc::linger).cast(),
            size_of::<libc::linger>() as libc::socklen_t,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // Imported sockets can have aliases outside this runtime. AF_UNSPEC invokes
    // TCP's real disconnect and purges queued ownership even when close(fd)
    // would not be the last socket-file reference. No peer ACK is required.
    let address = libc::sockaddr {
        sa_family: libc::AF_UNSPEC as libc::sa_family_t,
        sa_data: [0; 14],
    };
    if unsafe { libc::connect(fd, &address, size_of::<libc::sockaddr>() as libc::socklen_t) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
