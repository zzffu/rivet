//! Cold Winsock setup and the native ABI boundaries used by the RIO driver.

use crate::{driver::SocketKind, socket::SocketOptions};
use socket2::{SockAddr, Socket, Type};
use std::{
    io,
    mem::size_of,
    net::SocketAddr,
    os::windows::io::{AsRawSocket, AsSocket, FromRawSocket},
    ptr,
};
use windows_sys::{Win32::Networking::WinSock::*, core::GUID};

pub(super) fn wsa_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn exhausted(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}

pub(super) fn unsupported(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}

pub(super) fn unicast(address: SocketAddr) -> io::Result<()> {
    let unsupported_address = match address.ip() {
        std::net::IpAddr::V4(ip) => ip.is_multicast() || ip.is_broadcast(),
        std::net::IpAddr::V6(ip) => {
            ip.is_multicast()
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|ip| ip.is_multicast() || ip.is_broadcast())
        }
    };
    if unsupported_address {
        Err(unsupported(
            "multicast and broadcast sockets are outside the supported scope",
        ))
    } else {
        Ok(())
    }
}

pub(super) struct Winsock;

impl Winsock {
    pub fn new() -> io::Result<Self> {
        let mut data = WSADATA::default();
        let error = unsafe { WSAStartup(0x0202, &mut data) };
        if error != 0 {
            return Err(io::Error::from_raw_os_error(error));
        }
        let guard = Self;
        if data.wVersion != 0x0202 {
            return Err(unsupported("Winsock 2.2 is required"));
        }
        Ok(guard)
    }
}

impl Drop for Winsock {
    fn drop(&mut self) {
        unsafe {
            WSACleanup();
        }
    }
}

pub(super) fn new_socket(addr: SocketAddr, kind: SocketKind) -> io::Result<Socket> {
    unicast(addr)?;
    let (ty, protocol) = match kind {
        SocketKind::Udp => (SOCK_DGRAM, IPPROTO_UDP),
        _ => (SOCK_STREAM, IPPROTO_TCP),
    };
    let raw = unsafe {
        WSASocketW(
            if addr.is_ipv4() {
                AF_INET as i32
            } else {
                AF_INET6 as i32
            },
            ty,
            protocol,
            ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if raw == INVALID_SOCKET {
        return Err(wsa_error());
    }
    // Registered-I/O sockets reject FIONBIO with WSAEOPNOTSUPP. RIO and the
    // overlapped ConnectEx/AcceptEx paths already submit without blocking.
    Ok(unsafe { Socket::from_raw_socket(raw as _) })
}

pub(super) fn configure(
    socket: &Socket,
    kind: SocketKind,
    options: &SocketOptions,
    new: bool,
) -> io::Result<()> {
    options.validate()?;
    if options.reuse_port {
        return Err(unsupported(
            "SO_REUSEPORT is not available on the Windows RIO backend",
        ));
    }
    if kind != SocketKind::Udp {
        crate::socket::reject_blocking_linger(socket)?;
    }
    if new {
        socket.set_reuse_address(options.reuse_address)?;
        if let Some(only_v6) = options.only_v6 {
            socket.set_only_v6(only_v6)?;
        }
    } else if let Some(only_v6) = options.only_v6
        && socket.only_v6()? != only_v6
    {
        return Err(invalid(
            "imported socket IPV6_V6ONLY does not match SocketOptions",
        ));
    }
    if kind == SocketKind::Udp {
        socket.set_broadcast(false)?;
    } else {
        socket.set_tcp_nodelay(options.nodelay)?;
        socket.set_keepalive(options.keepalive)?;
    }
    if let Some(bytes) = options.receive_buffer_bytes {
        socket.set_recv_buffer_size(bytes)?;
    }
    if let Some(bytes) = options.send_buffer_bytes {
        socket.set_send_buffer_size(bytes)?;
    }
    if let Some(hook) = &options.hook {
        hook.configure(socket.as_socket())?;
    }
    if kind != SocketKind::Udp {
        crate::socket::reject_blocking_linger(socket)?;
    }
    Ok(())
}

pub(super) fn address(addr: SockAddr) -> io::Result<SocketAddr> {
    addr.as_socket()
        .ok_or_else(|| unsupported("only IPv4 and IPv6 sockets are supported"))
}

pub(super) fn validate_import(
    socket: &Socket,
    kind: SocketKind,
) -> io::Result<(SocketAddr, Option<SocketAddr>)> {
    let expected = if kind == SocketKind::Udp {
        Type::DGRAM
    } else {
        Type::STREAM
    };
    if socket.r#type()? != expected {
        return Err(invalid("imported socket has the wrong socket type"));
    }
    if kind != SocketKind::Udp {
        let mut listening = 0i32;
        let mut length = size_of::<i32>() as i32;
        if unsafe {
            getsockopt(
                socket.as_raw_socket() as _,
                SOL_SOCKET,
                SO_ACCEPTCONN,
                (&mut listening as *mut i32).cast(),
                &mut length,
            )
        } == SOCKET_ERROR
        {
            return Err(wsa_error());
        }
        if (kind == SocketKind::TcpListener) != (listening != 0) {
            return Err(invalid(
                "imported socket listening state does not match its requested kind",
            ));
        }
    }
    let local = address(socket.local_addr()?)?;
    let peer = match socket.peer_addr() {
        Ok(addr) => Some(address(addr)?),
        Err(error)
            if error.raw_os_error() == Some(WSAENOTCONN) && kind != SocketKind::TcpStream =>
        {
            None
        }
        Err(error) => return Err(error),
    };
    if kind == SocketKind::Udp && local.port() == 0 {
        return Err(invalid("an imported UDP socket must already be bound"));
    }
    unicast(local)?;
    if let Some(peer) = peer {
        unicast(peer)?;
    }
    Ok((local, peer))
}

pub(super) unsafe fn extension<T: Copy + Default>(socket: SOCKET, guid: &GUID) -> io::Result<T> {
    let mut extension = T::default();
    let mut bytes = 0;
    let result = unsafe {
        WSAIoctl(
            socket,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            (guid as *const GUID).cast(),
            size_of::<GUID>() as u32,
            (&mut extension as *mut T).cast(),
            size_of::<T>() as u32,
            &mut bytes,
            ptr::null_mut(),
            None,
        )
    };
    if result == SOCKET_ERROR {
        return Err(wsa_error());
    }
    if bytes != size_of::<T>() as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated Winsock extension pointer",
        ));
    }
    Ok(extension)
}

pub(super) fn rio_table(socket: SOCKET) -> io::Result<RIO_EXTENSION_FUNCTION_TABLE> {
    let mut table = RIO_EXTENSION_FUNCTION_TABLE {
        cbSize: size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32,
        ..Default::default()
    };
    let mut bytes = 0;
    let result = unsafe {
        WSAIoctl(
            socket,
            SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
            (&WSAID_MULTIPLE_RIO as *const GUID).cast(),
            size_of::<GUID>() as u32,
            (&mut table as *mut RIO_EXTENSION_FUNCTION_TABLE).cast(),
            table.cbSize,
            &mut bytes,
            ptr::null_mut(),
            None,
        )
    };
    if result == SOCKET_ERROR {
        return Err(wsa_error());
    }
    if bytes < size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32
        || table.RIOReceive.is_none()
        || table.RIOReceiveEx.is_none()
        || table.RIOSend.is_none()
        || table.RIOSendEx.is_none()
        || table.RIOCloseCompletionQueue.is_none()
        || table.RIOCreateCompletionQueue.is_none()
        || table.RIOCreateRequestQueue.is_none()
        || table.RIODequeueCompletion.is_none()
        || table.RIODeregisterBuffer.is_none()
        || table.RIONotify.is_none()
        || table.RIORegisterBuffer.is_none()
    {
        return Err(unsupported(
            "Winsock provider did not expose the complete RIO extension table",
        ));
    }
    Ok(table)
}

pub(super) fn set_context(socket: SOCKET, option: i32, value: Option<SOCKET>) -> io::Result<()> {
    let result = match value {
        Some(value) => unsafe {
            setsockopt(
                socket,
                SOL_SOCKET,
                option,
                (&value as *const SOCKET).cast(),
                size_of::<SOCKET>() as i32,
            )
        },
        None => unsafe { setsockopt(socket, SOL_SOCKET, option, ptr::null(), 0) },
    };
    if result == SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

pub(super) fn flush(socket: SOCKET) -> io::Result<()> {
    let mut bytes = 0;
    let result = unsafe {
        WSAIoctl(
            socket,
            SIO_FLUSH,
            ptr::null(),
            0,
            ptr::null_mut(),
            0,
            &mut bytes,
            ptr::null_mut(),
            None,
        )
    };
    if result == SOCKET_ERROR {
        Err(wsa_error())
    } else {
        Ok(())
    }
}

pub(super) fn write_address(storage: &mut SOCKADDR_STORAGE, address: SocketAddr) {
    let address = SockAddr::from(address);
    *storage = SOCKADDR_STORAGE::default();
    unsafe {
        ptr::copy_nonoverlapping(
            address.as_ptr().cast::<u8>(),
            (storage as *mut SOCKADDR_STORAGE).cast::<u8>(),
            address.len() as usize,
        );
    }
}

pub(super) fn read_address(storage: &SOCKADDR_STORAGE) -> io::Result<SocketAddr> {
    let length = match storage.ss_family {
        AF_INET => size_of::<SOCKADDR_IN>(),
        AF_INET6 => size_of::<SOCKADDR_IN6>(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RIO returned a non-IP source address",
            ));
        }
    };
    // SockAddr uses the native Windows SOCKADDR_STORAGE layout.
    let address = unsafe {
        SockAddr::new(
            ptr::read((storage as *const SOCKADDR_STORAGE).cast()),
            length as _,
        )
    };
    self::address(address)
}
