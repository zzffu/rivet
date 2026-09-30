#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
use super::super::SocketKind;
#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
use super::socket;
use std::{io, mem};
#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::AsRawFd,
};

pub(super) const UDP_SEGMENT: i32 = 103;
pub(super) const UDP_GRO: i32 = 104;

#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
#[derive(Clone, Copy)]
pub(super) enum Offload {
    #[cfg(feature = "udp-gso")]
    Gso,
    #[cfg(feature = "udp-gro")]
    Gro,
}

// cmsghdr must be naturally aligned, including on Android's 16 KiB-page builds.
// Ancillary alignment is unrelated to the virtual-memory page size.
pub(super) struct Control(pub [usize; 8]);

impl Control {
    pub fn new() -> Self {
        Self([0; 8])
    }
    pub fn as_mut_ptr(&mut self) -> *mut libc::c_void {
        self.0.as_mut_ptr().cast()
    }
    pub fn capacity(&self) -> usize {
        mem::size_of_val(&self.0)
    }

    #[cfg(feature = "udp-gso")]
    pub fn segment(&mut self, size: u16, message: &mut libc::msghdr) {
        self.0.fill(0);
        let header = self.0.as_mut_ptr().cast::<libc::cmsghdr>();
        unsafe {
            (*header).cmsg_level = libc::IPPROTO_UDP;
            (*header).cmsg_type = UDP_SEGMENT;
            (*header).cmsg_len = libc::CMSG_LEN(mem::size_of::<u16>() as _) as _;
            libc::CMSG_DATA(header).cast::<u16>().write_unaligned(size);
            message.msg_control = self.as_mut_ptr();
            message.msg_controllen = libc::CMSG_SPACE(mem::size_of::<u16>() as _) as _;
        }
    }

    pub fn gro_segment_size(&self, used: usize) -> io::Result<Option<u16>> {
        if used > self.capacity() {
            return Err(invalid_control());
        }
        let header_length = aligned(mem::size_of::<libc::cmsghdr>());
        let mut offset = 0;
        while offset + header_length <= used {
            let header = unsafe {
                &*self
                    .0
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<libc::cmsghdr>()
            };
            let length = header.cmsg_len;
            if length < header_length || length > used - offset {
                return Err(invalid_control());
            }
            if header.cmsg_level == libc::IPPROTO_UDP && header.cmsg_type == UDP_GRO {
                if length - header_length < mem::size_of::<i32>() {
                    return Err(invalid_control());
                }
                let value = unsafe {
                    self.0
                        .as_ptr()
                        .cast::<u8>()
                        .add(offset + header_length)
                        .cast::<i32>()
                        .read_unaligned()
                };
                return u16::try_from(value)
                    .ok()
                    .filter(|size| *size != 0)
                    .map(Some)
                    .ok_or_else(invalid_control);
            }
            offset += aligned(length);
        }
        Ok(None)
    }
}

fn aligned(value: usize) -> usize {
    let alignment = mem::size_of::<usize>();
    (value + alignment - 1) & !(alignment - 1)
}

fn invalid_control() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid UDP ancillary data")
}

#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
pub(super) fn probe(offload: Offload) -> Result<(), String> {
    let name = match offload {
        #[cfg(feature = "udp-gso")]
        Offload::Gso => "GSO",
        #[cfg(feature = "udp-gro")]
        Offload::Gro => "GRO",
    };
    // Exercise both advertised address families, not only an option number.
    for address in [
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
    ] {
        probe_family(address, offload)
            .map_err(|error| format!("Android UDP {name} loopback probe failed: {error}"))?;
    }
    Ok(())
}

#[cfg(any(feature = "udp-gso", feature = "udp-gro"))]
fn probe_family(address: SocketAddr, offload: Offload) -> io::Result<()> {
    let receiver = socket::create(address, SocketKind::Udp)?;
    let sender = socket::create(address, SocketKind::Udp)?;
    socket::bind(receiver.as_raw_fd(), address)?;
    #[cfg(feature = "udp-gro")]
    if matches!(offload, Offload::Gro) {
        socket::set_int(receiver.as_raw_fd(), libc::IPPROTO_UDP, UDP_GRO, 1)?;
    }
    // GRO needs segmented input to prove coalescing, even in a GRO-only build.
    // This diagnostic sender does not enable the application's GSO send path.
    socket::set_int(sender.as_raw_fd(), libc::IPPROTO_UDP, UDP_SEGMENT, 4)?;
    let target = socket2::SockAddr::from(socket::local_addr(receiver.as_raw_fd())?);
    let payload = *b"abcdEFGH";
    let sent = unsafe {
        libc::sendto(
            sender.as_raw_fd(),
            payload.as_ptr().cast(),
            payload.len(),
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            target.as_ptr().cast(),
            target.len(),
        )
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    if sent as usize != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short probe datagram",
        ));
    }
    let mut assembled = [0u8; 8];
    let mut total = 0;
    for _ in 0..2 {
        let mut wait = libc::pollfd {
            fd: receiver.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut wait, 1, 100) };
        if ready < 0 {
            return Err(io::Error::last_os_error());
        }
        if ready == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "offload probe did not arrive",
            ));
        }
        let mut bytes = [0u8; 16];
        let mut iovec = libc::iovec {
            iov_base: bytes.as_mut_ptr().cast(),
            iov_len: bytes.len(),
        };
        let mut control = Control::new();
        let mut message: libc::msghdr = unsafe { mem::zeroed() };
        message.msg_iov = &mut iovec;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr();
        message.msg_controllen = control.capacity();
        let received = unsafe {
            libc::recvmsg(
                receiver.as_raw_fd(),
                &mut message,
                libc::MSG_DONTWAIT | libc::MSG_TRUNC,
            )
        };
        if received < 0 {
            return Err(io::Error::last_os_error());
        }
        let length = received as usize;
        let valid = match offload {
            #[cfg(feature = "udp-gso")]
            Offload::Gso => length == 4 && message.msg_controllen == 0,
            #[cfg(feature = "udp-gro")]
            Offload::Gro => {
                length == 8 && control.gro_segment_size(message.msg_controllen)? == Some(4)
            }
        };
        if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0
            || !valid
            || total + length > assembled.len()
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel did not preserve the requested UDP offload semantics",
            ));
        }
        assembled[total..total + length].copy_from_slice(&bytes[..length]);
        total += length;
        if total == payload.len() {
            break;
        }
    }
    if assembled != payload || total != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "UDP offload probe data mismatch",
        ));
    }
    Ok(())
}
