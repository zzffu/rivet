//! Native socket ownership and pre-connect host integration.

use std::{fmt, io, sync::Arc};

#[cfg(unix)]
pub type OwnedSocket = std::os::fd::OwnedFd;
#[cfg(unix)]
pub type BorrowedSocket<'a> = std::os::fd::BorrowedFd<'a>;
#[cfg(unix)]
pub type RawSocket = std::os::fd::RawFd;
#[cfg(windows)]
pub type OwnedSocket = std::os::windows::io::OwnedSocket;
#[cfg(windows)]
pub type BorrowedSocket<'a> = std::os::windows::io::BorrowedSocket<'a>;
#[cfg(windows)]
pub type RawSocket = std::os::windows::io::RawSocket;

/// Host-owned setup, for example Android VpnService.protect.
///
/// The handle is borrowed. Implementations must not close it, retain ownership,
/// perform network I/O on it, or change it after returning. Host-side effects
/// cannot be rolled back by Rivet if a later setup step fails.
///
/// With [`TcpStream::connect_from`](crate::net::TcpStream::connect_from), hooks
/// run before Rivet binds the explicit local endpoint. They must not bind or
/// connect the socket themselves; Rivet owns those steps.
pub trait SocketHook: Send + Sync + 'static {
    fn configure(&self, socket: BorrowedSocket<'_>) -> io::Result<()>;
}

impl<F> SocketHook for F
where
    F: for<'a> Fn(BorrowedSocket<'a>) -> io::Result<()> + Send + Sync + 'static,
{
    fn configure(&self, socket: BorrowedSocket<'_>) -> io::Result<()> {
        self(socket)
    }
}

#[derive(Clone)]
pub struct SocketOptions {
    pub nodelay: bool,
    pub keepalive: bool,
    pub reuse_address: bool,
    pub reuse_port: bool,
    pub only_v6: Option<bool>,
    pub receive_chunk: usize,
    pub receive_buffer_bytes: Option<usize>,
    pub send_buffer_bytes: Option<usize>,
    pub backlog: i32,
    pub android_network: Option<u64>,
    pub hook: Option<Arc<dyn SocketHook>>,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            nodelay: true,
            keepalive: false,
            reuse_address: true,
            reuse_port: false,
            only_v6: None,
            receive_chunk: 16 * 1024,
            receive_buffer_bytes: None,
            send_buffer_bytes: None,
            backlog: 1024,
            android_network: None,
            hook: None,
        }
    }
}

impl SocketOptions {
    pub fn udp() -> Self {
        Self {
            receive_chunk: 65536,
            nodelay: false,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.receive_chunk == 0 || self.receive_chunk > i32::MAX as usize || self.backlog <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid receive size or listen backlog",
            ));
        }
        if self.receive_buffer_bytes == Some(0) || self.send_buffer_bytes == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket buffer sizes must be nonzero",
            ));
        }
        if self.android_network == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Android Network handle must be nonzero",
            ));
        }
        #[cfg(not(target_os = "android"))]
        if self.android_network.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Android Network binding is only available on Android",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for SocketOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketOptions")
            .field("nodelay", &self.nodelay)
            .field("keepalive", &self.keepalive)
            .field("reuse_address", &self.reuse_address)
            .field("reuse_port", &self.reuse_port)
            .field("only_v6", &self.only_v6)
            .field("receive_chunk", &self.receive_chunk)
            .field("receive_buffer_bytes", &self.receive_buffer_bytes)
            .field("send_buffer_bytes", &self.send_buffer_bytes)
            .field("backlog", &self.backlog)
            .field("android_network", &self.android_network)
            .field("hook", &self.hook.as_ref().map(|_| "configured"))
            .finish()
    }
}

#[derive(Debug)]
pub struct ImportError {
    pub error: io::Error,
    pub socket: OwnedSocket,
}

impl ImportError {
    pub fn into_parts(self) -> (io::Error, OwnedSocket) {
        (self.error, self.socket)
    }
}
impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "socket import failed: {}", self.error)
    }
}
impl std::error::Error for ImportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Validate before changing the mode of an imported TCP socket, and after a
/// setup hook. Positive linger can block Unix close or fail Windows close on
/// a nonblocking socket; neither is compatible with owner-thread execution.
pub(crate) fn reject_blocking_linger(socket: &socket2::Socket) -> io::Result<()> {
    if socket.linger()?.is_some_and(|duration| !duration.is_zero()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "positive SO_LINGER is incompatible with nonblocking runtime ownership",
        ));
    }
    Ok(())
}
