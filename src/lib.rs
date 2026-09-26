//! A native, ownership-based asynchronous TCP/UDP runtime.
//!
//! Optional optimizations are compiled with Cargo features and requested through
//! [`config::RuntimeConfig`]. Explicit requests are strict unless Auto is chosen.
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
compile_error!("rivet supports Linux 7.2.7+, Windows, and Android");

pub mod buffer;
pub mod capability;
pub mod config;
pub mod io;
pub mod net;
pub mod runtime;
pub mod signal;
pub mod socket;
pub mod sync;
pub mod time;

pub(crate) mod driver;

pub use buffer::{BufferPool, ReadBuf, SendBuf, SendPayload, WriteBuf};
pub use capability::{CapabilityReport, OptimizationState, ZcStats};
pub use config::{BlockingConfig, Optimization, Policy, RuntimeConfig};
pub use net::{TcpListener, TcpStream, UdpSocket};
pub use runtime::{Handle, Runtime, spawn, spawn_blocking, spawn_local};
pub use socket::{ImportError, SocketOptions};
