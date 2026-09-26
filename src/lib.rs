//! A native, ownership-based asynchronous TCP/UDP runtime.
//!
//! Optional optimizations are compiled with Cargo features and requested through
//! [`config::RuntimeConfig`]. Explicit requests are strict unless Auto is chosen.
//!
//! # Compatibility
//!
//! The public interface after the backend report-management cutover is the
//! compatibility baseline for `0.1.x`. Compatibility includes documented buffer
//! ownership, send progress and cancellation, worker affinity, task Drop/detach,
//! lazy flush/write shutdown, and timer binding/reset/error behavior, not just
//! method signatures.
//!
//! Compatible releases do not add required methods or stronger bounds to open
//! traits. Independent capabilities use independent traits. The concrete
//! dependency types re-exported by [`sync`] are also part of this contract.
//! Public configuration/result fields and enum matching rules remain unchanged;
//! additions that invalidate downstream construction or matching are breaking.
//! Intentional breaks require a minor version bump before 1.0, or a major
//! version bump from 1.0 onward, with migration notes.
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
