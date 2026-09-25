//! Startup capability evidence and optional data-path observations.

use crate::config::{Optimization, Policy, RuntimeConfig};
use std::{fmt, io};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct KernelVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl KernelVersion {
    pub const MINIMUM_LINUX: Self = Self {
        major: 7,
        minor: 2,
        patch: 7,
    };

    pub fn parse(release: &str) -> io::Result<Self> {
        if release.contains("-rc") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "release-candidate kernels are outside the supported baseline",
            ));
        }
        let mut parts = release.split(['.', '-', '+']);
        let mut next = || -> io::Result<u16> {
            parts
                .next()
                .and_then(|part| part.parse().ok())
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid kernel release"))
        };
        Ok(Self {
            major: next()?,
            minor: next()?,
            patch: next()?,
        })
    }

    pub fn require_supported(self) -> io::Result<()> {
        if self < Self::MINIMUM_LINUX {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Linux {} is below required {}", self, Self::MINIMUM_LINUX),
            ))
        } else {
            Ok(())
        }
    }
}

impl fmt::Display for KernelVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReceiveMode {
    #[default]
    Copied,
    HardwareZeroCopy,
    /// Explicit kernel ZCRX NODEV validation, never hardware zero-copy evidence.
    NodevCopied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizationState {
    pub optimization: Optimization,
    pub policy: Policy,
    pub compiled: bool,
    pub supported: bool,
    pub enabled: bool,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CapabilityReport {
    pub backend: &'static str,
    pub worker: usize,
    pub kernel: Option<KernelVersion>,
    pub receive_mode: ReceiveMode,
    states: Vec<OptimizationState>,
    active: u64,
}

impl CapabilityReport {
    pub fn new(backend: &'static str, worker: usize) -> Self {
        Self {
            backend,
            worker,
            kernel: None,
            receive_mode: ReceiveMode::Copied,
            states: Vec::with_capacity(Optimization::ALL.len()),
            active: 0,
        }
    }

    pub fn states(&self) -> &[OptimizationState] {
        &self.states
    }
    pub fn state(&self, optimization: Optimization) -> Option<&OptimizationState> {
        self.states
            .iter()
            .find(|state| state.optimization == optimization)
    }
    pub fn enabled(&self, optimization: Optimization) -> bool {
        self.active & optimization.bit() != 0
    }
    pub fn enabled_mask(&self) -> u64 {
        self.active
    }

    /// Call after the backend has validated and, where applicable, initialized
    /// the real implementation. Opcode existence alone does not prove a NIC path.
    pub fn decide(
        &mut self,
        optimization: Optimization,
        policy: Policy,
        support: Result<(), String>,
    ) -> io::Result<bool> {
        let compiled = optimization.compiled();
        let (supported, reason) = match support {
            Ok(()) => (true, None),
            Err(reason) => (false, Some(reason)),
        };
        let unavailable = if !compiled {
            Some(format!(
                "implementation is not compiled for {}; check target support and Cargo feature {}",
                std::env::consts::OS,
                optimization.name()
            ))
        } else if !supported {
            reason.clone()
        } else {
            None
        };
        let enabled = policy != Policy::Off && unavailable.is_none();
        let state_reason = if policy == Policy::Off {
            Some("not requested".to_owned())
        } else {
            unavailable.clone()
        };
        let state = OptimizationState {
            optimization,
            policy,
            compiled,
            supported,
            enabled,
            reason: state_reason,
        };
        if let Some(existing) = self
            .states
            .iter_mut()
            .find(|s| s.optimization == optimization)
        {
            *existing = state;
        } else {
            self.states.push(state);
        }
        if enabled {
            self.active |= optimization.bit();
        } else {
            self.active &= !optimization.bit();
        }
        if policy == Policy::RequireCapability
            && let Some(reason) = unavailable
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                CapabilityError {
                    optimization,
                    reason,
                },
            ));
        }
        Ok(enabled)
    }

    /// Ensure that a requested optimization cannot disappear from the report.
    pub fn finish(&mut self, config: &RuntimeConfig) -> io::Result<()> {
        for &optimization in Optimization::ALL {
            if self.state(optimization).is_none() {
                self.decide(
                    optimization,
                    config.policy(optimization),
                    Err(format!("not available in {}", self.backend)),
                )?;
            }
        }
        self.states.sort_by_key(|state| state.optimization);
        Ok(())
    }
}

#[derive(Debug)]
pub struct CapabilityError {
    pub optimization: Optimization,
    pub reason: String,
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "required optimization {} is unavailable: {}",
            self.optimization, self.reason
        )
    }
}
impl std::error::Error for CapabilityError {}

/// Observed events, not claims derived from an enabled feature bit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ZcStats {
    pub tx_requests: u64,
    pub tx_notifications: u64,
    pub tx_copied_notifications: u64,
    /// Accepted bytes associated with copy-marked notifications, not an exact
    /// measurement of bytes copied inside a partially zero-copy operation.
    pub tx_copy_marked_bytes: u64,
    /// Data completions and leased bytes observed by this driver.
    pub rx_completions: u64,
    pub rx_bytes: u64,
    /// Cumulative kernel-instance counters; imported views share these values.
    pub rx_copy_events: u64,
    pub rx_copied_bytes: u64,
    /// Instance-wide observed ALLOC_FAIL notifications, not allocation attempts.
    pub rx_allocation_failures: u64,
}

impl ZcStats {
    /// Combine disjoint observation domains only. Imported views of one ZCRX
    /// instance expose the same RX copy/allocation counters and must not be summed.
    pub fn accumulate(&mut self, other: Self) {
        self.tx_requests = self.tx_requests.saturating_add(other.tx_requests);
        self.tx_notifications = self.tx_notifications.saturating_add(other.tx_notifications);
        self.tx_copied_notifications = self
            .tx_copied_notifications
            .saturating_add(other.tx_copied_notifications);
        self.tx_copy_marked_bytes = self
            .tx_copy_marked_bytes
            .saturating_add(other.tx_copy_marked_bytes);
        self.rx_completions = self.rx_completions.saturating_add(other.rx_completions);
        self.rx_bytes = self.rx_bytes.saturating_add(other.rx_bytes);
        self.rx_copy_events = self.rx_copy_events.saturating_add(other.rx_copy_events);
        self.rx_copied_bytes = self.rx_copied_bytes.saturating_add(other.rx_copied_bytes);
        self.rx_allocation_failures = self
            .rx_allocation_failures
            .saturating_add(other.rx_allocation_failures);
    }
}
