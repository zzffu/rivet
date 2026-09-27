use rivet::capability::KernelVersion;
use rivet::{Optimization as O, Policy, RuntimeConfig};
use std::io;

#[test]
fn explicitly_disabled_dependency_is_not_overridden() {
    let config = RuntimeConfig::single_thread()
        .with_policy(O::ZcTxFixed, Policy::Auto)
        .with_policy(O::ZcTx, Policy::Off);
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[cfg(not(all(target_os = "linux", feature = "zc-rx-large-chunks")))]
#[test]
fn uncompiled_auto_child_does_not_activate_conflicting_receive_mode() {
    let config = RuntimeConfig::single_thread()
        .with_policy(O::ZcRxNodev, Policy::Auto)
        .with_policy(O::ZcRxLargeChunks, Policy::Auto)
        .normalized()
        .unwrap();
    assert_eq!(config.policy(O::ZcRxNodev), Policy::Auto);
    assert_eq!(config.policy(O::ZcRxLargeChunks), Policy::Auto);
    assert_eq!(config.policy(O::ZcRx), Policy::Off);
}

#[cfg(all(target_os = "linux", feature = "zc-tx-fixed"))]
#[test]
fn required_child_strengthens_an_automatic_dependency() {
    let config = RuntimeConfig::single_thread()
        .enable(O::ZcTxFixed)
        .with_policy(O::ZcTx, Policy::Auto)
        .with_policy(O::RegisteredBuffers, Policy::Off)
        .normalized()
        .unwrap();
    assert_eq!(config.policy(O::ZcTx), Policy::RequireCapability);
    assert_eq!(config.policy(O::RegisteredBuffers), Policy::Off);
    assert_eq!(
        config.normalized().unwrap().optimizations,
        config.optimizations
    );
}

#[test]
fn incompatible_ring_requests_are_rejected_before_backend_creation() {
    for pair in [
        (O::SqPoll, O::ZcRx),
        (O::SqPoll, O::ZcRxNodev),
        (O::SqPoll, O::SqRewind),
        (O::ZcRx, O::ZcRxNodev),
    ] {
        let config = RuntimeConfig::single_thread()
            .with_policy(pair.0, Policy::Auto)
            .with_policy(pair.1, Policy::Auto);
        assert_eq!(
            config.normalized().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn kernel_release_parser_preserves_numeric_vendor_and_rc_versions() {
    for (release, expected) in [
        ("6.6.87-vendor.3", (6, 6, 87)),
        ("6.12.34+", (6, 12, 34)),
        ("6.18-rc1", (6, 18, 0)),
        ("6.18.0-rc4-custom", (6, 18, 0)),
        ("6.12", (6, 12, 0)),
        ("6.6-vendor", (6, 6, 0)),
    ] {
        assert_eq!(
            KernelVersion::parse(release).unwrap(),
            KernelVersion {
                major: expected.0,
                minor: expected.1,
                patch: expected.2,
            },
            "{release}"
        );
    }
}

#[test]
fn malformed_kernel_components_are_not_replaced_with_zero() {
    for release in [
        "",
        "not-a-release",
        "6",
        "6-rc1",
        "6..1",
        "6.18.",
        "6.18.invalid",
        "6.invalid.1",
        "-6.18.0",
        "65536.18.0",
        "6.65536.0",
        "6.18.65536",
    ] {
        assert_eq!(
            KernelVersion::parse(release).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
            "{release}"
        );
    }
}

#[test]
fn invalid_resource_limits_do_not_reach_os_allocation() {
    let mut config = RuntimeConfig::single_thread();
    config.limits.pool.block_size = config.limits.pool.bytes + 1;
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    config = RuntimeConfig::single_thread();
    config.workers = 2;
    config.limits.pool.bytes = usize::MAX;
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn unaddressable_receive_queue_is_a_configuration_error() {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_pending_receives = isize::MAX as usize;
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn unaddressable_accept_queue_is_a_configuration_error() {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_pending_accepts = isize::MAX as usize;
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn unaddressable_completion_queue_is_a_configuration_error() {
    let mut config = RuntimeConfig::single_thread();
    config.limits.completion_budget = isize::MAX as usize;
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn windows_udp_payload_estimate_uses_exact_allocations_and_the_shared_pool_minimum() {
    let mut limits = rivet::config::Limits {
        max_pending_receives: 3,
        pool: rivet::buffer::PoolConfig {
            bytes: 4096,
            block_size: 1024,
            max_leases: 8,
        },
        ..Default::default()
    };
    assert_eq!(limits.windows_udp_receive_bytes(256).unwrap(), 3072);
    // The allocator takes exact extents, not a rounded count of pool blocks.
    assert_eq!(limits.windows_udp_receive_bytes(1537).unwrap(), 4611);
    // An over-budget estimate remains available for offline configuration.
    limits.pool.bytes = 1024;
    limits.pool.max_leases = 1;
    limits.max_operations = 1;
    assert_eq!(limits.windows_udp_receive_bytes(1537).unwrap(), 4611);
}

#[test]
fn windows_udp_payload_estimate_rejects_invalid_sizes_and_overflow() {
    for (lanes, block, chunk) in [
        (0, 1024, 1024),
        (1, 0, 1024),
        (1, 1024, 0),
        (1, 1024, i32::MAX as usize + 1),
        (usize::MAX, 2, 1),
    ] {
        let limits = rivet::config::Limits {
            max_pending_receives: lanes,
            pool: rivet::buffer::PoolConfig {
                bytes: 4096,
                block_size: block,
                max_leases: 8,
            },
            ..Default::default()
        };
        assert_eq!(
            limits.windows_udp_receive_bytes(chunk).unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
            "lanes={lanes}, block={block}, chunk={chunk}",
        );
    }
}

#[test]
fn polling_durations_cannot_truncate_to_unbounded_spin() {
    let mut config = RuntimeConfig::single_thread();
    config.linux.sqpoll_idle = std::time::Duration::from_nanos(1);
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    config = RuntimeConfig::single_thread().with_policy(O::NapiBusyPoll, Policy::Auto);
    config.linux.napi_busy_poll = std::time::Duration::from_nanos(1);
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn shared_nodev_keeps_the_explicit_copied_receive_mode() {
    let config = RuntimeConfig::single_thread()
        .with_policy(O::ZcRxNodev, Policy::Auto)
        .with_policy(O::ZcRxShared, Policy::Auto)
        .normalized()
        .unwrap();
    assert!(config.requested(O::ZcRxNodev));
    assert!(!config.requested(O::ZcRx));
}

#[cfg(target_pointer_width = "64")]
#[test]
fn oversized_task_index_capacity_is_a_configuration_error() {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = u32::MAX as usize + 1;
    assert_eq!(
        config.normalized().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[cfg(not(all(target_os = "linux", feature = "zc-tx")))]
#[test]
fn strict_uncompiled_selection_has_structured_failure_identity() {
    let error = RuntimeConfig::single_thread()
        .enable(O::ZcTx)
        .normalized()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    let detail = error
        .get_ref()
        .unwrap()
        .downcast_ref::<rivet::capability::CapabilityError>()
        .unwrap();
    assert_eq!(detail.optimization, O::ZcTx);
}

#[cfg(not(all(target_os = "linux", feature = "zc-tx-fixed")))]
#[test]
fn strict_uncompiled_child_reports_the_explicit_request_not_its_dependency() {
    let error = RuntimeConfig::single_thread()
        .enable(O::ZcTxFixed)
        .normalized()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    let detail = error
        .get_ref()
        .unwrap()
        .downcast_ref::<rivet::capability::CapabilityError>()
        .unwrap();
    assert_eq!(detail.optimization, O::ZcTxFixed);

    let conflict = RuntimeConfig::single_thread()
        .enable(O::ZcTxFixed)
        .with_policy(O::ZcTx, Policy::Off)
        .normalized()
        .unwrap_err();
    assert_eq!(conflict.kind(), io::ErrorKind::InvalidInput);
}
