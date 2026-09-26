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
        .normalized()
        .unwrap();
    assert_eq!(config.policy(O::ZcTx), Policy::RequireCapability);
    assert_eq!(
        config.policy(O::RegisteredBuffers),
        Policy::RequireCapability
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
fn kernel_release_guard_handles_vendor_versions_and_rejects_rc() {
    let minimum = KernelVersion::parse("7.2.7-vendor.3").unwrap();
    assert_eq!(minimum, KernelVersion::MINIMUM_LINUX);
    minimum.require_supported().unwrap();
    assert_eq!(KernelVersion::parse("7.2.7+").unwrap(), minimum);
    assert_eq!(
        KernelVersion::parse("7.2.6")
            .unwrap()
            .require_supported()
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        KernelVersion::parse("7.3-rc4").unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        KernelVersion::parse("not-a-release").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
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
