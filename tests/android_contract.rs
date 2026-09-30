#![cfg(target_os = "android")]

// The ordinary-App JNI harness exercises these same consumer-visible contracts.
// Running this test binary under adb shell is useful regression coverage, but is
// not evidence that Android application sandbox/network integration succeeded.
#[path = "../android-smoke/native/src/smoke.rs"]
#[allow(dead_code)]
mod smoke;

use std::{
    future::Future,
    io,
    net::{Ipv4Addr, Ipv6Addr},
    time::Duration,
};

fn run(future: impl Future<Output = io::Result<String>>) {
    let mut runtime = rivet::Runtime::new(smoke::config()).expect("Android epoll runtime");
    runtime
        .block_on(rivet::time::timeout(Duration::from_secs(8), future))
        .expect("Android contract deadline")
        .expect("Android network contract");
}

#[test]
fn ipv4_tcp_full_duplex_and_half_close_preserve_bytes() {
    run(smoke::tcp_full_duplex(Ipv4Addr::LOCALHOST.into()));
}

#[test]
fn ipv6_tcp_full_duplex_and_half_close_preserve_bytes() {
    run(smoke::tcp_full_duplex(Ipv6Addr::LOCALHOST.into()));
}

#[test]
fn ipv4_udp_preserves_empty_datagrams_source_and_truncation() {
    run(smoke::udp_datagrams(Ipv4Addr::LOCALHOST.into()));
}

#[test]
fn ipv6_udp_preserves_empty_datagrams_source_and_truncation() {
    run(smoke::udp_datagrams(Ipv6Addr::LOCALHOST.into()));
}

#[test]
fn cancel_waiter_does_not_discard_queued_tcp_bytes() {
    run(smoke::queued_tcp_cancellation());
}

#[test]
fn one_slot_credits_resume_known_ready_udp_and_accept_edges() {
    run(smoke::edge_credit_rearm());
}

#[test]
fn finite_pool_pressure_resumes_receive_after_recycling() {
    smoke::pool_pressure_recovery().expect("pool-pressure recovery without a new edge");
}

#[test]
fn cross_thread_waker_resumes_an_idle_owner() {
    run(smoke::cross_thread_wake());
}

#[test]
fn native_import_failure_returns_the_original_usable_socket() {
    run(smoke::import_ownership());
}

#[test]
fn inherited_positive_tcp_linger_returns_unchanged_owned_fd() {
    run(smoke::inherited_tcp_linger());
}

#[test]
fn setup_hooks_cannot_introduce_blocking_tcp_linger() {
    run(smoke::hook_tcp_linger());
}

#[test]
fn connected_imports_reject_network_binding_before_any_mutation() {
    // No active Network is needed: rejection must precede libandroid binding.
    run(smoke::connected_import_network_rejection(u64::MAX));
}

#[test]
fn network_and_host_protection_failures_stop_establishment() {
    run(smoke::network_and_protection_errors());
}

#[test]
fn receive_lease_and_alias_outlive_runtime_shutdown() {
    smoke::lease_outlives_runtime().expect("receive lease lifetime");
}

#[test]
fn linux_only_capabilities_are_strict_or_explicitly_auto_unavailable() {
    smoke::capability_policy().expect("Android capability policy");
}

#[test]
fn udp_offload_preserves_boundaries_when_the_real_probe_succeeds() {
    smoke::udp_offload().expect("selected Android UDP offload semantics");
}

#[test]
fn imported_udp_gro_preserves_prequeued_boundaries_with_off_policy() {
    if smoke::imported_udp_gro()
        .expect("inherited Android GRO semantics with Off policy")
        .is_none()
    {
        eprintln!("native UDP_GRO or UDP_SEGMENT option is unsupported; inherited GRO skipped");
    }
}
