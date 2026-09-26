use rivet::{
    Optimization, Policy, Runtime, RuntimeConfig,
    capability::{CapabilityError, KernelVersion},
};
use std::io;

pub fn runtime(config: RuntimeConfig) -> Option<Runtime> {
    let config = config.normalized().unwrap();
    match Runtime::new(config.clone()) {
        Ok(runtime) => {
            for report in runtime.capabilities() {
                for (&optimization, &policy) in &config.optimizations {
                    match policy {
                        Policy::RequireCapability => assert!(
                            report.enabled(optimization),
                            "required {optimization} was not enabled"
                        ),
                        Policy::Off => assert!(
                            !report.enabled(optimization),
                            "disabled {optimization} was enabled"
                        ),
                        Policy::Auto => {}
                    }
                }
            }
            Some(runtime)
        }
        Err(error) => {
            assert_eq!(error.kind(), io::ErrorKind::Unsupported, "{error}");
            let missing = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<CapabilityError>())
                .unwrap_or_else(|| panic!("unexpected native initialization failure: {error}"));
            assert_eq!(
                config.policy(missing.optimization),
                Policy::RequireCapability
            );
            // Only known cross-version gaps may skip a strict-path row. Native
            // failures on a capable guest must still fail the regression.
            let (major, minor, patch) = match missing.optimization {
                Optimization::DirectDescriptors => (6, 8, 0),
                Optimization::RegisteredWait => (6, 13, 0),
                Optimization::MixedCqe => (6, 18, 0),
                Optimization::SqRewind => (7, 0, 0),
                Optimization::RegisteredBuffers => (7, 2, 0),
                Optimization::IncrementalBuffers => (7, 2, 7),
                _ => panic!("unexpected missing native capability: {error}"),
            };
            let minimum = KernelVersion {
                major,
                minor,
                patch,
            };
            let mut automatic = config;
            for policy in automatic.optimizations.values_mut() {
                if *policy == Policy::RequireCapability {
                    *policy = Policy::Auto;
                }
            }
            let fallback = Runtime::new(automatic)
                .unwrap_or_else(|error| panic!("Auto fallback failed to initialize: {error}"));
            for report in fallback.capabilities() {
                let kernel = report
                    .kernel
                    .expect("missing native kernel evidence for skip");
                assert!(
                    kernel < minimum,
                    "unexpected capability failure on {kernel}: {error}"
                );
                let state = report.state(missing.optimization).unwrap();
                assert_eq!(state.policy, Policy::Auto);
                assert!(state.compiled);
                assert!(!state.supported);
                assert!(!state.enabled);
                assert_eq!(state.reason.as_deref(), Some(missing.reason.as_str()));
                eprintln!(
                    "SKIP strict native path: {} unavailable on Linux {kernel}: {}",
                    missing.optimization, missing.reason
                );
            }
            None
        }
    }
}
