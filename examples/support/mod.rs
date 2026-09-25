use rivet::{Optimization, Policy, RuntimeConfig};
use std::io;

pub fn error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

pub fn configuration() -> io::Result<RuntimeConfig> {
    let mut config = RuntimeConfig {
        workers: 2,
        ..RuntimeConfig::default()
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--workers" => {
                config.workers = args
                    .next()
                    .ok_or_else(|| error("--workers needs a count"))?
                    .parse()
                    .map_err(|_| error("invalid worker count"))?;
            }
            "--enable" | "--auto" => {
                let name = args
                    .next()
                    .ok_or_else(|| error("optimization name required"))?;
                let feature = Optimization::ALL
                    .iter()
                    .copied()
                    .find(|feature| feature.name() == name)
                    .ok_or_else(|| error(format!("unknown optimization {name}")))?;
                let policy = if arg == "--enable" {
                    Policy::RequireCapability
                } else {
                    Policy::Auto
                };
                config = config.with_policy(feature, policy);
            }
            _ => {
                return Err(error(
                    "usage: <example> [--workers N] [--enable FEATURE | --auto FEATURE]...",
                ));
            }
        }
    }
    Ok(config)
}
