//! Runs real generic-runtime and TCP scenarios; not a throughput benchmark.
#[path = "support/services.rs"]
mod services;
mod support;

fn main() -> std::io::Result<()> {
    let detail = services::run(support::configuration()?)?;
    println!("PASS: {detail}; runtime workers and blocking work joined");
    Ok(())
}
