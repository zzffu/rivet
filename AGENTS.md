# Repository Guidelines

## Project Overview

Rivet is a native Rust `Future` runtime for OS TCP/UDP, bounded blocking work, non-socket waits, timers, and coordination. The unpublished package is `rivet-runtime`; consumers import `rivet`. It is not a Tokio compatibility layer, protocol stack, TLS/DNS/HTTP implementation, or native asynchronous file-I/O runtime.

## Architecture & Data Flow

- `Runtime::new(RuntimeConfig)` validates budgets/policies and creates per-worker drivers, buffer pools, task queues, and timers. Worker 0 advances only inside `block_on`; background workers continue until shutdown.
- Network futures in `src/net/mod.rs` register bounded operations in `src/runtime/io.rs`. The compile-time-selected driver executes native I/O and emits semantic `Event`s; the worker updates queues, wakes futures, returns credits, and recycles leases. Native completion records stay behind the driver interface.
- `Runtime`, sockets, and ordinary buffer leases are worker-local and `!Send`. `Handle::spawn` transfers a `Send` **factory**, which constructs a potentially `!Send` future on its selected worker. `spawn_local` stays local. `TcpListener::serve_until` can transfer a newly accepted socket before data I/O, then supervise its handler; active sockets/futures do not migrate.

| Platform | Minimum supported OS (inclusive) | Native implementation |
| --- | --- | --- |
| Linux x86_64/aarch64 | Stable Linux 7.2.7 and later; RC kernels excluded | io_uring; no silent epoll fallback |
| Windows x86_64 | Windows 10 / Server 2016 and later (Rust target baseline; no extra OS-version gate) | RIO data path with IOCP, AcceptEx, and ConnectEx |
| Android ARM64; x86_64 verification target | API 23 and later | Ordinary-app epoll/nonblocking sockets; no io_uring probing |

These are minimum supported versions, not exact-version pins or claims that every release has been natively tested. Required native facilities must be available; optional optimizations still depend on compiled features, policy, and capability checks. Public Rust `Future` compatibility alone does not establish OS support.

## Key Directories

| Path | Where to work |
| --- | --- |
| `src/runtime/` | Scheduling, task ownership/groups, blocking pool, timers, logical I/O state |
| `src/net/` | Public TCP/UDP traits, socket types, concrete operation futures |
| `src/driver/` | Shared completion contract and Linux/Windows/Android native implementations |
| `src/io/` | Bounded non-socket registrations: Unix pollable FDs and Windows waitable handles |
| `src/sync/`, `src/signal/` | Coordination primitives and scoped process-signal subscriptions |
| `tests/`, `tests/support/` | Public behavior regressions and small platform-aware helpers |
| `examples/` | Executable consumer scenarios; shared service scenarios also feed Android smoke checks |
| `tools/verification/`, `android-smoke/` | Isolated Linux VM runner and separate Android test application |

## Development Commands

Run from the repository root on a supported host:

| Purpose | Command |
| --- | --- |
| Build library | `cargo build` |
| Check all compiled paths/targets | `cargo check --all-features --all-targets` |
| Default-feature tests | `cargo test --all-targets` |
| All-feature tests | `cargo test --all-features --all-targets` |
| Focused regression suite | `cargo test --test tcp_source_binding` |
| Strict lint | `cargo clippy --all-features --all-targets -- -D warnings` |
| Format check | `cargo fmt --all -- --check` |
| Public documentation | `cargo doc --no-deps --all-features` |
| IPv4-only native smoke | `cargo run --example native_traits` |

Other real scenarios: `cargo run --example loopback`, `cargo run --example runtime_services`, and `cargo run --release --example mixed_load`. Examples share `-- --workers N`, `--enable NAME` (strict), and `--auto NAME`; default to two workers. For example, on suitable Linux: `cargo run --release --all-features --example loopback -- --enable zc-tx-fixed`.

Platform harnesses require separate setup:

- Linux/WSL Debian 13 x86_64: `python3 tools/verification/linux_vm.py prepare`, then `python3 tools/verification/linux_vm.py run --accel tcg /path/to/static-elf`. Requires Python 3, curl, dpkg-deb, and sqv. Supply an already-built static x86_64 Linux ELF; the runner does not build it. KVM is the default; TCG must be explicit. Global `--state` precedes the subcommand.
- Windows Android packaging: `./android-smoke/build.ps1 -Abi x86_64` or `-Abi arm64-v8a`. Requires the matching Rust target, JDK, SDK, and NDK described in the script; it builds but does not install/run the APK.

## Code Conventions & Common Patterns

- Use rustfmt, four-space indentation, grouped imports, `snake_case` functions/modules/tests, and `PascalCase` types. Keep rustdoc ownership contracts and narrow platform/feature `cfg` gates. `src/lib.rs` denies `unsafe_op_in_unsafe_fn`; unsafe operations need explicit blocks and their safety invariants.
- Owner state uses `Rc`/`Weak`/`RefCell`/`Cell`; cross-thread queues and wakes use `Arc`, atomics, and short locks. Preserve notifier clear/recheck/wait ordering. Do not destroy user futures or captures while holding admission locks or mutable task-table borrows.
- Native traits use static dispatch and concrete futures with explicit `poll`/`Drop` behavior. Do not add per-operation boxed futures, payload copies, or blanket `Send`/`Sync`/`Unpin` bounds. Resources and queues stay bounded.
- Inject configuration through `RuntimeConfig`, explicit pools, and `SocketOptions`. `SocketOptions::hook` borrows a socket for pre-bind/connect host setup; it must not retain, close, bind, or connect that socket. Drivers are selected by `cfg`, not injected trait objects.
- Only `WriteBuf` permits mutation; freezing yields immutable `SendBuf`/`ReadBuf` leases. **Send completion is not kernel-memory release.** Preserve owning guards until real release, including cancellation and shutdown; published leases may outlive the runtime.
- `send` returns the original payload plus accepted-byte count; `send_all` preserves ordering and returns only the unaccepted suffix. Cancelling a receive waiter keeps queued data; cancelling a submitted send does not undo output. Preserve lazy flush/write-half-close and the independent receive direction.
- Dropping a `JoinHandle` requests cancellation; independent execution requires explicit `detach()`. Already-running blocking work cannot be forcibly stopped and must finish before runtime destruction completes.
- Preserve `io::Result`/`io::ErrorKind` distinctions and structured `SpawnError`/`JoinError` results. Failed resource imports return the original owned resource; do not turn capacity or lifecycle failures into panics or silent fallback.

## Important Files

- `src/lib.rs`: public exports and compatibility baseline. `src/config.rs` / `src/capability.rs`: budgets, feature policies, capability reporting.
- `src/buffer.rs`: lease/storage lifetime rules. `src/socket.rs`: native-handle ownership and configuration hooks. `src/time.rs` / `src/sync.rs`: public timing and coordination interfaces.
- For socket changes, inspect `src/net/mod.rs`, `src/runtime/io.rs`, and the affected driver together. For optimization changes, also inspect `Cargo.toml`, configuration normalization, and backend initialization.
- Read `docs/architecture.md` and `docs/implementation-contract.md` before changing behavior. Keep `README.md` and `CHANGELOG.md` aligned with public changes, migration instructions, and actually exercised platform evidence.
- Preserve the documented `0.1.x` public contract: trait implementer obligations, configuration/result construction, enum matching, and the concrete dependency types re-exported by `sync`. Intentional breaks require a minor bump before 1.0, a major bump afterward, and migration notes.
- `docs/linux-io-uring-compatibility.md` records API-history analysis, not a change to the existing Linux baseline or validation coverage.

## Runtime/Tooling Preferences

- Rust **1.98+**, edition **2024**, Cargo and Cargo-managed lockfiles. No checked-in toolchain pin or custom rustfmt configuration was found; do not invent a JavaScript package-manager workflow.
- Rust 1.77's ordinary Windows targets covered Windows 7+, including Windows 8; Rust 1.78 raised the client baseline to Windows 10. This Rust 1.98/edition 2024 crate follows the current Windows 10 / Server 2016 target baseline, not the old toolchain's coverage. Windows initialization checks Winsock/RIO facilities directly, with no OS-version/build-number gate; keep native errors observable and do not infer old-OS validation from RIO's introduction date.
- Default Cargo features are empty. Features compile implementations; runtime policies separately choose `Off`, `Auto`, or `RequireCapability`. `--all-features` does not activate every optimization. `linux-full` excludes `zc-rx-nodev`; explicit `Auto` alone permits capability fallback.
- `android-smoke/native/` is a separate Cargo package, not a root workspace member. Its Windows PowerShell packaging avoids Gradle and enforces API23 native/APK deployment, v1 signing, and 16KiB alignment. Keep Android startup free of post-23 strong symbol references; use the existing property-based API-level check rather than the external API29 getter. Debug signing is verification-only, not production packaging.
- Keep linker overrides process-scoped. Do not change host kernels, networking, NIC configuration, or security settings to make verification pass. Keep generated `target/`, `artifacts/`, Android build outputs, and APKs out of source changes.

## Testing & QA

- Use Cargo/libtest `#[test]` integration suites and private `#[cfg(test)]` modules, not Tokio test macros. `futures-lite` supports test polling. No configured coverage percentage or coverage gate was found; coverage expectations are behavioral and platform-specific.
- Follow descriptive contract names and isolated runtime/pool fixtures, ephemeral loopback ports, deadlines, and handshake channels. Assert bytes, metadata, cancellation, resource reuse, and ownership transitions. Capability injection belongs in private `src/capability.rs` tests; consumers query real runtime reports.
- Exercise relevant default and feature-gated paths. `--all-targets` covers Cargo targets, not every OS, and does not execute example mains or include the separate Android package. Run an actual relevant example after behavior changes.
- Keep real signal tests in isolated child processes (`tests/signal_behavior.rs`); never signal the test host. Reuse `tests/support/mod.rs` for address-family selection and Windows registered-I/O UDP setup.
- On intentionally IPv4-restricted Windows hosts, scope `$env:RIVET_VERIFY_IPV4_ONLY = '1'` to verification and report IPv6 as skipped. This is not a production setting or universal skip: Linux-network and Android-contract suites contain unconditional IPv6 cases.
- Cross-compilation is not native execution; Android shell tests are not ordinary-app sandbox proof. Loopback/NODEV results do not prove physical-NIC zero-copy or throughput. Historical `artifacts/` results and README test counts are evidence records, not current acceptance counts.
