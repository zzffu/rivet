# Repository Guidelines

## Project Overview

Rivet is a native Rust `Future` runtime for OS TCP/UDP, bounded blocking work, non-socket waits, timers, and coordination. The unpublished package is `rivet-runtime`; consumers import `rivet`. It is not a Tokio compatibility layer, protocol stack, TLS/DNS/HTTP implementation, or native asynchronous file-I/O runtime.

## Architecture & Data Flow

- `Runtime::new(RuntimeConfig)` validates budgets/policies and creates per-worker drivers, buffer pools, task queues, and timers. Worker 0 advances only inside `block_on`; background workers continue until shutdown.
- Network futures in `src/net/mod.rs` register bounded operations in `src/runtime/io.rs`. The compile-time-selected driver executes native I/O and emits semantic `Event`s; the worker updates queues, wakes futures, returns credits, and recycles leases. Native completion records stay behind the driver interface.
- `Runtime`, sockets, and ordinary buffer leases are worker-local and `!Send`. `Handle::spawn` transfers a `Send` **factory**, which constructs a potentially `!Send` future on its selected worker. `spawn_local` stays local. `TcpListener::serve_until` can transfer a newly accepted socket before data I/O, then supervise its handler; active sockets/futures do not migrate.

| Platform | Platform baseline and validation targets | Native implementation |
| --- | --- | --- |
| Linux x86_64/aarch64 | 6.18 LTS primary validation line; 6.6/6.12 compatibility targets; no global version/RC gate | io_uring; no silent epoll fallback |
| Windows x86_64 | Windows 10 / Server 2016 and later (Rust target baseline; no extra OS-version gate) | RIO data path with IOCP, AcceptEx, and ConnectEx |
| Android ARM64; x86_64 verification target | API 23 and later | Ordinary-app epoll/nonblocking sockets; no io_uring probing |

Platform baselines, reference UAPI, pinned verification guests and actual native evidence are different facts. Linux starts from required native capabilities and uses version/fix evidence only for individual optimizations; unknown/RC releases do not trigger a global rejection or imply support for arbitrary old kernels. Windows/Android retain their platform baselines. Public Rust `Future` compatibility alone does not establish OS support.

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

Other real scenarios: `cargo run --example loopback`, `cargo run --example runtime_services`, and `cargo run --release --example mixed_load`. Examples share `-- --workers N`, `--enable NAME` (strict), `--auto NAME`, and `--disable NAME`; default to two workers. For example, on suitable Linux: `cargo run --release --all-features --example loopback -- --enable zc-tx-fixed`.

Platform harnesses require separate setup:

- Linux/WSL Debian 13 x86_64: `python3 tools/verification/linux_vm.py --kernel 6.18 prepare`, then `python3 tools/verification/linux_vm.py --kernel 6.18 run --accel tcg /path/to/static-elf`. Kernel selectors also include 6.6, 6.12 and retained 7.2.7. Requires Python 3, curl, dpkg-deb and sqv. Supply already-built static x86_64 Linux ELFs; `run-suite` accepts JSON `{name, executable, args}` cases, not host mounts or scripts. The runner does not build code. KVM is the default; TCG must be explicit. Global `--state` and `--kernel` precede the subcommand.
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
- Preserve the documented `0.2.x` public contract after the Linux automatic-policy cutover: trait implementer obligations, configuration/result construction, enum matching, and concrete dependency types re-exported by `sync`. Intentional breaks require a minor bump before 1.0, a major bump afterward, and migration notes.
- `docs/linux-io-uring-compatibility.md` archives the earlier 0.1 API-history analysis; current support and selection rules are in the architecture and implementation contract. Do not restore its historical global gate or treat interface introduction dates as native validation.

## Runtime/Tooling Preferences

- Rust **1.98+**, edition **2024**, Cargo and Cargo-managed lockfiles. No checked-in toolchain pin or custom rustfmt configuration was found; do not invent a JavaScript package-manager workflow.
- Rust 1.77's ordinary Windows targets covered Windows 7+, including Windows 8; Rust 1.78 raised the client baseline to Windows 10. This Rust 1.98/edition 2024 crate follows the current Windows 10 / Server 2016 target baseline, not the old toolchain's coverage. Windows initialization checks Winsock/RIO facilities directly, with no OS-version/build-number gate; keep native errors observable and do not infer old-OS validation from RIO's introduction date.
- Default Cargo features include `linux-full`, excluding `zc-rx-nodev`; `default-features = false` keeps complete base networking. Linux unspecified policies inherit a legal balanced/sleepable automatic plan; explicit Off/Auto/Require overrides remain. Default SQPOLL/NAPI and NODEV are not inferred. Windows/Android runtime defaults stay unchanged. `--all-features` compiles implementations, not a runtime all-on mode. Registered memory resources for fixed ZC are independent of ordinary fixed SEND/RECV capability.
- `android-smoke/native/` is a separate Cargo package, not a root workspace member. Its Windows PowerShell packaging avoids Gradle and enforces API23 native/APK deployment, v1 signing, and 16KiB alignment. Keep Android startup free of post-23 strong symbol references; use the existing property-based API-level check rather than the external API29 getter. Debug signing is verification-only, not production packaging.
- Keep linker overrides process-scoped. Do not change host kernels, networking, NIC configuration, or security settings to make verification pass. Keep generated `target/`, `artifacts/`, Android build outputs, and APKs out of source changes.

## Testing & QA

- Use Cargo/libtest `#[test]` integration suites and private `#[cfg(test)]` modules, not Tokio test macros. `futures-lite` supports test polling. No configured coverage percentage or coverage gate was found; coverage expectations are behavioral and platform-specific.
- Follow descriptive contract names and isolated runtime/pool fixtures, ephemeral loopback ports, deadlines, and handshake channels. Assert bytes, metadata, cancellation, resource reuse, and ownership transitions. Capability injection belongs in private `src/capability.rs` tests; consumers query real runtime reports.
- Exercise relevant default and feature-gated paths. `--all-targets` covers Cargo targets, not every OS, and does not execute example mains or include the separate Android package. Run an actual relevant example after behavior changes.
- Keep real signal tests in isolated child processes (`tests/signal_behavior.rs`); never signal the test host. Reuse `tests/support/mod.rs` for address-family selection and Windows registered-I/O UDP setup.
- On intentionally IPv4-restricted Windows hosts, scope `$env:RIVET_VERIFY_IPV4_ONLY = '1'` to verification and report IPv6 as skipped. This is not a production setting or universal skip: Linux-network and Android-contract suites contain unconditional IPv6 cases.
- Cross-compilation is not native execution; Android shell tests are not ordinary-app sandbox proof. Loopback/NODEV results do not prove physical-NIC zero-copy or throughput. Historical `artifacts/` results and README test counts are evidence records, not current acceptance counts.
