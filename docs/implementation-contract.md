# Implementation contract

This document specifies the internal ownership and completion contracts. The system requirements are in `architecture.md`. No backend may claim support for a path it does not execute.

## Module boundaries

`config`, `capability`, `socket` and `driver` define shared configuration, owning-handle and completion contracts. `runtime` schedules owner-local futures and translates driver completions into bounded network operations.

`buffer` owns reusable leases; `net` exposes concrete network futures; `time` owns timer futures. Native implementations are isolated in `driver::linux`, `driver::windows` and `driver::android`. Platform details remain behind the driver contract rather than leaking into public network types.

## Platform handles

`socket::OwnedSocket` and `socket::BorrowedSocket<'a>` are aliases to the standard owning/borrowing socket handles on Windows and `OwnedFd`/`BorrowedFd` on Unix. `RawSocket` is the corresponding platform raw type.

`ImportError { error: io::Error, socket: OwnedSocket }` returns ownership on import failure. `SocketOptions` is cloneable, contains ordinary TCP/UDP options, an optional Android network handle, and an optional `Arc<dyn SocketHook>` invoked before connect or the first send. `SocketHook::configure(BorrowedSocket<'_>) -> io::Result<()>` is synchronous, Send + Sync, and may not close or retain ownership of the borrowed handle. Android Network binding and protection failures stop connection establishment.

## Buffer seam

The buffer implementation provides:

- `PoolConfig { bytes: usize, block_size: usize, max_leases: usize }`.
- `BufferPool::new(PoolConfig) -> io::Result<BufferPool>` and cheap local clones.
- `BufferPool::try_acquire() -> io::Result<WriteBuf>` for a normal receive block.
- `BufferPool::try_acquire_at_least(usize) -> io::Result<WriteBuf>`; memory accounting remains bounded.
- `BufferPool::set_recycle_waker(Waker)`; returning memory can wake the owning worker.
- `BufferPool::flush_recycles()` retries deferred external returns without allocating. `Recycle::recycle(ReturnToken) -> bool` returns true only after the provider accepted ownership of that return; false keeps the lease slot pending and prevents reuse. A bounded refill queue must never drop a token when full.
- `BufferPool::pending_recycles() -> usize` exposes deferred returns for orderly owner-thread drain. Backend recyclers must accept late returns after kernel teardown; published user leases may outlive the Runtime.
- `BufferPool::regions() -> Vec<MemoryRegion>` at initialization, with stable addresses and lengths for registration. Cold initialization allocation is allowed; per-packet allocation is not.
- `WriteBuf::{capacity, initialized_len, as_ptr, as_mut_ptr, as_slice, spare_capacity_mut, clear, extend_from_slice, freeze}`. `extend_from_slice` returns an error rather than growing storage beyond capacity. `unsafe set_initialized_len` requires that the caller has actually initialized the bytes.
- `WriteBuf::freeze() -> SendBuf`; `SendBuf::{len,is_empty,as_slice,as_ptr,slice}`. `slice(Range<usize>)` is an owning read-only subrange and copies no payload.
- `SendBuf::try_into_write(self) -> Result<WriteBuf, SendBuf>` may recover uniquely owned normal storage, never external memory or an aliased range. Aliases becoming unique notify the recycle waker so a retained receive allocation can be reused.
- `ReadBuf` is the same immutable ownership contract as `SendBuf`; a published receive region may not be modified until all aliases and kernel references are gone.
- `SendPayload::Single(SendBuf)` and `SendPayload::Vectored(Vec<SendBuf>)`, with `len`, `is_empty`, `segments`, and owning `remaining(bytes)` operations. Cloning an in-flight guard must not copy payload; metadata for repeated I/O should be pooled or shared rather than repeatedly allocating iovec storage.
- A bounded external-lease facility: `unsafe BufferPool::lease_external(region: Arc<dyn ExternalMemory>, offset: usize, length: usize, recycler: Arc<dyn Recycle>, token: ReturnToken) -> io::Result<ReadBuf>`. `ExternalMemory: Send + Sync` guarantees a stable base pointer and byte length for its lifetime. `Recycle: Send + Sync` accepts the exact `ReturnToken { offset: u64, length: u32, tag: u32 }` only after the last derived lease is released. The pool must retain the region and recycler. The implementation must not allocate a new reference-count control block for every received packet; use bounded reusable lease slots.

Normal leases remain worker-local (`!Send`). An external backing allocation can be shared safely without making arbitrary local leases transferable. Storage includes `UnsafeCell` where the kernel writes; the unsafe contract must distinguish writable kernel-owned ranges from immutable published ranges.

`unsafe trait ExternalMemory: Send + Sync` exposes `as_ptr() -> NonNull<u8>`, `len() -> usize` and `is_empty() -> bool`. `MemoryRegion` exposes stable `ptr: NonNull<u8>`, `len: usize`, and `id: u32`. Buffer split/clone increments local lease references; it must never create writable aliases. `SendPayload` need not implement Clone: a ZC Driver preserves its kernel guards in a reusable per-operation segment array rather than allocating another Vec on every send.

`SendOutcome { result: io::Result<usize>, data: SendPayload }` gives the caller read-only ownership even on errors and short writes. `send` returns the submitted payload; `send_all` advances ownership to the unsent suffix and returns empty data on complete success, so retrying after an error cannot duplicate an accepted prefix. Neither operation hands writable storage back before kernel release.

## Driver seam

`Token(u64)` and `SocketId(u64)` are generation-bearing identifiers. Reserved kernel notification user_data values must not overlap application tokens.

`SocketKind` is `TcpStream`, `TcpListener`, or `Udp`.

`SocketInfo { id: SocketId, kind: SocketKind, local_addr: SocketAddr, peer_addr: Option<SocketAddr> }` carries logical identity, not a transferable public fd.

`Received { data: ReadBuf, peer: Option<SocketAddr>, truncated: bool, original_len: Option<usize>, gro_segment_size: Option<u16> }` preserves datagram semantics. TCP EOF is a separate event and UDP zero length is valid.

The common event variants are:

- `Connected { token, result: io::Result<SocketInfo> }`.
- `Accepted { token, result: io::Result<SocketInfo> }` for a persistent accept operation.
- `Received { token, result: io::Result<Received> }` for a persistent receive operation.
- `ReceiveEof { token }` for TCP only.
- `Sent { token, outcome: SendOutcome, memory_released: bool }`.
- `Released { token }` when a previously reported send's kernel memory references have ended.
- `Spliced { token, result: io::Result<usize> }`.
- `Stopped { token, result: io::Result<()> }` when a persistent operation can no longer produce events.

A receive/accept token remains alive across kernel multishot restarts. Completion of one kernel shot does not masquerade as logical stream EOF. Resource exhaustion must not silently discard TCP data. Accepted sockets are not automatically pre-read, so an idle new socket can be handed to another worker before it becomes local.

Each platform exports a concrete `Driver`, `Notifier`, and `Shared`. The common module selects them with target cfg, with no trait-object dispatch in the steady-state I/O path.

Required methods:

- `Shared::new(workers: usize) -> Shared`.
- `Notifier::new() -> io::Result<Notifier>`, `notify(&self)`, `reset(&self)`, `close(&self)`. The runtime stores it in `Arc`. `notify` is thread-safe and coalesced; reset plus a work recheck precedes sleeping. Platform-specific native handle methods may be private.
- `Driver::new(config: &RuntimeConfig, worker: usize, pool: BufferPool, notifier: Arc<Notifier>, shared: Arc<Shared>) -> io::Result<Driver>`.
- `Driver::capabilities(&self) -> &CapabilityReport`.
- `Driver::listen(&mut self, addr: SocketAddr, options: &SocketOptions) -> io::Result<SocketInfo>`.
- `Driver::bind_udp(&mut self, addr: SocketAddr, peer: Option<SocketAddr>, options: &SocketOptions) -> io::Result<SocketInfo>`.
- `Driver::connect(&mut self, token: Token, addr: SocketAddr, options: &SocketOptions) -> io::Result<()>`.
- `Driver::import(&mut self, socket: OwnedSocket, kind: SocketKind, options: &SocketOptions) -> Result<SocketInfo, ImportError>`.
- `Driver::take_idle_socket(&mut self, socket: SocketId) -> io::Result<OwnedSocket>`; internal new-connection dispatch only, fails for active I/O or queued receive data.
- `Driver::start_accept(&mut self, socket: SocketId, token: Token) -> io::Result<()>`.
- `Driver::start_recv(&mut self, socket: SocketId, token: Token) -> io::Result<()>`.
- `Driver::receive_capacity(&mut self, socket: SocketId, slots: usize) -> io::Result<()>` and `accept_capacity` set absolute publication credits. Persistent operations begin with zero credits; Core sets them before polling. Core updates credits only after dispatching the complete previous event batch and after a consumer dequeues.
- `Driver::send(&mut self, socket: SocketId, token: Token, data: SendPayload, destination: Option<SocketAddr>, segment_size: Option<u16>) -> Result<(), SendOutcome>`. Synchronous rejection returns the original payload and error. Success must eventually emit Sent and, if needed, Released.
- `Driver::splice(&mut self, token: Token, source: SocketId, destination: SocketId, bytes: usize) -> io::Result<()>`.
- `Driver::shutdown(&mut self, socket: SocketId, how: std::net::Shutdown) -> io::Result<()>`.
- `Driver::close(&mut self, socket: SocketId) -> io::Result<()>` and `cancel(&mut self, token: Token) -> io::Result<()>`.
- `Driver::poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()>`; `Some(Duration::ZERO)` is nonblocking. Append events to caller-reused storage, bounded by its configured completion budget. Wake-only events are consumed internally.
- `Driver::is_idle(&self) -> bool` and `begin_shutdown(&mut self)` for deterministic owner-thread cleanup. Drop must not release memory still referenced by the OS.
- `Driver::zc_stats(&self) -> ZcStats` returns counters only when observation is enabled, never fabricates zero-copy success from opcode support.

Every published receive/accept event consumes one credit. At zero credits the backend stops rearming. An already running multishot may race with cancellation: retain consumed TCP data and accepted sockets in bounded backend-owned storage until credits return, without reporting logical EOF/stop for an internal pause. Publish retained data before rearming. Readiness backends retain known-ready state while paused rather than waiting for a second edge. Ordinary buffer-budget backpressure must not prevent reading another chunk merely because the previous immutable chunk remains borrowed when unused budget exists.

Windows Core preflights one logical UDP receive operation and calls `prime_udp` during socket insertion, before public bind/import returns. The RIO driver admits exactly `max_pending_receives` native lanes, metadata slots and receive buffers before creating an irreversible request queue; Core starts the persistent token and grants/commits its full initial credits. TCP newly accepted socket handoff remains un-pre-read. Subsequent asynchronous submission failures stay runtime-owned and converge real outstanding requests before publishing the receive error; they are not import failures returning an already-associated handle. Lane rearming reuses uniquely owned storage or acquires a budgeted replacement while published immutable leases survive.

Cold socket creation/listen/bind/import may use synchronous nonblocking socket system calls; connect and actual data I/O must not block worker threads waiting for network progress. Optional direct descriptor paths must really execute the respective io_uring operations when enabled.

## Linux ring/extension seam

`linux::uapi` and `linux::ring` pin the project-owned syscall layouts to Linux 7.2.7, including mixed CQEs, SQ_REWIND and the updated ZCRX structures. Their sizes, offsets and memory ordering are part of the backend contract rather than assumptions about a third-party wrapper.

`Ring` exposes native SQE reservation/submission, normalized CQE retrieval preserving enabled 32-byte extras, a registration syscall helper, raw ring fd, and feature bits. Optional implementation modules, native resources and submission branches follow their Cargo compile gates independently of runtime policy. Extension code must not approximate updated structures with older library bindings.

ZCRX owns the mapped CPU area and refill memory, and returns exact region leases through the buffer seam. Shared import/export between workers uses a typed internal owner retaining the export fd, CPU area, refill mapping, sizes and offsets; a raw import fd alone does not describe these resources and is not a public configuration input. Shared import/export requires synchronized refill producers or a single owning producer; it must not treat a shared SPSC ring as lock-free MPSC. Shared mode is opt-in and cannot hide cross-core ownership violations.

## Runtime/Interface behavior

`Runtime::new(RuntimeConfig)` constructs workers and returns initialization failures synchronously. `Runtime::block_on` polls the root future on the owning thread; the Runtime is not movable across threads after construction. A cloneable Send `Handle` supports automatic worker selection for Send factories that create local futures on their selected worker. `spawn_local` permits non-Send futures. Task cancellation drops the future on its owner.

Worker zero progresses only inside `block_on`; its retained local tasks resume on the next call or cancel/drain on owner-thread Runtime drop. Background workers progress continuously. Automatic placement excludes inactive worker zero; a single-thread Handle cannot accept a factory outside `block_on` and silently strand it.

`runtime::buffer_pool() -> io::Result<BufferPool>` returns the current worker's existing pool, including inside automatically placed factories. `runtime::zc_stats()` snapshots the current driver; `Runtime::zc_stats()` snapshots only the owner/root driver, not an aggregate across workers. Shared ZCRX kernel-instance observations must not be summed repeatedly through imported views.

Public network operations are concrete futures; they do not box each I/O. TCP/UDP values are local handles. Receive/accept waiting is separate from persistent Driver operations. Core keeps bounded per-socket incoming queues and never treats cancellation of a waiter as permission to erase queued TCP bytes.

Socket closure, task cancellation, operation retirement and kernel memory release are separate transitions. Driver completion events wake the matching operation/receive waiter; stale generations cannot affect a newer object. The root loop drains shutdown and backend completions before destroying the Driver.

Default optional policies are Off until explicitly requested. `RuntimeConfig::enable(Optimization)` requests RequireCapability. Explicit Auto permits fallback only for that optimization. CPU/queue sizes and memory budgets are machine-level configuration, not manual per-workload scheduling groups.
