# Implementation contract

This document specifies the internal ownership and completion contracts. The system requirements are in `architecture.md`. No backend may claim support for a path it does not execute.

## Module boundaries

`config`, `capability`, `socket` and `driver` define shared configuration, owning-handle and completion contracts. `runtime` schedules owner-local futures and translates driver completions into bounded network operations.

`buffer` owns reusable leases; `net` exposes concrete network futures; `time` owns timer futures. Native implementations are isolated in `driver::linux`, `driver::windows` and `driver::android`. Platform details remain behind the driver contract rather than leaking into public network types.

`runtime::blocking` owns bounded synchronous work independently of the network workers. `io` owns bounded non-socket native registrations; `sync` supplies executor-independent coordination; `signal` owns explicitly scoped process-signal subscriptions. None of these modules implements a Tokio reactor or a protocol stack.

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
- Abortive-close setup must select zero linger on the same underlying TCP socket, including Linux fixed/direct references, before entering the existing close path. Linux and Android also disconnect with `AF_UNSPEC` before releasing the descriptor: imported sockets may have host-owned aliases, which must not postpone connection-level reset. It must not fabricate send completion or native memory release.
- `Driver::poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()>`; `Some(Duration::ZERO)` is nonblocking. Append events to caller-reused storage, bounded by its configured completion budget. Wake-only events are consumed internally.
- `Driver::is_idle(&self) -> bool` and `begin_shutdown(&mut self)` for deterministic owner-thread cleanup. Drop must not release memory still referenced by the OS.
- `Driver::zc_stats(&self) -> ZcStats` returns counters only when observation is enabled, never fabricates zero-copy success from opcode support.

Every published receive/accept event consumes one credit. At zero credits the backend stops rearming. An already running multishot may race with cancellation: retain consumed TCP data and accepted sockets in bounded backend-owned storage until credits return, without reporting logical EOF/stop for an internal pause. Publish retained data before rearming. Readiness backends retain known-ready state while paused rather than waiting for a second edge. Ordinary buffer-budget backpressure must not prevent reading another chunk merely because the previous immutable chunk remains borrowed when unused budget exists.

Windows Core preflights one logical UDP receive operation and calls `prime_udp` during socket insertion, before public bind/import returns. The RIO driver admits exactly `max_pending_receives` native lanes, metadata slots and receive buffers before creating an irreversible request queue; Core starts the persistent token and grants/commits its full initial credits. TCP newly accepted socket handoff remains un-pre-read. Subsequent asynchronous submission failures stay runtime-owned and converge real outstanding requests before publishing the receive error; they are not import failures returning an already-associated handle. Lane rearming reuses uniquely owned storage or acquires a budgeted replacement while published immutable leases survive.

Cold socket creation/listen/bind/import may use synchronous nonblocking socket system calls; connect and actual data I/O must not block worker threads waiting for network progress. Optional direct descriptor paths must really execute the respective io_uring operations when enabled.

## Linux ring/extension seam

`linux::uapi` and `linux::ring` pin the project-owned syscall layouts to Linux 7.2.7, including mixed CQEs, SQ_REWIND and the updated ZCRX structures. Their sizes, offsets and memory ordering are part of the backend contract rather than assumptions about a third-party wrapper.

`Ring` exposes native SQE reservation/submission, normalized CQE retrieval preserving enabled 32-byte extras, a registration syscall helper, raw ring fd, and feature bits. Optional implementation modules, native resources and submission branches follow their Cargo compile gates independently of runtime policy. Extension code must not approximate updated structures with older library bindings.

A Linux poll distinguishes SQ-capacity-deferred submissions from requests waiting for credits, buffers or retry deadlines. It must not block for CQEs while an otherwise runnable operation, cancellation or wake registration is deferred solely because the SQ is full. Finite submission/completion budgets and ordinary idle blocking remain intact.

Linux UDP ancillary decoding is required for correctness even without the `udp-gro` feature. Both ordinary and multishot recvmsg decode inherited `UDP_GRO` metadata through the same parser, so importing an already aggregated receive queue preserves individual datagrams. Compile/runtime optimization gates control actively enabling GRO, not interpreting data already supplied by an imported socket.

ZCRX owns the mapped CPU area and refill memory, and returns exact region leases through the buffer seam. Shared import/export between workers uses a typed internal owner retaining the export fd, CPU area, refill mapping, sizes and offsets; a raw import fd alone does not describe these resources and is not a public configuration input. Shared import/export requires synchronized refill producers or a single owning producer; it must not treat a shared SPSC ring as lock-free MPSC. Shared mode is opt-in and cannot hide cross-core ownership violations.

## Runtime/Interface behavior

`Runtime::new(RuntimeConfig)` constructs workers and returns initialization failures synchronously. `Runtime::block_on` polls the root future on the owning thread; the Runtime is not movable across threads after construction. A cloneable Send `Handle` supports automatic worker selection for Send factories that create local futures on their selected worker. `spawn_local` permits non-Send futures. Task cancellation drops the future on its owner.

Configuration normalization validates addressable array layouts for receive results, accepted socket results and completion events before constructing native resources. Impossible capacities return `InvalidInput`; passing this check is not a guarantee that physical memory allocation will succeed.

Worker zero progresses only inside `block_on`; its retained local tasks resume on the next call or cancel/drain on owner-thread Runtime drop. Background workers progress continuously. Automatic placement excludes inactive worker zero; a single-thread Handle cannot accept a factory outside `block_on` and silently strand it.

Load-based placement is a hint. For each candidate, checking active/closed state, admitting the task and committing its factory are serialized by that worker's inbox lock. An inactive or full candidate does not prevent a bounded attempt at the remaining workers. Factory destruction is outside that lock.

Cancellation and shutdown isolate panics when dropping the captures of an admitted but unlaunched factory, just as they isolate a launched future's destructor. Captures stay owner-thread-local at destruction, cancellation is still reported as `JoinError::Cancelled`, and admission is released exactly once. Join results and the shared finished flag are published only after future/factory destruction. Execution and normal-completion destructor panics are `JoinError::Panicked`; native I/O convergence remains a separate lifetime.

`runtime::buffer_pool() -> io::Result<BufferPool>` returns the current worker's existing pool, including inside automatically placed factories. `runtime::zc_stats()` snapshots the current driver; `Runtime::zc_stats()` snapshots only the owner/root driver, not an aggregate across workers. Shared ZCRX kernel-instance observations must not be summed repeatedly through imported views.

Public network operations are concrete futures; they do not box each I/O. TCP/UDP values are local handles. Receive/accept waiting is separate from persistent Driver operations. Core keeps bounded per-socket incoming queues and never treats cancellation of a waiter as permission to erase queued TCP bytes.

Socket closure, task cancellation, operation retirement and kernel memory release are separate transitions. Driver completion events wake the matching operation/receive waiter; stale generations cannot affect a newer object. The root loop drains shutdown and backend completions before destroying the Driver.

Default optional policies are Off until explicitly requested. `RuntimeConfig::enable(Optimization)` requests RequireCapability. Explicit Auto permits fallback only for that optimization. CPU/queue sizes and memory budgets are machine-level configuration, not manual per-workload scheduling groups.

## Native library capability interfaces

The public traits live in the existing `net`, `runtime`, and `time` modules. They are open for downstream implementations, use static dispatch, and introduce no per-operation boxing, payload copy, global configuration, or alternative scheduler. Native methods remain usable directly. `Connector`, `Acceptor`, proxy protocols, routing, resolution policy, and session metadata belong to downstream libraries, not this crate.

The network interfaces take `&self` and return `impl Future` without `Send`, `Sync`, `Unpin`, or `'static` supertraits:

- `StreamRecv::recv` returns `io::Result<Option<ReadBuf>>`.
- `StreamSend::{send, send_all}` take `SendPayload` and return `SendOutcome`; `flush` returns `io::Result<()>`.
- `StreamShutdown::shutdown_write` returns `io::Result<()>`.
- `DatagramRecv::recv` returns `io::Result<Received>`.
- `DatagramSend::{send, send_to}` take native payloads, with `send_to` additionally taking `SocketAddr`, and return `SendOutcome`.

`TcpStream` implements the three stream traits; `UdpSocket` implements the two datagram traits. Adapters delegate to the existing native futures. `send_all` is required, not a default loop over `send`: its logical send group must survive short writes without interleaving another send. One receive waiter owns each receive lane; a competing waiter gets `WouldBlock`. Reading and writing remain independent. Datagram operations preserve empty messages, boundaries, source addresses, and truncation rather than treating a short message as stream EOF.

Send results always describe the caller's input byte domain, including for downstream transforming streams. `send` returns the original immutable payload even on errors; `send_all` returns the unaccepted suffix. An implementation must not both retain a suffix as accepted buffered output and return it as unaccepted input. Success does not imply peer delivery or kernel memory release. Dropping a submitted send future abandons observation, not already-submitted output; blindly retrying the original payload can duplicate bytes.

`flush` drains accepted data retained in this layer and flushes the lower layer. Callers first finish their own sends; concurrent newly submitted sends are not implicitly joined. Native TCP has no additional user-space send buffer, so its lazy flush validates `Socket::owner` on first poll and then completes without another native operation. It must still report missing/wrong worker and stopped-runtime errors. It does not wait for peer acknowledgement or read-only lease reclamation.

`shutdown_write` is lazy: constructing or dropping an unpolled future does not close anything. A buffered/transforming implementation must drain accepted output and protocol trailers before closing its lower write direction. Native TCP calls its existing synchronous write shutdown only when polled; no new driver or socket state is needed. Receiving remains possible after write shutdown. Close cancellation does not promise rollback, and Drop/abort are not substitutes for orderly draining.

`runtime::Current` is a copyable stateless capability entry point, not a captured worker handle. It implements `LocalSpawn`; `spawn_local<F>` accepts `F: Future + 'static`, `F::Output: 'static`, and returns `Result<JoinHandle<F::Output>, SpawnError>` using the current worker at invocation. It is legal to construct Current outside a runtime; operations retain their native missing-context errors.

`Handle` implements `Spawn` with `F: FnOnce() -> Fut + Send + 'static`, `Fut: Future<Output = T> + 'static`, and `T: Send + 'static`, returning the native join handle and spawn error. It also implements `BlockingSpawn` with the existing `Send + 'static` closure/output bounds and native blocking handle/error. No borrowed/scoped spawning, implicit detach, additional queues, or handle-to-worker-local execution is introduced.

`time::Timer::{sleep(Duration), sleep_until(Instant)}` take `&self` and return native `Sleep`. The implementation for `Current` delegates to the existing constructors without binding early, capturing the entry point, or allocating. Reset, first-poll worker binding, overflow, capacity, cancellation, shutdown errors, interval behavior, and timeout precedence remain unchanged.

## Generic host services

`BlockingConfig { threads, queue_capacity }` bounds a lazy, runtime-owned blocking pool. Accepted closures and results are `Send + 'static`. `BlockingJoinHandle` can cancel queued work, but running synchronous code is not forcibly interrupted. Dropping a waiter does not free a still-running slot; rejected/cancelled captures and discarded results are destroyed outside admission locks with panic isolation. Completion is published after cleanup and running-slot release. Runtime shutdown closes admission, cancels queued work, stops native registrations, cleans/joins asynchronous workers, then joins blocking workers. A blocking closure must not depend on async work that shutdown has stopped.

`TaskGroup<T>` retains every admitted join handle, including completed results until consumed. `join_next` is ready-driven; cancelling it or `shutdown` does not detach children. `shutdown` permanently closes group admission and remembers an observed failure across retries. Group drop requests abort, whereas successful joining proves future cleanup. `serve_until` owns its handler group, prioritizes stop over accept, sends cooperative cancellation, observes the grace deadline, then aborts/joins. Its business capacity and the driver's bounded pre-accept queue are distinct.

`io::Registry` is runtime-owned and capacity-checked before allocation. Generation identities are never reused after wrapping. Unix registrations cache edge-triggered readiness; `try_io` clears only the matching direction/epoch on `WouldBlock`, so a concurrent newer edge survives. The shared epoll helper is only for explicitly imported non-socket I/O, not a network fallback. Windows validates object type/access without consuming a signal: mutex and file handles are rejected; threadpool waits arm only when a waiter is polled, cache one completion, and do not rearm merely because a manual-reset object remains signaled. Close disables rearming and joins callbacks before native ownership can be released. Runtime shutdown invalidates retained wait futures with `BrokenPipe`, even when their object outlives the runtime.

`sync` uses async-channel, async-lock, futures-channel and event-listener without requiring an executor context. Custom watch updates are published under a value/version lock before notification; the final unread version takes precedence over last-sender closure. User value destructors run outside publication locks. Notify's single stored permit is independent of its broadcast generation; cancelling a broadcast listener must not turn that broadcast into a notification for a later waiter. Cancellation tokens remain sticky and broadcast, not cleanup acknowledgements.

Timer reset adjusts an existing indexed-heap node instead of accumulating stale deadlines. Expired slots retain generation checks; queue shutdown permanently rejects re-admission. An interval owns one Sleep and commits its next deadline only on successful tick delivery. Burst advances one period, Skip advances to the next future grid point, and Delay schedules relative to the current instant. Zero periods and arithmetic overflow fail explicitly.

Native signal callbacks only mark bounded pending bits and notify the OS wake object. A static in-flight-reader protocol guards route lookup against descriptor/HANDLE reuse during last-subscription teardown. Unix saves/restores default or ignored dispositions, rejects pre-existing custom handlers, and never overwrites a later replacement; external disposition changes must be serialized by the host. Windows uses the current console and unregisters its own handler. The shared dispatcher wakes subscribers outside registry locks. Last-subscription drop joins the dispatcher except when invoked by a custom Waker on that same thread; then the stop flag ensures exit and resource release after the callback returns.

Each signal subscription has one receiver enforced by `recv(&mut self)`, bounded pending bits, and one `AtomicWaker`. Receiving checks state, registers, and rechecks to close the lost-wakeup window; completing or cancelling the receive removes its Waker without consuming pending bits on cancellation. Notification releases both registry/list locks and the Waker registration state before invoking `Waker::wake`, so synchronous task destruction can unregister its receive and drop the last subscription without reentering a held lock.
