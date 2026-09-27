# Rivet

原生 Rust `Future` 网络运行时。TCP／UDP 使用操作系统协议栈；提供有界阻塞执行、非 socket 原生等待和通用异步协调。不提供 Tokio 兼容层、TLS、DNS、HTTP、QUIC、原生异步文件 I/O、用户态协议栈、DMA-BUF 或 BPF 执行路径。

系统设计与平台契约：[`docs/architecture.md`](docs/architecture.md)、[`docs/implementation-contract.md`](docs/implementation-contract.md)。

## 平台与构建

| 平台 | 平台基线与验证目标 | 后端 |
| --- | --- | --- |
| Linux x86_64／aarch64 | 6.18 LTS 主验证线，6.6／6.12 兼容目标；无全局版本门禁 | io_uring；无静默 epoll 回退 |
| Windows x86_64 | Windows 10／Server 2016 及后续版本（当前 Rust 目标基线）；不额外按 OS 版本号拦截 | RIO 数据路径＋IOCP／AcceptEx／ConnectEx |
| Android ARM64；x86_64 验证 | API 23 及后续版本，普通 App 进程 | 非阻塞 socket＋epoll；不探测或依赖 io_uring |

Linux 按必要原生能力初始化，不因版本号、RC 或无法解析版本字符串直接拒绝整个 Runtime；更旧环境可以尝试运行，但这不承诺任意旧内核可用。版本与已知修复条件只参与逐项优化选择。Windows／Android 保留表中的平台基线。所有后端仍要求权限与资源可用；参考 UAPI、固定验证 guest、支持目标与实际执行证据各自独立。实际验证环境与限制见下文“已执行的原生验证”。

Rust 1.98+，edition 2024。当前包名为 `rivet-runtime`、库名为 `rivet`；本仓库不配置 crates.io 发布。

Windows 的接口可用版本、Rust 目标基线和实测范围不同：[RIO 从 Windows 8／Server 2012 引入](https://learn.microsoft.com/en-us/windows/win32/api/mswsock/ns-mswsock-rio_extension_function_table)；[Rust 1.77 的常规 MSVC 目标基线为 Windows 7+](https://doc.rust-lang.org/1.77.2/rustc/platform-support.html)，包含 Win8，但旧系统依赖社区测试；[Rust 1.78 起提高到 Windows 10](https://blog.rust-lang.org/2024/02/26/Windows-7/)，[当前服务器基线为 Server 2016](https://doc.rust-lang.org/rustc/platform-support/windows-msvc.html)。本包需要 Rust 1.98／edition 2024，不能用旧工具链的基线承诺 Win8 支持。Windows 后端不读取 OS 版本号，也不设置 Windows 11／Server 2022 门槛；初始化仍要求 Winsock 2.2、registered-I/O socket 和完整 RIO 扩展表可用，失败返回真实错误，不替换后端。表中版本不代表本仓库已在每个版本原生验证。

Android 最低 API 23 保留 `android_setsocknetwork` 的 Network 绑定能力。版本检测按 NDK 的低 API 实现读取 `ro.build.version.sdk`，不强链接 API 29 才导出的 `android_get_device_api_level`；读取失败不伪造版本。验证 App 的原生链接、DEX、manifest 和签名最低版本均为 23，SDK／NDK 工具版本不等于部署最低版本。

```toml
[dependencies]
rivet = { package = "rivet-runtime", path = "../rivet" }
```

## 公开 API 与兼容性

`0.2.0` 有意改变 Linux 默认编译和运行时优化选择，并删除全局 Linux 版本门禁。新的 `0.2.x` 内保持源码与已承诺行为兼容；以后有意破坏兼容时，`0.y.z` 提升次版本，`1.0` 及以后提升主版本，并给出迁移说明。这不代表所有平台路径都已完成原生验证。

- 能力报告由 Runtime 生成：从 `Runtime::capabilities()` 读取 worker 元数据，通过 `states()`、`state(optimization)`、`enabled(optimization)` 查询。`Optimization::{ALL, name, compiled}` 和结构化 `CapabilityError` 保持公开。原 `CapabilityReport::{new, decide, finish}` 与 `Optimization::bit` 不再供外部调用，`enabled_mask` 删除；迁移为按优化项查询，不依赖内部位图。
- 兼容契约包含拥有型缓冲区与内核租约、发送输入字节计数和未接受后缀、取消与 worker 归属、任务 Drop／detach、惰性 flush／写半关闭及反向读取、数据报元数据、计时器绑定／reset／错误语义，而不只是函数签名。
- 开放 trait 不在兼容版本中增加必需方法或更强约束；独立能力使用独立 trait。本次不改变配置／结果结构的字段和构造方式，不添加 `#[non_exhaustive]`；以后破坏下游字面量或穷尽匹配的扩展按破坏性变更处理。
- `sync` 直接重导出的 `async-channel` 2.x、`async-lock` 3.x、`futures-channel` 0.3.x 是公开依赖，类型身份、方法／约束、错误及取消／关闭行为也属于契约。升级、替换或改变 dependency feature 时须验证下游编译和相应行为；不新增包装，也不固定每个补丁版本。

完整约束见[系统与架构设计](docs/architecture.md#31-公开-interface-的兼容契约)及[实施契约](docs/implementation-contract.md#public-compatibility)。外部消费编译、管理入口不可访问检查和真实运行各证明不同方面，不能互相替代。

### 从 0.1 迁移

- 默认 Cargo 集合改为 `linux-full`，不包含 NODEV。需要精简构建时设置 `default-features = false`，基础 TCP／UDP 和分段发送语义仍完整。
- Linux 未指定的优化继承均衡、空闲可休眠的自动方案；用 `with_policy(optimization, Policy::Off)` 明确关闭，用 `enable` 严格要求。需要全部关闭时，为 `Optimization::ALL` 中每项显式设置 `Off`，而不是依赖空配置表。
- `policy`／`requested` 查询配置请求，不预测内核选择；从 `Runtime::capabilities()` 查询实际结果。Windows／Android 的运行时默认策略不变。
- 删除 `KernelVersion::MINIMUM_LINUX` 和 `require_supported`。不要在应用中复制固定版本门禁；创建 Runtime 并处理真实能力错误。`KernelVersion::parse` 可解析 RC／vendor 数字版本，无法解析的数据仍返回解析错误，但后端将版本缺失作为可选信息处理。
- `registered-buffers` 表示普通 fixed SEND/RECV 优化；关闭它不禁止 `zc-tx-fixed` 使用独立的内部注册资源。需要禁止这两种用途时，分别关闭两项。

## 自动放置与本地执行

`Runtime`、socket 和缓冲区租约均为 `!Send`。`Handle::spawn` 接受 `Send` 工厂，在自动选出的 worker 内创建本地 Future；Future 本身不必 `Send`。`spawn_local` 直接在当前 worker 创建本地任务。已经创建的 Future、正在收发的 socket 和租约不做任意跨核迁移。

`TcpListener::serve` 将刚接收、尚未开始数据 I/O 的 socket 自动移交到选出的 worker，再构造处理 Future；handler 由有界任务组监督，不再 detach。不需要应用划分工作负载组。需要自定义连接额度和优雅停止时使用 `serve_until`。

```rust
use rivet::{Runtime, RuntimeConfig, SendPayload, TcpListener, TcpStream};
use std::{io, net::Shutdown};

async fn echo(stream: TcpStream) -> io::Result<()> {
    while let Some(data) = stream.recv().await? {
        stream.send_all(SendPayload::Single(data)).await.result?;
    }
    stream.shutdown(Shutdown::Write)
}

fn main() -> io::Result<()> {
    let mut runtime = Runtime::new(RuntimeConfig::default())?;
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:9000".parse().unwrap())?;
        listener.serve(|stream| async move {
            if let Err(error) = echo(stream).await {
                eprintln!("connection: {error}");
            }
        }).await
    })
}
```

- Worker 0 只在 `block_on` 内推进；根 Future 返回后，其既有本地任务暂停，下一次 `block_on` 恢复。后台 worker 持续运行至 Runtime 销毁。
- 自动投递排除不活跃的 worker 0；候选在准入前失活时继续选择其他活跃 worker。单 worker 的外部 `Handle::spawn` 在未运行时返回 `NotRunning`，不默默挂起新工作。
- `JoinHandle` 丢弃会取消任务；需要独立运行时显式 `detach()`。Future 与已准入但未启动的工厂捕获值均在所属线程销毁，之后才发布完成结果。取消／shutdown 隔离其析构 panic，仍报告 `Cancelled`；执行或正常完成时的析构 panic 报告 `Panicked`。
- `runtime::buffer_pool()` 返回当前 worker 已有的池，适用于自动放置的工厂；不要为每个数据包创建新池。
- 任务、socket、操作、接收队列和池均有界。不可寻址的接收／接受／完成队列容量在配置阶段返回 `InvalidInput`。`idle_spin` 默认零；可选自旋受时间预算及下一定时器期限约束。

## 面向上层库的原生 trait

这些开放 Interface 让上层协议／代理库不绑定具体 socket 类型，但仍使用 Rivet 的拥有型缓冲区和运行时模型；不是 Tokio 兼容层，也不是跨运行时抽象。

| Module | 能力 | 原生实现 |
| --- | --- | --- |
| `net` | `StreamRecv`、`StreamSend`、`StreamShutdown` | `TcpStream` |
| `net` | `DatagramRecv`、`DatagramSend` | `UdpSocket` |
| `runtime` | `LocalSpawn` | `Current` |
| `runtime` | `Spawn`、`BlockingSpawn` | `Handle` |
| `time` | `Timer` | `runtime::Current` |

网络方法使用 `&self` 和静态分发的 Future，不逐操作装箱，不统一要求 `Send`／`Sync`／`Unpin`。接收仍只允许一个活动等待者，冲突返回 `WouldBlock`；一读一写独立推进。`send` 返还原始 payload，`send_all` 返还未接受后缀，原生发送分组不会被普通循环替换。上层编码／加密包装也必须按调用方输入字节计数，不能把编码后字节数或已接受数据作为未发送输入返还。

`StreamSend::flush` 排空包装层已接受的输出，并 flush 下层；不代表对端收到或内核释放租约。原生 TCP 没有额外发送缓存，但 flush 仍在首次 poll 时检查所属 worker 与 Runtime 存活状态。`StreamShutdown::shutdown_write` 是惰性异步半关闭：未轮询不会关闭，完成后接收方向仍可返回响应。调用方先完成自己的发送，再 flush／关闭；取消已提交发送不是撤销，不能直接重发原始数据。

```rust
use rivet::{
    SendPayload,
    net::{StreamRecv, StreamSend, StreamShutdown},
};

async fn forward<R, W>(reader: &R, writer: &W) -> std::io::Result<()>
where
    R: StreamRecv + ?Sized,
    W: StreamSend + StreamShutdown + ?Sized,
{
    while let Some(data) = reader.recv().await? {
        writer.send_all(SendPayload::Single(data)).await.result?;
        writer.flush().await?;
    }
    writer.shutdown_write().await
}
```

双向代理同时推进两个 `forward`，一个方向 EOF 只关闭相对端的写方向，不提前终止反向响应。缓冲区池由上层显式传入编码逻辑；`Connector`／`Acceptor`、域名处理、路由、代理握手和会话信息由上层定义。原生 connect/bind/accept 与 `serve_until` 仍可直接使用；新连接先完成 worker 放置，再做握手，不能跨线程移动已经开始 I/O 的连接。

`Current` 是无状态的当前上下文入口，不捕获 Runtime／worker，也不延长它们的生命周期。`LocalSpawn` 保留本地 Future／输出可为 `!Send` 但须 `'static` 的约束；`Handle` 的 `Spawn` 仍投递 `Send` 工厂，产生的 Future 可为 `!Send`，输出须 `Send`。`BlockingSpawn` 保留独立阻塞池及原生取消语义。所有任务返回现有句柄／错误，不改变 Drop 取消和显式 detach。

`Timer::sleep`／`sleep_until` 返回原生 `Sleep`，不提前绑定 worker，保留 `reset`、取消释放额度与原生计时错误；timeout／interval 继续复用现有函数。有限的出站实现可由上层枚举组合；这些 trait 不承诺 `dyn` 兼容，不要求为每次 I/O 做类型擦除。批量数据报、splice、GSO 等仍为独立能力。

## 通用运行时能力

### 阻塞执行和任务所有权

- `spawn_blocking`／`Handle::spawn_blocking` 接收 `Send` 闭包，返回 `BlockingJoinHandle<T>`。`RuntimeConfig.blocking` 的默认线程上限为 4、排队上限为 128；按需启动，满额显式返回 `BlockingSpawnError::AtCapacity`。外部 `Handle` 可在异步 worker 暂停时提交阻塞工作。
- 排队中的阻塞工作可以取消；已开始的同步代码不能强杀。丢弃句柄不提前释放仍执行工作的额度。Runtime 先停止准入、取消排队工作并清理异步 worker，再等待正在运行的阻塞工作及原生线程退出。阻塞闭包必须能够自行结束，不能依赖 Runtime 销毁后继续推进的异步任务。
- `runtime::TaskGroup<T>::new(capacity)` 拥有有界子任务集合。`spawn`／`spawn_on` 投递工厂，`spawn_local` 接收本地 Future；`join_next` 按就绪顺序取结果。完成但尚未 join 的任务仍占容量。空组立即返回 `None`，但仍可接纳新任务；动态 supervisor 应等待外部准入／停止通知，不能围绕 `None` 忙循环。
- `JoinHandle::abort_handle()` 和任务组 spawn 返回的 `AbortHandle` 可克隆，只提供取消／完成状态，不转移结果所有权。`TaskGroup::shutdown().await` 关闭准入、abort 并排空；取消一次等待可以重试。组 Drop 只请求取消，join 才证明子任务 Future／captures 已析构；返回值持有的资源和 native I/O 引用可能仍然存活。
- `TcpListener::serve_until(ServeConfig, stop, handler)` 在 `max_connections` 名额可用后轮询 accept；停止信号优先，handler 收到 `sync::CancellationToken`，经过 `shutdown_grace` 后才强制 abort 并 join。导入失败和任务 panic 作为错误返回。简单 `serve` 默认最多 1024 个 handler、30 秒错误清理宽限期。

每次 `join_next()` 创建新的等待 Future；不要重复 poll 同一个已返回 Ready 的普通 Future。显式融合类型或 `Sleep::reset` 有自己的复用契约，不代表所有 Future 都可重复等待。`runtime_services` 展示动态空组准入和取消后 join；整个 Runtime 关闭还会单独推进 native 请求收敛。

### 非 socket I/O

`RuntimeConfig.max_async_io` 默认 256，约束整个 Runtime 的原生注册数。导入失败返回 `io::ImportError { error, resource }`，保留原句柄所有权。

- Linux／Android：`io::AsyncFd::import(OwnedFd)` 接管已经非阻塞、可轮询的 FD。`readable`／`writable` 等待 readiness；`try_io(Interest, closure)` 临时借出 `BorrowedFd`，遇到 `WouldBlock` 清除对应 readiness。闭包不得关闭／保留句柄、改变非阻塞模式或执行阻塞工作。该专用等待路径不是 TCP／UDP 的 epoll 回退。
- Windows：`io::AsyncHandle::import(OwnedHandle)` 支持具有 `SYNCHRONIZE` 权限的 Event、Semaphore、Timer、Process、Thread、Job。`wait` 缓存自动复位对象已经取得的通知，取消等待不会丢掉该通知；持续 signaled 不导致后台自旋。不接受普通文件和拥有线程归属的 mutex。
- 对象保持 `!Send/!Sync`。等待 Future 不借用对象；`close`／Runtime 停止会唤醒它并返回 `BrokenPipe`。取消等待不关闭对象。Runtime 停止注销等待；外部对象仍拥有的句柄在该对象析构时释放。

### 协调、计时与退出事件

`sync` 不要求当前 Runtime：提供有界 `mpsc::bounded`、`oneshot`、`Mutex`、`RwLock`、`Semaphore`、`watch::channel`、`Notify` 和 `CancellationToken`。channel／锁复用执行器无关的成熟实现；不是 Tokio 兼容层。watch 合并更新，最后发送者关闭后仍可读取最终未读值；不要跨 `.await` 持有同步 watch 借用。Notify 保存至多一个单次通知许可，`notify_waiters` 只唤醒已经开始等待的调用。

`Sleep::reset(Instant)` 原位调整计时器，完成后也能复用。`time::interval`／`interval_at` 返回周期计时器，`tick` 返回计划时刻；取消等待不消费 tick。`MissedTickBehavior` 支持 `Burst`、`Skip`、`Delay`，默认 `Skip`；零周期和时间溢出报错。

`signal::ShutdownSignals::new()` 显式订阅 Unix SIGINT／SIGTERM 或 Windows Ctrl+C／Ctrl+Break；`recv()` 返回 `SignalKind`，不主动退出进程。每个订阅者独立接收，两种事件分别合并。最后一个订阅释放时注销处理器并停止辅助线程。Unix 已有自定义 handler 时返回 `AlreadyExists`，原有默认／忽略 disposition 会恢复；宿主必须串行化其他 `sigaction` 修改。Windows 订阅期间不能更换 console。自定义 Waker 若在信号辅助线程中释放最后订阅，该线程在回调返回后自行退出，不能同步 join 自身。

## 缓冲区、发送结果与关闭

`BufferPool` 预分配稳定的普通内存及租约槽。`WriteBuf` 独占初始化，`freeze()` 生成只读 `SendBuf`；`ReadBuf` 与 `SendBuf` 使用同一只读所有权。克隆／切片不复制 payload，也不为每个包分配引用计数控制块。

`WriteBuf::as_mut_slice()` 安全地借出已初始化前缀，适合原地编解码或修改报文头，不扩大长度、不分配、不复制。未初始化尾部仍通过 `spare_capacity_mut()` 写入。不要从 `ReadBuf`／`SendBuf` 的指针强造可写切片：先调用 `try_into_write()`，只有普通池存储唯一拥有且没有内核 guard 时才能成功。

```rust
async fn request(stream: &rivet::TcpStream) -> std::io::Result<()> {
    let pool = rivet::runtime::buffer_pool()?;
    let mut writable = pool.try_acquire_at_least(5)?;
    writable.extend_from_slice(b"hello")?;
    writable.as_mut_slice().make_ascii_uppercase();
    let outcome = stream.send_all(rivet::SendPayload::Single(writable.freeze())).await;
    outcome.result?;
    Ok(())
}
```

- `send` 返回原始只读 payload 和实际接受字节数。短写后用 `data.remaining(n)` 获得未发送部分。
- `send_all` 返回未发送后缀；完全成功时后缀为空。失败时重试后缀，不重复已接受前缀。
- 成功发送不等于对端已收到，也不等于内核已释放用户内存。ZC 操作在真实释放通知前保留 guard；`try_into_write` 只能恢复唯一拥有的普通存储，不能突破内核／其他租约的引用。
- 接收 Future 是队列等待者；丢弃它不清除已经到达的 TCP 字节。TCP EOF 与合法的零长度 UDP 数据报分别表示。
- 已发布租约可以跨越 socket／Runtime 的销毁继续读取；池和外部映射按实际引用释放。共享 refill 的队列满时保留返还 token，不丢弃它。
- 普通 TCP 关闭不默认设置 abortive linger。整个 Runtime 销毁会取消未结束业务；Linux 会中止剩余 TCP 传输并等待真实内核释放。需要完整交付时，先完成半关闭和对端协议确认。
- `TcpStream::abort(self)` 显式选择 RST 式关闭，设置失败可观察；Linux／Android 会断开底层连接，宿主保留的 FD 别名不会延后中止。它不改变普通 Drop／半关闭的语义，也不提前释放在途发送的内核内存引用。
- 原生 TCP socket 的正值 `SO_LINGER` 会阻塞关闭或导致非阻塞关闭失败，因此在接管及配置钩子后拒绝；不偷偷改成 abortive linger。

## TCP 源地址绑定

`TcpStream::connect_from(local, peer, options)` 显式选择出站 TCP 的本地 IP／端口，返回与普通建连相同的惰性 `Connect` Future：

```rust
use rivet::{SocketOptions, TcpStream};
use std::{io, net::SocketAddr};

async fn connect_bound(local: SocketAddr, peer: SocketAddr) -> io::Result<TcpStream> {
    TcpStream::connect_from(local, peer, SocketOptions::default()).await
}
```

- 本地端口 `0` 由系统分配；非零端口按原值绑定。本地和对端必须同属 IPv4 或 IPv6，族不一致在首次 poll 时、宿主 hook／原生 socket 创建前返回 `InvalidInput`。
- bind 失败原样返回错误，不以默认源地址重试。Linux direct-descriptors 路径也释放已经分配的固定描述符槽，失败不能持续吞掉准入额度。
- `connect`／`connect_with_options` 保留自动源地址语义：Windows 仍执行 ConnectEx 所需的通配地址绑定，Linux／Android 不增加提前 bind。
- 使用显式入口时，hook 负责保护、出口接口设置等宿主配置，不再自行 bind／connect。Android 先完成 protect 和指定 Network 绑定，再绑定本地地址。
- UDP 和 listener 继续使用已有的本地地址参数；`SocketOptions` 不增加第二个地址来源，导入的 socket 不重新绑定。取消已提交连接仍按原生完成路径回收，不承诺固定源端口可以立即复用。

## UDP、接管与宿主接入

`UdpSocket` 支持连接／非连接收发、来源地址、截断和可用的原始长度信息。`send_batch` 接受可复用的 `Datagram` 数组并保留每项结果及所有权；`recv_batch` 填入调用方空槽，收到至少一项即可返回，不等待填满。

Linux／Android 可选 `send_segments` 使用 UDP GSO；接收的 GRO 聚合由前端还原为数据报视图。组内只有一个目标地址与分段大小。不实现广播或多播。

Linux 接管已启用 GRO 的 socket 时，即使没有编译 `udp-gro` 或策略为 `Off`，也会解析已有分段元数据并还原数据报边界；不会为迎合策略而关闭 GRO、破坏已排队的数据。feature／策略仍控制运行时是否主动启用优化。

`TcpStream::splice_to` 与 `net::splice_bidirectional` 是显式 Linux 内核 socket→pipe→socket 转发；未启用／不支持时返回错误，不静默改成用户态复制。

各类型 `import(OwnedSocket, SocketOptions)` 转移拥有型句柄；失败返回 `ImportError { error, socket }`。失败不偷偷重建连接。Windows 要求 socket 创建时带 `WSA_FLAG_REGISTERED_IO`，并且没有既有 RIO request queue；普通 Winsock socket 不能静默降级接管。新接收 TCP socket 的 RQ 延迟至最终 worker 放置。

RIO UDP 在 `bind/import` 返回前提交完整的 `max_pending_receives` 接收窗口，不能等第一次 `recv` 才投递。每个窗口槽预留一个原生操作、注册元数据及至少 `max(receive_chunk, pool.block_size)` 字节的池存储；完整窗口无法准入时显式报错，不缩小窗口。暂时没有空闲替换缓冲区时暂停补充；这不是无限收包保证，UDP 超出已投递窗口仍可能丢包。创建 RQ 前的接管失败返还原 socket；RQ 接管后的异步投递失败等待原生请求收敛，再通过该 socket 的接收结果报告，不返还已带 RQ 的句柄供伪重试。

Android 的 `android_network` 使用真实 `android_setsocknetwork`。已连接的外部 TCP／UDP 不允许再指定 Network：旧内核可能保留原路由。先由宿主完成正确绑定／连接，再以 `android_network: None` 接管。`SocketHook` 提供建立连接／首次发送前的宿主控制入口，例如 `VpnService.protect`；宿主负责权限和 Java/JNI 生命周期，失败直接阻止建立。钩子不能关闭、保留或自行进行 socket I/O；其外部副作用无法回滚。库不附带 VPN 服务或进程级 Network 绑定。

### Windows UDP 容量计算

`max_pending_receives = N` 是**每 socket** 的窗口；`pool` 和 `max_operations` 是**每 worker 共享**的预算。`SocketOptions::receive_buffer_bytes` 只是 OS 缓冲区请求，不能替代已投递的 RIO 接收。

`config.limits.windows_udp_receive_bytes(options.receive_chunk)?` 计算一个窗口的初始 payload 字节数，所有平台都可用来规划 Windows 部署。它采用有检查的 `N × max(receive_chunk, pool.block_size)`；非法尺寸或溢出返回 `InvalidInput`，但允许返回比当前池更大的需求，以便调用方诊断。它不预留资源，也不意味着 bind 必然成功。

同一 worker 上 U 个同尺寸 UDP socket 的初始需求：

| 资源 | 需求 | 约束 |
| --- | --- | --- |
| 普通 RX payload | `U × N × max(receive_chunk, block_size)` | `pool.bytes` |
| distinct leases | `U × N` | `pool.max_leases` |
| driver native receives | `U × N` | driver 的 `max_operations` |
| Core logical receives | `U` | 独立的 Core `max_operations` |

例如 N=8、chunk=64 KiB、block=16 KiB，4 个 socket 的初始 RX 为 2 MiB／32 leases。64 KiB allocation 只占一个 lease，不是四个。如果上层保留完整旧窗口、又要挂满新窗口，还需额外 2 MiB／32 leases。另留发送、TCP、其他 operation、待交付完成和关闭收敛的余量；metadata 不计入 payload pool，池预算不是进程 RSS。不同 chunk 按 socket 求和，多 socket 乘加也应检查溢出。

准入失败仍保留原错误类别：池／槽位暂时不足为 `WouldBlock`，单块超过池等非法请求为 `InvalidInput`，原生调用保留 OS 错误。窗口失败诊断显示请求规模和配置预算，不把总预算冒充当前空闲量。碎片、旧租约和慢消费都可能暂停 rearm；窗口扩大是突发容量与并发 socket 成本的取舍，不是无丢包保证。详细 RQ 预约及所有权规则见[系统架构](docs/architecture.md#54-windows-udp-容量规划)。

## Linux 优化策略

默认编译 `linux-full`，Linux 在初始化期间根据已编译实现、内核版本／修复条件、实际原生能力和资源选择合法组合。**不是把每项策略都设为 Auto，也不是版本足够就全开**：

- 未指定项继承自动方案，显式 `Off` 优先；默认不启用 SQPOLL／NAPI 忙轮询。
- `config.enable(Optimization::...)` = `RequireCapability`；能力、权限或资源不满足时明确失败。显式 `Auto` 允许该项不可用。
- `capabilities()` 分别报告 compiled／supported／enabled／reason。探测和版本判断只在初始化进行，不用业务请求试错后重发。
- MSG_RING 默认只用于多个 worker；硬件 RX 需要已配置的网卡队列，NODEV 始终显式选择。mixed CQE 默认只在需要扩展完成项时考虑。
- 普通多段发送在旧内核使用 io_uring SENDMSG；multishot receive 使用相应内核支持的长度规则。普通 fixed 收发、incremental、ZCRX 等新路径不可用时，不反向抬高基础运行门槛。
- `linux-full` 是编译聚合，不是运行时预设；`default-features = false` 可关闭默认编译集合，`--all-features` 也不会自动选择互斥模式。

| Cargo feature | 路径 |
| --- | --- |
| `fixed-files`, `direct-descriptors` | 固定文件槽、direct socket／accept 与受控句柄移交 |
| `registered-ring`, `registered-wait`, `registered-buffers` | 注册 ring、等待参数、普通 CPU 缓冲区 |
| `provided-buffers`, `incremental-buffers` | provided buffer rings；TCP 增量与 UDP 普通接收分组，共享有界存储 |
| `multishot-accept`, `multishot-recv`, `multishot` | 持续 accept／recv／recvmsg |
| `buffer-bundles` | 合法的 send／recv bundle 路径 |
| `sq-rewind`, `mixed-cqe` | SQ_REWIND／NO_SQARRAY、混合完成项布局 |
| `zc-tx`, `zc-tx-fixed`, `zc-tx-vectored` | SEND_ZC／SENDMSG_ZC、固定／向量输入与真实释放通知 |
| `zc-rx`, `zc-rx-large-chunks`, `zc-rx-shared` | CPU 内存 RECV_ZC、refill、较大接收块、类型化内部 export/import |
| `zc-observe` | 实际复制通知、复制计数及资源分配失败事件 |
| `zc-rx-nodev` | 明确的 NODEV 复制接收验证，绝非硬件 RX ZC 证据 |
| `uring-sqpoll`, `uring-napi`, `uring-msg-ring` | 可选有限轮询、NAPI 注册、跨 worker 通知 |
| `udp-gso`, `udp-gro`, `udp-offload` | 数据报分段与聚合处理 |
| `tcp-splice` | 显式内核透明转发 |

`zc` 聚合 ZC 实现，不包含 NODEV 或 observe。显式请求的必要依赖会正规化；显式关闭依赖、显式同选 SQPOLL 与 RECV_ZC／NODEV／SQ_REWIND、硬件 RX 与 NODEV 同选，均拒绝，包括双方都是 `Auto` 的情况。默认候选先避开这些冲突。NODEV 可共享，但大接收块要求真实 RX 模式。可同时注册普通与 provided 存储，但单条 SQE 不混用不合法的 fixed／buffer-select／bundle／vector 组合；fixed ZC 的注册资源不再要求启用普通 fixed 收发。

硬件 RX 必须提供管理员已经配置好的网卡队列。运行时不修改 RSS、flow steering、网卡设置或全局网络／安全策略。`RequireCapability` 保证路径可用，不保证每次 ZC 发送都不复制；内核复制成功仍是成功，不能重发。

`runtime::zc_stats()`／`Runtime::zc_stats()` 是当前／根 driver 快照，不是跨 worker 总量。`tx_copy_marked_bytes` 表示被复制标记的操作接受字节数，不冒充精确复制字节。ZCRX 的数据 CQE／字节数是 driver 局部值；复制和分配失败通知属于共享内核实例，不能把各 importer 快照重复累加。

## 可执行验证

```text
cargo run --example loopback
cargo run --example runtime_services
cargo run --example native_traits
cargo run --example mixed_load
cargo test --all-targets
```

`loopback` 覆盖 IPv4／IPv6、向量前缀、取消接收等待、持有旧租约继续接收、UDP 空包与来源信息，以及 Runtime 销毁后的读取。`mixed_load` 同时运行小 RPC、大块 TCP 和有界窗口 UDP，自动分配 worker；输出不是 NIC 吞吐／延迟保证。

`runtime_services` 覆盖真实阻塞任务与异步网络并行、非 socket pipe／Windows event、任务组、计时器重置与周期、watch 最终值、受监督 echo／停止和对端 RST。相同场景也集成到 Android 普通 App。`cargo test --test signal_behavior` 在隔离子进程中验证真实退出信号及默认处理恢复，不向运行测试的宿主发送信号。

`native_traits` 是仅使用 IPv4 回环的可执行代理场景：原生 `serve_until` 放置连接，出站通过 `connect_from` 绑定 `127.0.0.2` 并在源站核验，泛型转发保留写半关闭后的反向响应；同时验证泛型 UDP 空报文／来源、`!Send` 本地任务输出、工厂投递、阻塞执行和计时器原位重置。它不建立外部连接，也不修改宿主 TUN、路由或 IPv6 设置。

本次 TCP 源地址绑定验证：

- Windows：IPv4 模式下 128 项回归通过，`native_traits` 实际代理确认源地址与写半关闭后的反向响应。
- Linux：隔离 `7.2.7-arch1-1` guest 的 4 项绑定回归覆盖普通、fixed-files、direct-descriptors；`native_traits --enable direct-descriptors` 实跑通过。guest 使用 TCG、无外部 NIC，仅操作自身 IPv4 回环。
- 静态检查：Clippy／rustdoc 将警告视为错误；新增回归和修改后的代理示例通过格式检查。Linux x86_64 GNU／musl、Android ARM64／x86_64、Windows GNU 完成 `--all-features --all-targets` 编译检查，Linux GNU 另通过无 feature 检查。

本次未执行 IPv6 连通性或 Android 原生场景；Android 编译检查不代替原生执行。未修改宿主 TUN、路由或 IPv6 设置。

Windows 主机明确只允许 IPv4 时，可为**验证进程**设置 `RIVET_VERIFY_IPV4_ONLY=1`。相关测试／示例明确打印未执行 IPv6；默认仍测试两个地址族，生产库不读取此变量。

```powershell
$env:RIVET_VERIFY_IPV4_ONLY = '1'
cargo run --example loopback
cargo test --all-targets
```

示例默认使用自动方案，也可用 `--disable NAME`、`--auto NAME`、`--enable NAME` 覆盖；`--workers N` 默认是两个 worker。严格请求高级能力须在对应能力可用的环境执行，例如：

```text
cargo run --release --all-features --example loopback -- --enable zc-tx-fixed --enable zc-tx-vectored --enable zc-observe
cargo run --release --all-features --example loopback -- --enable zc-rx-nodev --enable zc-rx-shared --enable zc-observe
```

`tools/verification/linux_vm.py` 提供签名／校验和固定、无磁盘和无外部 NIC 的隔离 guest。`--kernel 6.18` 是默认主验证线，另有 `6.6`、`6.12` 和保留的 `7.2.7`；全局 `--kernel`／`--state` 放在子命令前。先运行 `--kernel 6.18 prepare`，再运行 `--kernel 6.18 run --accel tcg /path/to/static-elf`。`run-suite` 接收 JSON 数组，每项包含唯一 `name`、静态 ELF 的 `executable` 和字符串数组 `args`，相对 ELF 路径按清单所在目录解析；一次 guest 启动逐项校验散列、执行并记录结果。runner 不构建代码、不挂载宿主目录、不修改 WSL 全局内核；KVM 不可用时须显式选择 TCG。loopback／NODEV 不能证明真实 NIC RX ZC、RSS、NAPI 或硬件吞吐。

Android 使用 [`android-smoke/build.ps1`](android-smoke/build.ps1) 构建专用普通 App，支持 `-Abi x86_64` 和 `-Abi arm64-v8a`，不依赖 Gradle。NDK 目标和 APK 最低版本为 API23，JNI 库采用 16KiB ELF／APK 对齐和真正未压缩的 ZIP 条目，并保留 API23 所需的 v1 签名。界面、`RivetSmoke` logcat 标签和私有 `files/smoke-result.json` 给出结构化结果；每次进程运行先清除旧成功状态，JNI 初始化失败也持久化。验证 APK 的 debuggable 配置和测试签名不能用于生产应用。

### 已执行的原生验证

本轮缓冲区／容量／生命周期 Interface 改进的验证（2026-09-28）：

- Windows x64 IPv4：默认及 `--all-features --all-targets` 回归各 **135 通过、0 失败**。包括已排队 UDP waiter 取消、旧租约跨窗口保留、精确容量接管失败回滚，以及两方向实际受压时的独立反向进展和半关闭。
- `native_traits`、`runtime_services -- --workers 1` 和 `--workers 2` 实跑通过。动态准入场景观察到三次空组等待、两个正常结果以及取消后本地析构完成。
- 临时外部消费者实际调用 `as_mut_slice()` 和 `windows_udp_receive_bytes()`，验证只读别名阻止可写恢复、完整 RIO 窗口超预算时 `WouldBlock`、修改后的精确 UDP 字节，以及 Runtime 收敛后接收租约恢复可写。验证后删除临时源码，不把编译或单元测试当作该原生场景的替代。
- 严格 Clippy、严格 rustdoc 和格式检查通过。Linux x86_64／Android ARM64 的全 feature／all-targets 检查及独立 Android smoke 包编译检查通过；这些不是 Linux／Android 原生执行或 APK 安装验证。
- `RIVET_VERIFY_IPV4_ONLY=1` 仅作用于测试子进程；Windows IPv6 未执行，未修改宿主 TUN／网络，也不声称物理 NIC 或吞吐验证。命令与日志索引：`artifacts/rivet-contracts-20260928/verification-summary.json`。


0.2.0 自动优化与兼容改造的本轮验证（2026-09-27）：

| 实际环境 | 已执行结果 | 限制 |
| --- | --- | --- |
| Linux 6.6.72-1-lts、6.12.75-1-lts、6.18.54-1-lts；x86_64 静态 musl | 默认及无默认 feature 的测试、`loopback`／`native_traits`／`runtime_services`／`mixed_load` 均通过；另覆盖单 worker、显式 Off、严格 fixed files、Auto registered wait 和显式 SQPOLL | 隔离 TCG guest、root、IPv4／IPv6 loopback；不支持的高级路径明确跳过，并校验对应严格失败／Auto 停用，不计作该优化原生通过 |
| Linux 7.2.7-arch1-1；x86_64 静态 musl | 全 feature 与精简构建的测试／四个示例通过；显式 NODEV＋shared＋observe、fixed＋vectored ZC 且普通 registered buffers Off 的 loopback 通过 | NODEV 为复制接收；没有物理 NIC／NAPI／RSS 或吞吐证据 |
| Windows x64，build 26200 | 全 feature／all-targets 测试及 `native_traits`、`runtime_services`、`loopback` 实跑通过 | 验证进程限定 IPv4，Windows IPv6 未执行；未改宿主 TUN、路由或网络设置 |

7.2.7 的初次混合负载暴露 UDP 增量尾部截断。永久回归在修复前准确失败于第 125 个 256 字节数据报；TCP／UDP 描述符分组后，该回归、小预算普通接收回退和原混合负载均通过，未延长业务超时、隐藏截断或重传丢失数据。

静态验证另包含 26 个独立 Linux feature、4 个聚合 feature 和 6 个交叉组合的全 target 编译检查；Linux 全 feature／精简构建与 Windows 全 feature 的严格 Clippy、严格 rustdoc、格式检查通过。Linux ARM64、Windows GNU、Android ARM64／x86_64 全 feature／全 target 及独立 Android native 包编译检查通过；这些不是新增的对应平台原生运行。证据索引在 `artifacts/linux-auto/verification-summary.json`，逐 guest 的精确内核、程序散列、参数、跳过原因、结果及修复前失败保留在索引指向的 `report.json`／`serial.log` 中。

此前 API23 下限迁移的验证：

- Android 6.0／API23、x86_64、4KiB 页、kernel 3.10.0+、SELinux Enforcing：普通 App UID 10055 的 17 项场景通过，1 项不可用的 UDP GSO/GRO 明确跳过；实际覆盖 IPv4／IPv6、Network 绑定和通用运行时能力。旋转及后台恢复继续显示缓存结果，不重跑原生场景。
- 同一 API23 环境的 Android 目标全 feature 原生回归共 127 项通过。既有保留 FD 别名的 IPv6 `abort` 回归先暴露 `EINVAL`；修正 Android 的 `AF_UNSPEC` 地址长度后通过，不跳过断言或吞掉错误。adb shell 回归与普通 App 结果分别记录。
- API37／x86_64／16KiB 页普通 App 的 18 项场景通过；另执行 3 项 `tcp_abort_behavior` 回归通过。两个 ABI 的 API23 APK 均完成 v1 签名与 16KiB 对齐校验，全部 77 个强动态引用都能在相应 ABI 的 API23 NDK 导出中匹配，不再引用 API29 getter；两个 Android 目标严格 Clippy、修改 Rust 文件格式检查和既有 Java 结果持久化回归通过。
- 证据、最终 APK SHA256 与限制见 `artifacts/android-api23/verification-summary.json`，原始结果与截图在该目录及 `artifacts/android-api23-modern/`。本次 ARM64 仅构建／符号检查，未执行 API23 ARM64 真机，也未改动或重新验证 Linux／Windows 后端。

此前各轮原生验证记录：

| 环境 | 已观察结果 | 限制 |
| --- | --- | --- |
| Windows 11 x64，build 26200 | 121 项全 feature 构建测试通过；`runtime_services` 实际阻塞任务、事件等待、受监督 TCP／RST 场景通过 | IPv4／IPv6 loopback，不经过 HTTP 代理；不证明外部路由或 NIC 性能 |
| 隔离 Linux 7.2.7-arch1-1 x86_64，musl 静态程序 | 129 项全 feature 构建测试与 `runtime_services` 通过，包括非 socket pipe、子进程信号、direct/fixed／ZC 发送中止回收；既有可选优化组合证据保留在验证索引 | guest 以 root 运行，loopback-only；不是普通用户权限或物理 NIC 证明 |
| Android API29，x86_64，4KiB 页，kernel 4.14.175 | 普通 App UID 10116 的 16 项场景通过；分别确认不可用 GSO／GRO 的 Auto 报告及 RequireCapability 失败 | 1 项实际 offload 场景明确跳过，不声称旧内核支持 |
| Android API37，x86_64，16KiB 页 | 普通 App UID 10230 的 18 项场景通过，包含新增通用能力及 IPv4／IPv6、真实 Network 绑定、GSO／GRO；另有 adb shell 下 45 项通用能力回归通过 | shell 回归不冒充 App 沙箱验证；模拟器，不是 ARM64 真机性能结果 |
| OnePlus 13 真机，Android 15／API35，ARM64，4KiB 页 | 普通 App UID 10385 的 17 项场景全部通过，包括 IPv4／IPv6、Network 绑定与 GSO／GRO；SELinux Enforcing | 本机 USB 连接，无 root 或安全策略修改；未覆盖 ARM64 16KiB 页设备 |

Android 的 x86_64／aarch64 均通过无 feature、独立 `udp-gso`、独立 `udp-gro` 和 `udp-offload` 的编译检查；两个 ABI 的 API29 APK 均完成构建、16KiB 对齐及签名验证。ARM64 APK 已在 OnePlus 13 普通 App 进程中执行，通过全部 17 项场景；真机结果、环境、APK 散列和截图保存在 `artifacts/android-usb-arm64-*`。

Linux GNU 的 x86_64／aarch64 release 库已构建，Windows GNU 目标完成编译检查；实际桌面运行来自 Windows MSVC 与 Linux x86_64 musl。Linux 的无 feature、26 个独立 feature 和 14 个组合共 41 组编译检查通过，结果保存在 `artifacts/linux-feature-checks.json`。仅编译 NODEV 时，不可编译的 Auto shared／large-chunk 子项不会激活冲突的硬件 RX 模式；该边界有失败前／修复后原生证据。

Windows／Linux 的 `cargo clippy --all-features --all-targets -- -D warnings`（Linux 指定 musl target）通过。Android 的两处生产代码及一处阻塞线程退出测试的 `thread_local!` 声明针对[上游 #13422](https://github.com/rust-lang/rust-clippy/issues/13422) 已知误报，使用仅限 Android 的 `cfg_attr(..., allow(clippy::missing_const_for_thread_local, reason = ...))` 定点豁免；保留正确的 const 初始化，不修改工具链或全局 lint 级别。aarch64／x86_64 Android 的全 feature、全 target 严格 Clippy 检查均通过。

两个桌面后端的混合负载均完成 4096 次小 RPC、每方向 16MiB 大块 TCP、每方向 8192 个 UDP 数据报，未用重传掩盖丢包。独立空闲探针保持两个 worker 及已绑定 TCP／UDP，等待约 2 秒：Linux 进程 CPU 时间 5.294ms；Windows `GetProcessTimes` 读数为 0，受计时精度限制，不能解释为绝对零 CPU。

验证总览在 `artifacts/verification-summary.json`。详细证据包括 `artifacts/windows-native-suite.log`、`artifacts/windows-ipv6-evidence.json`、`artifacts/linux-native-results.json`、`artifacts/android-usb-arm64-*`，以及 API29／API37 模拟器结果与截图；`artifacts/android-final-build-evidence.json` 记录模拟器执行 APK 的散列，USB 真机 APK 散列见其环境记录。Linux runner 在其 `--state` 目录的 `runs/<id>/report.json` 和 `serial.log` 保留内核、二进制散列、参数及实际结果，索引明确关联已修复的历史失败与后续通过记录。这些生成物不代替可重跑的测试／示例。

通用宿主能力的本轮证据在 `artifacts/runtime-services-verification.json`，包含 Linux 各 suite 的独立 guest 报告和 Android 普通 App 原始结果。本轮没有重新执行 API29、ARM64 真机、Server 2022 或硬件 NIC 场景；这些既有结果不能替代新增功能在相应设备上的原生验证。公开 rustdoc（warnings 为错误）、Linux 无 feature 构建和 Android ARM64 编译检查同时通过。

上述记录之后的两项修复验证：Windows 全 feature／全 target 的 122 项测试、严格 Clippy 和 `runtime_services` 通过；Android API37／x86_64／16KiB 模拟器上的 `signal_behavior` 6 项与 `tcp_abort_behavior` 3 项通过。独立消费者程序在 Windows 和 Android 验证了信号 Waker 同步销毁等待任务、最后订阅及重新订阅；Android 另确认 IPv4／IPv6 保留 FD 别名时对端仍收到 RST，别名无法继续发送。Android 原生执行来自 adb shell UID 2000，不冒充普通 App 沙箱验证；ARM64 Android 完成全 feature／全 target 编译检查，未执行 ARM64 真机或重新运行 Linux guest。

真实硬件 RX ZC、large-chunk DMA、NIC NAPI／RSS 行为及线上吞吐仍需要满足要求且已由部署方配置好的 NIC／队列。NODEV 明确是复制验证；本机未配置此类硬件，也未为验证修改外部主机内核、NIC 或主机安全策略。

Windows Server 2022、Linux ARM64 及 Android ARM64 16KiB 页设备未执行原生场景；Android ARM64 的已验证范围为上述 API35／4KiB 页真机。
