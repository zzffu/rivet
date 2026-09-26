# Rivet

原生 Rust `Future` 网络运行时。TCP／UDP 使用操作系统协议栈；不提供 Tokio 兼容层、TLS、DNS、HTTP、QUIC、文件 I/O、用户态协议栈、DMA-BUF 或 BPF 执行路径。

系统设计与平台契约：[`docs/architecture.md`](docs/architecture.md)、[`docs/implementation-contract.md`](docs/implementation-contract.md)。

## 平台与构建

| 平台 | 基线 | 后端 |
| --- | --- | --- |
| Linux x86_64／aarch64 | 稳定内核 7.2.7+；拒绝旧内核和 RC | io_uring；无静默 epoll 回退 |
| Windows x86_64 | Windows 11／Server 2022+ | RIO 数据路径＋IOCP／AcceptEx／ConnectEx |
| Android ARM64；x86_64 验证 | API 29+，普通 App 进程 | 非阻塞 socket＋epoll；不探测或依赖 io_uring |

Rust 1.98+，edition 2024。当前包名为 `rivet-runtime`、库名为 `rivet`；本仓库不配置 crates.io 发布。

```toml
[dependencies]
rivet = { package = "rivet-runtime", path = "../rivet" }
```

## 自动放置与本地执行

`Runtime`、socket 和缓冲区租约均为 `!Send`。`Handle::spawn` 接受 `Send` 工厂，在自动选出的 worker 内创建本地 Future；Future 本身不必 `Send`。`spawn_local` 直接在当前 worker 创建本地任务。已经创建的 Future、正在收发的 socket 和租约不做任意跨核迁移。

`TcpListener::serve` 将刚接收、尚未开始数据 I/O 的 socket 自动移交到选出的 worker，再构造处理 Future。不需要应用划分工作负载组。

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
- `JoinHandle` 丢弃会取消任务；需要独立运行时显式 `detach()`。Future 与已准入但未启动的工厂捕获值均在所属线程销毁；取消／shutdown 隔离其析构 panic，不中断其余清理。
- `runtime::buffer_pool()` 返回当前 worker 已有的池，适用于自动放置的工厂；不要为每个数据包创建新池。
- 任务、socket、操作、接收队列和池均有界。不可寻址的接收／接受／完成队列容量在配置阶段返回 `InvalidInput`。`idle_spin` 默认零；可选自旋受时间预算及下一定时器期限约束。

## 缓冲区、发送结果与关闭

`BufferPool` 预分配稳定的普通内存及租约槽。`WriteBuf` 独占初始化，`freeze()` 生成只读 `SendBuf`；`ReadBuf` 与 `SendBuf` 使用同一只读所有权。克隆／切片不复制 payload，也不为每个包分配引用计数控制块。

```rust
async fn request(stream: &rivet::TcpStream) -> std::io::Result<()> {
    let pool = rivet::runtime::buffer_pool()?;
    let mut writable = pool.try_acquire_at_least(5)?;
    writable.extend_from_slice(b"hello")?;
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
- 原生 TCP socket 的正值 `SO_LINGER` 会阻塞关闭或导致非阻塞关闭失败，因此在接管及配置钩子后拒绝；不偷偷改成 abortive linger。

## UDP、接管与宿主接入

`UdpSocket` 支持连接／非连接收发、来源地址、截断和可用的原始长度信息。`send_batch` 接受可复用的 `Datagram` 数组并保留每项结果及所有权；`recv_batch` 填入调用方空槽，收到至少一项即可返回，不等待填满。

Linux／Android 可选 `send_segments` 使用 UDP GSO；接收的 GRO 聚合由前端还原为数据报视图。组内只有一个目标地址与分段大小。不实现广播或多播。

Linux 接管已启用 GRO 的 socket 时，即使没有编译 `udp-gro` 或策略为 `Off`，也会解析已有分段元数据并还原数据报边界；不会为迎合策略而关闭 GRO、破坏已排队的数据。feature／策略仍控制运行时是否主动启用优化。

`TcpStream::splice_to` 与 `net::splice_bidirectional` 是显式 Linux 内核 socket→pipe→socket 转发；未启用／不支持时返回错误，不静默改成用户态复制。

各类型 `import(OwnedSocket, SocketOptions)` 转移拥有型句柄；失败返回 `ImportError { error, socket }`。失败不偷偷重建连接。Windows 要求 socket 创建时带 `WSA_FLAG_REGISTERED_IO`，并且没有既有 RIO request queue；普通 Winsock socket 不能静默降级接管。新接收 TCP socket 的 RQ 延迟至最终 worker 放置。

RIO UDP 在 `bind/import` 返回前提交完整的 `max_pending_receives` 接收窗口，不能等第一次 `recv` 才投递。每个窗口槽预留一个原生操作、注册元数据及至少 `max(receive_chunk, pool.block_size)` 字节的池存储；完整窗口无法准入时显式报错，不缩小窗口。暂时没有空闲替换缓冲区时暂停补充；这不是无限收包保证，UDP 超出已投递窗口仍可能丢包。创建 RQ 前的接管失败返还原 socket；RQ 接管后的异步投递失败等待原生请求收敛，再通过该 socket 的接收结果报告，不返还已带 RQ 的句柄供伪重试。

Android 的 `android_network` 使用真实 `android_setsocknetwork`。已连接的外部 TCP／UDP 不允许再指定 Network：旧内核可能保留原路由。先由宿主完成正确绑定／连接，再以 `android_network: None` 接管。`SocketHook` 提供建立连接／首次发送前的宿主控制入口，例如 `VpnService.protect`；宿主负责权限和 Java/JNI 生命周期，失败直接阻止建立。钩子不能关闭、保留或自行进行 socket I/O；其外部副作用无法回滚。库不附带 VPN 服务或进程级 Network 绑定。

## Linux 优化策略

Cargo feature 只纳入实现，**不自动启用运行策略**。默认所有可选策略为 `Off`：

- `config.enable(Optimization::...)` = `RequireCapability`；能力、权限或资源不满足时明确失败。
- `with_policy(..., Policy::Auto)` 才允许该优化降级；不是全局静默回退。
- `capabilities()` 分别报告 compiled／supported／enabled／reason。
- `linux-full` 是编译聚合，不是把所有开关同时打开；`zc-rx-nodev` 必须另行显式选择。

| Cargo feature | 路径 |
| --- | --- |
| `fixed-files`, `direct-descriptors` | 固定文件槽、direct socket／accept 与受控句柄移交 |
| `registered-ring`, `registered-wait`, `registered-buffers` | 注册 ring、等待参数、普通 CPU 缓冲区 |
| `provided-buffers`, `incremental-buffers` | provided buffer ring、增量区间消费与回收 |
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

`zc` 聚合 ZC 实现。具体依赖会正规化；显式关闭必要依赖、SQPOLL 与 RECV_ZC／NODEV／SQ_REWIND 同 ring、硬件 RX 与 NODEV 同选，均拒绝。NODEV 可共享，但大接收块要求真实 RX 模式。可同时注册普通与 provided 存储，但单条 SQE 不混用不合法的 fixed／buffer-select／bundle／vector 组合。

硬件 RX 必须提供管理员已经配置好的网卡队列。运行时不修改 RSS、flow steering、网卡设置或全局网络／安全策略。`RequireCapability` 保证路径可用，不保证每次 ZC 发送都不复制；内核复制成功仍是成功，不能重发。

`runtime::zc_stats()`／`Runtime::zc_stats()` 是当前／根 driver 快照，不是跨 worker 总量。`tx_copy_marked_bytes` 表示被复制标记的操作接受字节数，不冒充精确复制字节。ZCRX 的数据 CQE／字节数是 driver 局部值；复制和分配失败通知属于共享内核实例，不能把各 importer 快照重复累加。

## 可执行验证

```text
cargo run --example loopback
cargo run --example mixed_load
cargo test --all-targets
```

`loopback` 覆盖 IPv4／IPv6、向量前缀、取消接收等待、持有旧租约继续接收、UDP 空包与来源信息，以及 Runtime 销毁后的读取。`mixed_load` 同时运行小 RPC、大块 TCP 和有界窗口 UDP，自动分配 worker；输出不是 NIC 吞吐／延迟保证。

Windows 主机明确只允许 IPv4 时，可为**验证进程**设置 `RIVET_VERIFY_IPV4_ONLY=1`。相关测试／示例明确打印未执行 IPv6；默认仍测试两个地址族，生产库不读取此变量。

```powershell
$env:RIVET_VERIFY_IPV4_ONLY = '1'
cargo run --example loopback
cargo test --all-targets
```

Linux 特性按合法组合分别选择，例如：

```text
cargo run --release --all-features --example loopback -- --enable zc-tx-fixed --enable zc-tx-vectored --enable zc-observe
cargo run --release --all-features --example loopback -- --enable zc-rx-nodev --enable zc-rx-shared --enable zc-observe
```

`tools/verification/linux_vm.py` 提供签名／校验和固定的、无磁盘和无外部 NIC 的 Linux 7.2.7 guest；记录实际 guest 内核、配置、可执行文件 SHA256、串口输出与退出状态。它不修改 WSL 全局内核。KVM 不可用时须显式选择 TCG，不静默回退。loopback／NODEV 不能证明真实 NIC RX ZC、RSS、NAPI 或硬件吞吐。

Android 使用 [`android-smoke/build.ps1`](android-smoke/build.ps1) 构建专用普通 App，支持 `-Abi x86_64` 和 `-Abi arm64-v8a`，不依赖 Gradle。NDK 目标为 API29，JNI 库采用 16KiB ELF／APK 对齐和真正未压缩的 ZIP 条目。界面、`RivetSmoke` logcat 标签和私有 `files/smoke-result.json` 给出结构化结果；每次进程运行先清除旧成功状态，JNI 初始化失败也持久化。验证 APK 的 debuggable 配置和测试签名不能用于生产应用。

### 已执行的原生验证

| 环境 | 已观察结果 | 限制 |
| --- | --- | --- |
| Windows 11 x64，build 26200 | 61 项既有测试通过；切换 HTTP PROXY 后，原生 IPv4／IPv6 loopback 与取消 IPv4-only 限制后的 31 项网络行为测试通过 | 本次验证直接连接 `127.0.0.1`／`::1`，不经过 HTTP 代理；不证明外部 IPv6 路由或 NIC 性能 |
| 隔离 Linux 7.2.7-arch1-1 x86_64，musl 静态程序 | 全 feature 的 66 项测试、NODEV-only 的 11 项配置回归通过；IPv4／IPv6、splice、GSO／GRO、registered-wait、multishot、增量 buffers、bundles、SQPOLL、MSG_RING、ZC TX、共享 NODEV 组合通过 | guest 以 root 运行，loopback-only；不是普通用户权限或物理 NIC 证明 |
| Android API29，x86_64，4KiB 页，kernel 4.14.175 | 普通 App UID 10116 的 16 项场景通过；分别确认不可用 GSO／GRO 的 Auto 报告及 RequireCapability 失败 | 1 项实际 offload 场景明确跳过，不声称旧内核支持 |
| Android API37，x86_64，16KiB 页 | 普通 App UID 10230 的 17 项场景通过，包括 IPv4／IPv6、真实 Network 绑定与 GSO／GRO | 模拟器，不是 ARM64 真机性能结果 |
| OnePlus 13 真机，Android 15／API35，ARM64，4KiB 页 | 普通 App UID 10385 的 17 项场景全部通过，包括 IPv4／IPv6、Network 绑定与 GSO／GRO；SELinux Enforcing | 本机 USB 连接，无 root 或安全策略修改；未覆盖 ARM64 16KiB 页设备 |

Android 的 x86_64／aarch64 均通过无 feature、独立 `udp-gso`、独立 `udp-gro` 和 `udp-offload` 的编译检查；两个 ABI 的 API29 APK 均完成构建、16KiB 对齐及签名验证。ARM64 APK 已在 OnePlus 13 普通 App 进程中执行，通过全部 17 项场景；真机结果、环境、APK 散列和截图保存在 `artifacts/android-usb-arm64-*`。

Linux GNU 的 x86_64／aarch64 release 库已构建，Windows GNU 目标完成编译检查；实际桌面运行来自 Windows MSVC 与 Linux x86_64 musl。Linux 的无 feature、26 个独立 feature 和 14 个组合共 41 组编译检查通过，结果保存在 `artifacts/linux-feature-checks.json`。仅编译 NODEV 时，不可编译的 Auto shared／large-chunk 子项不会激活冲突的硬件 RX 模式；该边界有失败前／修复后原生证据。

Windows／Linux 的 `cargo clippy --all-features --all-targets -- -D warnings`（Linux 指定 musl target）通过。Android 的两处 `thread_local!` 声明针对[上游 #13422](https://github.com/rust-lang/rust-clippy/issues/13422) 已知误报，使用仅限 Android 的 `cfg_attr(..., allow(clippy::missing_const_for_thread_local, reason = ...))` 定点豁免；保留正确的 const 初始化，不修改工具链或全局 lint 级别。aarch64／x86_64 Android 以及 Windows 的严格 Clippy 检查均通过。

两个桌面后端的混合负载均完成 4096 次小 RPC、每方向 16MiB 大块 TCP、每方向 8192 个 UDP 数据报，未用重传掩盖丢包。独立空闲探针保持两个 worker 及已绑定 TCP／UDP，等待约 2 秒：Linux 进程 CPU 时间 5.294ms；Windows `GetProcessTimes` 读数为 0，受计时精度限制，不能解释为绝对零 CPU。

验证总览在 `artifacts/verification-summary.json`。详细证据包括 `artifacts/windows-native-suite.log`、`artifacts/windows-ipv6-evidence.json`、`artifacts/linux-native-results.json`、`artifacts/android-usb-arm64-*`，以及 API29／API37 模拟器结果与截图；`artifacts/android-final-build-evidence.json` 记录模拟器执行 APK 的散列，USB 真机 APK 散列见其环境记录。Linux runner 在其 `--state` 目录的 `runs/<id>/report.json` 和 `serial.log` 保留内核、二进制散列、参数及实际结果，索引明确关联已修复的历史失败与后续通过记录。这些生成物不代替可重跑的测试／示例。

真实硬件 RX ZC、large-chunk DMA、NIC NAPI／RSS 行为及线上吞吐仍需要满足要求且已由部署方配置好的 NIC／队列。NODEV 明确是复制验证；本机未配置此类硬件，也未为验证修改外部主机内核、NIC 或主机安全策略。

Windows Server 2022、Linux ARM64 及 Android ARM64 16KiB 页设备未执行原生场景；Android ARM64 的已验证范围为上述 API35／4KiB 页真机。
