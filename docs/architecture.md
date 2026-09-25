# Rivet 系统与架构设计

## 1. 范围与不可变约束

Rivet 是 Rust 原生网络异步运行时。Linux 基线为 7.2.7，Windows 使用 RIO + IOCP，Android 普通应用使用 epoll 和非阻塞 socket。公开 Interface 使用标准 `Future`，不依赖 Tokio，不提供 TLS、DNS、HTTP、QUIC、RPC 编解码、文件 I/O、用户态协议栈、DMA-BUF 或 BPF 执行。

支持 IPv4/IPv6、TCP 客户端与服务端、UDP connected/unconnected、批量数据报、分段发送、外部 socket 接管以及 Android Network/VPN 接入。广播和组播不在交付范围内。TCP splice 是纯字节流透明转发优化，不是文件 I/O 或协议代理框架。

目标是在相同 CPU/内存预算和尾延迟约束下取得最高持续有效吞吐，同时覆盖小消息、大块 TCP 和高包速 UDP。无绝对性能数字时比较完整吞吐—尾延迟曲线；不得用 opcode 存在、loopback 成绩或 NODEV 接收冒充真实网络零拷贝。

应用不指定 worker 或负载分组。运行时自动分配新任务，网络对象归属创建它的 worker。运行中的 `!Send` 任务不跨线程迁移。空闲必须能够休眠；SQPOLL/NAPI 轮询有明确预算和退出条件。

## 2. 平台支持

| 平台 | 支持基线 | Implementation |
| --- | --- | --- |
| Linux | 7.2.7+；x86_64、aarch64 | 项目内 7.2.7 UAPI、SQ/CQ、资源注册和网络状态机 |
| Windows | Windows 11 / Server 2022+；x86_64 | 注册缓冲区 RIO，IOCP 通知及 overlapped 连接/接受 |
| Android | API 29+；arm64-v8a，x86_64 验证目标 | epoll、非阻塞 socket、libandroid Network 绑定、宿主保护回调 |

Linux 不维护 6.x 兼容 Implementation，不自动退到 epoll。Android 不尝试调用 io_uring。平台通过条件编译选择，不在每次网络操作上进行平台动态分派。

## 3. Module 与职责

采用单 crate，公开库名 `rivet`，包名 `rivet-runtime`。

- `config`：资源预算、功能选择和静态依赖/冲突校验。
- `capability`：编译、环境支持、启用状态和原因；正常数据路径不执行能力探测。
- `buffer`：有界内存池、稳定存储、可写所有权、只读租约、分段视图和外部接收区域回收。
- `runtime` / `task` / `time`：worker、自动投递、本地任务、跨线程安全唤醒、定时器及关闭协议。
- `net`：TCP/UDP 公共 Interface，流顺序、接收等待、在途背压、socket 生命周期。
- `socket`：平台原生句柄所有权、接管错误和建立连接前的配置钩子。
- `driver`：真实平台 Seam。Core 只依赖操作语义，不依赖 SQE/CQE 或 epoll 事件。
- `driver::linux`：ring、UAPI、固定资源、网络请求、ZC、GSO/GRO、splice、NAPI 和 MSG_RING。
- `driver::windows`：RIO RQ/CQ、内存注册、IOCP、AcceptEx/ConnectEx 和 overlapped 生命周期。
- `driver::android`：epoll ready list、recvmsg/sendmsg、数据报批处理及 Android 网络配置。

Driver 是内部 Interface；公开 Interface 不暴露内核队列、buffer ID 或 native completion record。三个平台 Adapter 可有不同的 Implementation，不能把完成式后端强行变成“先模拟 readable，再调用读取”。

## 4. 执行模型

每个 worker 拥有 Driver、就绪任务、定时器、socket/operation 槽位、缓冲区池及有界跨线程入口。任务生成工厂可跨线程传递，Future 在选定 worker 内创建，因此本地 Future 可以是 `!Send`。`spawn_local` 在当前 worker 创建任务；普通任务投递自动选 worker，不暴露负载组。

网络对象继承所属任务的 worker。`TcpListener::serve` 可以在接受完成、尚未开始数据 I/O 时将 socket 所有权交给自动选出的 worker，再创建连接处理 Future；普通 `accept` 保持调用者本地语义。不得暗示单个 UDP 流或任意已有 `!Send` Future 自动获得多核并行。

就绪任务、I/O 完成和定时器轮转处理，均有单轮预算。已存在的工作可以批处理，但不等待凑满批。没有任务或 I/O 进展时，有限轮询后进入平台等待。跨线程通知必须采用 clear/recheck/wait 协议，防止丢失唤醒。

标准 Waker 必须线程安全。不得用 `Rc` 构造可跨线程使用的 Waker，不得通过错误的 `unsafe Send` 迁移本地状态。任务销毁也发生在归属 worker。

## 5. 缓冲区与操作生命周期

### 5.1 租约

- `WriteBuf` 独占可写区域；只有唯一所有权且无内核引用时才能修改。
- `SendBuf` 是稳定内存上的只读拥有型视图，可廉价派生只读范围。
- `ReadBuf`/接收数据块只暴露已初始化且已交付的字节。
- 分段发送持有各段，不为统一 Interface 强制拼接 payload。
- 租约元数据与 payload 尽量复用；稳定收发路径不得逐包分配任务、Box Future 或回收控制块。
- 内存池及在途字节/租约数量都有上限；资源不足时施加背压或明确返回资源错误，不能无限增长。

接收缓冲区的“应用视图引用”和“内核剩余使用权”分别追踪。增量 buffer ring 与 ZCRX 可能让多个数据块共享同一区域；不能把一次 CQE 等同于整个缓冲区已归还。

### 5.2 发送

发送有两个独立事实：

1. I/O 结果可用：接受字节数或错误。
2. 内存引用结束：底层不会再读取该区域。

`send().await` 返回第一项和只读数据所有权，不必等待第二项。Driver 保留内存租约直至真正释放；发送者可以继续推进协议或提交只读剩余区间。普通复制发送可以同时完成两项，不能人为增加一次异步回收。

ZC 的 `F_MORE`/`F_NOTIF` 决定实际通知生命周期，不无条件等待两个 CQE。实际复制回退不等于传输失败，不能因此重复发送。每个 TCP 连接有有序的逻辑发送序列，接收与发送独立，保持全双工。

### 5.3 接收、取消、关闭

接收等待与后台接收请求分离。取消一次接收等待，不丢弃已经进入连接队列但尚未交付的 TCP 字节。UDP 保留边界、来源和截断信息；零长度数据报不是 EOF。

Windows RIO 不为尚未投递接收的 UDP 保证内核排队。UDP 创建／接管在公开返回前提交有界多槽接收窗口，与应用第一次轮询 `recv` 无关。窗口操作、注册元数据与缓冲区全部预先准入；发布额度是绝对值，暂停和取消仍保留已经消费的数据报直到发布或显式关闭。TCP 不预读新接受连接，因此仍可在第一次数据 I/O 前完成自动 worker 放置。

Drop Future 不保证撤回网络效果。未提交操作可撤销；已提交操作继续追踪到终态。关闭 socket 时取消或收敛所有相关请求，处理迟到完成和 ZC 释放通知后再复用操作槽位或注销内存。

普通 TCP 句柄关闭不默认设置 abortive linger：先发起写半关闭，异步收敛请求和发送释放通知，不阻塞 worker 等待对端 ACK。销毁整个 Runtime 则取消尚未结束的业务；Linux 必要时以 socket 级 `SO_LINGER(1,0)` 中止剩余连接，释放 native/fixed 引用，再等待真实内核释放。不得伪造通知或提前释放内存。需要保证业务完整交付时，应用应在销毁 Runtime 前完成 `shutdown(Write)` 与对端协议确认；发送结果本身不是远端交付确认。

操作/连接使用代际 token，且内核仍可引用的槽位绝不复用。所有内核使用的控制结构必须放在稳定存储中。

## 6. 功能选择与能力契约

Cargo features 只决定构建内容。`linux-full` 是编译聚合，不是运行时全开。

显式启用优化默认 `RequireCapability`。`Off` 不要求该项能力；`Auto` 必须显式选择，才允许回到正常路径。报告区分 compiled、supported、enabled、inactive reason，以及可选的实际复制统计。未实现的功能不得被报告为支持。

Linux 基础能力低于 7.2.7，或 io_uring 整体不可用时，初始化失败。ZC `Auto` 可以选择普通 io_uring 数据路径，不能悄悄替换平台后端。`RequireCapability` 要求能力/资源成立，不承诺内核每个请求都不复制。

### 6.1 优化目录

- fixed files、direct descriptors、registered ring、registered wait。
- registered buffers，以及 7.2 普通 SEND/RECV 固定缓冲区路径。
- provided buffer rings、增量消费、批量回收。
- multishot accept/recv/recvmsg，适用的 send/recv bundles。
- SINGLE_ISSUER、DEFER/COOP_TASKRUN、TASKRUN_FLAG、NO_SQARRAY、SQ_REWIND、CQE_MIXED。
- SEND_ZC/SENDMSG_ZC、fixed/vectored 发送、复制回退观测。
- RECV_ZC、区域/refill ring、large chunks、ZCRX export/import/shared。
- 显式 NODEV 复制接收验证模式，不冒充 RX ZC。
- 有限 SQPOLL、NAPI polling、MSG_RING 跨 worker 通知。
- UDP GSO/GRO，保留数据报语义。
- TCP socket→pipe→socket splice，不支持把需要用户态处理的数据自动旁路。

### 6.2 合法组合

- RX ZC 要求 DEFER_TASKRUN、SINGLE_ISSUER，以及 CQE32 或 CQE_MIXED。
- SQPOLL 与 DEFER/COOP_TASKRUN、TASKRUN_FLAG、SQ_REWIND 不能用于同一 ring。
- SQ_REWIND 要求 NO_SQARRAY。
- CQE32 与 CQE_MIXED 不能同时选择。
- 普通 fixed-buffer receive 与同一请求上的 buffer selection/multishot/bundle 互斥。
- 普通 send bundle 与 SEND_ZC 是不同请求路径，不能直接把两组 flags 混入同一 SQE。
- 编译期共存不等于每个请求可以同时使用所有能力。

## 7. 预配置资源与接管

运行时不改全机 RSS、流规则、网卡队列数、sysctl、SELinux 或 seccomp。RX ZC 的网卡和队列由部署方准备，运行时只验证并使用。

外部 socket 使用拥有型平台句柄传入。成功后由运行时负责关闭；失败返回原句柄和错误。导入时验证类型、状态和后端约束，不偷偷重建连接。内部用于新连接初始分配的 idle socket 移交与任意运行中迁移不同；有数据 I/O 的对象不得未经完整收敛就迁移。

Android 允许指定 Network，并提供连接/首次发送前的宿主保护钩子。宿主负责 Android 权限和 Java/JNI 对象生命周期。保护或绑定失败必须阻止继续连接。核心不附带 Kotlin SDK、VPN 应用、TUN 或后台保活机制。

## 8. 验证

- Windows 实际 RIO/IOCP TCP/UDP、IPv4/IPv6、接管、超时、关闭和数据完整性。
- Linux 7.2.7 实际 syscall 与网络路径；分别验证各优化和合法组合。
- 真实 RX ZC 需要相应网卡/驱动；NODEV 仅验证该内核 Interface 的复制接收与回收流程。
- Android 在普通 App 进程内验证，不能用 root/adb shell 成功替代。
- 特性开关不改变字节流/数据报语义；失败、取消和资源压力不产生提前回收、重复发送、丢失唤醒或句柄重复关闭。
- 同资源预算下测量小消息、大块 TCP、UDP、混跑和空闲唤醒；不得捏造未测性能。

现有本机 Windows 可运行验证；WSL 6.18 不满足 Linux 基线，须使用隔离的 7.2.7 环境或用户升级后的主机。不得擅自升级用户 VPS 或更改现有 WSL 全局内核。Android 使用专用验证应用和隔离模拟器，不改变用户现有 VPN 配置。
