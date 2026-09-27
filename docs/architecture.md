# Rivet 系统与架构设计

## 1. 范围与不可变约束

Rivet 是 Rust 原生网络异步运行时。Linux 以 6.18 LTS 为主验证基线，明确覆盖 6.6／6.12 兼容路径，不设置全局内核版本门禁；Windows 使用 RIO + IOCP，Android 普通应用使用 epoll 和非阻塞 socket。公开 Interface 使用标准 `Future`，不依赖 Tokio，不提供 TLS、DNS、HTTP、QUIC、RPC 编解码、原生异步文件 I/O、用户态协议栈、DMA-BUF 或 BPF 执行。同步文件及 FFI 工作可交给有界阻塞执行通道。

支持 IPv4/IPv6、TCP 客户端与服务端、UDP connected/unconnected、批量数据报、分段发送、外部 socket 接管以及 Android Network/VPN 接入。广播和组播不在交付范围内。TCP splice 是纯字节流透明转发优化，不是文件 I/O 或协议代理框架。

目标是在相同 CPU/内存预算和尾延迟约束下取得最高持续有效吞吐，同时覆盖小消息、大块 TCP 和高包速 UDP。无绝对性能数字时比较完整吞吐—尾延迟曲线；不得用 opcode 存在、loopback 成绩或 NODEV 接收冒充真实网络零拷贝。

应用不指定 worker 或负载分组。运行时自动分配新任务，网络对象归属创建它的 worker。运行中的 `!Send` 任务不跨线程迁移。空闲必须能够休眠；SQPOLL/NAPI 轮询有明确预算和退出条件。

## 2. 平台支持

| 平台 | 平台基线与验证目标 | Implementation |
| --- | --- | --- |
| Linux x86_64、aarch64 | 6.18 LTS 主验证线，6.6／6.12 兼容目标；按必要能力初始化，不按版本号拒绝 | 保留 7.2.7 参考 UAPI，按实际能力选择 SQ/CQ、资源注册和网络路径 |
| Windows x86_64 | Windows 10／Server 2016 及后续版本（当前 Rust 目标基线）；不额外校验 OS 版本号 | 注册缓冲区 RIO，IOCP 通知及 overlapped 连接/接受 |
| Android arm64-v8a；x86_64 验证目标 | API 23 及后续版本 | epoll、非阻塞 socket、libandroid Network 绑定、宿主保护回调 |

平台基线、参考 UAPI、验证 runner 的具体 guest pin 和实际运行证据是不同事实。Linux 版本用于逐项优化的保守选择和已知缺陷规避，不构成整个 Runtime 的准入条件；未识别版本和 RC 不因版本字符串直接失败，但不因此获得未经验证的支持承诺。Android 保留 API 下限检查，Windows 按原生能力初始化。公开 Rust `Future` Interface 的兼容性不替代底层 OS/ABI、权限和硬件要求。

Linux 必要 io_uring 接口不可用时返回原生错误或明确的缺失能力，不自动退到 epoll。更旧内核可以尝试必要能力路径，不承诺所有带 io_uring 的版本可用。Android 不尝试调用 io_uring。平台通过条件编译选择，不在每次网络操作上进行平台动态分派。

Windows RIO 的接口引入版本为 Windows 8／Server 2012，但这不是本包的工具链或验证基线。Rust 1.77 的常规 MSVC 目标基线包含 Win8；Rust 1.78 起不再覆盖它。本包要求 Rust 1.98／edition 2024，遵循当前 Windows 10／Server 2016 目标基线，不承诺 Win8。后端不设置 OS 版本／build 门禁，直接初始化 Winsock 2.2、registered-I/O socket、RIO／IOCP 及连接扩展；原生能力缺失或资源失败仍显式报错，不降级为其他后端。

Android 保留 API23 引入的 `android_setsocknetwork`，启动时通过 API23 可用的系统属性接口读取 `ro.build.version.sdk`，拒绝低于 23 或无法判定版本的环境。不能直接强链接 API29 的 `android_get_device_api_level` 再用它判断旧系统版本。验证应用的构建与安装下限同步为 API23；可选 UDP offload 仍按实际内核能力与策略决定。

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

### 3.1 公开 Interface 的兼容契约

`0.2.0` 有意改变 Linux 默认功能选择：默认编译生产优化，未指定项由后端自动选择，并删除全局 Linux 版本门禁及其公开管理入口。配置／结果结构、优化枚举和原生 trait 的形状保持不变。新的 `0.2.x` 兼容基线保留源码和已承诺的行为兼容；以后有意破坏兼容时，`0.y.z` 提升次版本，`1.0` 及以后提升主版本，并给出迁移说明。支持策略不冒充所有平台路径都已完成原生验证。

兼容范围不只包括方法名称和参数，还包括：

- 拥有型缓冲区、发送结果的输入字节计数、`send_all` 的未接受后缀、发送分组以及内核租约的实际释放时机；取消等待不丢弃排队接收，取消已提交发送不撤销输出。
- 网络对象和本地任务的 worker 归属、`!Send` Future／输出及工厂的现有泛型约束、句柄 Drop 取消和显式 detach；计时器保持首次 poll 绑定、reset 和原生错误分类。
- flush 和写半关闭的惰性、排空责任、上下文校验及半关闭后仍可读取反向响应；数据报保留边界、空报文和来源等元数据。
- 开放 trait 的下游实现要求。兼容版本不增加必需方法或更强的类型约束；新增独立能力使用独立 trait，不补入 `dyn`、统一 `Send` 或跨运行时承诺。

公开配置／结果结构的字段、构造方式和枚举穷尽匹配规则保持现状，不添加 `#[non_exhaustive]`，也不改用 builder。它们同样属于公开契约；以后新增字段或枚举成员若破坏合法的下游构造／匹配，必须按破坏性变更处理，而不是称为内部优化。

`sync` 直接重导出的 `async-channel` 2.x、`async-lock` 3.x、`futures-channel` 0.3.x 类型属于公开依赖，不是可随意替换的 Implementation。类型身份、公开方法／泛型约束、错误及取消／关闭行为都计入兼容评估；升级、替换或改变这些依赖的 feature 必须检查下游编译和相关行为回归。不为此增加一层包装，也不承诺每个补丁版本都不更新。

验证分为三类：外部消费编译检查公开查询与依赖类型互操作，外部编译拒绝后端管理入口，现有行为回归及真实 `native_traits` 场景检查生命周期契约。编译检查不替代实际运行，Windows／Linux／Android 各自的原生证据不能互相代替。

## 4. 执行模型

每个 worker 拥有 Driver、就绪任务、定时器、socket/operation 槽位、缓冲区池及有界跨线程入口。任务生成工厂可跨线程传递，Future 在选定 worker 内创建，因此本地 Future 可以是 `!Send`。`spawn_local` 在当前 worker 创建任务；普通任务投递自动选 worker，不暴露负载组。

网络对象继承所属任务的 worker。`TcpListener::serve` 可以在接受完成、尚未开始数据 I/O 时将 socket 所有权交给自动选出的 worker，再创建连接处理 Future；普通 `accept` 保持调用者本地语义。不得暗示单个 UDP 流或任意已有 `!Send` Future 自动获得多核并行。

就绪任务、I/O 完成和定时器轮转处理，均有单轮预算。已存在的工作可以批处理，但不等待凑满批。没有任务或 I/O 进展时，有限轮询后进入平台等待。跨线程通知必须采用 clear/recheck/wait 协议，防止丢失唤醒。

标准 Waker 必须线程安全。不得用 `Rc` 构造可跨线程使用的 Waker，不得通过错误的 `unsafe Send` 迁移本地状态。任务销毁也发生在归属 worker。

任务放置的负载快照只是候选提示。候选 worker 的活跃状态复核、资源准入和工厂入队使用同一 inbox 锁，与 worker 0 退出 `block_on` 的失活转换串行化；候选失活或满额时，在一次有界扫描内尝试其他 worker。不能因一个候选失活而忽略仍可用的后台 worker，也不能在锁内销毁用户工厂。

任务生命周期从尚未调用的工厂开始。取消和 Runtime shutdown 均在所属线程销毁捕获值，并隔离其析构 panic；一个工厂清理失败不能阻断后续任务清理或泄漏准入额度。

Linux Driver 区分“等待完成或资源”和“SQ 满导致尚欠提交”。只有后者要求继续非阻塞推进，包括数据操作、取消和唤醒请求；额度不足、缓冲区不足和未到重试期限不能因此变成无限自旋。

## 5. 缓冲区与操作生命周期

### 5.1 租约

- `WriteBuf` 独占可写区域；只有唯一所有权且无内核引用时才能修改。
- `SendBuf` 是稳定内存上的只读拥有型视图，可廉价派生只读范围。
- `ReadBuf`/接收数据块只暴露已初始化且已交付的字节。
- 分段发送持有各段，不为统一 Interface 强制拼接 payload。
- 租约元数据与 payload 尽量复用；稳定收发路径不得逐包分配任务、Box Future 或回收控制块。
- 内存池及在途字节/租约数量都有上限；资源不足时施加背压或明确返回资源错误，不能无限增长。
- 接收队列、接受队列和完成事件队列的容量在配置阶段按实际元素布局检查；不可寻址的数组返回 `InvalidInput`，不延迟到 worker／socket 创建时 panic。这不承诺任意可寻址预算都能由当前机器成功分配。

`WriteBuf::as_mut_slice()` 只借出 `0..initialized_len()` 的可变字节视图，不改变长度、容量或初始化状态，不分配或复制。未初始化尾部仍只能通过 `spare_capacity_mut()` 初始化。只读租约必须先通过 `try_into_write()` 的唯一所有权检查；外部区域及尚有应用／内核别名的租约不能借此变为可写。该 Interface 将原地编解码的安全证明集中到 buffer Module，而不是让消费者从裸指针自行构造切片。

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

### 5.4 Windows UDP 容量规划

`Limits` 配置复制到每个 worker，但 `max_pending_receives = N` 是每 socket 的逻辑接收队列上限；Windows UDP 还为每个 socket 预留 N 个 RIO 接收 lane。TCP 的同名队列上限不代表并行投递 N 个原生接收。worker 的 payload pool、distinct lease 槽和 operation 槽则由其全部 socket 共享。

单个 UDP socket 的初始 RX payload 需求为 `N × max(receive_chunk, pool.block_size)`，每 lane 使用一个 distinct lease，不按 block 数拆成多个 lease。`Limits::windows_udp_receive_bytes(receive_chunk)` 提供有检查的纯算术计算，在所有平台可用于 Windows 部署规划；零／非法接收尺寸、零窗口／block 及乘法溢出返回 `InvalidInput`。结果允许超过当前池预算，以便调用者比较和调整；它不创建资源、不预留额度，也不保证原生初始化成功。

同一 worker 的 U 个同尺寸 UDP socket 至少需要 `U × N × max(receive_chunk, pool.block_size)` payload、`U × N` distinct leases 和 driver native operation 槽；Core 另需 U 个逻辑 receive operation，两个 operation arena 分别受 `max_operations` 限制，不能合并计数。RIO RQ 的额外接收预约为 `U × (N - 1)`。异尺寸 socket 按各自需求求和。上述初始占用不包含 TX、TCP、metadata、待交付完成、关闭收敛或上层持有旧块时的替换块。

例如 N=8、chunk=64 KiB、block=16 KiB、U=4 时，初始 RX 为 2 MiB／32 leases／32 native operations，另有 4 个 Core receives；若上层仍保留完整旧窗口，同时要求挂满新窗口，RX 规划需再留 2 MiB／32 leases。池总空闲字节充足仍可能因碎片缺少连续 extent，且池预算不等于进程 RSS。

继续复用已有 bind/import 准入和回滚；错误补充窗口规模及资源类别，但保留 `WouldBlock`、`InvalidInput` 和原生错误身份。不得通过静默缩小窗口、扩池或提高默认槽数掩盖配置问题。未投递接收时 RIO 不保证普通 UDP 式的内核排队，`receive_buffer_bytes` 不能替代注册接收窗口。窗口耗尽时仍可能丢包；预算公式不是吞吐或无丢包承诺。静态预算计算不创建动态快照；运行中的资源观测遵守下一节的独立只读契约。

### 5.5 按需资源诊断

资源诊断只回答“占用了什么、在哪个所有权阶段等待”，不判断泄漏／丢包，不自动调整容量。公开 `diagnostics` Module 保存独立的 `PoolUsage`、`WorkerResources`、`DriverResources`、`ReceiveResources` 和 `RioReceiveResources` 值类型；字段对消费者不可写，通过只读 getter 查询，类型不持有 payload、句柄或 Runtime 引用。保留既有 `Limits`／`ZcStats`／结果结构、枚举及 trait 的构造和实现义务，不添加兼容别名或必需 trait 方法。

三个查询 Seam：

- `BufferPool::usage() -> PoolUsage`：无需运行时上下文，即使 Runtime 已销毁也能查询保留的池。普通 arena 的容量、占用、空闲与最大连续空闲 extent 单独计量；distinct lease 容量／占用／可用包含 normal 和 external 租约以及等待 provider 接受的回收。别名不重复收费，payload 统计使用 allocation 容量，不是初始化或可见字节数；外部 backing 不计入普通 arena 字节。查询不调用 recycler。
- `runtime::resource_snapshot() -> io::Result<WorkerResources>`：当前 worker 的身份、后端名、Core socket／operation 占用及可用槽和上限、排队 receive／accept 结果、Core send-byte 准入预算、池和 driver 快照。没有运行中 worker 时保持 `NotConnected`；I/O 分发或回调重入导致不可变借用不可用时返回 `WouldBlock`，不 panic。不提供隐式全 Runtime 汇总，不把自动 spawn N 次当成逐 worker 遍历。
- `UdpSocket::receive_snapshot() -> io::Result<ReceiveResources>`：沿用 socket 的 owner／存活检查，报告该 socket 的队列容量／占用、waiter、逻辑接收是否活跃、Core 额度更新是否待提交、后端当前发布额度以及可用的 native／RIO 细节。Core 队列余量和 driver 已获额度可能暂时不同；查询绝不为消除差异而 flush。

`DriverResources` 统计保留的 socket／operation 槽、实际可用槽、软件持有的完成记录、关闭后仍保留的 socket，以及可用时的 native outstanding／retiring operation 数。后两者计 operation record，不计裸 SQE/CQE、唤醒或取消控制请求。Linux 包含仍有 native 操作或零拷贝 guard 的记录；Windows 使用尚未收割完成的 RIO／overlapped 状态，包含 deferred commit；“retiring”是其中已经停止／取消或 socket 关闭的子集。Android readiness 没有这种跨异步调用的 native 内存引用，两项返回 `None`，不能以零冒充同一后端模型。软件快照不是内核／NIC 瞬时可用接收数量。

Windows 单 UDP socket 的 RIO 分区报告已准入 lane、完成后待发布的数据、空闲 lane、最近一次补挂因 pool 不足而暂停的 lane、累计 rearm allocation failures、commit pending 和 stopping。`DatagramReceive.pending` 同时包含 native 未完成及已完成未发布的工作，不能直接命名为 posted receives。pool-blocked 标记只表示最近一次尝试，资源刚归还但尚未再次补挂时可以仍为真；它在该 lane 成功获得可写存储时清除。仅真实分配 `WouldBlock` 分支递增本地饱和 `u64`，分别保留 socket 生命周期计数和 worker 累计计数；不是丢包数，也不是不同数据包的失败数。其他后端的 RIO 分区及计数为 `None`。

成功查询只读现有 owner-local 状态：不分配、复制 payload、持有全局锁、增加逐包原子计数或调用 OS；不 poll、回收、试分配、重投、消耗 credits 或唤醒任务。池扫描 free extents，worker 扫描有界 socket／operation 表，单 RIO socket 只遍历自己的 lane；成本按需发生，不维护成功 I/O 的镜像统计。查询返回值可跨线程传递，但不因此使 Runtime／socket／lease 可迁移。跨 worker 采集不是同一时刻的原子视图，时间戳、序列化、展示和告警属于调用方。

验收围绕可见转移：字节足够但 lease 耗尽、总空闲足够但连续 extent 不足、provider 拒绝回收、旧 RX 租约阻止补挂及释放后恢复、已排队 waiter 取消、关闭后的 native 退役、join 结果继续拥有租约，以及重入查询明确失败。查询前后资源和交付行为必须相同，重复成功查询必须无分配。Windows 原生回环与各平台编译／原生证据分别记录，不用静态检查冒充 native 生命周期证明。

关闭轮询可能在同一轮软件 service 中释放最后一个尚未投递的窗口，且不生成应用完成事件。Windows 在 shutdown 期间必须于进入 IOCP 等待前再次确认 driver 是否已经 idle；已收敛时只做非阻塞收割，不能等待一个再也不会到达的完成。该检查不把正常 idle 运行改成自旋，也不伪造 native 完成或释放。

## 6. 功能选择与能力契约

Cargo features 只决定构建内容，默认集合为 `linux-full`，不包含 NODEV。`default-features = false` 保留精简构建及完整基础网络语义；`linux-full` 不表示运行时全开，也不要求运行内核支持每个已编译优化。Windows／Android 的运行时默认策略保持不变。

Linux 的配置表保存调用者覆盖：未指定项继承均衡、空闲可休眠的自动方案；显式 `Off` 禁止该项，`Auto` 允许该优化不可用，`RequireCapability` 必须成功或返回结构化错误。`enable` 仍表示严格要求。`policy`／`requested` 查询配置中的请求，不能预测内核决策；生效结果从 `Runtime::capabilities()` 读取。静态正规化只解决显式请求及其依赖，Linux 后端再补入不与这些请求冲突的默认候选。

自动选择综合已编译实现、版本与已知修复条件、实际 feature bits／opcode／注册结果、资源及硬件配置。版本信息缺失时保守禁用没有可靠独立探测方式的新 modifier，不阻止完整基础路径。报告区分 compiled、supported、enabled 和 inactive reason；不能把注册成功或 opcode 存在作为其他字段和组合可用的证明。`RequireCapability` 不承诺每次 ZC 发送都不复制，优化回退也不能更换平台后端。

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
- 默认候选不能重新开启显式 `Off`，也不能为了补齐依赖绕过它。两个显式互斥请求仍报配置错误，包括两者都为 `Auto`；自动方案在填入候选之前消除与显式请求的冲突。
- 注册内存是内部资源，不等于普通 fixed SEND/RECV 优化。`zc-tx-fixed` 的运行时依赖是 ZC TX 和实际注册资源，不再强制开启 `registered-buffers` 所代表的普通 fixed 收发；Cargo 依赖仍负责纳入共享实现。
- fixed＋vectored ZC 必须验证具体 modifier 组合，且一次请求的全部非空段位于同一个注册区域。未获证据的组合不提交到业务 socket 试错，不重发已经可能产生网络效果的请求。

### 6.3 能力报告的公开边界

能力报告由原生后端在 Runtime 初始化期间生成。应用通过 `Runtime::capabilities()` 取得各 worker 的报告，读取现有元数据字段以及 `CapabilityReport::{states, state, enabled}`，并通过 `Optimization::{ALL, name, compiled}` 识别构建内容。`CapabilityError` 继续公开优化项身份和失败原因；报告查询与严格请求的错误身份不因内部表示变化而改变。

`CapabilityReport::{new, decide, finish}` 仅供 crate 内部构造和完成能力探测，收为 `pub(crate)`。没有调用者的 `enabled_mask` 删除，`Optimization::bit` 收为 `pub(crate)`；内部启用位图不作为外部查询或持久化协议。应用按优化项查询，不自行构造后端能力证据。现有报告元数据字段、配置和结果结构的构造方式保持不变。

能力决策回归随实现放在 `capability` 的单元测试中，不为了集成测试重新开放管理入口。外部消费验证通过真实 Runtime 查询报告；Windows、Linux 和 Android 的内部创建流程继续共用原有决策逻辑，不增加包装、探测或数据路径开销。

### 6.4 初始化决策与资源

每个 worker 在初始化期间确定不可变的数据路径能力。`config` 验证调用者意图并补入合法默认候选，`driver::linux` 结合实际内核证据决定 ring 模式和具体收发路径，`capability` 记录结果；不增加面向调用者的内核版本 profiles、动态 Driver 或逐请求能力探测。

默认方案不启用 SQPOLL／NAPI 忙轮询，它们保留显式选择。跨 worker MSG_RING 仅在有多个 worker 时成为默认候选。只有已配置网卡队列时才考虑硬件 ZCRX；共享模式还要匹配队列拓扑，较大接收块遵守显式块大小配置。NODEV 始终显式选择，不能作为硬件 RX 的自动替代。没有扩展完成项需求时，不为版本支持而默认选择 mixed CQE。

先选择路径，再申请它需要的可选资源。资源和队列继续有界；自动初始化失败沿既有所有权清理流程收敛，不能泄漏注册或把仍持有失败注册的 ring 继续用于普通 I/O。正常路径只读取已确定的能力和请求形态，不执行版本解析、探测、逐包分配或 payload 拼接。

启用增量接收时，TCP 使用增量 provided ring，UDP 使用独立的普通 provided ring；两者分摊同一个有界 payload slab，保留全局 buffer ID、租约和回收队列，不额外复制或翻倍 payload 预算。UDP 不能使用小于其接收容量的 TCP／共享增量尾部，否则合法数据报也会被内核截断。两组描述符各自保持二次幂容量，回收按原始组路由；预算不足以建立两组时，Auto 回到原单组普通接收，严格请求明确失败。

### 6.5 跨版本基础路径与验收矩阵

- 普通分段发送在旧内核使用 io_uring SENDMSG，具备对应能力时使用 SEND_VECTORIZED；复用稳定消息结构和 iovec 存储，不改变发送顺序、已接受字节计数或未接受后缀。
- multishot receive 对旧内核使用其支持的长度规则，具备 per-invocation cap 的内核才写入非零上限。额度、取消、迟到完成和缓冲区回收语义不随版本缩水。
- 普通 fixed SEND/RECV、fixed ZC 和内存注册分别判定。incremental 当前保守采用已确认的 7.2.7 级语义，UDP 不使用增量尾部；ZCRX control／大小／事件、SQ_REWIND 等保留逐项条件，不反向抬高基础运行门槛。
- 6.6／6.12／6.18 分别执行默认优化、精简构建、显式关闭、严格请求、不可用路径和互斥配置；新内核 guest 保留高级路径验证。参考 UAPI 不随最低验证目标降级。
- 原生验收覆盖多段发送、UDP 空包／来源／截断、multishot 背压、关闭和真实内存释放，并运行实际消费者示例。硬件 ZCRX、普通用户权限和不同架构的证据单独列出；loopback、NODEV、交叉编译不能替代它们。

## 7. 预配置资源与接管

运行时不改全机 RSS、流规则、网卡队列数、sysctl、SELinux 或 seccomp。RX ZC 的网卡和队列由部署方准备，运行时只验证并使用。

外部 socket 使用拥有型平台句柄传入。成功后由运行时负责关闭；失败返回原句柄和错误。导入时验证类型、状态和后端约束，不偷偷重建连接。内部用于新连接初始分配的 idle socket 移交与任意运行中迁移不同；有数据 I/O 的对象不得未经完整收敛就迁移。

Linux 的 UDP 分段元数据解析属于数据报语义，不属于可选的优化启用策略。接管时保留已有 GRO 设置及队列，所有构建均解析内核返回的 `UDP_GRO` 元数据并还原数据报；`udp-gro` feature 与策略只控制运行时主动启用该优化。

Android 允许指定 Network，并提供连接/首次发送前的宿主保护钩子。宿主负责 Android 权限和 Java/JNI 对象生命周期。保护或绑定失败必须阻止继续连接。核心不附带 Kotlin SDK、VPN 应用、TUN 或后台保活机制。

## 8. 通用宿主能力设计

以下设计先于实现确定。目标是让应用适配 Rivet，而非模拟 Tokio；网络 worker、socket 和缓冲区继续保持本地所有权。协议、路由、网络代际、启动事务和业务排空顺序仍属于应用。

### 8.1 阻塞执行

Runtime 拥有独立阻塞池；`RuntimeConfig` 指定线程上限和排队上限。`Handle::spawn_blocking` 与当前 worker 的 `runtime::spawn_blocking` 接收 `Send + 'static` 闭包，返回可异步等待的拥有型任务句柄。池按需启动，不为每个调用创建线程；满额和停止均显式拒绝，不静默产生无界队列。

取消区分排队与运行：尚未执行的闭包可取消；开始运行的同步代码不能安全强杀。丢弃等待者不能提前释放仍运行工作的额度或关联资源。执行 panic 转成任务失败，取消期析构 panic 则隔离并保留 `Cancelled` 分类，不能杀死执行通道。关闭先停止准入并取消尚未开始的工作，再回收异步 worker，最后等待所有已开始阻塞工作及其线程真正结束。阻塞代码不得依赖在 Runtime 销毁之后继续执行的异步任务；库不提供会暂停本地网络 worker 的 `block_in_place`。

### 8.2 非 socket I/O

`io` Module 为每个 Runtime 管理有界原生注册表，独立于 TCP/UDP driver。Linux/Android 提供拥有型 `AsyncFd`：仅接管可轮询的非阻塞描述符，分别等待读/写 readiness，通过同步 `try_io` 闭包执行实际操作；`WouldBlock` 清除相应 readiness 后重新等待。Windows 提供拥有型 `AsyncHandle`，异步等待原生可等待对象；不把普通文件或任意 HANDLE 冒充 socket 或异步文件。

注册返回前验证容量和原生句柄；失败返还所有权。事件使用不可复用的代际身份，避免迟到通知命中复用的描述符。取消等待不关闭描述符，也不丢掉尚未消费的 readiness；关闭或 Runtime 停止注销原生等待并唤醒等待者。注册表按需使用共享原生等待设施，不为每个 FD 创建线程。Unix 非 socket readiness 的专用等待路径不是 Linux TCP/UDP 的 epoll 回退。

### 8.3 TCP 中止

`TcpStream::abort` 消费连接，先选择 abortive close，再经现有关闭路径收敛请求。正常 Drop 和 `shutdown(Write)` 的语义不变。设置失败必须可观察；即使中止连接，已提交发送的存储也保留至真实内核释放。三个网络后端均实现这项 TCP 语义，包括 Linux direct descriptor 路径；不向调用方泄露可随意关闭或长期持有的裸句柄。

Android 的中止顺序为零 linger → `connect(AF_UNSPEC)` 断开底层 TCP → 现有 Driver close 路径。接管拥有型 FD 不代表底层 socket 没有其他别名；中止不能等待宿主释放最后一个 `try_clone` 句柄。该断开语义与 Linux 一致，仍由各平台 Adapter 管理操作回收，不引入新的公开 Interface 或影响正常关闭。

### 8.4 任务组和受监督连接

`runtime::TaskGroup` 有明确容量，接收自动放置工厂或本地 Future，拥有所有子任务句柄。支持等待任意完成、统一 abort，以及可重试的异步 shutdown；取消一次 join/shutdown 等待不丢失其余任务的回收责任。组 Drop 请求取消，只有实际 join 才证明子任务已经终止。可克隆的 abort 控制不转移结果接收权。

空组的 `join_next().await` 立即得到 `None`，不等待未来的 spawn，也不关闭任务组准入；以后仍可 spawn 并创建新的 join Future。动态 supervisor 在空组时等待外部准入／停止通知，不能对 `None` 忙循环或重复 poll 已完成的同一个 Future。普通 Future 不承诺 Ready 后继续可 poll；显式融合类型及 `Sleep::reset` 按各自契约复用，不强加统一 panic 义务，也不自动融合来隐藏 owner 状态错误。

abort／Drop 是取消请求；join 证明子任务 Future／captures 的析构已经完成，不证明返回值中的资源已释放，更不证明 socket 的 native 引用已退役。Runtime 关闭另行推进 driver 到真实收敛，已发布的只读租约仍可跨 Runtime 存活。示例沿用 `runtime_services`，展示空组再次接纳和取消后 join；原生回归保留既有 burst／retirement 证据，并以唤醒或握手确认排队接收与背压状态，不用固定睡眠冒充这些状态。

`TcpListener` 的受监督服务 Interface 在业务名额可用后才轮询下一次 accept，转交 idle socket 后把 handler 纳入任务组。停止信号优先于新准入；已有连接先获得协作取消通知并在宽限期内排空，超时才 abort 并 join。handler panic 和原生接管失败不能隐藏在 detached 任务中。业务名额与 driver 的有界预接受队列分别计数。简单 `serve` 也使用受监督实现，不再遗留无主 handler。

### 8.5 执行器无关的同步

`sync` Module 选用成熟、执行器无关的有界 channel、oneshot、Mutex、RwLock 和 Semaphore 实现，不重写其竞争算法。补充 watch 最新值广播、Notify 和协作 `CancellationToken`。所有等待通过标准 Waker；注册等待与再次检查状态必须闭合丢失唤醒窗口。channel 背压、关闭后排空、watch 合并更新、Notify 单个保留许可与取消广播分别具有明确契约。同步原语不需要当前 Runtime，也不把 `!Send` payload 伪装成可跨线程数据。

### 8.6 定时器

`Sleep::reset` 在已注册的索引堆槽中更新 deadline，不累积旧节点；完成后的 Sleep 可重新启动。周期 `Interval` 使用单个 Sleep，返回计划 tick 时刻，取消等待不消费 tick；零周期和时间溢出显式报错。错过 tick 的策略显式区分补发、跳过和从当前时间延后，不用忙循环追赶。已有 timeout 的“同时就绪时操作优先”不变。

### 8.7 宿主信号

`signal` Module 显式订阅宿主退出事件：Unix SIGINT/SIGTERM，Windows Ctrl+C/Ctrl+Break。无订阅时不安装处理器；订阅释放后解除本次注册，不吞掉宿主后续默认行为。信号回调只做平台允许的标记/通知，不分配、不运行应用代码。等待可取消，重复事件允许合并，停止订阅必须回收原生等待和辅助线程。该 Module 只发出事件，不主动退出进程或规定应用的关闭顺序。

Unix 仅接管默认／忽略 disposition，遇到现有自定义 handler 返回 `AlreadyExists`；宿主负责把其他 `sigaction` 修改与订阅构造／销毁串行化。Windows 订阅期间不更换 console。原生 handler 使用在途读者协议保护通知句柄的关闭／复用；dispatcher 在注册锁外唤醒。自定义 Waker 若在 dispatcher 自身释放最后订阅，停止标记保证回调返回后退出，不尝试 join 自身。

每个 `recv(&mut self)` 订阅只有一个并发等待者。订阅的 pending 位保留尚未消费的信号事实，`AtomicWaker` 只保存唤醒提示；接收使用检查／注册／再检查协议，完成或取消时注销等待。dispatcher 发布 pending 后在所有注册锁和等待者同步状态之外调用 Waker，允许回调同步销毁等待 Future 及最后订阅。复用现有 futures 依赖，不为信号增加任务队列、线程或多等待者事件结构。

### 8.8 验收

验证必须覆盖真实阻塞任务及其排队/运行取消、真实 pipe 或 waitable event、真实 TCP RST、跨 worker 任务组与服务排空、同步竞争和定时器重置/错过 tick。进程信号只在隔离子进程中触发，不能向开发宿主发送退出信号。Windows 原生执行及隔离 Linux guest 执行分别提供证据；Android 编译和普通 App 场景不以桌面结果代替。

### 8.9 面向上层库的原生能力 Interface

#### 8.9.1 范围与职责

上层代理／协议库通过开放 trait 使用 Rivet 的网络、任务执行和计时能力，不绑定具体 socket 类型。该 seam 是 Rivet 原生 Interface，不是跨运行时兼容层：缓冲区、发送结果、任务句柄、计时器和错误直接复用已有类型。原生具体方法仍可直接使用，Driver、资源准入、worker 放置和回收机制不另建 Implementation。

`Connector`／`Acceptor` 的定义与 Implementation 留给需要它们的上层库；域名处理、路由、认证、代理链、协议握手和会话元数据同样属于上层。Rivet 保留原生 connect/bind/accept 和 `serve_until`：新连接先完成 worker 放置，再进行上层握手；已使用的 `!Send` 连接不得借 trait 任意跨 worker 迁移。该设计不引入业务 Handler 框架或 TLS/DNS/代理协议 Implementation。

| Module | Interface | 原生 Adapter |
| --- | --- | --- |
| `net` | `StreamRecv::recv` | `TcpStream` |
| `net` | `StreamSend::{send, send_all, flush}` | `TcpStream` |
| `net` | `StreamShutdown::shutdown_write` | `TcpStream` |
| `net` | `DatagramRecv::recv` | `UdpSocket` |
| `net` | `DatagramSend::{send, send_to}` | `UdpSocket` |
| `runtime` | `LocalSpawn::spawn_local` | `Current` |
| `runtime` | `Spawn::spawn`、`BlockingSpawn::spawn_blocking` | `Handle` |
| `time` | `Timer::{sleep, sleep_until}` | `runtime::Current` |

#### 8.9.2 收发、排空与关闭

网络异步方法使用 `&self` 和返回 `impl Future` 的静态分发 Interface，不要求 `Send`／`Sync`／`Unpin`，不装箱每次操作。`StreamRecv` 返回 `io::Result<Option<ReadBuf>>`，`None` 是 EOF；数据报返回 `io::Result<Received>`，空报文不是 EOF，保留来源、截断及可用的原始长度。收发方向独立推进；一个接收方向只有一个活动等待者，冲突返回 `WouldBlock`。上层包装不得跨网络等待占用会阻断另一方向的整连接锁。

`send` 返回原始 payload 和接受字节数；`send_all` 返回未发送后缀，成功时后缀为空。上层编码／加密包装也必须按调用方输入字节计数，不能返还密文字节数，不能既保存待发送数据又把同一部分作为未接受数据返还。原生 `send_all` 直接复用已有发送分组，不用普通 `send` 循环替换；顺序以实际进入发送序列为准，不以 Future 构造顺序为准。

发送成功允许包装层保留已接受数据，因此 `flush` 是发送 Interface 的必要方法：排空本层已接受数据并完成下层相应的排空，不代表对端收到，也不代表内核释放内存。原生 TCP 没有额外发送缓存，`flush` 首次 poll 时验证 socket 所属 worker 和存活状态即可完成；不能用无条件成功掩盖错误上下文或已停止 Runtime。调用方须先完成自己的发送，再 flush／关闭；不承诺等待与它并发的新发送。

`shutdown_write` 是惰性的异步写半关闭：构造或丢弃未轮询 Future 不产生关闭副作用。包装层须排空自身已接受数据及协议尾部，再关闭下层写方向。成功后不能继续发送应用数据，但接收方向仍可推进；代理一个方向 EOF 不能立即终止反向响应。TCP Adapter 首次 poll 执行原有 `shutdown(Write)`；不模拟额外关闭状态，不改变原生错误。整体 Drop、立即 abort 和有序半关闭分开，Drop 不承诺数据交付。

取消接收等待不丢弃尚未交付的排队数据；丢弃已提交发送 Future 不撤销发送，也不能整体重试原始数据。内核仍使用的租约继续受原生回收机制保护。`BufferPool` 由调用方显式传入编码逻辑，不增加通用分配器 trait，不逐包创建池。trait 层本身不增加 payload 复制；这不承诺上层变换操作没有自身必要的存储成本。

#### 8.9.3 执行能力与计时

`Current` 是可复制、无状态的当前上下文入口，不持有 Runtime 或 worker 身份，不延长其生命周期。`LocalSpawn` 在调用时选择当前 worker，Future 和输出可为 `!Send`，但均保持现有 `'static` 要求。`Handle` 实现的 `Spawn` 接收 `Send + 'static` 工厂，在目标 worker 内产生可为 `!Send` 的 Future，输出仍须 `Send + 'static`；`BlockingSpawn` 保留独立阻塞池及其准入、排队取消和不可强杀运行中闭包的语义。

任务直接返回原生 `JoinHandle`／`BlockingJoinHandle` 和错误；不引入通用 join/abort trait，不改变 Drop 取消、显式 detach 或任务组所有权。`Timer` 返回原生 `Sleep`，保留 `reset` 和取消释放额度的能力；`sleep` 的溢出处理与 `sleep_until` 的首次 poll 绑定均沿用原生 Implementation。`Current` 不把已绑定 Sleep 转移给另一个 worker。现有 timeout、interval 和错误分类继续复用，不添加虚拟时钟或第二套调度系统。

#### 8.9.4 组合与验收

上层有限出站集合优先使用枚举和静态分发，本 Interface 不承诺 `dyn` 兼容，不默认引入类型擦除。真实 socket 地址和代理会话元数据由上层携带，不要求所有包装流暴露原生句柄或伪造 socket 地址。批量 UDP、splice、GSO 等仍使用已有独立能力，不塞入基础 trait 或静默降级。

验收使用真实回环代理：泛型双向转发、请求写半关闭后仍返回响应、flush 上下文错误、未轮询关闭无副作用、数据报空包和元数据，以及本地任务、自动放置工厂、阻塞执行和可重置计时。已有取消、发送分组、额度与回收回归继续执行。宿主使用 TUN 且阻断 IPv6 时，验证进程设置 `RIVET_VERIFY_IPV4_ONLY=1`，仅运行 IPv4 回环并明确记录 IPv6 未执行；不更改 TUN、路由、代理或 IPv6 阻断，也不让生产库读取这个验证开关。

### 8.10 出站 TCP 显式本地绑定

#### 8.10.1 Interface 与范围

新增 `TcpStream::connect_from(local: SocketAddr, peer: SocketAddr, options: SocketOptions) -> Connect`，明确本地 IP／端口，不把本地地址放进 TCP／UDP／listener 共用的 `SocketOptions`。本地端口 `0` 表示由系统分配；指定非零端口时不得悄悄改为其他端口。`connect`／`connect_with_options` 继续表示未显式选择本地地址，并使用同一个拥有型 `Connect` Future。

构造 Future 不创建 socket、不运行 hook、不执行 bind；首次 poll 才在当前 worker 验证选项及本地／对端地址族。族不一致返回 `InvalidInput`，在原生 socket 创建和宿主 hook 之前停止。真实 bind 错误直接向调用方传播，不以默认源地址重试，不改宿主接口或路由。UDP 的 `bind_with_options(local, ...)`／`bind_connected(local, peer, ...)` 和 listener 的绑定参数仍是其唯一地址来源；接管已有 socket 不重绑。

#### 8.10.2 数据流与原生绑定顺序

`Connect` 保存一个 `Option<SocketAddr>`，经 `runtime::io::Core::connect` 传给平台 `Driver::connect(token, peer, local, options)`；`None` 保留原有行为，`Some` 只要求一次显式原生 bind，不增加任务、队列、分配或另一套建连 Implementation。

| 平台 | 未指定本地地址 | 指定本地地址 |
| --- | --- | --- |
| Windows | socket 配置／hook → 通配地址 bind → ConnectEx | socket 配置／hook → 指定地址 bind → ConnectEx |
| Linux | socket 配置／hook → io_uring connect，不提前 bind | socket 配置／hook → 指定地址 bind → io_uring connect |
| Android | protect／宿主 hook → Network／socket 配置 → connect | protect／宿主 hook → Network／socket 配置 → 指定地址 bind → connect |

显式绑定调用中的 hook 只负责保护、接口设置等宿主配置，不再调用 bind 或 connect。Windows 不再在指定源地址之后另做通配地址绑定。Linux／Android 的无源地址路径不增加 bind，保留内核原有的自动源地址和端口分配时机。完成后的 `local_addr` 与对端观察的来源必须反映实际内核端点。

#### 8.10.3 失败、取消与资源

绑定发生在 socket 发布及原生 connect 提交之前。普通 FD／Windows socket／Android FD 在失败时通过既有所有权回收；Linux direct-descriptors 在创建阶段已经占用固定描述符槽，bind 失败还必须注销该槽，不能只关闭临时导出的 FD。Core 在 Driver 同步拒绝时释放连接操作额度。错误不发布半初始化连接，不影响后续正常准入。

已提交连接仍使用原有代际 token、取消、完成处理和关闭回收路径；未轮询 Future 的丢弃没有原生副作用。已绑定后取消不承诺立即复用固定源端口，内核可能仍在收敛请求或保持 TCP 状态；应用不能绕过真实完成重用原生资源。

#### 8.10.4 验收与环境约束

真实回环验证源 IP／端口及数据传输；错误地址族须在 hook 之前拒绝，未轮询取消不能产生绑定，已占用源端口必须失败且不能回退，低容量下重复失败后仍能成功建连。Linux 普通、fixed-files、direct-descriptors 分别选择；不可用的严格模式须报告对应能力，不能让默认自动选择把各行变成同一条路径。Linux 多内核隔离 guest 执行 IPv4／IPv6，当前 Windows 验证进程限定 IPv4；Android 编译检查与设备原生证据分开记录。不得修改现有 TUN、IPv6 阻断、宿主路由、DNS 或网络接口。

## 9. 验证

- Windows 实际 RIO/IOCP TCP/UDP、IPv4/IPv6、接管、超时、关闭和数据完整性。
- Linux 实际 syscall 与网络路径；记录实际内核版本，分别验证各优化和合法组合，不将单个版本的结果扩展为整个支持范围的实测证明。
- 真实 RX ZC 需要相应网卡/驱动；NODEV 仅验证该内核 Interface 的复制接收与回收流程。
- Android 在普通 App 进程内验证，不能用 root/adb shell 成功替代。
- 特性开关不改变字节流/数据报语义；失败、取消和资源压力不产生提前回收、重复发送、丢失唤醒或句柄重复关闭。
- 同资源预算下测量小消息、大块 TCP、UDP、混跑和空闲唤醒；不得捏造未测性能。

Linux 验证采用可复现、隔离的多内核 guest，以 6.18 LTS 为主验证线并覆盖 6.6／6.12；保留 7.2.7 guest 和原始结果用于高级路径及历史复现。逐项记录实际内核补丁版本、架构、权限和运行场景，不将设计目标写成已通过结果。不得擅自升级用户 VPS、更改 WSL 全局内核或宿主网络／安全策略。Android 使用专用验证应用和隔离模拟器，不改变用户现有 VPN 配置。
