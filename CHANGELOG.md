# Changelog

## Unreleased

- 修复全库审查复现的回调重入问题：Core 在完整完成批次和额度更新后通过有界、带代际身份的唤醒源交付通知；计时器释放节点后唤醒；Waker clone／wake／drop 不再跨内部借用。自有同步及非 socket 等待改用私有 pinned 通知 Module，移除直接 `event-listener` 依赖，保留公开 async-* 类型；注销在 lifecycle 锁外通知，原生 helper／threadpool 回调可关闭自身而不 self-join。
- 修复执行／工厂／析构 panic 的 payload 再次 panic 时绕过完成发布的问题；任务组继续排空失败结果，取消分类和准入回收不变。socket 根据实际停止状态返回 `BrokenPipe`，不再由保留的完成 Future 是否持有 Worker 强引用决定错误类别。
- Linux 接收／接受错误同样消耗发布额度；零额度保留终态错误及其 selected-buffer 所有权，停止时仍可退役，避免远端 RST 触发 Core 队列断言或重放时重复回收。UDP 元数据回归移除已由原生 socket 反证的跨发送者全局到达顺序假设，继续逐包检查内容、来源、原长、截断、唯一性和保留租约。
- Android 所有 feature 配置均保留并解析导入队列的 GRO 元数据；普通 TCP Drop 在宿主仍持有 FD 别名时发起写半关闭，idle handoff 和 abort/RST 不变。Windows RIO 导入保留原生错误码与失败 socket；registered-I/O 测试 helper 独立初始化 Winsock，跨 Runtime 重试不再依赖其他测试顺序。
- 修复前冻结消费者及独立回归证实上述十项问题，修复后外部消费者通过。Windows IPv4 默认及全 feature／all-targets 各 163 项通过；隔离 Linux 6.18.54-1-lts TCG guest 的全 feature／精简构建分别 184／151 项通过；Android API37／x86_64／16KiB 页 shell 下两种构建各 159 项通过，普通 App 的 20 项场景通过，包含新增 GRO 与别名 FIN/RST 场景。原生、编译及环境限制详见 README 本轮证据；保留失败日志和 guest 校验报告，不以 WSL 的 GRO 输入前置条件失败冒充库路径失败或跳过后成功。

- 按系统架构与实现契约新增按需、只读的 `BufferPool::usage()`、当前 worker 的 `runtime::resource_snapshot()` 和单个 UDP 的 `receive_snapshot()`。结果类型集中在 `diagnostics`，采用私有字段和 getter；不增加全局聚合、后台采样、资源租约或公开构造义务，保持 0.2.x 的源兼容边界。
- 快照区分普通 payload／lease 元数据／待回收、逻辑队列／原生占用／关闭退休，以及 RIO 窗口中的 ready、idle、最近分配受阻和真实重投分配失败累计值。Linux 原生占用同时考虑尚未释放的 ZC 内存 guard；Android 不伪造异步内核占用，平台无对应语义时返回 `None`。成功采集不分配、不执行 I/O、不推进回收、不触发回调；worker／driver 正在可变借用的重入查询返回 `WouldBlock`，而不是借用 panic。
- 资源回归发现并修复 Windows teardown 的无限等待：软件服务可在没有完成事件时退休最后一个未投递接收窗口；关闭中的 driver 已 idle 时，`poll` 不再无限等待 IOCP。保留触发旧行为的原生回归，不伪造完成、不提前释放 kernel guard，也不改变正常空闲休眠。
- 本轮资源观测验证（2026-09-28）：Windows IPv4 默认及全 feature／all-targets 各 145 项通过，精简构建的池／观测回归 22 项通过；原生示例与外部压力／退休／租约消费者通过。隔离 Linux 6.18.54-1-lts TCG guest 中，全 feature 和精简构建的相关回归各 21 项及 `native_traits` 通过。严格 Clippy／rustdoc、格式、Linux 全 feature／精简检查、Android 双 ABI 全 feature／all-targets 及独立 ARM64 smoke 包编译检查通过；Windows IPv6、Android 原生、物理 NIC 和吞吐未在本轮执行。证据索引：`artifacts/resource-observation-20260928/verification-summary.json`。

- 先补充系统架构与实现契约，再新增 `WriteBuf::as_mut_slice()`：只安全借出已初始化前缀，不扩大长度、不分配或复制；保留普通存储唯一拥有、外部区域不可恢复及内核 guard 阻止写入的约束。补强清空、切片恢复和空尾切片的可变视图回归。
- 新增全平台可调用的纯算术 `Limits::windows_udp_receive_bytes(receive_chunk)`，计算单个 Windows UDP 接收窗口的初始 payload 需求并检查非法尺寸／乘法溢出。结果允许超过当前池，用于部署规划，不冒充资源预留或原生成功保证。
- 明确每 socket 的接收窗口与每 worker 共享的 payload／lease／operation 预算，给出多 socket、旧租约及重投余量算例。现有 RIO 准入失败补充请求规模和预算类别，仅在失败路径格式化，保留错误类别、完整窗口回滚和接管所有权；默认槽数及正常 I/O 路径不变。
- 明确空 `TaskGroup::join_next()` 返回 `None` 但不关闭准入、完成 Future 不可假设能重复 poll，以及取消请求／join 后任务析构／native retirement 的区别。`runtime_services` 展示有界动态准入、空组再次接纳和取消后 join；不增加兼容运行时、统一融合或每 I/O 装箱。
- 增加 Windows 原生组合回归：观察 UDP waiter 被排队数据唤醒后再取消、跨窗口保留旧租约，以及真实 TCP 双向背压期间独立反向推进和半关闭。容量估计用于完整窗口接管准入的边界回归，保留失败后的原 descriptor 与重试能力。
- 本轮 Windows IPv4 默认及全 feature／all-targets 回归各 135 项通过；`native_traits`、单／双 worker `runtime_services` 及临时外部消费者实跑通过。严格 Clippy／rustdoc、格式检查、Linux x86_64／Android ARM64 全 feature／all-targets 检查和独立 Android smoke 包编译检查通过。IPv6 未执行，未新增 Linux／Android 原生或物理 NIC／吞吐证据；索引见 `artifacts/rivet-contracts-20260928/verification-summary.json`。

## 0.2.0

- 先更新系统架构与实现契约，再将 Linux 改为 6.18 LTS 主验证线及 6.6／6.12 兼容目标。删除全局版本／RC 准入门禁，以及 `KernelVersion::MINIMUM_LINUX`、`require_supported`；保留可解析的版本元数据，无法识别版本时保守选择能力，不伪造兼容证据、不回退到 epoll。
- 默认 Cargo 集合改为 `linux-full`，不包含 NODEV。Linux 未指定项继承合法、均衡、空闲可休眠的自动优化方案；显式 Off、Auto、RequireCapability 和结构化失败身份保留。默认不启用 SQPOLL／NAPI 忙轮询，硬件 RX 仍需要预配置队列；Windows／Android 运行时默认策略不变。
- `default-features = false` 保留完整基础网络功能。旧内核普通多段发送使用稳定消息／iovec 存储上的 SENDMSG，旧版 multishot 接收使用合法长度；不增加 payload 拼接、逐操作分配或业务请求试错重发。
- 将 fixed ZC 的内部注册资源与普通 fixed SEND/RECV 优化分离，后者不可用或显式关闭不再阻止前者。分别选择旧内核 scalar fixed、非 fixed SENDMSG_ZC 向量、已确认的 fixed message vectors 和较新 fixed SEND_VECTORIZED 组合，保留同注册区域及真实内存释放约束。
- 可选路径按实际 modifier／注册／修复条件判定，能力报告保留原始 Auto／严格请求和失败原因。ZCRX 初始化失败后的 ring 重建不重新推导默认项，不重新激活已失败依赖或更改显式选择。
- 修复自动组合暴露的 UDP 增量尾部截断：合法数据报也可能因共享缓冲区只剩短尾部而被截断，继而使混合负载等待丢失数据。TCP 增量 ring 与 UDP 普通 ring 现在分摊原有 payload slab，保留全局租约／回收所有权；小预算 Auto 回到单组普通接收，不关闭整个优化集合、不吞截断错误或重发数据报。增加跨缓冲区边界、保留租约及小预算回退回归。
- 迁移配置和网络回归，明确隔离 ordinary／fixed／direct 路径；覆盖向量短写后缀、多段 multishot 背压与取消、普通收发和私有 ZC 注册共存。示例新增 `--disable NAME`，并显示未启用能力的原因。
- 验证工具增加签名固定的 6.6／6.12／6.18 guest，原样保留 7.2.7 制品身份；`--kernel` 选择隔离状态，`run-suite` 在一次启动内逐项校验并执行多个静态 ELF。工具不构建内核、不更改宿主内核／网络／安全设置，不以 loopback 或 NODEV 冒充物理 NIC 证据。
- 迁移：从 Runtime 能力报告读取生效结果，不把配置请求查询当作内核决策；要保留全关闭行为，逐项显式设置 Off，或关闭默认编译集合。普通 fixed 收发和 fixed ZC 需分别关闭。公开配置／结果结构、枚举和原生 trait 形状不变，默认行为和旧门禁入口的有意变更以次版本升级交付。
- 本轮原生验证：隔离 x86_64 TCG guest 的 6.6.72／6.12.75／6.18.54 默认与精简构建测试及四个实际示例通过，覆盖 IPv4／IPv6；7.2.7 全 feature／精简构建及 NODEV shared／fixed-vectored 独立组合通过。UDP 尾部回归在修复前失败于第 125 个合法数据报，修复后与原混合负载通过；低版本不可用的高级路径明确记录严格失败／Auto 停用及跳过，不冒充执行成功。
- Windows IPv4 全 feature／all-targets 测试及三个实际示例、36 组 Linux feature 编译组合、Linux 全 feature／精简及 Windows 严格 Clippy、严格 rustdoc、格式检查通过。Linux ARM64、Windows GNU、Android 双架构和独立 Android 包编译检查通过；未新增这些目标的原生证据或物理 NIC 验证。完整索引见 `artifacts/linux-auto/verification-summary.json`，保留实际限制及修复前／后原始报告。

## 0.1.0

- 移除 Windows 11／Server 2022 的显式版本门禁，删除 `require_supported_windows`、`VersionInfo`、`RtlGetVersion` 声明及废弃导入，不替换为另一个版本号检查。启动仍要求真实 Winsock 2.2、registered-I/O socket、完整 RIO 扩展表及相关原生能力，缺失时保持可观察错误，不静默回退。
- Windows 文档改为当前 Rust 工具链的 Windows 10／Server 2016 基线，明确不额外按 OS 版本号拦截，不承诺 Win8 或未经执行的版本覆盖。本次 Windows 11 上 IPv4 模式 128 项全 feature／全 target 回归、`native_traits`／`runtime_services` 实跑、严格 Clippy、修改文件格式及 Windows GNU 编译检查通过；两个实跑程序均不再导入 `RtlGetVersion`。未执行 Windows 10／Server 2016 原生验证，Linux／Android 实现与门槛未改动。
- Android 最低支持版本调整为 API23：启动读取 `ro.build.version.sdk`，去掉 API29 `android_get_device_api_level` 强符号依赖，保留版本读取失败报错与低于 23 的拒绝。`android_setsocknetwork` 的 API23 Network 绑定能力不变。
- 验证 App 的原生链接／JNI C、DEX、manifest、签名最低版本同步为 23，明确保留 v1 签名及 16KiB 对齐；Java 向 JNI 传入实际 API level。移除 API24 `CompletableFuture` 和 API26 `java.nio.file` 依赖，保留每进程单次运行、Activity 停止／恢复时的结果交付以及旧结果失效和初始化失败持久化。
- API23 完整回归发现并修复 Android IPv6 TCP `abort` 的旧 SELinux 长度检查：`AF_UNSPEC` 请求提供完整 `sockaddr_in6` 存储，保持宿主 FD 别名仍打开时立即 RST。既有回归失败前／修复后结果均保留，不改变 Linux 实现，不吞掉原生错误。
- 本次验证：API23／x86_64／4KiB 普通 App 17 项通过、1 项不支持的 UDP offload 明确跳过，另有 127 项 Android 原生回归通过；API37／x86_64／16KiB 普通 App 18 项及 3 项 abort 回归通过。双 ABI API23 APK 的签名／对齐与全部强符号检查通过，严格 Android Clippy、修改文件格式与既有 Java 持久化回归通过；ARM64 未做本次原生运行。
- 新增 `docs/linux-io-uring-compatibility.md` 归档接口沿革及默认路径／可选优化下限分析；现有 Linux 门禁、UAPI／runner pin 和承诺不变。Windows 代码不动，文档区分 Rust1.77 的历史 Win7+ 基线、Rust1.78 起的 Win10 基线与本包 Rust1.98／edition2024、Windows11／Server2022 门禁，不承诺 Win8。
- 统一平台支持表述为“最低支持版本（含）及后续版本”，区分版本下限、原生能力条件、UAPI 参考版本、固定验证 guest 和实际测试范围。仅更新文档与 Linux 模块注释，不降低版本门槛、不改变能力探测，也不扩大已验证范围。
- 本次文档校验：Linux 版本下限／vendor 后缀／RC 拒绝的既有配置回归通过，Windows IPv4 `native_traits` 实跑通过；未新增 Linux／Android／Windows 最低支持版本上的原生验证。
- 收紧后端管理 Interface：`CapabilityReport::{new, decide, finish}` 与 `Optimization::bit` 改为 crate 内部可见，删除无调用者的 `enabled_mask`。这是一次有意的公开面收口；外部迁移为从 `Runtime::capabilities()` 获取报告，再用 `states`／`state`／`enabled` 按优化项查询。原有能力决策回归迁入内部单元测试，不保留兼容别名。
- 先更新系统架构与实施契约，再落实公开兼容说明：以本次收口后的 Interface 为 `0.1.x` 兼容基线，保留拥有型缓冲区、取消、worker、任务、flush／半关闭、数据报和计时器行为，以及开放 trait 的实现要求；将 `sync` 重导出的 `async-channel`／`async-lock`／`futures-channel` 类型身份与行为纳入兼容评估。本次不修改配置／结果结构构造方式、枚举匹配规则或依赖版本，不添加 `#[non_exhaustive]` 或包装层。
- 本次收口验证：Windows IPv4 模式 `--all-features --all-targets` 的 128 项回归、严格 Clippy／rustdoc、修改文件格式检查及双／单 worker `native_traits` 实跑通过；临时外部程序实际查询能力并验证公开 channel／锁／guard／semaphore／oneshot 类型互操作，5 个后端管理／位图入口均按预期被编译器拒绝。未执行 Linux／Android 原生场景或 IPv6 连通性，未修改宿主网络设置。
- 收口后的 Linux GNU／musl、Android ARM64／x86_64、Windows GNU 均通过 `--all-features --all-targets` 编译检查；Linux GNU 的默认 feature 检查及 GNU／musl 全 feature 检查另以 `-D warnings` 通过。能力回归迁移后的条件导入已处理，编译检查不冒充跨平台原生执行。
- 新增 `TcpStream::connect_from(local, peer, options)`，在惰性建连时显式绑定源 IP／端口；端口 0 由系统分配，非零端口保持指定值，地址族不匹配在原生 socket／hook 之前拒绝，绑定失败不回退到自动源地址。原有 TCP 自动选源与 UDP 绑定接口不变。
- Windows 在显式源地址和默认通配地址之间二选一，只执行一次 ConnectEx 前置绑定；Linux／Android 仅在请求显式源地址时增加绑定。Linux direct-descriptors 绑定失败同时回收固定文件槽和导出 FD，保留原有建连完成／取消生命周期；hook 继续负责 protect／接口配置，不负责绑定。
- 新增源地址／端口、惰性构造、双向地址族误配及连续绑定失败后的资源复用回归；更新系统架构、实现契约和 `native_traits`，由代理源站核验 `127.0.0.2` 及写半关闭后的完整反向响应。
- 本次源地址绑定验证：Windows IPv4 模式 128 项回归与代理实跑通过；隔离 Linux 7.2.7 guest 的 4 项绑定回归覆盖普通／fixed-files／direct-descriptors，严格启用 direct-descriptors 的代理实跑通过。严格 Clippy／rustdoc、Linux GNU／musl、Android ARM64／x86_64、Windows GNU 跨目标检查及 Linux 无 feature 检查通过；本次未执行 IPv6 连通性或 Android 原生场景，未修改宿主 TUN／网络设置。
- 新增开放的 Rivet 原生流／数据报收发 trait，直接复用拥有型缓冲区、原生 Future 和发送分组；包含惰性 flush 与异步写半关闭，保留接收等待者冲突、取消、输入字节计数和租约回收契约，不引入逐操作装箱或 payload 复制。
- 新增 `LocalSpawn`、`Spawn`、`BlockingSpawn`、`Timer` 与无状态 `Current` 入口；保留 `!Send` 本地 Future、工厂跨 worker 投递、原生任务句柄／错误及 `Sleep::reset`。Connector／Acceptor 和代理策略仍由上层定义。
- 新增 `native_traits` IPv4 回环代理示例和 flush／半关闭生命周期回归；泛型双向转发保留请求 EOF 后的反向响应，并覆盖数据报、任务执行和可重置计时。补充系统架构、接口契约和使用文档。
- 原生 trait 初次验证在 TUN／IPv6 阻断保持不变的条件下进行：Windows IPv4 模式 124 项回归通过，默认双 worker／单 worker 代理与原有 loopback 通过；严格 Clippy、rustdoc、变更 Rust 文件格式检查及 Linux GNU／musl、Android ARM64／x86_64、Windows GNU 跨目标检查通过。当时未执行 IPv6 连通性或 Linux／Android 原生运行。
- 修复 Android 导入 TCP 仍有宿主 FD 别名时 `abort` 不能立即中止连接的问题；使用 `AF_UNSPEC` 断开底层 TCP，补充 IPv4／IPv6 保留别名时的真实 RST 回归。
- 修复信号 Waker 同步销毁等待任务及最后订阅时的重入死锁；改用单等待者 `AtomicWaker`，取消等待注销 Waker 但保留待处理信号，补充隔离子进程销毁／重新订阅回归。
- 新增按需启动的有界阻塞池，区分排队取消与运行中工作，Runtime 停止时保留额度、清理责任和原生线程 join。
- 新增 Runtime 级有界非 socket 注册表：Linux／Android `AsyncFd` readiness 与 Windows `AsyncHandle` 原生等待；保留失败接管所有权、取消安全和迟到事件隔离。
- 新增三后端 `TcpStream::abort`，显式触发 TCP RST，并保持 direct/fixed socket、在途发送及 ZC 内存释放的真实生命周期。
- 新增 `TaskGroup`、可克隆 `AbortHandle` 和 `serve_until`；连接分发不再 detach，支持有界准入、协作停止、宽限期及强制回收。任务完成回执推迟到 Future／工厂销毁之后。
- 新增执行器无关的 channel、oneshot、异步锁、Semaphore、watch、Notify 和 `CancellationToken`。
- 新增原位 `Sleep::reset`、可取消等待的 `Interval` 及 Burst／Skip／Delay 错过 tick 策略。
- 新增作用域退出信号订阅，多订阅广播并在释放后恢复宿主处理；Unix 不覆盖已有自定义 handler，真实信号回归仅操作隔离子进程。
- 新增跨平台 `runtime_services` 可执行场景，并复用于 Android 普通 App；补充系统设计、生命周期契约与消费者行为回归。
- 新增原生 Rust Future 网络运行时：自动初始放置、本地任务、跨线程唤醒、有界定时器／资源与空闲等待。
- 新增 TCP／UDP IPv4／IPv6、半关闭、批量数据报、拥有型 socket 接管及 Android Network／宿主配置钩子。
- 新增 Linux 7.2.7 io_uring、Windows RIO＋IOCP、Android 普通 App epoll 后端；不以其他后端静默替代明确请求的路径。
- 新增稳定池化只读租约及真实内核释放追踪；发送结果、取消等待、操作终止与缓冲区回收分别处理。
- 新增细粒度 Cargo features、显式 Off／Auto／RequireCapability 策略及能力报告；聚合 feature 不自动打开运行开关。
- Linux 使用项目内固定 UAPI、独立的 kernel user_msghdr／cmsghdr 布局和初始化 padding，避免 GNU／musl 字段差异影响原始 io_uring 参数。
- 将异步内核可写控制区与 Rust 操作状态分离；拒绝会阻塞或妨碍关闭的正值 TCP linger，并保留失败接管的句柄所有权。
- 修正固定向量 ZC 的空段处理、RIO 后续提交失败的 TCP 已发送前缀计数、IOCP／RIO 完成预算公平性、忙轮询延迟定时器及导入前取消导致的 serve 确认挂起。
- Windows RIO UDP 在接管返回前提交有界多槽接收窗口，保留已完成但尚无发布额度的数据报，支持旧租约仍存活时的缓冲区轮换；导入仅对 TCP 查询 `SO_ACCEPTCONN`。
- Linux 注册等待参数先使用禁用 ring 完成内存注册再启用；正确处理不带 buffer ID 的 multishot TCP EOF，并对每个 multishot UDP 完成独立执行接收截断上限。
- Android 接管拒绝对已连接 socket 进行迟到 Network 绑定；专用验证 App 在 JNI 初始化前作废旧结果，并持久化初始化失败。
- Android 普通 App 同时验证 API29／4KiB 和 API37／16KiB：旧内核 offload 不可用时明确区分 Auto 与 RequireCapability；GSO／GRO 的编译开关独立。
- Linux 可选模块、原生资源及提交路径使用独立条件编译；无 feature、26 个独立 feature 与 14 个交叉组合的编译检查全部通过。
- 修正未编译的 Auto 子优化仍展开隐式依赖、错误激活冲突 RX 模式的问题；保留显式禁用依赖的冲突检查，并用 NODEV-only 原生程序及配置回归验证。
- 新增实际 loopback／混合负载程序、行为回归、签名内核隔离 runner 和支持 16KiB 页的普通 Android 验证 App。验证工具不修改主机全局网络、安全策略、WSL 内核或外部 VPS。
- 补充 Windows 原生 IPv4／IPv6 loopback 验证；移除验证进程的 IPv4-only 限制后，31 项网络行为测试通过。
- 补充 OnePlus 13／Android 15／ARM64／4KiB 页真机验证，普通 App 的 17 项场景全部通过。
- 对 Android 标准库线程局部宏的已知 Clippy 误报添加两处平台限定的 lint 豁免，保留 const 初始化；Android 双架构及 Windows 严格 Clippy 检查通过。
- 修正 Linux 小 SQ 下未提交操作、取消和唤醒注册被阻塞 CQ 等待阻断的问题；资源／额度等待仍允许休眠。
- Linux 普通和 multishot recvmsg 始终解析已有 GRO 分段元数据，修复无 `udp-gro` feature 接管外部 socket 时合并数据报的问题；主动启用优化仍受 feature／策略控制。
- 将自动投递的活跃状态复核、准入和入队串行化，失活候选不再导致仍有后台容量时错误返回 `NotRunning`。
- 隔离尚未启动工厂捕获值的析构 panic，覆盖取消和 shutdown，保持所属线程销毁与准入额度回收。
- 在配置阶段按实际队列元素布局拒绝不可寻址的接收、接受和完成队列容量，避免首次创建 socket 或 worker 时出现容量溢出 panic。
