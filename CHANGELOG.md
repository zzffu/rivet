# Changelog

## 0.1.0

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
