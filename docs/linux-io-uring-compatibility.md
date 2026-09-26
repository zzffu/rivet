# Linux io_uring API 历史与兼容性分析

原始记录日期：2026-09-26，针对 **0.1.0**。本文归档源码与上游资料分析，**不是旧内核部署验证报告**；下面的旧版本门禁、默认路径和源码行号均属于记录当时的状态。

**0.2.0 策略已独立调整**：6.18 LTS 为主验证线，6.6／6.12 为兼容目标，删除全局版本／RC 门禁，默认自动选择合法优化组合。现行契约见 [README](../README.md)、[架构文档](architecture.md#6-功能选择与能力契约) 和 [实现契约](implementation-contract.md)。保留 v7.2.7 参考 UAPI 和原 guest 制品，不把这份历史记录当作现行准入条件。

## 1. 记录时的结论与支持契约（0.1.0）

- 当时 Rivet 的 Linux 支持范围是 **x86_64／aarch64、稳定内核 7.2.7 及后续稳定版本，不支持 RC，不静默回退到 epoll**。0.2.0 改变版本准入和默认选择，不改变无 epoll 回退及所有权契约。
- 当时 `src/capability.rs:14-49` 的 `MINIMUM_LINUX = 7.2.7`、RC 拒绝及下限检查在 ring 创建前执行；这些门禁入口已在 0.2.0 删除，不保留无操作兼容方法。
- `src/driver/linux/uapi.rs` 与 `src/driver/linux/zcrx.rs` 以 **v7.2.7 UAPI** 为参考；当时 runner 固定 **7.2.7-arch1-1** guest。现在的多内核锁保留该制品身份和校验信息。参考 ABI、可复现 runner、支持目标和实际运行证据是四件不同的事。
- **[INFERENCE] 对记录时的实现，忽略显式门禁、只按上游主线 API 引入时间推导，强制 setup flags 给出 6.6；无可选 feature 门控的普通多段 SEND_VECTORIZED 将已识别的必要下限提高到 6.17。** 这不是“6.17 已兼容”的充分条件，也不适用于后来增加 SENDMSG 兼容路径的实现。
- 原始分析轮次没有修改 Linux 实现、配置、门禁或 pins，也没有为这些历史版本执行原生验证。后续实际结果单独记录，不能倒填为该轮证据。

## 2. 从 5.1 到本次核实的上游版本

[初始合入提交][initial] 与 [v5.1 UAPI][v5.1] 表明，io_uring 于 Linux **5.1（2019）** 引入，共享 SQ/CQ、setup/enter、轮询与固定资源构成起点；这不等于当时已有今天的网络接口组合。

| 上游阶段 | 与本项目相关的演进 |
| --- | --- |
| 5.3—5.6 | `SENDMSG/RECVMSG`、`ACCEPT/CONNECT/ASYNC_CANCEL`、普通 `SEND/RECV` 及 opcode probe 陆续出现。 |
| 5.11—6.1 | 扩展等待参数、多次触发 poll、提交／task-run 调度、资源注册、multishot 与发送零拷贝陆续增加。 |
| 6.6—6.18 | `NO_SQARRAY`、direct descriptor 安装、NAPI、buffer bundles、incremental buffers、registered wait、ZCRX、vectored SEND、mixed CQE 等继续扩展。 |
| 6.19—7.2 | ZCRX control/import/export、RX 大小配置、NODEV／事件统计，`SQ_REWIND`、`min_left`、普通 SEND/RECV 使用 registered buffers 等影响可选路径。具体版本证据见下表。 |
| 7.2.7 | 包含与 `MSG_TRUNC`、incremental buffer 消耗量有关的正确性修复，并非所有相关 API 都首次出现在此版本。 |
| 7.2.8／7.3-rc4 | [kernel.org 发布索引][releases] 在记录日列出的最新 stable 是 **7.2.8（2026-09-25）**，mainline 是 **7.3-rc4（2026-09-20）**；后者是预发布，不是稳定版。 |

[7.3 合入记录][v7.3-merge] 与 [v7.3-rc4 ZCRX 头文件][v7.3-zcrx] 包含动态 ZCRX area provisioning，以及 refill／锁／记账修复。Rivet 当前不使用新增 `ADD_AREA` 控制操作；这些预发布变化既不改变 RC 拒绝策略，也不构成稳定兼容性验证。

## 3. 必须区分 setup 下限与完整默认数据路径

下表的版本是 API 引入时间，而不是 Rivet 的受支持最低版本。上游依据为 [setup 手册][setup]、[enter／opcode 手册][enter]、[register 手册][register] 及明确标出的 tagged source。

| 必需接口／行为 | 上游版本 | 仓库使用位置 |
| --- | --- | --- |
| `CQSIZE`；`SUBMIT_ALL`；`TASKRUN_FLAG` | 5.5；5.18；5.19 | `src/driver/linux/mod.rs:437-441` |
| `SINGLE_ISSUER`；`DEFER_TASKRUN`；`NO_SQARRAY` | 6.0；6.1；**6.6** | `src/driver/linux/mod.rs:439-442`；`src/driver/linux/ring.rs:101-129` |
| `NODROP`；`EXT_ARG` feature／enter | 5.5；5.11 | `src/driver/linux/ring.rs:88-95,296` |
| `SENDMSG/RECVMSG`；`ACCEPT/CONNECT/ASYNC_CANCEL`；`SEND/RECV` | 5.3；5.5；5.6 | `src/driver/linux/mod.rs:513-528,2290-2497` |
| `REGISTER_PROBE`；multishot `POLL_ADD` | 5.6；5.13 | `src/driver/linux/ring.rs:399`；`src/driver/linux/mod.rs:2243` |
| `REGISTER_SYNC_CANCEL`，含 `ANY/ALL` | 6.0 | `src/driver/linux/ring.rs:462-478`；`src/driver/linux/mod.rs:3298` 的 teardown |
| 普通多段 SEND 的 `IORING_SEND_VECTORIZED` | **6.17** | **[`src/driver/linux/mod.rs:2494`](../src/driver/linux/mod.rs#L2494)**，不是可选 ZC 分支 |

这里的“完整默认路径”包含调用方传入多段数据的普通发送，不是只看单段发送是否能工作。`SEND_VECTORIZED` 的 [引入提交][vectored] 修改了 SEND/SEND_ZC；它在 [v6.16 头文件][v6.16] 中不存在，在 [v6.17 头文件][v6.17] 中存在。因此，“强制 setup flags 的最大版本是 6.6”不能推出完整默认行为只需 6.6。

[`src/driver/linux/mod.rs:512-528`](../src/driver/linux/mod.rs#L512) 的 opcode probe 能证明 `SEND` opcode 存在，**不能证明该 opcode 的新 modifier 也可用**。同样，ring 创建成功不等于全部请求语义、取消回收和网络交互都正确。

## 4. 可选优化有各自的版本与组合要求

表中“已确认”仅表示该 tagged source 已含对应定义，不把它扩写为所有组合／稳定分支回移的精确首次可用版本。启用还受 Cargo feature、运行时策略、注册结果、权限和硬件约束；不能把各行最早版本简单拼成一个经过验证的“全 feature 下限”。

| 可选路径 | 上游历史／证据 | 仓库使用位置 |
| --- | --- | --- |
| fixed files／direct descriptors | sparse `FILES2` 5.19；allocation range 6.0；`SOCKET` 5.19；`FIXED_FD_INSTALL` 6.8。[register][register]／[enter][enter] | `src/driver/linux/resources.rs:39-63`；`src/driver/linux/mod.rs:590,1653,1674` |
| registered buffers 用于普通 SEND/RECV | 注册机制始于 5.1，但**普通 SEND/RECV 的 fixed-buffer 支持是 7.2**，不能由注册机制的年龄替代。[7.2 合入记录][v7.2-merge] | `src/driver/linux/resources.rs:137`；`src/driver/linux/mod.rs:2350,2484-2491` |
| provided buffer ring／incremental buffers | 5.19／6.12。[buffer-ring 手册][buf-ring] | `src/driver/linux/resources.rs:269-283` |
| incremental `min_left` | v7.0 没有，v7.1 已确认；本项目 incremental 模式实际写入非零值。[引入修复][min-left]／[v7.1 UAPI][v7.1] | `src/driver/linux/resources.rs:275-278` |
| multishot accept／receive；buffer bundles | 5.19／6.0；6.10。[维护者网络历史][network-history]／[enter][enter] | `src/driver/linux/mod.rs:2307,2328,2332,2461` |
| `SEND_ZC`／`SENDMSG_ZC` | 6.0／6.1；vector modifier 另需 6.17，fixed＋vectored＋ZC 等组合不能只看 opcode 年龄。[enter][enter]／[vectored 提交][vectored] | `src/driver/linux/zc.rs:98-193` |
| registered ring；registered wait／`MEM_REGION` | 5.18；6.13。[register][register]／[enter][enter] | `src/driver/linux/ring.rs:292,306,410-450` |
| NAPI；static NAPI IDs | 6.9；v6.13 已确认。[register][register]／[v6.13 UAPI][v6.13] | `src/driver/linux/mod.rs:1155-1184` |
| `MSG_RING`；同步 `REGISTER_SEND_MSG_RING` | 5.18；6.13；本项目也需要后者，不能仅据前者定下限。[enter][enter]／[register][register] | `src/driver/linux/mod.rs:101-114,169-182` |
| `CQE32`／`CQE_MIXED`／`SQ_REWIND` | 5.19／6.18／7.0。[setup][setup] | `src/driver/linux/mod.rs:456,475,477`；`src/driver/linux/ring.rs:177-205,340-378` |
| ZCRX 基础；control/import/export；可配置 RX 大小 | 6.15；v6.19 已确认；v7.0 已确认。[v6.15][v6.15]／[v6.19][v6.19]／[v7.0][v7.0] | `src/driver/linux/zcrx.rs` |
| ZCRX NODEV；events／stats | 7.1；7.2。[NODEV 提交][nodev]／[v7.1 ZCRX][v7.1-zcrx]／[7.2 合入记录][v7.2-merge] | `src/driver/linux/zcrx.rs:53-65,99-127` |

真实 NIC ZCRX 还需要 header/data splitting、flow steering、RSS 等设备配置，见 [最初的上游 ZCRX 文档][zcrx-doc]。**NODEV 会复制数据**，不能作为物理 NIC RX 零拷贝证据；loopback 或注册成功也不能替代硬件路径验证。

## 5. ABI 稳定不等于没有扩展或没有缺陷

应区分“保留旧用法的兼容语义”与“所有年代的内核都理解新字段／新标志”。io_uring 持续增加 opcode、modifier、feature bit，复用保留字段，并协商新队列布局。部分结构体填零会保留旧含义，但本项目使用的非零 `min_left`、`rx_buf_len`、`event_desc` 分别需要对应扩展。`CQE_MIXED` 需要处理条目宽度与 skip，`SQ_REWIND` 改变提交索引规则且不能与 SQPOLL 组合；`src/driver/linux/ring.rs` 为此有专门处理。

已核对的 v7.2.7／v7.2.8 ZCRX 定义支持本项目采用的 96 字节 IFQ／event descriptor 布局、72 字节 control 对象、NODEV／event 常量及保留字段分配；这些定义不是来自尚未稳定的未来 ABI。对照入口为 [v7.2.7 UAPI][v7.2.7]、[v7.2.7 ZCRX][v7.2.7-zcrx] 和 [v7.2.8 ZCRX][v7.2.8-zcrx]。

### 7.2.7 中与本项目有关的正确性修复

[7.2.7 ChangeLog][changelog-7.2.7] 包含上游提交 `6028b543884f8735e057ec9eea4908cd61cab230`：`io_uring/net: don't overconsume buffers when using MSG_TRUNC`。当报文大于 provided buffer 时，`MSG_TRUNC` 返回完整报文长度，而实际写入较少；修复让 incremental buffer 按实际填充区域推进，同时保留完整长度返回语义。

Rivet 在 [`src/driver/linux/mod.rs:2381`](../src/driver/linux/mod.rs#L2381) 为 UDP receive 设置 `MSG_TRUNC`，因此这与相应可选缓冲路径的正确性相关。它是**缺陷修复，不是新 syscall 的引入**；相关性也不证明所有默认配置都依赖该修复。本文没有核定它在每条 stable／发行版分支上的最早回移版本。

### 证据冲突时优先采用 tagged source

- [register 手册][register] 将 `ZCRX_CTRL` 标成“since 6.15”，但已核对的 [v6.15][v6.15] 和 [v6.18][v6.18] 头文件没有该注册命令，[v6.19][v6.19] 才有。这个手册日期过于宽泛，不能据此宣称 control/import/export 在 6.15 可用；应以对应 tag 的 UAPI 和实现为准。
- `src/driver/linux/mod.rs:2383` 的“7.2 multishot cap”注释不能当作引入版本证据：[cap 提交][mshot-cap] 与 [v6.17 `io_uring/net.c`][v6.17-net] 已有每次调用的 `mshot_len` 处理。本文仅记录差异，不修改该注释；更早和回移版本的完整覆盖未核定。

## 6. 维护时如何使用这份记录

1. **6.17** 是原始实现已识别的默认路径接口条件，不是跨版本兼容实现的全局最低版本，也不覆盖发行版回移、工具链或依赖版本。
2. 排查可选能力失败时，核对实际 tag／发行版源码、modifier／结构字段和组合语义，不只看 uname、opcode probe 或单次注册成功。fixed／vectored ZC 等组合及 backport 的版本必须按实际证据判断。
3. 0.2.0 的兼容与自动选择工作按独立批准的系统设计实施；保留本记录作为历史依据，不恢复旧全局门禁，也不将版本表冒充逐版本实测、权限或硬件可用性证明。

## 7. 0.2 实现选择补充（2026-09-27）

本节描述后来增加的兼容路径，不改写上述 0.1 历史结论。具体原生证据按实际内核和场景记录在 README／变更记录，不能从这张接口表推导实测结果。

| 路径 | 0.2 的选择依据 |
| --- | --- |
| 普通向量发送 | 旧版或未知版本用 SENDMSG；具备 6.17 modifier 条件时可用 SEND_VECTORIZED。 |
| TCP multishot 长度 | 旧版使用零长度；6.17 的 per-invocation cap 可用时才写非零长度。 |
| scalar fixed ZC | [v6.6][v6.6-net]／[v6.12][v6.12-net] 已有 SEND_ZC fixed 导入；注册资源与普通 fixed 收发分开。 |
| message/vector ZC | 旧版使用非 fixed SENDMSG_ZC；[v6.17][v6.17-net]／[v6.18][v6.18-net] 的 SENDMSG_ZC 支持 registered iovec 导入，但 fixed SEND_ZC 仍走 scalar 导入，不能因为接受 VECTOR flag 就认为组合正确。 |
| fixed SEND_VECTORIZED | 当前采用 [v7.2.7 实现][v7.2.7-net] 已确认的 fixed-vector 导入路径作为保守条件，不声称它是精确首次引入版本。 |
| TX／RX 观测 | TX usage flag 在 [v6.2][v6.2] 已有，RX events／stats 另需较新的 ZCRX 扩展，分别判断。 |
| incremental | 保守选择已确认的 7.2.7 级语义。TCP 增量与 UDP 非增量描述符分组，共享原 slab；UDP 不得选中不足以容纳其接收容量的增量尾部。 |

最后一项与 7.2.7 的 MSG_TRUNC 内核修复不同：正确记录实际消费量，并不能保证一个较短的增量尾部装得下调用者允许的数据报。0.2 原生混合负载暴露了该运行时组合问题，新增边界回归在原实现上复现了第 125 个 256 字节数据报被截断；解决方式是隔离描述符选择，不是隐藏截断或重试已丢失的数据。

## 上游来源

版本结论依据 tagged UAPI／实现、引入提交和发布记录；liburing `master` 手册是记录日的辅助资料，之后可能更新。

[initial]: https://github.com/torvalds/linux/commit/2b188cc1bb857a9d4701ae59aa7768b5124e262e
[v5.1]: https://raw.githubusercontent.com/torvalds/linux/v5.1/include/uapi/linux/io_uring.h
[releases]: https://www.kernel.org/releases.json
[setup]: https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_setup.2
[enter]: https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_enter.2
[register]: https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_register.2
[buf-ring]: https://raw.githubusercontent.com/axboe/liburing/master/man/io_uring_register_buf_ring.3
[network-history]: https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023
[vectored]: https://github.com/torvalds/linux/commit/6f02527729bd31ca4e473bff19fda4ccd5889148
[v6.13]: https://raw.githubusercontent.com/torvalds/linux/v6.13/include/uapi/linux/io_uring.h
[v6.15]: https://raw.githubusercontent.com/torvalds/linux/v6.15/include/uapi/linux/io_uring.h
[v6.16]: https://raw.githubusercontent.com/torvalds/linux/v6.16/include/uapi/linux/io_uring.h
[v6.17]: https://raw.githubusercontent.com/torvalds/linux/v6.17/include/uapi/linux/io_uring.h
[v6.18]: https://raw.githubusercontent.com/torvalds/linux/v6.18/include/uapi/linux/io_uring.h
[v6.19]: https://raw.githubusercontent.com/torvalds/linux/v6.19/include/uapi/linux/io_uring.h
[v7.0]: https://raw.githubusercontent.com/torvalds/linux/v7.0/include/uapi/linux/io_uring.h
[min-left]: https://github.com/torvalds/linux/commit/7deba791ad495ce1d7921683f4f7d1190fa210d1
[v7.1]: https://raw.githubusercontent.com/torvalds/linux/v7.1/include/uapi/linux/io_uring.h
[nodev]: https://github.com/torvalds/linux/commit/825f2764919fca61a88ab2f93dfdfd1d22566264
[v7.1-zcrx]: https://raw.githubusercontent.com/torvalds/linux/v7.1/include/uapi/linux/io_uring/zcrx.h
[v7.2-merge]: https://github.com/torvalds/linux/commit/9b40ba14edcdf70240af8114092a76f75f070774
[v7.2.7]: https://raw.githubusercontent.com/gregkh/linux/v7.2.7/include/uapi/linux/io_uring.h
[v7.2.7-zcrx]: https://raw.githubusercontent.com/gregkh/linux/v7.2.7/include/uapi/linux/io_uring/zcrx.h
[v7.2.8-zcrx]: https://raw.githubusercontent.com/gregkh/linux/v7.2.8/include/uapi/linux/io_uring/zcrx.h
[changelog-7.2.7]: https://cdn.kernel.org/pub/linux/kernel/v7.x/ChangeLog-7.2.7
[mshot-cap]: https://github.com/torvalds/linux/commit/6a8afb9fff64
[v6.17-net]: https://raw.githubusercontent.com/torvalds/linux/v6.17/io_uring/net.c
[zcrx-doc]: https://raw.githubusercontent.com/torvalds/linux/v6.15/Documentation/networking/iou-zcrx.rst
[v7.3-merge]: https://github.com/torvalds/linux/commit/f5437ff7299e47e76e52d37a2937a4b0f04e399f
[v7.3-zcrx]: https://raw.githubusercontent.com/torvalds/linux/v7.3-rc4/include/uapi/linux/io_uring/zcrx.h
[v6.2]: https://raw.githubusercontent.com/torvalds/linux/v6.2/include/uapi/linux/io_uring.h
[v6.6-net]: https://raw.githubusercontent.com/torvalds/linux/v6.6/io_uring/net.c
[v6.12-net]: https://raw.githubusercontent.com/torvalds/linux/v6.12/io_uring/net.c
[v6.18-net]: https://raw.githubusercontent.com/torvalds/linux/v6.18/io_uring/net.c
[v7.2.7-net]: https://raw.githubusercontent.com/gregkh/linux/v7.2.7/io_uring/net.c
