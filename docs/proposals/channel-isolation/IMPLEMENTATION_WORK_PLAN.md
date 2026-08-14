# QwenPaw Channel 隔离架构实施工作计划

## 1. 文档信息

- 状态：待开始（已按 main 2026-08-11 代码复核）
- 对应设计：`DESIGN.md`
- 目标：按设计完成 Channel 的环境隔离、进程隔离、stdio IPC、可靠投递和迁移
- 当前产品约束：每个 Agent 每种 `channel_key` 保持一个用户可见实例
- 发布策略：Phase 仅表示内部施工顺序；不是用户分期。所有 Gate 通过后一次性切换
  默认 Channel 执行路径，不长期维护两套用户可见流程。
- 进度规则：任务未经独立 Review 和最终验证不得标记完成；Gate 在阶段任务全部完成后
  单独验收

## 2. 已冻结基线

以下决策来自 `DESIGN.md`，实施中不得静默改变：

- [x] Core 保留 `ChannelManager`、`BaseChannel`、ACL、队列、AgentRequest/Event、平台
  无关渲染语义和 approval；Runner 负责平台原生 payload 编码。
- [x] `core` Channel 和现有 legacy Plugin Channel 保持外部行为兼容。
- [x] isolated Channel 使用每个默认 instance 一个 Runner 进程。
- [x] Core↔Runner 使用 stdio，不使用 loopback TCP 或本地端口。
- [x] stdio 使用 LSP `Content-Length` framing。
- [x] 上层消息语义使用 JSON-RPC 2.0；不采用完整 LSP 业务协议。
- [x] 借鉴 Matrix AS 的 transaction、ACK、retry、dedup，不采用 Matrix AS HTTP API。
- [x] 借鉴 MCP 的 command/args/env/cwd 和按需拉起；`uvx` 不是官方生产 launcher。
- [x] stdout 只承载协议，stderr 只承载日志；bootstrap 在导入 SDK 前完成 FD 分流。
- [x] secret value 不进入 JSON-RPC、hello、命令行或持久环境文件；默认通过一次性继承
  pipe/handle 注入，SDK 必须读取环境变量时才临时设置并立即清除。
- [x] Channel 源码随当前 QwenPaw 发布，不安装进 dependency environment。
- [x] dependency environment 只安装 lock 指定的第三方依赖。
- [x] 代码变化但 lock、ABI、平台和 condition set 不变时复用环境。
- [x] 安装、repair、doctor 严格校验；不兼容 environment 禁止启动。
- [x] 检测到不匹配后只能显式或策略允许的自动 repair；repair 失败保持停止，不能
  使用不兼容 environment。
- [x] v1 自动准备只允许由用户启用/启动 Channel 触发；health、list 和 Core 启动不
  隐式联网安装依赖。
- [x] 新配置/新声明只有在 candidate prepare 和 health 成功后才 commit；commit 前失败
  保留当前已提交配置及满足它的 active generation，不属于回退。
- [x] 当前声明已经 commit 或 active environment 已不匹配时，构建失败必须停止并
  `repair_required`；不得启动不满足当前声明的旧源码或旧 environment。
- [x] 入站 ACK 以 Inbox 持久化和幂等检查完成为准。
- [x] 普通媒体直接传 Content 定位符；Core 提供 effective 入站媒体工作目录；v1 不实现
  Core↔Runner media pipe。
- [x] Voice/Twilio 目标是由 Runner 持有公网 WebSocket、Webhook 和平台状态机；Core 只接收
  稳定事件并通过 `channel.send` 返回出站操作。若部署边界确实需要 Core-owned ingress，
  必须单独 ADR、capability 和 DTO 验收，不把 FastAPI socket 跨进程传递。
- [x] 本期不开放多实例，仅保留内部 `instance_id` 扩展空间。
- [x] 现有 Plugin 保留 legacy；新插件使用 isolated Channel Plugin SDK。
- [x] 本期只处理 Channel，不迁移其他 Plugin 类型。
- [x] `Runtime` 专用于 Agent 请求编排层；Channel 类、模块/目录、字段、状态和用户文案
  不使用 `runtime`。
- [x] descriptor 分离 `source_kind` 和 `process_mode`；驱动接口由 `process_mode` 唯一
  确定，不建立重复字段。
- [x] Console Channel 是 Core 控制面入口，保留 `process_mode=in_process` 和
  `BaseChannel` 驱动接口并参加兼容回归。
- [x] 扫码/设备码登录保留在 Core，是显式且有界的例外；不得导入平台 SDK，也不得引用
  Channel 内部符号（现有 WeChat 分支的跨层引用必须切断）。
- [x] bot 身份查重改为 config 级比较，身份字段由 descriptor 声明；不访问其他 Agent 的
  运行对象。接受命中范围由“正在运行”扩大为“已配置”。
- [x] 环境/依赖校验命令为 `channels verify-env`，不占用 `doctor` 名称；既有
  `qwenpaw doctor`（平台连通性）保持不变。
- [x] descriptor 声明环境变量透传白名单，`minimal_env` 按白名单构造；代理
  （`TELEGRAM_HTTP_PROXY`、`DISCORD_HTTP_PROXY`、`HTTP_PROXY`/`HTTPS_PROXY`）和 TLS
  （`SSL_CERT_FILE`）等真实部署约束必须能到达 Runner。
- [x] 平台网关地址注入（Feishu `domain` 的 URL 形态、WeCom/XiaoYi/Yuanbao 的 `ws_url`、
  QQ 的端点环境变量）是测试 mock 注入点，不是产品功能，不作为兼容目标，也不得为其调整
  架构实现；失效由测试侧处理。Feishu `domain` 的 `feishu`/`lark` 枚举值除外（ADR-033）。
- [x] Voice 重构后与 OneBot 一致：自行维护生命周期、监听和平台事件，Core 只处理传过来的
  数据。若 Runner-owned 经原型证明不可行，必须由独立 ADR 决定是否保留当前 Core-owned
  ingress，不将其作为自动后备选项。
- [x] bot 查重两侧都要求 `enabled=true`：提交配置为禁用时跳过检查，比较时只看其他 Agent
  中已启用的配置段。
- [x] ACL 身份不得跨发送者合并。Core 已在 merge 前按 ACL 身份切分批次，隔离后 Runner
  逐事件携带 `acl_sender_id`，该不变量必须保持。
- [x] `media_work_dir` 是新增的 Core 侧解析能力，不是既有实现的搬迁；所有需要入站媒体
  落盘的 Channel 使用统一的最终目录，不追加 Channel 子目录；各 Channel 的下载、命名、
  覆盖和清理逻辑保持不变（ADR-034）。
- [x] Voice 当前入口完全由 Core 持有；Runner-owned ingress 是新建目标，Core-owned 是
  现状而非“将来的备选”。
- [x] `direct_session`（Voice、SIP）当前同时绕过 Core 的 ACL gate 和 TaskTracker；本期
  保持该行为，补齐 ACL 属于独立的行为变更。

## 3. 状态和变更规则

```text
[ ] 未开始
[-] 实施中或等待 Review
[x] 当前任务的独立 Review 和最终验证均通过
[!] 阻塞
[~] 已完成结论因设计或新证据失效，需要重新验证
```

- 每个任务只维护一个进度记录块。
- 设计、协议、环境模型或进程拓扑变化必须先更新 `DESIGN.md`，再更新本计划。
- 任务不得顺手实现后续任务或无关重构。
- 每个任务必须能由一个实施 Chat 完成、验证并交给独立 Review；如果只完成其中一部分
  就无法形成可验证产物，应在开工前继续拆分，而不是在多个 Chat 中共用同一个任务号。
- 批量迁移任务必须逐个 Channel 留下实现和测试证据；批次只共享迁移套路，不共享
  “整体通过”的模糊结论。
- 实施 Chat 修改前必须先输出方案和 checklist，并等待确认。
- Review Chat 只审查指定任务和 Gate，不以 checkbox 代替证据。
- 代码任务使用 conda 环境 `qwenpaw`，Python 单测通过率必须为 100%。
- 每个任务完成前执行相关测试、`pre-commit` 和 `git diff --check`。

## 4. 阶段和 Gate

| 阶段 | 内容 | 状态 | Gate |
| --- | --- | --- | --- |
| Phase 0 | 边界、协议和纵向原型 | 未开始 | G0 |
| Phase 1 | Lock、环境和 Runner bootstrap | 未开始 | G1 |
| Phase 2 | 进程监督、Core 适配和 Catalog | 未开始 | G2 |
| Phase 3 | 可靠投递、媒体和切换恢复 | 未开始 | G3 |
| Phase 4 | 官方 Channel 正式迁移 | 未开始 | G4 |
| Phase 5 | Plugin SDK 和选择性 legacy 迁移 | 未开始 | G5 |
| Phase 6 | CLI/API/Console 和发布验证 | 未开始 | G6 |

### 4.1 执行依赖

- Gate 严格按 `G0 -> G1 -> G2 -> G3 -> G4 -> G5 -> G6` 推进；前一 Gate 未通过，
  后一阶段不得进入正式实施。
- 同一 Phase 默认按任务编号顺序执行。只有依赖图明确独立且用户批准时才可并行；并行
  必须使用独立工作树/分支，并先确认文件归属，多个实施 Chat 不得共享同一工作树。
- Phase 0 中 `CH-0-003` 完成 framing，`CH-0-004` 才构建 JSON-RPC；`CH-0-005`
  依赖前两者；`CH-0-007` 至 `CH-0-009` 依赖 `CH-0-001` 至 `CH-0-006`。
- Phase 1 中 installer 依赖 lock 和 spec；repair/doctor 依赖 installer；RunnerSpec 同时
  依赖 Phase 0 bootstrap 结论和跨平台解释器策略。
- Phase 2 先完成 spawn/supervision/generation，再接入 HostAdapter、Proxy、Registry
  和 Catalog；Phase 3 的切换与 Journal 依赖 Inbox、Delivery 和 Media 数据面稳定。
- Phase 4 的三个参考 Channel 复用 Phase 0 纵向产物，剩余三批迁移复用同一模板；
  Phase 5、Phase 6 分别在官方 Channel 隔离边界和 Plugin 边界稳定后推进。

## 5. 全局完成标准

- [ ] isolated Channel 的第三方 SDK 不再作为默认 Core 依赖安装。
- [ ] 两个 Channel 可以使用互不兼容的同名依赖并同时运行。
- [ ] 两个 Agent 可共享同一 environment，但 Runner、secret、checkpoint、日志和
  generation 完全隔离。
- [ ] 代码变化但 lock 不变时复用 environment。
- [ ] lock、ABI、平台、condition 或实际 distribution 不匹配时禁止启动。
- [ ] 未 commit 的候选构建失败保留当前合法 active generation；当前声明不匹配时停止，
  不启动不兼容环境。
- [ ] Runner 崩溃、EOF、Core 关闭和 stderr 高流量不会导致 Core 崩溃或死锁。
- [ ] 入站持久化 Inbox、出站 delivery ledger 和幂等去重可恢复。
- [ ] Core Channel、runner-process Channel 和 legacy Plugin Channel 可混合运行，现有
  行为无回归。
- [ ] Core 侧不保留平台入口：`routers/voice.py` 已删除，`twilio` 不在 Core 默认依赖；
  扫码登录是登记在案的有界例外。
- [ ] Channel 的存在性、身份字段、配置字段展示投影和环境变量透传均以 descriptor 为唯一
  事实来源；完整配置 value schema 仍按 Design §11.0 的 Pydantic/JSON Schema 边界校验。
- [ ] pip/source/conda、frozen desktop 和容器语义一致。
- [ ] Windows、Linux、macOS 验证通过。
- [ ] 相关 Python 单测通过率 100%，pre-commit 和 diff check 通过。

## 6. Phase 0：边界、协议和纵向原型

目标：在大规模迁移前冻结 Core/Runner 边界，验证 stdio 协议在三类入口中的可行性。
纵向原型使用 Phase 0 的协议一致性 harness、mock Host 和预构建 fixture environment，
不提前实现 Phase 1 至 Phase 3 的 Environment/Process/Host 基础设施。平台 Runner 代码
必须进入目标生产代码路径，不得创建随后整体丢弃的第二套平台实现；Phase 4 在正式
进程与环境管理机制上完成接入、迁移和兼容收尾。

### CH-0-001：Core/Runner 职责和兼容边界

- 状态：[x] 独立 Review 和最终验证通过

- [x] 盘点 `BaseChannel` 的 public/protected 接口。
- [x] 将 ACL、队列、AgentRequest/Event、Workspace、渲染和 approval 归入 Core。
- [x] 将平台 SDK、连接、原生事件解析、平台 API 和 checkpoint 归入 Runner。
- [x] 定义 effective `media_work_dir` 的解析规则、绝对化基准和 Channel 媒体模式清单。
  正常 Agent 的 `from_config` 规则为 `config.media_dir` → `workspace_dir / "media"` →
  `WORKING_DIR / "media"`；无 Agent workspace 的 `from_env` 兼容入口规则为
  `<CHANNEL>_MEDIA_DIR` → `WORKING_DIR / "media"`。Core 解析后经 prepare host context
  传给 Runner，Runner 不依赖 Workspace 对象自行推断。注意当前**不存在**集中解析器，且
  QQ、Telegram、Matrix、XiaoYi 的配置字段或透传链路不完整，因此本任务要记录补齐清单。
  入站落盘型 Channel 为 Discord、DingTalk、Feishu、QQ、Telegram、Mattermost、WeCom、
  Matrix、Slack、WeChat、XiaoYi、Yuanbao；OneBot 为定位符直传；MQTT、Voice、SIP 不提供
  入站媒体目录；Console/iMessage 的目录用途不属于该契约。最终目录统一平铺，不追加
  Channel 子目录（Design §9.1、ADR-034）。**本任务只冻结规则和清单，解析器实现属于
  `CH-2-004`；Phase 0 不实现 Core 侧基础设施。**
- [x] 盘点 `uses_manager_queue` 等调度差异；descriptor 显式声明 `dispatch_mode`，Voice 与
  SIP 保持 `direct_session`，不得把所有 Channel 强制接入 manager queue。记录
  `direct_session` 当前同时绕过 ACL gate 和 TaskTracker，本期不改变该行为（ADR-026）。
- [x] 定义 `ChannelHostAdapter`、`ChannelDriver` 和 `IsolatedChannelProxy`。
- [x] 证明 Core 不需要把 Python 对象传给 Runner。范围按 Design §8.4 的清单：`Path`、
  配置段、`Workspace`、`Event`；确认 `reply_future`/`reply_loop`/`incoming_message` 为死
  代码，`session_webhook` 系列为可序列化真实用法。
- [x] 确认 ACL 身份不跨发送者合并的不变量，并定义 Runner 逐事件携带 `acl_sender_id` 的
  映射（ADR-031）。
- [x] 定义 Core Channel、runner-process Channel 和 legacy Plugin Channel 混合运行的
  兼容调用表。

证据：职责矩阵和 override 基线见
`docs/proposals/channel-isolation/CH-0-001_CORE_RUNNER_BOUNDARY.md`；契约、Telegram 和
WeCom 测试共 `182 passed`；目标文件 `pre-commit` 全部通过，`git diff --check` 通过。
独立 Review 已通过（用户确认）；实现与修复提交为 `34e691b8` 和 `3cdf8f9d`。

验收：职责矩阵和接口边界可以覆盖现有内置 Channel、Voice 和 legacy Plugin。

### CH-0-002：标识、descriptor 和单实例模型

- 状态：[x] 独立 Review 和最终验证通过

本任务先冻结 `DESIGN.md` §10.1、§10.4、§11.0、§11.1 的 v1 契约；在同一任务内
实现 canonical encoder、Requirement canonicalizer、ID/目录键模型和 descriptor v1 validator，
并完成聚焦单测。直接依赖为 `CH-0-001`（已独立 Review 通过）；所属 Gate 为 `G0`。
`CH-2-006` 只负责消费这些模型并实现 Registry/实例解析，`CH-1-001`/`CH-1-002` 消费 lock、
condition 和 environment spec，`CH-6-001`/`CH-6-003` 消费配置字段展示投影，
`CH-6-007`/`CH-6-008` 分别收敛查重和扫码例外。

契约交付：

- [x] `channel_key`、`instance_id`、`environment_spec_id`、`environment_id` 的前缀、payload、
  canonical JSON、domain separator、SHA-256、安装 ID 持久化和 `dir1_` 目录键规则见 Design
  §10.1.1--§10.1.2；目录布局映射见 §10.4；跨平台 hash 向量见 §11.0.1、ADR-035。
- [x] Python ABI、release target registry 的 platform tag 成员校验、有限 condition domain、
  默认值展开和 digest 规则见 Design §10.1.3。
- [x] Requirement 的 project name、extras、specifier、marker、URL、排序和去重 canonical
  算法见 Design §10.1.4。
- [x] descriptor v1 closed object、required/nullable/empty 语义、entrypoint、依赖、条件、
  targets、capabilities、bot identity、secret default、配置投影边界和环境透传字段见 Design
  §11.0、§11.0.1、ADR-036。
- [x] `source_kind`、`process_mode`、`dispatch_mode`、`ingress_owner` 及从 process mode
  派生 `BaseChannel`/`ChannelDriver`/`IsolatedChannelProxy` 的唯一映射见 Design §4.1、
  §5.1--§5.3、§11.0。
- [x] Core/isolated dependency boundary、静态 descriptor 不 import 平台模块和 legacy Plugin
  合成限制见 Design §6.4、§10.3、§11.0、§12.1--§12.2。
- [x] Design §11.1 六张硬编码表的 owner、后续任务和本任务不做项已逐表列明；未声明字段只
  能在显式空值语义下表示“不参与”，validator 漏配必须返回 `descriptor_invalid`。
- [x] `channels.<channel_key>`、CLI、API、Console 单实例兼容面及内部 `instance_id` ownership
  约束见 Design §4.2、§14.3；本任务不增加多实例产品入口。

实现交付：

- [x] 实现含唯一 string escape、finite decimal JSON number 的受限 canonical JSON encoder、
  domain-separated SHA-256 helper 和 Design §10.1.4 Requirement canonicalizer；所有 digest
  只接收 canonicalized value。
- [x] 实现 `channel_key`、`instance_id`、`environment_spec_id`、`environment_id`、
  installation ID、ABI/platform 输入、release target registry 成员校验和 `dir_key` 的 value
  model、生成与严格校验函数。
- [x] 实现 descriptor v1 closed data model、交叉字段 validator 和 canonical digest；验证
  unknown field、secret default、Requirement、identity 和 capability 规则，静态校验不得
  import descriptor entrypoint；CH-0-002 不解析 Pydantic/JSON Schema，schema adapter 和
  投影一致性检查完整归入 `CH-2-006`。
- [x] 为 canonical、ID、目录键、descriptor 和单实例 ownership 添加聚焦单测；测试只使用
  pure value model，不提前实现 Registry/Catalog。

验证向量和证据要求：

- [x] canonical JSON 至少覆盖 ASCII/Unicode NFC、唯一 control character escape、literal
  slash/U+2028/U+2029、surrogate 拒绝、object key 重排、array 顺序、整数/decimal 边界、
  null、重复 key、binary float/bytes/Path 拒绝和 domain separator 隔离；固定向量不得读取
  host locale、路径规则或默认 JSON encoder，G0 在目标平台复用同一测试文件。
- [x] ID 向量至少覆盖三个平台 tag、三个 Python ABI、大小写敏感 `agent_id`、空 condition `{}`、
  lock/condition/platform/ABI 变化、release target registry 成员/非成员、repair 新 installation
  和短目录键碰撞校验。
- [x] descriptor validator 向量覆盖 closed object、每个 enum、entrypoint/process 映射、
  LocalizedText 空值/fallback/URL、secret default/identity 引用/值不外泄、Requirement
  canonicalization/重复折叠/`extra` marker 拒绝、requirement array 的 process-mode 约束、
  有限 condition/allowed-values、
  capability-ingress 组合、identity 排序与重复 name、allowlist、完整 descriptor digest fixture
  和 legacy 非 canonical key。
- [x] 单实例兼容测试证明两个 Agent 的同一 `channel_key` 得到不同 `instance_id`，同一 Agent
  的重复解析稳定，内部状态 key 不以 `channel_key` singleton 覆盖；测试执行使用 conda 环境
  `qwenpaw`，并通过 Python 单测、pre-commit 和 `git diff --check`。

验证证据：CH-0-002 聚焦测试 `102 passed`，`tests/unit/channel_isolation` 共
`113 passed`；目标 Python 和测试文件的 pre-commit 全部通过，`git diff --check`
通过。Design §11.0.1 的完整 descriptor fixture 从文档原文机械重算为
`8b05ef521e5f2ae268f90f704dd36f1fe1e8eb958182c1c0220ffe6405e7cdb8`，与实现固定向量一致。
独立 Review 已通过（用户确认）；实现与修复提交为 `ee21c660` 和 `91d46ebf`。

本任务明确不做：登记实际 builtin/Plugin descriptor；迁移或删除 Registry/Catalog 和 §11.1
硬编码表；生成或安装 lock；创建 environment/Runner/Proxy；改变现有 Pydantic config schema；
迁移 bot 查重或扫码登录；修改 CLI/API/Console、SDK、Channel 行为和前端。

验收：Design §10.1、§10.4、§11.0、§11.1 的契约由本任务实现的 canonical encoder、ID model、
descriptor validator 和聚焦单测机械验证；ADR-035/036 已确认。独立 Review 和最终验证
通过后，任务状态更新为 `[x]`。

### CH-0-003：stdio framing 和传输层

- 状态：[x] 独立 Review 和最终验证通过

- [x] 实现严格的 LSP `Content-Length` framing。
- [x] 处理半帧、粘包、非法 Header、重复 Header、超长 Header、非法 UTF-8 和 EOF。
- [x] 设置控制帧最大长度、Header 上限、读取超时和写入超时。
- [x] 实现单一写入器、并发写锁和有界写队列。
- [x] 定义 stdin/stdout 半关闭、broken pipe 和协议错误后的关闭规则。
- [x] 完成 Windows、Linux、macOS 的分片读写和 EOF 测试。

验收：纯 framing 层不依赖 Channel 业务方法，能够稳定传输任意合法 JSON message；
非法帧、超时、半关闭和并发写测试通过。实现位于
`src/qwenpaw/channel_protocol/framing.py`，测试和跨平台子进程 fixture 位于
`tests/unit/channel_isolation/test_ch_0_003_framing.py`、
`tests/unit/channel_isolation/test_ch_0_003_transport.py` 和
`tests/fixtures/channel_isolation/framing_peer.py`；不依赖 Channel 业务方法或 JSON-RPC。
审查修复覆盖本端显式关闭和 broken pipe 解除既有 pending receive、调用者取消后回收
内部读取 task，以及 Header 上限内超长十进制 `Content-Length` 返回 `FrameLimitError`
并关闭双向传输。聚焦测试 `48 passed`，`tests/unit/channel_isolation` 共 `161 passed`；
目标文件 pre-commit 全部通过，`git diff --check` 通过。独立 Review 已通过（用户确认）；
实现与修复提交为 `c94dd06f` 和 `a972409c`。

### CH-0-004：JSON-RPC 2.0、Schema 和生命周期

- 状态：[x] 独立 Review 和最终验证通过。

- [x] 实现 JSON-RPC 2.0 request、response、notification、error 和 cancel。
- [x] 实现 pending request 上限、request timeout、未知方法和重复 response 处理。
- [x] 定义 `runner.hello`、`channel.prepare/activate/quiesce/health/stop`、
  `channel.commit/lease_renew/generation_status`、`channel.send`、普通媒体定位符、
  `ingress.endpoint.register/update/unregister`、`host.state.*` 和 `request.cancel`
  的 schema。
- [x] `channel.prepare` 的 host context 定义跨平台绝对 `media_work_dir`；它只对需要
  入站落盘的 Channel 生效，不作为出站文件访问白名单。
- [x] 定义 Runner-owned ingress endpoint 的 host、port/path、可选 `public_base_url`、
  protocol、readiness、generation、quiesce 和 unregister 语义；定义 Voice event 的稳定
  `event_kind`、`connection_id`、sequence、session binding 和稳定错误码。
- [x] 定义 `setup` 产生并通过 `call.started` 上报的 `session_binding`、
  `platform_session_id=CallSid` 及 status callback 的幂等关闭映射；Core-owned 兼容入口的
  DTO 仅作为独立 capability 记录。
- [x] 实现持续 reader/dispatcher，使 request handler 可以发起反向 request；禁止在 reader
  loop 内等待业务 handler，覆盖嵌套调用与 response 乱序。
- [x] 定义 created、preparing、standby、active、quiescing、stopped、failed 状态转换。
- [x] 实现 protocol version、capability negotiation 和稳定错误码。
- [x] 校验 `channel_key`、`instance_id`、generation 和 environment identity。

验收：mock Core/Runner 能完成 hello、prepare、activate、commit、lease renewal、
health、cancel 和 stop；commit 前不能正式消费。非法状态转换、Schema 不匹配、超时和
协议版本不兼容均返回稳定结果。

实现位于 `src/qwenpaw/channel_protocol/models.py`、`rpc.py` 和 `lifecycle.py`，
公共导出与错误类型位于 `__init__.py`、`errors.py`；聚焦测试位于
`tests/unit/channel_isolation/test_ch_0_004_models.py`、`test_ch_0_004_rpc.py` 和
`test_ch_0_004_lifecycle.py`。当前聚焦测试命令
`conda run -n qwenpaw pytest -q tests/unit/channel_isolation/test_ch_0_004_models.py
tests/unit/channel_isolation/test_ch_0_004_lifecycle.py
tests/unit/channel_isolation/test_ch_0_004_rpc.py` 为 `28 passed`，
`conda run -n qwenpaw pytest -q tests/unit/channel_isolation` 为 `189 passed`；目标文件
pre-commit 全部通过，`git diff --check` 通过。独立 Review 已通过（用户确认）；实现与
修复提交为 `95836112`、`26958f06`、`339766f7` 和 `4cd74739`。任务状态更新为 `[x]`。

### CH-0-005：可靠事件、ACK 和幂等原型

- 状态：[x] 独立 Review 和最终验证通过

- [x] 定义 `event.batch`、`batch_id`、稳定 `event_id` 和
  accepted/duplicate/rejected ACK；rejected 带 reason code 与 `retryable`。
- [x] 定义 Runner 未收到 ACK 的重试和退避规则。
- [x] 定义 Core 在 Inbox 持久化后才 ACK 的边界。
- [x] 定义 OutboundDeliveryLedger、`delivery_id` 和 `unknown` 发送语义。
- [x] 注入 ACK 丢失、Core 重启、Runner 重启和重复事件。

验收：至少一次投递不会导致重复进入 Agent；ACK 丢失可以安全重试。

### CH-0-006：Runner bootstrap 和日志分流原型

- 状态：[-] 初始实现与验证通过，等待独立 Review 和最终验证

- [x] 在导入 Channel SDK 前校验 isolated Python、代码根和 manifest。
- [x] 保存协议输出句柄，并将普通 stdout/FD 1 输出导向 stderr。
- [x] 验证 `python -I <absolute-bootstrap>` 不依赖 dependency environment 中安装
  QwenPaw，并仅从显式 `code_root` 加载源码。
- [x] 清除 `PYTHONPATH`、user site 和不必要环境变量。
- [x] 验证飞书 SDK、普通 `print()` 和原生 FD 1 输出不会污染协议。
- [x] 验证 stderr 持续排空和日志 backpressure。
- [x] 验证后代进程不得继承协议专用句柄。

实现位于 `src/qwenpaw/channel_protocol/runner_bootstrap.py`，使用标准库 standalone
bootstrap、闭合的任务局部 manifest、显式绝对 `code_root` 和不可继承的私有协议句柄；
dependency environment 不需要安装 QwenPaw。聚焦测试位于
`tests/unit/channel_isolation/test_ch_0_006_bootstrap.py`，fixture 位于
`tests/fixtures/channel_isolation/bootstrap_code/`。当前聚焦测试 `10 passed`，
`tests/unit/channel_isolation` 共 `212 passed`；目标文件 pre-commit 全部通过，
`git diff --check` 通过。macOS 本机验证已通过；同一标准库 subprocess 测试文件需在
Linux、Windows 和 frozen desktop 发布 CI 复用执行。本记录保持 `[-]`，待独立 Review
和最终平台验证后才能标记完成。

验收：macOS、Linux、Windows 和 frozen desktop 的 stdout 污染、日志堵塞和句柄继承
测试通过。

### CH-0-007：飞书主动连接原型

- [ ] 将飞书平台连接逻辑拆为 `ChannelDriver`。
- [ ] 使用 mock Host 和预构建 fixture environment，不实现正式 Env/Process Manager。
- [ ] 验证主动长连接、鉴权、私聊/群聊/mention、文本/媒体/卡片和 streaming。
- [ ] 验证两个 Agent 的默认 instance 共用 environment，但 Runner、secret、checkpoint
  和状态独立。
- [ ] 验证 Runner 崩溃、重连和 Core 回复。

验收：两个 Agent 使用不同飞书配置并行收发，无配置、session 或状态串用。

### CH-0-008：OneBot ingress 原型

- [ ] 将 OneBot WebSocket ingress 和平台消息解析放入 Runner。当前 OneBot 已在 Channel
  模块内用 aiohttp 自持监听 socket，是唯一已具备 Runner-owned ingress 形态的 Channel，
  按生产路径复用而非重写。
- [ ] 使用 mock Host 和预构建 fixture environment，不实现正式 Env/Process Manager。
- [ ] 验证显式端口、动态端口发现、反向连接和平台消息上报。
- [ ] 明确平台 ingress 端口不用于 Core↔Runner stdio IPC。
- [ ] 保持 loopback 默认绑定与派生鉴权不变量：非 loopback 绑定强制要求 access token，
  未配置 token 时拒绝连接；endpoint DTO 上报绑定暴露面与鉴权状态。
- [ ] 端口冲突自愈重新绑定后必须触发 `ingress.endpoint.update`，Core 记录与实际监听
  一致。
- [ ] 区分 Runner 内部事件并发上限与 Core↔Runner 批次背压两层机制，分别给出诊断计数。
- [ ] 验证引用消息展开的反向平台调用（按 message id 拉取被引用消息及文件 URL）不阻塞
  ingress 读循环，且 `event.batch` 超时参数容纳该往返延迟。
- [ ] 验证 candidate 在 commit 前不监听或消费正式入口，Core 回复和 shutdown 可恢复。

验收：OneBot 外部连接不经过基础 Core↔Runner 协议，端口交接、断线、鉴权不变量和
backpressure 测试通过。

### CH-0-009：Voice/Twilio Runner-owned ingress 原型

- [ ] 先记录当前基线：Voice 入口**完全由 Core 持有**（`routers/voice.py` 的三条路由挂载
  在 Core app 上、Core 直接 import `twilio.request_validator`、Core `accept()` 后把 FastAPI
  `WebSocket` 对象交给 Channel、一次性 token 在 Core 进程内存 mint/validate、Cloudflare
  Tunnel 由 Channel 启动但指向 Core 端口）。本任务是新建 Runner-owned 入口，不是搬迁。
- [ ] 验证 Runner 自有 Webhook、签名校验、TwiML、一次性 WebSocket token 和
  ConversationRelay WebSocket；Core 不接收原始 HTTP/WebSocket 帧。token 的 mint 与
  validate 必须一起迁入 Runner。
- [ ] 使用 mock Host 和预构建 fixture environment；Twilio SDK、Tunnel 和入口库只在
  Runner environment 中导入。若继续以 FastAPI 语义实现 ConversationRelay，`fastapi`
  必须成为该 Channel lock 的显式依赖（当前它只是传递依赖）。
- [ ] 验证 Runner 动态/显式端口绑定，通过 `ingress.endpoint.register` 报告 endpoint、
  readiness 和 generation；Core 或受信任代理只路由 committed active generation。
- [ ] `twilio_auth_token` 通过 Core secret store 的受控 handle 注入 Runner，不进入 JSON；
  standby 不得修改 Twilio webhook/status callback。
- [ ] Runner 解析 `setup`、`prompt`、`interrupt`、`dtmf`、status callback，并通过
  `event.batch` 提交 `call.started`、`message.query`、`call.interrupted`、`dtmf`、
  `call.closed` 等稳定事件。
- [ ] 验证 `event.batch ACK -> Core direct_session` 和 `channel.send -> Runner ->
  ConversationRelay text/end` 闭环；入站 ACK 不等待 Agent 执行，出站携带
  `delivery_id` 并记录未知结果。
- [ ] 验证 `CallSid -> connection_id -> generation` binding、token TTL、原子单次消费、
  逐连接顺序、有界背压、过载关闭、断线、半关闭和旧 generation fencing。
- [ ] 验证 quiesce 时停止新连接、排空已有通话并通过 endpoint unregister/update 完成
  切换；当前不实现音频 pipe。
- [ ] 若 Runner-owned 原型因部署边界无法成立，单独记录 ADR 和最小 Core-owned 兼容方案，
  不把兼容桥接混入默认 Voice 协议。

验收：Voice 完成 Runner-owned Webhook、实时 WebSocket、Core query/reply、endpoint
注册、切换和 shutdown 故障矩阵；兼容方案仅在有独立 ADR 时验收。

### G0 Gate

- [ ] 职责、descriptor、stdio framing、JSON-RPC、可靠投递、媒体和 Runner-owned Voice
  ingress 边界冻结；如需 Core-owned 兼容入口，必须有独立 ADR。
- [ ] CH-0-002 的 canonical encoder、ID/目录键模型和 descriptor v1 validator 已通过聚焦
  单测与独立 Review；目标平台运行同一固定 hash 向量得到一致结果。
- [ ] 飞书、OneBot、Voice/Twilio 三条纵向原型分别通过。
- [ ] 无阻塞当前 Phase 的 P0/P1。

## 7. Phase 1：Lock、Environment 和 Bootstrap

### CH-1-001：Lock 和发布制品

- [ ] 定义每个 Channel 的依赖输入和 conditional config domain。
- [ ] 生成目标 Python ABI、platform tag 和 condition set 的完整 lock 矩阵。
- [ ] lock 包含精确版本、传递依赖、environment marker 和 wheel hash。
- [ ] 生成 manifest，保证每个目标 key 唯一映射一个 lock。
- [ ] 明确离线安装、缓存和缺失 wheel 的 `unsupported_platform` 状态。
- [ ] 不得假设当前主环境已有的库“本来就在”：`aiohttp`（OneBot 需要）和 `fastapi`
  （Voice 若沿用 FastAPI 语义需要）目前都不是 `pyproject.toml` 声明的直接依赖，仅为传递
  依赖，必须在对应 Channel lock 中显式声明。盘点时逐 Channel 核实实际 import 与声明的
  差集。

### CH-1-002：Environment Spec 和严格校验

- [ ] 实现 environment spec 选择和严格 manifest 校验。
- [ ] 验证不继承主环境 `site-packages`、user site 和 `PYTHONPATH`。
- [ ] 依赖不匹配时进入 `repair_required`，不得静默使用旧环境。
- [ ] 校验 Python ABI、platform tag、condition set、distribution inventory、适用时的
  direct URL、安装时记录的 wheel provenance 和 installed files `RECORD` 完整性。

验收：同一 descriptor/config 在目标平台只选择唯一合法 environment spec；不匹配时
明确返回 `unsupported_platform`、`config_invalid` 或 `repair_required`。

### CH-1-003：Environment Installer 和原子发布

- [ ] 实现 staging venv、只读/不可变安装、原子 rename 和安装锁。
- [ ] 按 lock 和 wheel hash 安装，不解析未锁定依赖或现场构建 sdist。
- [ ] 处理离线缓存、缺失 wheel、磁盘不足、并发安装和失败 staging 清理。
- [ ] 安装完成后写入不可变 install manifest，并在严格校验后才发布 environment。
- [ ] 验证已有 environment 不被原地覆盖或升级。

验收：安装失败不产生可启动 environment；并发安装只发布一个完整、严格匹配的不可变
environment。

### CH-1-004：Environment Repair、Doctor 和清理

- [ ] 实现 repair、doctor 的严格校验、operation progress 和诊断结果。
- [ ] repair 在同一 spec 下创建新的不可变 `environment_id`，不覆盖旧安装。
- [ ] 实现孤儿 staging、失效 environment 和缓存的安全清理策略。
- [ ] 正在被 Runner lease 引用的 environment 禁止清理。
- [ ] 验证 repair 失败保持停止，不切换 pointer，也不使用不兼容 environment。

验收：doctor 能解释不匹配项；repair 和清理在失败、并发及 active lease 下不会破坏
当前合法 environment。

### CH-1-005：跨平台解释器策略

- [ ] 实现 desktop bundled Python、pip/source/conda 和 container interpreter 选择。
- [ ] 验证 `sys.executable` 不被错误当作 frozen desktop 的 Python 解释器。
- [ ] 验证 Windows、macOS、Linux 的路径、venv 启动和进程树清理策略。

验收：四种安装形态使用同一 environment/lock 语义，不共享主环境 site-packages。

### CH-1-006：RunnerSpec 和受控 bootstrap

- [ ] 实现 command/args/env/cwd 的 RunnerSpec。
- [ ] 实现 `python -I <absolute-bootstrap>` 和显式 `code_root` 加载；发布物只读，
  source/editable 安装不得依赖 ambient cwd 或 `PYTHONPATH`。
- [ ] 实现协议 FD、日志 FD 和后代句柄白名单。
- [ ] 实现最小环境变量和一次性 secret pipe/handle 注入；SDK 必须读取环境变量时临时
  设置并在初始化后清除。
- [ ] 按 descriptor 的透传白名单构造 `minimal_env`，不从 Core 环境无条件继承，也不把代理
  和证书变量一并清掉。验证 Telegram/Discord/Slack 的代理变量和 `SSL_CERT_FILE` 在隔离后
  仍生效（ADR-030）。
- [ ] 证明 secret value 不进入 JSON-RPC、hello、日志或命令行。

### G1 Gate

- [ ] 空 Runner 可在三平台启动、hello、prepare、health、stop。
- [ ] lock、manifest、ABI、platform 和环境完整性校验通过。
- [ ] 源码变化不触发环境重建。

## 8. Phase 2：进程监督、Proxy 和 Catalog

### CH-2-001：Runner Spawn 和 IPC I/O

- [ ] 实现 spawn、hello、prepare、activate、quiesce、stop、terminate、kill。
- [ ] 持续读取 stdout 协议和 stderr 日志。
- [ ] 处理 stdin/stdout/stderr 关闭和写入背压。

验收：Process Manager 可以可靠启动、握手、发送 RPC、停止空 Runner，并持续排空
stderr。

### CH-2-002：进程监督和故障恢复

- [ ] 处理 EOF、退出码、超时、孤儿进程和重启退避。
- [ ] Windows 使用 Job Object 或等价机制。
- [ ] 实现连续失败熔断、诊断日志和 Core/Runner 关闭顺序。

验收：Runner 崩溃、无响应和日志高流量不会导致 Core 崩溃或死锁。

### CH-2-003：Generation、Lease 和 fencing

- [ ] 实现 instance generation 和 active lease。
- [ ] 实现不可猜测 lease token、TTL 和 `channel.lease_renew`；IPC 断开、token 不匹配或
  lease 过期后停止 Runner 消费。
- [ ] 所有 inbound、state、delivery 写操作携带 generation。
- [ ] 所有 ingress endpoint register/update/unregister 和相关 event/send 操作携带并校验
  generation；连接不在 generation 之间迁移。
- [ ] Core 重启不重新附着旧 Runner。
- [ ] 验证旧 generation 不能写入 inbox、state、checkpoint 或 delivery。

### CH-2-004：ChannelHostAdapter

- [ ] Core 保留 ACL、队列、debounce、AgentRequest、Event、approval 和平台无关
  渲染语义；Runner 负责平台原生 payload 编码。
- [ ] 定义 Core 侧配置、session、ACL、queue 和 Agent 生命周期适配。
- [ ] 按 `CH-0-001` 定义的规则**实现** Core 侧 `media_work_dir` 解析器（当前无集中实现，
  属新增代码），确保其可用并通过 prepare host context 传入 Runner。配置启动统一解析为
  `config.media_dir` → `workspace_dir/media` → `WORKING_DIR/media`，不追加 Channel 子目录；
  相对配置值绝对化，不依赖 Runner cwd（Design §9.1、ADR-034）。
- [ ] 将 `conversation` 映射为 Core `session_id`，将 `sender_name` 映射为兼容
  `meta["user_name"]`，并保证 merge 后保留实际 `acl_sender_id`。

验收：现有 `BaseChannel` 调用契约可以映射为稳定的 Core↔Runner DTO 和 delivery 操作。

### CH-2-005：IsolatedChannelProxy

- [ ] Proxy 实现 ChannelManager 所需的兼容方法。
- [ ] 将 send、typing、reaction、media 转换为 RPC。
- [ ] 对不支持 capability 返回稳定错误。
- [ ] 验证 proxy 与 legacy `BaseChannel` 并存。

### CH-2-006：Descriptor Registry 和实例解析

- [ ] 实现静态 descriptor 枚举和 Channel 实例解析两层 Registry。
- [ ] 复用 CH-0-002 的 descriptor model/validator；实现 builtin Pydantic JSON Schema
  adapter 和 `config_fields` 投影一致性检查，不得复制或改写 canonical/ID/descriptor 算法。
- [ ] 支持 Core Channel、runner-process Channel 和 legacy Plugin Channel 混合运行。
- [ ] legacy descriptor 从既有 PluginRegistration 合成。
- [ ] 禁止通过 import 成功与否推断 Channel 存在性。

验收：Registry 可以在不 import 平台 SDK、不启动 Runner 的情况下解析所有静态
descriptor，并根据 `process_mode` 返回正确 Channel 实例和驱动接口类型。

### CH-2-007：Catalog 和状态聚合

- [ ] Catalog 展示 source/process 分类、派生驱动接口以及 install、instance、platform
  状态。
- [ ] 拆分 install、instance、platform 三个状态维度。
- [ ] 验证未安装、repair_required、配置错误、Runner 崩溃和平台鉴权错误不会混淆。
- [ ] 验证 Catalog 不以 import 成功与否判断 Channel 是否存在。

### G2 Gate

- [ ] Console/其他 in-process Channel、runner-process Channel 和 legacy Plugin Channel
  可同时运行。
- [ ] 一个 isolated Runner 崩溃不会影响 Core 或其他 Channel。
- [ ] 现有 `ChannelManager` API 和单实例配置保持兼容。

## 9. Phase 3：可靠投递、媒体和切换恢复

### CH-3-001：InboundEventStore、ACK 和去重

- [ ] 实现 durable inbox、batch/event dedup 和 accepted/duplicate ACK。
- [ ] 实现逐事件 rejected/retryable 语义，永久 poison event 不得无限重试。
- [ ] 保证 Inbox 和幂等结果提交后才返回 ACK。
- [ ] 处理 ACK 丢失、重复 batch/event、Runner 重启和 Core 重启。
- [ ] 实现事件保留、清理和 pending/accepted/processed/failed 状态可观测性。

验收：相同平台事件在 ACK 丢失和任一侧重启后仍只进入 Agent 一次，Runner 可以使用
相同 `batch_id` 安全重试。

### CH-3-002：OutboundDeliveryLedger 和未知结果

- [ ] 实现 delivery state：requested、sending、acknowledged、failed、timeout、unknown。
- [ ] 将不可变 `delivery_id` 贯穿 Core、RPC、Runner 和平台发送结果。
- [ ] 区分可安全重试、平台支持幂等键和结果未知三种情况。
- [ ] 处理发送成功但 response 丢失、Runner 崩溃、Core 重启和状态迟到。
- [ ] 为 CLI/API/Console 提供稳定的 delivery 诊断字段。

验收：任何 response 丢失都不会触发无条件重复发送；可恢复状态和 `unknown` 状态有
明确证据及运维诊断。

### CH-3-003：媒体定位与兼容回归

- [ ] 定义普通附件的统一媒体定位符：直接传现有 Content 字段中的本地路径、`file://`
  URI 或 `http(s)://` URL，并保留 `filename`、mime 等元数据。
- [ ] 保持入站和出站现有流程；Core 不复制、下载或改写普通媒体定位符，Runner 继续按
  Channel 原有逻辑处理路径或 URL。
- [ ] 保持现有入站落盘流程：Core 解析 effective `media_work_dir`，Runner 将文件直接写入
  该最终目录，不追加 Channel 子目录；Runner 继续使用各 Channel 现有的文件名、覆盖、
  下载和 URL/路径处理逻辑。本任务不迁移既有文件、不引入新的下载算法，并盘点
  `config/utils.py` 的历史路径改写逻辑（ADR-034）。
- [ ] 为所有入站落盘型 Channel 补齐 `media_dir` 配置入口；保留 `from_env` 兼容入口的
  `*_MEDIA_DIR` 设置，并补齐缺失的 QQ、Telegram、Matrix、Slack、Yuanbao 环境变量读取。
  `from_config` 不读取环境变量，环境变量不覆盖显式配置。
- [ ] 验证 Agent 出站的任意可访问路径不受 `media_work_dir` 限制；Runner stop、ACK 或
  generation 切换不新增自动删除行为。
- [ ] 验证路径定位符兼容桌面等非 media 目录文件；验证 `file://`、Windows 路径和
  OneBot 平台 URL 的双向回归。
- [ ] 验证大文件只传定位字符串，文件字节不进入 JSON，不因文件大小触发 Base64、pipe
  或额外复制。
- [ ] 验证当前 Voice/Twilio ConversationRelay 只传文本和控制 DTO，不依赖二进制
  data plane。
- [ ] 将 Twilio Media Streams、Core-owned STT/TTS 或 SIP PCM 跨边界记录为未来版本化
  协议触发条件；本任务不预实现方法、capability 或数据面。

### CH-3-004：Staging、Activate 和 Commit

- [ ] 实现候选 Runner 健康门禁和 standby。
- [ ] 实现 quiesce、checkpoint、activate 和唯一 commit point。
- [ ] candidate 在 CAS 前只持有 provisional lease；pointer/generation CAS 后确认 committed
  lease，确认丢失时通过只读 generation status 恢复。
- [ ] quiesce 停止新入站/新发送并有界排空；pointer 只保存 config/secret revision，
  不复制配置正文或 secret value。
- [ ] 新配置/新声明在 candidate prepare 和 health 成功前不得成为 current；commit 前
  失败保留旧的已提交配置和 active generation。
- [ ] 旧 generation 失去 lease 后不得继续写入。

验收：候选 Runner 在 commit 前不消费正式事件，切换只产生一个 active generation。

### CH-3-005：Operation Journal 和崩溃恢复

- [ ] 实现 operation journal、崩溃恢复和孤儿清理。
- [ ] 定义 current pointer、CAS 前置条件和原子 replace 语义。
- [ ] 注入 prepare、quiesce、checkpoint、commit、activate 各步骤的 Core 崩溃。
- [ ] 验证不兼容声明下不启动旧版本作为回退。

### G3 Gate

- [ ] ACK、幂等、媒体、切换和崩溃矩阵通过。
- [ ] 不兼容环境不会启动或作为回退。
- [ ] 仅仍满足当前已提交声明的 active generation 可以继续服务；不得启动旧版本作为
  回退。

## 10. Phase 4：官方 Channel 正式迁移

### CH-4-001：飞书正式迁移

- [ ] 复用 `CH-0-007` 的生产路径原型，不重新实现第二套 Runner。
- [ ] 平台连接、鉴权、事件解析、文本/媒体/卡片完全进入 Runner。
- [ ] Core 保留 ACL、session、Agent 和平台无关渲染语义；Runner 负责飞书原生文本、
  卡片和媒体 payload 编码。
- [ ] 保持现有配置、私聊、群聊、mention、streaming 和媒体行为。
- [ ] 删除 Core 对飞书 SDK 的进程内 import。

验收：Phase 0 原型上的临时限制被清除，飞书全链路、依赖移除和兼容回归达到发布标准。

### CH-4-002：OneBot 正式迁移

- [ ] 复用 `CH-0-008` 的生产路径原型，不重新实现第二套 ingress。
- [ ] Runner 承担 WebSocket ingress 和平台消息解析。
- [ ] 支持显式端口与动态端口发现。
- [ ] 明确这些端口仅属于平台 ingress，不得用于 Core↔Runner IPC。
- [ ] 验证 candidate 在 commit 前不监听或消费正式入口。

验收：OneBot 现有配置和行为回归通过，端口交接、反向连接和隔离故障达到发布标准。

### CH-4-003：Voice/Twilio 正式迁移

- [ ] 复用 `CH-0-009` 的生产路径原型，不重新实现第二套 Voice data plane。
- [ ] Runner 承担 Webhook、签名验证、TwiML、ConversationRelay WebSocket、status callback、
  Tunnel/反向代理入口和平台消息适配；Core 只接收稳定事件并返回 `channel.send`。
- [ ] 删除 Core 侧 `routers/voice.py` 的三条路由及其在 Core app 上的挂载，并从 Core 默认
  依赖中移除 `twilio`。只要二者仍在 Core，本任务不得视为完成。
- [ ] Tunnel 由指向 Core 端口改为指向 Runner 自身入口，移除对 Core 端口发现的依赖。
- [ ] Runner 通过 endpoint register/update/unregister 报告入口；Core/受信任代理只把正式
  流量路由到 committed active generation，切换窗口暂停新连接准入或返回明确 busy/error。
- [ ] 确认迁移后 Runner 不直接复用 Core 的 `ProcessHandler`、ChannelManager 或数据库；
  保持 Voice 的 `direct_session` 行为和 ConversationRelay 文本状态机。
- [ ] 目标形态与 OneBot 一致：Voice 自行维护生命周期、监听和平台事件，Core 只处理传过来
  的数据，不再持有 socket 对象。若原型证明 Runner-owned 确实不可行，必须由独立 ADR 决定
  是否保留当前 Core-owned ingress；即便如此平台 SDK 仍留在 Runner，不得把 Core socket
  对象或原始 HTTP/WebSocket 帧传给 Runner。
- [ ] 验证签名、TwiML、token、ConversationRelay WebSocket 文本消息、断线和旧 generation
  fencing、endpoint 切换、有界背压和结果不明确的发送；不把当前 Voice 标记为 raw media
  pipe 使用者。

验收：Voice/Twilio 的 Runner-owned Webhook、ConversationRelay 文本会话、依赖隔离、
endpoint 切换和故障矩阵达到发布标准；Core-owned 兼容实现仅按独立 ADR 验收。

### CH-4-004：标准主动连接 Channel 批次

- [ ] 迁移 Telegram、Discord、Slack、Matrix 和 MQTT。
- [ ] 逐个盘点 polling/WebSocket/MQTT 连接、订阅、checkpoint、重连和 shutdown 语义。
- [ ] 逐个确认入站落盘 Channel 使用同一解析后的最终 `media_work_dir` 并平铺写入；各自
  下载、命名、覆盖和清理逻辑与迁移前一致（ADR-034）。
- [ ] 每个 Channel 分别建立 descriptor、lock/manifest、Runner entrypoint 和 capability。
- [ ] 分别回归 ACL 身份、session、群聊/私聊、mention、streaming、媒体、typing/reaction
  和主动发送中原本支持的能力。
- [ ] 每个 Channel 完成两个 Agent 并行、重复事件、平台断线和 Runner 重启测试。
- [ ] 逐个移除 Core 默认环境中的平台 SDK 依赖和进程内 import。

验收：五个 Channel 均有独立的 contract、行为回归、依赖移除和隔离证据；任一 Channel
未完成时本任务不得整体通过。

### CH-4-005：企业 SDK 和复杂行为 Channel 批次

- [ ] 迁移 WeCom、DingTalk、QQ、WeChat 和 Mattermost。
- [ ] 逐个盘点企业鉴权、token 刷新、群聊/私聊、mention、卡片、媒体和平台限流。
- [ ] 不为 WeCom `ws_url`、QQ 端点环境变量等 mock 注入点做兼容设计（ADR-033）；WeChat
  迁移时解除 §14.5 扫码模块对 `wechat.client` 私有符号的引用。
- [ ] WeCom 默认 `share_session_in_group=true`，是 ACL 身份不跨发送者合并的主要压力场景，
  必须有针对性回归（ADR-031）。
- [ ] 移除 `manager.py` 中 `ch.channel == "dingtalk"` 的 Core 侧诊断分支。
- [ ] 每个 Channel 分别建立 descriptor、lock/manifest、Runner entrypoint 和 capability。
- [ ] 保持 `acl_sender_id`、`sender_name`、session、approval、streaming 和发送目标解析行为。
- [ ] 每个 Channel 完成两个 Agent 并行、鉴权失效、限流、断线、重启和 shutdown 测试。
- [ ] 逐个移除 Core 默认环境中的可迁移 SDK 依赖和进程内 import。

验收：五个 Channel 的企业平台特定行为与 Core 通用消息编排均无回归，并分别留下
测试证据。

### CH-4-006：平台特定和系统耦合 Channel 批次

- [ ] 评估并迁移 XiaoYi、Yuanbao、iMessage 和 SIP；不适合隔离的必须明确保留
  `process_mode=in_process`。
- [ ] XiaoYi/Yuanbao 的 `ws_url` 属 mock 注入点，不做兼容设计（ADR-033）；迁移时以官方
  网关路径为准，不保留其附带的备用连接关闭行为作为回归项。
- [ ] SIP 与 Voice 同为 `direct_session`，保持当前不经过 Core ACL gate 与 TaskTracker 的
  行为（ADR-026）。
- [ ] 逐个评估私有 SDK、协议差异、本机权限、系统服务、实时媒体和入口所有权。
- [ ] 能够隔离的 Channel 分别建立 descriptor、lock/manifest、Runner entrypoint 和
  capability；保留 Core 的 Channel 记录不可隔离原因和职责边界。
- [ ] 不因“全部迁移”要求突破 Core-owned 系统权限、Webhook 或实时媒体边界。
- [ ] 每个 Channel 分别完成行为回归、权限、故障、Catalog 状态和支持平台测试。

验收：四个 Channel 均有明确的 `runner_process` 或 `in_process` 结论、对应驱动接口、
实现边界和独立证据，现有系统能力和用户可见行为无回归。

### G4 Gate

- [ ] 参考 Channel 和迁移批次完成全部行为回归。
- [ ] Core 不导入已迁移 Channel 的第三方 SDK；`routers/voice.py` 的路由与挂载已删除，
  `twilio` 不在 Core 默认依赖中。扫码登录模块按 ADR-027 是登记在案的例外，且不导入平台
  SDK、不引用 Channel 内部符号。
- [ ] 所有入站落盘 Channel 的最终目录解析和配置/env 入口一致；共享 session 群聊的 ACL
  身份不跨发送者合并。
- [ ] Console 及明确保留 `process_mode=in_process` 的 Channel 行为和 Catalog 状态无回归。
- [ ] Core Channel、runner-process Channel 和 legacy Plugin Channel 混合运行无回归。

## 11. Phase 5：Isolated Plugin SDK 和 Legacy 迁移

### CH-5-001：Isolated Plugin 描述、依赖和安装模型

- [ ] 定义静态 plugin descriptor 和版本兼容矩阵。
- [ ] 冻结 isolated Plugin artifact 的完整版本化 config value schema；descriptor
  `config_fields` 只作为该 schema 的 UI 投影，不替代运行时验证。
- [ ] 定义插件依赖声明、lock、wheel hash 和支持目标。
- [ ] 定义插件源码不进入 dependency environment 的加载方式。
- [ ] 定义 plugin artifact digest、来源 metadata 和 `code_root` 校验边界；仅复用产品
  已有可信签名链，本期不新建插件 PKI。
- [ ] 接入 Catalog、installer、repair、doctor 和卸载清理保护。

验收：Core 无需 import 插件业务模块即可枚举、校验并安装 isolated Channel Plugin，
不支持的平台和协议版本得到稳定状态。

### CH-5-002：Isolated Plugin Runner contract 和开发工具

- [ ] 定义 isolated `ChannelDriver` entrypoint 和生命周期接口。
- [ ] 提供 plugin handshake、capability、配置、secret、media、checkpoint 和 delivery
  contract。
- [ ] 提供本地开发 launcher；`uvx` 只能作为可选开发模式。
- [ ] 提供最小示例插件、协议一致性测试套件和版本兼容测试。
- [ ] 验证插件 stdout 污染、崩溃、超时和不支持 capability 不影响 Core。

验收：示例插件可在隔离 environment 中启动、握手、收发、处理媒体并停止；SDK 合同
测试可由第三方插件独立运行。

### CH-5-003：Legacy Plugin compatibility

- [ ] 保留 `register_channel(channel_class=...)`。
- [ ] legacy descriptor 从已加载注册信息合成。
- [ ] legacy 与 isolated 可同时运行。
- [ ] 旧插件配置、API、UI 和消息行为无回归。

### CH-5-004：选择性 Legacy → Isolated 迁移

- [ ] 以仓库现有 `plugins/channel/azure_bot` 作为代表性 legacy Channel Plugin，先记录
  迁移前 contract 基线，再完成拆分和迁移。
- [ ] 迁移配置、secret、媒体、checkpoint、日志和 instance 状态。
- [ ] 保持原 plugin key、配置 schema、自动表单、API 和消息行为兼容。
- [ ] 若迁移尚未 commit 且原 legacy 实例仍 active、声明匹配，则保持原路径；不得将
  新的 isolated 配置静默改成 legacy 启动。
- [ ] 不要求任意旧插件自动迁移。

### G5 Gate

- [ ] 新 isolated Plugin 可独立安装、校验、启动和通信。
- [ ] Plugin SDK 的静态安装模型和 Runner contract 均有独立测试证据。
- [ ] legacy Plugin 行为保持兼容。
- [ ] Plugin 不影响官方 Channel 的 lock、Catalog 和隔离执行边界。

## 12. Phase 6：CLI、API、Console 和一次性发布验证

### CH-6-001：CLI 和运维操作

- [ ] 保持 `channels.<channel_key>` 配置兼容。
- [ ] 实现 `channels list/install/repair/verify-env/restart` 的运维语义。环境校验命令命名
  为 `verify-env`，不占用 `doctor`（ADR-029）。
- [ ] 保持既有 `channels list`、`channels config`、`channels send` 兼容：`list` 输出兼容；
  `config` 的显示名和 `config_fields` 可表达的 projection 按 Design §11.1 收敛到 descriptor，
  未投影的 array/object/float 等字段继续从完整 Pydantic/JSON Schema（Plugin 为 artifact
  schema）取得类型、默认值和交互渲染；`channels send` 经由 Runner 的 `channel.send` 执行，
  Runner 未运行时返回明确错误，不绕过 Runner 直连平台 SDK。
- [ ] 明确 `channels verify-env` 不产生网络 I/O，与顶层 `qwenpaw doctor` 的平台连通性
  探测语义分离，并在帮助文本和文档中区分。
- [ ] health/list/Core 启动不触发安装；显式 install/repair 或用户启用/启动 Channel 时
  允许自动准备，并返回可查询的 operation id。
- [ ] 不开放多实例 UI 或 API。

验收：CLI 能区分 environment 操作和默认 instance 操作，正确报告 repair_required，且
`verify-env` 与 `doctor` 的结果不会被用户混淆。

### CH-6-002：API、Catalog 和状态模型

- [ ] 实现静态 descriptor Catalog 和 install/instance/platform 三维状态。
- [ ] 展示 source/process 分类、派生驱动接口以及 install、instance、platform 和 repair
  状态。
- [ ] 保持现有按 `channel_key` 的单实例 API 兼容适配。
- [ ] 不通过 import 成功与否判断 Channel 是否存在。

### CH-6-003：Console 和文档

- [ ] Channel 列表以 descriptor 为事实来源。
- [ ] 明确 source/process 分类、派生驱动接口、legacy 标识、未安装和 repair_required
  状态。
- [ ] 现有配置字段、ACL、streaming 和文档链接保持兼容。

### CH-6-004：安装形态发布验证

- [ ] 验证 Desktop bundled Python、pip/source/conda 和 container。
- [ ] 验证源码定位、发布物只读 `code_root`、source/editable 显式仓库根、基础解释器
  选择和 venv 创建语义。
- [ ] 验证各安装形态不继承主环境 site-packages、user site 或 `PYTHONPATH`。
- [ ] 验证安装包、离线 wheel 缓存和首次环境准备流程。

验收：四种安装形态使用相同的 descriptor、lock、manifest、environment 和 Runner
协议语义，差异仅限基础解释器及发布物定位。

### CH-6-005：OS、Python ABI 和架构矩阵

- [ ] 验证 macOS Intel/Apple Silicon、Windows 11、Linux x86_64。
- [ ] 验证 Python 3.11、3.12、3.13 的 lock/ABI 矩阵。
- [ ] 验证跨平台路径、进程树、FD/handle、原子 replace、venv 和 wheel 可用性。
- [ ] 将支持矩阵接入 CI 或发布前可重复运行的自动化任务。

验收：所有支持的 OS、架构和 Python ABI 组合都有 lock/wheel 结果及自动化验证证据；
不支持组合稳定显示 `unsupported_platform`。

### CH-6-006：故障恢复和质量门禁

- [ ] 验证安装失败、修复失败、Core 崩溃、Runner 崩溃和回滚。
- [ ] 切换补偿只允许继续使用仍满足当前已提交声明且已验证的 active generation；
  未 commit 的候选失败不改变 current。当前声明已 commit 且无兼容 environment 时
  必须停止并报告 `repair_required`，不得启动旧版本回退。

### CH-6-007：Bot 身份查重收敛

依赖 `CH-0-002` 的 `bot_identity_fields` 字段定义和 `CH-2-006` 的 Registry。

- [ ] 将 `conflict.py` 的 `_CHANNEL_IDENTITY_FIELDS` 硬编码表收敛到 descriptor 的
  `bot_identity_fields`，保留现有归一化规则（`homeserver`/`url` 去尾部斜杠、值 strip）和
  Discord、Telegram、Slack、Mattermost、WeChat 的 secret token 查重。
- [ ] 改为 config 级比较：枚举已配置 Agent（与 `/agents` 列表同源）后读取各自
  `channels.<key>` 配置段提取身份值，不再遍历其他 Agent 的 `channel_manager.channels`，
  也不读取其内存态 `Config`。
- [ ] 覆盖现有实现遗漏的内置 Channel 与 Plugin Channel；descriptor 未声明该字段表示
  “不参与查重”，与漏配区分并可被校验发现。
- [ ] 保持告警式语义：命中不阻塞保存也不阻塞启动，Console 仍可选择继续。
- [ ] 两侧都要求 `enabled=true`：提交配置为禁用时跳过检查（沿用现有行为），比较时只看
  其他 Agent 中已启用的配置段，取代现有存活探测。禁用配置不参与，不产生无意义告警。
- [ ] 响应附带对方的 enabled/instance 状态，使 Console 能区分“已配置”与“正在运行”两种
  文案；状态取自 Core 侧 instance 注册信息，不反射其他 Agent 的对象。
- [ ] 保持 secret 不回显：响应只含 `agent_id` 和 Agent 名称，不含身份字段值；沿用现有
  回归测试。secret effective value 只在 Core 内存中比较，不进入 digest、ID、日志、RPC、
  持久化诊断或 API 响应。

验收：查重在隔离后仍生效且不依赖存活探测；未启动 Agent 的配置冲突可被发现；命中范围
扩大为“已配置”这一变化在 Console 文案中有明确表达。

### CH-6-008：扫码登录例外边界收敛

- [ ] 在 Catalog/文档中登记扫码登录为 Core 侧显式例外，覆盖 WeChat/iLink、WeCom、
  DingTalk、Feishu/Lark、QQ 五个平台（ADR-027）。
- [ ] 验证该模块不导入任何平台 SDK（当前仅用 `httpx`，必须保持）。
- [ ] 切断对 Channel 内部符号的引用：WeChat 分支当前 `from ..channels.wechat.client import
  ILinkClient, _DEFAULT_BASE_URL` 引用私有符号，需在 `CH-4-005` 迁移 WeChat 时一并解除，
  本任务确认结果。
- [ ] 明确 Feishu `domain` 在 Core 与 Runner 两侧的解释规则：当前扫码分支只识别
  `feishu`/`lark`，遇到自定义 URL 会回落到官方 accounts 域名，属现存缺陷，需给出结论。
- [ ] 记录 WeCom 页面正则抓取与 QQ 在 Core 内做 AES-256-GCM 解密为已知技术债；迁移期间
  行为不变，但不得新增同类逻辑。
- [ ] 确认新增 Channel 默认不进入该模块，如需扫码登录必须在 descriptor 声明并登记。

验收：扫码登录的例外范围有明确清单和边界约束；Core 不因该例外引入平台 SDK 或跨层
私有引用。

### G6 Gate

- [ ] 相关平台和安装形态通过发布验证。
- [ ] Design §11.1 的 per-channel 硬编码表已按归属收敛，descriptor 漏配可被校验发现，
  Core 通用编排层不保留按 `channel_key` 的行为分支。
- [ ] bot 身份查重不依赖存活探测且不回显 secret；`channels verify-env` 与 `qwenpaw doctor`
  语义分离。
- [ ] 所有 Gate 通过后一次性切换默认 Channel 执行路径；不存在按 Channel 分期开放的产品
  流程。
- [ ] 所有行为回归、单测、pre-commit 和故障注入测试通过。
- [ ] 无阻塞当前发布的 P0/P1。

## 13. 任务执行规范

每个任务按以下顺序执行：

1. 实施 Chat 先只读检查，输出假设、文件范围、步骤和 checklist。
2. 用户确认方案后再修改。
3. 运行针对性测试和 pre-commit。
4. 按实施 Chat 中的明确授权创建只包含该任务的初始提交，但任务保持 `[-]`；没有提交
   授权时停在验证完成状态并请求用户决定。
5. 独立 Review Chat 只检查该任务范围。
6. 修复已确认问题并再次验证。
7. 通过 Review 后更新任务唯一进度记录和 checkbox。
8. 所属 Gate 的全部任务完成后，再运行独立 Gate 验收 Chat。

## 14. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| SDK 输出污染 stdout | bootstrap FD 分流；协议 stdout 单写入器；SDK 日志持续进 stderr |
| stderr 管道堵塞 | Core 持续读取、日志上限和诊断降级 |
| 子进程句柄泄漏 | Windows Job Object；POSIX close-on-exec 和显式继承白名单 |
| 依赖解析不稳定 | 发布 lock、wheel hash、ABI/platform 矩阵和离线缓存 |
| 新环境验证失败 | 未 commit 的候选失败保留仍满足当前已提交声明的 active generation；当前声明不匹配时停止 |
| ACK 丢失 | durable inbox、batch/event dedup、同 ID 重试 |
| 发送结果未知 | delivery ledger 和 `unknown`，不盲目重复发送 |
| Runner-owned Voice ingress 切换或乱序 | endpoint readiness、generation fencing、有界队列、sequence、deadline、过载关闭和排空 |
| 未来需要原始媒体跨进程 | 单独设计版本化二进制数据面；不得把字节塞入 JSON 或提前绑定当前 Voice |
| legacy 插件回归 | legacy 保持原注册和 BaseChannel，隔离迁移选择性执行 |
| Core/Runner 协议升级 | protocol version、capability negotiation 和兼容矩阵 |
| 未声明的传递依赖在隔离环境缺失 | 逐 Channel 核对实际 import 与 pyproject 声明的差集；`aiohttp`、`fastapi` 必须显式进 lock |
| 仅靠环境变量的平台端点覆盖静默失效 | descriptor 声明透传白名单，`minimal_env` 按白名单构造并回归验证 |
| Core 侧遗留平台入口被误认为已隔离 | Voice 完成定义包含删除 Core 路由与 `twilio` 依赖；扫码登录作为登记例外单独约束 |
| 硬编码表与 descriptor 并行生效导致漂移 | 逐表收敛归属；漏配即校验失败，不静默跳过 |
| 入站落盘目录解析不一致 | 统一 `config.media_dir` → `workspace_dir/media` → `WORKING_DIR/media`；`from_env` 使用 `<CHANNEL>_MEDIA_DIR` → `WORKING_DIR/media`，逐 Channel 回归配置和环境变量入口 |

## 15. 任务索引

| 任务 | 建议 Chat 标题 |
| --- | --- |
| CH-0-001 | Channel 隔离 - CH-0-001 Core/Runner 边界 |
| CH-0-002 | Channel 隔离 - CH-0-002 Descriptor 与标识 |
| CH-0-003 | Channel 隔离 - CH-0-003 stdio Framing |
| CH-0-004 | Channel 隔离 - CH-0-004 JSON-RPC 与生命周期 |
| CH-0-005 | Channel 隔离 - CH-0-005 可靠事件与 ACK |
| CH-0-006 | Channel 隔离 - CH-0-006 Runner Bootstrap |
| CH-0-007 | Channel 隔离 - CH-0-007 飞书主动连接原型 |
| CH-0-008 | Channel 隔离 - CH-0-008 OneBot ingress 原型 |
| CH-0-009 | Channel 隔离 - CH-0-009 Voice/Twilio ingress 原型 |
| CH-1-001 | Channel 隔离 - CH-1-001 Lock 制品 |
| CH-1-002 | Channel 隔离 - CH-1-002 Environment Spec 与校验 |
| CH-1-003 | Channel 隔离 - CH-1-003 Environment Installer |
| CH-1-004 | Channel 隔离 - CH-1-004 Environment Repair 与 Doctor |
| CH-1-005 | Channel 隔离 - CH-1-005 跨平台解释器策略 |
| CH-1-006 | Channel 隔离 - CH-1-006 RunnerSpec 与 Bootstrap |
| CH-2-001 | Channel 隔离 - CH-2-001 Runner Spawn 与 IPC I/O |
| CH-2-002 | Channel 隔离 - CH-2-002 进程监督与故障恢复 |
| CH-2-003 | Channel 隔离 - CH-2-003 Generation 与 Lease |
| CH-2-004 | Channel 隔离 - CH-2-004 ChannelHostAdapter |
| CH-2-005 | Channel 隔离 - CH-2-005 IsolatedChannelProxy |
| CH-2-006 | Channel 隔离 - CH-2-006 Descriptor Registry |
| CH-2-007 | Channel 隔离 - CH-2-007 Catalog 与状态聚合 |
| CH-3-001 | Channel 隔离 - CH-3-001 Inbox、ACK 与去重 |
| CH-3-002 | Channel 隔离 - CH-3-002 Delivery Ledger |
| CH-3-003 | Channel 隔离 - CH-3-003 媒体定位与兼容回归 |
| CH-3-004 | Channel 隔离 - CH-3-004 Staging、Activate 与 Commit |
| CH-3-005 | Channel 隔离 - CH-3-005 Journal 与崩溃恢复 |
| CH-4-001 | Channel 隔离 - CH-4-001 飞书迁移 |
| CH-4-002 | Channel 隔离 - CH-4-002 OneBot 迁移 |
| CH-4-003 | Channel 隔离 - CH-4-003 Voice/Twilio 迁移 |
| CH-4-004 | Channel 隔离 - CH-4-004 标准主动连接 Channel 批次 |
| CH-4-005 | Channel 隔离 - CH-4-005 企业 SDK 与复杂行为批次 |
| CH-4-006 | Channel 隔离 - CH-4-006 平台特定与系统耦合批次 |
| CH-5-001 | Channel 隔离 - CH-5-001 Plugin 描述与安装模型 |
| CH-5-002 | Channel 隔离 - CH-5-002 Plugin Runner Contract |
| CH-5-003 | Channel 隔离 - CH-5-003 Legacy 兼容 |
| CH-5-004 | Channel 隔离 - CH-5-004 Plugin 选择性迁移 |
| CH-6-001 | Channel 隔离 - CH-6-001 CLI 与运维操作 |
| CH-6-002 | Channel 隔离 - CH-6-002 API、Catalog 与状态模型 |
| CH-6-003 | Channel 隔离 - CH-6-003 Console 与文档 |
| CH-6-004 | Channel 隔离 - CH-6-004 安装形态发布验证 |
| CH-6-005 | Channel 隔离 - CH-6-005 OS 与 Python ABI 矩阵 |
| CH-6-006 | Channel 隔离 - CH-6-006 故障恢复和质量门禁 |
| CH-6-007 | Channel 隔离 - CH-6-007 Bot 身份查重收敛 |
| CH-6-008 | Channel 隔离 - CH-6-008 扫码登录例外边界 |
