# QwenPaw Channel 隔离架构设计

## 1. 文档信息

- 状态：重新设计，已按 main 最新代码复核，待实施
- 基线代码：`main`（2026-08-11），复核覆盖 bot 身份查重、OneBot 重构与 Voice 入口现状
- 范围：QwenPaw Channel，包括官方内置 Channel 和第三方 Channel 插件兼容路径
- 目标：在不破坏现有 Core Channel 与 legacy Plugin Channel 行为的前提下，隔离
  第三方 SDK、连接、状态和故障域
- 产品约束：本期每个 Agent 每种 `channel_key` 只有一个用户可见实例
- 发布策略：Phase 只是内部施工依赖和验收顺序，不是用户可见的分期发布；全部 Gate
  通过后再一次性切换默认 Channel 执行路径
- 设计原则：一次性冻结清晰的边界，避免长期维护两套用户可见实现
- 术语约束：`Runtime` 仅指 `src/qwenpaw/runtime/` 中的 Agent 请求编排层。Channel
  隔离实现不得在类名、模块或目录名、descriptor/API 字段、状态字段和用户文案中使用
  `runtime`；本文除说明这条保留规则外不使用该术语

本设计是实施的唯一架构依据。工作计划只拆分实施顺序、任务和验收，不重新定义
协议或职责。

### 1.1 核心概念

本文反复使用以下概念，含义固定：

| 概念 | 含义 |
| --- | --- |
| Channel descriptor | 一个 Channel 的**静态元数据记录**：它是什么、怎么启动、需要哪些依赖、支持哪些能力。读取 descriptor 不导入平台 SDK、不安装依赖、不启动进程。它是 Catalog、lock 生成、installer、Registry、CLI/API/Console 的唯一事实来源 |
| Runner | 运行被隔离 Channel 的独立子进程。第三方平台 SDK 只在 Runner 中导入 |
| `ChannelDriver` | Runner 内的平台接入接口。内置 Channel 和 Plugin 提供的 Channel 都可以实现它 |
| `IsolatedChannelProxy` | Core 内代表一个 `runner_process` Channel 的对象，向 `ChannelManager` 提供与 `BaseChannel` 兼容的调用面 |
| `instance_id` | 一个 Agent 的一个 Channel 的运行实例键。进程、环境 lease、checkpoint、日志和状态都按它隔离 |
| generation | 一个 instance 的单调递增世代号。切换时用于 fencing 旧进程 |
| lease | Core 授予 Runner 的、带 TTL 的消费许可。只有 committed active lease 可以消费正式平台事件 |

descriptor 之所以是本设计的中心，是因为当前 Channel 的“存在性”和“属性”分散在多张
per-channel 硬编码表中（见 §11.1），且部分信息只能通过 import 平台模块才能得到。
隔离要求在不 import 平台 SDK、不启动进程的前提下就能枚举和校验 Channel，因此必须
先把这些事实收敛到静态 descriptor。

### 1.2 全局交付检查清单

- [ ] 完成 Core/Runner 职责矩阵
- [ ] 完成 Channel descriptor、标识和目录规则
- [ ] 冻结 stdio、LSP framing 和 JSON-RPC 2.0 协议边界
- [ ] 冻结 event batch、ACK、retry 和 dedup 语义
- [ ] 冻结普通媒体定位符、入站落盘目录和可选原始实时媒体流的边界
- [ ] 冻结 Runner-owned Voice ingress 目标、endpoint 注册、顺序、背压和
  generation fencing，并记录 Core-owned ingress 的受控备选边界
- [ ] 冻结环境 lock、ABI、平台和 manifest 校验规则
- [ ] 冻结源码与 dependency environment 的分离规则
- [ ] 冻结 Channel 来源、进程位置及其对应的驱动接口规则
- [ ] 冻结扫码/设备码登录保留在 Core 的例外边界及其依赖约束
- [ ] 冻结 bot 身份查重的 descriptor 字段和 config 级比较规则
- [ ] 冻结环境变量透传白名单的范围（代理、TLS）和 mock 注入点的排除边界
- [ ] 完成 per-channel 硬编码表到 descriptor 的收敛清单
- [ ] 完成飞书、OneBot、Voice/Twilio 三条纵向原型
- [ ] 完成 Windows、Linux、macOS 和 frozen desktop 验证
- [ ] 完成现有单实例 API、CLI、配置和前端兼容验证

## 2. 问题和目标

当前所有 Channel 与 Core 共用 Python 进程和依赖环境。一个 Channel 的第三方 SDK
版本、导入失败、全局状态、原生扩展或后台任务可能影响其他 Channel 以及 Core。
插件动态安装依赖还会修改共享环境，导致版本覆盖、运行中升级不可预测和失败回滚
困难。

重构目标：

1. Core 保留消息编排和产品行为，继续使用 `ChannelManager`、`BaseChannel`、ACL、
   队列、AgentRequest/Event、平台无关渲染语义和 approval。
2. 需要隔离的 Channel 在独立 Runner 进程中运行，第三方 SDK 只在 Runner 环境中
   导入。
3. Runner 的 dependency environment 只包含 Python 和第三方依赖，不安装 Channel
   源码、Core 源码或 Protocol SDK 的副本。
4. Channel 源码随当前 QwenPaw 发布。源码改变但依赖 lock、Python ABI、平台和
   condition set 不变时复用同一个 environment。
5. Core 与 Runner 使用进程绑定的 stdio IPC，不依赖本地端口。
6. 入站事件具备至少一次投递、持久化 ACK 和幂等去重语义。
7. 依赖不满足时禁止启动对应 Channel；安装、repair 和 doctor 严格验证，启动只做
   轻量校验和必要的 Runner probe。检测到不匹配后可以由用户显式 repair，也可以由
   产品策略触发自动 repair。v1 默认只在用户启用/启动 Channel 且环境缺失或声明变化时
   自动准备；普通 health、list 和 Core 启动不隐式联网安装。两者失败都保持停止状态，
   不得使用不兼容环境。
8. 保留现有 core Channel 和 legacy Plugin Channel 的外部行为。
9. 为未来 isolated Channel Plugin SDK 预留稳定边界，但不强制旧插件立即迁移。

### 2.1 迁移兼容原则

本次 Channel 隔离重构以迁移进程、依赖和协议边界为主。除本设计、已确认 ADR 或当前
任务验收条件明确要求外，应保持各 Channel 迁移前的用户可观察行为和平台交互语义。

实施和 Review 应区分：

- 迁移引入或放大的行为回归、违反冻结边界的问题，由当前迁移任务处理；
- 迁移前已存在且未因隔离而恶化的问题，记录为残余风险或归入后续任务，不阻塞当前
  迁移；
- 修复既有缺陷如果会改变兼容行为，应单独设计和验收，不在迁移任务中顺带实施。

Design 中的全局目标和 ADR 应在其对应实施任务及 Gate 验证；除非当前任务明确将其列为
验收条件，不要求早期纵向原型提前完成后续阶段的基础设施。

Channel 迁移应优先从现有实现抽取 Runner-safe 的平台逻辑并复用；没有必要性说明和行为
等价证据时，不得重新实现第二套平台适配逻辑。无法复用时，必须提供迁移前后行为矩阵，
并由直接覆盖生产 adapter 的契约测试证明等价。

迁移实现应以最终目标架构及其生产执行路径为设计中心。复用的对象是迁移前已验证的
平台行为、平台交互语义，以及适合目标边界的实现逻辑，而不是 legacy Channel 的内部
类结构、调用接口或抽象形态。除非迁移期间维持旧执行路径是任务的明确验收条件，不要求
legacy Channel 改为调用新的 Runner 组件；不得仅为兼容即将删除的旧入口，在目标
`ChannelDriver`、平台组件或协议边界中引入额外接口、分支、适配层或双消费者抽象。
确需过渡兼容时，兼容代码应位于 legacy 一侧，保持薄且可删除，并且不得改变或扭曲
最终生产路径。

测试用于验证目标生产路径，不得反向定义或扭曲生产实现。历史测试基础设施、全局模块
替身、mock/fixture 约定及 legacy 测试的内部 patch 点，不构成生产兼容要求。迁移任务应
通过任务局部的测试进程、fixture 或导入隔离建立自包含的验证环境，使新测试不受历史
测试约定影响；除非任务范围明确要求，不得修改 legacy 测试或共享测试基础设施，也不得
为适配这些测试约定而在 `ChannelDriver`、平台组件或 SDK 导入边界中增加生产复杂度。若
无法在任务局部完成隔离，应停止并单独处理测试基础设施问题，不得将其转化为生产代码
约束。

## 3. 非目标

- 本期不把 tool、provider、hook、frontend、app 等其他 Plugin 类型迁入 Runner。
- 本期不提供不可信插件的完整安全沙箱；隔离解决依赖和故障域，不等同于权限沙箱。
- 本期不开放同一 Agent 的多个同类型 Channel 实例，不增加实例 CRUD、`display_name`
  或“加号创建”UI。
- 本期不把 Matrix Application Service API、MCP Protocol 或 LSP 业务协议作为
  QwenPaw 业务协议。
- 本期不依赖 `uvx`、动态网络解析或 Channel 启动时安装未锁定依赖。
- 本期不把整个 QwenPaw Core 拆成微服务。

## 4. 总体架构

```text
QwenPaw Core
  ├── Channel Catalog / Descriptor
  ├── ChannelManager
  ├── ChannelHostAdapter
  ├── ChannelProcessManager
  ├── ChannelEnvManager
  ├── InboundEventStore / OutboundDeliveryLedger
  ├── IsolatedChannelProxy
  └── Agent / ACL / Queue / Renderer
          │
          │ child-process stdio
          │ Content-Length framing
          │ JSON-RPC 2.0
          │
  ┌───────┴─────────────────────────────────────────────┐
  │ Runner bootstrap                                    │
  │   ├── protocol stdout                              │
  │   ├── protocol stdin                               │
  │   ├── stderr log pipe                              │
  │   └── ChannelDriver + third-party SDK              │
  └─────────────────────────────────────────────────────┘
```

### 4.1 Descriptor 分类维度

Channel 来源和进程位置是两个正交分类维度，descriptor 必须分别声明，不能用一个
混合字段同时表达：

| 分类 | 字段 | 允许值 | 含义 |
| --- | --- | --- | --- |
| 来源 | `source_kind` | `builtin`、`plugin` | Channel 由 QwenPaw 内置还是由 Plugin 提供 |
| 进程位置 | `process_mode` | `in_process`、`runner_process` | Channel 在 Core 进程还是独立 Runner 进程执行 |

当前组合固定为：

| Channel 类型 | `source_kind` | `process_mode` | 驱动接口 | 依赖环境 |
| --- | --- | --- | --- | --- |
| Console 等 Core Channel | `builtin` | `in_process` | `BaseChannel` | Core 主环境 |
| 隔离后的官方 Channel | `builtin` | `runner_process` | `ChannelDriver` | Channel 专属 environment |
| legacy Plugin Channel | `plugin` | `in_process` | `BaseChannel` | 当前插件共享环境 |
| isolated Plugin Channel | `plugin` | `runner_process` | `ChannelDriver` | Channel 专属 environment |

`runner_process` Channel 在 Core 中使用 `IsolatedChannelProxy`，平台代码在 Runner 中
实现 `ChannelDriver`。`ChannelDriver` 是 Channel 平台接入接口，不代表 Plugin；内置
Channel 和 Plugin 提供的 Channel 都可以实现它。驱动接口不作为第三个 descriptor
字段：`in_process` 固定解析为 `BaseChannel`，`runner_process` 固定解析为
`IsolatedChannelProxy` + `ChannelDriver`，避免重复事实来源。

两个字段都必须明确声明或由 legacy 注册信息确定性合成，不得根据 import 是否成功、
安装形态或启动探测动态推断。

### 4.2 单实例和未来多实例

本期外部行为保持：

```text
channels.<channel_key>
ChannelManager.get_channel(channel_key)
默认 instance_id = deterministic(agent_id, channel_key)
```

Channel 隔离执行层的所有进程、环境 lease、checkpoint、日志、generation 和状态都按
`instance_id` 管理。不得把 `channel_key` 作为全局进程或全局状态唯一键。

未来若开放多实例，可以在不改写底层进程与环境管理机制的前提下增加：

- 用户定义的 instance key 或不可变 ID；
- `display_name`；
- 实例列表、创建、删除和路由 API；
- 每个实例独立配置和 secret。

本期不得实现这些产品入口，也不得为未来功能维护第二套配置结构。

### 4.3 公共隔离基础设施与 Channel 专属能力

本期只重构 Channel，不为尚未决定迁移的 tool、provider、hook、frontend 或 app
设计通用业务协议。可以提取、但必须保持业务无关的公共能力包括：

- 不可变依赖环境的选择、校验、repair、引用切换和清理保护；
- 子进程启动、监督、日志排空、健康检查、停止和崩溃恢复；
- 通用 framing、JSON-RPC request/response、timeout、cancel 和错误 envelope；
- generation、lease、operation journal 和原子指针切换；
- 操作进度、诊断、健康状态和跨平台进程树清理。

request-scoped response 的协议语义（`response_handle`、显式
`channel.response.finish`、outcome、幂等和关闭 fencing）由 Channel 协议统一定义；其
Runner 实现不应由每个 Channel 各自复制。active route、closed tombstone、重启恢复、TTL、
容量分片、Host State 布局、并发读改写和 publication fencing 属于可复用的 Runner 基础
设施，应在 Phase 3 统一实现和验收。Channel 只负责将不透明 handle 映射到平台目标，以及
清理该平台自有的 delivery、card、typing 或 stream 资源。

Phase 0 的纵向原型可以在任务局部暂存 route store，以验证协议闭环和平台目标映射，但这
种实现是过渡性代码：其分片数量、容量、TTL、checkpoint key 和本地索引不是跨 Channel
规范。后续迁移不得复制该 Channel 局部实现；应先复用或抽取共享 Runner route store。

以下能力保持 Channel 专属，不提前抽象成未知 Plugin 的通用语义：

- `ChannelHostAdapter`、`ChannelDriver` 和 Channel descriptor；
- 入站消息 DTO、session、ACL、inbox、ACK、delivery ledger；
- Channel 媒体、Webhook/WebSocket ingress 和平台鉴权；
- Channel Catalog、Channel CLI/API 和 Console 表单。

未来如果迁移其他 Plugin 类型，应复用已稳定的公共进程与环境原语，并为该 Plugin 类型
单独定义 Host Adapter、协议和生命周期；不得让当前 Channel 协议承载未知语义。

## 5. Core 与 Runner 职责

### 5.1 Core 保留的职责

Core 和 `ChannelHostAdapter` 负责：

- Channel 配置、secret 引用和权限策略；
- 入站事件持久化、去重、ACK 和队列；
- `acl_sender_id`、ACL gate 和 pending approval；
- session、debounce、`AgentRequest` 构造和 Agent 调度；
- Agent Event 消费、streaming 聚合、平台无关 ContentParts/approval 语义、fallback
  文本渲染和发送目标解析；
- approval、usage、delivery ledger 和发送结果；
- effective media 目录解析、入口授权和 endpoint/generation 路由；
- Runner 生命周期、健康、lease、generation、重启和故障状态；
- Catalog、install status、instance status 和 platform status。

Core 不导入 isolated Channel 的第三方平台 SDK，不把 Core Python 对象传给 Runner。
`ChannelHostAdapter` 必须把 Runner DTO 映射回现有 Core contract：Core 根据
`conversation` 生成 `session_id`，保留每条事件的 `acl_sender_id`，并把协议 DTO 的
`sender_name` 写入现有兼容契约字段 `meta["user_name"]`。这里 `sender_name` 是本设计
新增的协议 DTO 字段，`meta["user_name"]` 是当前代码已在使用的契约键（`base.py` 的 ACL
pending 用户名即读取它）；当前 payload 中并不存在名为 `sender_name` 的字段，实施时不要
把它当作既有字段去“保持兼容”。

`acl_sender_id` 的语义是“真实发送者，不受共享 session 影响”。当前 Core 在合并前先按
ACL 身份切分批次，因此合并后的 payload 永不跨发送者，ACL gate 判定的身份与实际内容
一致。隔离后必须保持这条不变量：Runner 提交的每条事件各自携带 `acl_sender_id`，Core
侧的 debounce/merge 只允许在同一 ACL 身份内进行。共享 session 的群聊
（`share_session_in_group=true`，WeCom 默认开启）是这条不变量的主要压力场景，必须有
针对性回归。

descriptor 的 `dispatch_mode` 明确选择默认的 `manager_queue` 或现有 Voice/SIP 所需的
`direct_session`；Core 同时拥有这两条调度路径，不能按 `channel_key` 硬编码。

`direct_session` 的现状边界必须如实记录，不能只描述为“跳过 debounce/merge”。当前
`uses_manager_queue=False` 只有 Voice 和 SIP 两个 Channel，其效果是 Core 不为该 Channel
注入 enqueue 回调，因而整条 manager 管线都不生效：优先级分类、按 session 的队列路由、
批量 drain/merge、时间 debounce、**ACL gate** 和 **TaskTracker 路由**全部不执行，这两个
Channel 直接调用 `self._process(request)`。因此 `direct_session` 在本设计中意味着：仍先
完成 Inbox ACK，但保持迁移前的可见行为，即不经过 manager debounce/merge，且不启用
Core 的 ACL gate 与 TaskTracker。若希望隔离后为这两个 Channel 补上 ACL，属于行为变更，
必须单独提出并回归，不得在迁移任务中顺手引入。

### 5.2 Runner 负责的职责

Runner 内的 `ChannelDriver` 负责：

- 平台 SDK 初始化、鉴权和连接管理；
- 平台原生事件解析为稳定的 Channel DTO；
- 将 Core 的平台无关输出 DTO 编码为平台原生文本、卡片和媒体 payload，并调用平台
  API 执行发送、typing、reaction 和媒体操作；
- 平台游标、连接恢复和 instance-scoped checkpoint；
- 平台-owned ingress，例如 OneBot WebSocket server；
- Voice/Twilio 等需要公网回调的 Channel 的 HTTP/WebSocket ingress、平台签名校验、
  webhook/TwiML、token 和平台连接生命周期；
- 平台 SDK 的 import 和初始化探测。

Runner 不依赖 AgentScope、Core 的 Workspace、ChannelManager、数据库或内部配置对象。
Runner 可以在自己的 environment 中使用 HTTP/WebSocket server library；这些库只服务
Runner-owned ingress，不得把其对象传入 Core。Runner 只接收可序列化的配置快照、受控
secret handle 和协议 DTO。secret
value 默认通过一次性继承 pipe/handle 注入；只有第三方 SDK 在初始化时硬性读取环境
变量时，才使用进程私有的临时环境变量，并在 SDK 初始化完成后立即从 `os.environ`
删除。secret 不能通过 JSON-RPC、命令行或持久环境文件传递。

Runner 也不自行推断 Agent Workspace 或全局默认媒体目录。Core 必须把解析后的
`media_work_dir` 作为 `channel.prepare` 的 host context 传入；Runner 只把它当作平台入站
附件的工作目录路径，不依赖 Workspace 类型或 Core 路径工具。该解析器目前尚不存在，需要
按 §9.1 的现状说明新建。所有需要入站落盘的 Channel 使用解析后的最终目录本身，不再由
Runner 或 Channel 追加 `channel_key` 子目录。

Runner-owned ingress 的平台 HTTP/WebSocket 类型、签名校验和消息状态机必须只存在于
Runner environment。Runner 可以使用 aiohttp、FastAPI/Starlette 或平台 SDK 要求的其他
入口库，但不得导入 Core 的 `ChannelManager`、`ProcessHandler`、Workspace 或数据库对象。
Runner 通过 `event.batch` 向 Core 提交稳定事件，通过 `channel.send` 接收平台无关的
出站操作。Core 不转发原始 HTTP body、headers 或 WebSocket 帧。

如果部署边界确实要求 Core 持有统一公网入口，可以为特定 Channel 选择
`core_owned_ingress`。这只是兼容性备选，不是 Voice 的默认目标；必须先完成独立原型、
新增 capability/schema 和 ADR，再引入有界的 DTO 桥接。不得把该备选误写成所有 Channel
的通用入口，也不得把 Core socket 对象跨进程传递。

approval 的请求、状态和决策始终属于 Core；Runner 只负责原生卡片呈现、按钮回调解析
和平台 API。这样既保留 Core 的统一行为，也不会要求 Core import 平台卡片 SDK。

### 5.3 现有 BaseChannel 和 Plugin 兼容

- `ChannelManager` 保留；内部接受 `BaseChannel` 或 `IsolatedChannelProxy`。
- `BaseChannel` 保留为 Core Channel 和 legacy Plugin Channel 的基类。
- 当前 Voice Channel 仍继承 `BaseChannel`，迁移前后的用户可见消息、配置和生命周期
  行为必须保持兼容；隔离后的 Voice 由 Core proxy 对外提供同一 contract。
- 现有 `api.register_channel(channel_class=...)` 保留为 legacy 注册接口。
- legacy Plugin Channel 的配置、消息生命周期、API、CLI、前端和外部行为保持兼容。
- 新 isolated Plugin 使用独立的 descriptor 和 `ChannelDriver` 入口，不再把
  完整 `BaseChannel` 类直接放进 Runner。
- 旧插件迁移是选择性迁移，不自动把任意 legacy 类“搬进” Runner。

## 6. 进程启动和 stdio

### 6.1 启动模型

Core 通过受控的 `RunnerSpec` 启动子进程：

```text
RunnerSpec
  executable
  args
  cwd
  minimal_env
  code_root
  manifest_path
  instance_id
```

这借鉴 MCP/`uvx` 的 command、args、env、cwd 和按需拉起思想，但生产 launcher
由 QwenPaw 管理，不依赖 `uvx` 的动态解析和安装。

启动顺序：

1. Channel Catalog 解析 descriptor 和 effective config。
2. ChannelEnvManager 选择完全匹配的 lock 和 environment。
3. ChannelProcessManager 创建子进程并将 stdin、stdout、stderr 设为 pipe。
4. Runner bootstrap 在导入任何 Channel 或 SDK 前校验 `python -I`、代码根和 manifest。
5. bootstrap 分离协议 stdout 与日志输出。
6. Runner 发送 `runner.hello`，Core 校验身份和 environment；secret value 不出现在
   hello 或其他协议 payload 中。
7. Core 调用 `channel.prepare`，Runner 执行 import 和驱动初始化探测。
8. Core 通过 `channel.activate` 授予 candidate provisional lease；只有 current pointer
   CAS 成功且 `channel.commit` 确认后，Runner 才允许平台连接正式消费。

### 6.2 stdout、stderr 和 FD 约束

```text
Core writes ──> Runner stdin
Core reads  <── Runner protocol handle duplicated from initial stdout
Core reads  <── Runner stderr log pipe
```

约束：

- stdout 只能写协议帧；协议实现不得直接使用 `sys.stdout`；
- stderr 只能作为日志和诊断输出；Core 必须持续排空日志 pipe；
- bootstrap 必须先复制初始 stdout pipe 作为私有协议句柄，再在导入第三方 SDK 前把
  普通 `print()`、logging handler 和原生 FD 1 输出导向 stderr；
- Core 的自身 stdout 不会进入 Runner 协议 pipe；Core 只读写子进程对象的 stdin/stdout；
- Runner 不得把协议专用句柄传给不受控的后代进程；Windows 需显式控制 handle inheritance；
- EOF、broken pipe、进程退出和 Core 关闭均视为 IPC 断开，旧 generation 不能继续消费。

### 6.3 为什么不用 TCP、Matrix AS 或直接 uvx

- TCP 需要端口分配、冲突处理、listener 生命周期和本地连接鉴权；父子进程 stdio
  不需要服务发现。
- 这里不限制平台自身的网络入口：例如 OneBot 仍可由 Runner 监听平台要求的
  WebSocket 端口；该端口不用于 Core↔Runner 通讯，也不改变 stdio IPC 的选择。
- 平台 ingress 的端口由 descriptor/config 声明；若使用动态端口，绑定成功后的地址
  通过受控 RPC status 返回 Core。candidate Runner 在 commit 前不得绑定或消费正式
  入口。
- Matrix Application Service 是 Homeserver 与 Application Service 的 HTTP API，
  只借鉴其事务/ACK/重试/幂等，不采用其 Matrix 专属 HTTP 和事件协议。
- `uvx` 适合开发和临时运行；生产 Channel 必须使用发布 lock、ABI、平台和不可变
  environment，不能在启动时动态联网解析依赖。

### 6.4 源码组织与 Runner 加载

官方源码、Runner bootstrap 和 Protocol SDK 随当前 QwenPaw 发布。dependency
environment 只提供 Python 和第三方 distribution，不安装 Channel 源码、Core 源码
或完整 QwenPaw。推荐的代码边界如下：

```text
src/qwenpaw/
  channel_protocol/
    models.py
    errors.py
    framing.py
    rpc.py
    runner_bootstrap.py
  app/channels/
    base.py
    proxy.py
    catalog.py
    env_manager.py
    process_manager.py
    runners/
      <channel>_runner.py
    <channel>/
      channel.py
      handler.py
      sender.py
      media.py
```

isolated Runner 的业务源码从当前安装包或受信任的 plugin artifact 加载，放在显式
`code_root`；environment 内不复制源码。发布安装中的 `code_root` 应只读；source/
editable 开发安装允许指向显式仓库根，但不能从 ambient cwd 或 `PYTHONPATH` 推断。
`RunnerSpec` 显式传入经过校验的
`executable`、`args`、`cwd`、`code_root` 和 `manifest_path`。bootstrap 必须在导入
任何平台 SDK 前：

1. 使用受支持的 environment Python，以 isolated mode 启动；
2. 校验 `code_root`、descriptor、manifest 和 entrypoint；
3. 清除 `PYTHONPATH`、user site 和未声明的环境变量；
4. 保存协议 stdout 句柄，把普通 `print()`、logging handler 和原生 FD 1 重定向到
   stderr；
5. 仅加载 descriptor 声明的 `ChannelDriver` entrypoint。

environment Python 以 `-I` 执行绝对路径的受信任 bootstrap artifact（普通安装可为
脚本/zipapp，frozen desktop 可为应用内等价入口）。bootstrap 自身不依赖 QwenPaw 已
安装在 dependency environment 中；它校验 `code_root` 和 manifest 后，才把该根加入
进程内受控 import path。不得用 `PYTHONPATH`、当前目录或 user site 完成 bootstrap。

frozen desktop、pip/source/conda 和 container 只改变基础 Python 的来源，不改变
上述代码加载、environment 隔离和协议语义。

## 7. Framing 和 JSON-RPC 2.0

### 7.1 帧格式

控制通道使用 LSP 风格的 Content-Length framing：

```text
Content-Length: <UTF-8 byte length>\r\n
\r\n
<UTF-8 JSON-RPC 2.0 message>
```

v1 只允许一个 Header：`Content-Length`。Header 大小、帧大小、读取超时和待处理
请求数量都必须有上限。长度按 UTF-8 字节数计算，不按 Python 字符数计算。

选择 LSP framing 的原因：跨语言实现成熟、可读、易调试、第三方 Plugin SDK 容易
接入；它只借鉴 framing，不采用 LSP 的业务方法和语言服务器状态机。

### 7.2 JSON-RPC 约束

- 使用 JSON-RPC 2.0 request、response 和 notification；
- request 的 `id` 为字符串或整数，不使用 null；
- 与正常 request 关联的 response 继续使用同一个字符串或整数 `id`；只有 parse error
  无法恢复 request ID 时，error response 才按 JSON-RPC 2.0 使用 `id=null`。`id=null`
  的 error 不得匹配任何 pending request，success response 不得使用 `id=null`；
- 一个 request 只能有一个 response；
- notification 不需要 response，不用于要求可靠 ACK 的事件；
- 方法名使用 `runner.*`、`channel.*`、`event.*`、`delivery.*`、`ingress.*`、
  `host.*` 和 `request.*` 命名空间；
- 错误使用稳定的 `code`、`message` 和可选 `data`；
- 所有 payload 必须通过版本化 JSON Schema 校验；
- 单一写入器和写锁保证完整帧不交错；
- reader loop 只负责解帧、校验和分派，不能等待业务 handler 完成；双向 request handler
  可以发起反向 request，dispatcher 必须持续读取并匹配 response，避免嵌套调用死锁；
- 读写超时、最大帧、队列上限和 backpressure 必须可配置但有安全默认值。

### 7.3 最小方法集合

```text
runner.hello       Runner -> Core request
channel.prepare    Core -> Runner request
channel.activate   Core -> Runner request
channel.commit     Core -> Runner request
channel.lease_renew Core -> Runner request
channel.generation_status Either side request
channel.quiesce    Core -> Runner request
channel.health     Core -> Runner request
channel.stop       Core -> Runner request
channel.send       Core -> Runner request
channel.response.finish Core -> Runner request, optional
channel.typing     Core -> Runner request, optional
channel.reaction   Core -> Runner request, optional
ingress.endpoint.register   Runner -> Core request, optional
ingress.endpoint.update     Runner -> Core request, optional
ingress.endpoint.unregister Runner -> Core request, optional
event.batch        Runner -> Core request
delivery.update    Runner -> Core request
host.state.get     Runner -> Core request
host.state.put     Runner -> Core request
host.state.delete  Runner -> Core request
request.cancel     Either side notification
```

方法表中的 `optional` 只表示该方法不是所有 Channel 的全局必需方法。声明
`ingress_owner=runner_owned` 的 Channel 必须实现 `ingress.endpoint.register` 和
`ingress.endpoint.unregister`；只有 endpoint 地址、public URL 或 readiness 会变化时才
需要 `ingress.endpoint.update`。具体必需方法必须在 descriptor capability 和对应 Schema
中明确，不能仅依赖方法表中的 optional 标记。

`channel.send` 是平台无关出站操作的唯一消息方法，不为卡片、streaming 或消息更新新增
平台专属 RPC。v1 `SendParams` 是 closed object，包含共同的 identity、唯一
`delivery_id`、`to_handle` 和 `operation`；为兼容最初的 v1 实现，缺省 `operation` 等价于
`message.create`。`operation` 只允许 `message.create`、`message.update`、
`stream.start`、`stream.delta` 和 `stream.end`。

- `message.create` 携带非空 `content_parts`。可选 `approval` 是 closed object，只包含
  `request_id`、`tool_name` 和 `severity=low|medium|high|critical`；按钮语义固定为
  `approve`/`deny`，Runner 负责生成平台原生卡片和回调 payload。不支持卡片时使用同一
  `content_parts` 作为 Core 已生成的 fallback，不允许 Core 传入飞书、企微或其他平台
  原生卡片 JSON。
- `stream.start` 携带 `stream_type=reasoning|message`、`sequence=0` 和当前完整的
  `accumulated_text`，其 `delivery_id` 同时成为后续更新的稳定目标。
- `stream.delta` 和 `stream.end` 携带 `target_delivery_id`、相同的 `stream_type`、严格
  单调且连续的 `sequence` 及当前完整的 `accumulated_text`。`stream.end` 后不得继续更新
  同一目标。
- `message.update` 只服务 v1 streaming 产生的平台消息更新，携带
  `target_delivery_id`、严格单调且连续的 `sequence` 和非空 `content_parts`；目标必须是
  先前成功接收的 `stream.start`。任意非 streaming 消息编辑不属于 v1。
- 每个可能产生平台副作用的 request 使用新的 `delivery_id`；update/delta/end 通过
  `target_delivery_id` 引用先前的 `stream.start`。平台 message/card ID 只保存在 Runner，
  不进入 Core wire DTO。

`channel.send` 和 `channel.reaction` 使用相同的 closed result object，只包含
`delivery_id`、`state=acknowledged|failed|timeout|unknown`、可选稳定 `reason_code` 和
`retryable`。返回的 `delivery_id` 必须等于 request；未知字段、非终态或 ID 不一致属于
Schema mismatch。Runner 在交给可能产生平台副作用的 handler 前占用 `delivery_id`；调用
被取消、超时、异常终止、lease 在调用期间失效或平台结果不明确时，该 ID 仍保持已使用并
收敛为 `unknown`，不得以相同 ID 重入。只有 `acknowledged` 结果才能建立后续 target 或推进
stream sequence；`failed`、`timeout` 和 `unknown` 不得成为 update/reaction target。

出站 attempt 的准入和结果提交是生命周期短临界区；平台 handler 在临界区外执行，不得跨
handler await 阻塞 `channel.lease_renew` 或 Runner→Core 反向 RPC。attempt 开始时记录当前
generation 的 fencing epoch；正常续租只延长 expiry，不改变 epoch。lease 过期、quiesce、
stop 或 generation 撤销关闭新准入并推进 epoch；不属于 quiesce drain cohort 的旧 attempt
不得在撤销后提交 ACK，而应收敛为 `unknown`。request cancel 是独立于 handler 异常传播的
协议事实；即使 handler 捕获 `CancelledError` 并返回 ACK，或取消发生在 handler 返回后的
结果提交阶段，attempt 仍必须先完成不可中断的 `unknown` 清理，不能停留在 `sending`。
对于 `channel.send` 和 `channel.reaction`，handler 结果在 JSON-RPC response 成功写入前只
是 provisional：此时不得最终建立 target 或推进 stream sequence。response publication
期间收到 cancel，或 response 写入失败时，attempt 必须收敛为 `unknown`；只有 response
成功写入后才能提交 ACK 及 ordering 状态。唯一 publication 线性化点位于单一 writer 持有
write lock 时、transport adapter 确认底层输出已接受完整 response frame 的边界：普通
asyncio stream 可由同步 `writer.write()` 确认，Windows 线程托管 pipe 必须等待后台线程
完成全部 HANDLE 写入，不能以进入线程队列作为确认。在该边界内先取得生命周期锁，重新
检查 lease、generation、lifecycle 和绝对 drain deadline，选择最终 result，并将完全一致的
delivery/target/order 变更保存在 attempt 私有的 prepared 状态，再向底层输出提交同一
frame。prepared 状态不得写入正式 delivery ledger 或 target/order 表，也不得被并发的
`stream.delta`、`message.update` 或 reaction 用作已确认 target；对既有 target 的 prepared
更新在 settlement 前仍保持 busy。adapter 确认完整 frame 已被接受后，必须在不释放生命
周期锁且无新 `await` 的回调中将 prepared 状态原子发布到正式 ledger。若完整 frame 未被
接受，则在任何 transport cleanup await 前将 delivery 收敛为 `unknown`，且不得产生正式
target/order 变化；一旦完整 frame 被接受，后续 finally、cancel 或 transport 关闭不得回滚
该 result，也不得发送第二个 response。已越过该点的 attempt 不得再被并发 stop、lease
fencing 或 drain deadline 回滚为 `unknown`，尚未越过该点的 attempt 则继续受绝对 drain
deadline 和生命周期 fencing 约束。

底层 frame acceptance 与外层 write deadline/cancel 是两个独立事实。若 deadline 或 cancel
先发生，transport 必须立即关闭新写入并向原调用传播 `FrameTimeoutError` 或取消，不能因
HANDLE 随后成功而把原调用改为正常成功；底层 acceptance settlement 则继续独立收敛。若
完整 frame 迟到成功，仍执行唯一 `on_write_succeeded`，重新取得生命周期锁后才把私有
prepared ACK/target/order 发布为正式状态；在 settlement 完成前，这些状态对其他 request
不可见。若 frame 未被接受，则执行唯一 rollback 并收敛为 `unknown`。deferred settlement
不得阻塞 stop 或原调用传播 deadline/cancel，也不得令已关闭 writer 对外表现为可继续使用
的 transport。

`channel.reaction` 使用独立的 closed `ReactionParams`，包含 identity、唯一
`delivery_id`、`to_handle`、`target_delivery_id` 和 `reaction`。v1 只允许
`reaction=completed`，目标必须是先前成功接收的 `message.create` 或 `stream.start`；
平台 emoji/reaction key 由 Runner 映射，不进入协议。

request-scoped 回复目标使用独立的 response lifecycle，不能从单次 delivery 推断整轮
Agent 响应是否结束。声明 `response_lifecycle` capability 的 Runner 可以在
`InboundEvent` 中携带可选 `response_handle`；该字段是长度不超过 512、不得包含控制字符
的非空不透明字符串。Core 只原样保存并作为该轮 `channel.send.to_handle`、
`channel.reaction.to_handle` 和最终 `channel.response.finish.response_handle` 回传，不能
解析其中的平台 message、thread 或 topic 身份。未协商该 capability 时 Runner 不得发送
该字段，Core 也不得调用 `channel.response.finish`。

ChannelDriver 在提交带 handle 的 `event.batch` 前，通过 Runner 内部的
`open_response_scope()` 登记 active scope；该调用不是 RPC，也不产生平台副作用。事件获得
accepted/duplicate ACK 后保留 scope；永久 rejected 时通过 `discard_response_scope()` 撤销
尚无在途 delivery 的登记。恢复时 Driver 从自己的有界持久状态重新登记 active route，并
把尚在 TTL 内的 closed tombstone 注入 lifecycle controller。Core 收到未协商 capability
却携带 `response_handle` 的事件时，按事件返回不可重试的 `CAPABILITY_REQUIRED` rejection。

`channel.response.finish` 是可靠、幂等的 Core -> Runner request，使用 closed DTO，包含
共同 identity、`response_handle` 和 `outcome=completed|failed|cancelled`。它表示 Core
确认一轮 request-scoped response 已经终止，此后不会再使用该 handle 产生新的平台副作用；
它不依附于某个 `delivery_id`，也不使用 notification。`message.create`（包括 approval
card）、`stream.end`、completed reaction 和 delivery `acknowledged` 都只描述各自操作，
不得隐式关闭 response scope。成功但没有任何出站消息的 response 同样可以 finish。

Runner 为每个 active response scope 保存有界状态，并由 ChannelDriver 的可选 finish
handler 幂等释放平台 route、typing/card 等资源。关闭使用单调 tombstone，而不是无记录
删除：同一 handle、同一 outcome 重复 finish 返回成功；冲突 outcome 返回
`RESPONSE_OUTCOME_CONFLICT`；未知 handle 返回 `RESPONSE_HANDLE_UNKNOWN`；closed handle 上
的新 `channel.send` 或 `channel.reaction` 返回 `RESPONSE_CLOSED`，且不得调用平台 handler。
Driver 的持久化清理失败时 scope 仍保持逻辑关闭，返回可重试的
`RESPONSE_FINISH_FAILED`；相同 outcome 的重试再次执行幂等 handler。Runner 重启只恢复
active route 和尚在 TTL 内的 closed tombstone，closed tombstone 不得恢复为 active；有界
TTL GC 之后再次 finish 可以返回 unknown。

finish 与出站操作共享短生命周期临界区，但不得跨平台 handler await 持锁。若 send 或
reaction 先完成准入，finish 返回可重试的 `RESPONSE_BUSY`，由 Core 在该 attempt 的
publication settlement 后重试；若 finish 先原子关闭准入，后续操作返回
`RESPONSE_CLOSED`。in-flight 状态必须持续到 response frame publication 成功或 attempt
收敛为 failed/timeout/unknown，不能在平台 handler 返回时提前释放。finish handler 在锁外
执行；同 outcome 的并发 finish 共享同一个幂等清理 attempt，request cancel 或 response
丢失不会重新开放 scope。所有 open、finish、send 和 reaction 仍受 active state、lease、
generation 和 identity fencing。

出站 capability 绑定是协议事实来源：非文本 ContentPart 要求 `media`；所有 stream 操作
和 `message.update` 要求 `streaming`；带 `approval` 的 `message.create` 要求
`approval_card`；`channel.reaction` 要求 `reaction`。缺少 capability 返回
`CAPABILITY_REQUIRED`；未知 target 返回 `OUTBOUND_TARGET_UNKNOWN`；重复 delivery、非法
target、stream type 不一致、sequence 跳跃或结束后更新返回
`OUTBOUND_ORDER_VIOLATION`。所有出站操作还必须通过 active generation、有效 lease 和
identity fencing。

`runner.hello` 必须包含：`protocol_min/max`、`qwenpaw_version`、`channel_key`、
`instance_id`、`environment_spec_id`、`environment_id`、`lock_sha256`、Python ABI、
platform tag 和 capability 声明。Core 校验失败时拒绝激活。

### 7.4 Envelope、状态机和错误码

所有 request、response 和可靠 event 使用 JSON-RPC 2.0 envelope。除 `jsonrpc`、`id`、
`method`、`params` 外，业务参数必须包含版本化 DTO；控制调用至少校验
`channel_key`、`instance_id`、`generation` 和 capability。`id` 使用字符串或整数，
同一个 request 只能产生一个 response；可靠事件不能使用无 ACK notification。

Runner 生命周期固定为：

```text
created -> preparing -> standby -> active -> quiescing -> stopped
     \---------- failure from any state ----------> failed
```

`channel.prepare` 将 `created` 转为 `preparing`；成功响应时进入 `standby`，失败时进入
`failed`。`preparing` 和 `standby` 只能执行 import、配置检查、鉴权探测、checkpoint
导入和只读 health，不能消费正式平台事件。`activate` 只授予 provisional lease；
`channel.commit` 在 pointer CAS 成功后把它转成 active lease。两者必须携带单调递增的
generation、不可猜测 lease token 和 `lease_ttl_ms`；Core 使用
`channel.lease_renew` 定期续租。
IPC 断开、token 不匹配或 lease 过期后 Runner 必须停止消费，并拒绝旧 generation 的
state、event 和 delivery 写入。`channel.quiesce` 停止新入站和新发送、排空有界工作并
导出 checkpoint；`channel.stop` 可从任一非终态进入 `stopped`。

`channel.lease_renew` 到达 Runner 后不得排在慢平台 handler 之后；只要它在原 lease 过期
前取得生命周期临界区并通过 token/generation 校验，就更新 expiry，且既有 attempt 可在新
expiry 内正常提交。`channel.quiesce` 必须先原子进入 `quiescing`、撤销 endpoint/lease 并
关闭新准入，再最多等待 `drain_timeout_ms`；deadline 内完成的既有 attempt 可提交结果，超时
的 attempt 收敛为 `unknown`，方法不得无限等待。`channel.stop` 同样不得被永久阻塞的平台
handler 卡住；它先进入 `stopped` 并将未确定 attempt 收敛为 `unknown`，进程级 terminate/
kill 仍由 Core 按 §13.3 的关闭流程执行。
endpoint 的路由登记必须在 quiesce/stop/lease fencing 的生命周期临界区内同步摘除；可能
阻塞或发起反向 RPC 的平台 unregister hook 在锁外执行，并受同一 drain deadline 或
best-effort 语义约束，不得延迟状态转换。drain cohort 使用绝对 monotonic deadline；一旦
deadline 到达，即使 attempt 正在等待生命周期锁，也不得再提交 ACK、target 或 sequence。

Core 必须持有独立于 Runner 进程状态的 generation authority，不能通过共享
`LifecycleController` 或其他同进程对象推断 Host RPC 或正式路由。该 authority 的可变状态
有界为至多一个 authorized active generation、至多一个 candidate generation，以及单调
`highest_generation` watermark；旧 active 在新 candidate 的 prepare/activate 期间继续服务，
直到新 generation 的 commit 成功才被替换。Core 在调用 prepare 前建立 candidate admission，
使 Runner 可在 prepare 内登记本地 endpoint；prepare 失败或取消只删除 candidate，不影响旧
active。相同 generation 的 prepare 重试必须获得新的 candidate epoch，旧 endpoint 记录不能
被复用。endpoint registry 只保存 endpoint DTO 和对应 epoch，不重复保存 lifecycle 事实。

prepare、activate、commit 和 lease renewal 在释放 authority lock、等待 Runner RPC 前都必须
取得 operation token；RPC 返回后只有 generation、candidate epoch、operation sequence 和
phase 仍匹配时才能提交。quiesce、stop、lease expiry、新 candidate replacement 或新
generation commit 会使旧 token 失效，迟到的成功响应不得复活 generation。Core 发起
Runner control response 已成功返回后，Core authority settlement 必须抗调用方取消并先
收敛；取消仍向上层传播，但不能让 Runner 已进入新状态而 Core 留在旧 phase。
Core 发起
`channel.quiesce` 或 `channel.stop` 时，必须先校验完整 identity，再在调用 Runner RPC 前同步
撤销 authorization；RPC 超时、失败或 Runner 断联均不得恢复。合法的重复 stop/quiesce 即使
前一次 RPC 超时也可继续调用 Runner，但 authorization 始终保持撤销。

该 shutdown 重试资格属于具体 Core control client 与其绑定的 Runner peer，不属于 route
authorization。Core 在一个 client 首次成功完成本地 prepare admission 时签发不透明、仅进程内
使用的 control capability，并绑定 authority 私有 nonce、client 私有 nonce、generation 和
candidate epoch；该 capability 不进入 wire DTO、checkpoint 或持久状态。它从 candidate 贯穿
active、prepare abort、candidate replacement 和 retired，直到 client/peer 被释放；authority
不保存 retired generation 或 capability 历史集合。同一 client 不得再次 prepare 其他 generation
或 epoch，必须为新 Runner 创建新的 peer-bound client。
`CoreLifecycleClient` 的 Runner peer 在构造后不可替换；control capability 与该 client/peer
绑定共同存续。shutdown 的唯一正式入口是该 client 的 stop/quiesce 控制路径，authority
不得暴露无 capability 的 shutdown revoke 旁路。

stop/quiesce 必须依次校验 channel/instance identity、params generation、authority/client
binding，再检查当前 slot。slot 仍为 capability 对应的 generation 和 epoch 时先单调 revoke；
slot 已消失或 epoch 已变化时不得修改当前 authority，但仍向 capability 绑定的旧 Runner peer
发送控制请求。因此旧 Runner 的 stop/quiesce 重试不会被后续 candidate abort、replacement 或
新 generation commit 覆盖，也不能撤销同 generation 的新 epoch。该 capability 只能发送
stop/quiesce 以及约束同一 peer 的 activate/commit/renew 调用，不能恢复 route、允许 Host RPC、
注册 endpoint 或绕过 lease、phase、readiness 门禁。quiesce 的重复调用仍服从 Runner 自身的
稳定状态转换语义；quiesce 结果不确定后，同一旧 client 必须仍能继续发送 stop。

同步 endpoint resolve 只读取 authority 在锁内发布的 immutable snapshot，不读取异步锁保护
的可变 slot。正式 Host State、event、delivery 和 endpoint admission 使用同一 authority；
不得再次读取独立 Runner Controller。`host.state.get` 在 `preparing`、`standby` 和 `active`
允许，用于 checkpoint/import；`host.state.put/delete`、`event.batch` 和 `delivery.update` 只在
`active` 允许。endpoint register/update 在 candidate 和 active 可用，仍保留 capability、
外部暴露鉴权和 standby 外部绑定限制；revoked generation 的 register/update 稳定拒绝，
unregister 继续幂等成功但不得恢复 authorization。Core 时钟观察到 lease expiry 或新的
generation commit 时同步撤销旧 generation；只有新的有效 generation 完成唯一 commit 才能
建立新的 authorization。

Core authority 的错误分类冻结为：identity mismatch、generation unknown/stale/revoked 使用
`-32011`；phase/state violation 与首次观察到的 lease expiry 使用 `-32010`；lease token
mismatch 使用 `-32012`；capability 缺失使用 `-32013`。首次 expiry 将 slot 单调撤销，后续同
generation 请求返回 `GENERATION_REVOKED`。兼容保留的 `runner.hello` Controller 必须与
authority 的 `channel_key`、`instance_id` 一致，不能形成第二份身份事实来源。

provisional lease 不单列为生命周期状态；Runner 在收到 `channel.commit` 前仍对外表现为
`standby`。它可以完成连接预热和资源准备，但不能绑定正式 ingress、消费平台事件或
调用会产生外部副作用的平台 API。

稳定错误码至少包括：

```text
PROTOCOL_MISMATCH
AUTH_FAILED
CONFIG_INVALID
DEPENDENCY_MISSING
DEPENDENCY_INCOMPATIBLE
PLATFORM_AUTH_FAILED
PLATFORM_RATE_LIMITED
TEMPORARY_UNAVAILABLE
MESSAGE_DUPLICATE
CHECKPOINT_UNSUPPORTED
RUNNER_SHUTTING_DOWN
CAPABILITY_REQUIRED
OUTBOUND_TARGET_UNKNOWN
OUTBOUND_ORDER_VIOLATION
RESPONSE_CLOSED
RESPONSE_HANDLE_UNKNOWN
RESPONSE_OUTCOME_CONFLICT
RESPONSE_BUSY
RESPONSE_FINISH_FAILED
SECRET_HANDLE_INVALID
SECRET_HANDLE_CONSUMED
INGRESS_CONNECTION_UNKNOWN
INGRESS_ORDER_VIOLATION
INGRESS_BACKPRESSURE
```

协议 v1 必须定义 JSON Schema、未知方法处理、lease renewal、timeout、cancel、最大帧、
最大并发、backpressure 和重连规则。新增字段允许旧端忽略；改变已有字段语义必须
提升协议主版本。

### 7.5 Runner-owned ingress 与 Voice/Twilio 目标闭环

`runner_owned_ingress` 是 Voice/Twilio 的目标架构，也是 OneBot 已有模式的延伸。Runner
可以自行启动受控的本地 HTTP/WebSocket server，或把该 server 注册给受信任的反向代理；
Core 不接收和转发平台原始 HTTP body、headers 或 WebSocket 帧。

Runner 通过 `ingress.endpoint.register` 向 Core 报告监听协议、host、port/path、可选的
`public_base_url`、generation 和 readiness。动态端口必须在绑定成功后报告；Core 或受信任
代理只把正式流量路由到 committed active generation。`endpoint.update` 用于端口、public
URL 或健康状态变化，`endpoint.unregister` 用于 quiesce/stop。endpoint DTO 不包含 secret，
public URL 也不作为跨边界的凭证使用。

正式路由条件冻结为：generation 等于 Core 当前 authorized generation、authorization 为
active、Core 时钟尚未到 lease expiry、`readiness == "ready"` 且 `quiescing == false`。
`starting` endpoint 可以作为 candidate 提前登记但不接收正式流量；`degraded` 保留诊断信息
但不接收正式流量；`stopped` 和 `ready && quiescing` 同样不可路由。未发生 lifecycle revoke
时，endpoint update 回 `ready && !quiescing` 可以恢复路由；generation 已撤销后，同
generation 的健康更新不能恢复 authorization。

endpoint DTO 还必须携带**绑定暴露面和鉴权要求**，这来自 OneBot 已经落地的安全不变量：它
默认绑定 `127.0.0.1`，并把“是否要求鉴权”从绑定地址推导出来（非 loopback 绑定即强制要求
access token，未配置 token 时拒绝所有连接），token 只接受 `Authorization` 头且用
`hmac.compare_digest` 比较。隔离后这条不变量必须由 Runner 继续持有并在 endpoint DTO 中
如实上报，Core 据此校验“对外暴露的入口是否已鉴权”，不允许出现绑定到非 loopback 却无
token 的 committed endpoint。

OneBot 现有实现还包含一个端口冲突自愈 watchdog，会在绑定失败后重试并可能改变实际监听
端口。隔离后这类重新绑定必须触发 `ingress.endpoint.update`，使 Core 侧记录与实际监听状态
一致；不得出现 Runner 已换端口而 Core 仍按旧 endpoint 路由的情形。

Runner 内部的事件并发上限（OneBot 现有的事件任务硬上限即属此类）与 Core↔Runner 的批次
背压是两层不同机制，必须分别定义：前者保护 Runner 自身不被平台洪峰打爆，后者保护协议
通道和 Core 的 Inbox。`INGRESS_BACKPRESSURE` 错误码用于后者；Runner 内部丢弃或降级必须
有独立的诊断计数，不能静默吞掉平台事件。

另外，OneBot 的引用消息展开会在构造入站事件时**反向调用平台 API**（按 message id 拉取被
引用消息，必要时再取文件 URL），这些调用通过同一条 WebSocket 的 echo 机制等待响应。这
意味着 Runner 在提交 `event.batch` 之前存在平台往返延迟，且该往返依赖自身 ingress 连接
处于可用状态。协议层的 `event.batch` 超时与重试参数必须容纳这段延迟，Runner 也必须保证
这类反向调用不会阻塞 ingress 读循环。

入站平台消息统一通过 `event.batch` 提交，事件必须有稳定的 `event_id` 和明确的
`event_kind`。Voice 至少使用以下事件种类：

- `call.started`：包含平台会话标识（如 CallSid）、脱敏后的 from/to 和 Runner 生成的
  connection/session binding；
- `message.query`：包含 ConversationRelay 转录文本；
- `call.interrupted`：包含已说出的文本（如果平台提供）；
- `dtmf`：包含按键；
- `call.closed`：包含关闭原因和最后序号。

Core 收到 `event.batch` 后按现有 Inbox、ACL、session 和 Voice `direct_session` 规则处理，
但 ACK 只表示事件已持久化，不等待 Agent 回复。`call.started` 中的 binding 由 Core
按 instance/generation 保存，用于 session 和幂等收敛。Agent 输出通过 `channel.send` 返回
Runner，Runner 再编码为 ConversationRelay `text`/`end` JSON 写回平台。`channel.send`
必须携带 `delivery_id`；连接写入失败或结果不明确时由 Runner 返回可诊断状态，Core 不
盲目重复发送。

Runner 负责 Twilio 签名校验、TwiML、一次性 WebSocket token、ConversationRelay 状态机、
status callback 和 Cloudflare Tunnel/等价公网暴露方式。Twilio webhook 的配置只能由
committed active generation 执行；standby 可以绑定本地端口并做只读 probe，但不得修改
Twilio 外部配置或消费正式通话。切换时先 quiesce 旧 generation、停止新连接准入并在有界
时间内排空已有通话，再切换反向代理或 tunnel 路由；不能迁移已有 WebSocket 连接。

Core 只负责生成和校验 lease、generation、配置/secret 引用以及 endpoint 路由授权。若
桌面部署或统一公网入口确实要求 Core-owned ingress，可以在独立 ADR 中保留兼容实现：
Core 负责最小的 HTTP/WebSocket 接入，Runner 仍只处理版本化文本 DTO，并通过
`event.batch`/`channel.send` 完成业务闭环。该备选不属于 v1 Voice 默认实现，也不应在
没有新的 schema、顺序、背压、关闭和 fencing 验收前加入协议基线。

### 7.6 Voice 与 OneBot 的当前实现基线

`runner_owned_ingress` 对 OneBot 是既有模式的延伸，对 Voice 则是一次真正的重写。两者
现状不同，必须分别记录，避免把 Voice 的迁移当成“搬运”。

OneBot 已经是 Channel 自持入口：它在 Channel 模块内用 aiohttp 建立
`web.Application()` / `AppRunner` / `TCPSite`，自己监听 `/ws` 接受平台反向连接，不在 Core
的 FastAPI app 上注册任何路由。它是当前仓库中唯一持有独立监听 socket 的 Channel，因此
可以作为 Runner-owned ingress 的现实参照。

Voice 当前恰好相反，**入口完全由 Core 持有**：

- Core 的 `routers/voice.py` 注册 `POST /voice/incoming`、`WS /voice/ws` 和
  `POST /voice/status-callback` 三条路由，并挂载在 Core app 根路径上；
- Twilio 签名校验在 Core 完成，且 Core 直接 `import twilio.request_validator`；
- WebSocket 由 Core `accept()` 并校验一次性 token 后，把 FastAPI `WebSocket` 对象交给
  Channel 模块的 ConversationRelay 处理器；即原始 socket 对象目前跨模块传递；
- 一次性 WS token 在 Core 进程内存中 mint 和 validate，因此 mint 与 validate 必须一起
  迁移，不能只搬一半；
- Cloudflare Tunnel 由 Channel 启动，但指向的是 **Core 的端口**；public URL 靠抓取
  `cloudflared` 输出得到，没有配置字段。

因此 ADR-025 的表述必须理解为：Core-owned ingress 不是“将来可能退守的备选”，而是**当前
的既有实现**；Runner-owned 才是需要新建的目标。CH-4-003 的完成定义必须显式包含删除
`routers/voice.py` 的三条路由及其在 Core app 上的挂载，并从 Core 默认依赖中移除
`twilio`；只要这些还在 Core，就不能声称 Voice 已完成隔离。

重构后的 Voice 目标形态与 OneBot 一致：**Voice 自己维护自己的生命周期、监听和平台事件，
Core 只处理传过来的数据，和其他 Channel 一样。** Voice 不是需要特殊对待的 Channel，它
现在看起来特殊只是历史实现的结果。具体说，Runner 侧持有 Webhook 与 ConversationRelay
的监听、签名校验、TwiML、一次性 token 的 mint 与 validate、tunnel 生命周期和
ConversationRelay 状态机；Core 侧只通过 `event.batch` 收稳定事件、通过 `channel.send`
回出站操作，不接触原始 HTTP body、headers 或 WebSocket 帧，也不再持有 socket 对象。

当前的 Core-owned ingress 不作为 v1 迁移流程中的自动后备路径。若 Runner-owned 原型证明
确实无法成立，必须先由独立 ADR 决定是否保留当前实现，并单独冻结最小 DTO、顺序、背压、
关闭和 generation fencing；即便如此，平台 SDK 仍必须留在 Runner，Core socket 对象不得
跨进程传递。

这里还有一个依赖声明问题需要在 lock 阶段解决：`fastapi` 和 `aiohttp` 目前都**不是**
`pyproject.toml` 中声明的直接依赖，而是传递依赖。Voice Runner 若继续以 FastAPI 语义实现
ConversationRelay，`fastapi` 必须成为该 Channel lock 中的显式依赖；OneBot Runner 同理需要
显式声明 `aiohttp`。CH-1-001 生成 lock 时不能假设这些库“本来就在”。

### 8.1 Matrix AS 风格但非 Matrix 协议

入站使用 Matrix AS 值得借鉴的事务模型：

```text
Runner -> event.batch(batch_id, events)
Core -> 持久化 Inbox、完成幂等检查
Core -> response(accepted_event_ids, duplicate_event_ids, rejected_events)
Runner -> 未收到 response 时重发同一个 batch_id
```

示例：

```json
{
  "jsonrpc": "2.0",
  "id": "batch-request-1",
  "method": "event.batch",
  "params": {
    "batch_id": "generation-7-batch-42",
    "events": [
      {
        "event_id": "platform-event-001",
        "channel_key": "feishu",
        "instance_id": "instance-001",
        "generation": 7,
        "conversation": {
          "id": "chat-001",
          "type": "dm",
          "thread_id": null
        },
        "sender_id": "user-display-id",
        "acl_sender_id": "user-001",
        "sender_name": "Alice",
        "content_parts": [],
        "metadata": {}
      }
    ]
  }
}
```

Core 只有在 Inbox 持久化和去重结果提交后才 ACK。ACK 表示“Core 已接收”，不表示
Agent 已完成回复，也不承诺平台 exactly-once。`batch_id`、`event_id` 和平台原生
事件 ID 必须可用于幂等。`event_id` 必须在平台重投和 Runner 重启后保持稳定：优先由
平台原生事件 ID 派生；没有原生 ID 的 Channel 必须在 profile 中定义稳定派生字段和
碰撞处理，不得使用随机 UUID 作为可重放事件的唯一去重键。

逐事件校验失败必须进入 `rejected_events`，包含稳定的 reason code 和 `retryable`；同一
batch 中的合法事件仍可 accepted/duplicate。整个 request 只有在 envelope、身份、
generation 或 schema 无法解析时才返回 JSON-RPC error。Runner 只重试未获响应或明确
`retryable=true` 的原 event_id；永久拒绝必须记录诊断，不能形成 poison-event 忙循环。

### 8.2 出站 delivery

Core 将每次发送写入 OutboundDeliveryLedger，使用不可变 `delivery_id`。Runner
回复平台 ACK、失败、未知或超时状态。发送成功但 RPC response 丢失时，Core 不得
盲目重复发送；由 Channel profile 声明是否支持平台侧幂等键，并将不确定情况标为
`unknown`。

### 8.3 消息 DTO、状态和幂等

入站 DTO 使用稳定字段，不传 Core Python 对象，也不把关键身份只塞入无约束的
`meta`：

```json
{
  "event_id": "platform-event-001",
  "channel_key": "feishu",
  "instance_id": "instance-001",
  "generation": 7,
  "conversation": {"id": "chat-001", "type": "dm", "thread_id": null},
  "sender_id": "user-display-id",
  "acl_sender_id": "user-stable-id",
  "sender_name": "Alice",
  "content_parts": [],
  "metadata": {}
}
```

`session_id` 由 Core 根据 `conversation`、`channel_key`、`instance_id` 和会话策略
生成；Runner 不决定群聊共享还是按成员隔离。Core 只有完成 Inbox 持久化和幂等检查
事务后才返回 accepted/duplicate ACK。入站默认 at-least-once；出站默认 at-least-once
或 best-effort。只有平台提供服务端幂等键时，才声明 `server_side_idempotency` 或
`exactly-once-visible` capability。

OutboundDeliveryLedger 至少保存 `requested`、`sending`、`acknowledged`、`failed`、
`timeout` 和 `unknown`。RPC response 丢失而平台可能已发送成功时必须记录 unknown，
不能盲目自动重试。

### 8.4 meta 序列化边界的现状

“Core 不把 Python 对象传给 Runner”这条要求在 `meta` 上有具体落点。当前 `merge_native_items`
在合并时会保留一组固定 meta 键，其中三个看起来不可序列化，但实际情况需要核实：

- `reply_future`、`reply_loop`、`incoming_message` 只出现在合并键列表和注释中，**没有任何
  Channel 真正写入它们**，属于历史残留。它们不构成迁移障碍，实施 CH-0-001 时不必为其设计
  跨进程方案，可直接确认为死代码并在迁移相应 Channel 时清理。
- `session_webhook` 与 `session_webhook_expired_time` 是 DingTalk 真实使用的键，值分别是
  URL 字符串和过期时间戳，均可序列化，直接进入协议 DTO 即可。它在 Core 侧有专门的保底
  透传逻辑，迁移 DingTalk 时必须保持等价行为。

真正需要处理的跨边界对象集中在别处：`Path` 类型的 `workspace_dir` 与 `media_dir`（按
§9.1 收敛为 `media_work_dir` 字符串）、`Config` 的 pydantic 配置段（按 §14.1 改为 schema
校验后的配置快照）、`Workspace` 对象（Runner 不再持有，改由 host context 与
`host.state.*` 提供所需能力）以及 `Event` 流（保持在 Core 侧消费）。CH-0-001 的“证明 Core
不需要把 Python 对象传给 Runner”应以这份清单为范围，避免为不存在的用法设计协议。

## 9. 媒体和实时数据

媒体分为两种互不混用的场景：普通附件定位和原始连续媒体流。普通附件遵循现有
QwenPaw Content contract；原始连续媒体只有在确实需要跨 Core↔Runner 传输字节流时，
才启用可选的 pipe capability。

### 9.1 普通附件定位

图片、文件、视频和离散语音附件继续使用现有 Content 字段，字段值就是媒体定位符：

| Content 类型 | 现有字段 | 允许的定位值 |
| --- | --- | --- |
| image | `image_url` | 本地路径、`file://`、`http(s)://` |
| video | `video_url` | 本地路径、`file://`、`http(s)://` |
| file | `file_url` | 本地路径、`file://`、`http(s)://` |
| audio | `data` | 本地路径、`file://`、`http(s)://` 或平台音频定位值 |

Core↔Runner 的 JSON 直接携带这些字符串和已有的 `filename`、mime 等元数据，不把文件
内容 Base64 内联，也不要求先生成不透明引用或文件句柄。示例：

```json
{
  "type": "file",
  "file_url": "/Users/haino/Desktop/report.pdf",
  "filename": "report.pdf"
}
```

OneBot 等平台可能直接提供下载 URL：

```json
{
  "type": "file",
  "file_url": "https://platform.example/files/report.pdf",
  "filename": "report.pdf"
}
```

路径和 URL 在协议层都是定位符，不由 Core 统一下载、复制或转换。Runner 的 Channel
适配器继续按现有行为处理：本地路径交给平台 SDK 上传，URL 交给平台 SDK 或 Channel
既有下载逻辑。`file://` 继续作为兼容输入；OneBot 的 `base64://`、`data:` 等格式只
在 OneBot 或具体平台适配层处理，不升级为通用 Core↔Runner 二进制协议。

协议不把本地路径强制限制在某个 media 根目录。Agent 传入桌面文件或其他可访问文件时，
Runner 可以按当前进程权限读取。部署方如果需要更严格的访问控制，可以在进程沙箱、
允许目录策略或用户确认层增加限制，但这不是 v1 普通附件协议的前置条件。Runner 仍须
拒绝把定位符当作命令执行，并在读取前检查路径存在、普通文件类型和操作系统访问权限；
URL 的认证信息不得写入日志。

普通附件仍保持现有双向流程：入站由 Channel 按平台规则下载或解析定位值，写入
Content 后经 `event.batch` 传给 Core；出站由 Core 经 `channel.send` 把 Agent 产生的
Content 定位值传回 Runner，Channel 再按平台规则上传或发送。该协议不引入额外文件复制
和 lease 生命周期。

隔离后的 Runner 不能依赖 Workspace 对象来推断入站下载位置。Core 解析 effective
`media_dir`，将平台原生、绝对的 `media_work_dir` 放入 `channel.prepare.host_context`。
相对配置值由 Core 按当前 Agent 的工作目录规则绝对化，不能改为依赖 Runner cwd。

必须注意：**当前代码没有集中的媒体目录解析器**，`media_work_dir` 这个名字在现有代码中
也不存在。正常 Agent 的 `ChannelManager.from_config` 会注入 `workspace_dir`，多数 Channel
在自己的 `__init__` 中重复以下优先级：显式 `config.media_dir` → `workspace_dir / "media"`
→ Channel 自己的 fallback。后者目前有的使用 `WORKING_DIR/media`，有的使用
`WORKING_DIR/media/<channel_key>`；这不是目标架构要求，也不应继续作为兼容约束。QQ、
Telegram、Matrix 和 XiaoYi 当前还缺少完整的配置字段或透传链路，不能把它们描述为已经具备
统一的 `media_dir` 配置。

本设计的 `media_work_dir` 是**新增的 Core 侧解析能力**，不是把现有下载函数搬到 Core。
对于需要入站落盘的 Channel，目标解析规则统一为：

```text
from_config: config.media_dir → workspace_dir / "media" → WORKING_DIR / "media"
from_env:    <CHANNEL>_MEDIA_DIR → WORKING_DIR / "media"
```

`from_config` 是正常 Agent 路径；`from_env` 是没有 Agent workspace 上下文的独立兼容入口，
两者不叠加，也不互相覆盖。显式 `media_dir` 为空时，最终目录就是 `workspace_dir/media` 或
`WORKING_DIR/media`，不再追加 Channel 子目录。所有落盘型 Channel 共用这个最终目录；
Runner 继续使用各 Channel 现有的下载、文件名、覆盖和 URL/路径处理逻辑，本次不引入文件迁移、
跨 Channel 子目录或新的下载算法。`media_dir` 配置入口和对应 `*_MEDIA_DIR` 环境变量只对
实际需要入站落盘的 Channel 统一提供；OneBot 的定位符直传以及 MQTT、Voice、SIP 的非落盘
媒体行为不适用。Console 和 iMessage 的现有目录用途属于上传引用或出站暂存，不纳入该契约。

`config/utils.py` 中仍存在历史 `~/.copaw` 路径改写逻辑，迁移时一并盘点。`media_work_dir`
只决定 Runner 自己创建的入站文件放在哪里，不是出站路径白名单；Agent 传来的桌面文件等
其他路径仍按普通定位符规则处理。Core 负责目录选择、创建条件和实例配置生命周期，Runner
负责调用平台下载逻辑；v1 保持现有文件保留和清理行为。

### 9.2 可选原始实时媒体流

`media_pipe` 不是普通附件的默认方式，也不是当前 Voice/Twilio 的必需方式。v1 没有
任何已确认的 Channel 需要让原始连续媒体字节跨 Core↔Runner，因此不定义
`media.open`/`media.close`，不实现二进制数据面，也不为它增加当前 Gate。

当前 Voice 使用 Twilio ConversationRelay。Twilio WebSocket 传递的是 `setup`、
`prompt`、`interrupt`、`dtmf` 等 JSON 文本消息，QwenPaw 返回 `text` 和 `end` 消息；
语音识别和语音合成由 ConversationRelay 完成。因此当前 Voice/Twilio 的 Core↔Runner
协议只传会话、转录文本、文本回复、状态和错误，不实现或验收音频 pipe。

未来若采用 Twilio Media Streams、让 Core 负责 STT/TTS，或把 SIP 的原始 PCM 音频移到
Core 侧，才需要单独提出版本化协议扩展。届时控制信息走 JSON-RPC，原始二进制帧走独立
pipe，不进入 JSON Content-Length 帧，也不使用 Base64；同时必须定义 capability、
stream_id、方向、编码、采样率、有界帧、序号、时间戳、背压、断线、半关闭和
generation fencing。若音频处理始终留在 Runner 内部，音频不跨 Core↔Runner 边界，
也不需要 pipe。

未来跨主机场景还必须另行设计认证、加密和流控，不能假定本机匿名 pipe 可以直接复用。

## 10. 环境和源码模型

### 10.1 标识

标识只使用逻辑值，不拼接文件系统路径，也不读取当前工作目录。所有 hash 输入都使用
§10.1.1 的 canonical JSON；hash 为完整 SHA-256 小写十六进制，不截断。逻辑 ID 可以写入
协议和 manifest，但 secret、绝对路径和平台凭证不得进入任何标识输入。

#### 10.1.1 Canonical JSON

本设计使用一个受限、版本化的 JSON 子集作为标识和 descriptor digest 的唯一编码：

- 输入只允许 object、array、UTF-8 string、boolean、null、signed 64-bit integer 和有限
  base-10 decimal；禁止 binary float、NaN/Infinity、bytes、`Path` 和语言专属对象。decoder
  必须将 JSON number 解析为精确 decimal，而不是 host float；
- object key 和 string value 先执行 Unicode NFC；NFC 后出现重复 key 必须拒绝；
- object key 按 Unicode code point 升序排列；array 保持声明顺序；集合语义字段必须在进入
  encoder 前按各字段规则排序和去重；
- string 只允许 Unicode scalar value；NFC 后包含 U+D800--U+DFFF surrogate 的 string/key
  必须拒绝。string 使用 UTF-8 字面量，不转义 `/`、非 ASCII、U+2028 或 U+2029；只允许
  `"`、`\\`、`\b`、`\t`、`\n`、`\f`、`\r` 六种短转义，其他 U+0000--U+001F 控制字符
  必须写成唯一的 `\u00xx`（小写 hex）转义；
- integer 写为十进制且不带 `+`、前导零或 exponent；decimal 先去尾随零，负零写为 `0`，再
  写为无 exponent、无前导零的十进制 JSON number。canonical decimal 超过 128 个字符必须
  拒绝，避免等价指数形式产生无界编码；
- 使用 UTF-8、无 BOM、无缩进、分隔符 `,`/`:`，不写末尾换行。`ensure_ascii=false` 只描述
  输出中非 ASCII scalar 的字面量规则，不能替代本节的完整转义规则；
- hash 输入为 ASCII domain separator、单个 NUL byte 和 canonical JSON bytes。domain
  separator 中包含 schema version，避免未来算法升级与 v1 碰撞。

任何实现语言都必须用测试向量证明输出 bytes 完全一致，不能依赖默认 JSON encoder、dict
插入顺序、locale、系统路径分隔符或平台换行。

#### 10.1.2 逻辑 ID

`channel_key` 是用户可见配置、CLI 和 API 使用的稳定逻辑 key。新的 builtin descriptor 和
isolated Plugin descriptor 必须使用 1--64 字符的 canonical key，格式为
`^[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?$`；解析时不自动 trim、lower 或替换字符，非
canonical 输入直接拒绝。现有 legacy Plugin 继续使用既有注册流程生成的 key；若其历史 key
不满足新格式，只能合成为 `source_kind=plugin`、`process_mode=in_process` 的 legacy
descriptor，不能升级为 isolated descriptor，也不能直接用作磁盘目录名。builtin、legacy
Plugin 和 isolated Plugin 的最终 key 在 Catalog 中必须全局唯一。

`agent_id` 使用 Core 已提交配置中的精确值，区分大小写；生成标识时不得再次 sanitize 或
lower。系统默认 Agent 使用其已提交的稳定 ID。空 `agent_id` 或空 `channel_key` 必须拒绝。

```text
instance_payload = {
  "agent_id": agent_id,
  "channel_key": channel_key
}
instance_id = "chi1_" + sha256(
  "qwenpaw.channel.instance.v1" + NUL + canonical_json(instance_payload)
)

condition_set_sha256 = sha256(
  "qwenpaw.channel.conditions.v1" + NUL + canonical_json(condition_set)
)

environment_spec_payload = {
  "channel_key": channel_key,
  "condition_set_sha256": condition_set_sha256,
  "lock_sha256": lock_sha256,
  "platform_tag": platform_tag,
  "python_abi": python_abi
}
environment_spec_id = "ches1_" + sha256(
  "qwenpaw.channel.environment-spec.v1" + NUL
  + canonical_json(environment_spec_payload)
)

immutable_installation_id = "install1_" + 32 lowercase hex characters
environment_id = environment_spec_id + "." + immutable_installation_id
```

`lock_sha256` 和 `condition_set_sha256` 必须是完整的 64 个小写 hex 字符。`immutable_installation_id`
使用 128-bit CSPRNG，在一个 staging installation 创建时生成一次，并原样持久化到
`install.json`；同一个 installation 重读时不变，repair 或重新安装必须产生新值。因此同一
spec 可以对应多个不可变 installation，但一个 `environment_id` 只指向一个 installation。

同一 `environment_spec_id` 可以有多个不可变 `environment_id`，用于 repair 和候选
切换。活动 environment 不原地升级。磁盘目录键与上述逻辑 ID 分离：目录键使用
`dir1_` 加逻辑 ID UTF-8 bytes 的 SHA-256 前 32 个 hex；创建或读取目录时必须用 manifest
中的完整逻辑 ID 检查碰撞，不能因短 hash 相同而复用。任何位置都不得把 `channel_key`、
`agent_id` 或完整逻辑 ID 当作相对/绝对路径片段拼接。

#### 10.1.3 Python ABI、platform tag 和 condition set

canonical `python_abi` 是 PEP 425 interpreter tag 与 ABI tag 的组合
`<interpreter>-<abi>`，例如 CPython 3.11--3.13 分别为 `cp311-cp311`、
`cp312-cp312`、`cp313-cp313`；debug、free-threaded 或其他解释器必须保留其真实 ABI tag，
不得折叠为普通 CPython。实现从 `packaging.tags.sys_tags()` 中选择当前解释器最优先且
ABI 不为 `none` 的 tag，取其 `interpreter` 和 `abi`；结果必须等于 lower-case canonical
形式并匹配 `^[a-z0-9_]+-[a-z0-9_]+$`。

canonical `platform_tag` 是发布 lock/manifest 选择的 PEP 425 platform component，例如
`win_amd64`、`macosx_11_0_arm64`、`manylinux_2_28_x86_64`。它必须是 lower-case 且匹配
`^[a-z0-9]+(?:_[a-z0-9]+)*$`，但该 grammar 本身不证明 tag 合法；`packaging.tags.Tag` 能
构造也不得作为验证依据。一个 tag 只有同时满足下列条件才合法：

- 由版本化的 release target registry 枚举；该 registry 的 tag 必须来自受支持 target
  interpreter 上 `packaging.tags.sys_tags()` 产生的 `Tag.platform`，或来自该 target 已验证
  wheel 的 platform tag；
- 属于当前 descriptor 的 `supported_platform_tags` 与当前发布 manifest 的 target 集交集；
- 通过 `CH-0-002` validator 传入的 `allowed_platform_tags` 成员检查。

`windows`、`darwin`、`macos`、裸 `x86_64` 等产品别名从不在 registry 中，必须拒绝。
`CH-1-001` 负责生成和签入 release target registry/manifest；CH-0-002 只冻结其 value model
和成员校验接口，不从 `sys.platform`、路径、locale 或任意可构造的 `Tag` 猜测合法性。

`condition_set` 是 descriptor 声明的全部 condition fields 到 effective config value 的
object。Core 必须先用当前 config schema 展开默认值，再校验 allowed values，然后按字段名
排序进入 canonical JSON。condition value 只允许 string、boolean、null 或有符号 64-bit
integer；condition field 禁止引用 secret、任意路径、自由文本或 decimal。每个 condition
field 都必须声明非空、有限的 `allowed_values`：string/integer 必须显式列出所有允许值，
boolean 使用 `[false]`、`[true]` 或 `[false, true]`，null 只能作为显式 array 成员。没有
condition fields 时使用空 object `{}` 的 digest，而不是空字符串或缺失值。配置中与 condition
fields 无关的变化不得改变 `environment_spec_id`。

#### 10.1.4 Requirement canonicalization

descriptor 中的 requirement 字符串使用 PEP 508 解析，但 PEP 508 本身不规定唯一的重新
序列化形式。v1 validator 因此先用 `packaging.Requirement` 解析，再按下列规则产生唯一的
canonical requirement；descriptor digest 对 canonicalized descriptor 计算，不对生产者提供的
原始 requirement 拼写计算：

- project name 和 extras 使用 PEP 503 `canonicalize_name` 的小写结果；extras 按 ASCII
  升序排列，写成 `name[extra-a,extra-b]`；解析器折叠或 canonicalize 后的重复 extra 必须
  去重，不能要求 validator 从已解析对象恢复原始重复 token；
- specifier 先由 `packaging.specifiers.Specifier` 验证。除 arbitrary equality `===` 外，
  version 使用 `packaging.version.Version` 的字符串结果；`==`/`!=` 的 `.*` 前缀先按
  `Version` canonicalize 再加回 `.*`；`===` 的 value 按区分大小写的 NFC 字符串原样保留。
  每项写成无空白的 `operator + version`，按完整字符串 ASCII 升序排列，以逗号连接；
  解析器折叠或 canonicalize 后的重复 specifier 必须去重，不能要求 validator 从已解析对象
  恢复原始重复 token；没有 specifier 时省略该部分；
- direct URL requirement 使用 `name[extras] @ url` 形式。URL 必须是解析结果中的非空绝对
  URL，不含空白或用户信息；scheme 和 DNS host 转小写，host 使用 ASCII IDNA，移除
  `http:80`/`https:443` 默认端口，空 HTTP(S) path 写成 `/`，path 执行 RFC 3986 dot-segment
  移除；所有 URL component 中的 percent triplet 使用大写 hex，并解码 percent-encoded
  unreserved character。query 和 fragment 保持项目顺序，不做服务端语义推断；specifier
  与 direct URL 不能同时出现；
- marker 不得引用 PEP 508 `extra` variable。descriptor requirement 只描述当前 Channel 的
  直接依赖，v1 不携带 owner-extra context；若需要该语义，必须在未来 schema version 中显式
  建模。其余 marker 由 PEP 508 marker parser 生成 expression tree 后重新序列化：variable 使用规范中
  的小写标识；字符串先 NFC，再使用双引号并只转义 `\` 和 `"`；comparison 两侧和 operator
  之间各一个 ASCII 空格；同层 `and`/`or` 子项先 flatten、按其 canonical string 的 Unicode
  code point 升序排列并拒绝重复，只有保留 `and`/`or` 优先级所需时才写括号。完整 requirement
  使用单个 ASCII 空格包围分号：`base ; marker`；不同的布尔 expression tree 仍视为不同声明，
  v1 不做分配律或运行时 environment 等价推理；
- canonical requirement array 按上述完整字符串的 Unicode code point 升序排列并去重；
  `core_requirements` 与 `isolated_requirements` 的生产者声明顺序和等价重复项不产生语义，
  也不得影响 digest。

测试必须覆盖项目名大小写、extras 顺序、specifier 顺序、marker 空白/引号/同层布尔项顺序、
direct URL 和重复项，并证明等价输入得到相同 canonical requirement array 和 descriptor digest。

### 10.2 依赖校验

安装、repair 和 doctor 执行严格校验：

- lock 文件完整性和 SHA-256；
- Python ABI、平台 tag 和解释器路径；
- installed distribution 的版本、适用时的 direct URL、安装时记录的 wheel provenance
  及 installed files 的 `RECORD` 完整性；
- `PYTHONPATH`、user site 和 site-packages 隔离；
- 必要 import probe 和 Channel driver 初始化探测。

普通启动执行轻量 manifest 校验；Runner `prepare` 执行必要 import 和 driver 探测。
任一步发现当前已提交声明不满足，都不得启动或继续使用不兼容 environment，状态为
`repair_required` 或 `incompatible`。新环境构建失败时，如果不存在仍满足当前已提交
声明的 active generation，则停止对应 Channel；不能启动不符合当前声明的旧版本。

### 10.3 安装形态

| 安装形态 | isolated environment 基础解释器 |
| --- | --- |
| pip/source/conda | 当前 QwenPaw Python，创建不继承主环境 site-packages 的 venv |
| frozen desktop | 应用随附 bundled Python |
| container | 镜像内受支持 Python，或按同一 manifest 预构建 |
| `process_mode=in_process` | 不创建额外 Channel environment |

源码、bootstrap 和 Protocol SDK 从当前 QwenPaw 的显式 `code_root` 加载，不从 ambient
cwd、`PYTHONPATH` 或 dependency environment 推断。普通/frozen 发布物应只读；明确的
source/editable 开发安装可以使用仓库根，但其代码变化必须作为新的 source revision
启动新 generation。

isolated Plugin 的业务源码作为独立、可校验的 plugin artifact 分发，放在 dependency
environment 之外的只读 `code_root`；RunnerSpec 显式传入该根目录，bootstrap 只从
该目录加载声明的 entrypoint。这样既不会把源码安装进 environment，也不会依赖用户
工作树或隐式的 `PYTHONPATH`。

### 10.4 Lock、条件集和目录布局

每个 isolated Channel 的 descriptor 声明直接依赖、条件字段、支持的 Python ABI、
平台和 capabilities。发布流程预生成目标 lock 矩阵；Channel 启动时不得现场解析未锁定的
依赖。lock 必须包含直接及传递依赖的精确版本、environment marker、目标 wheel 和
hash。`condition_set` 只允许机器可求值的配置 equality，必须先按当前 config schema
展开默认值并通过 descriptor 的 allowed-values 校验。

`manifest.json` 对每个
`(python_abi, platform_tag, condition_set_sha256)` 只能映射一个 lock path 和
`lock_sha256`；缺失、重复或多重匹配都必须拒绝。没有完全匹配 lock/wheel 时状态为
`unsupported_platform` 或 `config_invalid`，不得退回宽松解析或现场构建 sdist。

目录布局：

```text
<working-dir>/channel_envs/
  <channel_dir>/
    channel.json
    environments/<environment_spec_dir>/
      environment_spec.json
      <environment_dir>/
        venv/
        dependency.lock
        install.json
    instances/<instance_dir>/
      instance.json
      current.json
      health.json
      state/
      logs/
```

目录键与逻辑 ID 的映射固定为：

```text
channel_dir = dir_key(channel_key)
environment_spec_dir = dir_key(environment_spec_id)
environment_dir = dir_key(environment_id)
instance_dir = dir_key(instance_id)
```

其中 `dir_key(logical_id)` 是 `dir1_` 加 logical ID UTF-8 bytes 的 SHA-256 前 32 个小写
hex。任何目录布局不得直接使用 `channel_key`、`agent_id` 或完整逻辑 ID 作为路径片段。
`channel.json` 保存完整 `channel_key`，`environment_spec.json` 保存完整
`environment_spec_id`，`install.json` 保存完整 `environment_id` 和其 spec ID，
`instance.json` 保存完整 `instance_id`、`agent_id` 和 `channel_key`；pointer 也必须保存其
引用的完整逻辑 ID。创建或读取每一级目录时都要核对对应 manifest，短目录键冲突时必须返回
稳定错误而不是复用目录。manifest 必须先写入 staging 并随目录原子发布；缺失、格式错误或
逻辑 ID 不一致的 manifest 都不得解释为空目录或合法 installation。

逻辑 ID 与磁盘目录键分离，目录键使用跨平台安全的短 hash，完整逻辑 ID 保存在
`install.json` 和 pointer 中。environment 按 Channel lock、ABI、平台和条件集共享，
Runner、secret、checkpoint、日志和运行状态按 `instance_id` 隔离。活动 environment
不可原地升级；repair 在同一 spec 下创建新的不可变 `environment_id`，安装使用跨进程
锁、staging 目录和原子 rename，正在被 lease 引用的 environment 禁止清理。

环境状态至少包括：`not_installed`、`installing`、`installed`、`incompatible`、
`repair_required`、`unsupported_platform` 和 `install_error`。

## 11. Channel descriptor 和 Catalog

Registry 分为静态描述和 Channel 实例解析两层：

```text
get_channel_descriptors() -> Mapping[channel_key, ChannelDescriptor]
resolve_channel_instance(descriptor, agent_context)
  -> BaseChannel | IsolatedChannelProxy
```

descriptor 至少包含：

- `schema_version`、`channel_key`、`source_kind`、`process_mode`；
- `dispatch_mode`，取 `manager_queue` 或 `direct_session`；
- label、description、icon、doc URL 和 entrypoint；
- `ingress_owner`，取 `none`、`runner_owned` 或 `core_owned`；以及 supported targets
  和 capabilities；
- required/conditional dependencies、core requirements 和 external requirements；
- plugin metadata 和配置字段。

#### 11.0 Descriptor v1 规范

CH-0-002 冻结 descriptor 的数据形状；Registry、Catalog 和各个配置面只负责消费该形状，
不在本任务中迁移现有硬编码表。descriptor 是一个 UTF-8 JSON object，顶层字段如下，所有
字段都必须出现（没有值时使用表中规定的空值）。顶层 object 和除 LocalizedText locale map
以外的所有嵌套 object 都是 closed object，等价于 JSON Schema `additionalProperties=false`；
任何未知字段都必须返回 `descriptor_invalid`。LocalizedText 的动态 object 只允许 BCP-47
locale key 和字符串值，不得借此引入其他字段：

```json
{
  "schema_version": 1,
  "channel_key": "feishu",
  "source_kind": "builtin",
  "process_mode": "runner_process",
  "dispatch_mode": "manager_queue",
  "ingress_owner": "none",
  "label": {"en": "Feishu", "zh": "飞书"},
  "description": {"en": "", "zh": ""},
  "icon": "",
  "doc_url": "",
  "plugin_metadata": null,
  "entrypoint": {
    "scope": "runner",
    "module": "qwenpaw.app.channels.feishu.driver",
    "qualname": "FeishuDriver"
  },
  "config_fields": [],
  "core_requirements": [],
  "isolated_requirements": [],
  "condition_fields": [],
  "supported_python_abis": [],
  "supported_platform_tags": [],
  "capabilities": [],
  "bot_identity_fields": [],
  "environment_passthrough_allowlist": []
}
```

字段语义和校验规则如下：

- `schema_version` 当前只能为整数 `1`；未知版本必须拒绝，不能按最佳努力解析。
- `channel_key` 遵守 §10.1.2。descriptor 集合中 key 全局唯一；`builtin` key 必须对应
  发布物中的内置记录，`plugin` key 必须包含稳定的 plugin owner metadata。`plugin_metadata`
  对 builtin 必须为 JSON `null`；对 plugin 必须为 object，严格包含 `plugin_id`、`version`
  和 `artifact_sha256` 三个非空字符串，其中 digest 是 64 个小写 hex 字符。plugin owner
  由 `plugin_id` 精确标识，不能从 channel key 反推。legacy Plugin 的 descriptor 是明确的
  compatibility profile：由既有注册记录确定性合成，`plugin_metadata.artifact_sha256` 可为
  空字符串（表示当前 manifest 没有 artifact digest），且只能保持 `in_process`；历史非
  canonical key 不能伪装成 v1 isolated descriptor。isolated Plugin 则必须提供非空 digest，
  并按 §12.2 校验 artifact 来源。
- `source_kind`、`process_mode`、`dispatch_mode` 和 `ingress_owner` 只能使用各自表中的
  枚举值。`ingress_owner=runner_owned` 必须是 `process_mode=runner_process` 且声明
  `ingress_endpoint` capability；`ingress_owner=none` 不得声明外部 ingress capability。
  `core_owned` 只表达入口归属，不允许把平台 SDK 或 socket 对象放进 Core。
- `entrypoint` 是 object，必须包含 `scope`、`module`、`qualname` 三个非空 ASCII 字段。
  `module` 和 `qualname` 使用点分隔 Python 标识符；`scope=core` 只可与
  `process_mode=in_process` 同时出现，`scope=runner` 只可与
  `process_mode=runner_process` 同时出现。`in_process` 的 entrypoint 解析为
  `BaseChannel`；`runner_process` 的 entrypoint 解析为 Runner 侧 `ChannelDriver`，Core
  侧固定创建 `IsolatedChannelProxy`。不得再添加 `driver_kind` 或其他重复映射字段。
- `label`、`description`、`doc_url`、`config_fields[].label`、`help` 和 `placeholder` 使用
  `LocalizedText`：一个 NFC string，或非空的 BCP-47 locale key 到 NFC string 的 closed-value
  map；locale key 以 ASCII lower-case primary language 开头，可带 BCP-47 subtag，按
  §10.1.1 canonicalize。map 不提供隐式 locale fallback：consumer 必须依次选择精确 locale、
  primary language、`en`、第一个按 key 排序的值。新的 builtin/isolated descriptor 的
  `label` 必须是 map 且含非空 `en`、`zh`；`description` 可以是空 string，map 可含空值；
  legacy 合成记录可以保留现有 string 或 map。`doc_url` 可以是空 string；非空 string 或 map
  中的每个非空值都必须是 `http://`/`https://` absolute URL，不允许 data URL、相对路径或文件
  路径。`icon` 同样只能是空值或 HTTP(S) absolute URL。
- `config_fields` 是保持声明顺序的 array。每个 field object 必须包含
  `name`、`label`、`help`、`placeholder`、`type`、`required`、`nullable`、`default`、
  `allowed_values`、`secret` 和 `condition`：`name` 匹配 `^[a-z][a-z0-9_]*$` 且在 descriptor
  内唯一；`label`、`help`、`placeholder` 是 `LocalizedText` 或空字符串；
  `type` 只能为 `text`、`password`、`number`、`switch` 或 `select`；`required`、
  `nullable`、`secret`、`condition` 为 boolean；无默认值用 JSON `null`；无枚举约束用空
  array。`number` 的 default/allowed value 是 §10.1.1 的 integer 或 finite canonical
  decimal JSON number；不得使用 binary float 或指数拼写。`required=true` 与
  `nullable=true` 的组合必须拒绝；`required=true` 时 effective value 不得为 null 或空字符串；
  `nullable=true` 时允许 null；`required=false` 且
  `default=null` 表示未提供时仍为 null。`config_fields` 是 CLI/API/frontend 的字段展示、
  录入和条件声明投影，不是完整的运行时 value schema；它只覆盖 v1 UI 控件和标量约束。
  array/object/float 等 v1 UI 无法表达的字段可以不出现在 projection 中，仍由完整 schema
  接受和校验；但 `condition_fields`、`bot_identity_fields` 引用的字段必须出现在 projection。
  builtin effective config 的完整类型、array/object/float 约束和默认值继续由现有 Pydantic/
  JSON Schema 负责；CH-0-002 只校验 descriptor 自身的 field 结构和交叉引用，不解析外部
  schema。`CH-2-006` 负责实现 builtin schema adapter 和投影一致性检查，并且不得要求
  descriptor validator import Channel 或平台模块。
  isolated Plugin 的完整 value schema 属于其 plugin artifact 的版本化 schema，由
  `CH-5-001` 冻结；不得把五种 UI type 当作插件运行时 schema。`allowed_values`
  只能包含与 `type` 相容的 canonical JSON scalar value 且不得重复，非空时 effective value
  必须命中；`secret=true` 时 `default` 必须是 JSON `null`，`allowed_values` 必须为空 array，
  且有效 secret value 永不进入 descriptor digest、ID、日志、RPC 或 manifest。
  `condition=true` 的 field 才能出现在 `condition_fields`；`secret=true` 与 `condition=true`
  的组合必须拒绝。
- `core_requirements` 和 `isolated_requirements` 是生产者声明顺序无语义、进入 digest 前按
  §10.1.4 canonicalize、排序并去重的 requirement array。前者表达平台无关的 Core
  最小依赖，后者表达 Runner environment 的直接第三方依赖；两者都可为空。
  CH-0-002 只做 PEP 508 解析和 canonicalization，不判断 distribution 是否属于平台 SDK、
  是否已安装在 Core，也不维护额外的依赖 policy。平台依赖放置由后续逐 Channel 迁移和发布
  验证检查。`process_mode=in_process` 时
  `isolated_requirements` 必须为空，`process_mode=runner_process` 时平台 SDK 必须只在
  `isolated_requirements` 中声明。
- `condition_fields` 是 field `name` 的排序去重 array，所有引用必须存在、`condition=true`、
  且其 effective value 满足 §10.1.3 的标量限制和非空有限 `allowed_values`。condition field
  不得使用 `type=password` 或 `type=number` 的 decimal value；boolean/null 必须按 §10.1.3
  显式列出有限值。无条件 descriptor 必须显式使用空 array；effective `condition_set` 没有
  字段时为 `{}`。
- `supported_python_abis` 和 `supported_platform_tags` 使用 §10.1.3 的 canonical 字符串，
  排序去重；空 array 只表示该 descriptor 不限定目标（`in_process` 的默认形态），不表示
  “未知”。isolated descriptor 至少声明一个 ABI 和一个 platform，发布 lock 再从中选择
  精确组合。
- `capabilities` 是排序去重的 capability ID array；ID 匹配
  `^[a-z][a-z0-9_.-]*$`，按 ASCII lower-case 比较，协议 capability registry 负责词汇表和
  方法 Schema 绑定。未登记的 ID、重复项或与 `ingress_owner` 不一致的 ID 必须拒绝；没有
  能力用空 array，不用隐式默认值。v1 保留以下稳定 capability ID：
  `streaming`、`typing`、`reaction`、`media`、`approval_card`、`server_side_idempotency`、
  `exactly-once-visible`、`ingress_endpoint`、`checkpoint`、`host_state` 和
  `response_lifecycle`。
  `exactly-once-visible` 只有在同时声明 `server_side_idempotency` 的平台 profile 中才可使用。
  能力声明只描述可用操作，
  不改变 `process_mode` 到驱动的唯一映射。
- `bot_identity_fields` 是按 `(name, normalization)` 排序的 object array，每项严格为
  `{ "name": string, "normalization": enum }`；`name` 必须引用 `config_fields` 中的字段，
  包括 `secret=true` 的字段；`normalization` v1 只能为 `strip` 或
  `strip_trailing_slash`。同一 `name` 只能声明一次，即使 normalization 不同也必须拒绝；
  validator 在计算 digest 前按该 key 排序；完整 object array 的源顺序不得影响 digest。
  Core 在 config 级比较时才读取并比较 effective value；descriptor 只保存字段名和
  normalization，任何 secret value 都不得进入
  descriptor、digest、ID、日志、RPC、持久化诊断数据或 API 响应。两种 normalization 都先
  转字符串并去首尾空白，后者再去除所有末尾 `/`；任一字段为空时 descriptor 不产生
  identity。显式空 array 表示该 Channel 不参与查重，不得解释为漏配或默认查重。
- `environment_passthrough_allowlist` 是排序去重的环境变量名 array；名称匹配
  `^[A-Z][A-Z0-9_]*$`，不得出现通配符、赋值号、值、路径或 secret。Runner 的
  `minimal_env` 只从该 allowlist 和 §6.4 的协议启动变量构造，不从 Core 环境无条件继承；
  代理和 TLS 变量必须逐 Channel 显式声明，空 array 表示没有额外透传。

静态 descriptor 读取只允许打开 descriptor 文件、解析 JSON、执行上述结构/字符串校验和
计算 canonical digest；不得 import `entrypoint.module`、探测平台 SDK、读取安装环境、执行
代码或启动 Runner。只有通过静态校验后，Runner bootstrap 才能在 §6.4 的 isolated environment
中按 entrypoint 加载 driver。上述 object 的 required/nullable/empty 语义是 descriptor v1 的兼容契约。
纯模型校验失败统一抛出 `DescriptorValidationError`，其稳定属性
`code == "descriptor_invalid"`；异常消息和字段路径仅用于本地诊断，暂不属于协议契约。
RPC/API 错误映射由后续协议或接入任务负责。
descriptor 的
canonical bytes 按 §10.1.1 编码；计算 digest 前必须把 Requirement arrays 和
`bot_identity_fields` 替换为上述 canonical form，其他 array 按各字段已经规定的顺序处理。
配置默认值展开后的 `condition_set` 和 descriptor digest 必须使用同一 canonical encoder。
任何字段新增、枚举扩展或归一化变化都必须递增
`schema_version` 或新增 ADR，不能静默改变 v1。

descriptor 的逻辑摘要使用 `descriptor_sha256 = sha256("qwenpaw.channel.descriptor.v1" +
NUL + canonical_json(descriptor))`，输出完整 64 个小写 hex 字符。descriptor digest 只对
静态 descriptor 字段计算；不得把 effective config、secret value、lock 内容、environment
installation ID 或运行时状态混入其中。

#### 11.0.1 Descriptor and ID test vectors

以下向量固定使用 §10.1.1 的 compact UTF-8 bytes 和 domain separator + NUL 规则，作为跨
平台实现的最小互操作基线：

| 用途 | canonical JSON bytes | domain separator | expected SHA-256 |
| --- | --- | --- | --- |
| `instance_id` payload | `{"agent_id":"default","channel_key":"feishu"}` | `qwenpaw.channel.instance.v1` | `00aaff7d5548053ae2a51a6bc5e64a3b2e5198a311dcd98be9916162b3e63b17` |
| empty `condition_set` | `{}` | `qwenpaw.channel.conditions.v1` | `dc4e5b494b66d21b82ac92cf406a37d007c80b7d5b986203d5b8d3094d1d051f` |
| string escape | `{"sample":"a\u0001b\n/  é"}` | `qwenpaw.channel.canonical-json.v1` | `5b086f7a2fbaa46869e971cc985df0b13d5422a3013b600ae9883e6e1d5e0b01` |
| `environment_spec_id` payload | `{"channel_key":"feishu","condition_set_sha256":"dc4e5b494b66d21b82ac92cf406a37d007c80b7d5b986203d5b8d3094d1d051f","lock_sha256":"0000000000000000000000000000000000000000000000000000000000000000","platform_tag":"macosx_11_0_arm64","python_abi":"cp313-cp313"}` | `qwenpaw.channel.environment-spec.v1` | `5c705f48418202bdafc20672ae0ccb7c1b178a389389ee6bbd9a8ec7c59264c1` |

`string escape` 中 `\u0001` 与 `\n` 是 ASCII escape bytes，`/`、U+2028、U+2029 和 `é` 是
UTF-8 literal bytes；它必须拒绝将这些 bytes 改写为 `\u000a`、`\/` 或 `\u2028`。前三个
hash 独立于 host platform；最后一个是有意的平台相关向量。实现必须额外把 object member
以不同源顺序提供，并得到相同 bytes。

完整 descriptor digest fixture 的 canonical bytes 由下列 UTF-8 JSON 给出。此 fixture 已完成
Requirement、identity 与集合 array 的 canonicalization；其中 `help` 的 `\n` 是两个 ASCII
escape bytes，`Fixturé` 和 `示例` 是 UTF-8 literal bytes：

```json
{"bot_identity_fields":[{"name":"bot_token","normalization":"strip"},{"name":"url","normalization":"strip_trailing_slash"}],"capabilities":["media","streaming"],"channel_key":"fixture","condition_fields":["region"],"config_fields":[{"allowed_values":["eu","us"],"condition":true,"default":"eu","help":"Line\nhelp","label":{"en":"Fixturé","zh":"示例"},"name":"region","nullable":false,"placeholder":"","required":true,"secret":false,"type":"select"},{"allowed_values":[],"condition":false,"default":null,"help":"","label":{"en":"Token","zh":"令牌"},"name":"bot_token","nullable":false,"placeholder":"","required":true,"secret":true,"type":"password"},{"allowed_values":[],"condition":false,"default":null,"help":"","label":{"en":"URL","zh":"地址"},"name":"url","nullable":false,"placeholder":"","required":false,"secret":false,"type":"text"}],"core_requirements":["requests>=2"],"description":{"en":"","zh":""},"dispatch_mode":"manager_queue","doc_url":{"en":"https://example.com/en","zh":"https://example.com/zh"},"entrypoint":{"module":"qwenpaw.fixture","qualname":"FixtureDriver","scope":"runner"},"environment_passthrough_allowlist":["HTTPS_PROXY"],"icon":"","ingress_owner":"none","isolated_requirements":["fixture[bar,foo]>=1.0 ; python_version >= \"3.11\""],"label":{"en":"Fixturé","zh":"示例"},"plugin_metadata":null,"process_mode":"runner_process","schema_version":1,"source_kind":"builtin","supported_platform_tags":["macosx_11_0_arm64"],"supported_python_abis":["cp313-cp313"]}
```

`sha256("qwenpaw.channel.descriptor.v1" + NUL + bytes)` 必须是
`8b05ef521e5f2ae268f90f704dd36f1fe1e8eb958182c1c0220ffe6405e7cdb8`。测试必须从包括
`Fixture[FOO,bar] >= 1.0`、集合乱序、Unicode 组合字符和 control character 的等价 producer
输入得到这一个 canonical descriptor 和 digest；surrogate、unknown field、`extra` marker、
非有限 condition domain 和 non-registry platform tag 必须失败。

`core requirements` 必须保持平台 SDK 无关，只允许 descriptor 声明的 Core 侧入口或
代理所需的最小、经过审计的实现；平台 SDK、平台客户端和可选原生扩展必须属于
isolated dependencies。

descriptor 是 Catalog、lock generator、installer、Registry、CLI/API/frontend 的静态发现、
展示投影、身份声明、条件声明和环境透传的单一事实来源；builtin effective config 的完整
value schema 仍由 Pydantic/JSON Schema 负责，isolated Plugin 的完整 schema 由其版本化
artifact schema 负责。枚举 descriptor 不应 import 平台 SDK、安装依赖或启动 Runner。

legacy Plugin descriptor 在插件完成既有注册后合成，依赖管理标为 `legacy_shared`，
不进入官方 Channel lock。isolated Plugin descriptor 必须在 Runner 启动前可静态读取，
不要求 Core 导入插件业务模块。

### 11.1 需要收敛到 descriptor 的现有硬编码表

“descriptor 是唯一事实来源”这条要求，落地时等价于消除下列已存在的 per-channel 硬编码
表。实施前必须逐项确认归属，不能只新增 descriptor 而让旧表继续并行生效：

| 位置 | 当前内容 | 收敛方向 |
| --- | --- | --- |
| `app/channels/registry.py` 的 `_BUILTIN_SPECS` | 18 个内置 Channel 的模块路径与类名 | descriptor 的 entrypoint |
| `app/channels/schema.py` 的 `ChannelType` 与 `BUILTIN_CHANNEL_TYPES` | `ChannelType` 为开放的 `str`；独立维护的 `BUILTIN_CHANNEL_TYPES` 仅列 13 个内置 key | descriptor 的 `channel_key` 集合 |
| `app/channels/conflict.py` 的 `_CHANNEL_IDENTITY_FIELDS` | 14 个 Channel 的 bot 身份字段名 | descriptor 的 `bot_identity_fields`（见 §14.4） |
| `app/channels/qrcode_auth_handler.py` 的 `QRCODE_AUTH_HANDLERS` | 5 个平台的扫码登录实现 | 保留在 Core，但按 §14.5 显式登记为例外 |
| `cli/doctor_connectivity.py` 的探测分派表 | 13 个 Channel 的平台连通性探测 | 保留在 Core CLI，按 §14.3 与环境校验区分命名 |
| `cli/channels_cmd.py` 的 label 与 configurator 表 | Channel 显示名与交互式配置函数 | descriptor 的 label/`config_fields` 投影；投影外字段继续由完整 schema 提供类型、默认值和交互渲染 |

收敛的执行 owner 和边界固定如下；本表是 CH-0-002 的契约输出，实际删表或改调用点由
后续任务完成：

| 硬编码表 | 契约 owner | 后续实施任务 | 本任务不做 |
| --- | --- | --- | --- |
| `_BUILTIN_SPECS` | Descriptor entrypoint | `CH-2-006` | 不删除 Registry 表 |
| `ChannelType` / `BUILTIN_CHANNEL_TYPES` | Descriptor key 集合 | `CH-2-006` | 不改变 Plugin key 兼容 |
| `_CHANNEL_IDENTITY_FIELDS` | `bot_identity_fields` | `CH-6-007` | 不迁移查重实现 |
| `QRCODE_AUTH_HANDLERS` | §14.5 的 Core 例外登记 | `CH-6-008` | 不移动扫码代码 |
| doctor connectivity 分派表 | Core CLI connectivity adapter | `CH-6-001` | 不改 `qwenpaw doctor` |
| CLI label/configurator 表 | Descriptor label/`config_fields` 投影；projection 外字段由完整 schema 驱动 | `CH-6-001`、`CH-6-003` | 不改 CLI/Console |

Descriptor 漏配按以下规则失败：业务身份字段、ingress capability、condition field 或
环境透传变量缺失时，validator 返回稳定 `descriptor_invalid`，而不是静默跳过；只有
`bot_identity_fields=[]` 这种显式声明才表示“不参与查重”。

此外 `app/channels/manager.py` 中存在 `ch.channel == "dingtalk"` 的诊断日志分支，属于
Core 侧 per-channel 特例，迁移 DingTalk 时必须一并移除；Core 的通用编排层不得保留按
`channel_key` 的行为分支。

`schema.py` 的 `ChannelType` 当前不是内置 Channel 枚举，而是允许 Plugin 使用任意 key 的
`str`。`BUILTIN_CHANNEL_TYPES` 与 `registry.py` 的 `_BUILTIN_SPECS` 也独立维护：前者只列出
13 个 key，遗漏 `mattermost`、`matrix`、`wecom`、`wechat` 和 `onebot`；后者的 18 个 key 才是
当前内置 Channel 的发现和加载基线。CH-0-001 的职责矩阵必须覆盖这 18 个 registry key，
不得以 13 项 `BUILTIN_CHANNEL_TYPES` 缩小范围。`conflict.py` 覆盖 14 个 Channel、遗漏
`imessage`、`mqtt`、`console`、`sip`、`onebot` 及全部 Plugin Channel。这类漂移正是收敛到
单一 descriptor 的直接动机：最终由 descriptor 的 `channel_key` 集合统一这些事实来源，
实施时应把“漏配即报错”作为 descriptor 校验的一部分，而不是像现在这样静默跳过。

## 12. Plugin 兼容和 isolated Plugin SDK

### 12.1 Legacy Plugin

现有 `type: channel` 插件保持：

- `plugin.py` 调用 `register_channel(channel_class=...)`；
- `BaseChannel` 在 Core 中实例化；
- 共享当前插件依赖环境；
- 原配置、API、UI、消息生命周期和外部行为不变。

Catalog 明确显示 legacy Plugin Channel 的 `source_kind=plugin`、
`process_mode=in_process` 和派生的 `BaseChannel` 驱动接口。legacy 不承诺依赖隔离、
权限沙箱或崩溃隔离。

### 12.2 Isolated Plugin SDK

新插件或主动迁移的旧插件使用：

- 静态 `channel.json`/descriptor；
- 固定的 `ChannelDriver` entrypoint；
- requirements/lock 和支持目标声明；
- Protocol version 和 capability 声明；
- Core 不导入平台 SDK 的 Runner 代码。

插件源码可以独立分发，但仍不应复制到 dependency environment。插件安装器负责校验
包 digest、来源 metadata、descriptor、lock 和 QwenPaw Protocol compatibility；只有
产品已有可信签名链时才额外验证签名，本期不新建插件 PKI。旧插件不自动迁移。
如果选择性迁移失败且迁移尚未 commit，只能继续使用原本仍 active 且声明匹配的
legacy 实例；不能把新的 isolated 配置静默改成 legacy 启动。

## 13. 更新、切换和故障恢复

### 13.1 正常启动

```text
resolve descriptor/config
  -> select exact lock/environment
  -> light validation
  -> spawn Runner
  -> hello
  -> prepare/import probe
  -> activate
  -> consume
```

### 13.2 依赖或源码变化

- 仅源码变化：复用符合 lock 的 environment，启动新 generation；
- lock、ABI、平台或 condition set 的候选变化：创建新的候选 environment；
- 新声明在 candidate prepare 和 health 通过前不 commit，当前活动 Runner 继续按旧的
  已提交声明服务；这不是回退；
- Runner-owned ingress 必须在 commit 前不消费正式事件；
- commit 是持久化 current pointer 与新 active generation 的唯一线性化点；
- 旧 generation 立即失去 lease，不能继续写入事件、状态或 delivery；
- 候选构建或验证失败：候选不 commit，保持当前已提交声明和仍满足它的 active
  generation；不得启动另一份旧源码或旧 environment 作为回退。如果当前声明已经
  commit，或 active environment 已不满足当前声明，则必须停止 Channel 并报告
  `repair_required`。

### 13.3 Core/Runner 崩溃

- Runner EOF 或退出：标记 instance unhealthy，停止其 lease 和已声明的媒体流，并关闭
  或摘除绑定该 generation 的平台 ingress 连接；
- Core 重启：不重新附着旧 Runner，按 instance 指针重新创建新 generation；
- Core 关闭：先 quiesce，再 stop，超时后 terminate/kill；
- Windows 使用 Job Object 或等价机制清理子进程树；
- Operation journal 记录每个切换步骤，支持补偿和孤儿清理。

### 13.4 切换 Journal 和 CAS

依赖或源码变化的切换流程为：

```text
select lock/environment
  -> build staging environment when required
  -> spawn candidate Runner
  -> hello / prepare / standby / health
  -> quiesce old generation
  -> export/import checkpoint
  -> activate candidate with a provisional lease
  -> compare-and-swap current pointer and commit generation
  -> confirm committed lease
  -> stop old Runner
```

`current.json` 至少保存 `instance_id`、`environment_spec_id`、`environment_id`、
`descriptor_sha256`、`source_revision`、`config_revision`、`generation` 和更新时间。
配置正文和 secret 值不写入 pointer；它们继续由现有 Core 配置/secret store 管理，
pointer 只引用不可变 revision。CAS 使用旧 environment/generation/config revision 作为
前置条件；journal 必须先持久化，再原子写 pointer，并使用 flush/fsync/replace。Core
重启时根据 journal、pointer 和进程状态继续、补偿或停止，不重新附着旧 Runner。
candidate 在 pointer CAS 前只能持有 provisional lease，不能消费正式事件或对外发送；
CAS 成功后 Core 发送 commit confirmation，Runner 才进入 active。若确认响应丢失，
Runner 可通过只读 generation status 校验 pointer 后进入 active；校验失败或超时则停止。

失败时不更新 current pointer；未 commit 的候选声明随 operation 一同丢弃，仍满足
当前已提交声明的 active generation 可以继续服务。当前声明已经 commit 或 active
environment 已不匹配时，必须停止并报告 `repair_required`，不能通过启动旧版本实现
回退。首次 legacy→isolated 迁移还必须记录配置、secret、checkpoint、旧实例 quiesce、
唯一 commit 和失败补偿。

## 14. 配置、入口和用户可见状态

### 14.1 配置、密钥和状态所有权

- Channel 配置、effective config、secret 引用和权限策略由 Core 管理；Runner 不持有
  Core 配置对象，也不把 secret 写入命令行、日志、JSON-RPC、descriptor 或持久化
  环境文件。
- `channel.prepare` 接收经过 schema 校验的配置快照、host context 和受控 secret
  handle。host context 至少包含 Core 按现有配置规则解析的 `media_work_dir`；Runner
  完成 import、配置和平台鉴权探测后，Core 才能提交配置变更；失败时保留旧配置和旧
  active generation。
- `secret_handle` 是 prepare-attempt 和 generation scoped 的不透明引用，不是路径、FD
  数字、Windows HANDLE 或 secret value。wire value 是有长度上限且不含控制字符的非空
  token；协议层不得解释其平台表示。handle 只允许出现在 `channel.prepare.host_context`，
  Runner 进入 `preparing` 后、返回 prepare response 前发起一次消费。消费尝试开始即失效，
  无论成功或失败都不得复用；prepare 结束后也不得保存在 standby/active 的 host context、
  日志、诊断或持久状态中。
- `request.cancel` 是 prepare attempt 的独立协议状态，不能依赖 secret consumer 是否重新
  抛出取消异常。即使 consumer/sink 捕获取消并正常返回，Runner 也必须在提交 host context
  和 `standby` 前检测取消，将 prepare 收敛到 `failed`，同时保持 handle 已消费且不可复用。
- 未知、跨 generation 或没有可用 consumer 的 handle 返回 `SECRET_HANDLE_INVALID`；已经
  消费、过期或重复使用返回 `SECRET_HANDLE_CONSUMED`。handle 解析成功但平台凭证无效仍
  返回 `PLATFORM_AUTH_FAILED`，不得混淆为 handle 错误。Phase 0 允许使用只存在于测试进程
  的 fixture consumer 验证这些 wire 语义；真实 POSIX pipe/FD、Windows HANDLE 的创建、
  继承、读取和销毁属于 CH-1-006。
- secret value 始终不得进入 JSON-RPC、命令行、日志、descriptor、diagnostic、返回值或
  持久环境。fixture consumer 也只能在进程内把解析值交给初始化逻辑，不能把值返回协议
  层。Runner 保存的 host context 必须删除 `secret_handle`。
- 上述旧实例继续服务只适用于新配置/新声明仍处于 staging、尚未 commit 的情况；此时
  当前生效声明没有变化，不属于版本回退。新声明一旦 commit，任何不满足它的旧
  environment 或旧源码都不得重新启动。
- 配置变化导致 `condition_set` 或 lock 变化时，按环境 reconcile 处理；不能继续
  使用旧条件分支的 environment。普通配置变化也必须经过 prepare 和唯一 commit。
- Runner 不直接并发写入 Core 的 instance state 目录。Channel 专属状态通过版本化
  checkpoint 或受限 Host State API 持久化：

```text
host.state.get
host.state.put
host.state.delete
```

状态必须带 `schema_version`、大小限制和原子写入语义；只有 active generation 可以
写入，standby Runner 只能读取或导入 checkpoint。

### 14.2 外部平台连接和入口归属

- Polling、WebSocket client、MQTT 和其他主动连接由 Runner 直接连接平台，所用平台
  协议与 Core↔Runner stdio IPC 解耦。
- `runner_owned_ingress` 由 Runner 或受信任反向代理接收平台请求。显式端口或动态
  `port=0` 必须在 descriptor/config 中声明；动态端口绑定成功后通过
  `ingress.endpoint.register/update/unregister` 返回 Core。candidate Runner 在 commit
  前可以绑定隔离的本地候选端口并报告 readiness，但不得接收正式平台流量或产生
  Twilio webhook 等外部副作用。
- `core_owned_ingress` 只在统一公网入口、桌面部署或运维边界确实需要时启用。它是
  受控兼容选项，不是 Voice 的首选目标；若采用，必须单独冻结最小 DTO、顺序、背压、
  关闭和 generation fencing。
- Voice/Twilio 的目标是 Runner-owned Webhook、签名校验、TwiML、一次性 WebSocket token
  和 ConversationRelay 连接。Runner 负责平台 SDK 和公网入口，Core 只接收稳定事件并
  通过 `channel.send` 返回出站操作。若部署条件使 Core-owned 入口更可行，仍须保持
  平台 SDK 在 Runner，且不得把 Core socket 对象跨进程传递。
- Voice 使用的 Cloudflare tunnel 或未来等价反向代理，原则上指向 active Runner 的
  本地入口；也可以由受控代理统一持有 public URL 并把流量转发到 active Runner。只有
  committed active generation 可以配置 Twilio webhook/status callback；standby 只能做
  无外部副作用的凭证探测和本地 readiness。`twilio_auth_token` 由 Core secret store
  通过 secret handle 注入 Runner，不通过 JSON 复制。切换窗口暂停新连接准入或返回明确
  busy/error，不把正式流量路由到 standby 或旧 generation。
- Runner-owned ingress 的热切换不承诺零停机。候选 Runner 可使用独立端口并在 commit
  时切换外部代理；不支持动态切换时允许短暂停机，但必须可恢复且不能让 standby
  消费正式事件。

### 14.3 Catalog、CLI/API 和状态

Catalog 必须是静态 descriptor 与 Channel 状态的组合，不能通过 import 成功与否判断
Channel 是否存在。descriptor 至少包含 `channel_key`、`source_kind`、`process_mode`、
entrypoint、依赖/条件声明、支持平台、入口归属和 capabilities。

状态拆成三个维度：

```text
install_status:
  unsupported_platform | not_installed | installing | installed |
  incompatible | repair_required | install_error

instance_status:
  disabled | stopped | starting | standby | running | quiescing |
  backoff | circuit_open | channel_error | config_invalid

platform_status:
  unknown | connected | degraded | auth_error | rate_limited
```

环境操作按 `channel_key` 作用于共享 dependency environment；实例操作按当前 Agent
的默认 `instance_id` 作用。建议提供：

```text
qwenpaw channels list
qwenpaw channels install <channel_key>
qwenpaw channels repair <channel_key>
qwenpaw channels verify-env <channel_key>
qwenpaw channels restart <channel_key>
```

新增子命令必须与现有 CLI 表面共存，不得改写既有语义：

- 仓库已有 `qwenpaw channels list`、`channels config`、`channels send` 三个子命令。
  `list` 的现有输出必须保持兼容；`config` 是交互式配置入口：显示名和 `config_fields`
  可表达的 field projection 按 §11.1 收敛到 descriptor，projection 外的 array/object/float
  等字段仍从完整 Pydantic/JSON Schema（Plugin 为 artifact schema）取得类型、默认值和
  交互渲染，不能因未出现在 descriptor 而丢失；`channels send` 是主动外发，隔离后必须经由
  Runner 的 `channel.send` 执行，Runner 未运行时返回明确错误，不得静默失败或绕过 Runner
  直接调用平台 SDK。
- 环境/依赖校验命名为 `channels verify-env`，**不叫 `doctor`**。仓库已存在顶层
  `qwenpaw doctor`，其语义是平台连通性探测（直连 `api.telegram.org`、`open.feishu.cn`
  等 13 个 Channel 的平台端点）并校验凭证字段，与本设计的 lock/ABI/distribution 完整性
  校验是两件事。两者都保留：`channels verify-env` 只做本地环境校验且不产生网络 I/O，
  `qwenpaw doctor` 继续负责平台可达性。文档、帮助文本和 Console 必须明确区分，避免
  用户把“环境不匹配”与“平台连不通”混为一谈。

安装和 repair 返回 operation id，由 CLI/API/Console 查询进度；health/list/Core 启动
只检查状态，不隐式安装依赖。用户启用/启动 Channel 时可以按产品策略自动准备精确
lock environment，并必须展示 operation 进度和失败结果。v1 不提供远程 environment
feed、源码版本选择或
`rollback-environment`；不匹配且 repair 失败时保持停止并报告 `repair_required`。

### 14.4 Bot 身份查重

Core 已有“同一个 bot 被多个 Agent 使用”的查重能力：Console 在保存 Channel 配置前调用
`POST /api/config/channels/{channel_name}/conflict-check`，命中时弹出确认框，用户仍可选择
继续保存。它是**告警式前置检查**，不阻塞保存也不阻塞启动，这个产品语义在隔离后保持不变。

当前实现有两处与隔离不兼容：它通过遍历**其他 Agent 的活跃 Channel 实例**
（`workspace.channel_manager.channels`）判断“对方是否在用”，并直接读取其他 Agent 的内存态
`Config`。隔离后活跃实例位于各自的 Runner 进程中，Core 不再持有可遍历的 `BaseChannel`
列表，这条路径必然失效。

本设计采用 config 级比较，不依赖存活探测：

- **身份字段来自 descriptor。** descriptor 增加 `bot_identity_fields`，声明该 Channel 用于
  判定“同一个 bot”的配置字段名及其归一化规则（当前实现对 `homeserver`/`url` 会去掉尾部
  斜杠，需保留）。这取代 `conflict.py` 的硬编码表，并顺带解决现有实现遗漏 5 个内置
  Channel 和全部 Plugin Channel 的问题：descriptor 未声明该字段的 Channel 明确表示“不参与
  查重”，与“忘记配”区分开。身份字段允许引用 secret config field，以保留 Discord、
  Telegram、Slack、Mattermost 和 WeChat 现有的 token 查重；descriptor 只保存字段名和
  normalization，secret effective value 只在 Core 内存中比较，不进入 digest、日志、RPC、
  持久化诊断或响应。
- **比较只读配置。** Core 枚举已配置的 Agent（`config.agents.profiles` 与
  `load_agent_config(agent_id)`，与 `/agents` 列表同源），读取各自的 `channels.<key>` 配置段，
  按 descriptor 声明提取身份值后比较。不访问其他 Agent 的运行对象，不发起 Runner RPC。
- **secret 不出响应体。** 查重只返回命中的 `agent_id` 和 Agent 名称，不回显身份字段值本身；
  现有实现已有对应回归测试，隔离后必须保持。

**`enabled` 的判定规则**：两侧都要求 `enabled=true` 才参与查重。

- 被保存的一侧：若提交的配置 `enabled=false`，直接跳过检查。这与当前实现一致
  （现有接口在 `enabled` 为假时立即返回 `conflict=false`），保留即可。理由是禁用的
  Channel 不会连接平台，不构成实际冲突；用户之后把它启用时会再次经过保存流程，那时
  才需要告警。
- 被比较的一侧：只比较其他 Agent 中 `enabled=true` 的 `channels.<key>` 配置段。这一点
  取代了现有的存活探测，且与现有效果等价——`ChannelManager` 只启动 `enabled` 的
  Channel，所以“存在活跃实例”本身就已隐含 `enabled=true`。

因此这次改动只把“是否正在运行”换成“是否已启用”，不改变 `enabled` 语义本身。禁用的
配置两侧都不参与，不会因为改成 config 级比较而产生一批无意义告警。

这带来一处需要接受的语义变化：去掉存活过滤后，命中范围从“对方正在运行”扩大为“对方已
配置且已启用”。这个方向是更正确的，因为配置冲突在对方启动的那一刻就会实际发生，提前告警比启动
后才暴露更有价值；同时也修掉了现有实现的一个盲区：只有已加载的 Agent 才可见，未启动的
Agent 即使配了同一个 bot 也不会被发现。为了让文案准确，响应中应附带对方的
enabled/instance 状态，由 Console 区分“已配置”和“正在运行”两种措辞。该状态取自 Core 侧的
instance 注册信息（§14.3 的 `instance_status`），不是反射其他 Agent 的对象。

### 14.5 扫码/设备码登录：保留在 Core 的显式例外

`app/channels/qrcode_auth_handler.py` 为 WeChat/iLink、WeCom、DingTalk、Feishu/Lark 和 QQ
提供扫码与设备码登录，由 Core 的 config router 挂载
`GET /config/channels/{channel}/qrcode` 和 `.../qrcode/status`。本设计明确：**该模块保留在
Core，作为“Core 不承载平台逻辑”这条原则的显式例外。**

理由是时序而非偏好：扫码登录的目的就是获取平台凭证，它必须在 Channel 配置提交之前完成。
那个时刻既没有可用配置，也没有 committed generation，因此不存在可以询问的 Runner。把它
迁入 Runner 需要引入“无配置的鉴权探测态”这一整套新生命周期和 RPC，代价远超收益。

例外必须是有界的，边界如下：

- 该模块**不得导入任何平台 SDK**。当前它只使用 `httpx` 直连平台端点，这一点满足依赖隔离
  的真实目标（避免第三方 SDK 进入 Core 环境），必须保持。
- 该模块**不得导入 Channel 内部符号**。当前 WeChat 分支 `from ..channels.wechat.client import
  ILinkClient, _DEFAULT_BASE_URL` 违反了这条，且引用的是私有符号。迁移 WeChat 时必须切断
  该依赖：把所需最小客户端逻辑下沉为 Core 侧可维护的实现，或由 Core 复制一份明确契约的
  最小 HTTP 调用，不再跨层引用 Channel 私有实现。
- 平台域名、状态码表和响应解析属于该例外的既有内容，不再扩大。新增 Channel 默认不进入
  该模块；若确实需要扫码登录，必须在 descriptor 中声明并在本节登记。
- WeCom 分支目前用正则抓取平台页面中的 `window.settings`，QQ 分支在 Core 内做
  AES-256-GCM 解密。这两处属于已知脆弱实现，迁移期间保持行为不变，但必须记录为技术债，
  且不得因“反正是例外”而继续增加同类逻辑。

Catalog 与 Console 的 Channel 存在性、状态和配置表单仍以 descriptor 为准；扫码登录只是
配置阶段的辅助入口，不构成第二套 Channel 事实来源。

### 14.6 环境变量透传白名单

`minimal_env` 原则上只包含 Runner 启动必需的变量（§6.4）。但有一类环境变量是 Channel
真实功能所依赖的，隔离后必须能够到达 Runner，否则属于功能回归：

- 代理：`TELEGRAM_HTTP_PROXY`、`TELEGRAM_HTTP_PROXY_AUTH`、`DISCORD_HTTP_PROXY`、
  `DISCORD_HTTP_PROXY_AUTH`，以及 Slack 使用的通用 `HTTP_PROXY`/`HTTPS_PROXY`；
- TLS：`SSL_CERT_FILE` 等证书路径变量。

这些是部署环境的真实约束（例如国内访问 Telegram/Discord 依赖代理），不是可选装饰。
因此 descriptor 必须支持声明**环境变量透传白名单**，`minimal_env` 按白名单构造：既不
从 Core 进程环境无条件继承，也不因为“最小化”而把代理和证书配置一并清掉。白名单是
descriptor 的一部分，逐 Channel 声明并可审计。

需要明确排除的一类：仓库中还存在若干把平台网关指向非官方地址的开关（Feishu `domain`
接受完整 URL、WeCom/XiaoYi/Yuanbao 的 `ws_url`、QQ 的 `QQ_TOKEN_URL`/`QQ_API_BASE`）。
这些是测试为方便 mock 而加入的注入点，**不是 Channel 的产品功能，也不是本设计的兼容
目标**。不得为保住这些注入路径而调整架构实现、协议 DTO 或 descriptor 字段。隔离过程中
若某条 mock 路径失效，由测试侧自行调整，不计入 Channel 迁移的行为回归项。

Feishu 的 `domain` 需要单独区分：`feishu` 与 `lark` 两个枚举值是真实产品能力（国内版与
国际版），必须保留，且 §14.5 的扫码登录在 Core 侧同样需要读取它来选择 accounts 域名；
被放宽为接受任意 URL 的那部分才属于上述 mock 注入，不予兼容。

## 15. 安全边界

stdio 是进程绑定的 IPC，不监听本地端口，但不构成不可信插件沙箱。官方源码默认受
QwenPaw 安装包信任；如果未来支持不可信第三方代码，需要另行评估 Windows
restricted token、macOS sandbox、Linux namespace/seccomp 或容器。本期必须实现：

- minimal environment，不继承 `PYTHONPATH`、user site 和不必要的 secret；
- protocol method allowlist；
- schema、最大帧、超时、并发和 backpressure 限制；
- `channel_key`、`instance_id`、generation、environment 和 capability 校验；
- secret 只通过受控进程启动链路注入，不写入日志、descriptor、协议响应或 command line；
- 媒体定位符的类型、路径读取权限和 URL 脱敏校验；
- 单一写入器；
- stderr 日志脱敏和持续排空；
- 旧 generation 的 fencing；
- Windows handle inheritance、macOS/Linux fd inheritance 的显式白名单。

## 16. 迁移边界

迁移顺序：

1. 先实现 Protocol SDK、stdio framing、Runner bootstrap 和环境管理；
2. 用飞书验证主动连接、媒体和两个 Agent 实例的进程隔离；
3. 用 OneBot 验证 Runner-owned ingress；
4. 用 Voice/Twilio 验证优先采用 Runner-owned ingress 的签名/TwiML 边界、endpoint 注册、
   ConversationRelay 文本消息和切换语义；只有原型证明部署边界不允许时，才另行验证
   Core-owned 兼容入口。原始媒体 pipe 只在未来采用对应平台数据流时单独验证；
5. 迁移其余官方 Channel，或对不适合隔离的 Channel 明确保留
   `process_mode=in_process`；
6. 选择性迁移 legacy Plugin；
7. 完成 CLI/API/frontend/Catalog 切换和跨平台发布验证。

每个 Channel 必须保留现有单测、contract 测试和行为回归测试。新架构只有在该
Channel 的进程隔离、Protocol、环境、迁移和发布 Gate 全部通过后，才切换默认执行路径。
Console Channel 是 Core 内部控制面入口，明确保留 `process_mode=in_process` 和
派生的 `BaseChannel` 驱动接口，不进入平台 SDK 迁移批次；其行为仍必须参加兼容回归。

迁移批次只用于控制单个 Chat 的工作量，不改变 Channel 最终采用的进程位置和驱动接口。
三个剩余批次按主要技术风险划分，避免一个 Chat 同时迁移全部官方 Channel：

| 批次 | Channel | 主要风险 |
| --- | --- | --- |
| 标准主动连接 | Telegram、Discord、Slack、Matrix、MQTT | 长连接、订阅、checkpoint、媒体和 streaming |
| 企业 SDK/复杂行为 | WeCom、DingTalk、QQ、WeChat、Mattermost | 企业鉴权、群聊、卡片、媒体和限流 |
| 平台特定/系统耦合 | XiaoYi、Yuanbao、iMessage、SIP | 私有 SDK、本机权限、系统服务、实时媒体或入口边界 |

表中的分类不是对进程位置的预判；每个 Channel 仍需在 descriptor、职责矩阵和迁移
任务中明确选择 `runner_process` 或 `in_process`，驱动接口按上述规则确定。

每个正式迁移任务都必须对批次内的每个 Channel 分别完成以下清单，不能只用批次级
冒烟测试代替单 Channel 证据：

1. 盘点现有配置 schema、Registry、CLI/API/Console、文档和第三方依赖；
2. 明确 ingress owner、`process_mode`、capability、checkpoint 和 secret 边界；
3. 建立静态 descriptor、lock/manifest、Runner entrypoint 和 Core proxy 映射；
3.1 从 §11.1 的硬编码表中移除该 Channel 的条目，并补齐 descriptor 的
   `bot_identity_fields` 与环境变量透传白名单；该 Channel 若涉及扫码登录或 mock 注入点，
   按 §14.5、§14.6 逐条确认边界；
4. 保持 `BaseChannel` 外部 contract，包括 ACL 的 `acl_sender_id`、session、群聊/私聊、
   `uses_manager_queue`/dispatch、mention、streaming、approval、卡片、媒体、
   typing/reaction 和主动发送中实际支持的项；
5. 完成两个 Agent 并行、Runner 崩溃/重启、shutdown、重复事件和平台断线测试；
6. 从 Core 默认依赖和进程内 import 路径中移除已经迁移的 SDK；决定保留
   `process_mode=in_process` 的 Channel 必须记录不可隔离原因、Catalog 状态和等价
   回归证据；
7. 单独记录该 Channel 的测试、未支持 capability 和平台限制，批次中的一个 Channel
   失败不得被其他 Channel 的通过结果掩盖。

## 17. 验收标准

### Protocol 和进程

- stdio 在 macOS、Linux、Windows 可双向收发；
- LSP framing 正确处理半帧、粘包、非法 Header、EOF、超时和最大帧；
- stdout 不被 SDK `print()` 或原生 FD 1 输出污染；
- stderr 持续排空，不因日志阻塞 Runner；
- JSON-RPC request/response/notification、cancel、错误和 capability 协商通过测试；
- Runner 崩溃、Core 关闭和 pipe 断开不会导致 Core 退出。

### 环境和兼容

- Channel 源码不在 dependency environment；
- 源码变化但 lock 不变时复用 environment；
- lock、ABI、平台或实际 distribution 不匹配时禁止启动；
- 未 commit 的候选环境构建失败保留当前合法 active generation；当前声明不匹配时
  停止，且不启动不兼容旧环境；
- 两个 Agent 可以共享同一 environment，但 Runner、secret、checkpoint、日志和
  generation 完全隔离；
- Core Channel、runner-process Channel 和 legacy Plugin Channel 可以混合运行；
- 现有 Core Channel 和 legacy Plugin Channel 外部行为无回归。

### 可靠性和业务

- 入站 ACK 只在 Inbox 持久化和幂等检查完成后返回；
- 重复 batch/event 不重复进入 Agent；
- delivery ledger 能表达 acknowledged、failed、timeout 和 unknown；
- standby 不消费正式事件，旧 generation 被 fencing；
- 普通媒体路径/URL 双向传递、入站下载目录解析、文件完整发布、跨平台路径解析和
  OneBot URL 回归通过测试；普通附件和当前 Voice/Twilio 不依赖二进制数据面；
- Voice ingress 的鉴权、endpoint/generation 绑定、顺序、背压、关闭和切换 fencing 通过
  测试；首选方案下 Runner 持有平台 WebSocket，Core-owned 兼容方案不得把 socket 对象
  跨进程传递；
- Core 侧不再保留 `routers/voice.py` 的三条路由及其挂载，`twilio` 不在 Core 默认依赖中；
- 非 loopback 绑定的 ingress endpoint 必须已鉴权；端口重新绑定触发 endpoint 更新且 Core
  记录与实际监听一致；
- 共享 session 群聊下 ACL 身份不跨发送者合并：白名单成员与非白名单成员在同一 debounce
  窗口发言时，各自按自身身份判定，通过测试；
- bot 身份查重不访问其他 Agent 的运行对象，未启动 Agent 的配置冲突同样可被发现，且响应
  不回显身份字段值；
- 环境变量透传白名单生效：Telegram/Discord/Slack 的代理变量和 `SSL_CERT_FILE` 在隔离后
  仍然生效；
- `channels verify-env` 不产生网络 I/O，与 `qwenpaw doctor` 的平台连通性结果互不混淆；
- 所有落盘型 Channel 使用 Core 解析的最终 `media_work_dir`，文件平铺在该目录中；各 Channel
  现有下载、文件名和发送行为无回归；
- 所有相关 Python 单测通过率 100%，改动文件通过 pre-commit。

### 测试矩阵

至少覆盖：协议编解码、半帧/粘包/非法 Header/EOF、握手和版本协商、错误码、环境 ID、
lock hash、状态机、Inbox/Delivery 去重、unknown、配置下发、Runner 崩溃、日志堵塞、
standby 禁止消费、旧 generation fencing、媒体路径/URL 双向传递、入站下载目录和文件
完整发布、跨平台路径解析、Polling checkpoint、Voice Webhook、一次性 token、
ConversationRelay 文本 WebSocket 的顺序/背压/关闭、Runner-owned endpoint 注册与
切换、必要时的 Core-owned 兼容入口、
Core Channel、runner-process Channel 和 legacy Plugin Channel 混合运行、两个 Agent
共享 environment 但状态隔离，以及并发安装、
磁盘不足、依赖源不可达、跨平台路径和进程树清理。

平台验证至少包括 macOS Intel/Apple Silicon、Windows 11、Linux x86_64，以及 Python
3.11、3.12、3.13；frozen desktop、pip/source/conda 和 container 的 environment 语义
必须一致。

## 18. 决策记录

| ID | 决策 | 状态 |
| --- | --- | --- |
| ADR-001 | 官方 Channel 源码随当前 QwenPaw 发布 | 已确认 |
| ADR-002 | dependency environment 只包含第三方依赖，不安装 Channel 源码 | 已确认 |
| ADR-003 | environment 按 channel/lock/ABI/platform/condition 复用 | 已确认 |
| ADR-004 | Core 保留 ChannelManager/BaseChannel；isolated 使用 Proxy | 已确认 |
| ADR-005 | Core↔Runner 使用 stdio，不使用 loopback TCP | 已确认 |
| ADR-006 | stdio 使用 LSP Content-Length framing | 已确认 |
| ADR-007 | 消息语义使用 JSON-RPC 2.0，不采用完整 LSP 业务协议 | 已确认 |
| ADR-008 | 借鉴 Matrix AS transaction/ACK/retry/dedup，不采用 Matrix AS HTTP API | 已确认 |
| ADR-009 | 借鉴 MCP command/args/env/cwd 和按需拉起，不把 uvx 作为生产 launcher | 已确认 |
| ADR-010 | 入站至少一次投递；ACK 以 Inbox 持久化为准 | 已确认 |
| ADR-011 | 普通媒体直接传 Content 定位符；原始实时媒体才可使用独立 pipe | 已确认 |
| ADR-012 | 本期单实例，底层保留 instance_id 扩展点 | 已确认 |
| ADR-013 | 现有第三方 Plugin 保留 legacy；新 Plugin 使用 isolated SDK | 已确认 |
| ADR-014 | Core 保留平台无关 ContentParts、approval 和 fallback；Runner 编码平台原生 payload | 已确认 |
| ADR-015 | bootstrap 先复制初始 stdout 协议句柄，再把普通 stdout/FD 1 重定向到 stderr | 已确认 |
| ADR-016 | secret 默认通过一次性继承 pipe/handle 注入，不进入控制协议或持久环境 | 已确认 |
| ADR-017 | activate 只授予 provisional lease；pointer/generation CAS 与 commit 是唯一切换点 | 已确认 |
| ADR-018 | v1 不实现 Core↔Runner media pipe；未来只有原始连续媒体跨边界时才单独设计版本化数据面 | 已确认 |
| ADR-019 | isolated Plugin 必须校验 digest 和来源；只复用已有签名链，不新建插件 PKI | 已确认 |
| ADR-020 | Runner 用 isolated Python 和显式 code_root 启动，不依赖 cwd、PYTHONPATH 或环境内 QwenPaw | 已确认 |
| ADR-021 | descriptor 分离 source_kind 和 process_mode；驱动接口由 process_mode 唯一确定 | 已确认 |
| ADR-022 | Runner 侧平台接入接口命名为 ChannelDriver；Plugin 只表示来源和分发方式 | 已确认 |
| ADR-023 | Runtime 专用于 Agent 请求编排层；Channel 标识符和用户文案不得复用该术语 | 已确认 |
| ADR-024 | Core 在 prepare host context 中传入 effective media_work_dir；该目录只服务入站落盘，不限制出站路径且不改变现有清理行为 | 已确认 |
| ADR-025 | Voice/Twilio 目标是 Runner-owned 公网 ingress。Core-owned ingress 是**当前既有实现**而非将来的退路；若最终仍需保留，必须有独立 ADR 并单独冻结 DTO、顺序、背压、关闭和 fencing | 已确认 |
| ADR-026 | descriptor 显式声明 manager_queue 或 direct_session 调度；Voice/SIP 保持现有 direct session 行为，包括不启用 Core ACL gate 与 TaskTracker | 已确认 |
| ADR-027 | 扫码/设备码登录保留在 Core，作为显式且有界的例外；不得导入平台 SDK，也不得引用 Channel 内部符号 | 已确认 |
| ADR-028 | bot 身份查重改为 config 级比较，身份字段由 descriptor 声明；不访问其他 Agent 的运行对象，命中范围由“正在运行”扩大为“已配置” | 已确认 |
| ADR-029 | 环境/依赖校验命令命名为 `channels verify-env`，与既有平台连通性 `qwenpaw doctor` 并存且语义分离 | 已确认 |
| ADR-030 | descriptor 声明环境变量透传白名单，用于代理和 TLS 等真实部署约束；`minimal_env` 按白名单构造，既不无条件继承 Core 环境，也不清掉这些变量 | 已确认 |
| ADR-031 | ACL 身份不得跨发送者合并；Core 在 merge 前按 ACL 身份切分批次，隔离后 Runner 逐事件携带 `acl_sender_id` | 已确认 |
| ADR-032 | `media_work_dir` 是新增的 Core 侧解析能力，不是既有实现的迁移；收敛时保留各 Channel 现有默认子目录 | 被 ADR-034 替代 |
| ADR-033 | 平台网关地址注入（Feishu `domain` 的 URL 形态、WeCom/XiaoYi/Yuanbao 的 `ws_url`、QQ 的端点环境变量）是测试 mock 注入点，不是产品功能，不作为兼容目标；失效由测试侧处理。Feishu `domain` 的 `feishu`/`lark` 枚举值除外，属真实能力 | 已确认 |
| ADR-034 | 对需要入站媒体落盘的 Channel，Core 统一解析 `config.media_dir` → `workspace_dir/media` → `WORKING_DIR/media`；`from_env` 使用 `<CHANNEL>_MEDIA_DIR` → `WORKING_DIR/media`。最终目录平铺，不追加 Channel 子目录；各 Channel 保留现有下载、命名、覆盖和清理行为，不迁移既有文件 | 已确认；替代 ADR-032 |
| ADR-035 | v1 标识使用带唯一 string escape 和 finite decimal 的受限 canonical JSON、domain separator + NUL、完整 SHA-256 和稳定前缀；逻辑 ID 与 `dir1_` 磁盘目录键分离，目录 manifest 保留并核对完整逻辑 ID；platform tag 必须属于版本化 release target registry | 已确认 |
| ADR-036 | v1 descriptor 使用 closed object、显式空值和字段级 required/nullable/secret/condition 语义；Requirement 在 digest 前统一 canonicalize，重复折叠且拒绝 `extra` marker；condition domain 必须有限；`config_fields` 是支持 number 的 UI 投影，完整 value schema 仍由 Pydantic/JSON Schema 或 plugin artifact schema 负责；身份声明可引用 secret 字段但 secret value 仅在 Core 内比较；静态读取不得 import 平台模块；process mode 唯一派生驱动接口 | 已确认 |
| ADR-037 | request-scoped response 的终止由 Core 通过可重试、幂等的 `channel.response.finish` 显式通知 Runner；不得由 message、stream 或 delivery ACK 推断。Runner 以有界 closed tombstone 保持关闭单调性，并在线性化边界上与在途出站操作排序 | 已确认 |
| ADR-038 | Core 以有界 generation authority 同时持有一个 active 和一个 candidate，并通过 immutable snapshot、candidate epoch 与 operation token 统一 Host RPC 和 endpoint route authorization；只有 committed generation 的 `ready && !quiescing` endpoint 可接收正式流量。quiesce/stop 在 Runner RPC 前撤销，lease expiry 和 generation replacement 单调 fencing，迟到 control/endpoint 响应不得复活旧 generation | 已确认 |
