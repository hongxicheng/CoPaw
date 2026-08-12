# CH-0-001 Core/Runner 职责和兼容边界

- 任务：`CH-0-001`
- 状态：`[-] 等待独立 Review`
- 事实来源：`DESIGN.md`；本文件只冻结职责、接口和兼容边界，不实现隔离运行时。
- 直接下游：`CH-0-007`、`CH-0-008`、`CH-0-009`
- 所属 Gate：`G0`

## 1. 范围和决策引用

本文件落实 `DESIGN.md` §1.1、§4、§4.1、§4.3、§5.1--§5.3、§8.3--§8.4、
§9.1、§11--§11.1、§12.1、§14.1--§14.2、§16 和 §17 中与
Core/Runner 边界直接相关的内容。适用 ADR 为 ADR-004、ADR-011、ADR-013、
ADR-014、ADR-018、ADR-021、ADR-022、ADR-023、ADR-024、ADR-025、ADR-026、
ADR-031 和 ADR-034。ADR-032 已被 ADR-034 替代，本任务不采用 ADR-032 的
“保留 Channel 子目录”规则。

本任务冻结的是可审查的职责矩阵、接口语义、序列化边界、媒体和调度清单以及
兼容调用表。正式的 stdio、JSON-RPC、环境、进程监督、媒体解析器和 Channel
迁移分别属于其它任务。

## 2. Core / Runner 职责矩阵

| 能力 | Core / `ChannelHostAdapter` | Runner / `ChannelDriver` |
| --- | --- | --- |
| 配置和密钥 | 读取 schema、权限策略和 secret 引用；生成快照 | 只消费受控快照和 secret handle；不保存 Core 配置对象 |
| 入站事件 | 持久化 Inbox、去重、ACK、session、debounce、队列和 AgentRequest | 连接平台、解析原生事件，逐事件产生稳定 DTO |
| ACL | 计算 `acl_sender_id`、执行 ACL gate、处理 approval | 原样携带真实发送者的 `acl_sender_id`，不合并发送者 |
| Agent 编排 | AgentRequest/Event、TaskTracker、Workspace、streaming 聚合和 usage | 不导入 Agent、Workspace、TaskTracker 或数据库 |
| 渲染语义 | 平台无关 ContentParts、fallback 文本和出站目标解析 | 将平台无关 DTO 编码为平台文本、卡片、媒体、typing 和 reaction |
| Approval | 状态、决策和统一策略属于 Core | 仅解析平台按钮回调并调用平台 API 呈现结果 |
| 媒体目录 | 按 ADR-034 解析 effective `media_work_dir` 并传入 host context | 只使用绝对目录执行平台入站落盘，不推断 workspace 或追加子目录 |
| 平台连接 | 管理 descriptor、lease、generation、生命周期和故障状态 | 初始化 SDK、鉴权、连接恢复和平台 checkpoint |
| 平台入口 | 只路由稳定 DTO；不得接收原始 HTTP/WebSocket 对象 | 持有 runner-owned ingress、签名校验、平台状态机和 endpoint |
| 主动发送 | 创建 `delivery_id`、记录 ledger、处理结果和重试语义 | 调用平台 API，返回可诊断的 delivery 状态 |
| 诊断 | 区分环境校验和平台连通性；保存状态 | 执行 SDK/平台 probe，报告稳定状态和错误码 |

`ChannelHostAdapter` 是 Core 中唯一把协议 DTO 映射到现有 Channel contract 的边界。
Runner 不依赖 `Workspace` 类型自行推断媒体目录；Core 传入的 `media_work_dir` 是
已经绝对化的最终目录。普通附件仍传定位符，不传文件内容或文件句柄。

## 3. BaseChannel 接口盘点和迁移映射

以下矩阵来自当前 `BaseChannel` 的 AST 直接定义，共 77 个方法。每个方法只出现一次。
`Core` 表示隔离后由 Host/Proxy 保留，`Runner` 表示平台驱动实现，`Split` 表示
Core 语义和 Runner 平台操作各保留一半，`Compatibility` 表示只为现有 in-process
调用面保留的兼容入口。

| method | declaration | owner | isolated mapping | notes |
| --- | --- | --- | --- | --- |
| `doctor_connectivity_notes` | sync/public | Runner | `ChannelDriver` probe result | 平台连通性探测 |
| `__init__` | sync/protected | Core | Proxy host state | 初始化渲染、ACL 和队列上下文 |
| `_is_native_payload` | sync/protected | Core | Host helper | 识别平台无关 payload |
| `get_debounce_key` | sync/public | Core | Host helper | session-scoped debounce key |
| `merge_native_items` | sync/public | Core | Host merge | 保留同一 ACL 身份 |
| `merge_requests` | sync/public | Core | Host merge | AgentRequest 批处理 |
| `_on_debounce_buffer_append` | sync/protected | Core | Host helper | debounce 缓冲 |
| `_content_has_text` | sync/protected | Core | Host helper | 内容语义判断 |
| `_content_has_audio` | sync/protected | Core | Host helper | 内容语义判断 |
| `_apply_no_text_debounce` | sync/protected | Core | Host helper | 无文本附件合并 |
| `_acl_msg` | sync/protected | Core | Host helper | ACL 文案 |
| `access_control_enabled` | sync/public | Core | Host property | ACL 开关 |
| `_access_control_gate` | async/protected | Core | Host gate | ACL 判定和 pending approval |
| `_check_group_mention` | sync/protected | Core | Host helper | 群聊 mention 策略 |
| `_get_acl_store` | sync/protected | Core | Host store | ACL store 访问 |
| `set_enqueue` | sync/public | Core | Host adapter | manager queue callback |
| `set_workspace` | sync/public | Core | Host adapter | 绑定 Core Workspace context |
| `_extract_chat_name` | sync/protected | Core | Host helper | Core 展示字段 |
| `_consume_with_tracker` | async/protected | Core | Host scheduler | TaskTracker 路由 |
| `_resolve_stream_type` | sync/protected | Core | Host helper | Event 类型归一化 |
| `_dispatch_streaming_event` | async/protected | Core | Host dispatcher | streaming Event 分发 |
| `_on_stream_msg_start` | async/protected | Core | Host hook | streaming 状态 |
| `_on_stream_content_delta` | async/protected | Core | Host hook | Content delta 聚合 |
| `_on_stream_msg_end` | async/protected | Core | Host hook | streaming 状态收束 |
| `_extract_text_from_event` | sync/protected | Core | Host helper | Event 文本抽取 |
| `_stream_with_tracker` | async/protected | Core | Host scheduler | streaming 与 tracker |
| `_sanitize_surrogate_text` | sync/protected | Core | Host helper | JSON 安全文本 |
| `_sanitize_for_json` | sync/protected | Core | Host helper | Event 序列化 |
| `_strip_event_headlines` | sync/protected | Core | Host helper | 展示语义 |
| `_serialize_event_for_sse` | sync/protected | Core | Host serializer | Core SSE 兼容 |
| `_flush_headline_stream_states` | sync/protected | Core | Host helper | streaming 状态清理 |
| `from_env` | sync/public | Core | Host factory | 无 workspace 的兼容入口 |
| `from_config` | sync/public | Core | Host factory | config 与 workspace 解析 |
| `resolve_session_id` | sync/public | Core | Host helper | session 规则 |
| `build_agent_request_from_user_content` | sync/public | Core | Host factory | 构造 AgentRequest |
| `build_agent_request_from_native` | sync/public | Core | Host factory | DTO 到 AgentRequest |
| `_payload_to_request` | sync/protected | Core | Host mapper | payload 映射 |
| `get_to_handle_from_request` | sync/public | Core | Host helper | 出站目标解析 |
| `get_on_reply_sent_args` | sync/public | Core | Host helper | delivery 回调字段 |
| `refresh_webhook_or_token` | async/public | Runner | `ChannelDriver` refresh | 平台 token/webhook |
| `consume_one` | async/public | Split | `event.batch` + Host dispatch | 入站 contract 兼容入口 |
| `_extract_query_from_payload` | sync/protected | Core | Host helper | 用户查询抽取 |
| `_debounce_payload` | sync/protected | Core | Host helper | debounce 策略 |
| `_consume_one_request` | async/protected | Core | Host consumer | ACL、Workspace、Agent 调度 |
| `_run_process_loop` | async/protected | Core | Host consumer | Agent process loop |
| `_get_response_error_message` | sync/protected | Core | Host helper | Agent 错误语义 |
| `_before_consume_process` | async/protected | Core | Host hook | Agent 调度前 hook |
| `on_event_content` | async/public | Core | Host hook | Event 内容消费 |
| `_get_stream_flush_meta` | sync/protected | Core | Host helper | streaming meta |
| `_safe_streaming_delta` | async/protected | Core | Host helper | streaming 安全处理 |
| `on_streaming_start` | async/public | Core | Host hook | streaming 公开 hook |
| `on_streaming_delta` | async/public | Core | Host hook | streaming 公开 hook |
| `on_streaming_end` | async/public | Core | Host hook | streaming 公开 hook |
| `on_event_message_completed` | async/public | Core | Host hook | completed message |
| `on_event_response` | async/public | Core | Host hook | Agent response |
| `_on_process_completed` | async/protected | Core | Host hook | process 完成 |
| `_finish_response_cycle` | async/protected | Core | Host helper | response cycle |
| `_clear_session_turn_usage` | sync/protected | Core | Host state | usage 清理 |
| `_commit_turn_usage` | async/protected | Core | Host state | usage 提交 |
| `_on_turn_usage_ready` | sync/protected | Core | Host hook | usage 回调 |
| `_on_consume_error` | async/protected | Core | Host error path | 错误归一化 |
| `send_response` | async/public | Split | `channel.send` + Host ledger | 响应语义与平台发送 |
| `_message_to_content_parts` | sync/protected | Core | Host mapper | 平台无关 ContentParts |
| `send_message_content` | async/public | Split | `channel.send` + Host ledger | 文本/卡片语义与发送 |
| `_truncate_stream_tool_chunk` | sync/protected | Core | Host helper | tool 输出裁剪 |
| `_format_stream_tool_output_body` | sync/protected | Core | Host helper | tool 输出展示 |
| `send_content_parts` | async/public | Split | `channel.send` + Host ledger | ContentParts 到平台操作 |
| `send_media` | async/public | Split | `channel.send` + Host ledger | 媒体定位符发送 |
| `_response_to_text` | sync/protected | Core | Host fallback | fallback 文本 |
| `clone` | sync/public | Compatibility | legacy BaseChannel | in-process 实例复制 |
| `health_check` | async/public | Split | `channel.health` + Host state | Runner probe 与 Core 状态 |
| `start` | async/public | Split | Proxy lifecycle + Driver start | 生命周期兼容 |
| `stop` | async/public | Split | Proxy lifecycle + Driver stop | 生命周期兼容 |
| `send` | async/public | Split | `channel.send` | 主动发送 contract |
| `to_handle_from_target` | sync/public | Core | Host helper | 目标句柄归一化 |
| `send_event` | async/public | Split | `channel.send` | Event/平台发送桥接 |
| `send_approval_notification` | async/public | Split | approval DTO + Driver render | Core 决策，Runner 呈现 |

`uses_manager_queue` 和 `streaming_enabled` 是类级调度能力，不是方法；它们在 descriptor
中分别映射为 `dispatch_mode` 和 streaming capability。当前只有 Voice、SIP 设置
`uses_manager_queue=False`，所以 direct session 继续绕过 manager queue、ACL gate 和
TaskTracker；其它 16 个 registry key 使用 manager queue 的现有路径。

## 4. 三个语义接口

本节只冻结调用面，不新增实现类。所有 Runner 参数和返回值都是版本化 JSON DTO；
`Path`、Pydantic 配置实例、`Workspace`、`AgentRequest`、`AgentResponse`、`Event`、
Future、loop、socket 和平台 SDK 对象不得出现在 Runner 方法签名或协议 payload 中。

### ChannelHostAdapter（Core）

| operation | input | output | owner |
| --- | --- | --- | --- |
| `prepare` | config snapshot + host context JSON | prepare result JSON | Core |
| `handle_event_batch` | inbound event DTO array | durable ACK DTO array | Core |
| `build_request` | event DTO + Core session context | AgentRequest (Core-only) | Core |
| `consume_event` | AgentRequest (Core-only) | Agent Event processing | Core |
| `send_output` | platform-independent ContentParts + target | delivery result DTO | Core/Runner |
| `approval` | approval state + decision | platform-independent result | Core |
| `state` | instance key + JSON value | JSON value | Core |

### ChannelDriver（Runner）

| operation | wire input | wire output | owner |
| --- | --- | --- | --- |
| `prepare` | config snapshot JSON, host context JSON | result JSON | Runner |
| `activate` | generation + lease JSON | status JSON | Runner |
| `commit` | generation + lease JSON | status JSON | Runner |
| `event_batch` | inbound event DTO array | ACK DTO array | Runner/Core |
| `send` | outbound operation DTO | delivery result DTO | Runner |
| `health` | health request JSON | health result JSON | Runner |
| `checkpoint` | checkpoint command JSON | checkpoint DTO JSON | Runner |
| `stop` | stop command JSON | stopped status JSON | Runner |

The `ChannelDriver` may use platform-native Python objects internally, but none cross the
boundary. Its wire input/output consists only of strings, numbers, booleans, null, arrays and
objects validated by the protocol schema.

### IsolatedChannelProxy（Core）

| operation | proxy behavior |
| --- | --- |
| manager queue methods | expose the `BaseChannel`-compatible Core contract |
| inbound | map `event.batch` DTOs to Core `consume_one` semantics |
| outbound | map ContentParts and target to `channel.send`, then update ledger |
| lifecycle | map `start`/`stop`/`health_check` to Runner lifecycle RPCs |
| ACL and approval | execute entirely in Core; send only platform-neutral decision/render DTOs |

The proxy never exposes a raw `Workspace`, `Event`, socket, Future or SDK object to the Runner.

## 5. Python 对象和 DTO 跨边界映射

| Core concept | wire representation | rule |
| --- | --- | --- |
| `Path` | platform-native absolute string | Core resolves relative paths; Runner treats it as a locator |
| Pydantic/config section | schema-validated JSON snapshot | no live model, callback or secret value |
| `Workspace` | host context JSON + `host.state.*` instance state | Runner cannot infer workspace from cwd |
| `AgentRequest` | Core-only object; selected scalar DTO fields | never sent as a Python object |
| `Event` | Core-only object; event DTO for protocol | Runner sends stable DTO, Core builds Event |
| `reply_future` / `reply_loop` / `incoming_message` | omitted | current merge meta keys are dead code |
| `conversation_id` / `message_id` | JSON scalar in controlled meta | preserve only when used by a Channel |
| DingTalk `session_webhook` fields | JSON string/number | real usage is serializable and remains allowed |

ACL mapping is explicit: each Runner event carries `acl_sender_id` for the real sender;
Core maps `sender_name` to the existing `meta["user_name"]` compatibility key. Core performs
debounce and merge only within one ACL identity and never combines two senders in one merged
payload, even when a group shares a session.

## 6. `media_work_dir` and inbound media modes

The effective directory is frozen by ADR-034:

```text
from_config: config.media_dir -> workspace_dir / "media" -> WORKING_DIR / "media"
from_env:    <CHANNEL>_MEDIA_DIR -> WORKING_DIR / "media"
```

Core performs this resolution, converts relative values using the Agent workspace rule, creates
the directory when required, and passes the resulting absolute path as `host_context.media_work_dir`.
The final directory is flat: Runner and Channel code must not append a Channel subdirectory.
The directory is for inbound downloads only; it is not an outbound path allowlist. Existing
download, naming, overwrite and cleanup behavior remains unchanged. This task records the rule
only; the parser belongs to `CH-2-004`.

| channel_key | inbound_media_mode | media_work_dir contract | current gap to record |
| --- | --- | --- | --- |
| `imessage` | not in contract | no inbound directory requirement | directory is upload/staging behavior |
| `discord` |落盘 | effective directory | none |
| `dingtalk` |落盘 | effective directory | none |
| `feishu` |落盘 | effective directory | none |
| `qq` |落盘 | effective directory | config field and env passthrough incomplete |
| `telegram` |落盘 | effective directory | config field and env passthrough incomplete |
| `mattermost` |落盘 | effective directory | none |
| `mqtt` |不提供 | not applicable | no inbound media directory |
| `console` | not in contract | no inbound directory requirement | directory is console-specific |
| `matrix` |落盘 | effective directory | config/constructor/env chain incomplete |
| `slack` |落盘 | effective directory | `SLACK_MEDIA_DIR` env passthrough incomplete |
| `voice` |不提供 | not applicable | text ingress; no v1 media pipe |
| `sip` |不提供 | not applicable | no inbound media directory |
| `wecom` |落盘 | effective directory | none |
| `xiaoyi` |落盘 | effective directory | config field incomplete; env exists |
| `yuanbao` |落盘 | effective directory | `YUANBAO_MEDIA_DIR` env passthrough incomplete |
| `wechat` |落盘 | effective directory | none |
| `onebot` |定位符直传 | no forced directory | platform locator formats stay in Runner |

## 7. Dispatch and ACL invariants

| channel_key | dispatch_mode | queue/ACL behavior |
| --- | --- | --- |
| `imessage` | `manager_queue` | manager queue and Core ACL path |
| `discord` | `manager_queue` | manager queue and Core ACL path |
| `dingtalk` | `manager_queue` | manager queue and Core ACL path |
| `feishu` | `manager_queue` | manager queue and Core ACL path |
| `qq` | `manager_queue` | manager queue and Core ACL path |
| `telegram` | `manager_queue` | manager queue and Core ACL path |
| `mattermost` | `manager_queue` | manager queue and Core ACL path |
| `mqtt` | `manager_queue` | manager queue and Core ACL path |
| `console` | `manager_queue` | manager queue and Core ACL path |
| `matrix` | `manager_queue` | manager queue and Core ACL path |
| `slack` | `manager_queue` | manager queue and Core ACL path |
| `voice` | `direct_session` | preserves bypass of ACL gate and TaskTracker |
| `sip` | `direct_session` | preserves bypass of ACL gate and TaskTracker |
| `wecom` | `manager_queue` | manager queue and Core ACL path |
| `xiaoyi` | `manager_queue` | manager queue and Core ACL path |
| `yuanbao` | `manager_queue` | manager queue and Core ACL path |
| `wechat` | `manager_queue` | manager queue and Core ACL path |
| `onebot` | `manager_queue` | manager queue and Core ACL path |

The dispatch value is descriptor data, not a new Channel-specific branch. `ChannelManager`
currently injects an enqueue callback only for manager-queue channels. The direct-session
behavior is recorded for compatibility and is not changed here.

## 8. Core / Runner / Plugin compatibility matrix

| source_kind | process_mode | Core representation | driver contract | compatibility promise |
| --- | --- | --- | --- | --- |
| `builtin` | `in_process` | concrete `BaseChannel` | BaseChannel | current Core behavior |
| `builtin` | `runner_process` | `IsolatedChannelProxy` | `ChannelDriver` | same manager-facing contract |
| `plugin` | `in_process` | legacy concrete `BaseChannel` | BaseChannel | existing `register_channel` behavior |
| `plugin` | `runner_process` | `IsolatedChannelProxy` | `ChannelDriver` | descriptor/SDK migration path |

The legacy reference is `plugins/channel/azure_bot`: `PluginAPI.register_channel` accepts a
concrete `BaseChannel` subclass, the registry and `ChannelManager.from_config` use the existing
factory contract, and no automatic legacy-to-runner conversion is performed. Console remains
an in-process builtin. Voice/SIP keep their direct-session compatibility path until a later
migration task changes it with separate evidence.

## 9. Follow-up ownership and exclusions

This document does not implement `ChannelHostAdapter`, `ChannelDriver`, `IsolatedChannelProxy`,
stdio framing, JSON-RPC, process/environment management, checkpoint persistence, a media parser,
Runner-owned Voice ingress, or any individual Channel migration. It records the configuration
gaps for QQ, Telegram, Matrix, XiaoYi, Slack and Yuanbao for later tasks; it does not repair them.
The three dead merge-meta keys are recorded but not deleted. ADR-032 is not used.

