# 通知投递生命周期可观测性与死信重驱 - 产品需求文档

## Overview

- **Summary**: 为每个「事件 × destination」建立可观测、可持久化的投递生命周期（pending → delivered / dead-letter），记录尝试次数、错误类别与脱敏错误摘要、最近/下次尝试时间；对可重试故障执行跨重启的持久指数退避；对确定失败或达到上限的记录转入死信并停止自动发送；提供分页筛选的管理 API 与页面，并支持修复 channel 后的人工重驱（幂等、不复制事件）。
- **Purpose**: 现状下长期故障的 webhook/聊天端点只会让通知无限滞留在 pending 中，运维既看不到失败原因与重试节奏，修好后也无法定向确认或重发；pending 积压还会按容量上限淘汰更早的事件。需要让每一条投递的状态可见、失败可分类、终态可沉淀、修复可重驱。
- **Target Users**: 配置并运维通知 channel（Slack/Teams/Telegram/Webhook/Email）与事故渠道的 Dagu 运维人员、开发者。

## Goals

- 每条事件到每个 destination 的投递都有独立生命周期记录：自动尝试次数、最近一次错误摘要（不含凭据）、错误类别、最近尝试时间、下次尝试时间、进入终态的时间与原因。
- 429、5xx、网络故障按持久化指数退避重试，退避状态写入 monitor state，进程重启后继续遵守；支持 429 的 `Retry-After` 提示。
- 确定性 4xx 与本地配置类错误立即进入 dead-letter；可重试错误达到可配置尝试上限（默认 10 次）后进入 dead-letter；死信记录停止一切自动发送。
- delivered 与 dead-letter 等终态记录不再占用活动 pending 容量上限；一个 destination 的故障、积压或死信不阻塞其他 destination 的投递。
- 通知管理 API 与页面支持按 pending / delivered / dead-letter 分页筛选，查看失败详情；修复 channel 后可人工重驱选定记录。
- 重驱幂等：并发或重复请求最多产生一个新的 pending 代际；已送达、已重驱（pending 中）、不存在的记录返回稳定结果；不复制事件。
- 投递成功/失败与状态保存继续服从现有 monitor lease 与「先持久化后生效」事务；保存失败不得错误确认或丢失待处理项。
- 旧版（v1）monitor state 无损读取；事件游标、批处理窗口、路由与去重、channel test、凭据隐藏、健康 destination 的投递行为保持兼容。
- 通知 monitor 与事故 monitor 共用 chatbridge 引擎，两者均获得完整生命周期能力，并各自提供管理 API 与页面入口。

## Non-Goals

- 不改变事件采集、`SourceCursor` 语义与 at-least-once 投递模型；不实现 exactly-once。
- 不改造批处理窗口（15s/2min；事故 1s）与批次渲染格式、不改变通知内容模板。
- 不引入外部数据库、消息队列或新的控制面服务；状态仍为单个 JSON 文件原子写。
- 不提供跨记录的批量编辑、终态记录的删除 API（TTL/容量自动清理）、邮件 SMTP 之外的新 provider。
- 不改变 transport 内部的即时重试（3 次、250ms–2s）语义；尝试次数以 monitor 批次级投递尝试为准。
- 不解决通用 webhook body-template 多事件「部分已发送」的历史重复投递行为（维持现状）。
- 分进程部署（独立 `dagu scheduler` 进程持锁、API 所在 server 进程 standby）下的跨进程重驱指令通道；该场景列表可读、重驱返回明确的 503（见 Open Questions）。

## Background & Context

- monitor 主循环：`internal/service/chatbridge/monitor.go`，租约 `Lease`（dirlock fence token + 进程 flock），心跳 1s、僵死阈值 45s、standby 轮询 5s；所有状态变更与投递前经 `ensureNotificationLockOwnership` fence。
- 状态文件：`<DataDir>/notifications/monitor-state.json`（事故为 `incidents/monitor-state.json`），copy-on-write：`applyStateTransitionLocked` 先 clone、原子 Save 成功后才替换内存态。v1 结构：`notificationDestinationState{Pending map[key]NotificationEvent, Delivered map[key]time.Time}`，版本常量 `notificationMonitorStateVersion = 1`。
- 批处理：`NotificationBatcher` 按 `destination|class` 分桶，紧急 15s / 成功摘要 2min 窗口；投递失败时事件保留在 Pending，由下一个 PollInterval（默认 10s）`requeuePending` 重新入桶——这是现有的「持久重试层」，但没有退避、没有次数上限、没有失败信息。
- 投递：`NotificationTransport.FlushNotificationBatch(ctx, destination, batch, allowLLM) bool`；实现方为 `internal/service/notification` 与 `internal/service/incident`。HTTP 层 `deliverRequest` 已把网络错误、HTTP 429、5xx 包为 `temporaryDeliveryError`，其余为永久错误；另有 `withRetry` 做进程内即时重试。
- 容量：`trimPendingNotifications` 按 destination 以 `PendingLimit`（默认 1000）淘汰最旧 pending；`Delivered` 按 `SeenTTL`（默认 2h）由 `evictStaleDelivered` 清理。
- 装配：`start-all` 中 scheduler 与 frontend server 同进程各自创建 monitor 竞争同一把文件租约，仅一个实例 active；`dagu server` 独立运行时 frontend 自身的 monitor 持锁。管理 API 位于 `internal/service/frontend/api/v1`，当前不持有 monitor 引用。
- 管理面：OpenAPI 权威定义在 `api/v1/api.yaml`（`make api` 生成 Go，`cd ui && pnpm gen:api` 生成前端）；页面 `ui/src/pages/notifications/index.tsx`，导航 `ui/src/menu.tsx`；channel API 已做凭据隐藏。

## Functional Requirements

### FR-1 生命周期记录模型

- FR-1.1：以「destination + 事件 key」为唯一标识建立投递记录，状态为 `pending`、`delivered`、`dead_letter` 三态之一。
- FR-1.2：每条记录包含：事件快照（pending/dead_letter 期间保留完整 `NotificationEvent` 以便渲染与重驱；delivered 仅保留紧凑摘要）、自动尝试次数 `attempts`、代际 `generation`、最近错误类别、最近错误脱敏摘要、最近尝试时间、下次尝试时间、记录创建/更新时间；delivered 记录含送达时间；dead_letter 记录含死信时间与原因（`client_error` / `attempt_limit`）。
- FR-1.3：状态文件升级为版本 2；解码接受 v1/v2（含 version=0）。v1 的 Pending 无损迁移为 pending 记录（attempts=0、立即可投递），v1 的 Delivered 无损迁移为 delivered 记录（保留 key 与送达时间，事件摘要缺省）。首次保存后以 v2 结构写出。
- FR-1.4：`Delivered` TTL 去重、waiting legacy key 迁移、run 级替换与抑制（`shouldSuppressNotificationEvent`）等既有去重语义在新记录模型上保持等价。

### FR-2 尝试记录、错误分类与持久退避

- FR-2.1：transport 通过可选的「详细结果」接口返回投递结果：是否送达、是否可重试、错误类别（`rate_limited` / `server_error` / `network` / `client_error` / `configuration` / `unknown`）、脱敏摘要、可选 `Retry-After`。未实现该接口的 transport 保持 bool 语义：失败按可重试、类别 `unknown` 处理。
- FR-2.2：每次批次投递（一个 `FlushNotificationBatch*` 调用）对批次内每个仍处于 pending 的事件记为一次自动尝试：递增 `attempts`、写 `lastAttemptAt`、最近错误类别与摘要。
- FR-2.3：429（含 Teams 200 响应体 429）、5xx、408、网络 I/O 故障为可重试；下次尝试时间 = 最近尝试时间 + 持久指数退避（初始 30s、倍增、上限 1h、带抖动），并取 `Retry-After` 与退避计算值的较大者；`nextAttemptAt` 持久化，重启后继续生效。
- FR-2.4：确定性 4xx（408 以外的非重试 4xx）与本地配置类错误（URL 未配置、SSRF 拒绝、scheme 非法、模板渲染失败、不支持的 provider 等）不可重试，当次即转入 dead-letter，原因 `client_error`/`configuration`，并清空 `nextAttemptAt`。
- FR-2.5：可重试错误在 `attempts` 达到配置上限（默认 10）时转入 dead-letter，原因 `attempt_limit`，此后不再自动投递。
- FR-2.6：requeue 仅将 `nextAttemptAt` 为空或已到期的 pending 记录重新放入 batcher；未到期记录不产生投递。一个 destination 中各事件的退避独立；一个 destination 的持续失败、死信或超上限不影响其他 destination 的批次生成与投递。
- FR-2.7：错误摘要不得包含凭据：网络错误剥离 URL（仅保留主机名与操作，如 dial/i/o timeout）、HTTP 错误以状态码加截断响应体表示，并经正则脱敏（Telegram `bot<数字>:<密钥>`、Bearer/Basic、`token=`/userinfo 等）；摘要长度有上界。
- FR-2.8：transport 进程内即时重试（withRetry）维持现状；monitor 层不重复计数这些即时尝试。

### FR-3 终态有界与容量隔离

- FR-3.1：pending 容量上限（默认 1000/destination，沿用 `PendingLimit`）只统计 pending 记录；delivered 与 dead_letter 不占用该上限；超限时仍按最旧 pending 淘汰（维持现有告警行为）。
- FR-3.2：dead-letter 默认保留 14 天，且每个 destination 最多保留 1000 条，超限淘汰最旧；delivered 沿用 2h `SeenTTL`。清理随现有 eviction 周期执行并持久化。
- FR-3.3：destination 配置被删除时，其 pending 记录按现状丢弃并从 batcher 移除；终态记录（delivered/dead_letter）保留至 TTL/容量清理，以便事后查看。
- FR-3.4：退避参数（`maxAttempts`、`initialBackoff`、`maxBackoff`）与 dead-letter 保留参数（`deadLetterTTL`、`deadLetterLimit`）可在配置文件中调整，缺省值为 10 次 / 30s / 1h / 14d / 1000；非法或零值回退默认。

### FR-4 管理 API

- FR-4.1：新增通知投递记录列表接口，支持按状态集合（pending/delivered/dead-letter，可多选）、destination 精确过滤、分页（limit 默认 50、最大 200，不透明游标，按更新时间倒序加稳定 tie-break）；返回 destination、事件 key、DAG 名称、事件类型、运行状态、attempts、generation、错误类别/摘要、最近尝试、下次尝试、送达/死信时间与原因。
- FR-4.2：新增单条记录详情接口（以不透明 recordId 编码 destination+key），返回列表字段全集与完整错误摘要。
- FR-4.3：新增重驱接口，接受一组 recordId；对每条返回稳定结果：`redriven`（dead_letter → 新 pending 代际）、`already_pending`（已经在 pending，含已被重驱）、`delivered`（已送达）、`not_found`；响应包含代际号与当前状态；单次请求在一次状态事务内提交。
- FR-4.4：重驱语义：仅 dead_letter 可转 pending——attempts 清零、generation+1、清除错误/退避/死信字段、`nextAttemptAt` 置为立即、保留原事件快照并入 batcher；不修改 SourceCursor、不新建重复记录。
- FR-4.5：并发与重复重驱在状态锁内串行化：首个请求完成代际跃迁后，其余请求看到 pending 即返回 `already_pending`，不产生第二个代际、不入队两次；若重驱后已被自动投递成功，则返回 `delivered`。
- FR-4.6：读取接口直接从状态文件加载最新已提交状态（无需租约）；重驱必须由当前持有 monitor lease 的活动 monitor 实例执行（进程内通过活动实例注册表路由到 frontend 或 scheduler 的持锁 monitor）；本进程无持锁实例时返回 503 与明确错误码。
- FR-4.7：事故 monitor 提供同构的一组接口（`/incident-deliveries` + redrive），权限网关复用事故管理权限；通知接口复用 `requireNotificationManagement`（developer 及以上）。
- FR-4.8：所有新接口纳入 OpenAPI（`api/v1/api.yaml`），经 `make api` 与前端 `pnpm gen:api` 生成；不手改生成文件。

### FR-5 管理页面

- FR-5.1：通知模块新增「Delivery / 投递记录」导航入口与页面：状态筛选（pending/delivered/dead-letter tabs 或 chips，可分页）、destination 过滤、更新时间倒序分页表格。
- FR-5.2：页面展示 DAG、事件类型/状态、attempts、错误类别、脱敏错误摘要、最近尝试、下次尝试（pending）、送达/死信时间与原因；死信记录可展开查看失败详情。
- FR-5.3：dead-letter 记录支持勾选后「Redrive / 重驱」，调用重驱接口并按记录展示结果（已重驱/已在投递/已送达/不存在），成功后刷新列表；pending/delivered 记录不提供重驱按钮。
- FR-5.4：事故模块提供同构页面或明确的同页 scope 切换，覆盖事故投递记录。
- FR-5.5：页面文案沿用现有 `I18nText` 英文默认值模式；类型检查、单测与构建通过。

### FR-6 租约、事务与兼容性

- FR-6.1：投递结果落库（尝试、退避、死信、送达确认）与重驱均在 lease fence 下、经「clone → Save → 替换内存态/动 batcher」的现有事务路径执行；Save 失败时内存态与 batcher 不变更：已远程成功但确认保存失败时不标记 delivered（维持 at-least-once），重驱保存失败时不入队、不产生代际。
- FR-6.2：锁丢失（心跳失败/standby 切换）后不进行任何状态写入或投递确认，行为与现状一致。
- FR-6.3：以下既有行为保持兼容并由测试守护：bootstrap 游标边界与 `Bootstrapped`、按事件路由、批处理窗口与 ready/drain 行为、关停 drain、`PendingLimit` 淘汰、waiting key 迁移、成功摘要抑制/opt-in policy、channel test、channel 凭据隐藏、健康 destination 的正常送达与去重、事故 monitor 既有窗口（1s）与事件集合。

## Non-Functional Requirements

- **NFR-1 可靠性**：所有生命周期状态变更原子落盘，崩溃/重启后 pending、退避时刻、尝试次数、终态不丢失、不回退；状态文件损坏仍走既有 quarantine 恢复路径。
- **NFR-2 隔离性**：单 destination 故障不阻塞其他 destination；单个坏批次不影响轮询、游标推进与其他批次。
- **NFR-3 性能**：列表查询为单次状态文件加载后的内存分页，默认页大小下响应与现状同量级；记录模型与清理策略保证状态文件有界增长。
- **NFR-4 安全**：错误摘要在任何路径（含网络错误、HTTP 响应体、事故 provider）都不得泄露 token、密码、webhook URL 路径密钥、HMAC 密钥。
- **NFR-5 可测试性**：退避时刻、尝试计数、分类、死信、迁移、重驱幂等均有 Go 单元/集成测试；前端有组件测试；优先扩展 `chatbridge`、`notification`、`incident` 既有测试文件。
- **NFR-6 可维护性**：复用现有 `internal/cmn/backoff`、状态事务与 OpenAPI/UI 生成链路；新增抽象最小化，跨包接口为可选接口，不破坏 `NotificationTransport` 既有签名。

## Constraints

- **Technical**: Go 单模块 + React/TypeScript UI；状态文件 JSON 原子写（.tmp + fsync + replace，0600）；Windows 兼容（`make check` 含 Windows 路径检查）；不手改 `api.gen.go` 与 `ui/src/api/v1` 生成代码。
- **Business**: 遵守既有权限模型（通知 developer+、事故管理网关与许可）；审计日志覆盖重驱操作。
- **Dependencies**: OpenAPI codegen 与 pnpm 工具链可用；`internal/cmn/backoff`、dirlock/flock 租约机制复用。

## Assumptions

- 标准部署为 `dagu start-all`（同进程 frontend+scheduler，注册表可把重驱路由给持锁 monitor）或 `dagu server`（frontend monitor 自持锁）；两种部署下功能完整。
- 批次是投递与确认的最小粒度：批次失败对批次内全部 pending 事件记一次尝试；现有渲染与抑制规则不变。
- 运维主要通过 UI/API 在修复 channel 后重驱；不提供自动重驱死信的定时扫描。
- 邮件（SMTP）错误按同一分类处理：连接类故障可重试，配置/认证类确定性失败入死信。

## Acceptance Criteria

### AC-1: v1 状态无损迁移
- **Type**: `rule`
- **Given**: 一个由现网版本写出的 v1 `monitor-state.json`（含 Pending 事件与 Delivered 时间戳）
- **When**: 新 monitor 加载该文件
- **Then**: Pending 事件成为 attempts=0、无 nextAttemptAt（立即可投递）的 pending 记录；Delivered key 成为保留原送达时间的 delivered 记录；游标、Bootstrapped、destination 集合不变；首次保存后文件为 v2 且信息不丢失
- **Pass Condition**: 用真实 v1 JSON fixture 解码测试通过（字段逐一断言），且加载后投递/去重行为与旧版一致
- **Evidence**: `chatbridge` 包状态迁移单元测试（如 `monitor_state_test.go`）

### AC-2: 投递尝试与错误信息持久化
- **Type**: `rule`
- **Given**: transport 返回可重试失败并携带类别与摘要
- **When**: monitor 完成一次批次投递
- **Then**: 批次内每个 pending 记录的 attempts 递增 1，写入 last_error_category、不含凭据的 last_error_summary、last_attempt_at 与未来的 next_attempt_at，且这些字段出现在落盘的状态文件中
- **Pass Condition**: 构造可重试失败的测试：内存记录与状态文件 JSON 中上述字段均正确
- **Evidence**: `chatbridge` monitor 投递结果落库测试

### AC-3: 持久退避跨重启继续
- **Type**: `rule`
- **Given**: 某 destination 的事件因 5xx/429/网络故障失败并产生 nextAttemptAt
- **When**: monitor 进程重启（或 session 重建）后在 nextAttemptAt 之前/之后分别到达 poll 周期
- **Then**: 到期前不入 batcher、不发生投递；到期后才重新投递；退避序列为 30s 起指数倍增、1h 封顶并含抖动；429 带 Retry-After 时下次尝试不早于其指示
- **Pass Condition**: 注入时钟/短参数测试断言退避序列、重启后等待行为与 Retry-After 下限
- **Evidence**: `chatbridge` backoff/requeue 测试

### AC-4: 确定失败与尝试上限进入死信并停止自动发送
- **Type**: `rule`
- **Given**: (a) transport 返回 404/400/401 等确定 4xx 或配置类错误；(b) 可重试错误持续且 attempts 已达上限 10
- **When**: monitor 处理投递结果
- **Then**: (a) 当次记录立即转为 dead_letter（reason=client_error/configuration，attempts 为 1）；(b) 第 10 次可重试失败后转为 dead_letter（reason=attempt_limit）；此后 requeue 与 batcher 不再包含该记录，观察多个 poll 周期无自动投递
- **Pass Condition**: 两类路径分别测试；dead_letter 记录在后续 requeue 中零投递
- **Evidence**: `chatbridge` 死信转换测试

### AC-5: 终态不占 pending 容量且 destination 间相互隔离
- **Type**: `rule`
- **Given**: destination A 有大量 dead_letter/delivered 记录（>1000）与持续故障，destination B 健康
- **When**: 事件持续进入，A 达到 pending 上限并持续失败
- **Then**: 终态记录不被计入 1000 上限、不被 pending 淘汰逻辑删除；A 的 pending 按最旧淘汰；B 的事件按正常窗口持续送达，不被 A 阻塞或延迟出本批次调度
- **Pass Condition**: 双 destination 测试断言计数、淘汰集合与 B 的送达时序
- **Evidence**: `chatbridge` 容量/隔离测试

### AC-6: 终态记录有界清理
- **Type**: `rule`
- **Given**: 超过 14 天的 dead_letter 记录、每 destination 超过 1000 条死信、超过 2h 的 delivered 记录
- **When**: eviction 周期执行
- **Then**: 过期/超量终态记录被持久化删除（死信按最旧淘汰），pending 与未过期终态保留
- **Pass Condition**: 注入时间的清理测试通过
- **Evidence**: `chatbridge` 终态清理测试

### AC-7: 错误摘要脱敏
- **Type**: `rule`
- **Given**: 故障响应中包含 Telegram bot token URL、Bearer/Basic 头痕迹、`?token=`、userinfo 或响应体回显密钥
- **When**: transport 生成失败结果、monitor 落库、API 返回详情
- **Then**: 任何输出中都不存在原始密钥片段；HTTP 错误包含状态码与截断响应体，网络错误只含主机/操作级信息
- **Pass Condition**: 以已知密钥字符串做子串断言（not contains）的单测通过，覆盖 notification 与 incident 两个 transport
- **Evidence**: `internal/service/notification`、`internal/service/incident` 脱敏测试

### AC-8: 管理列表分页与筛选
- **Type**: `rule`
- **Given**: 状态文件中存在三种状态、多个 destination 的记录
- **When**: 调用列表接口，分别按状态、destination、分页游标组合查询
- **Then**: 仅返回匹配记录；按更新时间倒序+稳定 tie-break；游标可向后翻页且无重复/遗漏；limit 上限 200 被强制；字段包含 FR-4.1 全部内容
- **Pass Condition**: handler/服务层分页测试（含边界页、空结果、非法游标 400）
- **Evidence**: `frontend/api/v1` 投递记录接口测试

### AC-9: 重驱幂等与稳定结果
- **Type**: `rule`
- **Given**: dead_letter、pending、delivered、不存在四类 recordId
- **When**: 并发（含重复）调用重驱接口
- **Then**: 同一 dead_letter 记录无论多少并发/重复请求，最多产生一次 generation+1 与一次入队；分别返回 redriven/already_pending/delivered/not_found；不新增重复记录、不改 SourceCursor、不产生重复事件（DAG 运行 key 去重仍生效）
- **Pass Condition**: 并发重驱测试断言代际差 ≤1、入队次数=1、各类结果码正确
- **Evidence**: `chatbridge` 重驱服务测试 + API 层结果测试

### AC-10: 租约与保存失败安全
- **Type**: `rule`
- **Given**: (a) 状态 Save 注入失败；(b) monitor 未持有租约
- **When**: 发生投递成功/失败、重驱请求
- **Then**: Save 失败时记录保持 pending、不标记 delivered、不入队重驱、不产生代际（远程已发送仍会被后续重试，at-least-once）；无租约时不写状态，重驱经注册表无活动实例则返回 503 明确错误码
- **Pass Condition**: 故障注入测试断言内存/磁盘状态与 batcher 内容；锁状态测试断言拒绝写入
- **Evidence**: `chatbridge` 事务安全测试

### AC-11: 既有行为兼容
- **Type**: `rule`
- **Given**: 现有 chatbridge/notification/incident 测试与 conformance 资产
- **When**: 运行 `go test ./internal/service/chatbridge/... ./internal/service/notification/... ./internal/service/incident/...`、相关 frontend API 测试与 `make conformance`（若覆盖通知语义）
- **Then**: 既有用例不回归（允许因结构升级做必要的 fixture 同步）；游标、窗口、路由、去重、waiting 迁移、channel test、健康送达、关停 drain 行为保持
- **Pass Condition**: 上述命令全部通过
- **Evidence**: 测试命令输出

### AC-12: 配置可调
- **Type**: `rule`
- **Given**: 配置文件中设置 maxAttempts/initialBackoff/maxBackoff/deadLetterTTL/deadLetterLimit
- **When**: 服务启动装配 monitor
- **Then**: 运行参数取配置值；缺省/非法值回退默认（10、30s、1h、14d、1000）；通知与事故 monitor 均生效
- **Pass Condition**: 配置解析/装配测试
- **Evidence**: `cmn/config` 与装配层测试

### AC-13: 通知与事故双覆盖
- **Type**: `rule`
- **Given**: 通知与事故两条链路
- **When**: 分别制造投递失败、死信与修复后重驱
- **Then**: 两组 API 路径与两个页面入口均能查看记录、看到脱敏详情并完成重驱；权限分别走通知与事故管理网关
- **Pass Condition**: 后端两组端点测试通过；UI 两个入口可达
- **Evidence**: API 测试 + UI 路由/组件测试

### AC-14: 投递记录页面可用性
- **Type**: `rubric`
- **Dimension**: 运维人员发现故障、理解失败、完成修复重驱的端到端体验
- **Scale**: 1-5
- **Anchors**: 1 = 无法筛选或看不出失败原因；3 = 能分页筛选并看到错误，但重驱/详情操作别扭、状态反馈不完整；5 = 筛选/分页/详情/批量重驱/结果反馈闭环顺畅，状态与时间信息一目了然
- **Pass Threshold**: >= 4
- **Evidence**: `ui/src/pages` 新页面实现与组件测试，浏览器手工走查

### AC-15: 实现与既有架构的契合度
- **Type**: `rubric`
- **Dimension**: 复用现有状态事务/租约/退避/代码生成链路、最小侵入的程度
- **Scale**: 1-5
- **Anchors**: 1 = 新引入并行状态体系或破坏 transport 接口；3 = 功能可用但存在重复抽象或绕过事务；5 = 完全在现有事务/租约/生成链路内扩展，可选接口向后兼容，改动集中
- **Pass Threshold**: >= 4
- **Evidence**: 代码审查（`chatbridge`/transport/API/UI diff）

## Open Questions

- [ ] 分进程部署（独立 scheduler 持锁、server 的 monitor standby）下重驱返回 503 是否可接受，还是需要额外的「重驱请求文件 + 持锁进程轮询执行」跨进程通道？（默认按 503 实现；列表读取不受影响）
- [ ] 408 Request Timeout 归入可重试 `network` 类（超时语义）是否认可？
- [ ] 死信/投递记录是否需要审计日志与额外的 Webhook 事件外发？（默认仅重驱操作写审计日志）
