# 多租户节点化 Agent 平台技术设计方案

## 1. 背景和目标

本方案基于 tRPC-Agent-Go 设计一个面向企业的 Agent 平台。平台支持多个租户创建和发布自己的 Agent，并通过 IM 软件、Web 或 API 等入口为用户提供服务。

平台需要解决以下问题：

- 不同租户的 Agent、模型、工具、知识库、会话和密钥如何隔离；
- 多个 Agent Worker 如何水平扩容，并保证同一会话不会乱序；
- Session、Memory、Knowledge、文件和审计数据如何选择不同后端；
- IM 消息如何进入 Agent，并将结果安全地回复给用户；
- 如何限制危险 Tool、记录成本和审计行为；
- Worker、IM、存储、模型或 Tool 出现故障时如何避免重复执行并恢复服务。

本方案以设计为主，不绑定具体 IM SDK、数据库产品、监控产品或密钥管理产品。第一版优先完成统一架构、核心数据模型和关键恢复边界，具体实现参数在开发和压测阶段确定。

## 2. 图示

### 2.1 系统架构图

系统架构图见：

```text
系统架构图.html
系统架构图.png
```

平台主链路如下：

```text
IM 平台
→ Channel Adapter
→ Agent Gateway
→ Session Scheduler
→ Agent Worker / Runtime
→ LLM / Tool / MCP
→ Storage Adapter / Router
→ 共享存储
→ IM 平台回复
```

### 2.2 核心时序图

核心时序图见：

```text
核心时序图.html
```

主要过程如下：

```text
IM 用户发消息
→ Channel Adapter 验签和标准化
→ Gateway 做幂等、识别租户和 Agent
→ Scheduler 保证 Session 顺序
→ Worker 执行 Agent 和 Tool
→ 写入 Session / Memory
→ Channel Adapter 回复 IM 用户
```

## 3. 总体设计原则

### 3.1 Worker 无状态

Worker 可以部署多个副本，也可以随时扩缩容。Worker 本地只缓存 Runtime，不保存唯一业务状态。

Session、Memory、Event、Summary 等数据保存在共享后端。某个 Worker 故障后，其他 Worker 可以重新加载 Runtime，并从共享存储恢复上下文。

### 3.2 所有请求携带租户上下文

Gateway 根据可信的 IM 通道绑定或身份信息确定：

```text
tenant_id
agent_app_id
agent_version
channel_binding_id
user_id
session_id
request_id
trace_id
```

这些信息在 Gateway、Scheduler、Worker、Tool、Storage 和审计链路中持续传递，用于租户隔离、权限判断、数据路由和问题排查。

### 3.3 配置版本化

Agent 的 Prompt、模型、Tool、知识库、治理策略等配置以 Agent Version 的形式保存。已发布版本不直接修改，配置变化时创建新版本。

Agent Deployment 决定生产环境当前使用哪个版本，并支持权重灰度和快速回滚。

## 4. 多租户与节点部署

### 4.1 核心资源模型

平台的核心关系如下：

```text
Tenant
└── Agent App
    ├── Agent Version
    ├── Agent Deployment
    ├── Model Config
    ├── Tool Definition / Permission
    ├── Knowledge Base
    ├── Channel Binding
    ├── Backend Binding
    ├── Audit Policy
    └── Secret Reference
```

- **Tenant**：企业、部门或业务团队，是资源隔离边界；
- **Agent App**：客服、运维、数据分析等独立 Agent；
- **Agent Version**：某一时刻的完整运行配置；
- **Agent Deployment**：某个环境当前发布的版本和灰度权重；
- **Channel Binding**：外部 IM 账号与 Tenant、Agent 的绑定；
- **Backend Binding**：某类数据使用哪个后端；
- **Secret Reference**：密钥引用，不保存密钥明文。

### 4.2 Gateway、Scheduler 和 Worker

各组件职责如下：

| 组件 | 主要职责 |
| --- | --- |
| Channel Adapter | 接收 IM 消息、验签、解析、身份映射、回复消息 |
| Agent Gateway | 入站幂等、Tenant 和 Agent 识别、Session 定位、创建请求上下文 |
| Session Scheduler | 保证同一 Session 顺序执行，并分派健康 Worker |
| Agent Worker | 加载 Runtime，执行 Runner、模型、Tool 和存储读写 |
| Runtime Registry | Worker 本地 Runtime 缓存，可在丢失后重新构建 |

Session Scheduler 不是独立部署的服务，而是 Gateway 和 Worker 共用的调度库。它由两部分组成，状态都保存在 Redis：

```text
Session 租约：同一时刻只有一个 Worker 能处理某个 Session，带 TTL 自动过期
Session 信箱：该 Session 待处理的消息，按到达顺序排列
```

执行过程如下：

```text
Gateway 写入信箱 → 通过消息队列通知
→ Worker 抢占租约，抢不到则不处理
→ 持有租约期间排空信箱，并持续续期
→ 信箱为空后释放租约
```

租约按轮持有，范围是一次排空信箱的时间，不是整个会话生命周期。同一个 Session 的下一轮消息由任意健康 Worker 处理，因此 Session 不绑定 Worker，也不需要 sticky session。不同 Session 之间并行执行。

消息队列只承担“该 Session 有待处理消息”的分发提示，不要求有序，顺序性由租约和信箱共同保证。

### 4.3 租户隔离

隔离范围包括：

```text
配置隔离：不同 Tenant 只能读取自己的 Agent、模型、Tool 和通道配置；
数据隔离：Session、Memory、Knowledge、Artifact、Audit 都携带 tenant_id；
工具隔离：Tool 调用时检查 Tenant、Agent、用户和环境权限；
日志隔离：日志和审计包含 Tenant 标识，并进行脱敏；
密钥隔离：不同 Tenant 的 Secret Reference 和访问权限分开管理。
```

## 5. 数据同步与多后端支持

### 5.1 Storage Adapter / Router

业务代码不直接依赖某一种数据库。Worker 通过 Storage Adapter / Router，根据以下信息选择后端：

```text
tenant_id + agent_app_id + data_type
```

例如：

```text
租户 A / 客服 Agent
├── Session、Summary → Redis 或 SQL
├── Memory → PostgreSQL
├── Knowledge → 向量数据库
├── Artifact → 对象存储
└── Audit Log → SQL 或日志分析存储
```

### 5.2 数据类型与后端

| 数据类型 | 主要内容 | 建议后端 |
| --- | --- | --- |
| Session / State | 会话状态、短期上下文 | Redis、MySQL、PostgreSQL 等 |
| Session Event | 用户消息、Agent 回复、Tool 调用 | SQL 或可靠 Session 后端 |
| Summary | 历史会话摘要 | 跟随 Session 后端 |
| Memory | 跨会话长期记忆 | Redis、SQL、外部 Memory 服务 |
| Knowledge | 文档切片、向量和检索索引 | Qdrant、Milvus、pgvector 等 |
| Artifact | 图片、文件、音视频 | S3、COS 等对象存储 |
| Audit / Usage | 审计、Token、成本 | SQL、ClickHouse 或日志系统 |

### 5.3 数据写入顺序

一次 Agent 执行完成后，主要按以下顺序处理：

```text
保存 Session Event
→ 更新 Session State
→ 按策略更新 Memory
→ 异步生成或更新 Summary
```

Summary 更新失败不影响当前回复，可后续重试。

### 5.4 一致性和迁移

- 同一个 Session 由 Scheduler 串行处理，避免多个 Worker 同时修改；
- Event 和 State 尽量使用事务或原子操作保持一致；
- Memory、Summary、Knowledge 允许有限的最终一致；
- 更换后端时采用“全量复制 → 增量同步 → 校验 → 切换绑定 → 保留回滚窗口”的方式迁移。

## 6. IM 软件接入

### 6.1 Channel Adapter

平台通过统一 Channel Adapter 接入企业微信、微信客服、Telegram 或其他 IM。

Channel Adapter 负责：

```text
接收 IM 回调或事件
→ 验签、解密和消息解析
→ 映射外部用户为内部用户
→ 转换为统一消息
→ 调用 Agent Gateway
→ 将 Agent 结果转换为 IM 回复
```

统一消息包含：

```text
channel
channel_binding_id
external_event_id / message_id
external_user_id
external_group_id
text
content_parts
request_id
```

### 6.2 通道、用户和 Session

Channel Binding 表示：

```text
IM 账号
→ Tenant
→ Agent App 或 Deployment
```

用户映射：

```text
channel_binding_id + external_user_id
→ internal_user_id
```

Session 规则：

```text
单聊：
channel_binding_id + external_user_id
→ session_id

群聊：
channel_binding_id + external_group_id
→ session_id
```

不同 Tenant、不同通道、不同群聊之间的 Session 相互隔离。

### 6.3 IM 幂等和回复

IM 平台重复投递同一事件时：

```text
Channel Adapter 提取 event_id / message_id
→ Gateway 在共享存储创建幂等记录
→ 重复事件不再次执行 Agent 或 Tool
```

如果 Agent 已完成但 IM 回复发送失败：

```text
只重试 IM 投递
不重新执行 Agent 或 Tool
```

第一版优先支持文本消息、单聊和群聊 Session、用户映射、入站幂等、长文本拆分和有限重试。图片、文件、卡片和流式模拟回复可后续按通道能力增加。

## 7. 治理、监控和安全

### 7.1 Plugin、Guardrail 和 Callback

平台使用三类扩展能力：

| 类型 | 作用 |
| --- | --- |
| Plugin | 提供上下文注入、格式化、成本统计等可插拔能力 |
| Guardrail | 在关键步骤做允许、拒绝、确认和脱敏等安全决策 |
| Callback | 记录 Metrics、Trace、Audit、Usage 和错误事件 |

Worker 构建 Runtime 时，根据 Tenant 和 Agent Version 的策略装配这些扩展。它们主要在模型调用前后、Tool 调用前后和 IM 回复前工作。

### 7.2 治理策略

主要策略包括：

```text
Tool 白名单：allow / deny / ask
危险 Tool 二次确认
IM 用户与角色权限校验
单次请求 Token 和成本限制
租户日/月预算限制
输入、Tool 结果和输出内容脱敏
模型和 Tool 的白名单
```

例如 Tool 调用前需要同时检查：

```text
当前 Tenant 是否允许
当前 Agent Version 是否绑定
当前 IM 用户是否有权限
当前环境是否允许
是否需要二次确认
```

### 7.3 Metrics、Trace 和审计

平台通过 OpenTelemetry 或等价能力埋点。

Metrics 用于看板和告警，重点包括：

```text
请求量和错误率
模型调用耗时、Token 和成本
Tool 调用耗时和失败率
IM 投递成功率
Session Scheduler 等待时间
Session / Memory 后端延迟
Guardrail 拒绝和二次确认次数
```

Trace 用于查看一次请求经过哪些步骤：

```text
IM callback
→ Gateway
→ 幂等处理
→ Session Scheduler
→ Worker / Runner
→ LLM
→ Tool
→ Session / Memory
→ IM 投递
```

审计日志重点记录：

```text
tenant_id
channel
user_id
session_id
agent_name
tool_name
decision
latency
error_type
cost
trace_id
```

审计用于回答“谁在什么时间，通过哪个通道，使用哪个 Agent 或 Tool，系统为什么允许或拒绝”。

### 7.4 密钥和脱敏

配置中只保存密钥引用：

```text
secret://prod/tenant-a/model/primary-api-key
secret://prod/tenant-a/channel/im-token
```

IM Token、模型 API Key、数据库密码和 Tool 凭据的明文保存在 Secret Manager 或等价密钥服务中。

Worker 运行时按 Tenant、用途和权限读取密钥，用于初始化模型、IM、数据库或 Tool Client。密钥不能进入：

```text
普通配置表
日志
Trace Attribute
Metric Label
错误报告
Prompt
Session / Memory
```

平台使用统一 Redactor 脱敏 Authorization、Token、API Key、Password、Cookie、数据库 DSN、个人信息和敏感 Tool 参数。

## 8. 故障恢复与运维

### 8.1 节点故障

Worker 故障时：

```text
Scheduler 不再分派新请求
→ Session 执行权在超时后释放
→ 健康 Worker 从共享后端读取 Session / Memory
→ 重新构建 Runtime 并处理后续消息
```

Gateway 和 Channel Adapter 应部署多个实例。实例故障后，负载均衡将后续请求发到健康实例；重复 IM 回调由幂等机制拦截。

### 8.2 存储、模型和 Tool 故障

| 场景 | 处理原则 |
| --- | --- |
| 幂等记录、Session State 不可用 | 状态不确定时不继续执行，避免重复 Agent 或 Tool |
| Memory 不可用 | 有限重试，必要时延迟写入或本次不使用长期 Memory |
| Summary 不可用 | 异步重试，不阻塞 IM 回复 |
| Knowledge 不可用 | 降级为无知识库回答或提示能力受限 |
| 模型超时 | 有限重试，可选备用模型，受总超时和预算限制 |
| 查询类 Tool 失败 | 有限重试 |
| 有副作用 Tool 失败 | 使用操作幂等键；结果不确定时不盲目重试 |
| IM 回复失败 | 只重试投递，不重跑 Agent 或 Tool |

### 8.3 Go 资源管理

每个请求都使用 `context.Context`，并向下传递到 Scheduler、Runner、模型、Tool、存储和 IM Client。

当请求超时、排队超时、Worker 停机或系统主动取消时，下游调用应尽快停止。

实现时需要保证：

```text
每个 goroutine 有明确退出条件；
后台重试和异步任务使用受控 Worker Pool 或任务队列；
Runner 的流式 Event 通道持续消费到关闭或取消；
Worker 停机时停止新请求、等待或取消旧请求、排空 Event 通道后再退出。
```

### 8.4 灰度发布和回滚

第一版支持基于 Deployment 权重的灰度发布：

```text
v1：90%
v2：10%
```

Gateway 在创建新 Session 时，根据稳定标识和版本权重选择 Agent Version，并将版本写入 Session。后续同一 Session 固定使用该版本。

发现问题时，通过调整 Deployment 权重回滚：

```text
v1：100%
v2：0%
```

Deployment 预留策略扩展字段，后续可支持按 Tenant、Channel、用户或标签灰度。

### 8.5 容量与部署

第一版完成后，通过压测和监控确定具体容量。重点关注：

```text
IM 入站峰值 QPS
Worker 并发执行数
请求平均和 P95 时延
模型 Token 和耗时
Tool 调用耗时
Redis / SQL QPS 与延迟
Scheduler 排队时间
IM 投递成功率
Worker CPU、内存和 goroutine 数
```

最小可运行部署可使用 Docker Compose：

```text
Gateway + Channel Adapter
单个 Worker
Redis
MySQL / PostgreSQL
可选向量数据库和对象存储
```

生产环境建议使用 Kubernetes：

```text
Gateway / Channel Adapter 多副本
Session Scheduler 多副本
Worker Deployment 和自动扩缩容
Redis、SQL 高可用
向量数据库、对象存储
Secret Manager
OpenTelemetry Collector
Metrics、Trace、日志和告警系统
```

## 9. 最小数据模型

第一版保留以下核心实体，字段可随实现扩展：

```text
tenants
agent_apps
agent_versions
agent_deployments
channel_bindings
backend_configs
backend_bindings
sessions
session_events
session_summaries
memories
tool_definitions
agent_tool_bindings
audit_logs
usage_records
```

运行数据通常至少包含：

```text
tenant_id
agent_app_id
session_id
request_id
agent_version
created_at
updated_at
```

Session Event 还需要顺序字段：

```text
sequence
```

用于保证会话消息和 Tool 事件的正确顺序。

## 10. 第一版范围和后续扩展

第一版重点实现：

1. Tenant、Agent App、Version、Deployment 和 Channel Binding；
2. Gateway、Session Scheduler、无状态 Worker；
3. 共享 Session / Memory 和多后端路由；
4. 抽象 IM Channel Adapter；
5. 入站幂等、出站投递重试；
6. Plugin / Guardrail / Callback 的基础治理能力；
7. Metrics、Trace、Audit、Usage 和统一脱敏；
8. Secret Reference + Secret Provider；
9. 权重灰度发布与回滚；
10. 最小 Docker Compose 部署。

后续可逐步补充：

```text
具体企业微信、Telegram 等 SDK 接入
图片、文件、卡片和流式 IM 回复
更精细的灰度规则
动态数据库凭据
更复杂的审批和内容审核
消息队列、死信和自动补偿
容量压测和自动扩缩容阈值
```

## 11. 结论

本方案以多租户隔离、无状态 Worker、共享状态存储和统一 IM 接入为基础，将 Agent Runtime、模型、Tool、Memory、Knowledge、审计、治理和运维能力整合为一个可扩展的平台。

平台的核心原则是：

```text
配置和数据按 Tenant 隔离；
Session 不绑定 Worker；
关键状态进入共享存储；
IM 消息先幂等、再调度、再执行；
危险行为先治理、再执行；
所有关键步骤可观测、可审计；
故障时避免重复执行，并优先保证状态一致性。
```

