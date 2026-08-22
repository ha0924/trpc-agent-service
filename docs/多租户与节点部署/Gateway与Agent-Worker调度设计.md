# Gateway 与 Agent Worker 调度设计

## 1. 要解决的问题

多租户平台中存在多个 Agent Worker 实例，每个租户又可以创建多个 Agent，并发布不同版本。

平台需要解决三个核心问题：

1. Gateway 如何发现当前可用的 Worker；
2. 请求如何被分配到合适的 Worker；
3. Worker 如何找到或加载请求指定的 Agent 版本。

## 2. 核心概念

### 2.1 Agent Worker

Agent Worker 是负责执行 Agent 的逻辑服务。

实际部署时，一个 Worker Instance 通常对应一个进程或 Kubernetes Pod。多个 Worker Instance 共同组成 Worker 集群，并通过增加实例数量实现水平扩展。

### 2.2 Agent Runtime Key

平台使用以下组合唯一标识一个已经发布的 Agent 版本：

```text
tenant_id + agent_id + agent_version
```

例如：

```text
tenant-a / customer-service / v2
```

`session_id` 是会话标识，不属于 Agent Runtime Key。Session 会绑定其使用的 Agent 版本，但不会绑定固定的 Worker 实例。

### 2.3 Runtime Registry

每个 Worker 内部维护一个本地 Runtime Registry，保存当前进程已经加载的 Agent Runtime：

```text
tenant-a / customer-service / v2 → Agent Runtime
tenant-b / operations-agent / v1 → Agent Runtime
```

Agent Runtime 可以包含：

- tRPC-Agent-Go Agent；
- `runner.Runner`；
- Model 配置；
- Tool / MCP；
- Knowledge；
- Plugin / Guardrail；
- Session、Memory 和 Artifact Service 引用。

平台配置中心是 Agent 配置的权威来源，Worker 的 Runtime Registry 只是可丢失、可重新构建的本地缓存。

## 3. Worker 服务发现

Worker 启动后，需要让 Gateway 知道当前有哪些实例可以接收请求。

```text
Worker 启动
  → 注册服务并上报健康状态

Gateway
  → 获取健康 Worker 列表
  → 选择可用 Worker
```

服务发现可以由 Kubernetes Service、Consul、etcd、Nacos 等基础设施实现，平台不需要重复开发一套注册中心。

第一版只需要保证 Gateway 能够获得健康的 Worker 地址，不要求 Gateway 感知每个 Worker 已缓存了哪些 Agent。

## 4. 请求路由过程

Gateway 收到请求后，首先确定：

```text
tenant_id
agent_id
agent_version
session_id
request_id
```

其中：

- `tenant_id` 确定租户；
- `agent_id` 确定 Agent 应用；
- `agent_version` 确定本次运行使用的配置版本；
- `session_id` 确定需要读取和更新的会话；
- `request_id` 标识本次执行。

Session Scheduler 保证同一个 Session 的消息按顺序执行。随后 Gateway 从健康 Worker 中选择一个实例，并将上述运行上下文发送给它。

Gateway 不直接操作 Worker 内部的 Agent 对象。

## 5. Worker 执行过程

Worker 收到请求后，根据 Runtime Key 查询本地 Runtime Registry。

### 5.1 Runtime 已加载

如果本地已经存在对应 Runtime，则直接复用：

```text
查询 Runtime Registry
  → 命中 Agent Runtime
  → 调用 Runner.Run
```

### 5.2 Runtime 未加载

如果本地没有对应 Runtime：

```text
查询 Runtime Registry
  → 未命中
  → 从配置中心读取 Agent 版本配置
  → 创建 Model、Tool、Knowledge 和 Plugin
  → 构建 Agent 与 Runner
  → 写入 Runtime Registry
  → 执行请求
```

Worker 需要对相同 Runtime Key 的加载过程进行并发控制，避免多个请求同时重复构建同一个 Agent Runtime。

## 6. Session 与 Worker 的关系

Session 绑定 Agent 及其版本，但不绑定固定 Worker：

```text
session_id → tenant-a / customer-service / v2
```

Session 和 Memory 保存在共享后端，因此同一 Session 的后续请求可以由不同 Worker 执行：

```text
第一次请求 → Worker 1
第二次请求 → Worker 3
```

新的 Worker 可以通过 `session_id` 从共享存储恢复上下文，并继续使用相同的 Agent 版本执行。

Session Scheduler 负责避免同一个 Session 同时被多个 Worker 并发执行。

## 7. Runtime 缓存管理

Worker 的 Runtime Registry 不能无限增长，需要支持：

- 长时间未使用的 Runtime 自动淘汰；
- Agent 版本下线后停止接收新请求；
- 正在运行的请求完成后再释放 Runtime；
- 配置版本变化时加载新的 Runtime，而不是原地修改旧版本；
- Worker 关闭时调用 Runner 及相关资源的 `Close()`；
- 缓存丢失后能够从配置中心重新构建。

Runtime 缓存键必须包含 `tenant_id`，避免不同租户的模型、工具、知识库和密钥发生混用。

## 8. 完整流程

```text
用户请求
  → Message Adapter 生成统一消息
  → Gateway 确定租户、Agent、版本和 Session
  → Session Scheduler 保证同一会话顺序执行
  → Gateway 选择健康 Worker
  → Worker 查询本地 Runtime Registry
      → 命中：直接执行
      → 未命中：读取配置并构建 Runtime
  → Runner 从共享存储读取 Session / Memory
  → 执行 Agent
  → 写回 Session / Memory
  → 返回执行结果
```

## 9. 第一版设计原则

1. 一个 Worker 可以承载多个租户、多个 Agent 和多个版本；
2. Agent 创建或发布时不永久绑定某个 Worker；
3. Session 绑定 Agent 版本，但不绑定 Worker；
4. Gateway 负责 Worker 选择，不操作 Worker 内部 Runtime；
5. Worker 根据请求按需加载并缓存 Agent Runtime；
6. 配置中心是权威数据源，本地 Runtime Registry 只是缓存；
7. Worker 保持无状态，Session 和 Memory 使用共享后端；
8. 第一版先明确职责和接口，不提前绑定具体的服务发现与调度实现。
