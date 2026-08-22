# Agent Runtime 多节点扩缩容设计

## 1. 文档目标

本文是《Gateway 与 Agent Worker 调度设计》的后续设计。

前一篇文档说明请求如何到达 Worker，以及 Worker 如何找到或加载 Agent Runtime。本文继续说明：

- 同一个 Agent Runtime 应该加载到多少个 Worker；
- Worker 扩容后如何承接 Agent 请求；
- 如何减少重复加载造成的资源浪费；
- Worker 故障或缩容时如何转移请求。

## 2. 基本原则

平台扩展的是 Worker 实例数量，不是把一个 Agent 永久绑定到某个 Worker。

```text
Agent 配置和版本 → 保存在平台配置中心
Agent Runtime    → 加载并缓存在 Worker 中
Session / Memory → 保存在共享存储中
```

同一个 Agent Runtime 可以同时加载到多个 Worker，以提高并发能力和可用性。

## 3. Agent Runtime 加载方式

### 3.1 启动预加载

Worker 启动时，可以提前加载一部分 Agent，例如：

- 平台默认 Agent；
- 调用量较高的热门 Agent；
- 重要租户的核心 Agent；
- 配置中明确要求预热的 Agent。

这样可以减少首次请求的等待时间。

### 3.2 请求懒加载

Worker 收到请求后，如果本地没有对应的 Agent Runtime，则从配置中心读取配置并加载：

```text
请求到达 Worker
  → 查询本地 Runtime Registry
  → 未找到对应 Runtime
  → 从配置中心加载
  → 缓存并执行
```

懒加载适合调用频率较低的 Agent，可以避免所有 Worker 都提前占用资源。

### 3.3 专属 Worker 池

资源消耗较大或隔离要求较高的 Agent，可以使用专属 Worker 池，例如：

- 需要独立沙箱的 Agent；
- 使用大量 MCP 连接的 Agent；
- 高流量租户的核心 Agent；
- 对安全和资源隔离要求较高的 Agent。

Gateway 只将这些 Agent 的请求发送到指定 Worker 池。

## 4. Runtime 副本控制

同一个 Agent Runtime 可以加载到多个 Worker，但不应无控制地加载到所有 Worker。

平台可以为 Agent Deployment 设置运行策略：

```yaml
runtime_policy:
  load_mode: lazy
  preferred_replicas: 2
  max_replicas: 4
  idle_ttl: 30m
```

字段含义：

- `load_mode`：懒加载、预加载或专属 Worker 池；
- `preferred_replicas`：希望保持的 Runtime 副本数；
- `max_replicas`：允许加载的最大副本数；
- `idle_ttl`：长时间没有请求后，可以释放 Runtime。

第一版不一定立即实现全部策略，但数据模型和接口应保留扩展能力。

## 5. Gateway 调度策略

Gateway 确定 `tenant_id + agent_id + agent_version` 后，可以按以下顺序选择 Worker：

1. 选择健康的 Worker；
2. 优先选择已经加载目标 Runtime 的 Worker；
3. 在候选 Worker 中选择负载较低的实例；
4. 如果没有命中，则选择允许懒加载的 Worker。

```text
Gateway
  → 查找已加载目标 Runtime 的健康 Worker
      ├─ 找到：选择低负载 Worker
      └─ 未找到：选择一个 Worker 懒加载
```

优先命中已加载 Runtime 是一种调度优化。即使没有命中，其他 Worker 也可以重新加载，因此不会形成固定节点依赖。

## 6. Worker 水平扩容

当请求量增加时，平台启动新的 Worker 实例：

```text
新增 Worker
  → 加入服务发现
  → 根据策略预加载部分热门 Agent
  → 开始接收请求
  → 其他 Agent 在请求到达时懒加载
```

新 Worker 不需要复制全部 Agent，也不需要复制 Session 和 Memory。

因为 Session 和 Memory 位于共享存储中，新 Worker 加载正确的 Agent 版本后，就可以处理已有会话。

## 7. Runtime 扩容

如果某个 Agent 的流量持续增加，可以让更多 Worker 加载该 Agent Runtime：

```text
Agent v2 原有副本：
Worker 1、Worker 2

流量增加后：
Worker 1、Worker 2、Worker 3、Worker 4
```

这里增加的是 Agent Runtime 的可执行副本，不会创建新的 Agent 配置版本，也不会改变原来的 Session。

## 8. Worker 缩容

Worker 缩容时，应按以下顺序处理：

1. 将 Worker 标记为不再接收新请求；
2. 等待正在执行的请求完成；
3. 超时后取消仍未完成的请求；
4. 关闭 Runner、连接和后台任务；
5. 从服务发现和 Runtime 状态中移除该 Worker。

由于 Session 和 Memory 位于共享存储中，后续请求可以由其他 Worker 继续处理。

## 9. Worker 故障

Worker 心跳超时或健康检查失败后：

```text
Gateway 停止向故障 Worker 分配请求
  → 选择其他健康 Worker
  → 加载对应 Agent Runtime
  → 从共享存储读取 Session 和 Memory
  → 继续处理后续消息
```

如果故障发生在请求执行过程中，需要结合消息幂等和 Session Scheduler 判断是否重试，避免同一个请求被重复执行。

## 10. Worker 状态信息

为了支持更合理的调度，Worker 可以定期上报：

- Worker 是否健康；
- 当前运行请求数；
- 当前并发 Session 数；
- CPU 和内存使用情况；
- 已加载的 Runtime Key；
- Runtime 最近使用时间；
- Agent 加载失败次数。

第一版可以只使用健康状态和基础负载信息，后续再增加 Runtime 感知调度。

## 11. 完整流程

```text
用户请求
  → Gateway 确定 Agent 版本
  → Session Scheduler 保证会话顺序
  → Gateway 选择健康 Worker
      → 优先选择已加载目标 Runtime 的 Worker
      → 否则选择可懒加载的 Worker
  → Worker 查找或加载 Agent Runtime
  → Runner 读取共享 Session / Memory
  → 执行 Agent 并返回结果
```

## 12. 第一版建议

第一版采用以下简单方案：

1. Worker 启动时预加载少量默认或热门 Agent；
2. 其他 Agent 在请求到达时懒加载；
3. 同一个 Agent 允许被多个 Worker 加载；
4. Gateway 优先选择已加载目标 Runtime 的 Worker；
5. Session 和 Memory 使用共享后端；
6. Agent 不永久绑定 Worker；
7. 为后续副本控制、专属 Worker 池和自动扩缩容保留配置能力。

## 13. 总结

> Agent 配置保存在控制面，Agent Runtime 分布在 Worker 中，Session 和 Memory 保存在共享存储中。平台通过预加载、懒加载和副本控制，让 Agent 能够随 Worker 数量一起水平扩展。
