# CLAUDE.md

基于 tRPC-Agent-Go 的多租户节点化 Agent 平台。租户可创建并发布自己的 Agent，通过 IM、Web 或 API 对外提供服务。

## 文档分工

全部文档集中在 `docs/`，索引见 `docs/README.md`。改动前先确认写在哪一份，避免内容重复或漂移。

| 文档 | 职责 |
|---|---|
| `docs/技术设计方案.md` | 主设计文档，交付件。其余为其详细展开 |
| `docs/多租户与节点部署设计.md` | 租户模型、隔离、调度、Runtime 装配、扩缩容 |
| `docs/IM通道接入设计.md` | Channel 抽象、消息模型、通道差异 |
| `docs/数据同步与多后端适配.md` | Storage Router、后端选择、一致性与迁移 |
| `docs/治理监控与安全设计.md` | Guardrail、Metrics、审计、密钥脱敏 |
| `docs/故障恢复与运维设计.md` | 降级、资源管理、灰度回滚、部署 |
| `docs/数据模型设计.md` | 表清单与建表 DDL，唯一权威来源 |
| `docs/风险清单.md` | 生产风险及缓解措施 |
| `docs/框架复用与扩展.md` | 复用什么、扩展什么、为什么 |
| `docs/assets/` | 架构图与时序图 |
| `docs/dev/` | 施工过程文档，非交付物：迭代计划、技术栈、代码组织、一期范围与建表 |

## 已定决策

- Gateway 与 Worker 是两个进程，一个仓库两个可执行文件，通过消息队列与共享存储协作，不互相调 RPC；
- Gateway 不做服务发现、不维护 Worker 列表、不选择由哪个 Worker 执行，由消费者争抢；
- 同一 Session 的顺序由 Session 租约与信箱保证，不依赖消息队列的顺序特性；
- 技术栈为 tRPC-Agent-Go + Gin + MySQL + Redis，不引入 tRPC-Go RPC 框架；
- 消息队列选型未定，一期用 Redis List 顶替，通过 `SessionDispatcher` 接口隔离；
- `trpcservice/agent` 包的职责是 Runtime 装配，不存放具体 Agent 定义；
- `context.Context` 必须贯穿 Gateway 到模型、Tool、存储的全链路，用于承载租户上下文与 `trace_id`；
- 平台 `Channel` 接口内嵌 `openclaw/channel.Channel`，统一消息模型基于 `openclaw/gwproto.ContentPart`；只依赖这两个包，不依赖 openclaw 的 `app` 与 `internal`；
- Storage Router 实现框架的 `session.Service` 接口，经 `runner.WithSessionService` 注入；
- 模型配置是 `agent_versions` 的列，不单独建表；密钥引用分散在各表的 `_ref` 列，不建密钥表；
- 流式响应不在交付范围内。

## 代码规范

### 可追溯

每个实现文件顶部写一行「设计依据」，指明它实现的是哪份文档的哪一节：

```go
// 设计依据：docs/IM通道接入设计.md §3「统一消息模型」
package types
```

目的是让评审能从代码走到理由，而不必猜。**改代码时若发现与设计文档不符，先改文档再改代码**，不允许两者漂移——本项目已经因文档漂移出现过一次「Gateway 选 Worker」与「队列争抢」两套并存的矛盾。

实测发现的坑要回写文档，不能只留在聊天记录里。例：openclaw 会把核心框架降级到 v1.8.0，已记入 `docs/框架复用与扩展.md` §5.1。

### 依赖方向

```text
cmd/gateway → trpcservice/gateway  ┐
cmd/worker  → trpcservice/agent    ┴→ scheduler / storage / config / channels → types
```

两条硬约束：

- `types` 不依赖任何其他内部包，其余包都可依赖它；
- `gateway` 与 `agent` 互不依赖，协作只经由队列与共享存储。

### context 传递

**每个跨越包边界的函数第一个参数必须是 `context.Context`**，且必须一路传到模型、Tool、存储与 IM 客户端。

这不是风格偏好：`RequestContext` 承载 `tenant_id` 与 `trace_id`，缺少 context 的签名无法参与隔离、追踪与取消，而后补需要改动所有实现。这是设计中唯一真正无法后补的部分。

不要把 `context.Context` 存进结构体字段。

### 租户隔离：失败即拒绝

选择配置或访问数据时，必须用 `types.FromContext` 并在出错时中止：

```go
rc, err := types.FromContext(ctx)
if err != nil {
    return err   // 不要退化为空租户继续执行
}
```

`types.TenantID(ctx)` 这类宽松取值只允许用于日志和指标。把未知租户当成空字符串继续跑，正是跨租户泄漏的成因。

Runtime 缓存键一律用 `RequestContext.RuntimeKey()`，不要手工拼字符串——两个租户可能用同名 Agent 和同一个版本号。

### 错误处理

- 用 `fmt.Errorf("...: %w", err)` 包装并保留错误链，包装信息说明「在做什么」而非重复被包装的错误；
- 哨兵错误定义为包级 `Err` 前缀变量，调用方用 `errors.Is` 判断；
- 状态不确定时（幂等记录写失败、租约续期失败）**一律不继续执行**，宁可让上游重投；
- 抢不到租约是正常结果，不是错误，不要记为 error 级日志。

### 日志与脱敏

- 结构化日志统一用 `RequestContext.LogFields()` 提供的键，不要各自发明键名，否则两个进程的日志无法关联；
- `trace_id` 必须出现在每条与请求相关的日志中；
- 密钥、Token、API Key、DSN、个人信息一律经 Redactor 脱敏，且禁止进入 Trace 属性与 Metric 标签。

### 注释

注释写「为什么」，不写「是什么」。已经能从代码读出来的事实不要重复。

值得写的是：为什么选这个方案而不是显而易见的另一个、这里有什么不变量、违反了会怎样。

### 测试

- 与被测包同目录，`_test.go` 结尾；
- 表驱动用 `t.Run` 命名子用例；
- **隔离性与顺序性必须有测试**：同名不同租户的缓存键、租约失效后的并发写、重复投递的幂等拦截；
- 不追求覆盖率数字，优先覆盖出错会导致数据错误或跨租户泄漏的路径。

### 其他

- 提交前跑 `./format.sh`、`./lint.sh`、`go test ./...`；
- 导出标识符必须有文档注释，以标识符名开头；
- 装配结果返回接口（`agent.Agent`）而非具体类型；
- 扩展按列表遍历挂载，不写死名字。
