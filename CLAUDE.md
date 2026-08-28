# CLAUDE.md

基于 tRPC-Agent-Go 的多租户节点化 Agent 平台。租户可创建并发布自己的 Agent，通过 IM、Web 或 API 对外提供服务。

## 文档分工

改动前先确认写在哪一份，避免内容重复或漂移。

| 文档 | 职责 |
|---|---|
| `技术方案/多租户节点化Agent平台技术设计方案.md` | 主设计文档，交付件 |
| `docs/` 各模块目录 | 模块详设，主文档由此汇总 |
| `docs/技术实现文档/技术栈说明.md` | 选了什么技术、为什么 |
| `docs/技术实现文档/代码组织方案.md` | 代码放在哪里 |
| `docs/技术实现文档/数据模型设计.md` | 表结构 |
| `时间规划/迭代计划.md` | 三期怎么分 |
| `时间规划/一期实现内容.md` | 一期做什么、做到什么程度 |

## 已定决策

- Gateway 与 Worker 是两个进程，一个仓库两个可执行文件，通过消息队列与共享存储协作，不互相调 RPC；
- 同一 Session 的顺序由 Session 租约与信箱保证，不依赖消息队列的顺序特性；
- 技术栈为 tRPC-Agent-Go + Gin + MySQL + Redis，不引入 tRPC-Go RPC 框架；
- 消息队列选型未定，一期用 Redis List 顶替，通过 `SessionDispatcher` 接口隔离；
- `trpcservice/agent` 包的职责是 Runtime 装配，不存放具体 Agent 定义；
- `context.Context` 必须贯穿 Gateway 到模型、Tool、存储的全链路，用于承载租户上下文与 `trace_id`；
- 流式响应不在交付范围内。

## 代码规范

待一期实现后补充。
