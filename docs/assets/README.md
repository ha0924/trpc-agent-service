# 图稿

技术设计方案的两张核心图及其源文件。

| 文件 | 说明 |
| --- | --- |
| `系统架构图.html` | 源文件，浏览器直接渲染。展示 Gateway、Worker、Channel Adapter、Storage Adapter、Plugin / Guardrail、Telemetry、数据库与 IM 平台之间的关系 |
| `系统架构图.png` | 由上述 HTML 渲染导出，供 Markdown 内嵌 |
| `系统架构图.svg` | 矢量版，供放大查看或再编辑 |
| `核心时序图.html` | HTML + Mermaid，展示「IM 用户发消息 → Agent 执行 → Tool 调用 → Session / Memory 写入 → IM 回复」的完整主时序 |

图在 [`../技术设计方案.md`](../技术设计方案.md) 第 2 章引用。

## 图稿约定

- IM 通道保持抽象，可映射到企业微信、微信客服、Telegram 或其他支持 Webhook / 事件订阅的平台；各通道的实际差异见 [`../IM通道接入设计.md`](../IM通道接入设计.md)；
- 实线表达请求、执行或数据访问的主链路；
- 虚线表达配置下发、异步任务、治理与可观测等横切能力；
- 治理、监控、安全与故障恢复在图中体现为架构挂载点与关键边界，具体策略见各专项文档。
