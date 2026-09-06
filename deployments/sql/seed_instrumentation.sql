-- 多租户节点化 Agent 平台 —— 补充：调用埋点扩展
--
-- 设计依据：docs/技术设计方案.md §7.3「Metrics、Trace 和审计」
--
-- 模型与工具的调用耗时只能在框架回调内部测量——那是唯一能同时观察到一次调用
-- 开始与结束的地方。Worker 看到的是一整轮 Agent 执行，而不是其中的每次调用。
-- 因此这项埋点做成扩展，由本文件为各版本启用。
--
-- priority 设为 5，早于所有 Guardrail：耗时应当包含治理策略本身的开销，
-- 否则测出来的模型耗时会漏掉脱敏与预算检查所花的时间。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

INSERT INTO extension_definitions
  (tenant_id, kind, extension_name, display_name, description, status)
VALUES
  ('', 'callback', 'instrumentation', '调用埋点',
   '在框架回调内测量模型与工具调用耗时，并把策略拒绝与真实失败区分开', 'active')
AS new
ON DUPLICATE KEY UPDATE display_name = new.display_name, description = new.description;

INSERT INTO agent_extension_bindings
  (tenant_id, agent_app_id, version, kind, extension_name, enabled, priority, params)
VALUES
  ('tenant-demo', 'assistant', 'v1', 'callback', 'instrumentation', 1, 5, NULL),
  ('tenant-demo', 'assistant', 'v2', 'callback', 'instrumentation', 1, 5, NULL),
  ('tenant-acme', 'assistant', 'v1', 'callback', 'instrumentation', 1, 5, NULL)
AS new
ON DUPLICATE KEY UPDATE enabled = new.enabled, priority = new.priority;
