-- 多租户节点化 Agent 平台 —— 死信演示素材
--
-- 设计依据：docs/风险清单.md #5「毒消息阻塞 Session 信箱」
--
-- 这份数据刻意构造一个**必然装配失败**的 Agent：它绑定了一个代码中未注册的
-- Tool。装配时 tool.Registry.Resolve 会直接报错，因此发给它的每条消息都失败。
--
-- 用途是演示并回归验证死信链路：
--
--   失败 → 对账扫描重投 → 再失败 → 达到 max_message_attempts → 移入死信
--
-- 以及最关键的那条性质：毒消息反复失败期间，其他会话不受影响。
--
-- 为什么不把它做成单元测试就算了：装配失败发生在 Worker 的执行路径深处，
-- 涉及扫描、重试计数、租约与信箱的相互作用，只有真的跑起来才能验证闭环。
--
-- 生产环境不应导入本文件。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

INSERT INTO agent_apps (tenant_id, agent_app_id, name, description, status) VALUES
  ('tenant-demo', 'broken', '故意失败的 Agent',
   '绑定了未注册的 Tool，用于死信链路演示。勿在生产导入。', 'active')
AS new
ON DUPLICATE KEY UPDATE name = new.name, description = new.description;

INSERT INTO agent_versions
  (tenant_id, agent_app_id, version, status, system_prompt, model_name,
   model_api_key_ref, description, published_at)
VALUES
  ('tenant-demo', 'broken', 'v1', 'published',
   '这个版本装配不出来。', 'deepseek-chat', NULL,
   '死信演示版本', NOW(3))
AS new
ON DUPLICATE KEY UPDATE status = new.status, description = new.description;

-- 关键一行：绑定一个代码中没有注册的 Tool。
--
-- Resolve 对未注册的 Tool 报错而非静默跳过，这本身也是刻意的：一个发布时
-- 期望有该 Tool 的版本，不该悄悄降级运行。此处正是利用了这个性质。
INSERT INTO agent_tool_bindings (tenant_id, agent_app_id, version, tool_name, mode) VALUES
  ('tenant-demo', 'broken', 'v1', 'tool_that_does_not_exist', 'allow')
AS new
ON DUPLICATE KEY UPDATE mode = new.mode;

INSERT INTO agent_deployments (tenant_id, agent_app_id, env, routes, updated_by) VALUES
  ('tenant-demo', 'broken', 'prod',
   JSON_ARRAY(JSON_OBJECT('version', 'v1', 'weight', 100)), 'seed-deadletter')
AS new
ON DUPLICATE KEY UPDATE routes = new.routes;

INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-broken', 'tenant-demo', 'broken', 'prod', 'mock',
   '/webhook/mock/broken',
   'secret://prod/tenant-demo/channel/mock-token',
   JSON_OBJECT('inbound_mode', 'payload', 'supports_push', TRUE,
               'supports_edit', FALSE, 'max_text_length', 2048,
               'rate_limit_per_min', 60),
   'active')
AS new
ON DUPLICATE KEY UPDATE webhook_path = new.webhook_path, status = new.status;
