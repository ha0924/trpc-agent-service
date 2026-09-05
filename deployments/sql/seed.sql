-- 多租户节点化 Agent 平台 —— 初始数据
--
-- 设计依据：docs/dev/一期建表与初始数据.md §3「初始数据」
--
-- 预置一条可运行的完整链路：
--   tenant-demo → assistant → v1(published) → prod 部署(权重 100)
--                → Mock 通道绑定 cb-demo，webhook /webhook/mock/demo
--
-- 幂等：可重复执行，重复执行只刷新配置内容，不产生重复行。
-- 使用 INSERT ... AS new ... ON DUPLICATE KEY UPDATE 的别名语法，
-- 而非已弃用的 VALUES() 函数（MySQL 8.0.20+ 起弃用）。

USE trpc_agent_platform;

-- 租户
INSERT INTO tenants (tenant_id, name, status, settings) VALUES
  ('tenant-demo', '演示租户', 'active',
   JSON_OBJECT('daily_token_budget', 1000000, 'rate_limit_per_min', 60))
AS new
ON DUPLICATE KEY UPDATE
  name = new.name, status = new.status, settings = new.settings;

-- Agent 应用
INSERT INTO agent_apps (tenant_id, agent_app_id, name, description, status) VALUES
  ('tenant-demo', 'assistant', '通用助手', '一期端到端链路验证用 Agent', 'active')
AS new
ON DUPLICATE KEY UPDATE
  name = new.name, description = new.description, status = new.status;

-- Agent 版本：已发布，不可修改。改配置一律创建新版本。
-- 模型密钥只存引用，明文在 Secret Manager。
INSERT INTO agent_versions
  (tenant_id, agent_app_id, version, status, system_prompt,
   model_name, model_api_key_ref, model_params, description, published_at)
VALUES
  ('tenant-demo', 'assistant', 'v1', 'published',
   '你是一个乐于助人的助手。回答简洁准确，不确定时直说不知道。',
   'deepseek-chat',
   'secret://prod/tenant-demo/model/primary-api-key',
   JSON_OBJECT('temperature', 0.7, 'max_tokens', 2048),
   '一期基线版本',
   NOW(3))
AS new
ON DUPLICATE KEY UPDATE
  system_prompt = new.system_prompt,
  model_name    = new.model_name,
  model_params  = new.model_params,
  status        = new.status;

-- Tool 绑定：两个查询类 Tool，均为 allow
INSERT INTO agent_tool_bindings (tenant_id, agent_app_id, version, tool_name, mode, params) VALUES
  ('tenant-demo', 'assistant', 'v1', 'search',     'allow', JSON_OBJECT('timeout_ms', 5000)),
  ('tenant-demo', 'assistant', 'v1', 'calculator', 'allow', NULL)
AS new
ON DUPLICATE KEY UPDATE
  mode = new.mode, params = new.params;

-- 扩展绑定：一个日志类 Callback。
-- 装配时须按列表遍历挂载，不可写死——二期往列表追加即可，装配签名不变。
INSERT INTO agent_extension_bindings
  (tenant_id, agent_app_id, version, kind, extension_name, enabled, priority, params)
VALUES
  ('tenant-demo', 'assistant', 'v1', 'callback', 'request_logger', 1, 10, NULL)
AS new
ON DUPLICATE KEY UPDATE
  enabled = new.enabled, priority = new.priority, params = new.params;

-- agent_mcp_bindings 与 agent_skill_bindings 一期建表但不写入数据。
-- 建表目的是装配逻辑按四类能力统一遍历，二期接入时不改装配结构。

-- 发布：一行一环境，权重存于 routes。一期只有一个版本占 100%，
-- 但 Gateway 侧按权重选版本的逻辑需真实实现。
INSERT INTO agent_deployments (tenant_id, agent_app_id, env, routes, updated_by) VALUES
  ('tenant-demo', 'assistant', 'prod',
   JSON_ARRAY(JSON_OBJECT('version', 'v1', 'weight', 100)),
   'seed')
AS new
ON DUPLICATE KEY UPDATE
  routes = new.routes, updated_by = new.updated_by;

-- 通道绑定：Mock 通道。
-- capabilities 即使对 Mock 通道也照填，保证二期接入真实通道时结构不变。
INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-demo', 'tenant-demo', 'assistant', 'prod', 'mock',
   'mock-app-1', '/webhook/mock/demo',
   'secret://prod/tenant-demo/channel/mock-token',
   JSON_OBJECT(
     'inbound_mode',        'payload',
     'supports_push',       TRUE,
     'supports_edit',       FALSE,
     'max_text_length',     2048,
     'rate_limit_per_min',  20
   ),
   'active')
AS new
ON DUPLICATE KEY UPDATE
  capabilities = new.capabilities,
  webhook_path = new.webhook_path,
  status       = new.status;

-- sessions、session_events、inbound_events 由运行时写入，不预置。
