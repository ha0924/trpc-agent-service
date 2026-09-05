-- 多租户节点化 Agent 平台 —— 第二个租户
--
-- 设计依据：docs/多租户与节点部署设计.md §3「租户隔离」
--
-- 这份数据是**对抗性**的：第二个租户刻意使用与第一个完全相同的
-- agent_app_id（assistant）和版本号（v1）。
--
-- 如果 Runtime 缓存键遗漏了 tenant_id，两个租户就会共用同一个 Runtime，
-- 也就共用了提示词、模型、工具与密钥——这是隔离设计中最容易被忽略、
-- 后果又最严重的一处。同名同版本正是能把这个缺陷暴露出来的最小构造。
--
-- 两个租户的差异刻意设得肉眼可辨：提示词不同、工具集不同、治理策略不同、
-- 预算不同。只要回复内容或可用工具串了，一眼就能看出来。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

-- ---------------------------------------------------------------------------
-- 租户二：预算刻意设得极小，用于演示预算拦截
-- ---------------------------------------------------------------------------

INSERT INTO tenants (tenant_id, name, status, settings) VALUES
  ('tenant-acme', 'ACME 公司', 'active',
   JSON_OBJECT(
     'daily_token_budget',   500,
     'monthly_token_budget', 10000,
     'rate_limit_per_min',   30
   ))
AS new
ON DUPLICATE KEY UPDATE name = new.name, settings = new.settings, status = new.status;

-- 同名 Agent：与 tenant-demo 的 agent_app_id 完全一致
INSERT INTO agent_apps (tenant_id, agent_app_id, name, description, status) VALUES
  ('tenant-acme', 'assistant', 'ACME 助手', '与 tenant-demo 同名同版本，用于验证隔离', 'active')
AS new
ON DUPLICATE KEY UPDATE name = new.name, description = new.description;

-- 同版本号 v1，但提示词与模型参数完全不同。
-- 若两租户串了 Runtime，回复的口吻会立刻暴露。
INSERT INTO agent_versions
  (tenant_id, agent_app_id, version, status, system_prompt,
   model_name, model_api_key_ref, model_params, description, published_at)
VALUES
  ('tenant-acme', 'assistant', 'v1', 'published',
   '你是 ACME 公司的内部助手。只回答与 ACME 业务相关的问题，其余一律拒答。',
   'deepseek-chat',
   'secret://prod/tenant-acme/model/primary-api-key',
   JSON_OBJECT('temperature', 0.1, 'max_tokens', 512),
   'ACME 基线版本',
   NOW(3))
AS new
ON DUPLICATE KEY UPDATE
  system_prompt = new.system_prompt,
  model_params  = new.model_params,
  status        = new.status;

-- 工具集不同：只给 calculator，且刻意 deny 掉 search。
-- 若白名单按租户隔离失效，tenant-acme 就能调到 search。
INSERT INTO agent_tool_bindings (tenant_id, agent_app_id, version, tool_name, mode, params) VALUES
  ('tenant-acme', 'assistant', 'v1', 'calculator', 'allow', NULL),
  ('tenant-acme', 'assistant', 'v1', 'search',     'deny',  NULL)
AS new
ON DUPLICATE KEY UPDATE mode = new.mode;

-- 治理策略也不同：只启用脱敏与预算，不启用工具白名单之外的其余项
INSERT INTO agent_extension_bindings
  (tenant_id, agent_app_id, version, kind, extension_name, enabled, priority, params)
VALUES
  ('tenant-acme', 'assistant', 'v1', 'guardrail', 'redaction',      1, 10, NULL),
  ('tenant-acme', 'assistant', 'v1', 'guardrail', 'tool_whitelist', 1, 20, NULL),
  ('tenant-acme', 'assistant', 'v1', 'guardrail', 'budget_limit',   1, 30, NULL)
AS new
ON DUPLICATE KEY UPDATE enabled = new.enabled, priority = new.priority;

INSERT INTO agent_deployments (tenant_id, agent_app_id, env, routes, updated_by) VALUES
  ('tenant-acme', 'assistant', 'prod',
   JSON_ARRAY(JSON_OBJECT('version', 'v1', 'weight', 100)), 'seed')
AS new
ON DUPLICATE KEY UPDATE routes = new.routes;

-- 独立的通道绑定与 webhook 路径
INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-acme', 'tenant-acme', 'assistant', 'prod', 'mock',
   'acme-app-1', '/webhook/mock/acme',
   'secret://prod/tenant-acme/channel/mock-token',
   JSON_OBJECT(
     'inbound_mode',       'payload',
     'supports_push',      TRUE,
     'supports_edit',      FALSE,
     -- 长度上限也不同，用于验证拆分逻辑按绑定取值而非取全局常量
     'max_text_length',    512,
     'rate_limit_per_min', 30
   ),
   'active')
AS new
ON DUPLICATE KEY UPDATE
  capabilities = new.capabilities, webhook_path = new.webhook_path, status = new.status;

-- 刻意使用与 tenant-demo 相同的外部用户名 alice。
-- 会话由 channel_binding_id + scope + scope_key 定位，绑定不同则会话必然不同；
-- 若隔离失效，两个租户的 alice 会落到同一个会话。
INSERT INTO channel_users
  (tenant_id, channel_binding_id, external_user_id, internal_user_id, display_name, attributes, status)
VALUES
  ('tenant-acme', 'cb-acme', 'alice', 'acme-alice', 'ACME Alice',
   JSON_OBJECT('roles', JSON_ARRAY('staff')), 'active')
AS new
ON DUPLICATE KEY UPDATE internal_user_id = new.internal_user_id, attributes = new.attributes;
