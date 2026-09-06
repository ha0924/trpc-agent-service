-- 多租户节点化 Agent 平台 —— 企业微信智能机器人（长连接）测试租户
--
-- 设计依据：docs/IM通道接入设计.md §9「长连接接入模式」
--
-- 自成一套的测试租户，不复用 tenant-demo：真实机器人联调会产生真实会话与
-- 真实用量，混进演示租户会让「多租户隔离」的验证结论变得不可信——两个租户
-- 的数据一旦掺在一起，就说不清隔离是真的成立还是恰好没被触发。
--
--   tenant-test → test-agent → v1(published) → prod 部署(权重 100)
--                → 长连接绑定 cb-test-aibot，无 webhook_path
--
-- 与 seed.sql 的 tenant-demo 相比，此租户额外具备的验证价值：
--   1. 它是第三个租户，用同一份代码承载 stream 模式，验证隔离不依赖通道类型；
--   2. 它的预算刻意设小，便于观察预算拦截而不必等演示租户跑满。
--
-- 长连接与回调模式的三处结构差异（详见 §9.1）：
--   webhook_path 为 NULL —— 没有回调地址，Gateway 的 catch-all 路由不会命中；
--   secret_ref 的凭据块只含 bot_id / secret —— 明文传输，无 token 与 aes key；
--   inbound_mode 为 stream —— 决定 Gateway 是否为它起 Run()；
--   outbound_mode 为 via_holder —— 决定 Worker 是否把回复投出站信箱。
--
-- 凭据不入库：明文只存在于 Secret Manager（一期是环境变量）。
--
-- 执行顺序：须在 schema.sql 与 schema_governance.sql 之后——channel_users
-- 建在治理 schema 里，先跑此脚本会因表不存在而失败。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

-- 测试租户。预算刻意设小：真实联调时若把预算设得和演示租户一样宽，
-- 预算拦截这条治理策略在测试租户上就永远不会触发，等于没验证。
INSERT INTO tenants (tenant_id, name, status, settings) VALUES
  ('tenant-test', '测试租户', 'active',
   JSON_OBJECT('daily_token_budget', 50000, 'rate_limit_per_min', 30))
AS new
ON DUPLICATE KEY UPDATE
  name = new.name, status = new.status, settings = new.settings;

-- 测试 Agent
INSERT INTO agent_apps (tenant_id, agent_app_id, name, description, status) VALUES
  ('tenant-test', 'test-agent', '测试 Agent', '企业微信长连接联调用', 'active')
AS new
ON DUPLICATE KEY UPDATE
  name = new.name, description = new.description, status = new.status;

-- 版本。模型密钥只存引用；此租户用自己的引用，与 demo 不共享——
-- 一个租户的密钥解析失败只影响该租户。
INSERT INTO agent_versions
  (tenant_id, agent_app_id, version, status, system_prompt,
   model_name, model_api_key_ref, model_params, description, published_at)
VALUES
  ('tenant-test', 'test-agent', 'v1', 'published',
   '你是企业微信里的测试助手。回答简洁，不确定时直说不知道。',
   'deepseek-chat',
   'secret://prod/tenant-test/model/primary-api-key',
   JSON_OBJECT('temperature', 0.7, 'max_tokens', 2048),
   '长连接联调基线版本',
   NOW(3))
AS new
ON DUPLICATE KEY UPDATE
  system_prompt = new.system_prompt,
  model_name    = new.model_name,
  model_params  = new.model_params,
  status        = new.status;

-- Tool 绑定：只给 calculator。
-- 刻意比 tenant-demo（search + calculator）少一个，这样「同名 Agent 不同租户
-- 装配出不同 Runtime」这件事在联调时可以直接观察到，而不是只靠单元测试断言。
INSERT INTO agent_tool_bindings (tenant_id, agent_app_id, version, tool_name, mode, params) VALUES
  ('tenant-test', 'test-agent', 'v1', 'calculator', 'allow', NULL)
AS new
ON DUPLICATE KEY UPDATE
  mode = new.mode, params = new.params;

-- 扩展绑定：五条治理策略 + 日志 Callback。
--
-- 与 tenant-demo 同一套策略名，但**参数不同**：user_permission 的
-- allow_unmapped 设为 FALSE，即未建立映射的用户直接拒绝。demo 那边是 TRUE。
-- 这是刻意的对照——同名策略在不同租户下行为不同，才能说明治理是按租户配置
-- 驱动的，而不是写死在代码里。
--
-- priority 决定挂载顺序，值小者先执行。redaction 必须早于任何会持久化内容
-- 的策略，否则未脱敏的原文会被写进审计。
INSERT INTO agent_extension_bindings
  (tenant_id, agent_app_id, version, kind, extension_name, enabled, priority, params)
VALUES
  ('tenant-test', 'test-agent', 'v1', 'guardrail', 'redaction', 1, 10, NULL),
  ('tenant-test', 'test-agent', 'v1', 'guardrail', 'tool_whitelist', 1, 20, NULL),
  ('tenant-test', 'test-agent', 'v1', 'guardrail', 'budget_limit', 1, 40, NULL),
  -- allow_unmapped=FALSE：与 demo 相反。没有 channel_users 映射的用户会被
  -- 拒绝，因此下面那条映射不是可选项。
  ('tenant-test', 'test-agent', 'v1', 'guardrail', 'user_permission', 1, 50,
   JSON_OBJECT('allow_unmapped', FALSE)),
  ('tenant-test', 'test-agent', 'v1', 'callback', 'request_logger', 1, 90, NULL)
AS new
ON DUPLICATE KEY UPDATE
  enabled = new.enabled, priority = new.priority, params = new.params;

-- 发布
INSERT INTO agent_deployments (tenant_id, agent_app_id, env, routes, updated_by) VALUES
  ('tenant-test', 'test-agent', 'prod',
   JSON_ARRAY(JSON_OBJECT('version', 'v1', 'weight', 100)),
   'seed')
AS new
ON DUPLICATE KEY UPDATE
  routes = new.routes, updated_by = new.updated_by;

-- 长连接通道绑定
INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-test-aibot', 'tenant-test', 'test-agent', 'prod', 'wecom_aibot',
   -- BotID 放这里便于运维核对；建连实际读的是 secret_ref，不依赖此列。
   -- 因此换 bot 时改密钥即可，不必动这一行。
   'aibOG1vdLfZeDzhKIqQgouwITx_2bd-MEyp',
   -- 长连接无回调地址。uk_webhook 是唯一键，但 MySQL 允许多行 NULL，
   -- 所以多条长连接绑定可以并存。
   NULL,
   'secret://prod/tenant-test/channel/wecom-aibot',
   JSON_OBJECT(
     -- 由平台服务主动建连，服务端经该连接推送
     'inbound_mode',        'stream',
     -- 出站必须经持连进程：回复要透传回调的 req_id，只有收到它的那条
     -- 连接能做到。与 inbound_mode 分开声明，因为二者不可互推——
     -- Telegram 也主动建连，但回复走普通 HTTPS，任意 Worker 都能发。
     'outbound_mode',       'via_holder',
     -- aibot_send_msg 支持主动推送，前置条件是用户先发过消息
     'supports_push',       TRUE,
     -- 流式消息可反复刷新、卡片可更新，编辑是真实支持的
     'supports_edit',       TRUE,
     -- stream.content 上限 20480 字节，远高于回调模式的 2048
     'max_text_length',     20480,
     -- 官方限制：单会话 30 条/分钟、1000 条/小时
     'rate_limit_per_min',  30
   ),
   'active')
AS new
ON DUPLICATE KEY UPDATE
  capabilities    = new.capabilities,
  webhook_path    = new.webhook_path,
  external_app_id = new.external_app_id,
  secret_ref      = new.secret_ref,
  status          = new.status;

-- 用户映射。
-- external_user_id 是实测得到的加密 userid 形态（创建者非超管时下发的是
-- 「企业主体下的加密 userid」，见 docs/IM通道接入设计.md §9.2.1），
-- 不是明文 userid。平台无需特殊处理：channel_users 本就负责外部到内部的
-- 映射，且 uk_scope 以 channel_binding_id 起头，该标识天然隔离在本绑定内。
--
-- 缺这条映射不影响收发（一期直接用外部标识定位会话），但角色属性会缺，
-- 因此 role_checker 这条治理策略会拒绝——预置以便联调不被它挡住。
INSERT INTO channel_users
  (tenant_id, channel_binding_id, external_user_id, internal_user_id, display_name, attributes, status)
VALUES
  ('tenant-test', 'cb-test-aibot', 'T31560051A', 'u-test-1', '测试用户',
   JSON_OBJECT('roles', JSON_ARRAY('staff')), 'active')
AS new
ON DUPLICATE KEY UPDATE
  internal_user_id = new.internal_user_id,
  display_name = new.display_name,
  attributes = new.attributes;
