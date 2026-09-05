-- 多租户节点化 Agent 平台 —— 治理策略种子数据
--
-- 设计依据：docs/治理监控与安全设计.md §7.2「治理策略」
--
-- 为 tenant-demo/assistant/v1 启用五条治理策略，并演示租户级预算与角色限制。
-- 策略实现在代码中注册，本文件只决定「启用哪些、什么顺序、什么参数」——
-- 这正是拆出 agent_extension_bindings 的意义。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

-- ---------------------------------------------------------------------------
-- 扩展绑定：五条治理策略
--
-- priority 决定同类扩展的挂载顺序，值小者先执行。顺序不是摆设：
-- redaction 必须早于任何会持久化内容的策略，否则未脱敏的原文会被写进审计。
-- ---------------------------------------------------------------------------

INSERT INTO agent_extension_bindings
  (tenant_id, agent_app_id, version, kind, extension_name, enabled, priority, params)
VALUES
  -- 脱敏最先，保证后续策略看到的都是已清洗内容
  ('tenant-demo', 'assistant', 'v1', 'guardrail', 'redaction', 1, 10, NULL),

  -- 工具白名单：未绑定或 deny 的工具直接拒绝
  ('tenant-demo', 'assistant', 'v1', 'guardrail', 'tool_whitelist', 1, 20, NULL),

  -- 危险工具二次确认：仅对 mode=ask 的工具生效，审计写在副作用之前
  ('tenant-demo', 'assistant', 'v1', 'guardrail', 'dangerous_tool_approval', 1, 30,
   JSON_OBJECT('approval_ttl', '10m')),

  -- 预算限制：超额拦截而非仅统计
  ('tenant-demo', 'assistant', 'v1', 'guardrail', 'budget_limit', 1, 40, NULL),

  -- IM 用户权限校验。allow_unmapped 为 true，使未建立映射的租户不会被一键锁死；
  -- 真正在意的租户改为 false。
  ('tenant-demo', 'assistant', 'v1', 'guardrail', 'user_permission', 1, 50,
   JSON_OBJECT('allow_unmapped', TRUE)),

  -- 日志类 Callback 放最后，记录的是已经过治理的调用
  ('tenant-demo', 'assistant', 'v1', 'callback', 'request_logger', 1, 90, NULL)
AS new
ON DUPLICATE KEY UPDATE
  enabled = new.enabled, priority = new.priority, params = new.params;

-- ---------------------------------------------------------------------------
-- 一个危险 Tool，用于演示二次确认
--
-- 只加绑定不加实现：mode=ask 的工具在装配时仍会被 Resolve 查找，
-- 因此需要先在代码里注册同名 Tool。此处保持注释，待 delete_order 实现后启用。
-- ---------------------------------------------------------------------------

-- INSERT INTO agent_tool_bindings (tenant_id, agent_app_id, version, tool_name, mode, params)
-- VALUES ('tenant-demo', 'assistant', 'v1', 'delete_order', 'ask', NULL)
-- AS new ON DUPLICATE KEY UPDATE mode = new.mode;

-- ---------------------------------------------------------------------------
-- Tool 定义表：供 Admin 页面渲染与下线时反查影响面
-- ---------------------------------------------------------------------------

INSERT INTO tool_definitions
  (tenant_id, tool_name, display_name, description, category, danger_level, status)
VALUES
  ('', 'calculator', '计算器', '四则运算', 'query', 'low', 'active'),
  ('', 'search',     '搜索',   '按关键词检索信息', 'query', 'low', 'active'),
  ('', 'delete_order', '删除订单', '删除指定订单，操作不可逆', 'action', 'high', 'active')
AS new
ON DUPLICATE KEY UPDATE
  display_name = new.display_name, description = new.description,
  category = new.category, danger_level = new.danger_level;

-- tenant_id 为空串表示平台级定义，对所有租户可见。
-- 用空串而非 NULL，因为 MySQL 唯一键不约束 NULL，用 NULL 会允许重复插入。

-- ---------------------------------------------------------------------------
-- 扩展定义表
-- ---------------------------------------------------------------------------

INSERT INTO extension_definitions
  (tenant_id, kind, extension_name, display_name, description, status)
VALUES
  ('', 'guardrail', 'redaction',               '内容脱敏',     '清除进出模型内容中的凭据', 'active'),
  ('', 'guardrail', 'tool_whitelist',          '工具白名单',   '拒绝未授权的工具调用',   'active'),
  ('', 'guardrail', 'dangerous_tool_approval', '危险工具确认', '高风险工具执行前需确认', 'active'),
  ('', 'guardrail', 'budget_limit',            '预算限制',     '超出租户用量额度时拦截', 'active'),
  ('', 'guardrail', 'user_permission',         'IM用户权限',   '按角色限制可用的用户',   'active'),
  ('', 'callback',  'request_logger',          '请求日志',     '记录模型调用耗时与用量', 'active')
AS new
ON DUPLICATE KEY UPDATE
  display_name = new.display_name, description = new.description;

-- ---------------------------------------------------------------------------
-- 租户级策略：预算与角色
--
-- 额度刻意设得较大，使常规演示不会被拦；把 daily_token_budget 调成 1
-- 即可现场演示预算拦截。
-- ---------------------------------------------------------------------------

UPDATE tenants
   SET settings = JSON_OBJECT(
         'daily_token_budget',     1000000,
         'monthly_token_budget',   20000000,
         'max_tokens_per_request', 4096,
         'rate_limit_per_min',     60
       )
 WHERE tenant_id = 'tenant-demo';

-- allowed_roles 留空表示不限制角色。演示 IM 用户权限校验时，
-- 把它设为 ["staff"] 并在 channel_users 中给用户打上角色。

-- ---------------------------------------------------------------------------
-- 通道用户映射：演示身份映射与角色
-- ---------------------------------------------------------------------------

INSERT INTO channel_users
  (tenant_id, channel_binding_id, external_user_id, internal_user_id, display_name, attributes, status)
VALUES
  ('tenant-demo', 'cb-demo', 'alice', 'u-alice', 'Alice',
   JSON_OBJECT('roles', JSON_ARRAY('staff', 'admin')), 'active'),
  ('tenant-demo', 'cb-demo', 'bob',   'u-bob',   'Bob',
   JSON_OBJECT('roles', JSON_ARRAY('staff')), 'active'),
  ('tenant-demo', 'cb-demo', 'guest', 'u-guest', 'Guest',
   JSON_OBJECT('roles', JSON_ARRAY('visitor')), 'active')
AS new
ON DUPLICATE KEY UPDATE
  internal_user_id = new.internal_user_id,
  display_name = new.display_name,
  attributes = new.attributes,
  status = new.status;
