-- 多租户节点化 Agent 平台 —— 治理与审计相关表
--
-- 设计依据：docs/数据模型设计.md §1.2「能力定义与授权」、§1.6「审计与治理记录」
--           docs/治理监控与安全设计.md
--
-- 与 schema.sql 分开，因为这批表服务的是治理与可观测，
-- 而非跑通一条消息链路所必需。分开也便于按环境选择是否启用。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

-- ---------------------------------------------------------------------------
-- 能力定义表
--
-- 定义表跨版本共享，绑定表按版本冻结。绑定表存的是「这个版本能用什么」，
-- 定义表存的是「平台提供了什么」，用于 Admin 页面渲染与下线时反查影响面。
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tool_definitions (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64)  NOT NULL COMMENT '空串表示平台级 Tool，对所有租户可见',
  tool_name    VARCHAR(128) NOT NULL,
  display_name VARCHAR(128) NULL,
  description  VARCHAR(512) NULL,
  category     VARCHAR(32)  NOT NULL DEFAULT 'query' COMMENT 'query / action，action 有副作用',
  danger_level VARCHAR(16)  NOT NULL DEFAULT 'low' COMMENT 'low / medium / high，high 需二次确认',
  schema_json  JSON         NULL COMMENT '入参 JSON Schema，供 Admin 渲染',
  status       VARCHAR(32)  NOT NULL DEFAULT 'active',
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_tool_def (tenant_id, tool_name),
  KEY idx_danger (danger_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Tool 定义';

-- 表示「租户级默认」时用空串而非 NULL，因 MySQL 唯一键不约束 NULL，
-- 用 NULL 会让同一租户能插入多行平台级定义。

CREATE TABLE IF NOT EXISTS extension_definitions (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id      VARCHAR(64)  NOT NULL COMMENT '空串表示平台级扩展',
  kind           VARCHAR(16)  NOT NULL COMMENT 'plugin / guardrail / callback',
  extension_name VARCHAR(128) NOT NULL,
  display_name   VARCHAR(128) NULL,
  description    VARCHAR(512) NULL,
  params_schema  JSON         NULL COMMENT '参数 JSON Schema',
  status         VARCHAR(32)  NOT NULL DEFAULT 'active',
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_ext_def (tenant_id, kind, extension_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Plugin / Guardrail / Callback 定义';

-- ---------------------------------------------------------------------------
-- 治理运行记录
-- ---------------------------------------------------------------------------

-- 危险 Tool 的二次确认记录。
-- 状态机 pending → approved / rejected / expired。
-- 记录必须在副作用发生之前写入：事后补记的审计在故障时正好缺失。
CREATE TABLE IF NOT EXISTS tool_approvals (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  approval_id    VARCHAR(64)  NOT NULL,
  tenant_id      VARCHAR(64)  NOT NULL,
  agent_app_id   VARCHAR(64)  NOT NULL,
  session_id     VARCHAR(64)  NOT NULL,
  request_id     VARCHAR(64)  NOT NULL,
  trace_id       VARCHAR(64)  NULL,
  tool_name      VARCHAR(128) NOT NULL,
  tool_args      JSON         NULL COMMENT '已脱敏的调用参数',
  requested_by   VARCHAR(64)  NULL COMMENT '触发调用的 IM 用户',
  decided_by     VARCHAR(64)  NULL,
  state          VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending / approved / rejected / expired / consumed',
  reason         VARCHAR(512) NULL,
  -- 绑定审批与「当时被展示的那组参数」。缺了它，批准「删除订单 123」
  -- 会顺带授权「删除订单 999」——guardrail 只会看到该工具有一条已批准记录。
  args_fingerprint VARCHAR(64) NULL COMMENT '参数摘要，使审批只对被审的那次调用有效',
  expires_at     DATETIME(3)  NULL,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_approval (approval_id),
  KEY idx_pending (tenant_id, state, created_at),
  KEY idx_session (session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='危险 Tool 二次确认';

-- 审计日志。字段对齐验收标准要求的 11 项：
-- tenant_id、channel、user_id、session_id、agent_name、tool_name、
-- decision、latency、error_type、cost、trace_id。
--
-- 回答的是「谁在什么时间、经由哪个通道、使用哪个 Agent 或 Tool、
-- 系统为什么允许或拒绝」。
CREATE TABLE IF NOT EXISTS audit_logs (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64)  NOT NULL,
  agent_app_id VARCHAR(64)  NULL,
  agent_name   VARCHAR(128) NULL COMMENT '实际执行的 Agent 标识，含版本',
  channel      VARCHAR(32)  NULL,
  user_id      VARCHAR(64)  NULL,
  session_id   VARCHAR(64)  NULL,
  request_id   VARCHAR(64)  NOT NULL,
  trace_id     VARCHAR(64)  NULL,
  event_type   VARCHAR(32)  NOT NULL COMMENT 'agent_run / tool_call / model_call / guardrail / delivery',
  tool_name    VARCHAR(128) NULL,
  decision     VARCHAR(16)  NOT NULL DEFAULT 'allow' COMMENT 'allow / deny / ask / error',
  reason       VARCHAR(512) NULL COMMENT '拒绝或需确认的原因',
  latency_ms   BIGINT       NULL,
  error_type   VARCHAR(64)  NULL,
  cost_usd     DECIMAL(12,6) NULL,
  detail       JSON         NULL COMMENT '已脱敏的补充信息',
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_tenant_time (tenant_id, created_at),
  KEY idx_trace (trace_id),
  KEY idx_decision (tenant_id, decision, created_at),
  KEY idx_tool (tenant_id, tool_name, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='审计日志';

-- Token 与成本明细，用于对账。
--
-- 判断租户是否超预算不查这张表：实时 SUM 明细代价高，且多个 Worker 并发扣减
-- 需要原子操作。计数器放 Redis 按租户与周期累加，本表只做事后核对。
-- 若缺少 Redis 计数器，预算限制会退化为「只统计不拦截」。
CREATE TABLE IF NOT EXISTS usage_records (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id         VARCHAR(64)  NOT NULL,
  agent_app_id      VARCHAR(64)  NOT NULL,
  agent_version     VARCHAR(64)  NULL,
  session_id        VARCHAR(64)  NULL,
  request_id        VARCHAR(64)  NOT NULL,
  trace_id          VARCHAR(64)  NULL,
  model_name        VARCHAR(128) NOT NULL,
  prompt_tokens     INT          NOT NULL DEFAULT 0,
  completion_tokens INT          NOT NULL DEFAULT 0,
  total_tokens      INT          NOT NULL DEFAULT 0,
  cost_usd          DECIMAL(12,6) NOT NULL DEFAULT 0,
  latency_ms        BIGINT       NULL,
  created_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_tenant_time (tenant_id, created_at),
  KEY idx_request (request_id),
  KEY idx_tenant_model (tenant_id, model_name, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Token 与成本明细';

-- ---------------------------------------------------------------------------
-- 通道用户映射
--
-- 一期 Mock 通道直接用 external_user_id 作内部用户。接入真实通道后需要映射，
-- 因为同一个人在不同通道下标识不同，且平台需要挂载角色与权限属性——
-- IM 用户权限校验读的就是 attributes。
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS channel_users (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id          VARCHAR(64)  NOT NULL,
  channel_binding_id VARCHAR(64)  NOT NULL,
  external_user_id   VARCHAR(128) NOT NULL,
  internal_user_id   VARCHAR(64)  NOT NULL,
  display_name       VARCHAR(128) NULL,
  attributes         JSON         NULL COMMENT '角色与权限判断依据',
  status             VARCHAR(32)  NOT NULL DEFAULT 'active',
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_channel_user (channel_binding_id, external_user_id),
  KEY idx_internal (tenant_id, internal_user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='外部 IM 用户到内部用户的映射';

-- ---------------------------------------------------------------------------
-- 审计策略：租户模型的第 7 个要素
--
-- 设计依据：docs/治理监控与安全设计.md §8「密钥与脱敏」
--
-- 为什么需要按租户配置，而不是全局一套：不同租户对「审计里能留多少原文」
-- 的要求相反。受监管行业要求留证据以便复查，注重隐私的租户要求正文一律
-- 不落库、只留哈希。写死任何一种都会让另一类租户无法使用。
--
-- 三个字段各自对应一种真实诉求：
--   redact_level  脱敏强度。none 只用于本地调试，生产最低 standard
--   body_mode     正文如何留存：full 留原文 / truncate 截断 / hash 只留哈希 / drop 不留
--   retention_days 保留天数，到期由对账任务清理
--
-- 注意：本表建好后必须被真正读取。本项目已四次出现「表和函数都在、
-- 零调用点」的情况（见 docs/完成度台账.md §八 #4 #7 #9 #12），
-- 因此审计写入路径会读它，而不是只建表。
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS audit_policies (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id      VARCHAR(64) NOT NULL,
  redact_level   VARCHAR(32) NOT NULL DEFAULT 'standard' COMMENT 'none / standard / strict',
  body_mode      VARCHAR(32) NOT NULL DEFAULT 'truncate' COMMENT 'full / truncate / hash / drop',
  body_max_chars INT         NOT NULL DEFAULT 512 COMMENT 'body_mode=truncate 时的截断长度',
  retention_days INT         NOT NULL DEFAULT 90,
  created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_audit_policy (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='租户级审计策略，租户模型第 7 要素';
