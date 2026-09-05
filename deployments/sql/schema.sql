-- 多租户节点化 Agent 平台 —— 建表脚本
--
-- 设计依据：docs/数据模型设计.md §5「核心表结构」
-- 通用约定见同文档 §2：
--   - 物理主键统一自增 BIGINT UNSIGNED，业务标识另立 VARCHAR 唯一键
--   - 除 tenants 外所有表携带 tenant_id，索引以 tenant_id 起头
--   - 枚举用 VARCHAR 而非 ENUM，便于增加取值不改表
--   - 不使用外键约束，关联由应用层保证
--   - 字符集 utf8mb4，时间列统一 DATETIME(3)
--
-- 幂等：可重复执行。

CREATE DATABASE IF NOT EXISTS trpc_agent_platform
  DEFAULT CHARSET = utf8mb4
  DEFAULT COLLATE = utf8mb4_general_ci;

USE trpc_agent_platform;

-- ---------------------------------------------------------------------------
-- 1. 租户与配置
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenants (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id  VARCHAR(64)  NOT NULL COMMENT '业务标识',
  name       VARCHAR(128) NOT NULL,
  status     VARCHAR(32)  NOT NULL DEFAULT 'active' COMMENT 'active / suspended',
  settings   JSON         NULL COMMENT '预留：预算、限流等租户级策略',
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='租户';

CREATE TABLE IF NOT EXISTS agent_apps (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64)  NOT NULL,
  agent_app_id VARCHAR(64)  NOT NULL,
  name         VARCHAR(128) NOT NULL,
  description  VARCHAR(512) NULL,
  status       VARCHAR(32)  NOT NULL DEFAULT 'active',
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_app (tenant_id, agent_app_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Agent 应用';

-- 已发布版本不可修改。修改 Prompt、模型、Tool、扩展、MCP、Skill 均创建新版本。
-- 模型配置作为列而非独立表：一个版本恰好一份的定长配置，独立成表只增加装配查询次数。
CREATE TABLE IF NOT EXISTS agent_versions (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id         VARCHAR(64)  NOT NULL,
  agent_app_id      VARCHAR(64)  NOT NULL,
  version           VARCHAR(64)  NOT NULL COMMENT '如 v1、v2',
  status            VARCHAR(32)  NOT NULL DEFAULT 'draft' COMMENT 'draft / published / archived',
  system_prompt     TEXT         NULL,
  model_name        VARCHAR(128) NOT NULL,
  model_api_key_ref VARCHAR(255) NULL COMMENT '密钥引用，不存明文',
  model_params      JSON         NULL COMMENT 'temperature、max_tokens 等调用参数',
  description       VARCHAR(512) NULL,
  published_at      DATETIME(3)  NULL,
  created_by        VARCHAR(64)  NULL,
  created_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_version (tenant_id, agent_app_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Agent 版本配置';

-- 一行代表一个环境，各版本权重存于 routes。调权重与回滚均为单行更新，
-- 天然原子，不会出现权重之和不为 100 的中间态。
CREATE TABLE IF NOT EXISTS agent_deployments (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64) NOT NULL,
  agent_app_id VARCHAR(64) NOT NULL,
  env          VARCHAR(32) NOT NULL DEFAULT 'prod',
  routes       JSON        NOT NULL COMMENT '[{"version":"v1","weight":90},{"version":"v2","weight":10}]',
  strategy     JSON        NULL COMMENT '预留：按租户、通道、用户或标签灰度',
  updated_by   VARCHAR(64) NULL,
  created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_deploy (tenant_id, agent_app_id, env)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='环境发布与灰度权重';

-- ---------------------------------------------------------------------------
-- 2. 能力绑定
--
-- Tool、扩展、MCP、Skill 四类拆为独立绑定表而非 agent_versions 的单列 JSON：
-- 四类均为不定长列表且会持续增长，合并存储会使该列不断膨胀，而每次装配
-- Runtime 都需整列读出解析。拆表的多次查询由 Runtime 缓存吸收——版本不可变，
-- 缓存不失效，查询只发生在首次装配。
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS agent_tool_bindings (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64)  NOT NULL,
  agent_app_id VARCHAR(64)  NOT NULL,
  version      VARCHAR(64)  NOT NULL,
  tool_name    VARCHAR(128) NOT NULL,
  mode         VARCHAR(16)  NOT NULL DEFAULT 'allow' COMMENT 'allow / deny / ask',
  params       JSON         NULL COMMENT '该版本对此 Tool 的超时、限流等覆盖配置',
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_tool (tenant_id, agent_app_id, version, tool_name),
  KEY idx_tool_name (tool_name) COMMENT '反查哪些版本在使用某个 Tool'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='版本可用的 Tool';

-- kind 决定挂载到框架的哪一类扩展位，extension_name 对应代码中注册的实现名。
-- 实现不入表，因此新增一种扩展只需注册实现，不改表也不改装配逻辑。
-- priority 决定同类扩展执行顺序：脱敏须在审计记录之前，顺序不能依赖插入次序。
CREATE TABLE IF NOT EXISTS agent_extension_bindings (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id      VARCHAR(64)  NOT NULL,
  agent_app_id   VARCHAR(64)  NOT NULL,
  version        VARCHAR(64)  NOT NULL,
  kind           VARCHAR(16)  NOT NULL COMMENT 'plugin / guardrail / callback',
  extension_name VARCHAR(128) NOT NULL,
  enabled        TINYINT      NOT NULL DEFAULT 1,
  priority       INT          NOT NULL DEFAULT 0 COMMENT '同类扩展的挂载顺序，值小者先执行',
  params         JSON         NULL,
  created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_ext (tenant_id, agent_app_id, version, kind, extension_name),
  KEY idx_ext_name (kind, extension_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='版本启用的 Plugin、Guardrail、Callback';

CREATE TABLE IF NOT EXISTS agent_mcp_bindings (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64)  NOT NULL,
  agent_app_id VARCHAR(64)  NOT NULL,
  version      VARCHAR(64)  NOT NULL,
  server_name  VARCHAR(128) NOT NULL COMMENT '关联 mcp_servers',
  enabled      TINYINT      NOT NULL DEFAULT 1,
  tool_filter  JSON         NULL COMMENT '仅暴露该 MCP 的部分 Tool 时使用',
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_mcp (tenant_id, agent_app_id, version, server_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='版本使用的 MCP 服务';

CREATE TABLE IF NOT EXISTS agent_skill_bindings (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id    VARCHAR(64)  NOT NULL,
  agent_app_id VARCHAR(64)  NOT NULL,
  version      VARCHAR(64)  NOT NULL,
  skill_name   VARCHAR(128) NOT NULL COMMENT '关联 skills',
  enabled      TINYINT      NOT NULL DEFAULT 1,
  params       JSON         NULL,
  created_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_skill (tenant_id, agent_app_id, version, skill_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='版本挂载的 Skill';

-- ---------------------------------------------------------------------------
-- 3. 通道与身份
-- ---------------------------------------------------------------------------

-- webhook_path 唯一，Gateway 据请求路径反查绑定，从而确定租户与 Agent。
-- 入站报文是不可信输入，租户身份只能来自绑定，不能来自报文自称。
CREATE TABLE IF NOT EXISTS channel_bindings (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  channel_binding_id VARCHAR(64)  NOT NULL,
  tenant_id          VARCHAR(64)  NOT NULL,
  agent_app_id       VARCHAR(64)  NOT NULL,
  env                VARCHAR(32)  NOT NULL DEFAULT 'prod',
  channel            VARCHAR(32)  NOT NULL COMMENT 'mock / wecom / wechat_kf / telegram',
  external_app_id    VARCHAR(128) NULL COMMENT '通道侧应用标识',
  webhook_path       VARCHAR(255) NULL,
  secret_ref         VARCHAR(255) NULL COMMENT '密钥引用，不存明文',
  capabilities       JSON         NULL COMMENT '能力描述符，结构见数据模型设计 §6',
  status             VARCHAR(32)  NOT NULL DEFAULT 'active',
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_binding (channel_binding_id),
  UNIQUE KEY uk_webhook (webhook_path),
  KEY idx_tenant_app (tenant_id, agent_app_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='IM 账号与租户、Agent 绑定';

-- ---------------------------------------------------------------------------
-- 4. 运行数据
-- ---------------------------------------------------------------------------

-- uk_scope 直接对应会话定位规则：单聊由 channel_binding_id + external_user_id
-- 定位，群聊由 channel_binding_id + external_group_id 定位，统一到
-- scope 与 scope_key 两列。跨租户、跨群、群聊与单聊由此天然隔离。
CREATE TABLE IF NOT EXISTS sessions (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  session_id         VARCHAR(64)  NOT NULL,
  tenant_id          VARCHAR(64)  NOT NULL,
  agent_app_id       VARCHAR(64)  NOT NULL,
  agent_version      VARCHAR(64)  NOT NULL COMMENT '创建时固化，此后不随发布变化',
  channel            VARCHAR(32)  NOT NULL,
  channel_binding_id VARCHAR(64)  NOT NULL,
  scope              VARCHAR(16)  NOT NULL COMMENT 'single / group',
  scope_key          VARCHAR(128) NOT NULL COMMENT '单聊为 external_user_id，群聊为 external_group_id',
  internal_user_id   VARCHAR(64)  NULL COMMENT '群聊为空',
  last_sequence      BIGINT UNSIGNED NOT NULL DEFAULT 0,
  status             VARCHAR(32)  NOT NULL DEFAULT 'active',
  last_active_at     DATETIME(3)  NULL,
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_session (session_id),
  UNIQUE KEY uk_scope (channel_binding_id, scope, scope_key),
  KEY idx_tenant_active (tenant_id, last_active_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话元数据';

-- uk_seq 是顺序正确性的硬保证：同一会话的相同 sequence 无法写入两次。
-- 租约失效导致的并发写会在数据库层直接失败，而非产生错乱数据。
CREATE TABLE IF NOT EXISTS session_events (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id     VARCHAR(64) NOT NULL,
  session_id    VARCHAR(64) NOT NULL,
  sequence      BIGINT UNSIGNED NOT NULL COMMENT '会话内单调递增',
  event_type    VARCHAR(32) NOT NULL COMMENT 'user_message / agent_message / tool_call / tool_result / system',
  role          VARCHAR(16) NULL,
  content       JSON        NULL,
  request_id    VARCHAR(64) NOT NULL,
  trace_id      VARCHAR(64) NULL,
  agent_version VARCHAR(64) NULL,
  created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_seq (session_id, sequence),
  KEY idx_request (request_id),
  KEY idx_tenant_time (tenant_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话事件';

-- 入站幂等记录。写入顺序为先落库、再回 ACK、最后入队，
-- 因此本表也是在途请求的唯一可靠记录。
-- state 区分执行与投递两阶段：delivery_failed 表示 Agent 已执行完成但回复未送达，
-- 该状态只重试投递，不重新执行 Agent 与 Tool。
CREATE TABLE IF NOT EXISTS inbound_events (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  tenant_id          VARCHAR(64)  NOT NULL,
  channel_binding_id VARCHAR(64)  NOT NULL,
  external_event_id  VARCHAR(128) NOT NULL COMMENT '通道侧事件或消息标识',
  request_id         VARCHAR(64)  NOT NULL,
  trace_id           VARCHAR(64)  NULL,
  session_id         VARCHAR(64)  NULL COMMENT '识别会话后回填',
  payload            JSON         NULL COMMENT '标准化后的统一消息，供重放使用',
  state              VARCHAR(32)  NOT NULL COMMENT 'processing / succeeded / delivery_failed / failed',
  attempts           INT          NOT NULL DEFAULT 0,
  last_error         VARCHAR(512) NULL,
  created_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_event (channel_binding_id, external_event_id),
  KEY idx_state (state, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='入站幂等记录';
