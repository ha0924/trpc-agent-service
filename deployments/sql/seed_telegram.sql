-- 多租户节点化 Agent 平台 —— Telegram 通道绑定
--
-- 设计依据：docs/IM通道接入设计.md §7.1「差异对比」
--
-- 这是平台的**第二类真实 IM 平台**，用于回答 README「至少两类 IM 通道」。
-- 企业微信的回调与长连接差异极大，但它们是同一个平台的两种接入方式；
-- Telegram 带来的是另一套约束。
--
-- 与企业微信两种形态的关键差异：
--
--   入站   getUpdates 长轮询，由平台服务主动发起，无回调地址、无需公网
--   出站   sendMessage 普通 HTTPS，任意 Worker 都能发
--   凭据   只有一个 bot token，无签名密钥、无 AES key
--   幂等键 update_id，是单调游标而非不透明 id
--   群聊   chat.id 为负数
--   编辑   原生支持编辑已发消息（企业微信两种形态都不支持）
--
-- **这条绑定是「入站与出站两个维度必须分开」的现实依据**：
-- inbound_mode=stream 表示要起 Run 循环并选主，
-- outbound_mode=direct 表示回复不走出站信箱。
-- 二者曾是一个字段，那时无法表达这个组合。
--
-- 凭据不入库：从 @BotFather 取到的 token 存在 Secret Manager，
-- 库中只留 secret_ref。Telegram 的 token 直接出现在 URL 路径里，
-- 因此它比企业微信的凭据更敏感——任何打印 URL 的日志都会泄漏它。
--
-- 执行顺序：须在 schema.sql 与 schema_governance.sql 之后。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

-- 复用 tenant-demo 的 assistant，不新建租户。
--
-- 与 seed_wecom_aibot.sql 的取舍不同，理由也不同：那条要接真实机器人、
-- 会产生真实用量，混进演示租户会污染隔离验证的结论；这条的价值在于
-- 「同一个 Agent 可同时经三类通道提供服务，而主流程对它们一视同仁」，
-- 挂在既有 Agent 上恰恰是在证明这一点。
INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-telegram', 'tenant-demo', 'assistant', 'prod', 'telegram',
   -- bot 的 username，便于运维核对。建连读的是 secret_ref，不依赖此列。
   'demo_agent_bot',
   -- 无回调地址：getUpdates 是平台主动发起的长轮询。
   -- uk_webhook 允许多行 NULL，故与 cb-test-aibot 并存不冲突。
   NULL,
   'secret://prod/tenant-demo/channel/telegram',
   JSON_OBJECT(
     -- 主动建连拉取，需要 Run 循环与 per-bot 选主：
     -- 两个副本同时轮询同一个 bot 会各拿到一部分 update，
     -- 把一段对话劈到两个进程里。
     'inbound_mode',        'stream',
     -- 但回复走普通 HTTPS，任意 Worker 都能发，不必经持连进程。
     -- 这正是与企业微信 aibot 的分野所在。
     'outbound_mode',       'direct',
     'supports_push',       TRUE,
     -- Telegram 可原地编辑已发消息
     'supports_edit',       TRUE,
     -- 单条上限 4096（UTF-16 码元），超出由平台侧按段落拆分
     'max_text_length',     4096,
     -- 官方指引约为每会话每秒 1 条、允许突发；30/分钟留足余量
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
-- Telegram 的 user id 是数字，与企业微信的加密 userid、Mock 的自定义串
-- 都不同——同一个人经三类通道会落到三条 channel_users 记录、三个会话，
-- 这正是 uk_scope 以 channel_binding_id 起头的原因。
--
-- 这里的 external_user_id 是占位值，联调时替换为真实 Telegram user id
-- （给 bot 发一条消息，日志里的 external_user_id 即是）。
INSERT INTO channel_users
  (tenant_id, channel_binding_id, external_user_id, internal_user_id, display_name, attributes, status)
VALUES
  ('tenant-demo', 'cb-telegram', '000000000', 'u-telegram-1', 'Telegram 用户',
   JSON_OBJECT('roles', JSON_ARRAY('staff')), 'active')
AS new
ON DUPLICATE KEY UPDATE
  internal_user_id = new.internal_user_id,
  display_name = new.display_name,
  attributes = new.attributes;
