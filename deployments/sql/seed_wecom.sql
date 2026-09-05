-- 多租户节点化 Agent 平台 —— 企业微信通道绑定
--
-- 设计依据：docs/IM通道接入设计.md §7「通道接入差异」
--
-- 与 Mock 通道并存，用于验证「至少两种 IM 通道」及其接入差异。
-- 两条绑定指向同一个 Agent 版本，因此同一个 Agent 可同时经 Mock 与企业微信
-- 提供服务，而 Gateway 与 Worker 的主流程对二者一视同仁。
--
-- 凭据不入库：secret_ref 指向 Secret Manager 中的一个 JSON 凭据块，含
-- corp_id / secret / agent_id / token / encoding_aes_key。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-wecom', 'tenant-demo', 'assistant', 'prod', 'wecom',
   -- 企业微信侧的应用标识，即 AgentId
   '1000002',
   '/webhook/wecom/demo',
   'secret://prod/tenant-demo/channel/wecom',
   JSON_OBJECT(
     -- 回调自带消息内容（加密），与微信客服的 fetch 型不同
     'inbound_mode',        'payload',
     -- 支持主动推送，这是 ACK 后异步回复的前提；
     -- 被动响应窗口远短于一次 Agent 执行，必须走推送
     'supports_push',       TRUE,
     -- 企业微信可撤回但不能原地编辑
     'supports_edit',       FALSE,
     -- 文本消息体上限约 2048 字节，超出由平台侧按段落拆分
     'max_text_length',     2048,
     'rate_limit_per_min',  60
   ),
   'active')
AS new
ON DUPLICATE KEY UPDATE
  capabilities = new.capabilities,
  webhook_path = new.webhook_path,
  external_app_id = new.external_app_id,
  secret_ref = new.secret_ref,
  status = new.status;

-- 企业微信用户与内部用户的映射。
-- 企业微信的 FromUserName 是企业内的 UserId，与 Mock 通道的用户空间不同，
-- 因此同一个人在两个通道下会落到两个 channel_users 记录、两个会话——
-- 这正是 uk_scope 以 channel_binding_id 起头的原因。
INSERT INTO channel_users
  (tenant_id, channel_binding_id, external_user_id, internal_user_id, display_name, attributes, status)
VALUES
  ('tenant-demo', 'cb-wecom', 'zhangsan', 'u-zhangsan', '张三',
   JSON_OBJECT('roles', JSON_ARRAY('staff')), 'active')
AS new
ON DUPLICATE KEY UPDATE
  internal_user_id = new.internal_user_id,
  display_name = new.display_name,
  attributes = new.attributes;
