-- 多租户节点化 Agent 平台 —— 企业微信智能机器人（长连接）通道绑定
--
-- 设计依据：docs/IM通道接入设计.md §9「长连接接入模式」
--
-- 与 seed_wecom.sql 的 URL 回调绑定并存，二者是**方向相反**的接入模型：
--
--   cb-wecom        回调：企业微信 → 平台服务，需公网地址，报文加密
--   cb-wecom-aibot  长连接：平台服务 → 企业微信，无需公网地址，报文明文
--
-- 三处与回调绑定的关键差异：
--
--   1. webhook_path 为 NULL —— 长连接没有回调地址，消息经 WebSocket 推送。
--      这也意味着 Gateway 的 catch-all 路由永远不会命中这条绑定。
--   2. secret_ref 指向的凭据块结构不同 —— 含 bot_id / secret，
--      而非 corp_id / secret / agent_id / token / encoding_aes_key。
--   3. inbound_mode 为 stream —— Gateway 据此决定为它启动 Channel.Run()，
--      Worker 据此决定把回复投到出站信箱而非直接调接口。
--
-- 凭据不入库：secret_ref 指向 Secret Manager 中的 JSON 块，含 bot_id 与 secret。
--
-- 幂等：可重复执行。

USE trpc_agent_platform;

INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES
  ('cb-wecom-aibot', 'tenant-demo', 'assistant', 'prod', 'wecom_aibot',
   -- 智能机器人的 BotID，形如 aib_xxx。放在 external_app_id 便于运维核对，
   -- 真正用于建连的 bot_id 仍从 secret_ref 读取，不依赖这一列。
   'aib-demo-0001',
   -- 长连接无回调地址。
   NULL,
   'secret://prod/tenant-demo/channel/wecom-aibot',
   JSON_OBJECT(
     -- 由平台服务主动建连，服务端经该连接推送；
     -- 这个取值同时决定了入站要不要起 Run() 与出站走不走出站信箱
     'inbound_mode',        'stream',
     -- 支持主动推送（aibot_send_msg），但前置条件是用户先发过消息
     'supports_push',       TRUE,
     -- 流式消息可反复刷新，模板卡片可更新，因此编辑是真实支持的
     'supports_edit',       TRUE,
     -- stream.content 上限 20480 字节，远高于回调模式的 2048
     'max_text_length',     20480,
     -- 官方限制：单会话 30 条/分钟、1000 条/小时
     'rate_limit_per_min',  30
   ),
   'active')
AS new
ON DUPLICATE KEY UPDATE
  capabilities = new.capabilities,
  webhook_path = new.webhook_path,
  external_app_id = new.external_app_id,
  secret_ref = new.secret_ref,
  status = new.status;

-- 智能机器人的 userid 空间与自建应用的 FromUserName 未必相同：
-- 文档说明创建者非超管时下发的是「企业主体下的加密 userid」。
-- 因此这里是独立的 channel_users 记录，同一个人在两条绑定下会落到
-- 两个内部用户与两个会话——正是 uk_scope 以 channel_binding_id 起头的原因。
INSERT INTO channel_users
  (tenant_id, channel_binding_id, external_user_id, internal_user_id, display_name, attributes, status)
VALUES
  ('tenant-demo', 'cb-wecom-aibot', 'aibot-zhangsan', 'u-zhangsan', '张三',
   JSON_OBJECT('roles', JSON_ARRAY('staff')), 'active')
AS new
ON DUPLICATE KEY UPDATE
  internal_user_id = new.internal_user_id,
  display_name = new.display_name,
  attributes = new.attributes;
