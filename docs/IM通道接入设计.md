# IM 通道接入设计

本文说明外部 IM 平台如何接入平台，包括 Channel 抽象、统一消息模型、入站与出站流程、身份与会话映射，以及不同通道的接入差异。

对应主文档 `技术设计方案.md` 第 6 章，本文是其详细展开。

## 1. 设计目标

IM 接入不等同于 HTTP Chat API。它需要额外解决五个问题：

1. **消息会重复投递** —— IM 平台在未收到 ACK 时会重试，同一条消息可能到达多次；
2. **响应有硬性超时** —— 被动响应通道通常要求秒级返回，而 Agent 执行可能耗时数十秒；
3. **入站形态不统一** —— 有的回调自带消息内容，有的回调只是通知、需要再拉取；
4. **出站有平台限制** —— 消息长度上限、发送频率上限、凭据有效期；
5. **身份是外部的** —— 外部用户标识不能直接当作平台内部用户，且同一个人在不同通道下标识不同。

平台的做法是：**把差异收敛到 Channel 实现与能力描述符里，让 Gateway 与 Worker 的主流程对所有通道一致。**

## 2. Channel 抽象

### 2.1 复用并扩展 openclaw 的 Channel 模型

tRPC-Agent-Go 的 `openclaw/channel` 包定义了通道的最小接口：

```go
// trpc.group/trpc-go/trpc-agent-go/openclaw/channel
type Channel interface {
    ID() string
    Run(ctx context.Context) error
}

type TextSender interface {
    Channel
    SendText(ctx context.Context, target string, text string) error
}

type MessageSender interface {
    Channel
    SendMessage(ctx context.Context, target string, msg OutboundMessage) error
}
```

该接口面向**单用户本地 Runtime**设计，不含租户、会话与能力描述概念。平台在其上扩展：

```go
// 平台侧 Channel：内嵌 openclaw 接口，补齐多租户平台所需的能力
type Channel interface {
    openclawchannel.Channel        // 复用：ID() / Run()

    // 入站（Gateway 侧）
    Verify(r *http.Request, binding *ChannelBinding) error   // 验签
    Decode(r *http.Request, binding *ChannelBinding) ([]InboundMessage, error)
    Ack(w http.ResponseWriter) error                          // 立即 ACK

    // 出站（Worker 侧）
    openclawchannel.TextSender

    // 能力描述
    Capabilities() Capabilities
}
```

扩展的四点理由：

| 扩展项 | 原因 |
|---|---|
| `Verify` / `Decode` 分离 | 各通道验签算法与报文格式不同，但幂等与路由逻辑必须统一 |
| `Ack` 独立于回复 | 被动响应通道要求秒级 ACK，回复走异步推送 |
| `Capabilities()` | 长度、频率、是否支持推送等差异需要驱动主流程分支，而非写死在实现里 |
| 绑定上下文入参 | openclaw 的 Channel 一个实例对应一个账号；平台一个实现服务多个租户的多个绑定 |

### 2.2 入站与出站分属两个进程

```text
入站  Gateway   接收回调 → 验签解密 → 转统一消息 → 幂等 → 入队
出站  Worker    Agent 执行完成 → 调用主动推送接口回复用户
```

划分依据：IM 平台需要一个公网地址投递回调，只有 Gateway 对外暴露；而回复时机与内容只有 Worker 知晓。因此 Channel 接口天然分为入站与出站两组方法。

## 3. 统一消息模型

不同通道的报文格式差异很大，进入平台后统一为一种结构。平台复用 `openclaw/gwproto` 已定义的内容部件类型，在其上补充租户与通道维度：

```go
type InboundMessage struct {
    // 通道与租户维度（平台扩展）
    Channel          string   // wecom / wechat_kf / telegram / mock
    ChannelBindingID string
    TenantID         string
    AgentAppID       string

    // 身份与会话
    ExternalUserID   string
    ExternalGroupID  string   // 群聊时非空
    Scope            string   // single / group

    // 幂等
    ExternalEventID  string   // 通道侧事件或消息标识

    // 内容：复用 gwproto.ContentPart
    Text     string
    Parts    []gwproto.ContentPart   // text / image / audio / file / location / link

    // 链路
    RequestID string
    TraceID   string
    ReceivedAt time.Time
}
```

复用 `gwproto.ContentPart` 的收益是图片、音频、文件、位置、链接等富媒体类型不必重新定义，且与框架的 `model.Message` 转换路径一致。

出站方向复用 `openclaw/channel.OutboundMessage`（`Text` + `Files`），必要时按通道能力降级。

### 4. 与框架输入输出的转换

```text
InboundMessage
  → model.Message{Role: user, Content: text 或 ContentParts}
  → runner.Run(ctx, userID, sessionID, message)
  → <-chan *event.Event
  → 逐个 Event 交给输出函数
  → 聚合为 OutboundMessage
  → Channel.SendText / SendMessage
```

Worker **逐个消费 Event 并交给输出函数**，不攒完整个回复再返回。当前输出函数做缓冲后一次性发送；若后续需要分段推送或消息编辑模拟流式，只替换该函数，不改调用结构。

## 5. 入站流程

```text
IM 平台回调到达 Gateway
→ 按 webhook_path 反查 channel_bindings，得到 tenant_id / agent_app_id / secret_ref
→ Channel.Verify   验签，失败直接拒绝
→ Channel.Decode   解密并解析为 InboundMessage（fetch 型通道在此拉取消息）
→ 定位或创建 session
→ 幂等记录落库（inbound_events，uk_event 拦截重复）
→ Channel.Ack      立即返回 ACK
→ 写 Session 信箱 → 投递队列
```

三步顺序固定为**幂等记录落库 → ACK → 入队**：

- 先落库：即使入队失败也有可靠记录可重放；
- 再 ACK：避免 IM 平台因超时重投；
- 后入队：入队失败可由扫描 `processing` 状态的记录补偿。

`uk_event (channel_binding_id, external_event_id)` 是重复投递的硬拦截：同一事件第二次到达时插入失败，Gateway 直接返回 ACK 而不再执行 Agent。

## 6. 身份与会话映射

### 6.1 用户映射

外部用户标识不直接作为内部用户：

```text
channel_binding_id + external_user_id → internal_user_id
```

映射记录存于 `channel_users`。这样做的原因有三：同一个人在不同通道下标识不同；外部标识可能变化（如企业微信的 `external_userid` 随应用不同而不同）；平台需要挂载角色与权限属性。

### 6.2 会话定位

```text
单聊：channel_binding_id + external_user_id  → session
群聊：channel_binding_id + external_group_id → session
```

两者统一为 `sessions` 表的 `scope`（`single` / `group`）与 `scope_key` 两列，由唯一键 `uk_scope (channel_binding_id, scope, scope_key)` 约束。

隔离性由此直接成立：

- **跨租户隔离** —— `channel_binding_id` 已归属某个租户，不同租户的绑定不同，会话必然不同；
- **跨群隔离** —— 同一用户在不同群聊中 `external_group_id` 不同，会话不同；
- **群聊与单聊隔离** —— `scope` 不同，会话不同。

群聊会话不记录 `internal_user_id`，因为参与者是多人；发言者身份记录在 `session_events` 的事件内容中。

### 6.3 会话与版本

创建会话时按 Deployment 权重选定 `agent_version` 并写入 `sessions`，此后该会话固定使用该版本，不随发布变化。

## 7. 通道接入差异

平台通过能力描述符吸收差异，主流程不做通道分支。以下为三类通道的实际差异。

### 7.1 差异对比

| 维度 | 企业微信（自建应用） | 微信客服 | Telegram |
|---|---|---|---|
| 入站形态 | 回调自带消息内容 | **回调仅通知，需再拉取** | 回调自带内容，或主动轮询 |
| `inbound_mode` | `payload` | `fetch` | `payload` |
| URL 验证 | GET 请求返回解密后的 `echostr` | 同企业微信 | 无，或校验 secret token |
| 验签 | `msg_signature` = SHA1(排序拼接 token、timestamp、nonce、密文) | 同企业微信 | Webhook 模式校验请求头中的 secret token |
| 报文加密 | **AES-256-CBC，必须解密** | 同企业微信 | 无加密，HTTPS 传输 |
| 出站凭据 | `access_token`，由 corpid + secret 换取，**有效期约 2 小时需缓存** | 同企业微信 | Bot Token 长期有效，**无需换取** |
| 被动响应 | 支持，但有秒级超时 | 不适用 | 不适用 |
| 主动推送 | 支持 | 支持 | 支持 |
| 会话标识来源 | `FromUserName`（单聊）/ 应用与外部群标识 | `external_userid` + `open_kfid` | `chat_id`，群聊为负数 |
| 文本长度上限 | 约 2048 字节 | 依接口而定 | 约 4096 字符 |
| 频率限制 | 按应用与租户维度限制 | 按客服账号限制 | 按 bot 与会话限制 |
| 撤回/编辑 | 支持撤回，不支持编辑 | 有限 | **支持编辑已发送消息** |

具体数值以各平台官方文档为准，平台侧一律写入 `channel_bindings.capabilities`，不硬编码。

### 7.2 差异如何影响实现

**① `payload` 与 `fetch` 的分叉是最大的结构差异。**

微信客服的回调只携带一个通知令牌，消息内容必须再调用同步接口拉取，且接口是**游标式**的：

```text
payload 型：Decode 从回调报文直接解析出消息
fetch  型：Decode 内部调用同步接口，按游标拉取，可能一次返回多条
```

因此 `Decode` 的签名返回 `[]InboundMessage` 而非单条 —— 这个设计正是为 fetch 型通道预留的。游标需持久化，否则重启会重复拉取或漏拉。

**② 加密通道必须先解密再幂等。**

企业微信与微信客服的报文是密文，`external_event_id` 在密文里。因此顺序必须是「验签 → 解密 → 取事件 ID → 幂等」，不能对密文做幂等。

**③ `access_token` 的缓存与并发。**

企业微信的凭据有效期约 2 小时，需缓存在 Redis 并按 `channel_binding_id` 隔离。多个 Worker 同时发现过期时会并发刷新，需要加锁避免互相顶掉——后取到的令牌会使先取到的失效。

**④ 被动响应超时决定了必须异步。**

企业微信的被动响应有秒级超时，而 Agent 执行常远超该时限。因此平台一律：**回调时立即 ACK（可回空串表示暂不回复），执行完成后走主动推送**。这也是 Gateway 与 Worker 必须异步解耦的根本原因之一。

`capabilities.supports_push` 为 false 的通道无法采用该模式，需退化为同步等待并承担超时风险；当前接入的通道均支持推送。

### 7.3 一期与后续

一期实现 Mock Channel（HTTP 收发），但接口与能力描述符按真实通道的形状定义，保证接入真实通道时不调整结构。

Telegram 可复用 `openclaw/plugins/telegram` 的实现并适配到平台 Channel 接口；企业微信为平台自研。二者一复用一自研，正好覆盖 `payload` 型与凭据换取两类差异。

## 8. 出站回复

### 8.1 流程

```text
Worker 完成 Agent 执行
→ 读取 channel_bindings.capabilities
→ 按 max_text_length 拆分长文本
→ 按 rate_limit_per_min 限流
→ 获取出站凭据（如需）
→ Channel.SendText / SendMessage
→ 更新 inbound_events.state
```

### 8.2 长度与拆分

超过 `max_text_length` 的回复按段落边界拆分为多条依次发送，不截断。拆分在平台侧统一处理，Channel 实现不各自为政。

### 8.3 限频

按 `channel_binding_id` 维度在 Redis 中做计数限流。触发限流时排队而非丢弃，因为 Agent 已经执行完成，丢弃等于用户收不到回复。

### 8.4 投递失败与重试

关键约束：**Agent 已执行完成但回复未送达时，只重试投递，不重新执行 Agent 与 Tool。**

这由 `inbound_events.state` 的状态机保证：

```text
processing        执行中
→ succeeded       执行完成且投递成功
→ delivery_failed 执行完成但投递失败   ← 只重试投递
→ failed          执行失败
```

若不区分这两个阶段，一次投递失败会导致 Agent 与 Tool 重跑，对有副作用的 Tool 是严重问题。

重试采用有限次数加退避。多次失败后置为终态并记录，由对账流程处理，不无限重试占用资源。

### 8.5 富媒体降级

回复包含图片或文件而目标通道不支持时，按能力降级为文本加链接，而非直接失败。降级策略由 `capabilities` 驱动。

## 9. 设计要点汇总

1. Channel 接口内嵌 `openclaw/channel.Channel`，扩展验签、解码、ACK 与能力描述；
2. 统一消息模型复用 `gwproto.ContentPart`，补充租户与通道维度；
3. 入站顺序固定为幂等落库 → ACK → 入队；
4. 幂等键为 `channel_binding_id + external_event_id`，加密通道需先解密再取键；
5. `Decode` 返回消息切片，以支持 fetch 型通道一次拉取多条；
6. 会话由 `channel_binding_id + scope + scope_key` 定位，跨租户、跨群、群聊与单聊天然隔离；
7. 一律 ACK 后异步推送，不依赖被动响应；
8. 通道差异全部收敛到 `capabilities`，主流程无通道分支；
9. 投递失败只重试投递，不重跑 Agent 与 Tool。
