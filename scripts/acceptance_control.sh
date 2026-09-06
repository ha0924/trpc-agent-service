#!/usr/bin/env bash
# 管理面验收：只用 HTTP 接口走完一个租户从无到有的全过程。
#
# 这是第一批开发（管理面写入）的验收标准，也是对任务描述第一句的直接检验：
# 「平台需要支持多个租户创建和部署自己的 Agent」。在此之前建租户、建 Agent、
# 发版本都只能手写 SQL——「部署」有了，「创建」不存在。
#
# 因此本脚本刻意**一句 SQL 都不用**。全程只发 HTTP 请求，最后真的发一条消息
# 拿到回复：如果链路里还有任何一步依赖手写 SQL，这个脚本就跑不到底。
#
# 前置：Gateway 已启动（MySQL 与 Redis 就绪，schema 已导入）。
#
#   ./scripts/acceptance_control.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GW=http://127.0.0.1:8080
ADMIN="$GW/admin"

# 随机后缀，使脚本可重复执行而不与上一次的数据冲突。
SUF="$(date +%s)$RANDOM"
TENANT="acc-t-$SUF"
AGENT="acc-a-$SUF"
BINDING="acc-cb-$SUF"
WEBHOOK="/webhook/mock/$BINDING"
TOKEN="${MOCK_CHANNEL_TOKEN:-mock-token-abc}"

pass=0
fail=0

ok()    { printf '  \033[32mPASS\033[0m  %s\n' "$1"; pass=$((pass + 1)); }
no()    { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=$((fail + 1)); }
head2() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# code <method> <url> [body] —— 只回 HTTP 状态码
code() {
  local m="$1" u="$2" b="${3:-}"
  if [[ -n "$b" ]]; then
    curl -s -o /dev/null -w '%{http_code}' -X "$m" "$u" \
      -H 'Content-Type: application/json' -d "$b"
  else
    curl -s -o /dev/null -w '%{http_code}' -X "$m" "$u"
  fi
}

# body <method> <url> [body] —— 只回响应体
body() {
  local m="$1" u="$2" b="${3:-}"
  if [[ -n "$b" ]]; then
    curl -s -X "$m" "$u" -H 'Content-Type: application/json' -d "$b"
  else
    curl -s -X "$m" "$u"
  fi
}

check() {
  if [[ "$2" == "$3" ]]; then ok "$1"; else no "$1（期望 $2，实际 $3）"; fi
}

# hook <body> —— 发一条 mock 通道消息，回响应体。
# mock 通道校验共享 token，所以带上头；这也顺带说明绑定的 secret_ref
# 确实被解析并生效了。
hook() {
  curl -s -X POST "$GW$WEBHOOK" \
    -H 'Content-Type: application/json' -H "X-Mock-Token: $TOKEN" -d "$1"
}

hook_code() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "$GW$WEBHOOK" \
    -H 'Content-Type: application/json' -H "X-Mock-Token: $TOKEN" -d "$1"
}

# ---------------------------------------------------------------------------
head2 "0. 前置检查"

if [[ "$(code GET "$GW/healthz")" != "200" ]]; then
  echo "  Gateway 未就绪，先执行：set -a && . ./.env && set +a && ./start.sh" >&2
  exit 1
fi
ok "Gateway 可达"

# ---------------------------------------------------------------------------
head2 "1. 建租户（此前只能手写 SQL）"

check "创建租户" 201 "$(code POST "$ADMIN/tenants" \
  "{\"tenant_id\":\"$TENANT\",\"name\":\"验收租户\",\"settings\":{\"daily_token_budget\":100000}}")"

# 重复创建必须是 409 而非 500：「id 被占用」是调用方的错，
# 报成内部错误会让运维去查数据库而不是查自己的请求。
check "重复创建被拒（409）" 409 "$(code POST "$ADMIN/tenants" \
  "{\"tenant_id\":\"$TENANT\",\"name\":\"重复\"}")"

# id 会成为 Redis 键片段、指标标签与 URL 路径。含冒号会切断 Redis 键，
# 含斜杠会改变路由匹配，因此在入口一次拒掉，而不是在每个使用点转义。
check "非法 id 被拒（400）" 400 "$(code POST "$ADMIN/tenants" \
  '{"tenant_id":"bad:id/with slash","name":"x"}')"

# 租户模型第 7 要素：审计策略应随租户自动落库。
POLICY="$(body GET "$ADMIN/tenants/$TENANT/audit-policy")"
if grep -q '"body_mode":"truncate"' <<< "$POLICY"; then
  ok "审计策略随租户创建（默认 truncate，非 full）"
else
  no "审计策略缺失或默认过宽：$POLICY"
fi

# ---------------------------------------------------------------------------
head2 "2. 建 Agent 与版本"

check "创建 Agent" 201 "$(code POST "$ADMIN/tenants/$TENANT/agents" \
  "{\"agent_app_id\":\"$AGENT\",\"name\":\"验收 Agent\"}")"

check "创建版本（草稿）" 201 "$(code POST "$ADMIN/tenants/$TENANT/agents/$AGENT/versions" \
  '{"version":"v1","model_name":"deepseek-chat","system_prompt":"你是验收助手，回答简短。","model_api_key_ref":"secret://prod/tenant-demo/model/primary-api-key","model_params":{"temperature":0.3}}')"

# 草稿可改。
check "草稿可编辑" 200 "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1" \
  '{"model_name":"deepseek-chat","system_prompt":"你是验收助手，回答简短准确。","model_api_key_ref":"secret://prod/tenant-demo/model/primary-api-key"}')"

# ---------------------------------------------------------------------------
head2 "3. 配置能力（工具与治理）"

check "配置工具权限" 200 "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/tools" \
  '{"tools":[{"tool_name":"calculator","mode":"allow"}]}')"

# 三层校验的第一层：枚举必须已知。数据库唯一键管不了拼错的枚举值。
check "未知 mode 被拒（400）" 400 "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/tools" \
  '{"tools":[{"tool_name":"calculator","mode":"maybe"}]}')"

check "配置治理扩展" 200 "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/extensions" \
  '{"extensions":[{"kind":"guardrail","extension_name":"redaction","enabled":true,"priority":10},{"kind":"guardrail","extension_name":"tool_whitelist","enabled":true,"priority":20},{"kind":"callback","extension_name":"request_logger","enabled":true,"priority":90}]}')"

# ---------------------------------------------------------------------------
head2 "4. 发布与不可变性"

check "发布版本" 200 "$(code POST "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/publish")"

# 已发布版本不可改，且由 SQL 的 WHERE status='draft' 保证，不是靠约定——
# 这正是缓存的 Runtime 永远不会过期的前提。
check "已发布版本不可编辑（409）" 409 "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1" \
  '{"model_name":"deepseek-chat","system_prompt":"偷偷改"}')"

check "已发布版本的工具绑定被冻结（409）" 409 \
  "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/tools" '{"tools":[]}')"

# 重复发布不能静默重置 published_at，否则会丢掉真实上线时间。
check "重复发布被拒（409）" 409 \
  "$(code POST "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/publish")"

check "设置灰度权重" 200 "$(code PUT "$ADMIN/tenants/$TENANT/agents/$AGENT/deployment" \
  '{"env":"prod","routes":[{"version":"v1","weight":100}],"updated_by":"acceptance"}')"

# 仍在承载流量的版本不能归档，否则部署会指向一个装配不出来的版本，
# 失败会以用户可见的错误出现，而不是被一次拒绝的请求挡住。
check "承载流量的版本不可归档（409）" 409 \
  "$(code POST "$ADMIN/tenants/$TENANT/agents/$AGENT/versions/v1/archive")"

# ---------------------------------------------------------------------------
head2 "5. 绑定 IM 通道"

check "创建通道绑定" 200 "$(code PUT "$ADMIN/tenants/$TENANT/bindings/$BINDING" \
  "{\"agent_app_id\":\"$AGENT\",\"channel\":\"mock\",\"webhook_path\":\"$WEBHOOK\",\"secret_ref\":\"secret://prod/tenant-demo/channel/mock-token\",\"capabilities\":{\"inbound_mode\":\"payload\",\"supports_push\":true,\"max_text_length\":2048,\"rate_limit_per_min\":20}}")"

# 两种入站模式各有硬性要求，配错会得到一个「永远收不到消息」的绑定：
# 回调模式没有路径就不可达，长连接模式带路径会暗示一个不存在的 webhook。
check "回调模式缺 webhook_path 被拒（400）" 400 "$(code PUT "$ADMIN/tenants/$TENANT/bindings/x-$SUF" \
  "{\"agent_app_id\":\"$AGENT\",\"channel\":\"mock\",\"capabilities\":{\"inbound_mode\":\"payload\"}}")"

check "长连接模式带 webhook_path 被拒（400）" 400 "$(code PUT "$ADMIN/tenants/$TENANT/bindings/y-$SUF" \
  "{\"agent_app_id\":\"$AGENT\",\"channel\":\"wecom_aibot\",\"webhook_path\":\"/webhook/x\",\"capabilities\":{\"inbound_mode\":\"stream\"}}")"

# ---------------------------------------------------------------------------
head2 "6. 端到端：纯接口建出来的租户能真的收发消息"

EVT="acc-$SUF"
REPLY="$(hook "{\"event_id\":\"$EVT\",\"user_id\":\"acc-user\",\"text\":\"请回答：3 加 4 等于几\"}")"

# Gateway 的 ACK 带 request_id，是后续查状态的钥匙。
REQ="$(sed -n 's/.*"request_id":"\([^"]*\)".*/\1/p' <<< "$REPLY")"
if [[ -n "$REQ" ]]; then
  ok "消息被接收（request_id=$REQ）"
else
  no "消息未被接收：$REPLY"
fi

# 幂等：同一 event_id 第二次必须被 uk_event 拦住。
DUP="$(hook "{\"event_id\":\"$EVT\",\"user_id\":\"acc-user\",\"text\":\"重复投递\"}")"
if grep -q '"duplicate":true' <<< "$DUP"; then
  ok "重复投递被幂等拦截"
else
  no "重复投递未被拦截：$DUP"
fi

# 等 Worker 跑完。模型调用是慢的，所以给足时间；
# 只要状态离开 processing 就说明链路走通了。
if [[ -n "$REQ" ]]; then
  STATE=""
  for _ in $(seq 1 40); do
    STATE="$(body GET "$GW/console/api/requests/$REQ/state" | sed -n 's/.*"state":"\([^"]*\)".*/\1/p')"
    [[ -n "$STATE" && "$STATE" != "processing" ]] && break
    sleep 1
  done
  case "$STATE" in
    succeeded)
      ok "Agent 执行并投递成功（state=succeeded）" ;;
    delivery_failed)
      # 执行成功但投递失败：本机没起 reply collector 时的正常结果。
      # 关键在于 Agent 真的跑了，这一步仍算通过。
      ok "Agent 执行成功，投递失败（本机无 reply collector，属预期）" ;;
    "")
      no "查不到请求状态，Worker 可能未启动" ;;
    *)
      no "请求最终状态为 $STATE" ;;
  esac
fi

# ---------------------------------------------------------------------------
head2 "7. 租户隔离：新租户的配置不会串到别人"

# 用别的租户名去改这个租户的 Agent，必须查不到——
# 每条语句都带 tenant_id，所以指名另一个租户的 id 只会命中零行。
check "跨租户改 Agent 被拒（404）" 404 "$(code PUT "$ADMIN/tenants/tenant-demo/agents/$AGENT" \
  '{"name":"越权改名"}')"

check "跨租户建版本被拒（404）" 404 \
  "$(code POST "$ADMIN/tenants/tenant-demo/agents/$AGENT/versions" \
  '{"version":"v9","model_name":"m"}')"

# ---------------------------------------------------------------------------
head2 "8. 停用与收尾"

check "停用绑定" 200 "$(code POST "$ADMIN/tenants/$TENANT/bindings/$BINDING/status" \
  '{"status":"suspended"}')"

# 停用后 Gateway 的 binding 查询过滤 status='active'，因此 webhook 应 404。
check "停用后 webhook 不可达（404）" 404 \
  "$(hook_code "{\"event_id\":\"after-$SUF\",\"user_id\":\"u\",\"text\":\"x\"}")"

check "停用租户" 200 "$(code POST "$ADMIN/tenants/$TENANT/status" '{"status":"suspended"}')"

check "非法 status 被拒（400）" 400 \
  "$(code POST "$ADMIN/tenants/$TENANT/status" '{"status":"deleted"}')"

# ---------------------------------------------------------------------------
printf '\n\033[1m结果：%d 项通过，%d 项失败\033[0m\n' "$pass" "$fail"
printf '验收租户 %s 已停用，数据保留以便排查。\n' "$TENANT"
[[ "$fail" -eq 0 ]] || exit 1
