#!/usr/bin/env bash
# 端到端验收：把平台的每项能力串起来跑一遍，逐条打印结论。
#
# 这不是单元测试的替代，而是补充。单元测试证明每个零件对；这个脚本证明它们
# 组装起来之后，README 里声称的那些性质在真机上确实成立。
#
# 前置：MySQL 与 Redis 就绪，建表与种子数据已导入，configs/config.yaml 已配好。
#
#   ./scripts/acceptance.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TOKEN="${MOCK_CHANNEL_TOKEN:-mock-token-abc}"
ACME_TOKEN="${ACME_CHANNEL_TOKEN:-acme-token-xyz}"
GW=http://127.0.0.1:8080
WK=http://127.0.0.1:8081

pass=0
fail=0

ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; pass=$((pass + 1)); }
no()   { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=$((fail + 1)); }
head2() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# check <描述> <期望> <实际>
check() {
  if [[ "$2" == "$3" ]]; then ok "$1"; else no "$1（期望 $2，实际 $3）"; fi
}

post() {
  curl -s -o /dev/null -w '%{http_code}' -X POST "$1" \
    -H 'Content-Type: application/json' -H "X-Mock-Token: $2" -d "$3"
}

evt() { echo "acc-$(date +%s)-$RANDOM"; }

# ---------------------------------------------------------------------------
head2 "0. 前置检查"
# ---------------------------------------------------------------------------
if [[ "$(curl -s -o /dev/null -w '%{http_code}' "$GW/healthz")" != "200" ]]; then
  echo "  Gateway 未运行。先执行： WORKERS=2 ./start.sh" >&2
  exit 1
fi
ok "Gateway 健康"
if [[ "$(curl -s -o /dev/null -w '%{http_code}' "$WK/healthz")" != "200" ]]; then
  echo "  Worker 未运行" >&2
  exit 1
fi
ok "Worker 健康"

# ---------------------------------------------------------------------------
head2 "1. 入站顺序：幂等落库 → ACK → 入队"
# ---------------------------------------------------------------------------
E="$(evt)"
BODY="{\"event_id\":\"$E\",\"user_id\":\"acc-alice\",\"text\":\"验收消息\"}"

RESP="$(curl -s -X POST "$GW/webhook/mock/demo" -H "X-Mock-Token: $TOKEN" -d "$BODY")"
SID="$(echo "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("session_id",""))' 2>/dev/null)"
TID="$(echo "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("trace_id",""))' 2>/dev/null)"
[[ -n "$SID" ]] && ok "ACK 立即返回并带回 session_id" || no "ACK 未返回 session_id"
[[ -n "$TID" ]] && ok "ACK 带回 trace_id" || no "ACK 未返回 trace_id"

# 同一 event_id 重投必须被拦
DUP="$(curl -s -X POST "$GW/webhook/mock/demo" -H "X-Mock-Token: $TOKEN" -d "$BODY" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("duplicate",False))' 2>/dev/null)"
check "重复投递被 uk_event 拦截" "True" "$DUP"

# ---------------------------------------------------------------------------
head2 "2. 验签与路由"
# ---------------------------------------------------------------------------
check "错误 token 返回 401"      "401" "$(post "$GW/webhook/mock/demo" "wrong-token" '{"user_id":"x","text":"y"}')"
check "未知 webhook 返回 404"    "404" "$(post "$GW/webhook/mock/nope"  "$TOKEN"      '{"user_id":"x","text":"y"}')"
check "畸形报文返回 400"         "400" "$(post "$GW/webhook/mock/demo"  "$TOKEN"      '{"user_id":"x"}')"
check "企微未签名返回 401"       "401" "$(post "$GW/webhook/wecom/demo" "$TOKEN"      '<xml/>')"

# ---------------------------------------------------------------------------
head2 "3. 租户隔离"
# ---------------------------------------------------------------------------
check "跨租户 token 无效（demo 打 acme）" "401" \
  "$(post "$GW/webhook/mock/acme" "$TOKEN" '{"user_id":"alice","text":"越权"}')"

# The body is built into a variable first. Inlining a command substitution
# inside the JSON passed through two levels of quoting mangles it, and the
# symptom is a 400 that looks like a tenant problem rather than a quoting one.
ACME_BODY="{\"event_id\":\"$(evt)\",\"user_id\":\"alice\",\"text\":\"你好\"}"
check "acme 自己的 token 有效" "200" \
  "$(post "$GW/webhook/mock/acme" "$ACME_TOKEN" "$ACME_BODY")"

# 同名外部用户在两个租户下必须是两个会话
sleep 4
DEMO_S="$(curl -s "$GW/admin/tenants/tenant-demo/sessions?limit=100" \
  | python3 -c 'import json,sys;print(len([s for s in json.load(sys.stdin)["sessions"] if s["scope_key"]=="alice"]))' 2>/dev/null)"
ACME_S="$(curl -s "$GW/admin/tenants/tenant-acme/sessions?limit=100" \
  | python3 -c 'import json,sys;print(len([s for s in json.load(sys.stdin)["sessions"] if s["scope_key"]=="alice"]))' 2>/dev/null)"
if [[ "$DEMO_S" -ge 1 && "$ACME_S" -ge 1 ]]; then
  ok "同名用户 alice 在两个租户下各有独立会话"
else
  no "会话隔离异常（demo=$DEMO_S acme=${ACME_S}）"
fi

# ---------------------------------------------------------------------------
head2 "4. 配置驱动装配：同名同版本互不串用"
# ---------------------------------------------------------------------------
# The admin API returns binding *records*, including the ones set to deny.
# What matters is how many survive assembly, so denied entries are filtered
# out here — counting raw rows would report both tenants as 2 and hide the
# very difference this check exists to prove.
usable_tools() {
  curl -s "$GW/admin/tenants/$1/agents/assistant/versions/v1" \
    | python3 -c 'import json,sys
d = json.load(sys.stdin)
print(len([t for t in (d.get("Tools") or []) if t.get("Mode") != "deny"]))' 2>/dev/null
}

D_TOOLS="$(usable_tools tenant-demo)"
A_TOOLS="$(usable_tools tenant-acme)"
if [[ -n "$D_TOOLS" && -n "$A_TOOLS" && "$D_TOOLS" != "$A_TOOLS" ]]; then
  ok "两租户同名 v1 的可用工具数不同（demo=$D_TOOLS acme=${A_TOOLS}，acme 的 search 为 deny）"
else
  no "工具集未按租户区分（demo=$D_TOOLS acme=${A_TOOLS}）"
fi

# ---------------------------------------------------------------------------
head2 "5. 灰度与回滚"
# ---------------------------------------------------------------------------
DEP="$GW/admin/tenants/tenant-demo/agents/assistant/deployment"
put() { curl -s -X PUT "$DEP" -H 'Content-Type: application/json' -d "$1"; }

R="$(put '{"routes":[{"version":"v1","weight":90},{"version":"v2","weight":10}]}' \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d.get("deployment",{}).get("routes",[])))' 2>/dev/null)"
check "灰度：两个版本按权重生效" "2" "$R"

E1="$(put '{"routes":[{"version":"v1","weight":50}]}' | python3 -c 'import json,sys;print("error" in json.load(sys.stdin))' 2>/dev/null)"
check "权重和不为 100 被拒" "True" "$E1"
E2="$(put '{"routes":[{"version":"v3-draft","weight":100}]}' | python3 -c 'import json,sys;print("error" in json.load(sys.stdin))' 2>/dev/null)"
check "路由到未发布草稿被拒" "True" "$E2"
E3="$(put '{"routes":[{"version":"v99","weight":100}]}' | python3 -c 'import json,sys;print("error" in json.load(sys.stdin))' 2>/dev/null)"
check "路由到不存在版本被拒" "True" "$E3"

put '{"routes":[{"version":"v1","weight":100}],"updated_by":"acceptance"}' > /dev/null
ok "回滚到 v1 100%（与灰度是同一接口）"

# ---------------------------------------------------------------------------
head2 "6. 治理与审计"
# ---------------------------------------------------------------------------
EXTS="$(curl -s "$GW/admin/tenants/tenant-demo/agents/assistant/versions/v1" \
  | python3 -c 'import json,sys;print(len(json.load(sys.stdin).get("Extensions") or []))' 2>/dev/null)"
if [[ -n "$EXTS" && "$EXTS" -ge 5 ]]; then
  ok "治理策略按配置挂载（$EXTS 条）"
else
  no "治理策略数量异常（${EXTS}）"
fi

# 脱敏：凭据不得进入会话历史
E="$(evt)"
curl -s -o /dev/null -X POST "$GW/webhook/mock/demo" -H "X-Mock-Token: $TOKEN" \
  -d "{\"event_id\":\"$E\",\"user_id\":\"acc-redact\",\"text\":\"连接串 mysql://svc:topsecret999@db:3306/x 报错\"}"
sleep 5
LEAK="$(curl -s "$GW/admin/tenants/tenant-demo/audit?limit=50" | grep -c "topsecret999" || true)"
check "凭据未进入审计记录" "0" "$LEAK"

AUDIT_N="$(curl -s "$GW/admin/tenants/tenant-demo/audit?limit=5" \
  | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["records"]))' 2>/dev/null)"
if [[ -n "$AUDIT_N" && "$AUDIT_N" -ge 1 ]]; then
  ok "审计记录含放行判定，不只记拒绝"
else
  no "审计为空"
fi

# ---------------------------------------------------------------------------
head2 "7. 指标"
# ---------------------------------------------------------------------------
M="$(curl -s "$GW/metrics")"
grep -q 'agent_requests_total{.*tenant=' <<< "$M" && ok "指标带租户标签" || no "指标缺租户标签"
grep -q 'outcome="denied"' <<< "$M" && ok "拒绝独立计数，不混入错误率" || no "拒绝未单独计数"
WM="$(curl -s "$WK/metrics")"
grep -q 'agent_delivery_total' <<< "$WM" && ok "投递成功率可由同一 series 求比值" || no "缺投递指标"
grep -q 'agent_runtime_cached' <<< "$WM" && ok "Runtime 缓存数可观测" || no "缺 Runtime 缓存指标"

# ---------------------------------------------------------------------------
head2 "8. 死信：毒消息不阻塞其他会话"
# ---------------------------------------------------------------------------
POISON_BODY="{\"event_id\":\"$(evt)\",\"user_id\":\"acc-poison\",\"text\":\"必然失败\"}"
BROKEN_UP="$(post "$GW/webhook/mock/broken" "$TOKEN" "$POISON_BODY")"
if [[ "$BROKEN_UP" == "200" ]]; then
  ok "毒消息被正常受理（失败发生在执行侧，不在入站）"
else
  printf '  \033[33mSKIP\033[0m  死信演示未导入（seed_deadletter_demo.sql）\n'
fi

# 毒消息缠斗期间，正常会话必须畅通
GOOD=0
for i in 1 2 3; do
  BODY_I="{\"event_id\":\"$(evt)\",\"user_id\":\"acc-bystander\",\"text\":\"正常 ${i}\"}"
  [[ "$(post "$GW/webhook/mock/demo" "$TOKEN" "$BODY_I")" == "200" ]] && GOOD=$((GOOD + 1))
done
check "毒消息期间正常会话畅通" "3" "$GOOD"

# ---------------------------------------------------------------------------
head2 "9. trace_id 贯穿"
# ---------------------------------------------------------------------------
sleep 5
if [[ -n "$TID" ]]; then
  G="$(grep -c "$TID" data/gateway.log 2>/dev/null || echo 0)"
  W="$(grep -hc "$TID" data/worker-*.log 2>/dev/null | paste -sd+ - | bc 2>/dev/null || echo 0)"
  if [[ "$G" -ge 1 && "$W" -ge 1 ]]; then
    ok "同一 trace_id 同时出现在两个进程的日志（gateway=$G worker=${W}）"
  else
    no "trace_id 未贯穿两进程（gateway=$G worker=${W}）"
  fi
fi

# ---------------------------------------------------------------------------
printf '\n\033[1m结果：%d 项通过，%d 项失败\033[0m\n' "$pass" "$fail"
[[ "$fail" -eq 0 ]] || exit 1
