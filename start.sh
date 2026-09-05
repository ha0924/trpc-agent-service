#!/usr/bin/env bash
# 启动 Gateway 与 Worker。
#
# 两个进程独立启动、独立停止：它们之间没有调用关系，只通过消息队列与共享存储
# 协作，因此谁先起、谁后起、少起一个都不影响另一个能否运行。少了 Worker，
# Gateway 照样收消息入队，消息在信箱里等着。
#
# 前置条件：MySQL 与 Redis 已就绪，configs/config.yaml 已按本机情况填好，
# 密钥对应的环境变量已导出。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

CONFIG="${CONFIG:-$ROOT/configs/config.yaml}"
WORKERS="${WORKERS:-1}"
REPLY_URL="${REPLY_URL:-}"

if [[ ! -f "$CONFIG" ]]; then
  echo "配置文件不存在：$CONFIG" >&2
  echo "先执行：cp configs/config.example.yaml configs/config.yaml 并按本机情况修改" >&2
  exit 1
fi

mkdir -p "$ROOT/bin" "$ROOT/data"
if [[ ! -x "$ROOT/bin/gateway" || ! -x "$ROOT/bin/worker" ]]; then
  "$ROOT/build.sh"
fi

start_one() {
  local name="$1"; shift
  local pid_file="$ROOT/data/$name.pid"

  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "$name 已在运行：pid=$(cat "$pid_file")"
    return 0
  fi

  nohup "$@" >"$ROOT/data/$name.log" 2>&1 &
  echo $! >"$pid_file"
  echo "已启动 ${name}：pid=$(cat "$pid_file") 日志=data/$name.log"
}

start_one gateway "$ROOT/bin/gateway" -config "$CONFIG"

# 多个 Worker 共享同一个队列，各自争抢会话租约。它们是可互换的：
# 任何一个都能服务任何会话，因此扩容就是多起几个。
for ((i = 1; i <= WORKERS; i++)); do
  args=("$ROOT/bin/worker" -config "$CONFIG" -health-addr ":$((8080 + i))")
  [[ -n "$REPLY_URL" ]] && args+=(-reply-url "$REPLY_URL")
  start_one "worker-$i" "${args[@]}"
done

echo
echo "Gateway   http://127.0.0.1:8080"
echo "  健康检查  /healthz"
echo "  指标      /metrics"
echo "  管理接口  /admin/tenants"
echo "Worker    健康检查与指标在 :8081 起，每多一个 Worker 端口加一"
