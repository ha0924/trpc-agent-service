#!/usr/bin/env bash
# 停止 Gateway 与全部 Worker。
#
# 发 SIGTERM 而非 SIGKILL，并留出等待时间：Worker 正在执行的那一轮必须跑完。
# 一轮若被从中间掐断——Agent 已执行、回复未送达——留下的 inbound_events 记录
# 不能被重跑，因为它的 Tool 已经产生了副作用。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
GRACE="${GRACE:-30}"

stop_one() {
  local pid_file="$1"
  local name
  name="$(basename "$pid_file" .pid)"

  if [[ ! -f "$pid_file" ]]; then
    return 0
  fi

  local pid
  pid="$(cat "$pid_file")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "${name}：pid 文件残留（进程已不在）"
    rm -f "$pid_file"
    return 0
  fi

  kill -TERM "$pid"
  echo -n "${name}：等待在途请求完成"

  local waited=0
  while kill -0 "$pid" 2>/dev/null && ((waited < GRACE)); do
    sleep 1
    ((waited++))
    echo -n "."
  done

  if kill -0 "$pid" 2>/dev/null; then
    # 超过宽限期仍未退出。强杀，但要说明代价：在途那一轮的状态是不确定的，
    # 需要靠 inbound_events 中停留在 processing 的记录来对账。
    echo " 超时，强制结束"
    kill -KILL "$pid" 2>/dev/null || true
    echo "  注意：该进程未能优雅退出，请检查 inbound_events 中 state=processing 的记录"
  else
    echo " 已停止"
  fi
  rm -f "$pid_file"
}

shopt -s nullglob
pid_files=("$ROOT"/data/*.pid)
if ((${#pid_files[@]} == 0)); then
  echo "没有正在运行的进程"
  exit 0
fi

# Worker 先停：让它们把在途的轮次做完并释放租约，之后 Gateway 才停止接收新
# 消息。反过来先停 Gateway 也可以，只是队列里会短暂堆积。
for f in "$ROOT"/data/worker-*.pid; do stop_one "$f"; done
for f in "$ROOT"/data/gateway.pid; do stop_one "$f"; done
