#!/usr/bin/env bash
# 构建 Gateway 与 Worker 两个可执行文件。
#
# 设计依据：docs/dev/代码组织方案.md §1「仓库与进程」——
# 一个仓库、一个 Go module，编译出两个独立部署的二进制。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

mkdir -p "$ROOT/bin"

VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
LDFLAGS="-X github.com/liuzengh/trpc-agent-service/trpcservice.Version=${VERSION}"

for cmd in gateway worker; do
  go build -ldflags "$LDFLAGS" -o "$ROOT/bin/$cmd" "./cmd/$cmd"
  echo "built: bin/$cmd"
done

echo "version: $VERSION"
