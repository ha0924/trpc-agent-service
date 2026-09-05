# 同一个 Dockerfile 构建两个二进制，由 BINARY 参数决定。
#
# 两个进程共用统一消息模型、租户上下文、配置读取、存储路由与调度库，因此不拆
# 仓库，也没有理由拆构建。
#
#   docker build --build-arg BINARY=gateway -f deployments/Dockerfile .
#   docker build --build-arg BINARY=worker  -f deployments/Dockerfile .

FROM golang:1.24-alpine AS builder

ARG BINARY=gateway

WORKDIR /src

# 先只拷依赖清单，让 module 下载单独成层：改代码不会让依赖重新下载一遍。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO 关闭，产出静态二进制，使运行镜像可以是不带 libc 的最小基底。
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w" \
    -o /out/app "./cmd/${BINARY}"

FROM alpine:3.20

# ca-certificates 是调用模型与 IM 平台 HTTPS 接口所必需的；
# tzdata 使日志时间与运维所在时区一致，排查跨进程链路时省去心算。
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 app

WORKDIR /app
COPY --from=builder /out/app /app/app

# 不以 root 运行：这个进程会执行模型返回的工具调用，权限越小越好。
USER app

ENTRYPOINT ["/app/app"]
