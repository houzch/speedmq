# SwiftMQ broker 镜像：多阶段构建（构建阶段用官方 Go 镜像，运行阶段用精简 alpine）。
#
# 内核零外部依赖，因此构建阶段无需访问任何第三方模块仓库，完全可离线复现。

# ---------- 构建阶段 ----------
FROM golang:1.24-alpine AS build
WORKDIR /src

# 先只拷 go.mod，最大化利用层缓存（依赖不变时不会重复下载）
COPY go.mod ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/

# CGO_ENABLED=0 产出静态链接二进制，运行阶段不依赖 glibc
# -trimpath 去掉本机路径，-s -w 去掉符号表与调试信息（二进制更小）
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/swiftmqd ./cmd/swiftmqd

# ---------- 运行阶段 ----------
FROM alpine:3.21

# 仅用 busybox 自带命令，不执行 apk，避免构建阶段依赖外部软件源
RUN addgroup -g 10001 -S swiftmq \
    && adduser -u 10001 -S -G swiftmq -h /var/lib/swiftmq -s /sbin/nologin swiftmq \
    && mkdir -p /var/lib/swiftmq /etc/swiftmq \
    && chown -R 10001:10001 /var/lib/swiftmq /etc/swiftmq

COPY --from=build /out/swiftmqd /usr/local/bin/swiftmqd

# 容器内配置：把数据目录放到挂载卷，并放开 guest 的远端登录（容器里的连接来源
# 是 Docker 网关地址而非 127.0.0.1，不放开会被 403 拒绝）。仅用于本地开发。
COPY configs/swiftmqd.json /etc/swiftmq/swiftmqd.json

USER swiftmq
WORKDIR /var/lib/swiftmq

# 数据目录（M4 起真正写入消息与元数据）
VOLUME ["/var/lib/swiftmq"]

# AMQP 0-9-1
EXPOSE 5672

# 以非 root 运行；配置由 CMD 显式传入，便于运行时覆盖
ENTRYPOINT ["/usr/local/bin/swiftmqd"]
CMD ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
