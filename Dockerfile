# SpeedMQ broker 镜像：多阶段构建（前端 stage + Go stage + 精简运行 stage）。
#
# 关于"可离线复现"：Go 部分仍然零外部依赖（内核 go.mod 没有 require），
# 但管理 UI 的产物**不入库**（见 .gitignore），因此镜像构建需要在 ui 阶段访问 npm registry。
# 若需完全离线构建，可在构建前本地 `cd web && npm ci && npm run build`，
# 并把 web/dist 保留在工作区（.dockerignore 里注释掉 web/dist/ 一行即可带进上下文）。

# ---------- 前端构建阶段 ----------
# 只产出 web/dist；运行阶段与 Go 阶段都不需要 Node。
FROM node:22-alpine AS ui
WORKDIR /ui

# 先只拷依赖清单，最大化利用层缓存（源码改动不会导致重新 npm ci）
COPY web/package.json web/package-lock.json ./
RUN npm ci

# 源码 + 构建配置；vite.config.ts 里 outDir 是 dist
COPY web/ ./
RUN npm run build

# ---------- 构建阶段 ----------
FROM golang:1.24-alpine AS build
WORKDIR /src

# 先只拷 go.mod，最大化利用层缓存（依赖不变时不会重复下载）
COPY go.mod ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/
# 管理 UI 的构建产物（web/dist）被 go:embed 打进二进制，因此必须拷进来。
# 这里**不需要 Node**：dist 来自上面的 ui 阶段（仓库里不含产物，只含 web/dist/.gitkeep 占位）。
COPY web/ ./web/
COPY --from=ui /ui/dist/ ./web/dist/

# CGO_ENABLED=0 产出静态链接二进制，运行阶段不依赖 glibc
# -trimpath 去掉本机路径，-s -w 去掉符号表与调试信息（二进制更小）
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/speedmqd ./cmd/speedmqd \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/speedmqctl ./cmd/speedmqctl

# ---------- 运行阶段 ----------
FROM alpine:3.21

# 仅用 busybox 自带命令，不执行 apk，避免构建阶段依赖外部软件源
RUN addgroup -g 10001 -S speedmq \
    && adduser -u 10001 -S -G speedmq -h /var/lib/speedmq -s /sbin/nologin speedmq \
    && mkdir -p /var/lib/speedmq /etc/speedmq \
    && chown -R 10001:10001 /var/lib/speedmq /etc/speedmq

COPY --from=build /out/speedmqd /usr/local/bin/speedmqd
COPY --from=build /out/speedmqctl /usr/local/bin/speedmqctl

# 容器内配置：把数据目录放到挂载卷，并放开 guest 的远端登录（容器里的连接来源
# 是 Docker 网关地址而非 127.0.0.1，不放开会被 403 拒绝）。仅用于本地开发。
COPY configs/speedmqd.json /etc/speedmq/speedmqd.json

USER speedmq
WORKDIR /var/lib/speedmq

# 数据目录（M4 起真正写入消息与元数据）
VOLUME ["/var/lib/speedmq"]

# AMQP 0-9-1
EXPOSE 5672
# MQTT 3.1.1（内置协议插件，可用 plugins.mqtt.enabled=false 关闭）
EXPOSE 1883
# 管理面（Management HTTP API + 内嵌管理 UI + Prometheus 指标）
EXPOSE 15672

# 以非 root 运行；配置由 CMD 显式传入，便于运行时覆盖
ENTRYPOINT ["/usr/local/bin/speedmqd"]
CMD ["-config", "/etc/speedmq/speedmqd.json", "-log-level", "info"]
