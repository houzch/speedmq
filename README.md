<!-- i18n-switcher -->
**简体中文** | [繁體中文](docs/i18n/zh-TW/README.md) | [English](docs/i18n/en/README.md) | [日本語](docs/i18n/ja/README.md) | [한국어](docs/i18n/ko/README.md) | [Español](docs/i18n/es/README.md) | [Deutsch](docs/i18n/de/README.md) | [Français](docs/i18n/fr/README.md) | [العربية](docs/i18n/ar/README.md) | [Русский](docs/i18n/ru/README.md) | [Italiano](docs/i18n/it/README.md) | [Nederlands](docs/i18n/nl/README.md) | [Português](docs/i18n/pt/README.md) | [Bahasa Indonesia](docs/i18n/id/README.md) | [ไทย](docs/i18n/th/README.md) | [Tiếng Việt](docs/i18n/vi/README.md) | [Bahasa Melayu](docs/i18n/ms/README.md) | [Filipino](docs/i18n/fil/README.md)

# SwiftMQ

用 Go 编写的 **RabbitMQ 兼容**消息中间件。现有 RabbitMQ 客户端**不改代码、不换 SDK**，只改连接地址即可接入。

## 简介

- **协议兼容**：AMQP 0-9-1（含 RabbitMQ 扩展）与 MQTT 3.1.1；兼容基线为 **RabbitMQ 4.3 语义**。
- **部署简单**：一个二进制 / 一个容器，管理 UI 已内嵌，不需要额外的 Nginx、数据库或 Node 运行时。
- **运维够用**：管理 UI（队列 / 交换机 / 连接 / 账号权限 / 虚拟主机 / 策略 / 限制 / 集群）、Prometheus `/metrics`、命令行 `swiftmqctl`。
- **默认端口**：`5672`（AMQP）、`1883`（MQTT）、`15672`（管理 UI / HTTP API / 指标）。

已具备的能力：持久化（段日志 + fsync 档位 + 崩溃恢复）、发布确认、TTL / 死信 / 长度限制、消费者优先级、Direct Reply-To、集群（Raft 元数据 + 仲裁队列 + 跨节点转发）、插件热启停。

***

## 快速开始

### 方式一：Docker（推荐）

**不用克隆仓库，直接拉镜像跑起来：**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.0.0
```

镜像同时发布在两处（内容相同，挑网络快的那个）：Docker Hub `houzch/swiftmq`、GitHub GHCR `ghcr.io/houzch/swiftmq`；两个仓库都提供 `linux/amd64` 与 `linux/arm64`。

- 数据落在命名卷 `swiftmq-data`，容器重建不丢。
- 停止 / 删除：`docker stop swiftmq`、`docker rm swiftmq`（数据卷保留）。

**要改配置或用 compose 编排，再克隆仓库：**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # 用已发布的镜像；改成 up -d --build 则本地构建

docker compose ps        # 状态应为 Up (healthy)
docker compose logs -f   # 跟随日志
```

- 配置以只读方式挂载 `configs/swiftmqd.json`，改完 `docker compose restart` 生效。
- 停止：`docker compose down`（保留数据）；`docker compose down -v`（连数据一起删）。

### 方式二：本地二进制（需要 Go 1.24+）

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> 管理 UI 的构建产物不入库。若要用 UI，先在 `web/` 执行 `npm ci && npm run build`；
> 不构建也能正常启动收发消息，只是访问 `/` 会提示「管理 UI 未构建」。

### 首次登录（务必先改掉默认账号）

| 入口 | 地址 / 凭证 |
| --- | --- |
| 管理 UI | <http://localhost:15672/>（用户名 `guest`，口令 `guest`） |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883`（账号同上） |

新装实例的总账号带「首次登录强制改密」标记：管理 UI 登录后会**强制要求同时修改账号名与口令**，改完才能进入后台。

也可以直接调 API 完成（适合自动化）：

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ 默认 `guest/guest` 与 RabbitMQ 行为一致：**只允许本机登录**。从容器外 / 远程连接需在配置里为该用户开启 `remote_access`（示例配置已为容器场景开启）。
> **服务一旦对外可访问，请立即更换凭证。**

### 接入你的应用（改连接地址即可）

```python
# Python（pika）
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT（mosquitto 客户端）
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

管理 HTTP API 与 `rabbitmqadmin` 兼容；管理 UI 的「新增队列 / 交换机」就是标准声明端点，脚本同样能做：

```bash
# 声明队列（仲裁队列用 arguments: {"x-queue-type":"quorum"} 表达）
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### 日常运维

| 事项 | 入口 |
| --- | --- |
| 管理 UI | <http://localhost:15672/>：队列 / 交换机 / 连接 / 账号权限 / 虚拟主机 / 策略 / 限制 / 特性开关 / 集群，右上角可设置自动刷新与**界面语言** |
| 监控指标 | <http://localhost:15672/metrics>（Prometheus 文本，需认证）；面板与告警见 [docs/ops/monitoring](docs/ops/monitoring/README.md) |
| 命令行 | `./bin/swiftmqctl status`、`list_queues`、`plugins list`、`plugins disable amqp091`（热停用，端口立即关闭） |
| 健康检查 | `nc -z 127.0.0.1 15672`（compose 已内置 healthcheck） |
| 备份与恢复 | [docs/ops/backup-restore.md](docs/ops/backup-restore.md) |
| 升级 | [docs/ops/upgrade.md](docs/ops/upgrade.md) |
| 安全基线 | [docs/ops/security-baseline.md](docs/ops/security-baseline.md) |

常用配置（完整示例见 [configs/swiftmqd.json](configs/swiftmqd.json)，也可用 `SWIFTMQ_*` 环境变量覆盖）：

| 配置项 | 说明 | 默认 |
| --- | --- | --- |
| `data_dir` | 数据目录（消息 + 元数据），**务必持久化** | `data` |
| `listeners` | 各协议监听地址，可配 TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | 管理 UI / API 监听地址 | `:15672` |
| `management.language` | 管理 UI 默认语言；留空则按部署地时区自动选择 | 自动 |
| `storage.fsync` | 落盘档位 `none / os / batch / always`（同时决定 confirm 时机） | `os` |
| `storage.memory_high_watermark`、`storage.disk_free_limit` | 资源水位：触发即阻塞生产者，**不丢消息** | `0.4` / 50 MiB |
| `users` | 内置用户表（口令 + 标签 + `remote_access`） | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | 多节点集群（默认关闭），成员变更用 `swiftmqctl add_member` | 关闭 |

> 端口可能被占用：用 `listeners` / `management.addr` 换成其它端口即可。

***

## 项目结构

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # broker 进程入口（要跑的就是它）
│   └── swiftmqctl/      # 运维 CLI（走管理 HTTP API，与内核版本解耦）
├── internal/            # 内核实现
│   ├── protocol/        # 协议插件：amqp091、mqtt（编解码 / 方法 / 会话）
│   ├── broker/          # 内核：vhost、交换机、队列、死信、流控、管理面视图
│   ├── store/           # 持久化：段日志、队列索引、崩溃恢复
│   ├── raft/ meta/      # 集群：自研 Raft 与元数据复制
│   ├── management/      # 管理 HTTP API + Prometheus 指标 + 内嵌 UI 静态服务
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # 对外稳定契约：插件 API（plugin）与外部进程插件线协议（sidecar）
├── web/                 # 管理 UI 前端工程（Vue 3 + Vite），产物构建时经 go:embed 打进二进制
├── configs/             # 示例配置
├── docs/ops/            # 运维文档：备份恢复 / 升级 / 安全基线 / 监控
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## 贡献

欢迎提交 Issue 与 Pull Request。本项目的立身之本是**协议兼容**，因此：

- 修 bug 请说明对应的 RabbitMQ 行为（版本、客户端、复现步骤）；
- 涉及协议细节的改动，请附上与 RabbitMQ 的对照结果；
- 提交前确保 `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` 均通过。

***

## 许可证

本项目采用 [Apache License 2.0](LICENSE)。

允许使用、修改、分发（含商业使用），需保留版权与许可声明，且不提供任何担保。

Copyright 2026 houzch（见 [NOTICE](NOTICE)）

***

## 致谢

AMQP 0-9-1 协议规范与 [RabbitMQ](https://www.rabbitmq.com/) 的行为语义是本项目兼容性工作的对照基准。本项目为独立实现，与 RabbitMQ 官方无隶属关系，未使用其代码。

***

## 加入交流群

扫码加入 SwiftMQ 交流群，有问题可以在群里直接问：

![SwiftMQ 交流群](1280X1280.PNG)
