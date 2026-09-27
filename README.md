# SwiftMQ

用 Go 实现的 **AMQP 0-9-1 协议级兼容消息中间件**。目标是让现有 RabbitMQ 各语言客户端**无需改代码、无需换 SDK**，只改连接地址即可接入，并获得与 RabbitMQ 一致的语义。

> 兼容基线：**RabbitMQ 4.3 语义**（AMQP 0-9-1 + RabbitMQ 扩展）。不保留 3.x 与 4.0–4.2 中已被移除的能力（瞬时队列、全局 QoS、Classic Queue v1、经典镜像队列）。

---

## ⚠️ 当前状态：M1（协议底座），**还不能收发消息**

项目处于早期开发阶段，**目前只完成连接建立相关能力，请勿用于任何实际场景**。

**已实现**

- 协议头协商（版本不匹配时按规范回写支持的版本）
- `Connection.Start / Start-Ok`（SASL：`PLAIN`、`AMQPLAIN`）、`Tune / Tune-Ok`、`Open / Open-Ok`、`Close / Close-Ok`
- 心跳：主动发送 + 2 倍间隔超时判定
- `Channel.Open / Open-Ok`、`Channel.Flow / Flow-Ok`、`Channel.Close / Close-Ok`
- 完整的软/硬错误作用域框架（`403 / 402 / 530 / 540 / 505 / 501 / 502 / 503 / 504` 与 RabbitMQ 对齐）
- 插件框架：注册中心、依赖 DAG 排序与环检测、能力审计、失败隔离
- AMQP 0-9-1 以**第一个协议插件**的形式接入（内核不含任何 AMQP 知识）

**尚未实现**

- Exchange / Queue / Binding 声明与路由
- 消息收发（`Basic.Publish / Consume / Deliver / Ack`）与内容帧
- 发布确认、TTL、死信、优先级
- 消息持久化与崩溃恢复
- 集群、管理 HTTP API、管理 UI

完整路线图见下文「路线图」一节。

---

## 快速开始

### 方式一：Docker（推荐）

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose up -d --build

docker compose ps          # 状态应显示 Up (healthy)
docker compose logs -f     # 跟随日志
```

默认监听 `5672`（AMQP 0-9-1）。

### 方式二：本地构建

需要 Go 1.24+。

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq

go build -o bin/swiftmqd ./cmd/swiftmqd
./bin/swiftmqd -log-level debug
```

启动后日志应包含：

```
msg="SwiftMQ 启动中" version=0.1.0 data_dir=data vhost=/
msg="插件 amqp091 v0.1.0（API v1）能力: [net.listen]"
msg="监听已启动" protocol=amqp091 listener=amqp addr=[::]:5672
```

### 验证连接是否可用

仓库自带一个真实客户端探针（基于 `rabbitmq/amqp091-go`），会验证连接、Channel 开关与错误路径：

```bash
cd test/integration/amqp091probe
go run .
```

期望输出：

```
PASS  正常连接 + Channel 开关 + 优雅关闭
PASS  错误口令应被拒绝
PASS  不存在的 vhost 应被拒绝
```

也可以直接用任意 RabbitMQ 客户端连接测试：

```bash
# Python
pip install pika
```

```python
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
print(conn.is_open)          # True
conn.close()
```

> 注意：默认用户 `guest/guest` **只允许从本机登录**（对齐 RabbitMQ 行为）。从容器外或远程连接时，需在配置中为该用户开启 `remote_access`。

---

## 配置

命令行参数：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-config` | 空 | JSON 配置文件路径；不指定则使用内置默认值 |
| `-log-level` | `info` | `debug` / `info` / `warn` / `error` |

配置文件示例（见 [configs/swiftmqd.json](configs/swiftmqd.json)）：

```json
{
  "data_dir": "/var/lib/swiftmq",
  "default_vhost": "/",
  "vhosts": ["/"],
  "listeners": {
    "amqp091": [{ "addr": ":5672" }]
  },
  "users": {
    "guest": { "password": "guest", "tags": ["administrator"], "remote_access": true }
  },
  "plugins": {}
}
```

| 字段 | 说明 |
|---|---|
| `data_dir` | 节点数据目录（对齐 RabbitMQ 的 `RABBITMQ_MNESIA_DIR` 定位，M4 起真正落盘） |
| `vhosts` | vhost 清单；`default_vhost` 会自动加入，不会因漏写而连不上 |
| `listeners` | 按**插件名**覆盖监听地址 |
| `users` | 内置用户表，`remote_access: false` 时仅允许本机登录 |
| `plugins` | 各插件的配置段，插件通过 `Host.Config` 读取自己的段 |

> 环境变量（`SWIFTMQ_*`）与 YAML 配置将在 M5 随管理面一起支持。

---

## 设计要点

### 兼容性优先

不做自造协议，而是完整实现 AMQP 0-9-1 与 RabbitMQ 扩展。兼容性判定方式是**双跑对照**：同一份测试用例同时跑在 RabbitMQ 4.3 与本项目上，比对协议帧序列与客户端回调行为，差异即缺陷。

客户端兼容性验收矩阵覆盖：Java（`amqp-client` / Spring AMQP）、Python（`pika` / `kombu` / `celery`）、Go（`amqp091-go`）、.NET、Node.js（`amqplib`）、PHP、Ruby、Erlang。

### 内核稳定，能力外挂

内核只保证"不变的东西"（兼容性契约），一切"会变的东西"都做插件：

- **不可插件化**：AMQP 0-9-1、队列与存储引擎、Exchange 路由、vhost/权限、确认与 DLX/TTL 语义、管理 API 核心
- **必须插件化**：AMQP 1.0、MQTT、STOMP、Shovel、Federation、LDAP/OAuth2、延迟消息、一致性哈希 Exchange

插件只能 import 稳定的插件 API 包 `pkg/plugin`，内核内部包一律不可见。插件支持编译期注册与外部进程（gRPC）两种承载形态，并具备依赖排序、能力审计与失败隔离；每个插件还必须上报健康状态。

### 存储设计

消息数据用自研分段追加日志，元数据用内嵌 Raft + KV；**不依赖任何外部数据库或协调服务**。小消息（≤4096 字节）走每队列存储，大消息走 vhost 共享存储 + 引用计数（扇出时只写一份）。fsync 分四档（`none` / `os` / `batch` / `always`）并与 publisher confirm 时机强绑定，因此"经典队列"与"Quorum 队列"的差异是配置档位差异，而非两套语义。

---

## 项目结构

```
swiftmq/
├── cmd/
│   ├── swiftmqd/            # broker 进程入口
│   └── swiftmqctl/          # 运维 CLI（规划中）
├── internal/
│   ├── protocol/
│   │   ├── codec/           # 基础类型、field-table、帧编解码
│   │   ├── spec/            # 类/方法标识、错误码、软硬错误作用域
│   │   └── amqp091/         # AMQP 0-9-1 协议插件
│   ├── transport/           # 监听、TLS、协议嗅探与连接分发
│   ├── plugin/              # 插件注册中心、生命周期、Host 句柄
│   ├── broker/              # 内核：vhost、用户与（后续）路由模型
│   ├── auth/                # SASL：PLAIN / AMQPLAIN
│   └── config/              # 配置加载与默认值
├── pkg/plugin/              # 对外稳定插件 API
├── test/integration/        # 各语言客户端集成验证（独立 module）
├── configs/                 # 示例配置
└── Dockerfile / docker-compose.yml
```

---

## 开发

```bash
go build ./...        # 构建
go vet ./...          # 静态检查
go test ./...         # 单元测试
gofmt -l .            # 检查格式（应无输出）
```

构建镜像：

```bash
docker build -t swiftmq:0.1.0 .
```

镜像约 13 MB：静态链接二进制 + alpine，**以非 root（uid 10001）运行**，数据目录挂载在 `/var/lib/swiftmq`。

---

## 路线图

| 阶段 | 里程碑 | 内容 | 状态 |
|---|---|---|---|
| 一期 | M1 | 协议底座：连接握手、心跳、Channel 开关、插件框架 | ✅ 已完成 |
| 一期 | M2 | Exchange / Queue / Binding、三种路由、消息收发 | 规划中 |
| 一期 | M3 | 发布确认、mandatory/return、TTL、死信、优先级、权限 | 规划中 |
| 一期 | M4 | 持久化、fsync 档位、崩溃恢复、流控 | 规划中 |
| 一期 | M5 | 管理 HTTP API、管理 UI（Vue 3 + Element Plus）、`swiftmqctl` | 规划中 |
| 二期 | M6 | 集群、Quorum Queue 复制、分区处理、故障切换 | 规划中 |
| 二期 | M7 | 性能打磨、插件化验证（MQTT / AMQP 1.0） | 规划中 |

每个里程碑的完成标准是"**真实客户端跑通 + 与 RabbitMQ 行为一致**"，而非"代码写完"。

---

## 贡献

欢迎提交 Issue 与 Pull Request。由于项目强调协议兼容性：

- 修 bug 时请说明对应的 RabbitMQ 行为（版本、客户端、复现步骤）
- 涉及协议细节的改动，请附上与 RabbitMQ 的对照结果
- 提交前请确保 `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` 均通过

---

## 许可证

本项目采用 [Apache License 2.0](LICENSE)。

允许使用、修改、分发（含商业使用），需保留版权与许可声明，且不提供任何担保。

Copyright 2026 houzch（见 [NOTICE](NOTICE)）

---

## 致谢

AMQP 0-9-1 协议规范与 [RabbitMQ](https://www.rabbitmq.com/) 的行为语义是本项目兼容性工作的对照基准。本项目为独立实现，与 RabbitMQ 官方无隶属关系，未使用其代码。
