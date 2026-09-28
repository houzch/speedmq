# SwiftMQ

用 Go 实现的 **AMQP 0-9-1 协议级兼容消息中间件**。目标是让现有 RabbitMQ 各语言客户端**无需改代码、无需换 SDK**，只改连接地址即可接入，并获得与 RabbitMQ 一致的语义。

> 兼容基线：**RabbitMQ 4.3 语义**（AMQP 0-9-1 + RabbitMQ 扩展）。不保留 3.x 与 4.0–4.2 中已被移除的能力（瞬时队列、全局 QoS、Classic Queue v1、经典镜像队列）。

***

## ⚠️ 当前状态：M5（管理与观测），**仍不可用于生产**

处于早期开发阶段。M5 已交付管理 HTTP API、内嵌管理 UI、Prometheus 指标、`swiftmqctl` 与插件热启停。

**已实现**

- 连接层：协议头协商、`Connection.Start / Tune / Open / Close`、心跳超时、`Channel.*`
- **拓扑**：`Exchange / Queue / Binding` 的声明、绑定、解绑、删除；被动声明；`amq.*` 保留名与默认交换机保护；交换机间绑定
- **路由**：`direct`、`fanout`、`topic`（`*` 与 `#` 通配）、`headers`；默认交换机按队列名隐式路由
- **消息**：`Basic.Publish / Consume / Deliver / Get / Ack / Nack / Reject / Qos / Recover`，完整内容帧（14 个属性 + 任意长度分片）
- **语义**：FIFO、多消费者竞争消费、prefetch 额度、未确认跟踪、`requeue` 重投并置 `redelivered`、`mandatory` 回 `Basic.Return`、消费者取消通知
- **发布确认**：`confirm.select`，逐条 `basic.ack`，序号从 1 连续；`Basic.Return` 保证先于 confirm 发出
- **TTL 与死信**：`x-message-ttl`、消息级 `expiration`、`x-dead-letter-exchange` / `x-dead-letter-routing-key`，死信带 `x-death` 头（`reason` / `queue` / `time` / `count` / 原始路由信息）
- **长度限制**：`x-max-length` / `x-max-length-bytes`，`x-overflow` 支持 `drop-head` / `reject-publish` / `reject-publish-dlx`
- **其他队列参数**：`x-max-priority`（优先级队列）、`x-expires`（空闲队列自动删除）
- **权限**：按 vhost 的 `configure` / `write` / `read` 正则鉴权，越权返回 403
- **持久化（M4）**：durable 队列 + `delivery-mode=2` 的消息落盘，自研**分段追加日志 + 队列索引**（`msg_stores/vhosts/<vhost>/queues/<queue>/`），每条记录带长度前缀与 CRC32，恢复时丢弃尾部半写记录
- **fsync 档位（M4）**：`none` / `os` / `batch` / `always`，且 **publisher confirm 时机与档位强绑定** —— 持久消息只有按档位落盘后才回 `basic.ack`
- **崩溃恢复（M4）**：重启后重新声明 durable 队列即恢复磁盘上的消息；未确认的消息回到队头并置 `redelivered=true`
- **资源水位流控（M4）**：内存水位（进程占用 vs 物理内存比例）与磁盘剩余空间下限触发时**阻塞生产者**（读循环停读 → TCP 背压），并向连接下发 `Connection.Blocked` / `Unblocked`
- **管理 HTTP API（M5）**：RabbitMQ Management API 兼容子集（`/api/overview`、`queues`、`exchanges`、`bindings`、`connections`、`channels`、`consumers`、`users`、`permissions`、`vhosts`、`plugins`、`whoami`），Basic Auth + 标签与 vhost 权限双重校验
- **管理 UI（M5）**：Vue 3 + Vite + TypeScript + Element Plus，四个页面（Overview / Queues / Exchanges / Connections）+ 队列详情，产物经 `go:embed` 打进二进制，**单个二进制即可访问**，无额外静态服务
- **可观测性（M5）**：Prometheus 文本格式 `/metrics`（队列深度、未确认、投递/确认计数、磁盘/内存水位、插件状态）；`-log-format json` 结构化日志
- **运维 CLI（M5）**：`swiftmqctl`（`status` / `list_queues` / `list_connections` / `list_exchanges` / `list_bindings` / `add_user` / `set_permissions` / `close_connection` / `plugins list|show|enable|disable`）
- **插件治理（M5）**：`swiftmqctl plugins` 与管理 API 均可**不重启内核**热启用/停用插件（落到实处是关闭/恢复它的 listener），配置可声明 `enabled` / `required` / `builtin`
- **动态用户与权限（M5）**：通过管理 API / CLI 增删用户与权限（内存生效，不落盘 —— 元数据持久化属 M6 的内嵌 Raft）
- **错误语义**：`404 / 406 / 403 / 405 / 402 / 540 / 504` 与 RabbitMQ 对齐（软错误只关 Channel，硬错误关连接）
- 插件框架：注册中心、依赖 DAG 排序、能力审计、失败隔离；AMQP 0-9-1 是**第一个协议插件**（内核不含任何 AMQP 知识）

**尚未实现**

- Direct Reply-To（`amq.rabbitmq.reply-to`）、消费者优先级（`x-priority`）
- 用户 / vhost / 权限的**持久化**（重启后回到配置文件的内容）；vhost 的动态增删
- 策略（policies）接口：`/api/policies` 返回空数组，功能未实现
- 拓扑元数据的持久化（交换机与绑定重启后需客户端重新声明；M4/M5 只持久化队列消息）
- 段文件的轮转与磁盘回收（当前每队列单段，删除仅标记）
- 集群、仲裁队列与流队列、Stream 协议
- AMQP 1.0 / MQTT / STOMP（计划以插件形态提供）
- YAML 配置（当前支持 JSON 文件 + `SWIFTMQ_*` 环境变量；YAML 需要引入解析依赖，暂缓）

完整路线图见下文「路线图」一节。

***

## 快速开始

### 方式一：Docker（推荐）

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose up -d --build

docker compose ps          # 状态应显示 Up (healthy)
docker compose logs -f     # 跟随日志
```

默认监听 `5672`（AMQP 0-9-1）与 `15672`（管理 UI / HTTP API / 指标）。

### 方式二：本地构建

需要 Go 1.24+。

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq

go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -log-level debug
```

启动后日志应包含：

```
msg="SwiftMQ 启动中" version=0.5.0 data_dir=data vhost=/ fsync=os
msg="插件 amqp091 v0.1.0（API v1）能力: [net.listen]"
msg="监听已启动" protocol=amqp091 listener=amqp addr=[::]:5672
msg="管理面已启动" component=management addr=[::]:15672 api=/api/overview ui=/
```

### 管理与观测（M5）

启动后有两个入口（默认端口与 RabbitMQ 一致）：

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 管理 UI | <http://localhost:15672/> | Overview / Queues / Exchanges / Connections + 队列详情（发布测试消息 / 取消息 / purge / delete） |
| 管理 HTTP API | <http://localhost:15672/api/overview> | RabbitMQ Management API 兼容子集，Basic Auth（账号口令同 AMQP） |
| Prometheus 指标 | <http://localhost:15672/metrics> | 文本暴露格式，同样需要 Basic Auth |

```bash
# 命令行查看状态（swiftmqctl 走管理 API，因此 CLI 与内核版本解耦）
./bin/swiftmqctl status
./bin/swiftmqctl list_queues
./bin/swiftmqctl plugins list
./bin/swiftmqctl plugins disable amqp091   # 热停用：AMQP 端口立即关闭，管理面仍在服务
./bin/swiftmqctl plugins enable amqp091    # 热启用：端口恢复，无需重启进程

# 直接调 API
curl -u guest:guest http://127.0.0.1:15672/api/overview
```

> 管理 UI 的前端源码在 `web/`，构建产物 `web/dist` 随仓库提交并经 `go:embed` 打进二进制，
> 因此**部署只需一个二进制**、不装 Node，也没有额外的 Nginx。
> 改前端后必须重新 `npm run build` 并提交产物，否则二进制里跑的仍是旧页面。

### 验证连接是否可用

仓库自带一个真实客户端探针（基于 `rabbitmq/amqp091-go`），覆盖连接、拓扑声明、四种路由、发布消费与确认、TTL/死信、长度限制、持久消息与能力声明：

```bash
cd test/integration/amqp091probe
go run .
```

期望输出（21 个用例）：

```
PASS  M1 正常连接 + Channel 开关 + 优雅关闭
PASS  M1 错误口令应被拒绝
PASS  M1 不存在的 vhost 应被拒绝
PASS  M2 direct 路由：服务端命名队列 + 发布消费 + 手动 ack + FIFO 顺序
PASS  M2 topic 路由：通配匹配 + 同队列多绑定去重
PASS  M2 fanout 路由：一条消息广播到多个队列
PASS  M2 basic.get：有消息返回 Get-Ok，空队列返回 Get-Empty
PASS  M2 消息属性往返：content-type / headers / delivery-mode / correlation-id
PASS  M2 nack(requeue)：重投并置 redelivered=true
PASS  M2 被动声明不存在的队列：404 且只关 Channel
PASS  M2 参数不一致重声明：406
PASS  M2 保留名声明被拒：403
PASS  M2 发布到不存在的交换机：404
PASS  M2 队列 purge / delete 返回正确计数
PASS  M3 发布确认：confirm.select + 逐条 ack + 序号从 1 连续
PASS  M3 TTL 到期进入死信队列（x-death reason=expired）
PASS  M3 nack(requeue=false) 进入死信队列（x-death reason=rejected）
PASS  M3 长度限制 reject-publish：第二条被 basic.nack
PASS  M3 mandatory 未命中：Basic.Return 必须先于 confirm 到达
PASS  M4 durable 队列 + 持久消息：confirm 逐条 ack 且消息可正常消费
PASS  M4 服务端如实声明 connection.blocked 能力

全部通过（21/21）
```

> 崩溃恢复需要重启 broker，无法在探针里覆盖；它由内核单测（`internal/broker/m4_test.go`）
> 与 `internal/store` 的恢复用例验证，含"强杀进程后重启仍能取回已确认消息"的手工验证。

### 其他语言的客户端测试

跨语言兼容性测试放在**独立目录** `swiftmq-test/`（与代码仓库平级，不随本仓库发布）：

| 位置                     | 内容                               |
| ---------------------- | -------------------------------- |
| `swiftmq/test/`        | 只放 Go 测试（内核单测 + `amqp091-go` 探针） |
| `swiftmq-test/python/` | Python（`pika`）冒烟测试，7 个用例         |
| `swiftmq-test/java/`   | Java 用例清单（待补）                    |

这么切分是因为这些测试依赖各语言的运行时与包管理器，与 Go module 的生命周期无关；
放进本仓库会污染 `docker build` 的上下文与 `go vet ./...` 的扫描范围。详见 `swiftmq-test/README.md`。

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

***

## 配置

命令行参数：

| 参数           | 默认值      | 说明                                       |
| ------------ | -------- | ---------------------------------------- |
| `-config`    | 空        | JSON 配置文件路径；不指定则使用内置默认值                  |
| `-log-level` | `info`   | `debug` / `info` / `warn` / `error`      |
| `-log-format`| `text`   | `text`（人读）/ `json`（日志系统采集）               |

`-log-level` / `-log-format` 也可用环境变量 `SWIFTMQ_LOG_LEVEL` / `SWIFTMQ_LOG_FORMAT` 提供（命令行优先）。

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
  "storage": {
    "fsync": "os",
    "flush_interval_ms": 200,
    "memory_high_watermark": 0.4,
    "disk_free_limit": 52428800
  },
  "management": {
    "enabled": true,
    "addr": ":15672"
  },
  "plugins": {
    "amqp091": { "builtin": true, "enabled": true }
  }
}
```

| 字段          | 说明                                                      |
| ----------- | ------------------------------------------------------- |
| `data_dir`  | 节点数据目录（对齐 RabbitMQ 的 `RABBITMQ_MNESIA_DIR` 定位，M4 起真正落盘） |
| `vhosts`    | vhost 清单；`default_vhost` 会自动加入，不会因漏写而连不上                |
| `listeners` | 按**插件名**覆盖监听地址                                          |
| `users`     | 内置用户表，`remote_access: false` 时仅允许本机登录（管理面同样受限）           |
| `plugins`   | 各插件的配置段；内核只读其中的治理开关（`enabled` / `required` / `builtin`），其余原样交给插件 |
| `storage`   | 存储与流控配置段（M4 起生效）                                        |
| `management`| 管理面配置段（M5 起生效）：`enabled` 关闭后不监听任何管理端口                   |

`storage` 字段：

| 字段                      | 默认值       | 说明                                                                                                          |
| ----------------------- | --------- | ----------------------------------------------------------------------------------------------------------- |
| `fsync`                 | `os`      | 落盘档位：`none` / `os` / `batch` / `always`。它同时决定 publisher confirm 的时机：`os` 对齐 RabbitMQ 经典队列"confirm 前不 fsync"，`batch` / `always` 才承诺"收到 confirm 即已落盘" |
| `flush_interval_ms`     | `200`     | 兜底刷盘间隔：消息在内存里最多待多久的上界                                                                                       |
| `memory_high_watermark` | `0.4`     | 内存水位：本进程占用超过"该比例 × 物理内存"即阻塞生产者；`0` 关闭                                                         |
| `disk_free_limit`       | `52428800` | 数据目录剩余空间下限（字节，默认 50 MiB），低于它即阻塞生产者；`0` 关闭                                                  |

`management` 字段：

| 字段        | 默认值       | 说明                              |
| --------- | --------- | ------------------------------- |
| `enabled` | `true`    | 是否启用管理面（HTTP API + 内嵌 UI + 指标） |
| `addr`    | `:15672`  | 管理面监听地址                         |

`plugins` 治理开关：

| 字段         | 默认值    | 说明                                              |
| ---------- | ------ | ----------------------------------------------- |
| `enabled`  | `true` | 为 `false` 时启动阶段不注册扩展点、不建监听；之后仍可用管理 API 热启用      |
| `required` | `false`| 为 `true` 时该插件启动失败会**阻塞内核启动**（仅限官方核心插件）            |
| `builtin`  | `true` | 表示随内核编译进来（外部进程插件形态落地后由部署方声明为 `false`）            |

支持的 `SWIFTMQ_*` 环境变量（优先于配置文件，便于容器化覆盖）：

`SWIFTMQ_DATA_DIR`、`SWIFTMQ_DEFAULT_VHOST`、`SWIFTMQ_AMQP_ADDR`、`SWIFTMQ_FSYNC`、
`SWIFTMQ_FLUSH_INTERVAL_MS`、`SWIFTMQ_MEMORY_HIGH_WATERMARK`、`SWIFTMQ_DISK_FREE_LIMIT`、
`SWIFTMQ_MANAGEMENT_ENABLED`、`SWIFTMQ_MANAGEMENT_ADDR`、`SWIFTMQ_LOG_LEVEL`、`SWIFTMQ_LOG_FORMAT`

> 持久化范围（M4）：只针对 **durable 队列**中的 **`delivery-mode=2`** 消息，与 RabbitMQ 一致。
> 非 durable 队列、瞬时消息与 `fsync: none` 档位都不落盘。
>
> 拓扑元数据（交换机、绑定、队列声明）尚不持久化：重启后需要客户端重新声明队列，
> 重新声明时会自动从磁盘恢复该队列的持久消息。用户与权限的动态变更同样只在内存生效。
>
> YAML 配置暂不支持（需引入解析依赖）；当前用 JSON + `SWIFTMQ_*` 覆盖。

***

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

消息数据用自研分段追加日志 + 队列索引，**不依赖任何外部数据库或协调服务**：每条记录带长度前缀与 CRC32，恢复时丢弃尾部半写记录；索引只记 `seq-id → 位置 / 长度 / 状态`，`ack` 以追加一条删除标记表达（不做原地删除与随机写）。fsync 分四档（`none` / `os` / `batch` / `always`）并与 publisher confirm 时机强绑定：**持久消息只有按档位落盘后才回 `basic.ack`**。

M4 已落地"每队列段 + 队列索引 + 组提交 + 崩溃恢复 + 水位流控"。仍待补齐的是：小消息/大消息分流的 **vhost 共享存储 + 引用计数**、段文件轮转与磁盘回收，以及元数据的内嵌 Raft。

### 管理与观测

管理面属于**兼容性契约**，因此内建而不插件化：运维工具链（`rabbitmqadmin`、监控脚本、管理 UI）都直接指向它，一旦可插拔，"能不能管"就成了可配置项。

- **管理 API 对齐 RabbitMQ 的形状**：字段名、错误体（`{error, reason}`）、`amq.default` 指代默认交换机、`%2F` 指代默认 vhost 等约定都照搬，让现有工具不用改。
- **UI 只消费 `/api/*`**，不引入任何私有接口 —— UI 因此成了 API 兼容性的持续验证者，与 `rabbitmqadmin` 指向同一套接口。
- **UI 产物内嵌**：`web/dist` 经 `go:embed` 打进二进制，部署只有一个文件；CI 会重新构建并校验产物与源码一致，防止"改了前端忘记构建"。
- **可观测**：`/metrics` 用手写的 Prometheus 文本暴露格式（保持内核零第三方依赖、可离线构建），指标覆盖队列深度、未确认数、投递/确认累计、磁盘与内存水位、插件状态。
- **插件治理是能力级的**：热停用一个插件落到实处是**关掉它的 listener 并从协议嗅探候选里摘掉**，而不是只改一个状态位；管理面与内核其它部分不受影响。
- **`swiftmqctl` 走 HTTP API**：CLI 因此不依赖内核内部包、不与内核版本耦合；代价是管理面被停用时 CLI 也不可用（已在文档中说明）。

***

## 项目结构

```
swiftmq/
├── cmd/
│   ├── swiftmqd/            # broker 进程入口
│   └── swiftmqctl/          # 运维 CLI（走管理 HTTP API）
├── internal/
│   ├── protocol/
│   │   ├── codec/           # 基础类型、field-table、帧编解码
│   │   ├── spec/            # 类/方法标识、错误码、软硬错误作用域
│   │   └── amqp091/         # AMQP 0-9-1 协议插件
│   ├── transport/           # 监听、TLS、协议嗅探、按插件热启停监听
│   ├── plugin/              # 插件注册中心、生命周期与治理、Host 句柄
│   ├── broker/              # 内核：vhost、路由模型、队列、死信、水位流控、管理面视图
│   ├── store/               # 持久化：段日志、队列索引、组提交、崩溃恢复
│   ├── management/          # Management HTTP API + Prometheus 指标 + UI 静态服务
│   ├── auth/                # SASL：PLAIN / AMQPLAIN；用户与权限表
│   └── config/              # 配置加载（JSON + SWIFTMQ_*）与默认值
├── pkg/plugin/              # 对外稳定插件 API
├── web/                     # 管理 UI 前端工程（Vue 3 + Vite）；dist 经 go:embed 嵌入
├── test/integration/        # 各语言客户端集成验证（独立 module）
├── configs/                 # 示例配置
└── Dockerfile / docker-compose.yml
```

***

## 开发

```bash
go build ./...        # 构建
go vet ./...          # 静态检查
go test ./...         # 单元测试
gofmt -l .            # 检查格式（应无输出）
```

管理 UI（改前端时）：

```bash
cd web
npm install
npm run dev           # 开发态：HMR + /api 代理到 127.0.0.1:15672
npm run build         # 产出 web/dist（需连同产物一起提交）
npm run type-check    # TypeScript 严格模式检查
```

构建镜像：

```bash
docker build -t swiftmq:0.5.0 .
```

镜像约 13 MB：静态链接二进制 + alpine，**以非 root（uid 10001）运行**，数据目录挂载在 `/var/lib/swiftmq`。

***

## 路线图

| 阶段 | 里程碑 | 内容                                                   | 状态    |
| -- | --- | ---------------------------------------------------- | ----- |
| 一期 | M1  | 协议底座：连接握手、心跳、Channel 开关、插件框架                         | ✅ 已完成 |
| 一期 | M2  | Exchange / Queue / Binding、四种路由、消息收发与确认              | ✅ 已完成 |
| 一期 | M3  | 发布确认、TTL、死信、长度限制、优先级、权限校验                            | ✅ 已完成 |
| 一期 | M4  | 持久化（段日志 + 队列索引）、fsync 档位、崩溃恢复、资源水位流控                      | ✅ 已完成 |
| 一期 | M5  | 管理 HTTP API、管理 UI（Vue 3 + Element Plus）、Prometheus、`swiftmqctl`、插件热启停 | ✅ 已完成 |
| 二期 | M6  | 集群、Quorum Queue 复制、分区处理、故障切换                         | 规划中   |
| 二期 | M7  | 性能打磨、插件化验证（MQTT / AMQP 1.0）                          | 规划中   |

每个里程碑的完成标准是"**真实客户端跑通 + 与 RabbitMQ 行为一致**"，而非"代码写完"。

***

## 贡献

欢迎提交 Issue 与 Pull Request。由于项目强调协议兼容性：

- 修 bug 时请说明对应的 RabbitMQ 行为（版本、客户端、复现步骤）
- 涉及协议细节的改动，请附上与 RabbitMQ 的对照结果
- 提交前请确保 `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` 均通过

***

## 许可证

本项目采用 [Apache License 2.0](LICENSE)。

允许使用、修改、分发（含商业使用），需保留版权与许可声明，且不提供任何担保。

Copyright 2026 houzch（见 [NOTICE](NOTICE)）

***

## 致谢

AMQP 0-9-1 协议规范与 [RabbitMQ](https://www.rabbitmq.com/) 的行为语义是本项目兼容性工作的对照基准。本项目为独立实现，与 RabbitMQ 官方无隶属关系，未使用其代码。
