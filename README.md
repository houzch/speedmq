# SwiftMQ

用 Go 实现的 **AMQP 0-9-1 协议级兼容消息中间件**。目标是让现有 RabbitMQ 各语言客户端**无需改代码、无需换 SDK**，只改连接地址即可接入，并获得与 RabbitMQ 一致的语义。

> 兼容基线：**RabbitMQ 4.3 语义**（AMQP 0-9-1 + RabbitMQ 扩展）。不保留 3.x 与 4.0–4.2 中已被移除的能力（瞬时队列、全局 QoS、Classic Queue v1、经典镜像队列）。

***

## ⚠️ 当前状态：M7（插件化验证：MQTT 3.1.1 + 外部进程插件宿主 + 性能打磨），**仍不可用于生产**

处于早期开发阶段。M6 交付了集群地基（自研 Raft + 元数据复制），M6b 补齐**跨节点消息转发**，M6c 交付**仲裁队列**（每条队列一个 Raft 组，消息复制到多数派后才确认），M6d 让**集群成员可以在运行期增删**。M7 是**插件化验证**，共四个批次：**M7a** 把 MQTT 3.1.1 作为内核内置的**第二个协议插件**落地，验证"新增一种协议不改动内核任何代码"；**M7b** 交付**外部进程（B 形态）插件宿主**（自研零依赖线协议，插件可以是任意语言写的独立进程）；**M7c** 收口插件 DoD（隔离影响面、越权留痕、依赖图自带验证）；**M7d** 做性能打磨（可复现基准 + 一处数据驱动的优化 + 短时混沌）。

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
- **运维 CLI（M5）**：`swiftmqctl`（`status` / `list_queues` / `list_connections` / `list_exchanges` / `list_bindings` / `add_user` / `set_permissions` / `close_connection` / `plugins list|show|enable|disable`），集群相关另有 `cluster_status` / `list_members` / `add_member` / `remove_member`（M6d）
- **插件治理（M5）**：`swiftmqctl plugins` 与管理 API 均可**不重启内核**热启用/停用插件（落到实处是关闭/恢复它的 listener），配置可声明 `enabled` / `required` / `builtin`
- **动态用户与权限（M5）**：通过管理 API / CLI 增删用户与权限
- **集群与高可用（M6，地基）**：`cluster` 配置段定义节点身份与**静态成员表**；自研 Raft（零依赖）负责元数据一致性；**durable 拓扑**（durable 交换机 / durable 非 exclusive 队列 / 绑定）经 Raft 复制到全体节点，在 follower 上写入会自动**转发给 leader**；元数据在**单机模式下也落盘**（`<data_dir>/meta/state.json`），因此交换机与绑定重启后不再丢失
- **集群观测（M6）**：`GET /api/cluster`（模式 / 角色 / 任期 / leader / 成员 / 共识进度 / 元数据规模）与 `GET /api/cluster/name`（RabbitMQ 兼容）；`/api/nodes` 每个节点附带 `swiftmq_cluster` 扩展字段；`swiftmqctl cluster_status`
- **分区保护（M6）**：`partition_policy: pause_minority`（默认）下，节点与多数派失联即暂停服务并断开在途连接，客户端会自动重连到健康节点，避免脑裂产生分叉数据
- **跨节点消息转发（M6b）**：客户端连到**任意节点**都能发布、消费、主动拉取、清空、删除任意队列 —— 队列数据始终在 Owner 节点，非 Owner 节点做代理（发布转发给 Owner、投递由 Owner 推回代理节点）。远端队列也支持绑定（绑定是拓扑，与数据在哪无关）
- **消息跨节点保真（M6b）**：转发的消息用 AMQP 内容头的规范编码承载属性与消息头（**不是 JSON**），因此 `int32` / `double` / `bytes` / `timestamp` / 嵌套 field-table 的类型在跨节点后不变形
- **代理消费者自愈（M6b）**：代理节点每 3 秒续租，Owner 侧超过 15 秒未见续租（或投递直接失败）即摘除该消费者，把它未确认的消息放回队头 —— 代理节点进程崩溃不会留下永久悬挂的消费者，也不会让消息卡在未确认里
- **仲裁队列（M6c）**：`x-queue-type=quorum` 声明**每条队列一个独立 Raft 组**（多组共用集群端口），消息复制到**多数派**后才回 `basic.ack`；单节点故障不丢**已确认**消息。仲裁队列的 `purge` / TTL 过期同样作为日志条目复制，因此副本状态始终一致
- **仲裁队列的 leader 变更（M6c）**：组 leader 变化时，旧 leader 上的消费者会被服务端取消（`CONSUMER_CANCELLED`），未确认消息由新 leader 重投 —— 语义为**至少一次**，客户端应做好重连与幂等
- **动态成员变更（M6d）**：集群成员可在**运行期**增删（`swiftmqctl add_member` / `remove_member` 或管理 API），**不需要改配置文件、也不需要重启任何节点**。新节点先以 learner 身份加入（只复制日志、不投票、不竞选），追平后再提升为投票成员；成员表随 Raft 日志与快照持久化，重启后不会退回配置文件里的初始成员表
- **外部进程（B 形态）插件宿主（M7b）**：插件不必是编译进内核的 Go 包 —— 只要是一个实现了自研线协议的本机进程即可（`pkg/sidecar` 是对外契约，**零第三方依赖**，帧格式为 `[u32 长度][u8 类型][载荷]`，控制面 JSON、数据面原始字节，无 base64 开销）。内核按配置 `spawn` 拉起插件进程并托管其生命周期：握手校验（线协议版本 / 插件 API 版本 / 插件名）、心跳判活、客户端字节流双向代理、**崩溃后内核无感**（状态转 `down`、指标 `swiftmq_plugin_up` 转 0、其他插件与内核照常），并按 `restart: always|never` 策略自动重启/重连
- **插件隔离与留痕（M7c）**：API 版本不匹配、依赖缺失、依赖成环**不再阻塞内核启动** —— 只隔离出问题的那部分插件（隔离沿依赖链传播），内核与其余插件照常运行；失败原因通过管理 API 的 `runtime_note`、`swiftmqctl plugins list/show` 与 `/metrics` 对外可见；插件调用了未声明的能力（如没申请 `net.listen` 却注册协议）会被拒绝且同样留痕
- **性能基线与优化（M7d）**：`test/unit/broker/bench_test.go` 提供可复现基准（小消息发布→消费闭环、1 MiB 大消息、会话建立成本）；据此定位并优化了两处热路径：水位闸门由"每消息加锁"改为**原子快路径**，权限检查对 `.*` 这类"允许一切"的规则**跳过正则引擎**（本机 windows/386 上发布路径 ns/op 下降约 20%，会话建立下降约 38%）
- **错误语义**：`404 / 406 / 403 / 405 / 402 / 540 / 504` 与 RabbitMQ 对齐（软错误只关 Channel，硬错误关连接）
- **MQTT 3.1.1 协议插件（M7）**：内核内置的**第二个**协议插件（`internal/protocol/mqtt`，只依赖 `pkg/plugin` 与标准库）。CONNECT/CONNACK、PUBLISH/PUBACK、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT；QoS 0/1（订阅 QoS2 按规范降级授予 1，入站 QoS2 完整走完四步握手）；Clean Session 映射到内核队列的 durable/autoDelete（持久会话重连后继续投递）；保留消息与遗嘱消息；Keep Alive。**MQTT 主题复用内核的 `amq.topic` 交换机**，因此路由、死信、TTL、持久化、权限对两个协议完全同一套
- 插件框架：注册中心、依赖 DAG 排序、能力审计、失败隔离；AMQP 0-9-1 是**第一个协议插件**（内核不含任何 AMQP 知识）

**尚未实现**

- Direct Reply-To（`amq.rabbitmq.reply-to`）、消费者优先级（`x-priority`）
- **集群的剩余能力**：集群管理 UI 页面（成员的增删已有 CLI 与 API，但还没有界面）
- 用户 / 权限 / vhost 的集群复制（当前只在本地生效；本期复制的是 durable 拓扑）
- vhost 的动态增删
- 策略（policies）接口：`/api/policies` 返回空数组，功能未实现
- 段文件的轮转与磁盘回收（当前每队列单段，删除仅标记）
- 流队列与 Stream 协议
- AMQP 1.0 / STOMP（计划以插件形态提供）；MQTT 3.1.1 已落地（见上文）
- **外部进程插件的已知边界（M7b/M7c）**：外部协议插件拿到的是**原始字节流**，目前还**不能调用内核语义**（队列 / 路由 / 权限）—— 把 `plugin.Session` 桥成 RPC 是后续步骤；因此现阶段它适合做"接入 / 转换类"插件，而不是"需要内核存储与路由"的协议。另外数据面走本机连接**代理转发**（没有文件描述符传递，这是跨平台与零依赖之间的取舍），每次转发多一次内存拷贝。`swiftmqctl` 目前只能通过管理 API 观测外部插件，**不能**代为拉起进程（拉起只能由内核按配置 `spawn`）
- **MQTT 的已知边界**：保留消息存在插件内存（重启丢失）；QoS2 按"至少一次"处理（不做去重）；`x-mqtt-topic` 之外的跨协议主题映射按"."↔"/"反推，主题本身含点号时有歧义（与 RabbitMQ 的 MQTT 插件同）
- **性能：只有单机基线，没有 P99 与长稳数据**。M7d 给出的是本机（windows/386）**吞吐基准与相对改进**，以及 15 轮的插件反复崩溃混沌；**7×24 soak、延迟 P99、真实网络下的连接规模上限均未测**，因此任何绝对性能数字都不应外推
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

默认监听 `5672`（AMQP 0-9-1）、`1883`（MQTT 3.1.1）与 `15672`（管理 UI / HTTP API / 指标）。

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
msg="SwiftMQ 启动中" version=0.11.0 data_dir=data vhost=/ fsync=os
msg="插件 amqp091 v0.1.0（API v1）能力: [net.listen]"
msg="监听已启动" protocol=amqp091 listener=amqp addr=[::]:5672
msg="MQTT 插件已初始化" plugin=mqtt exchange=amq.topic max_packet_size=8388608 prefetch=32
msg="监听已启动" protocol=mqtt listener=mqtt addr=[::]:1883
msg="管理面已启动" component=management addr=[::]:15672 api=/api/overview ui=/
```

### 默认账号（首次登录用这个）

| 项 | 值 |
| --- | --- |
| 用户名 | `guest` |
| 口令 | `guest` |
| 标签 | `administrator`（可读写所有 vhost，并可使用管理面） |

服务起来后直接用这套凭证：

- **管理 UI**：打开 <http://localhost:15672/>，界面会弹出登录框（默认已填好 `guest` / `guest`，直接点"登录"即可）
- **AMQP 客户端**：`amqp://guest:guest@localhost:5672/`
- **命令行**：`./bin/swiftmqctl -user guest -pass guest status`

> ⚠️ **仅供本地开发与试用**。`configs/swiftmqd.json` 里把 `guest` 的 `remote_access` 设为 `true`，
> 是为了让容器内的访问（来源地址是 Docker 网关而非 `127.0.0.1`）不被拒绝。
> 一旦服务对外可访问，请务必更换凭证：改 `configs/swiftmqd.json` 的 `users` 段后重启，或运行期新建账号
> `./bin/swiftmqctl add_user <用户名> <口令> administrator`，
> 再用管理 API 删掉默认账号（`curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest`）。

### 管理与观测（M5）

启动后有两个入口（默认端口与 RabbitMQ 一致）：

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 管理 UI | <http://localhost:15672/> | Overview / Queues / Exchanges / Connections + 队列详情（发布测试消息 / 取消息 / purge / delete） |
| 管理 HTTP API | <http://localhost:15672/api/overview> | RabbitMQ Management API 兼容子集，Basic Auth（默认凭证 `guest` / `guest`，见上文「默认账号」） |
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

> 管理 UI 的前端源码在 `web/`，产物 `web/dist` **不入库**，由构建时生成并经 `go:embed` 打进二进制，
> 因此**部署只需一个二进制**、不装 Node，也没有额外的 Nginx。
> 本地从头编译前需先构建前端（`cd web && npm ci && npm run build`）；若未构建，
> 编译依然会通过，访问 `/` 会返回"管理 UI 未构建"的明确提示（仓库内保留了占位文件 `web/dist/.gitkeep`，
> 因为 `go:embed` 在编译期必须至少匹配到一个文件）。
> 镜像构建不需要你手动构建前端：Dockerfile 里有一个 Node 阶段专门产出 `web/dist`。

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

> 崩溃恢复需要重启 broker，无法在探针里覆盖；它由内核单测（`test/unit/broker/`）
> 与 `test/unit/store` 的恢复用例验证，含"强杀进程后重启仍能取回已确认消息"的手工验证。

MQTT 也有一个同样的真实探针（**手写的 MQTT 3.1.1 客户端，零第三方依赖**），覆盖连接鉴权、通配订阅、QoS0/1/2、保留消息、遗嘱、退订、心跳与畸形报文：

```bash
cd test/integration/mqttprobe
go run . -addr 127.0.0.1:1883
```

期望输出（12 个用例）：

```
PASS  连接 + CONNACK（含鉴权）
PASS  错误口令被拒（CONNACK 0x04）
PASS  协议版本不符被拒（CONNACK 0x01）
PASS  订阅 + SUBACK（QoS1）
PASS  QoS0 发布 → 订阅者收到（含 + 通配）
PASS  QoS1 发布/投递/确认闭环
PASS  QoS2 订阅降级为 QoS1 并完成四步握手
PASS  保留消息：订阅即收到，清空后不再收到
PASS  遗嘱消息：异常断开时下发
PASS  退订后不再投递
PASS  PINGREQ → PINGRESP
PASS  畸形报文（SUBSCRIBE 标志位非法）断开连接

全部通过（12/12）
```

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
    "amqp091": [{ "addr": ":5672" }],
    "mqtt": [{ "addr": ":1883" }]
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
  "cluster": {
    "enabled": false,
    "node_id": "swiftmq@node1",
    "listen": ":25672",
    "partition_policy": "pause_minority",
    "peers": {
      "swiftmq@node1": "10.0.0.1:25672",
      "swiftmq@node2": "10.0.0.2:25672",
      "swiftmq@node3": "10.0.0.3:25672"
    }
  },
  "plugins": {
    "amqp091": { "builtin": true, "enabled": true, "required": true },
    "mqtt": {
      "builtin": true,
      "enabled": true,
      "exchange": "amq.topic",
      "max_packet_size": 8388608,
      "prefetch": 32
    }
  }
}
```

| 字段          | 说明                                                      |
| ----------- | ------------------------------------------------------- |
| `data_dir`  | 节点数据目录（对齐 RabbitMQ 的 `RABBITMQ_MNESIA_DIR` 定位，M4 起真正落盘） |
| `vhosts`    | vhost 清单；`default_vhost` 会自动加入，不会因漏写而连不上                |
| `listeners` | 按**插件名**覆盖监听地址                                          |
| `users`     | 内置用户表；**默认内置 `guest` / `guest`（标签 `administrator`）**，`remote_access: false` 时仅允许本机登录（管理面同样受限） |
| `plugins`   | 各插件的配置段；内核只读其中的治理开关（`enabled` / `required` / `builtin`），其余原样交给插件 |
| `storage`   | 存储与流控配置段（M4 起生效）                                        |
| `management`| 管理面配置段（M5 起生效）：`enabled` 关闭后不监听任何管理端口                   |
| `cluster`   | 集群配置段（M6 起生效）：`enabled` 为 `true` 时才走 Raft，默认关闭时是单机语义                     |

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

`cluster` 字段：

| 字段                 | 默认值              | 说明                                                                             |
| ------------------ | ---------------- | ------------------------------------------------------------------------------ |
| `enabled`          | `false`          | 是否启用集群。为 `false` 时是纯单机：不监听集群端口、元数据落本地 `state.json`                              |
| `node_id`          | `swiftmq@<主机名>`   | 本节点标识，集群内唯一；默认与 `/api/nodes` 的 `name` 同口径                                         |
| `listen`           | `:25672`         | 节点间 RPC 监听地址（对齐 RabbitMQ 的节点间端口）                                                |
| `peers`            | 未启用时为 `{}`       | 集群**地址簿**：`node_id → RPC 地址`，**必须包含本节点**；留空时自动填 `{node_id: listen}`（即单节点集群）。它同时是首次引导时的投票成员集合，此后以持久化的成员表为准 |
| `join`             | `false`          | 本节点以 **learner** 身份加入既有集群：只复制日志、不投票、不发起竞选，等待被 `add_member` 提升为投票成员。仅**首次引导**时生效 |
| `partition_policy` | `pause_minority` | 分区策略：`pause_minority`（与多数派失联即暂停服务并断开在途连接）/ `ignore`（只记录状态，不暂停）                 |

`plugins` 治理开关：

| 字段         | 默认值    | 说明                                              |
| ---------- | ------ | ----------------------------------------------- |
| `enabled`  | `true` | 为 `false` 时启动阶段不注册扩展点、不建监听；之后仍可用管理 API 热启用      |
| `required` | `false`| 为 `true` 时该插件启动失败会**阻塞内核启动**（仅限官方核心插件）            |
| `builtin`  | `true` | 表示随内核编译进来（外部进程插件形态落地后由部署方声明为 `false`）            |

支持的 `SWIFTMQ_*` 环境变量（优先于配置文件，便于容器化覆盖）：

`SWIFTMQ_DATA_DIR`、`SWIFTMQ_DEFAULT_VHOST`、`SWIFTMQ_AMQP_ADDR`、`SWIFTMQ_FSYNC`、
`SWIFTMQ_FLUSH_INTERVAL_MS`、`SWIFTMQ_MEMORY_HIGH_WATERMARK`、`SWIFTMQ_DISK_FREE_LIMIT`、
`SWIFTMQ_MANAGEMENT_ENABLED`、`SWIFTMQ_MANAGEMENT_ADDR`、`SWIFTMQ_CLUSTER_ENABLED`、
`SWIFTMQ_CLUSTER_NODE_ID`、`SWIFTMQ_CLUSTER_LISTEN`、`SWIFTMQ_CLUSTER_JOIN`、`SWIFTMQ_CLUSTER_PARTITION_POLICY`、
`SWIFTMQ_LOG_LEVEL`、`SWIFTMQ_LOG_FORMAT`

> 持久化范围（M4）：只针对 **durable 队列**中的 **`delivery-mode=2`** 消息，与 RabbitMQ 一致。
> 非 durable 队列、瞬时消息与 `fsync: none` 档位都不落盘。
>
> 拓扑元数据（M6 起持久化）：**durable 交换机**、**durable 且非 exclusive 的队列**与它们之间的绑定
> 会落到 `<data_dir>/meta/state.json`（单机）或经 Raft 复制（集群），重启后自动恢复，
> **不再需要客户端重新声明**。transient / exclusive / auto-delete 的对象仍属会话本地，重启即消失。
> 用户与权限的动态变更仍只在本地生效，本期不参与集群复制。
>
> YAML 配置暂不支持（需引入解析依赖）；当前用 JSON + `SWIFTMQ_*` 覆盖。

***

## 集群（M6 / M6b / M6c / M6d）

> **经典队列**（默认）的消息数据仍只在队列的 Owner 节点上：客户端可以连到任意节点发布与消费，
> 但数据落盘与 TTL/死信/长度限制始终由 Owner 执行，Owner 宕机时其持有的消息不可用。
> 需要**跨节点冗余**时使用**仲裁队列**（`x-queue-type=quorum`）：消息按 Raft 复制到多数派，
> 单节点故障不丢已确认消息（见下文「仲裁队列」小节）。

开启集群只需在每个节点配置 `cluster` 段，并保证**各节点的 `vhosts` 列表一致**：

```json
{
  "data_dir": "/var/lib/swiftmq",
  "vhosts": ["/"],
  "cluster": {
    "enabled": true,
    "node_id": "swiftmq@node1",
    "listen": "10.0.0.1:25672",
    "partition_policy": "pause_minority",
    "peers": {
      "swiftmq@node1": "10.0.0.1:25672",
      "swiftmq@node2": "10.0.0.2:25672",
      "swiftmq@node3": "10.0.0.3:25672"
    }
  }
}
```

**怎么工作**

- **元数据一致性**：节点身份与初始成员表来自配置；元数据由**自研 Raft**（零依赖）复制 —— 选主、日志复制、任期与投票先落盘、快照与落后节点追赶。
- **写在哪都行**：在任意节点声明 durable 拓扑，写请求会**转发给 leader** 提交，再按同一顺序应用到全体节点。
- **队列放置**：Owner = 声明该队列的节点（对齐 RabbitMQ 经典的 client-local 放置）。
- **成员怎么变（M6d）**：集群成员可在运行期增删，无需改配置、无需重启；新节点先以 learner 追平再提升为投票成员（见下文「动态成员变更」）。
- **数据怎么走（M6b）**：客户端连到非 Owner 节点时，本节点做代理 —— 发布转发给 Owner，消费则在 Owner 上注册一个**代理消费者**、由 Owner 把投递推回来；`basic.get`、`purge`、`delete` 同样跨节点生效。绑定是拓扑，与数据在哪无关，因此远端队列也能被绑定。
- **消息怎么搬**：转发的消息用 **AMQP 内容头的规范编码**承载属性与消息头（不是 JSON），因此消息头里的 `int32` / `double` / `bytes` / `timestamp` / 嵌套表在跨节点后类型与取值都不变；持久消息在 Owner 侧落盘完成后才回转发应答，代理节点再回给客户端 —— confirm 语义不因跨节点而松动。
- **代理消费者自愈**：代理节点每 3 秒续租；Owner 侧 15 秒未见续租、或投递直接失败（对端进程没了）时，摘除该消费者并把它未确认的消息放回队头。消息最多被重投，不会被丢掉。
- **分区保护**：`pause_minority`（默认）下，与多数派失联的节点会暂停服务并断开在途连接，客户端自动重连到健康节点；这样避免两个分区各自接受写入而产生分叉数据。

**本期限制（有意为之）**

- **投递是同步 RPC**：Owner 的队列投递会等代理节点写客户端 socket，慢客户端会拖慢该队列的投递节奏（批量/流水线转发留给性能里程碑）。
- **至少一次**：转发应答丢失时消息会被重新入队并重投（客户端看到 `redelivered=true`），与 AMQP 自身语义同级，不承诺"恰好一次"。
- **成员变更逐次进行**：一次只允许一个未提交的配置变更（无 joint consensus）；变更期间可能出现短暂的角色抖动（leader 被移除时尤其明显，客户端按 AMQP 语义重连即可）。
- **仲裁队列组的成员不随集群成员变更自动调整**：仲裁队列组在**创建时**按当时的集群投票成员固定下来（`grow`/`rebalance` 留给后续里程碑）。新节点加入后，**新建**的仲裁队列会包含它；把既有仲裁队列扩到新节点需要删除重建。
- 用户 / 权限不参与集群复制。
- **经典队列**的消息数据不复制：Owner 宕机时它持有的（非 durable 或未复制的）消息不可用 —— 需要冗余请改用仲裁队列。
- 集群管理 UI 页面尚未提供（可先用下面的接口与 CLI）。

**动态成员变更（M6d）**

集群成员可以在运行期增删，**不需要改任何节点的配置文件、也不需要重启**：

```bash
# 1. 用 join 模式启动新节点：它只复制日志、不投票、不竞选
#    configs 里把它的 peers 写成"全体成员的地址簿"，并加 "join": true
# 2. 在任意节点上把它加入集群（会等待它追平后再提升为投票成员）
./bin/swiftmqctl add_member swiftmq@node4 10.0.0.4:25672
./bin/swiftmqctl list_members          # 投票成员 / 非投票成员
./bin/swiftmqctl remove_member swiftmq@node3
```

等价的 HTTP 接口：

```bash
curl -u guest:guest -X PUT  http://127.0.0.1:15672/api/cluster/members/swiftmq%40node4 \
     -H 'Content-Type: application/json' -d '{"addr":"10.0.0.4:25672"}'
curl -u guest:guest         http://127.0.0.1:15672/api/cluster/members
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/cluster/members/swiftmq%40node3
```

- **为什么要先 learner 再提升**：新节点通常没有日志，直接作为投票成员会让"多数派"里包含一个还没追上的节点，短暂降低可用性。先 learner 只复制、追平后再提升，对现有集群没有影响。
- **地址随配置变更传播**：新节点的 RPC 地址写在配置变更条目里，复制到全体成员后各自注册，因此**不需要改别人的配置**就能联系上它。
- **成员表会持久化**：它随 Raft 日志与快照落盘，节点重启后不会退回配置文件里的初始成员表。
- **在哪个节点发起都行**：非 leader 节点会把请求转发给 leader（与元数据写入同一条通道）。
- **被移除的节点**：它不再参与投票、也不会计入多数派；它的进程可以继续运行（建议停掉），重新加入需要走一次 `add_member`。

**仲裁队列（M6c）**

`x-queue-type=quorum` 声明的队列，其消息由**一组独立的 Raft 组**负责复制（每条队列一个组，多组共用一个集群端口）。

```python
channel.queue_declare(
    queue="orders",
    durable=True,
    arguments={"x-queue-type": "quorum"},
)
```

- **确认语义**：`basic.publish` 在消息被**多数派**接收后才回 `basic.ack`；因此单节点（少数派）故障不会丢已确认消息。
- **声明约束**（对齐 RabbitMQ）：仲裁队列必须 `durable=true`，且不能用 `exclusive` / `auto-delete`；不支持 `x-expires` / `x-max-priority` / `reject-publish-dlx`，声明这些参数会返回 `406`。
- **leader 变更**：组 leader 变化时，旧 leader 上的消费者被服务端取消（`CONSUMER_CANCELLED`），未确认消息由新 leader 重投 —— 语义为**至少一次**。
- **`/api/queues` 视角**：仲裁队列的 `type` 为 `quorum`，`node` 指向当前组 leader；`messages` / `consumers` 为 leader 上的实时值。
- **与经典队列的取舍**：仲裁队列以「写放大 + 内存占用」换「跨节点冗余」；它更适合对可靠性敏感、队列深度可控的场景。经典队列吞吐更高、更省内存，但不复制。

> 仲裁队列当前的状态（ready 列表）在内存中，受队列深度约束；**分段存储与内存/磁盘流控**留在 M7。因此本阶段请把仲裁队列用于「关键但深度可控」的队列。

**观测**

```bash
curl -u guest:guest http://127.0.0.1:15672/api/cluster        # 模式/角色/任期/leader/成员/共识进度
curl -u guest:guest http://127.0.0.1:15672/api/cluster/name   # RabbitMQ 兼容：集群名
curl -u guest:guest http://127.0.0.1:15672/api/cluster/members # 投票成员 / 非投票成员（M6d）

swiftmqctl cluster_status                                     # 命令行版
swiftmqctl list_members                                       # 成员划分（M6d）
```

`/api/cluster` 返回示例：

```json
{
  "enabled": true,
  "mode": "raft",
  "node_id": "swiftmq@node1",
  "role": "leader",
  "term": 3,
  "leader": "swiftmq@node1",
  "has_quorum": true,
  "paused": false,
  "peers": ["swiftmq@node1", "swiftmq@node2", "swiftmq@node3"],
  "learners": [],
  "commit_index": 12,
  "last_applied": 12,
  "applied_records": 12,
  "object_totals": { "queues": 4, "exchanges": 7, "bindings": 4, "users": 0 },
  "forwarding": {
    "proxy_consumers": 1,
    "remote_consumers": 0,
    "held_deliveries": 2,
    "forwarded_out": 15,
    "forwarded_in": 0,
    "deliveries": 0
  }
}
```

> `forwarding` 是 M6b 的转发运行态：`proxy_consumers` 是本节点代客户端持有的远端消费者数，
> `remote_consumers` 是远端挂在本节点队列上的代理消费者数，`held_deliveries` 是等待远端结算的投递数，
> `forwarded_out` / `forwarded_in` / `deliveries` 是累计转发计数。
> 另外 `/api/queues` 里每个队列的 `node` 字段现在指向**数据所在节点**（远端队列不再报成本节点），
> 队列的 `messages` / `messages_unacknowledged` / `consumers` 也是向 Owner 取来的真实值。

***

## MQTT 3.1.1（M7）

MQTT 是内核里的**第二个协议插件**（`internal/protocol/mqtt`）。它的意义不只是"多支持一个协议"，而是把插件边界真正压到极限：
**它只 import `pkg/plugin` 与标准库，内核没有任何功能性改动**（除版本号常量外，`internal/broker`、`internal/transport`、`internal/plugin`、`pkg/plugin` 一行未改）—— 新增协议对本项目的落地方式就是"新增一个包 + 在组装处多注册一行"。

```bash
# 默认监听 1883（可用 listeners.mqtt 覆盖），与 AMQP 5672 同一进程、同一份队列与权限
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

**主题怎么落到内核**（这是整个插件最需要解释的一处设计）

MQTT 的"主题"直接复用内核的 **`amq.topic` 交换机**，而不是另造一套路由：

| MQTT | 内核 |
| --- | --- |
| 主题 `sensors/room1/temp` | routing key `sensors.room1.temp` |
| 通配 `+`（单层） | `*`（单段） |
| 通配 `#`（多层，含父级） | `#`（零到多段） |
| 订阅（Client ID + QoS） | 一条队列 `mqtt-subscription-q<QoS>-<ClientID>` + 每个过滤器一条绑定 |
| Clean Session=1 | 队列 `durable=false, auto-delete=true`（断开即回收） |
| Clean Session=0 | 队列 `durable=true`（重连后继续投递积压消息） |

这样做的直接后果是**语义不会分裂**：MQTT 消息进的是同一个队列，吃同一套死信 / TTL / 长度限制 / 持久化 / 权限规则，管理面也看到同一份连接、消费者与队列统计。反过来，别的协议发到 `amq.topic` 的消息，MQTT 订阅者同样能收到（主题按 routing key 反推）。

**验证**：`test/unit/mqtt`（真实内核上跑手写 MQTT 客户端，覆盖握手/鉴权、订阅通配、QoS0/1/2、保留消息、遗嘱、退订、持久会话重投、跨协议互通、管理面可见性，以及一个 8 订阅者并发压测）；`test/integration/mqttprobe`（独立 module、**零第三方依赖**的手写 MQTT 客户端，12 项用例，见下）。

**已知边界（有意为之）**

- 保留消息存在**插件内存**，重启丢失（普通消息不受影响，它们在内核队列里）；
- 订阅 QoS2 按规范**降级授予 QoS1**；入站 QoS2 的握手完整，但语义为"至少一次"（不去重）；
- MQTT 没有 vhost 概念，一律使用内核的**默认 vhost**；
- Clean Session=0 的"持久会话"是用 durable 队列近似实现的：重连能拿到积压消息，但 MQTT 5.0 才有的"会话过期/离线队列上限"等细节不涉及。

***

## 外部进程插件（B 形态，M7b / M7c）

内核不仅能加载编译进来的 Go 插件（A 形态），也能托管**独立进程**作为插件（B 形态）。两者对内核是同一件事：外部进程插件在宿主侧被包装成一个普通的 `plugin.Plugin`，因此注册、依赖排序、能力审计、热启停、失败隔离、管理面展示全部复用同一套机制。

**线协议（自研，零第三方依赖）**

内核与插件进程之间是本机连接上的一条多路复用协议，帧格式极简：

```
[u32 长度（含类型字节）][u8 类型][载荷]
类型：Hello / HelloAck / Ping / Pong / Call / Reply / Open / OpenAck / Data / Close
数据帧载荷 = [u32 流号][原始字节]（不是 base64，避免 33% 膨胀；单块上限 64 KiB，边读边转）
```

`pkg/sidecar` 同时提供两端实现：**插件进程**用 `sidecar.Server` 实现 `Hello / Call / Open` 三个方法即可；**内核侧**的 `Client` 负责握手、心跳与多路复用。线协议版本 `v1`，与插件 API 版本（`pkg/plugin.APIVersion`）是两件事，二者不一致都会被**明确拒绝**（拒绝也回一帧说明原因，而不是把连接一关了事）。

**配置与运行**

```jsonc
"plugins": {
  "echo-sidecar": {
    "builtin": false,          // 外部进程插件：不是随内核编译进来的
    "enabled": true,
    "required": false,
    "sidecar": {
      "address": "tcp://127.0.0.1:19001",   // 内核连接插件进程的地址（tcp:// 或 unix://）
      "spawn": ["/path/to/plugin", "-addr", "tcp://127.0.0.1:19001"],  // 内核代为拉起；留空表示由你自己管理
      "restart": "always",      // always：崩溃后自动重启并重连；never：只标记 down，等运维介入
      "protocols": [
        { "name": "echo", "prefix": "ECHO",
          "listeners": [{ "name": "echo", "addr": ":1885" }] }
      ]
    }
  }
}
```

`configs/swiftmqd.json` 里带了一段默认停用的示例（`enabled: false`，不指向任何真实进程），可以直接照着改。

**参考实现**：`test/integration/echosidecar` 是一个**独立 module**（`replace` 指回仓库根，只依赖 `pkg/sidecar` 与标准库）的最小外部插件 —— 它把客户端发来的每一行原样回显，并提供 `stats` / `set_greeting` 两个控制面方法。编译后按上面的 `spawn` 配置指过去即可跑通"嗅探 → 代理 → 外部进程 → 回显"整条链路。

**崩溃之后会发生什么**（这是 B 形态与 A 形态最本质的差别）

| 事件 | 内核侧表现 |
| --- | --- |
| 插件进程被 `kill` | 连接断开 → 该插件状态转 `down`，`/metrics` 的 `swiftmq_plugin_up{...}` 转 `0`，`runtime_note` 带上原因 |
| 内核与其余插件 | **不受影响**：AMQP / MQTT 监听、管理面、其他插件照常服务 |
| `restart: always` | 按 500 ms → 10 s 退避重连，必要时重新拉起进程；恢复后状态回到 `enabled` |
| `restart: never` | 一直保持 `down`，等运维处理 |

**怎么验证**：`test/unit/sidecar`（握手成功/被拒/线协议版本不符/API 版本不符/插件名不符、控制面调用、双向流回显、连接断开可观测、心跳超时判死、宿主状态 enabled→down→自愈）；`test/integration/echosidecar`（参考插件本体）；内核侧真进程 e2e（`spawn` 拉起 → 回显正常 → `kill` 插件 → 指标转 0 且内核端口仍可连 → 自动重启并恢复），外加 15 轮反复杀进程的混沌。

**插件合约错误的影响面（M7c）**

API 版本不匹配、依赖缺失、依赖成环都**不再让内核拒绝启动**：出问题的那部分插件被隔离（状态 `failed`，原因写入 `runtime_note`），隔离会**沿依赖链传播**（A 被隔离后，依赖 A 的 B 也一起隔离，避免它带着坏依赖跑起来）。这类契约问题**不能**用热启用绕过。插件调用了未声明的能力（例如没申请 `net.listen` 却调用 `RegisterProtocol`）会被直接拒绝，且同样留痕。

***

## 性能基线（M7d）

`test/unit/broker/bench_test.go` 提供三个可复现基准，跑的是**真实内核路径**（会话 → 路由 → 队列 → 投递 → 结算），不含协议编解码与网络 IO，因此衡量的是内核自身：

```bash
go test ./test/unit/broker/ -run '^$' -bench . -benchtime 20000x
```

基于 CPU profile 定位到两处热路径上的无谓开销，并做了优化：

1. **水位闸门的读改为原子快路径**：每条消息发布前都要问一次"现在能不能发"，原来要拿一次互斥锁，现在绝大多数情况只是一次原子读（真正阻塞时才拿锁取等待通道）。
2. **权限检查对"允许一切"跳过正则引擎**：默认权限是 `.*`，而权限检查在每条消息的发布/投递路径以及每次会话建立上；现在 `.*` / `^.*$` 直接放行，不再进正则引擎。

本机（windows/386，12th Gen i5-12600KF）实测，优化前后各 5 次的区间：

| 基准 | 优化前 | 优化后 |
| --- | --- | --- |
| `BenchmarkPublishConsume`（256 B，发布→消费闭环） | ~411 ns/op | **292–367 ns/op**（中位 ~307，↓ 约 25%） |
| `BenchmarkSessionOpen`（认证 + 打开 vhost） | ~4179 ns/op | **2549–2670 ns/op**（中位 ~2600，↓ 约 38%） |
| `BenchmarkPublishConsumeLargeBody`（1 MiB） | ~378 ns/op | ~345 ns/op |

> 诚实说明：优化前的数字只有**单次**采样，且这台机器是 **32 位**构建、非生产平台，所以上面只是"同机同条件的前后对比"，**不能**当作 SwiftMQ 的性能承诺。1 MiB 那条尤其要小心：内核面**不拷贝消息体**（消费者拿到的是同一份 `[]byte`），所以它测的其实只有元数据路径 —— 真正的"大消息成本"在协议序列化与存储写入，本基准覆盖不到。


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
│   │   ├── amqp091/         # AMQP 0-9-1 协议插件
│   │   └── mqtt/            # MQTT 3.1.1 协议插件（只依赖 pkg/plugin 与标准库）
│   ├── transport/           # 监听、TLS、协议嗅探、按插件热启停监听
│   ├── plugin/              # 插件注册中心、生命周期与治理、Host 句柄
│   │   └── sidecar/         # 外部进程（B 形态）插件的内核侧宿主
│   ├── broker/              # 内核：vhost、路由模型、队列、死信、水位流控、管理面视图
│   ├── raft/                # 自研 Raft：选主、日志复制、持久化、快照（M6）
│   ├── meta/                # 集群元数据层：Raft 复制 / 本地落盘两种后端（M6）
│   ├── store/               # 持久化：段日志、队列索引、组提交、崩溃恢复
│   ├── management/          # Management HTTP API + Prometheus 指标 + UI 静态服务
│   ├── auth/                # SASL：PLAIN / AMQPLAIN；用户与权限表
│   └── config/              # 配置加载（JSON + SWIFTMQ_*）与默认值
├── pkg/
│   ├── plugin/              # 对外稳定插件 API（A 形态）
│   └── sidecar/             # 外部进程插件的线协议与两端实现（B 形态，零依赖）
├── web/                     # 管理 UI 前端工程（Vue 3 + Vite）；dist 由构建生成并经 go:embed 嵌入
├── test/
│   ├── unit/               # 仓库内单测（外部测试包，只依赖被测包的导出 API；含 mqtt/、sidecar/、broker 基准）
│   └── integration/        # 真实客户端集成验证（独立 module：amqp091probe / mqttprobe / echosidecar）
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
npm ci                # 安装依赖（node_modules 不入库）
npm run dev           # 开发态：HMR + /api 代理到 127.0.0.1:15672
npm run build         # 产出 web/dist（产物不入库，仅用于本地编译/预览）
npm run type-check    # TypeScript 严格模式检查
```

> 产物不入库，所以从零编译内核前要先 `npm run build`；Docker 构建会自动完成这一步。

构建镜像：

```bash
docker build -t swiftmq:0.11.0 .
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
| 二期 | M6  | 集群地基：节点身份与成员表、自研 Raft、元数据复制、分区处理、故障切换、集群观测 | ✅ 已完成 |
| 二期 | M6b | 跨节点消息转发：任意节点可发布/消费/拉取，代理消费者与自愈 | ✅ 已完成 |
| 二期 | M6c | 仲裁队列（Quorum Queue）：每队列一个 Raft 组、多数派确认、leader 变更重投 | ✅ 已完成 |
| 二期 | M6d | 动态成员变更：learner 加入 → 追平 → 提升、运行期移除成员、成员表持久化            | ✅ 已完成 |
| 二期 | M7a | 插件化验证（协议）：MQTT 3.1.1 作为第二个协议插件落地，内核零改动      | ✅ 已完成 |
| 二期 | M7b | 插件化验证（隔离）：外部进程（sidecar）插件宿主、崩溃隔离与健康上报       | ✅ 已完成 |
| 二期 | M7c | 插件 DoD 收口：错误影响面仅限该插件、越权拒绝留痕、依赖边界可验证             | ✅ 已完成 |
| 二期 | M7d | 性能打磨：可复现基准、大消息与连接规模、一处数据驱动优化、短时混沌              | ✅ 已完成 |

| 三期 | M8 | **生产就绪**：段文件回收、AMQP 事务、TLS、权限集群复制、双跑对照、长稳与性能达标（清单与优先级见设计文档 M8 章节） | 规划中 |

每个里程碑的完成标准是"**真实客户端跑通 + 与 RabbitMQ 行为一致**"，而非"代码写完"。

> 二期（M6 / M7）已全部完成，但**这还不等于可用于生产**。M8 是"生产就绪"这一期：在它完成之前，README 顶部会一直保留"不可用于生产"的声明。M8 的出口条件 = 磁盘回收、AMQP 事务、TLS、用户/权限集群复制这四类**硬缺口**补齐，且双跑对照与长稳压测给出可复现证据。

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
