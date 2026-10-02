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
- **管理 UI（M5，M8-15 扩展）**：Vue 3 + Vite + TypeScript + Element Plus，五个页面（Overview / Queues / Exchanges / Connections / **集群**）+ 队列详情，产物经 `go:embed` 打进二进制，**单个二进制即可访问**，无额外静态服务。集群页展示元数据层状态（模式/角色/任期/leader/共识进度/元数据规模）、成员划分与节点资源，并可在界面上**增删集群成员**（单机模式下按钮禁用并说明原因，不会点了才报错）
- **可观测性（M5）**：Prometheus 文本格式 `/metrics`（队列深度、未确认、投递/确认计数、磁盘/内存水位、插件状态）；`-log-format json` 结构化日志
- **运维 CLI（M5）**：`swiftmqctl`（`status` / `list_queues` / `list_connections` / `list_exchanges` / `list_bindings` / `list_vhosts` / `add_vhost` / `delete_vhost` / `add_user` / `set_permissions` / `close_connection` / `plugins list|show|enable|disable`），集群相关另有 `cluster_status` / `list_members` / `add_member` / `remove_member`（M6d）与 `grow_queue` / `rebalance_queue`（M8-15）
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
- **声明合法性对齐 RabbitMQ 4.x**：拒绝声明"**非持久且非独占**"的队列（`541 INTERNAL_ERROR`，对应 RabbitMQ 4.x 起默认禁用的 `transient_nonexcl_queues`，`auto_delete` **不豁免**）；队列重声明的等价性规则也按实测对齐 —— `exclusive` 不一致回 **405 RESOURCE_LOCKED**（两个方向都是），exclusive 队列**不比较** `durable`，`auto_delete` / `arguments` 不一致回 406。这些语义都是**双跑对照测试发现并补齐**的 —— 此前实现与本节声明的"不保留瞬时队列"基线不符
- **MQTT 3.1.1 协议插件（M7）**：内核内置的**第二个**协议插件（`internal/protocol/mqtt`，只依赖 `pkg/plugin` 与标准库）。CONNECT/CONNACK、PUBLISH/PUBACK、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT；QoS 0/1（订阅 QoS2 按规范降级授予 1，入站 QoS2 完整走完四步握手）；Clean Session 映射到内核队列的 durable/autoDelete（持久会话重连后继续投递）；保留消息与遗嘱消息；Keep Alive。**MQTT 主题复用内核的 `amq.topic` 交换机**，因此路由、死信、TTL、持久化、权限对两个协议完全同一套
- 插件框架：注册中心、依赖 DAG 排序、能力审计、失败隔离；AMQP 0-9-1 是**第一个协议插件**（内核不含任何 AMQP 知识）
- **段轮转与磁盘回收（M8-1）**：消息按大小分段（默认 8 MiB），段内消息**全部确认后整段删除**，索引随之压缩重写 —— 磁盘占用不再只增不减（此前每队列单段、删除仅标记）。旧格式索引（无段号）可原样读取，**不需要迁移**
- **AMQP 事务（M8-2）**：`tx.select / tx.commit / tx.rollback` 完整可用；事务在**协议层缓冲**实现（内核零改动），提交前对外不可见、回滚即丢弃，且与发布确认互斥（406）
- **TLS（M8-3）**：协议监听与管理面都可走 TLS，配置项 `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`（`client_auth: require_and_verify` 即双向认证）；证书在**启动时**读取校验 —— 配错了内核直接拒绝启动，而不是等客户端连上来才暴露
- **用户 / 权限的集群复制与持久化（M8-4）**：账号与权限的增删改走元数据层（集群下经 Raft 复制到全体节点，单机下落 `meta/state.json`），**重启后保留**；配置文件里的 `users` 只在**首次引导**时作为初始账号写进元数据，此后以元数据为准（改密码请用管理 API / CLI，不要再改配置文件）
- **策略 policies（M8-6）**：`/api/policies` 的完整 CRUD（`GET /api/policies`、`GET|PUT|DELETE /api/policies/{vhost}/{name}`，字段名与 RabbitMQ 一致：`pattern` / `apply-to` / `definition` / `priority`）。按**名称正则**匹配把一批配置施加到队列与交换机上并**秒级内生效**（已存在的对象会立刻跟着变，不用等重建）：队列侧支持 `max-length` / `max-length-bytes` / `message-ttl` / `expires` / `dead-letter-exchange` / `dead-letter-routing-key` / `overflow`，交换机侧支持 `alternate-exchange`（未路由消息的兜底交换机）。多个策略命中同一对象时**优先级高者生效**；队列自己声明的同名参数**覆盖**策略。策略同样存在元数据层，集群复制、重启保留；命中的策略会体现在队列/交换机对象的 `policy` 与 `effective_policy_definition` 字段上
- **vhost 的动态增删（M8-7）**：`PUT|DELETE /api/vhosts/{vhost}`（新建 201、重复 PUT 204、删除 204、不存在 404，仅 `administrator` 标签可用）与 `swiftmqctl add_vhost / delete_vhost / list_vhosts`。删除是**显式级联**：该 vhost 内的队列 / 交换机 / 绑定 / 权限 / 策略逐条从元数据删除，磁盘上的消息存储目录（`msg_stores/vhosts/<vhost>/`）一并回收，打开着它的连接会被服务端主动断开。**默认 vhost 不可删除**（400）—— 它是内核保证存在的连接落点。vhost 集合由此改为「配置首引导 + 元数据为准」（与账号同一约定）：配置文件里的 `vhosts` 只在首次启动时播种，之后增删请走管理 API / CLI

- **性能基线（M8-10，Linux 容器）**：按第 8 章目标在**容器化 Linux**（WSL2 内核）上实测。1 KiB 消息：**非持久 + confirm 164k msg/s**（限 4C/8G 时 175k）、**持久 + confirm 50.9k**（4C8G 55.2k）、非持久不开 confirm 308k；**P99 发布确认延迟 147–185 µs**（目标 < 5 ms，余量约 30×）。连接规模：双客户端容器合计**建立 100,000 条**连接（broker 侧峰值 86,356）。瓶颈结论有测量支撑：**不是 CPU**（4C 限额下吞吐反而更高）；写路径成本来自**持久化**；连接规模卡在**负载发生器的临时端口**而非 broker。**顺带修掉一个真实缺陷**：内存水位闸门原先按 `MemStats.Sys`（只升不降）判定，一旦触发**永不解除**、生产者被永久阻塞 —— 已改为按在用内存判定并加回归测试。**局限**：容器化 Linux / 单宿主 / 内部网桥（非真实网络）、未测集群与 TLS、无 pprof。详见 `swiftmq-test/bench/REPORT.md`
- **外部进程插件可调用内核语义（M8-12）**：外部进程（B 形态）插件不再只能拿原始字节流 —— 它可以经一次**反向调用**（插件 → 内核，与内核 → 插件的正向调用共用同一条连接、各自编号）触达内核语义：开关会话（`session.open` / `session.close`，走与协议插件**相同**的 vhost 与权限校验）、交换机/队列的声明与删除、交换机/队列绑定、发布（持久化等待在应答前完成，与 `confirm` 语义一致）、主动拉取、消费（内核把投递经 `session.deliver` 回推，插件用 `session.settle` 结算 `ack` / `requeue` / `reject`）、清空队列。`pkg/sidecar` 只新增方法名常量与自包含 JSON DTO（**仍零依赖**），类型化的友好封装由插件自己写（`test/integration/echosidecar` 里有一份薄封装与 `-session-demo` 演示）
- **长稳与混沌（M8-9，缩比口径）**：配套工程新增 `swiftmq-test/soak/`（一键编排 + 报告）。**单节点长稳**：持续「发布(confirm) + 消费(手动 ack)」5 分钟共 **87,533** 条，`missing=0`；每 5s 采样 RSS / 进程句柄数 / 数据目录磁盘，**句柄 60 个采样点恒为 11（零增长）**、RSS 无单调趋势、磁盘涨到 ~8 MiB 段上限即**整段回收**（5 分钟内 3 次）—— 三项均判定**不泄漏**。**三节点集群混沌**：Raft 集群 + 仲裁队列，5 轮随机 `docker kill`（其中 3 轮杀中的正是 leader），5/5 轮客户端自动重连、业务继续推进、多数派 1.2–5.4s 恢复、被重启节点 3.0–9.3s 归队，**已确认消息一条不丢**（`missing=0`；重复投递 ~20% 属"至少一次"语义）。**这是缩比验证**（5 分钟 vs 7×24、单机 Docker Desktop），**未覆盖**网络分区、磁盘写满、慢消费者、10 万连接，详见 `swiftmq-test/soak/REPORT.md` 的「未覆盖」一节
- **跨语言客户端矩阵：Java（M8-8）**：官方客户端 `com.rabbitmq:amqp-client` 5.37.0 的 12 个用例（`swiftmq-test/java/`），与 Python 一样纳入 `compare.py` 的**双跑对照**（同一份用例分别打 RabbitMQ 与 SwiftMQ，逐条比对客户端观察到的行为）。它**当场抓到并修掉一个真实缺陷**：SwiftMQ 把"放行暂存投递"写在了"回 `basic.consume-ok`"之前，两个协程抢着写帧，于是投递可能先于 `consume-ok` 到达 —— amqp-client 在 consumer-tag 为空时由**服务端**生成 tag（只能从 `consume-ok` 得知），必然踩中并报 `unsolicited delivery` 断开连接；而 pika / amqp091-go 自己生成 tag，侥幸避开了这个竞态。修复见 `internal/protocol/amqp091/channel_methods.go` 的 `handleConsume`
- **Direct Reply-To 与消费者优先级（M8-14）**：两项 RPC 场景常用能力，语义全部由**双跑对照实测**钉死（不照抄文档）。
  ① `amq.rabbitmq.reply-to` 伪队列 —— 属于 AMQP 0-9-1 专有约定，因此实现在协议层（`internal/protocol/amqp091/direct_reply.go`），内核只看到"一条每 channel 独占的普通队列"；`server-properties.capabilities` 已声明 `direct_reply_to: true`。实测规则：伪队列是**每 channel**的（同一连接的两个 channel 各有一份，跨 channel 发布请求会 406）；必须 `no-ack=true` 消费（`no-ack=false` → 406 `reply consumer cannot acknowledge`，同 channel 重复消费 → 406 `reply consumer already set`）；应答能回来的真正机制是**属性改写** —— 服务端发布把请求里的 `reply_to` 换成该 channel 的应答队列名，应答方（可以是**另一条连接**，甚至另一个用户）按这个值发布到默认交换机即可回到发起方；某个 channel 没有伪队列消费者却发布带该 `reply_to` 的消息 → 406 `fast reply consumer does not exist`（软错误，只关 channel）；把 `amq.rabbitmq.reply-to` 直接当 routing key 发布**没有**特殊语义（就是一条未路由消息，`mandatory` 时回 312）。伪队列名的 `queue.declare` 被当作被动探测（一定回 `message_count=0` / `consumer_count=1`），`queue.delete` 回 `Delete-Ok(0)`。
  ② 消费者优先级 `x-priority` —— `basic.consume` 的 arguments 里带整数优先级（缺省 0，越大越优先；只校验类型不校验范围，实测 300 / -1 都被 RabbitMQ 接受，非整数回 406 且报文里带队列名与 vhost）；`capabilities` 已声明 `consumer_priorities: true`。实测规则：**只在多个消费者都还有投递额度时**决定谁先拿消息 —— 优先给优先级最高的那个（与注册顺序无关），同优先级之间仍是**轮询**；高优先级消费者把 `prefetch` 额度占满后，低优先级消费者照常收到消息（**不会被饿死**）。内核侧的分配规则在 `internal/broker/queue.go` 的 `nextConsumerLocked`
  两项均有 Go 单测（`test/unit/amqp091/`、`test/unit/broker/consumer_priority_test.go`）与 Python 对照用例（`direct_reply_to` / `direct_reply_to_roundtrip` / `consumer_priority`，两侧一致）覆盖；真实客户端探针 25 项照常全通过
- **集群管理 UI 页面 + 仲裁队列扩副本 / 再平衡（M8-15）**：
  ① **集群页**（`web/src/views/ClusterView.vue`，路由 `#/cluster`）：展示模式 / 角色 / 任期 / leader / 投票与非投票成员 / 是否有多数派 / 是否暂停服务 / 共识进度（commit、applied、累计条数）/ 元数据规模（队列/交换机/绑定/用户）/ 跨节点转发计数 / 各节点资源（`GET /api/nodes`），并支持在界面上**增删集群成员**（走已有的 `PUT|DELETE /api/cluster/members/{name}`）。单机模式（`enabled=false`）下成员操作**直接禁用并给出说明**，而不是点了才收到 501。
  ② **仲裁队列副本集可见**：`GET /api/queues/{vhost}/{name}` 的队列对象上为仲裁队列增加 `members`（投票成员，沿用 RabbitMQ 字段名）、`leader` 与扩展字段 `swiftmq_quorum`（`replicas` / `voters` / `learners` / `count`）。
  ③ **`grow`（扩副本）**：`PUT /api/queues/{vhost}/{name}/grow`（body `{"count":N}`）与 `swiftmqctl grow_queue <vhost> <name> <count>`。声明时可用 `x-quorum-initial-group-size`（对齐 RabbitMQ）指定初始副本数；`grow` 把目标节点从 learner **提升为投票成员**并把副本集写进**元数据**（`meta.Queue.Replicas`，经 Raft 复制、重启后仍是 N 副本）。**只增不减**：缩小副本数、超过集群成员数、对经典队列操作分别返回 400 / 400 / 400，队列不存在 404，单机模式 501。
  ④ **`rebalance`（leader 再平衡）**：`PUT /api/queues/{vhost}/{name}/rebalance` 与 `swiftmqctl rebalance_queue <vhost> <name>`。把该队列的 leader 从"承担 leader 最多的节点"迁到同组中较空的投票成员（raft 新增最小化的 `TimeoutNow` + `TransferLeadership`）。**边界见下文「集群的已知边界」**。
  ⑤ 单测：`test/unit/broker/quorum_replication_test.go`（初始副本数、扩副本、幂等、边界、重启后仍是 N 副本、杀一台机器后队列仍可用、rebalance 契约、单机模式拒绝）。真实集群验证记录见交付说明。

- **运维配套（M8-16）**：新增 [`docs/ops/`](docs/ops/) —— `upgrade.md`（升级/迁移：裸机与容器步骤、灰度与回滚、**数据兼容性**按代码与实测写明、集群滚动升级顺序）、`backup-restore.md`（备份什么、一致性要求、单机/集群恢复步骤、恢复后如何校验，附**一次真实演练全文**：5 条持久消息 + vhost/用户/权限/策略 全部恢复）、`monitoring/`（10 条 Prometheus 告警规则 + 14 面板 Grafana 仪表盘；**指标名逐条对照真实 `/metrics` 输出**，集群类告警**故意不写**因为 `/metrics` 里没有集群指标）、`security-baseline.md`（可勾选清单：TLS/权限/暴露面/容器加固，每条给"为什么 + 怎么验证"，并**明确列出当前做不到的**：口令明文、无审计日志、无 LDAP/OAuth2、SASL EXTERNAL 未实现等）

**本轮的明确非目标（已结项，不是遗漏）**

- **AMQP 1.0 / STOMP 协议插件**、**流队列 / Stream 协议**：P2 项，**不开工** —— 插件化架构已由「AMQP 0-9-1 + MQTT 两个协议插件 + 外部进程插件 + M8-12 的内核语义桥」证明成立；这两项属于**新增协议/新消费模型**，工作量接近独立里程碑，放在 P1 收口后单独排期（不阻塞"可用于生产"的出口条件）
- **YAML 配置**：**不引入** —— 需要引入解析依赖，与"内核零第三方依赖"的硬约束冲突。替代口径是现有的 JSON 配置 + 完整字段表 + `SWIFTMQ_*` 环境变量覆盖（未做 JSONC / 配置热重载，记录为后续可选项）
  理由与验证方式的完整记录见设计文档的 M8-11 / M8-13 / M8-17 行

**尚未实现**

- **集群的剩余能力**：集群管理 UI 页面与仲裁队列 `grow` / `rebalance` 已在 M8-15 落地（见上文）；仍缺：仲裁队列**缩容**与副本在节点间**迁移数据**、跨节点转发的流水线/批量优化、以及真实网络下的分区演练（见下「集群的已知边界」）
- 流队列与 Stream 协议
- AMQP 1.0 / STOMP（计划以插件形态提供）；MQTT 3.1.1 已落地（见上文）
- **外部进程插件的已知边界（M7b/M7c/M8-12）**：外部协议插件**已能调用内核语义**（队列 / 路由 / 权限，见 M8-12），不再只做"接入 / 转换类"插件。剩余边界：**数据面仍走本机连接代理转发**（没有文件描述符传递，这是跨平台与零依赖之间的取舍），每次转发多一次内存拷贝；内核语义调用是**一次本机 RPC**，比进程内调用多一次 JSON 编解码与内存拷贝，且同一消费者上的投递逐条串行回推（等插件应答后才推下一条，顺序性有保证但吞吐受 RTT 限制），**性能开销未单独度量**；内核**主动取消**消费者（队列被删等）时只停止回推，**不向插件下发取消通知**（插件不会收到 `basic.cancel` 式的告知）；`swiftmqctl` 目前只能通过管理 API 观测外部插件，**不能**代为拉起进程（拉起只能由内核按配置 `spawn`）
- **MQTT 的已知边界**：保留消息存在插件内存（重启丢失）；QoS2 按"至少一次"处理（不做去重）；`x-mqtt-topic` 之外的跨协议主题映射按"."↔"/"反推，主题本身含点号时有歧义（与 RabbitMQ 的 MQTT 插件同）
- **性能：已有容器化 Linux 的基线数字，但仍不是承诺平台的定论**。M7d 给的是本机（windows/386）的相对改进；M8-9 给了缩比的长稳与混沌证据；**M8-10 给了容器化 Linux 上的吞吐 / P99 / 连接规模与瓶颈分析**（见上文与 `swiftmq-test/bench/REPORT.md`）。**仍未做**：裸机 Linux（当前是 WSL2 容器）、真实网络（当前是 Docker 内部网桥）、真实生产硬件上的**稳定持有 10 万连接**、集群/仲裁队列/TLS 下的性能、以及真正的 7×24 长稳。因此这些数字可以说明"量级达标"，**不应外推**为你环境里的容量承诺
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
msg="SwiftMQ 启动中" version=0.14.0 data_dir=data vhost=/ fsync=os
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
| 身份 | **总管理员**（总账号：不可删除/禁用/降级，且只有本人能改自己） |

服务起来后直接用这套凭证：

- **管理 UI**：打开 <http://localhost:15672/>，界面会弹出登录框（默认已填好 `guest` / `guest`，直接点"登录"即可）
- **AMQP 客户端**：`amqp://guest:guest@localhost:5672/`
- **命令行**：`./bin/swiftmqctl -user guest -pass guest status`

> 🔒 **首次登录必须改掉总账号**。新装实例的总账号带 `must_change_password` 标记，管理 UI
> 登录后会**强制**弹出对话框，要求同时修改**账号名**与**口令**（默认的 `guest/guest` 必须换成
> 你自己的），改完才能进入管理后台。也可以直接调 API 完成同一件事：
>
> ```bash
> curl -u guest:guest -X POST -H 'Content-Type: application/json' \
>   -d '{"name":"admin","password":"<新口令>"}' \
>   http://127.0.0.1:15672/api/users/guest/credentials
> ```
>
> 该调用会让**旧凭据立即失效**（`guest/guest` 从此不可用），账号名与权限记录会一并迁移。
> 账号与权限的日常管理在管理 UI 的「账号」页，或走管理 API（`/api/users`、`/api/permissions`）。

> ⚠️ **仅供本地开发与试用**。`configs/swiftmqd.json` 里把 `guest` 的 `remote_access` 设为 `true`，
> 是为了让容器内的访问（来源地址是 Docker 网关而非 `127.0.0.1`）不被拒绝。
> 一旦服务对外可访问，请务必更换凭证 —— 走上面的首次改密流程，或改 `configs/swiftmqd.json` 的
> `users` 段后重启。
>
> 说明：**强制改密在管理 UI 层实施**，管理 HTTP API 不做自造的全局拦截，以保持与 RabbitMQ
> Management API 的行为兼容（自动化脚本与 `rabbitmqadmin` 不受影响）。

#### 账号权限的两个维度

账号"能做什么"由**标签**决定，再叠加两处更细的授权。管理 UI 里都是勾选 / 选择，**不需要手写正则**。

| 维度 | 管什么 | 怎么配 | 默认 |
| --- | --- | --- | --- |
| 标签 | 管理面的读 / 写档位（对齐 RabbitMQ） | 勾选 `administrator` / `management` / `monitoring` | 无标签 = 用不了管理面 |
| vhost 权限 | 该账号在**某个 vhost** 里能否声明拓扑 / 发布 / 消费 | 选**权限档位**（完全管理 / 只读 / 只发布 / 只声明拓扑）+ **资源范围**（全部资源 / 指定前缀） | 不配 = 该 vhost 一律拒绝（对齐 RabbitMQ） |
| 管理接口功能组 | 该账号能访问**哪些管理 API** | 勾选功能组：概览与节点 / 队列与交换机 / 连接与通道 / 账号与权限 / 策略 / 虚拟主机 / 集群 / 插件 | **不勾 = 不限制**（用标签允许的全部接口，既有工具与脚本行为不变） |

- 越权时管理 API 回 **403**，`reason` 写明缺的是哪个功能组（例：`未被授予「队列与交换机」管理接口权限`）。
- `GET /api/whoami` 与"改自己的凭据"（`POST /api/users/{name}/credentials`）**不受功能组限制** ——
  否则账号连自己的初始口令都改不了。
- 接口级字段名是 `api_groups`（数组）：空数组 = 不限制。它是标签之上的**收窄**，不替代标签。

### 管理与观测（M5）

启动后有两个入口（默认端口与 RabbitMQ 一致）：

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 管理 UI | <http://localhost:15672/> | Overview / Queues / Exchanges / Connections / 集群 + **队列与交换机的新增（声明）/ 删除** + 队列详情（发布测试消息 / 取消息 / purge / delete）+ **账号与权限**（建号 / 改密 / 启停 / vhost 权限按预设选 / 管理接口功能组勾选） |
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

> **管理 UI 的「新增队列 / 新增交换机」不是 UI 自研功能**，而是两个**声明端点**（RabbitMQ 的 UI 也是这么做的：
> 队列页那个「Add a new queue」就是一个 `method=PUT` 的表单）。所以脚本可以完成同样的事：
>
> ```bash
> # 声明队列（仲裁队列用 arguments:{"x-queue-type":"quorum"} 表达）
> curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
>   -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
>   http://127.0.0.1:15672/api/queues/%2F/my.queue
> # 声明交换机（type: direct / fanout / topic / headers）
> curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
>   -d '{"type":"topic","durable":true,"auto_delete":false,"internal":false}' \
>   http://127.0.0.1:15672/api/exchanges/%2F/my.exchange
> # 删除交换机（?if-unused=true 时仍有绑定会被拒）
> curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/exchanges/%2F/my.exchange
> ```
>
> 语义与 AMQP 的 `queue.declare` / `exchange.declare` **完全一致** —— 它复用的就是同一段内核逻辑：
> 以调用方身份执行，受 `configure` 权限（403）与保留名（403）约束，参数不一致按等价性检查拒绝。
> 状态码对齐 RabbitMQ 实测：**新建 201、已存在且参数等价 204、参数不等价或类型非法 400**。

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

期望输出（25 个用例）：

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
PASS  M8 事务：commit 前的发布对外不可见，commit 后可消费
PASS  M8 事务：rollback 丢弃已缓冲的发布
PASS  M8 事务：与发布确认互斥（406，只关 channel）
PASS  M8-5 frame-max 协商：低于下限被拒（8192），下限值可用且大消息可分片

全部通过（25/25）
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
| `swiftmq-test/python/` | Python（`pika`）用例，33 个，已纳入双跑对照   |
| `swiftmq-test/java/`   | Java（`amqp-client`）用例，12 个，已纳入双跑对照 |

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
    "amqp091": [
      { "addr": ":5672" },
      { "addr": ":5671", "tls": { "cert_file": "/etc/swiftmq/cert.pem", "key_file": "/etc/swiftmq/key.pem" } }
    ],
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
    "addr": ":15672",
    "tls": {
      "cert_file": "/etc/swiftmq/cert.pem",
      "key_file": "/etc/swiftmq/key.pem",
      "ca_file": "/etc/swiftmq/ca.pem",
      "client_auth": "verify",
      "min_version": "1.2"
    }
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
| `vhosts`    | vhost 清单，**只在首次引导时生效**（随后 vhost 集合以元数据为准，见 M8-7）；`default_vhost` 会自动加入，不会因漏写而连不上。给已有实例加 vhost 请用管理 API / `swiftmqctl add_vhost` |
| `listeners` | 按**插件名**声明监听清单：第 i 项沿用该插件第 i 个默认监听的名字，多出来的项是**新增**监听（于是"同一插件明文 + TLS 并存"只要列两个 `addr` 即可） |
| `users`     | 内置用户表，**只在首次引导时生效**（随后账号以元数据为准，见下文持久化说明）；**默认内置 `guest` / `guest`（标签 `administrator`）**，`remote_access: false` 时仅允许本机登录（管理面同样受限） |
| `plugins`   | 各插件的配置段；内核只读其中的治理开关（`enabled` / `required` / `builtin`），其余原样交给插件 |
| `storage`   | 存储与流控配置段（M4 起生效）                                        |
| `management`| 管理面配置段（M5 起生效）：`enabled` 关闭后不监听任何管理端口                   |
| `cluster`   | 集群配置段（M6 起生效）：`enabled` 为 `true` 时才走 Raft，默认关闭时是单机语义                     |

`listeners.<插件>[].tls` 与 `management.tls` 共用同一套 `tls` 字段（M8-3 起）：

| 字段            | 默认值      | 说明                                                                                                   |
| ------------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `cert_file`   | 空        | 服务端证书链（PEM）。**与 `key_file` 同给**才算开启 TLS；只给其一即启动报错                                                        |
| `key_file`    | 空        | 服务端私钥（PEM）                                                                                           |
| `ca_file`     | 空        | 校验**客户端**证书用的 CA（PEM）；双向认证时必给                                                                            |
| `client_auth` | `none`   | 客户端证书策略：`none` / `request` / `require` / `verify_if_given` / `require_and_verify`。后两者要求同时提供 `ca_file` |
| `min_version` | `1.2`    | 最低 TLS 版本：`1.2` / `1.3`                                                                                |

> 证书在**启动时**读取并解析：配置写错立刻拒绝启动，而不是等第一个客户端连上来才暴露。
> 明文与 TLS **可以并存**（同一插件下给多个 `addr`，各带各的 `tls` 即可），默认不开启 TLS。


`storage` 字段：

| 字段                      | 默认值       | 说明                                                                                                          |
| ----------------------- | --------- | ----------------------------------------------------------------------------------------------------------- |
| `fsync`                 | `os`      | 落盘档位：`none` / `os` / `batch` / `always`。它同时决定 publisher confirm 的时机：`os` 对齐 RabbitMQ 经典队列"confirm 前不 fsync"，`batch` / `always` 才承诺"收到 confirm 即已落盘" |
| `flush_interval_ms`     | `200`     | 兜底刷盘间隔：消息在内存里最多待多久的上界                                                                                       |
| `memory_high_watermark` | `0.4`     | 内存水位：本进程**在用**内存（Go 的 live heap + 栈）超过"该比例 × 物理内存"即阻塞生产者，回落到阈值以下自动解除；`0` 关闭。**口径说明**：用的是"在用"而不是"申请过的地址空间（`MemStats.Sys`）"—— 后者是只升不降的高水位，会让闸门一旦触发就再也解除不了（M8-10 实测到的缺陷，已修并加回归测试 `TestWatermarkGateIsRecoverable`） |
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
> 用户与权限（M8-4 起持久化并复制）：增删改同样走元数据层，单机落 `state.json`、集群经 Raft 复制，
> 重启后保留。**配置文件里的 `users` 只在首次引导时生效**（写进元数据后即以元数据为准），
> 因此运行期用管理 API 改过的口令不会被重启打回配置里的旧值；运行期删掉的账号也不会复活。
>
> YAML 配置暂不支持（需引入解析依赖）；当前用 JSON + `SWIFTMQ_*` 覆盖。

***

## 集群（M6 / M6b / M6c / M6d）

> **经典队列**（默认）的消息数据仍只在队列的 Owner 节点上：客户端可以连到任意节点发布与消费，
> 但数据落盘与 TTL/死信/长度限制始终由 Owner 执行，Owner 宕机时其持有的消息不可用。
> 需要**跨节点冗余**时使用**仲裁队列**（`x-queue-type=quorum`）：消息按 Raft 复制到多数派，
> 单节点故障不丢已确认消息（见下文「仲裁队列」小节）。

开启集群只需在每个节点配置 `cluster` 段。`vhosts` 自 M8-7 起只是**首次引导**的种子
（vhost 集合以元数据为准，经 Raft 复制到全体节点），因此不必再强求各节点列表逐字一致 ——
但首次启动前把各节点配成同一份仍然更省心：

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
- **仲裁队列的副本集**：声明时由 `x-quorum-initial-group-size` 定下（默认 = 当时的集群成员数），之后可用 `grow` **只增不减**地扩大；副本集存在元数据里（`meta.Queue.Replicas`），因此重启后仍是 N 副本。**边界**：不支持缩容；副本只在 `cluster.peers` 地址簿内的节点之间选择；非副本节点以 learner 身份一直在复制该组日志（因此 `grow` 只是"提权"，不搬数据，也没有数据迁移窗口）；`grow` 不改变拓扑的 Owner / 绑定关系。
- **`rebalance` 只迁移 leader**：把某条仲裁队列的 leader 从"承担 leader 最多的节点"迁到同组中较空的投票成员（raft 的 `TimeoutNow` + `TransferLeadership`）。**边界**：不改变副本集、不搬数据、不做全局最优调度、不做流量控制；目标是该队列自己的投票成员（选谁接任由 Raft 选举规则保证安全）；当 leader 的负载不比最空的投票成员多时不做无谓换届（响应里 `moved=false` + `reason`）。
- **集群管理 UI / API 的集群专属操作在单机模式下不可用**：单机没有 Raft 成员表也没有副本集，成员增删、`grow`、`rebalance` 一律返回 **501 NOT_IMPLEMENTED**；UI 上对应的按钮直接禁用。
- 用户 / 权限已随元数据复制（M8-4），但**口令是明文**存储与复制的（与配置文件口径一致，见设计 10.2）；哈希与外部认证后端留给认证插件。
- **经典队列**的消息数据不复制：Owner 宕机时它持有的（非 durable 或未复制的）消息不可用 —— 需要冗余请改用仲裁队列。

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
- **`/api/queues` 视角**：仲裁队列的 `type` 为 `quorum`，`node` 指向当前组 leader；`messages` / `consumers` 为 leader 上的实时值。队列对象上另有 `members`（投票成员）、`leader` 与扩展字段 `swiftmq_quorum`（`replicas` / `voters` / `learners` / `count`）。
- **与经典队列的取舍**：仲裁队列以「写放大 + 内存占用」换「跨节点冗余」；它更适合对可靠性敏感、队列深度可控的场景。经典队列吞吐更高、更省内存，但不复制。

> 仲裁队列当前的状态（ready 列表）在内存中，受队列深度约束；**分段存储与内存/磁盘流控**留在 M7。因此本阶段请把仲裁队列用于「关键但深度可控」的队列。

**仲裁队列的扩副本与再平衡（M8-15）**

副本数在声明时用 `x-quorum-initial-group-size` 指定（省略 = 当时的集群成员数）；此后可在运行期**扩副本**（`grow`，只增不减）与**再平衡 leader**（`rebalance`）：

```python
# 1 副本起步（仅 3 节点集群里的一个节点持有投票权）
channel.queue_declare(queue="orders", durable=True, arguments={
    "x-queue-type": "quorum",
    "x-quorum-initial-group-size": 1,
})
```

```bash
# 扩到 3 副本：把另外两个节点从 learner 提升为投票成员（副本集写进元数据，重启后仍生效）
swiftmqctl grow_queue / orders 3
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/queues/%2F/orders/grow \
     -H 'Content-Type: application/json' -d '{"count":3}'

# 把 leader 从"承担 leader 最多的节点"迁到同组中较空的投票成员
swiftmqctl rebalance_queue / orders
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/queues/%2F/orders/rebalance
```

- `grow` 的响应（与队列对象上的 `swiftmq_quorum` 同构）：`{"vhost":"/","queue":"orders","leader":"swiftmq@n1","replicas":[...],"count":3,"voters":[...],"learners":[]}`。
- 错误码：不是仲裁队列 / 目标超过集群成员数 / 试图缩容 → **400 PRECONDITION_FAILED**；队列不存在 → **404**；单机模式 → **501 NOT_IMPLEMENTED**。
- `rebalance` 的响应：`{"vhost":"/","queue":"orders","moved":true,"from":"swiftmq@n2","to":"swiftmq@n1","reason":""}`；无需迁移时 `moved=false` 并给出 `reason`。
- 边界（不做的部分）：**不支持缩容**；不在 `cluster.peers` 之外的节点上放副本；`grow` 不搬数据（非副本节点本来就是 learner，一直在复制）；`rebalance` 只迁 leader、不做全局最优调度与流量控制。

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

## 外部进程插件（B 形态，M7b / M7c / M8-12）

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

**参考实现**：`test/integration/echosidecar` 是一个**独立 module**（`replace` 指回仓库根，只依赖 `pkg/sidecar` / `pkg/plugin` 与标准库）的最小外部插件 —— 它把客户端发来的每一行原样回显，并提供 `stats` / `set_greeting` 两个控制面方法。编译后按上面的 `spawn` 配置指过去即可跑通"嗅探 → 代理 → 外部进程 → 回显"整条链路；加 `-session-demo` 启动时，它还会在每条流上演示一次下面的内核语义桥（声明队列 → 发布 → 消费 → 结算，见 `session.go`）。

**崩溃之后会发生什么**（这是 B 形态与 A 形态最本质的差别）

| 事件 | 内核侧表现 |
| --- | --- |
| 插件进程被 `kill` | 连接断开 → 该插件状态转 `down`，`/metrics` 的 `swiftmq_plugin_up{...}` 转 `0`，`runtime_note` 带上原因 |
| 内核与其余插件 | **不受影响**：AMQP / MQTT 监听、管理面、其他插件照常服务 |
| `restart: always` | 按 500 ms → 10 s 退避重连，必要时重新拉起进程；恢复后状态回到 `enabled` |
| `restart: never` | 一直保持 `down`，等运维处理 |

**把 `plugin.Session` 桥成 RPC（M8-12）**

外部进程插件最初只能拿到**原始字节流**。M8-12 之后，它可以在自己的流上打开一个内核会话，用**反向调用**（插件 → 内核）直接调用内核语义 —— 这些调用在内核侧**一对一映射**到 `plugin.Session`，因此 vhost、权限、路由、队列语义、确认、死信、TTL 与进程内协议插件**完全同一套**，不存在"外部插件另有一套简化语义"。

协议层面：正向调用（内核 → 插件）与反向调用（插件 → 内核）走在同一条连接上，两端**各自从 1 开始编号**，因此应答帧带 `reverse` 标志、两端按标志路由到各自的等待表（否则 ID 撞车会唤醒错误的等待者）。这一点有专门的并发回归用例。

插件侧拿到的句柄：`sidecar.BridgeFromContext(ctx)`（挂在 `Handler` 的 `ctx` 上，**不改** `Handler` 接口，老插件实现在新宿主下仍旧可用）。可用的内核语义调用：

| 方法 | 参数要点 | 说明 |
| --- | --- | --- |
| `session.open` / `session.close` | `{stream, vhost}` / `{stream}` | 打开 / 释放该流上的会话（走与协议插件相同的 vhost 与权限校验） |
| `session.declare_exchange` / `delete_exchange` / `bind_exchange` / `unbind_exchange` | `{stream, ...}` | 交换机的声明 / 删除 / 绑定 |
| `session.declare_queue` / `delete_queue` / `bind_queue` / `unbind_queue` / `purge_queue` | `{stream, ...}` | 队列的声明 / 删除 / 绑定 / 清空（返回 `queue_info` / 条数） |
| `session.publish` | `{stream, exchange, routing_key, mandatory, message}` | 发布；返回 `{routed, rejected}`，**持久化等待在应答前完成**（返回即已按 fsync 档位落盘） |
| `session.get` | `{stream, queue, no_ack}` | 主动拉取；命中时返回带投递编号的 `delivery` |
| `session.consume` / `session.cancel` | `{stream, queue, tag, no_ack, exclusive, prefetch}` / `{stream, tag}` | 注册 / 取消消费者（返回内核最终使用的标签） |
| `session.settle` | `{delivery_id, action}` | 结算投递（`ack` / `requeue` / `reject`）；编号在整条连接上全局唯一，故不必带流号 |
| `session.deliver`（**正向**：内核 → 插件） | `{stream, delivery_id, queue, consumer_tag, redelivered, message}` | 内核回推一条投递；插件处理后调 `session.settle` |

错误**保留语义分类**：内核的 `plugin.Error.Kind` 随应答回传，插件侧可还原成 `plugin.Errorf(kind, ...)`，而不是被压成一句字符串。消息属性（`Timestamp` / `Headers` / 各类 ID）与消息体（base64）在自包含 JSON DTO 里**无损往返**。未结算的投递在流关闭 / 插件断开时由内核**重新入队**（与进程内协议插件的 `drainPending` 同一口径）。

代价：每次内核调用多一次本机 RPC（JSON 编解码 + 内存拷贝），且同一消费者上的投递逐条串行回推（顺序有保证，吞吐受一次 RTT 限制）。

**怎么验证**：`test/unit/sidecar`（握手成功/被拒/线协议版本不符/API 版本不符/插件名不符、控制面调用、双向流回显、连接断开可观测、心跳超时判死、宿主状态 enabled→down→自愈；**M8-12 追加**：反向调用基本通路、未配置处理器时明确报错、`session.open` 成功与 vhost 不存在返回 `invalid_path`、声明队列→发布→消费→内核回推投递→`settle(ack)` 后消息不再重投、断流后未结算投递重新入队、正向与反向调用并发不串台）；`test/integration/echosidecar`（参考插件本体 + `-session-demo`）；内核侧真进程 e2e（`spawn` 拉起 → 回显正常 → `kill` 插件 → 指标转 0 且内核端口仍可连 → 自动重启并恢复），外加 15 轮反复杀进程的混沌。

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
docker build -t swiftmq:0.14.0 .
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

| 三期 | M8 | **生产就绪**：M8-1 段回收 ✅、M8-2 AMQP 事务 ✅、M8-3 TLS ✅、M8-4 用户/权限复制与持久化 ✅、M8-5 双跑对照 ✅、M8-6 policies ✅、M8-7 vhost 动态增删 ✅、M8-8 跨语言矩阵（Java）✅、M8-9 长稳与混沌（缩比）✅、M8-10 性能基线（Linux）✅、M8-12 `plugin.Session` 桥成 RPC ✅、M8-14 Direct Reply-To 与消费者优先级 ✅、M8-15 集群管理 UI + 仲裁队列 `grow`/`rebalance` ✅、M8-16 运维配套 ✅；M8-11 / M8-13 / M8-17 **已结项不开工**（另立里程碑，理由见设计文档） | **P0/P1/P2 全部收口** |

每个里程碑的完成标准是"**真实客户端跑通 + 与 RabbitMQ 行为一致**"，而非"代码写完"。

> 二期（M6 / M7）已全部完成，三期（M8）进行中，但**这还不等于可用于生产**。在 M8 完成之前，README 顶部会一直保留"不可用于生产"的声明。M8 的出口条件 = **磁盘回收 ✅、AMQP 事务 ✅、TLS ✅、用户/权限的复制与持久化 ✅** 这四类硬缺口补齐，且**双跑对照 ✅**与长稳压测给出可复现证据；当前剩下的是长稳与性能验证（M8-9 长稳与混沌 / M8-10 性能达标）。
>
> 其中「与 RabbitMQ 双跑对照」的编排（两个 broker 同编排 + 各语言用例容器）与"已知差异"清单在配套工程 **`swiftmq-test/`**（独立于本仓库，含 `compare.py` 一键运行器），落地方式与判定口径见设计文档 §13.3。

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
