# SwiftMQ 监控与告警

本目录提供可直接使用的监控模板：

| 文件 | 作用 |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus 告警规则（`groups: - name: swiftmq`） |
| `grafana-dashboard.json` | 可导入的 Grafana 仪表盘（面板覆盖下述关键信号） |
| `README.md` | 用法、指标清单、每个告警的含义与处置、已知缺口 |

---

## 1. 怎么用

### 1.1 抓取（Prometheus）

管理面（默认 `:15672`）暴露 Prometheus 文本格式 `/metrics`，**需要 Basic Auth**：

```yaml
# prometheus.yml
scrape_configs:
  - job_name: swiftmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> 建议为监控单独建一个只读账号（`monitoring` 标签即可读指标），不要复用管理员口令。

验证抓取是否正常（PowerShell）：

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 告警规则

把 `prometheus-alerts.yml` 放进 Prometheus 的规则目录，在 `prometheus.yml` 里引用后 reload：

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

规则里统一使用 `job="swiftmq"`；若你的 job 名不同，请全文替换。

### 1.3 Grafana 仪表盘

`grafana-dashboard.json` 通过 **Dashboards → Import → 上传 JSON** 导入，导入时选择你的 Prometheus 数据源
（仪表盘里用 `${DS_PROMETHEUS}` 变量引用）。模板变量 `DS_PROMETHEUS` 会在导入映射里赋值。

**【未验证】** 本机未起 Grafana 实例，未做真实导入验证；该 JSON 仅做了 JSON 语法校验（14 个面板，解析通过）。

---

## 2. `/metrics` 真实片段（证据）

以下为本机 `0.13.0` 实例 `/metrics` 的**真实输出**（已建一条 durable 队列 `persist.q`，
故带 `vhost`/`queue` 标签的 per-queue 指标出现了）：

```
# HELP swiftmq_up 节点是否存活
# TYPE swiftmq_up gauge
swiftmq_up 1
# HELP swiftmq_build_info 构建信息
# TYPE swiftmq_build_info gauge
swiftmq_build_info 1{version="0.13.0",node="swiftmq@DESKTOP-HBDCVPA"}
# HELP swiftmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE swiftmq_resource_blocked gauge
swiftmq_resource_blocked 0
# HELP swiftmq_connections 当前连接数
# TYPE swiftmq_connections gauge
swiftmq_connections 0
# HELP swiftmq_channels 当前通道数
# TYPE swiftmq_channels gauge
swiftmq_channels 0
# HELP swiftmq_queues 当前队列数
# TYPE swiftmq_queues gauge
swiftmq_queues 0
# HELP swiftmq_exchanges 当前交换机数
# TYPE swiftmq_exchanges gauge
swiftmq_exchanges 6
# HELP swiftmq_consumers 当前消费者数
# TYPE swiftmq_consumers gauge
swiftmq_consumers 0
# HELP swiftmq_queue_messages 就绪消息总数
# TYPE swiftmq_queue_messages gauge
swiftmq_queue_messages 0
# HELP swiftmq_queue_messages_unacknowledged 未确认消息总数
# TYPE swiftmq_queue_messages_unacknowledged gauge
swiftmq_queue_messages_unacknowledged 0
# HELP swiftmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE swiftmq_process_memory_bytes gauge
swiftmq_process_memory_bytes 1564672
# HELP swiftmq_memory_total_bytes 物理内存总量
# TYPE swiftmq_memory_total_bytes gauge
swiftmq_memory_total_bytes 34181279744
# HELP swiftmq_disk_free_bytes 数据目录可用空间
# TYPE swiftmq_disk_free_bytes gauge
swiftmq_disk_free_bytes 240091688960
# HELP swiftmq_memory_high_watermark 内存水位比例
# TYPE swiftmq_memory_high_watermark gauge
swiftmq_memory_high_watermark 0.4
# HELP swiftmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE swiftmq_disk_free_limit_bytes gauge
swiftmq_disk_free_limit_bytes 52428800
# HELP swiftmq_queue_messages_ready 队列中的就绪消息数
# TYPE swiftmq_queue_messages_ready gauge
# HELP swiftmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE swiftmq_queue_messages_unacknowledged gauge
# HELP swiftmq_queue_consumers 队列上的消费者数
# TYPE swiftmq_queue_consumers gauge
# HELP swiftmq_queue_memory_bytes 队列内存占用估算值
# TYPE swiftmq_queue_memory_bytes gauge
# HELP swiftmq_queue_messages_published_total 队列累计接收的消息数
# TYPE swiftmq_queue_messages_published_total counter
# HELP swiftmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE swiftmq_queue_messages_delivered_total counter
# HELP swiftmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE swiftmq_queue_messages_acked_total counter
swiftmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
swiftmq_queue_consumers{vhost="/",queue="persist.q"} 0
swiftmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
swiftmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
swiftmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP swiftmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE swiftmq_plugin_info gauge
# HELP swiftmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE swiftmq_plugin_up gauge
swiftmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="amqp091",state="enabled"} 1
swiftmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. 指标清单（全部真实存在，来源 `internal/management/metrics.go`）

| 指标 | 类型 | 标签 | 语义 |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | 进程自报存活（当前恒为 1） |
| `swiftmq_build_info` | gauge | `version`,`node` | 构建信息，value 恒 1 |
| `swiftmq_resource_blocked` | gauge | — | 资源水位是否阻塞生产者（1=阻塞中） |
| `swiftmq_connections` | gauge | — | 当前连接数 |
| `swiftmq_channels` | gauge | — | 当前通道数 |
| `swiftmq_queues` | gauge | — | 当前队列数 |
| `swiftmq_exchanges` | gauge | — | 当前交换机数 |
| `swiftmq_consumers` | gauge | — | 当前消费者数 |
| `swiftmq_queue_messages` | gauge | — | **全局**就绪消息总数 |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | **全局**未确认消息总数 |
| `swiftmq_process_memory_bytes` | gauge | — | 进程**在用**内存（`HeapInuse+StackInuse`） |
| `swiftmq_memory_total_bytes` | gauge | — | 物理内存总量 |
| `swiftmq_disk_free_bytes` | gauge | — | 数据目录可用空间 |
| `swiftmq_memory_high_watermark` | gauge | — | 内存水位比例 |
| `swiftmq_disk_free_limit_bytes` | gauge | — | 磁盘剩余下限 |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | 某队列就绪消息数 |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | 某队列未确认消息数 |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | 某队列消费者数 |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | 某队列内存占用估算 |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | 队列累计接收消息数 |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | 队列累计投递消息数 |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | 队列累计确认消息数 |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | 插件元数据，value 恒 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | 插件是否在服务（1=enabled，0=其它） |

### 3.1 使用注意（避免写错）

- **同名不同基数的两族序列**：`swiftmq_queue_messages_unacknowledged` **既**有全局无标签序列、
  **又**有 per-queue 带标签序列；而"就绪"侧全局叫 `swiftmq_queue_messages`、per-queue 叫
  `swiftmq_queue_messages_ready`（名字不对称）。写规则时用 `{queue=~".+"}` 明确只取 per-queue 一族。
- **`swiftmq_process_memory_bytes` 的口径**：实现是 `MemStats.HeapInuse + StackInuse`（**在用内存**），
  与内核内存水位判定同一口径；但它的 `# HELP` 文案写的是"向操作系统申请的内存字节数"，**文案与实际口径不符**，
  以本文为准。
- **per-queue 序列仅在队列存在时出现**：队列被删除后该序列消失（Prometheus 端会变成 stale）。
  涉及"队列应存在但没有数据"的告警可配合 `absent()` 或 Grafana 的 `or vector(0)`。
- **counter 在进程重启后归零**：`*_total` 是进程内累计，重启即从 0 开始；请用 `rate()`/`increase()`，
  不要直接对绝对值设阈值。
- **没有 metrics 的集群信号**：见 §5。

---

## 4. 告警含义与建议处置（对应 `prometheus-alerts.yml`）

| 告警 | 触发条件 | 含义 | 建议处置 |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` 1m | 抓取目标整体不可达 | 查进程/端口/网路/认证；重启并看启动日志 |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` 1m | 抓取成功但进程自报不存活 | 兜底项，查异常退出日志 |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` 2m | 插件 disabled/failed/down | `swiftmqctl plugins show <name>` 看 `runtime_note`；外部插件 `restart=always` 一般自愈 |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` 5m | 内存/磁盘水位触发，生产者被阻塞 | 查内存水位与磁盘可用；确认消费者是否推进 |
| `SwiftMQMemoryWatermarkHigh` | 在用内存占比 > 0.9×水位 10m | 逼近内存水位 | 降低积压/提消费速度，防触发阻塞 |
| `SwiftMQDiskFreeLow` | 可用 < 1.5×磁盘下限 10m | 数据目录快满 | 扩容/清理；到下限会阻塞生产者 |
| `SwiftMQQueueBacklogGrowing` | 就绪 >10000 且 15m 单调增长 | 队列持续积压 | 扩消费者 / 查消费端；排查死信/TTL 异常 |
| `SwiftMQQueueNoConsumers` | 消费者=0 且有就绪消息 15m | 无人消费 | 查消费端进程；确认消费者未掉线 |
| `SwiftMQUnackedPileUp` | 未确认 >1000 15m | 消费者卡住/不 ack | 查消费者处理逻辑与 prefetch；必要时关连接重投 |
| `SwiftMQConnectionSpike` | 连接 >10000 10m | 连接数异常 | 查连接泄漏；客户端应复用连接 |

> 阈值（10000 / 1000 等）是**起点值**，请按你的队列规模与业务特征调整。

---

## 5. 已知缺口：集群"失去多数派 / 无 leader"目前没有指标

- **事实**：`/metrics` **没有**任何集群类指标（无 `swiftmq_cluster_*`）。集群状态只在 `GET /api/cluster` 的 JSON 里：
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`。
- **因此** `prometheus-alerts.yml` **故意不写**基于集群指标的告警——写了也**永远不会触发**
  （Prometheus 不会因指标名不存在而报错），那属于"看起来对、其实无效"的交付。
- **自建方案**（二选一，均需你在 SwiftMQ 之外搭建，不在本仓库范围）：
  1. 用通用 JSON exporter 抓 `/api/cluster`，映射成自定义指标（如 `swiftmq_cluster_has_quorum`），再对该指标告警；
  2. 用探针脚本定期调 `/api/cluster`，`has_quorum=false` 或 `paused=true` 时打点告警。
- 相关阈值口径：`has_quorum=false` 表示与多数派失联；`pause_minority`（默认）下此时**服务会暂停并断开连接**。

---

## 6. 其他**未验证**项

- Grafana 仪表盘**未在真实 Grafana 中导入验证**（仅 JSON 语法校验通过）。
- 告警规则**未在真实 Prometheus/Alertmanager 中加载验证**（本机未起 Prometheus）。
  但规则里的**指标名已逐条对照 `/metrics` 真实输出**（见 §2/§3），不存在"名字写错导致永不触发"的问题。
