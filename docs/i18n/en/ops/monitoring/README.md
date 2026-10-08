# SpeedMQ Monitoring and Alerting

This directory provides ready-to-use monitoring templates:

| File | Purpose |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus alerting rules (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Importable Grafana dashboard (panels cover the key signals described below) |
| `README.md` | Usage, metric inventory, the meaning and handling of each alert, and known gaps |

---

## 1. How to use

### 1.1 Scraping (Prometheus)

The management plane (default `:15672`) exposes `/metrics` in Prometheus text format, and **requires Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: speedmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> It is recommended to create a separate read-only account for monitoring (the `monitoring` tag is enough to read metrics); do not reuse the administrator password.

Verify that scraping works (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Alerting rules

Put `prometheus-alerts.yml` into Prometheus's rules directory, reference it in `prometheus.yml`, and then reload:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

The rules uniformly use `job="speedmq"`; if your job name differs, replace it throughout.

### 1.3 Grafana dashboard

Import `grafana-dashboard.json` via **Dashboards → Import → upload JSON**, selecting your Prometheus data source during import
(the dashboard references it via the `${DS_PROMETHEUS}` variable). The template variable `DS_PROMETHEUS` is assigned in the import mapping.

**【Not verified】** No Grafana instance was started on this machine and no real import verification was done; the JSON only underwent a JSON syntax check (14 panels, parsing passed).

---

## 2. Real `/metrics` excerpt (evidence)

The following is the **real output** of `/metrics` from this machine's `1.0.0` instance (a durable queue `persist.q` has been created,
so per-queue metrics with `vhost`/`queue` labels appear):

```
# HELP speedmq_up 节点是否存活
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info 构建信息
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections 当前连接数
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels 当前通道数
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues 当前队列数
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges 当前交换机数
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers 当前消费者数
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages 就绪消息总数
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged 未确认消息总数
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes 物理内存总量
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes 数据目录可用空间
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark 内存水位比例
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready 队列中的就绪消息数
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers 队列上的消费者数
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes 队列内存占用估算值
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total 队列累计接收的消息数
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Metric inventory (all actually exist; source: `internal/management/metrics.go`)

| Metric | Type | Labels | Semantics |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Process self-reports liveness (currently always 1) |
| `speedmq_build_info` | gauge | `version`,`node` | Build info, value is always 1 |
| `speedmq_resource_blocked` | gauge | — | Whether a resource watermark is blocking producers (1=blocked) |
| `speedmq_connections` | gauge | — | Current number of connections |
| `speedmq_channels` | gauge | — | Current number of channels |
| `speedmq_queues` | gauge | — | Current number of queues |
| `speedmq_exchanges` | gauge | — | Current number of exchanges |
| `speedmq_consumers` | gauge | — | Current number of consumers |
| `speedmq_queue_messages` | gauge | — | **Global** total number of ready messages |
| `speedmq_queue_messages_unacknowledged` | gauge | — | **Global** total number of unacknowledged messages |
| `speedmq_process_memory_bytes` | gauge | — | Process **in-use** memory (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | Total physical memory |
| `speedmq_disk_free_bytes` | gauge | — | Free space of the data directory |
| `speedmq_memory_high_watermark` | gauge | — | Memory watermark ratio |
| `speedmq_disk_free_limit_bytes` | gauge | — | Disk free space lower limit |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | Number of ready messages in a given queue |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Number of unacknowledged messages in a given queue |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Number of consumers on a given queue |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Estimated memory usage of a given queue |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | Cumulative number of messages received by the queue |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Cumulative number of messages delivered by the queue |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Cumulative number of messages acknowledged by the queue |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Plugin metadata, value is always 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Whether the plugin is in service (1=enabled, 0=other) |

### 3.1 Usage notes (to avoid mistakes)

- **Two series families with the same name but different cardinality**: `speedmq_queue_messages_unacknowledged` has **both** a global, label-free series
  **and** a per-queue labeled series; on the "ready" side, the global one is called `speedmq_queue_messages` while the per-queue one is called
  `speedmq_queue_messages_ready` (asymmetric names). When writing rules, use `{queue=~".+"}` to explicitly select only the per-queue family.
- **The semantics of `speedmq_process_memory_bytes`**: the implementation is `MemStats.HeapInuse + StackInuse` (**in-use memory**),
  the same measure used for the kernel's memory watermark determination; however, its `# HELP` text says "bytes of memory requested from the operating system",
  which **does not match the actual measure** — this document is authoritative.
- **Per-queue series appear only while the queue exists**: after the queue is deleted the series disappears (it becomes stale on the Prometheus side).
  For alerts about "a queue should exist but has no data", combine with `absent()` or Grafana's `or vector(0)`.
- **Counters reset to zero after a process restart**: `*_total` is an in-process cumulative value that starts from 0 on restart; use `rate()`/`increase()`,
  and do not set thresholds directly on absolute values.
- **Cluster signals have no metrics**: see §5.

---

## 4. Alert meanings and suggested handling (corresponding to `prometheus-alerts.yml`)

| Alert | Trigger condition | Meaning | Suggested handling |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | The scrape target is entirely unreachable | Check process/port/network/auth; restart and check the startup log |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Scrape succeeds but the process self-reports not alive | A catch-all; check for abnormal-exit logs |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Plugin disabled/failed/down | Use `speedmqctl plugins show <name>` to see `runtime_note`; external plugins with `restart=always` usually self-heal |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Memory/disk watermark triggered, producers blocked | Check the memory watermark and available disk; confirm whether consumers are making progress |
| `SpeedMQMemoryWatermarkHigh` | In-use memory ratio > 0.9×watermark 10m | Approaching the memory watermark | Reduce the backlog/increase consumption speed to avoid triggering blocking |
| `SpeedMQDiskFreeLow` | Available < 1.5×disk lower limit 10m | The data directory is nearly full | Expand/clean up; reaching the lower limit blocks producers |
| `SpeedMQQueueBacklogGrowing` | Ready >10000 and monotonically increasing for 15m | The queue keeps accumulating backlog | Scale out consumers / check the consumer side; investigate dead-letter/TTL anomalies |
| `SpeedMQQueueNoConsumers` | Consumers=0 and there are ready messages for 15m | Nobody is consuming | Check the consumer process; confirm the consumer has not disconnected |
| `SpeedMQUnackedPileUp` | Unacknowledged >1000 15m | Consumers stuck/not acking | Check the consumer processing logic and prefetch; if necessary, close the connection and redeliver |
| `SpeedMQConnectionSpike` | Connections >10000 10m | Abnormal connection count | Check for connection leaks; clients should reuse connections |

> The thresholds (10000 / 1000, etc.) are **starting values**; adjust them to your queue scale and business characteristics.

---

## 5. Known gap: there is currently no metric for the cluster "losing the majority / having no leader"

- **Fact**: `/metrics` has **no** cluster-related metrics at all (no `speedmq_cluster_*`). Cluster state is only in the JSON from `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Therefore** `prometheus-alerts.yml` **deliberately omits** alerts based on cluster metrics — they would **never fire** anyway
  (Prometheus does not error on a nonexistent metric name), which would be a "looks right but is actually ineffective" deliverable.
- **Do-it-yourself options** (choose one; both must be built outside SpeedMQ and are out of scope for this repository):
  1. Use a generic JSON exporter to scrape `/api/cluster`, map it into custom metrics (e.g. `speedmq_cluster_has_quorum`), and alert on those metrics;
  2. Use a probe script to periodically call `/api/cluster` and alert when `has_quorum=false` or `paused=true`.
- Related threshold semantics: `has_quorum=false` means the node has lost contact with the majority; under `pause_minority` (the default), the **service pauses and disconnects connections** at that point.

---

## 6. Other **unverified** items

- The Grafana dashboard **has not been imported and verified in a real Grafana** (only a JSON syntax check passed).
- The alerting rules **have not been loaded and verified in a real Prometheus/Alertmanager** (Prometheus was not started on this machine).
  However, the **metric names in the rules have been checked one by one against the real `/metrics` output** (see §2/§3), so there is no "misspelled name causing it to never fire" problem.
