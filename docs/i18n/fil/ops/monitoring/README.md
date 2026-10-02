# Monitoring at Alerting ng SwiftMQ

Nagbibigay ang directory na ito ng mga monitoring template na maaaring gamitin nang direkta:

| File | Ginagawa |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus alert rules (`groups: - name: swiftmq`) |
| `grafana-dashboard.json` | Ma-import na Grafana dashboard (sinasaklaw ng panels ang mga kritikal na signal sa ibaba) |
| `README.md` | Paggamit, listahan ng metrics, kahulugan at paghawak ng bawat alert, mga kilalang kakulangan |

---

## 1. Paano gamitin

### 1.1 Scraping (Prometheus)

Inilalantad ng management plane (default na `:15672`) ang `/metrics` sa Prometheus text format, **nangangailangan ng Basic Auth**:

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

> Inirerekomendang gumawa ng hiwalay na read-only account para sa monitoring (sapat na ang `monitoring` tag upang makabasa ng metrics); huwag gamitin muli ang password ng administrator.

I-verify kung normal ang scraping (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Alert rules

Ilagay ang `prometheus-alerts.yml` sa rules directory ng Prometheus, i-reference ito sa `prometheus.yml`, pagkatapos ay i-reload:

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

Ang `job="swiftmq"` ay pantay na ginagamit sa mga rule; kung iba ang pangalan ng iyong job, palitan ito sa buong teksto.

### 1.3 Grafana dashboard

Ini-import ang `grafana-dashboard.json` sa pamamagitan ng **Dashboards → Import → upload JSON**; sa pag-import, piliin ang iyong Prometheus data source
(sa dashboard, ginagamit ang `${DS_PROMETHEUS}` variable bilang reference). Ang template variable na `DS_PROMETHEUS` ay bibigyan ng halaga sa import mapping.

**【hindi pa na-verify】** Walang Grafana instance na pinatakbo sa makinang ito, at walang tunay na import verification; ang JSON ay sumailalim lamang sa JSON syntax check (14 panels, pumasa sa parsing).

---

## 2. Tunay na fragment ng `/metrics` (ebidensya)

Ang sumusunod ay ang **tunay na output** ng `/metrics` ng instance na `1.0.0` sa makinang ito (may nagawang isang durable queue na `persist.q`,
kaya lumitaw ang per-queue metrics na may `vhost`/`queue` labels):

```
# HELP swiftmq_up 节点是否存活
# TYPE swiftmq_up gauge
swiftmq_up 1
# HELP swiftmq_build_info 构建信息
# TYPE swiftmq_build_info gauge
swiftmq_build_info 1{version="1.0.0",node="swiftmq@DESKTOP-HBDCVPA"}
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

## 3. Listahan ng metrics (lahat ay tunay na umiiral, pinagmulan: `internal/management/metrics.go`)

| Metric | Uri | Labels | Semantics |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | Sariling pag-uulat ng process na buhay (kasalukuyang laging 1) |
| `swiftmq_build_info` | gauge | `version`,`node` | Impormasyon ng build, ang value ay laging 1 |
| `swiftmq_resource_blocked` | gauge | — | Kung hinaharang ng resource watermark ang mga producer (1=hinaharang) |
| `swiftmq_connections` | gauge | — | Kasalukuyang bilang ng koneksyon |
| `swiftmq_channels` | gauge | — | Kasalukuyang bilang ng channel |
| `swiftmq_queues` | gauge | — | Kasalukuyang bilang ng queue |
| `swiftmq_exchanges` | gauge | — | Kasalukuyang bilang ng exchange |
| `swiftmq_consumers` | gauge | — | Kasalukuyang bilang ng consumer |
| `swiftmq_queue_messages` | gauge | — | **Global** na kabuuang bilang ng ready messages |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | **Global** na kabuuang bilang ng unacknowledged messages |
| `swiftmq_process_memory_bytes` | gauge | — | Memory na **ginagamit** ng process (`HeapInuse+StackInuse`) |
| `swiftmq_memory_total_bytes` | gauge | — | Kabuuang physical memory |
| `swiftmq_disk_free_bytes` | gauge | — | Available na espasyo ng data directory |
| `swiftmq_memory_high_watermark` | gauge | — | Ratio ng memory watermark |
| `swiftmq_disk_free_limit_bytes` | gauge | — | Pinakamababang limitasyon ng natitirang disk |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | Bilang ng ready messages ng isang queue |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Bilang ng unacknowledged messages ng isang queue |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | Bilang ng consumer ng isang queue |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Tantya ng memory usage ng isang queue |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | Cumulative na bilang ng natanggap na mensahe ng queue |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Cumulative na bilang ng naihatid na mensahe ng queue |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Cumulative na bilang ng naka-confirm na mensahe ng queue |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Metadata ng plugin, ang value ay laging 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | Kung nagseserbisyo ang plugin (1=enabled, 0=iba pa) |

### 3.1 Mga paalala sa paggamit (upang maiwasan ang maling pagsulat)

- **Dalawang pamilya ng series na may parehong pangalan ngunit magkaibang cardinality**: Ang `swiftmq_queue_messages_unacknowledged` ay **parehong** may global na series na walang label,
  **at** may per-queue series na may label; samantalang sa panig ng "ready", ang global ay tinatawag na `swiftmq_queue_messages` at ang per-queue ay tinatawag na
  `swiftmq_queue_messages_ready` (asymmetrical ang pangalan). Kapag nagsusulat ng rule, gamitin ang `{queue=~".+"}` upang tahasang kunin lamang ang pamilyang per-queue.
- **Ang pagtrato sa `swiftmq_process_memory_bytes`**: ang implementasyon ay `MemStats.HeapInuse + StackInuse` (**memory na ginagamit**),
  parehong pagtrato tulad ng pagtukoy ng memory watermark ng kernel; ngunit ang `# HELP` text nito ay nakasulat na "bytes ng memory na hinihingi sa operating system", **hindi tugma ang text sa aktwal na pagtrato**;
  ang dokumentong ito ang mangingibabaw.
- **Ang per-queue series ay lumilitaw lamang kapag umiiral ang queue**: pagkatapos mabura ang queue, mawawala ang series na iyon (magiging stale ito sa panig ng Prometheus).
  Para sa alerts na may kinalaman sa "dapat umiiral ang queue ngunit walang data", maaaring gamitin kasama ang `absent()` o ang `or vector(0)` ng Grafana.
- **Nagre-reset sa zero ang counter pagkatapos ng restart ng process**: Ang `*_total` ay cumulative sa loob ng process, at magsisimula muli sa 0 kapag nag-restart; gamitin ang `rate()`/`increase()`,
  huwag direktang magtakda ng threshold sa absolute value.
- **Mga cluster signal na walang metrics**: tingnan ang §5.

---

## 4. Kahulugan ng alerts at rekomendadong paghawak (katumbas ng `prometheus-alerts.yml`)

| Alert | Trigger condition | Kahulugan | Rekomendadong paghawak |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` 1m | Kabuuang hindi maabot ang scrape target | Suriin ang process/port/network/auth; i-restart at tingnan ang startup log |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` 1m | Matagumpay ang scraping ngunit iniuulat ng process na hindi ito buhay | Fallback item, suriin ang log ng abnormal na pag-exit |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` 2m | Naka-disabled/failed/down ang plugin | Tingnan ang `runtime_note` gamit ang `swiftmqctl plugins show <name>`; ang external plugin na `restart=always` ay karaniwang self-healing |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` 5m | Na-trigger ang memory/disk watermark, naharang ang producer | Suriin ang memory watermark at available na disk; kumpirmahin kung umuusad ang consumer |
| `SwiftMQMemoryWatermarkHigh` | 在用内存占比 > 0.9×水位 10m | Papalapit na sa memory watermark | Bawasan ang backlog/pabilisin ang consumption, upang maiwasan ang pag-trigger ng blocking |
| `SwiftMQDiskFreeLow` | 可用 < 1.5×磁盘下限 10m | Malapit nang mapuno ang data directory | Palawakin/linisin; kapag umabot sa limitasyon ay haharangin ang producer |
| `SwiftMQQueueBacklogGrowing` | 就绪 >10000 且 15m 单调增长 | Patuloy na nag-iipon ang queue | Dagdagan ang consumer / suriin ang consumption side; siyasatin ang dead-letter/TTL anomaly |
| `SwiftMQQueueNoConsumers` | 消费者=0 且有就绪消息 15m | Walang kumokonsumo | Suriin ang process ng consumption side; kumpirmahin na hindi na-disconnect ang consumer |
| `SwiftMQUnackedPileUp` | 未确认 >1000 15m | Natigil/hindi nag-ack ang consumer | Suriin ang processing logic ng consumer at ang prefetch; kung kinakailangan, isara ang koneksyon at muling i-deliver |
| `SwiftMQConnectionSpike` | 连接 >10000 10m | Abnormal ang bilang ng koneksyon | Suriin ang connection leak; dapat gamitin muli ng client ang koneksyon |

> Ang mga threshold (10000 / 1000 atbp.) ay **starting values**; ayusin ang mga ito ayon sa laki ng iyong queue at katangian ng negosyo.

---

## 5. Kilalang kakulangan: ang "pagkawala ng majority / walang leader" ng cluster ay kasalukuyang walang metrics

- **Katotohanan**: Ang `/metrics` ay **walang** anumang cluster-type na metric (walang `swiftmq_cluster_*`). Ang estado ng cluster ay nasa JSON ng `GET /api/cluster` lamang:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Kaya** ang `prometheus-alerts.yml` ay **sadyang hindi nagsusulat** ng alert batay sa cluster metrics——kahit isulat ay **hindi kailanman magti-trigger**
  (hindi nag-e-error ang Prometheus dahil wala ang metric name), at iyon ay isang delivery na "mukhang tama ngunit walang bisa".
- **Sariling solusyon** (pumili ng isa, parehong kailangang itayo sa labas ng SwiftMQ, wala sa saklaw ng repository na ito):
  1. Gumamit ng generic JSON exporter upang kunin ang `/api/cluster`, i-map sa custom metric (tulad ng `swiftmq_cluster_has_quorum`), pagkatapos ay mag-alert sa metric na iyon;
  2. Gumamit ng probe script na regular na tumatawag sa `/api/cluster`, at mag-alert kung `has_quorum=false` o `paused=true`.
- Kaugnay na pagtrato sa threshold: ang `has_quorum=false` ay nangangahulugang nawalan ng koneksyon sa majority; sa ilalim ng `pause_minority` (default), sa oras na ito ay **pansamantalang hihinto ang serbisyo at ididiskonekta ang koneksyon**.

---

## 6. Iba pang **hindi pa na-verify** na item

- Ang Grafana dashboard ay **hindi na-import at na-verify sa tunay na Grafana** (JSON syntax check lamang ang pumasa).
- Ang alert rules ay **hindi na-load at na-verify sa tunay na Prometheus/Alertmanager** (walang Prometheus na pinatakbo sa makinang ito).
  Ngunit ang **mga metric name sa rules ay isa-isang itinugma sa tunay na output ng `/metrics`** (tingnan ang §2/§3), kaya walang problema na "maling pangalan na nagdudulot ng hindi kailanman pag-trigger".
