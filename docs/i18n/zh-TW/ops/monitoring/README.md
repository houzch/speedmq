# SwiftMQ 監控與警示

本目錄提供可直接使用的監控範本：

| 檔案 | 作用 |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus 警示規則（`groups: - name: swiftmq`） |
| `grafana-dashboard.json` | 可匯入的 Grafana 儀表板（面板涵蓋下列關鍵訊號） |
| `README.md` | 用法、指標清單、每個警示的意義與處置、已知缺口 |

---

## 1. 如何使用

### 1.1 抓取（Prometheus）

管理面（預設 `:15672`）暴露 Prometheus 文字格式 `/metrics`，**需要 Basic Auth**：

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

> 建議為監控單獨建立一個唯讀帳號（`monitoring` 標籤即可讀取指標），不要重複使用管理員密碼。

驗證抓取是否正常（PowerShell）：

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 警示規則

把 `prometheus-alerts.yml` 放進 Prometheus 的規則目錄，在 `prometheus.yml` 裡引用後 reload：

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

規則裡統一使用 `job="swiftmq"`；若你的 job 名稱不同，請全文取代。

### 1.3 Grafana 儀表板

`grafana-dashboard.json` 透過 **Dashboards → Import → 上傳 JSON** 匯入，匯入時選擇你的 Prometheus 資料來源
（儀表板裡用 `${DS_PROMETHEUS}` 變數引用）。範本變數 `DS_PROMETHEUS` 會在匯入對應裡賦值。

**【未驗證】** 本機未啟動 Grafana 執行個體，未做真實匯入驗證；該 JSON 僅做了 JSON 語法檢查（14 個面板，解析通過）。

---

## 2. `/metrics` 真實片段（證據）

以下為本機 `1.0.0` 執行個體 `/metrics` 的**真實輸出**（已建立一條 durable 佇列 `persist.q`，
故帶 `vhost`/`queue` 標籤的 per-queue 指標出現了）：

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

## 3. 指標清單（全部真實存在，來源 `internal/management/metrics.go`）

| 指標 | 類型 | 標籤 | 語意 |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | 處理程序自行回報存活（目前恆為 1） |
| `swiftmq_build_info` | gauge | `version`,`node` | 建置資訊，value 恆為 1 |
| `swiftmq_resource_blocked` | gauge | — | 資源水位是否阻塞生產者（1=阻塞中） |
| `swiftmq_connections` | gauge | — | 目前連線數 |
| `swiftmq_channels` | gauge | — | 目前通道數 |
| `swiftmq_queues` | gauge | — | 目前佇列數 |
| `swiftmq_exchanges` | gauge | — | 目前交換器數 |
| `swiftmq_consumers` | gauge | — | 目前消費者數 |
| `swiftmq_queue_messages` | gauge | — | **全域**就緒訊息總數 |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | **全域**未確認訊息總數 |
| `swiftmq_process_memory_bytes` | gauge | — | 處理程序**使用中**記憶體（`HeapInuse+StackInuse`） |
| `swiftmq_memory_total_bytes` | gauge | — | 實體記憶體總量 |
| `swiftmq_disk_free_bytes` | gauge | — | 資料目錄可用空間 |
| `swiftmq_memory_high_watermark` | gauge | — | 記憶體水位比例 |
| `swiftmq_disk_free_limit_bytes` | gauge | — | 磁碟剩餘下限 |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | 某佇列就緒訊息數 |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | 某佇列未確認訊息數 |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | 某佇列消費者數 |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | 某佇列記憶體用量估計 |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | 佇列累計接收訊息數 |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | 佇列累計投遞訊息數 |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | 佇列累計確認訊息數 |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | 外掛中繼資料，value 恆為 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | 外掛是否在服務（1=enabled，0=其他） |

### 3.1 使用注意事項（避免寫錯）

- **同名但基數不同的兩族序列**：`swiftmq_queue_messages_unacknowledged` **既**有全域無標籤序列、
  **又**有 per-queue 帶標籤序列；而「就緒」側全域叫 `swiftmq_queue_messages`、per-queue 叫
  `swiftmq_queue_messages_ready`（名稱不對稱）。撰寫規則時用 `{queue=~".+"}` 明確只取 per-queue 那一族。
- **`swiftmq_process_memory_bytes` 的定義**：實作是 `MemStats.HeapInuse + StackInuse`（**使用中記憶體**），
  與核心記憶體水位判定為同一定義；但它的 `# HELP` 文字說明寫的是「向作業系統申請的記憶體位元組數」，**文字說明與實際定義不符**，
  以本文為準。
- **per-queue 序列僅在佇列存在時出現**：佇列被刪除後該序列消失（Prometheus 端會變成 stale）。
  涉及「佇列應存在但沒有資料」的警示可搭配 `absent()` 或 Grafana 的 `or vector(0)`。
- **counter 在處理程序重新啟動後歸零**：`*_total` 是處理程序內累計，重新啟動即從 0 開始；請使用 `rate()`/`increase()`，
  不要直接對絕對值設定閾值。
- **沒有 metrics 的叢集訊號**：見 §5。

---

## 4. 警示意義與建議處置（對應 `prometheus-alerts.yml`）

| 警示 | 觸發條件 | 意義 | 建議處置 |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` 1m | 抓取目標整體無法連線 | 檢查處理程序/連接埠/網路/認證；重新啟動並查看啟動日誌 |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` 1m | 抓取成功但處理程序自行回報不存活 | 保底項目，檢查異常結束日誌 |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` 2m | 外掛 disabled/failed/down | `swiftmqctl plugins show <name>` 查看 `runtime_note`；外部外掛 `restart=always` 通常會自行恢復 |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` 5m | 記憶體/磁碟水位觸發，生產者被阻塞 | 檢查記憶體水位與磁碟可用空間；確認消費者是否推進 |
| `SwiftMQMemoryWatermarkHigh` | 使用中記憶體占比 > 0.9×水位 10m | 逼近記憶體水位 | 降低積壓/提高消費速度，避免觸發阻塞 |
| `SwiftMQDiskFreeLow` | 可用 < 1.5×磁碟下限 10m | 資料目錄即將寫滿 | 擴充/清理；到達下限會阻塞生產者 |
| `SwiftMQQueueBacklogGrowing` | 就緒 >10000 且 15m 單調成長 | 佇列持續積壓 | 擴充消費者 / 檢查消費端；排查死信/TTL 異常 |
| `SwiftMQQueueNoConsumers` | 消費者=0 且有就緒訊息 15m | 無人消費 | 檢查消費端處理程序；確認消費者未斷線 |
| `SwiftMQUnackedPileUp` | 未確認 >1000 15m | 消費者卡住/不 ack | 檢查消費者處理邏輯與 prefetch；必要時關閉連線並重投 |
| `SwiftMQConnectionSpike` | 連線 >10000 10m | 連線數異常 | 檢查連線洩漏；用戶端應重複使用連線 |

> 閾值（10000 / 1000 等）是**起始值**，請依你的佇列規模與業務特性調整。

---

## 5. 已知缺口：叢集「失去多數派 / 無 leader」目前沒有指標

- **事實**：`/metrics` **沒有**任何叢集類指標（無 `swiftmq_cluster_*`）。叢集狀態只在 `GET /api/cluster` 的 JSON 裡：
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`。
- **因此** `prometheus-alerts.yml` **刻意不寫**基於叢集指標的警示——寫了也**永遠不會觸發**
  （Prometheus 不會因指標名稱不存在而報錯），那屬於「看似正確、實則無效」的交付。
- **自行建置方案**（二選一，均需你在 SwiftMQ 之外建置，不在本儲存庫範圍）：
  1. 用通用 JSON exporter 抓 `/api/cluster`，對應成自訂指標（如 `swiftmq_cluster_has_quorum`），再對該指標發出警示；
  2. 用探針指令碼定期呼叫 `/api/cluster`，`has_quorum=false` 或 `paused=true` 時打點發出警示。
- 相關閾值定義：`has_quorum=false` 表示與多數派失聯；`pause_minority`（預設）下此時**服務會暫停並斷開連線**。

---

## 6. 其他**未驗證**項目

- Grafana 儀表板**未在真實 Grafana 中匯入驗證**（僅通過 JSON 語法檢查）。
- 警示規則**未在真實 Prometheus/Alertmanager 中載入驗證**（本機未啟動 Prometheus）。
  但規則裡的**指標名稱已逐條對照 `/metrics` 真實輸出**（見 §2/§3），不存在「名稱寫錯導致永不觸發」的問題。
