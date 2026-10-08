# SpeedMQ の監視とアラート

本ディレクトリはそのまま使用できる監視テンプレートを提供します：

| ファイル | 役割 |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus アラートルール（`groups: - name: speedmq`） |
| `grafana-dashboard.json` | インポート可能な Grafana ダッシュボード（パネルは後述の重要シグナルをカバー） |
| `README.md` | 使い方、メトリクス一覧、各アラートの意味と対処、既知のギャップ |

---

## 1. 使い方

### 1.1 スクレイプ（Prometheus）

管理面（デフォルト `:15672`）は Prometheus テキスト形式の `/metrics` を公開し、**Basic Auth が必要**です：

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

> 監視専用に読み取り専用アカウントを別途作成することを推奨します（`monitoring` タグがあればメトリクスを読めます）。管理者パスワードを流用しないでください。

スクレイプが正常か検証（PowerShell）：

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 アラートルール

`prometheus-alerts.yml` を Prometheus のルールディレクトリに置き、`prometheus.yml` で参照してから reload します：

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

ルール内では一律 `job="speedmq"` を使用します；job 名が異なる場合は全体を置換してください。

### 1.3 Grafana ダッシュボード

`grafana-dashboard.json` は **Dashboards → Import → JSON をアップロード**でインポートし、インポート時に自分の Prometheus データソースを選択します
（ダッシュボード内では `${DS_PROMETHEUS}` 変数で参照）。テンプレート変数 `DS_PROMETHEUS` はインポートのマッピングで値が割り当てられます。

**【未検証】** 本機では Grafana インスタンスを起動せず、実際のインポート検証は行っていません；この JSON は JSON 構文検証のみ実施（14 パネル、パース成功）。

---

## 2. `/metrics` の実片段（証拠）

以下は本機 `1.0.0` インスタンスの `/metrics` の**実際の出力**です（durable キュー `persist.q` を 1 つ作成済みのため、
`vhost`/`queue` ラベル付きの per-queue メトリクスが出現）：

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

## 3. メトリクス一覧（すべて実在、出典 `internal/management/metrics.go`）

| メトリクス | 型 | ラベル | セマンティクス |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | プロセスが自己申告する生存（現在は常に 1） |
| `speedmq_build_info` | gauge | `version`,`node` | ビルド情報、value は常に 1 |
| `speedmq_resource_blocked` | gauge | — | リソース水位が生産者をブロックしているか（1=ブロック中） |
| `speedmq_connections` | gauge | — | 現在の接続数 |
| `speedmq_channels` | gauge | — | 現在のチャネル数 |
| `speedmq_queues` | gauge | — | 現在のキュー数 |
| `speedmq_exchanges` | gauge | — | 現在のエクスチェンジ数 |
| `speedmq_consumers` | gauge | — | 現在のコンシューマ数 |
| `speedmq_queue_messages` | gauge | — | **グローバル**な就緒メッセージ総数 |
| `speedmq_queue_messages_unacknowledged` | gauge | — | **グローバル**な未確認メッセージ総数 |
| `speedmq_process_memory_bytes` | gauge | — | プロセスの**使用中**メモリ（`HeapInuse+StackInuse`） |
| `speedmq_memory_total_bytes` | gauge | — | 物理メモリ総量 |
| `speedmq_disk_free_bytes` | gauge | — | データディレクトリの空き容量 |
| `speedmq_memory_high_watermark` | gauge | — | メモリ水位比率 |
| `speedmq_disk_free_limit_bytes` | gauge | — | ディスク残量の下限 |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | あるキューの就緒メッセージ数 |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | あるキューの未確認メッセージ数 |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | あるキューのコンシューマ数 |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | あるキューのメモリ使用量推定 |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | キューの累計受信メッセージ数 |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | キューの累計配信メッセージ数 |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | キューの累計確認メッセージ数 |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | プラグインメタデータ、value は常に 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | プラグインがサービス中か（1=enabled，0=その他） |

### 3.1 使用上の注意（書き間違いを避ける）

- **同名で基数が異なる 2 つの系列ファミリー**：`speedmq_queue_messages_unacknowledged` は**グローバルなラベルなし系列**と、
  **per-queue のラベル付き系列**の**両方**を持ちます；一方「就緒」側はグローバルが `speedmq_queue_messages`、per-queue が
  `speedmq_queue_messages_ready`（名前が非対称）。ルールを書く際は `{queue=~".+"}` で per-queue のファミリーのみを明示的に取得してください。
- **`speedmq_process_memory_bytes` の扱い**：実装は `MemStats.HeapInuse + StackInuse`（**使用中メモリ**）で、
  カーネルのメモリ水位判定と同じ扱いです；ただしその `# HELP` の文言は「OS に要求したメモリバイト数」と書かれており、**文言と実際の扱いが一致していません**。
  本書の記載を優先してください。
- **per-queue 系列はキューが存在する場合のみ出現**：キューが削除されるとその系列は消えます（Prometheus 側では stale になります）。
  「キューが存在するはずなのにデータがない」というアラートには `absent()` や Grafana の `or vector(0)` を併用できます。
- **counter はプロセス再起動後にゼロへ戻る**：`*_total` はプロセス内の累計で、再起動すると 0 から始まります；`rate()`/`increase()` を使い、
  絶対値に直接しきい値を設定しないでください。
- **metrics にないクラスタシグナル**：§5 を参照。

---

## 4. アラートの意味と推奨対処（`prometheus-alerts.yml` に対応）

| アラート | 発火条件 | 意味 | 推奨対処 |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | スクレイプ対象が全体として到達不能 | プロセス/ポート/ネットワーク/認証を確認；再起動して起動ログを見る |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | スクレイプは成功したがプロセスが自己申告で非生存 | 保険的な項目、異常終了ログを確認 |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | プラグインが disabled/failed/down | `speedmqctl plugins show <name>` で `runtime_note` を確認；外部プラグインは `restart=always` で通常は自己回復 |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | メモリ/ディスク水位が発火し、生産者がブロックされている | メモリ水位とディスク空きを確認；コンシューマが進んでいるか確認 |
| `SpeedMQMemoryWatermarkHigh` | 使用中メモリ比率 > 0.9×水位 10m | メモリ水位に接近 | 滞留を減らす/消費速度を上げ、ブロックの発火を防ぐ |
| `SpeedMQDiskFreeLow` | 空き < 1.5×ディスク下限 10m | データディレクトリがほぼ満杯 | 拡張/クリーンアップ；下限に達すると生産者がブロックされる |
| `SpeedMQQueueBacklogGrowing` | 就緒 >10000 かつ 15m 単調増加 | キューの滞留が継続 | コンシューマを増やす / 消費側を確認；デッドレター/TTL の異常を調査 |
| `SpeedMQQueueNoConsumers` | コンシューマ=0 かつ就緒メッセージあり 15m | 誰も消費していない | 消費側プロセスを確認；コンシューマが切断されていないか確認 |
| `SpeedMQUnackedPileUp` | 未確認 >1000 15m | コンシューマがスタック/ack しない | コンシューマの処理ロジックと prefetch を確認；必要なら接続を閉じて再配信 |
| `SpeedMQConnectionSpike` | 接続 >10000 10m | 接続数が異常 | 接続リークを確認；クライアントは接続を再利用すべき |

> しきい値（10000 / 1000 など）は**出発点の値**です。あなたのキューの規模と業務特性に応じて調整してください。

---

## 5. 既知のギャップ：クラスタの「多数派喪失 / leader なし」には現状メトリクスがない

- **事実**：`/metrics` にはクラスタ系メトリクスが**一切ありません**（`speedmq_cluster_*` はない）。クラスタ状態は `GET /api/cluster` の JSON にのみ存在します：
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`。
- **したがって** `prometheus-alerts.yml` はクラスタメトリクスに基づくアラートを**意図的に書いていません**——書いても**絶対に発火しません**
  （Prometheus はメトリクス名が存在しないことでエラーにはならない）。それは「正しそうで、実際には無効」な成果物です。
- **自作の方法**（いずれか、いずれも SpeedMQ の外側に構築が必要で、本リポジトリの範囲外）：
  1. 汎用 JSON exporter で `/api/cluster` をスクレイプし、カスタムメトリクス（例 `speedmq_cluster_has_quorum`）にマッピングして、そのメトリクスにアラートする；
  2. プローブスクリプトで定期的に `/api/cluster` を呼び、`has_quorum=false` または `paused=true` のときアラートする。
- 関連するしきい値の扱い：`has_quorum=false` は多数派と疎通できないことを示します；`pause_minority`（デフォルト）下ではこのとき**サービスが一時停止し接続が切断されます**。

---

## 6. その他の**未検証**項目

- Grafana ダッシュボードは**実際の Grafana でのインポート検証を行っていません**（JSON 構文検証のみ通過）。
- アラートルールは**実際の Prometheus/Alertmanager でのロード検証を行っていません**（本機では Prometheus を起動していません）。
  ただしルール内の**メトリクス名は `/metrics` の実際の出力と 1 件ずつ照合済み**（§2/§3 を参照）で、「名前の間違いで永遠に発火しない」問題はありません。
