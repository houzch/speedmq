<!-- i18n-switcher -->
[简体中文](../../../README.md) | **繁體中文** | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

以 Go 撰寫的 **RabbitMQ 相容**訊息中介軟體。現有的 RabbitMQ 用戶端**不需修改程式碼、不需更換 SDK**，只要修改連線位址即可接入。

## 簡介

- **協定相容**：AMQP 0-9-1（含 RabbitMQ 擴充）與 MQTT 3.1.1；相容基準為 **RabbitMQ 4.3 語意**。
- **部署簡單**：一個二進位檔 / 一個容器，管理 UI 已內嵌，不需要額外的 Nginx、資料庫或 Node 執行環境。
- **維運夠用**：管理 UI（佇列 / 交換器 / 連線 / 帳號權限 / 虛擬主機 / 原則 / 限制 / 叢集）、Prometheus `/metrics`、命令列 `swiftmqctl`。
- **預設連接埠**：`5672`（AMQP）、`1883`（MQTT）、`15672`（管理 UI / HTTP API / 指標）。

已具備的能力：持久化（段日誌 + fsync 檔位 + 當機恢復）、發布確認、TTL / 死信 / 長度限制、消費者優先級、Direct Reply-To、叢集（Raft 中繼資料 + 仲裁佇列 + 跨節點轉發）、外掛熱啟停。

***

## 快速開始

### 方式一：Docker（推薦）

**不用複製倉庫，直接拉取映像檔跑起來：**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.0
```

映像檔同時發佈在兩處（內容相同，挑網路較快的）：Docker Hub `houzch/swiftmq`、GitHub GHCR `ghcr.io/houzch/swiftmq`；兩個倉庫都提供 `linux/amd64` 與 `linux/arm64`。

- 資料落在具名磁碟區 `swiftmq-data`，容器重建不會遺失。
- 停止 / 刪除：`docker stop swiftmq`、`docker rm swiftmq`（資料磁碟區保留）。

**要調整設定或用 compose 編排，再複製倉庫：**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # 使用已發佈的映像檔；改成 up -d --build 則在本機建置

docker compose ps        # 狀態應為 Up (healthy)
docker compose logs -f   # 追蹤日誌
```

- 設定以唯讀方式掛載 `configs/swiftmqd.json`，修改後 `docker compose restart` 即生效。
- 停止：`docker compose down`（保留資料）；`docker compose down -v`（連資料一併刪除）。

### 方式二：本機二進位檔（需要 Go 1.24+）

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> 管理 UI 的建置產物不入庫。若要用 UI，先在 `web/` 執行 `npm ci && npm run build`；
> 不建置也能正常啟動收發訊息，只是存取 `/` 會提示「管理 UI 未建置」。

### 首次登入（務必先改掉預設帳號）

| 入口 | 位址 / 憑證 |
| --- | --- |
| 管理 UI | <http://localhost:15672/>（使用者名稱 `guest`，密碼 `guest`） |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883`（帳號同上） |

新安裝執行個體的總帳號帶有「首次登入強制改密」標記：管理 UI 登入後會**強制要求同時修改帳號名稱與密碼**，修改完成後才能進入後台。

也可以直接呼叫 API 完成（適合自動化）：

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ 預設 `guest/guest` 與 RabbitMQ 行為一致：**只允許本機登入**。從容器外 / 遠端連線需在設定中為該使用者開啟 `remote_access`（範例設定已為容器情境開啟）。
> **服務一旦對外可存取，請立即更換憑證。**

### 接入你的應用程式（只要改連線位址）

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
# MQTT（mosquitto 用戶端）
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

管理 HTTP API 與 `rabbitmqadmin` 相容；管理 UI 的「新增佇列 / 交換器」就是標準宣告端點，指令碼同樣能做到：

```bash
# 宣告佇列（仲裁佇列以 arguments: {"x-queue-type":"quorum"} 表達）
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### 日常維運

| 事項 | 入口 |
| --- | --- |
| 管理 UI | <http://localhost:15672/>：佇列 / 交換器 / 連線 / 帳號權限 / 虛擬主機 / 原則 / 限制 / 功能開關 / 叢集，右上角可設定自動重新整理與**介面語言** |
| 監控指標 | <http://localhost:15672/metrics>（Prometheus 文字，需認證）；面板與警示見 [docs/ops/monitoring](ops/monitoring/README.md) |
| 命令列 | `./bin/swiftmqctl status`、`list_queues`、`plugins list`、`plugins disable amqp091`（熱停用，連接埠立即關閉） |
| 健康檢查 | `nc -z 127.0.0.1 15672`（compose 已內建 healthcheck） |
| 備份與還原 | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| 升級 | [docs/ops/upgrade.md](ops/upgrade.md) |
| 安全基準 | [docs/ops/security-baseline.md](ops/security-baseline.md) |

常用設定（完整範例見 [configs/swiftmqd.json](../../../configs/swiftmqd.json)，也可用 `SWIFTMQ_*` 環境變數覆寫）：

| 設定項 | 說明 | 預設 |
| --- | --- | --- |
| `data_dir` | 資料目錄（訊息 + 中繼資料），**務必持久化** | `data` |
| `listeners` | 各協定監聽位址，可設定 TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | 管理 UI / API 監聽位址 | `:15672` |
| `management.language` | 管理 UI 預設語言；留空則依部署地時區自動選擇 | 自動 |
| `storage.fsync` | 落盤檔位 `none / os / batch / always`（同時決定 confirm 時機） | `os` |
| `storage.memory_high_watermark`、`storage.disk_free_limit` | 資源水位：觸發即阻塞生產者，**不遺失訊息** | `0.4` / 50 MiB |
| `users` | 內建使用者表（密碼 + 標籤 + `remote_access`） | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | 多節點叢集（預設關閉），成員變更使用 `swiftmqctl add_member` | 關閉 |

> 連接埠可能被占用：用 `listeners` / `management.addr` 換成其它連接埠即可。

***

## 專案結構

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # broker 處理程序入口（要跑的就是它）
│   └── swiftmqctl/      # 維運 CLI（走管理 HTTP API，與核心版本解耦）
├── internal/            # 核心實作
│   ├── protocol/        # 協定外掛：amqp091、mqtt（編解碼 / 方法 / 工作階段）
│   ├── broker/          # 核心：vhost、交換器、佇列、死信、流量控制、管理面檢視
│   ├── store/           # 持久化：段日誌、佇列索引、當機恢復
│   ├── raft/ meta/      # 叢集：自研 Raft 與中繼資料複製
│   ├── management/      # 管理 HTTP API + Prometheus 指標 + 內嵌 UI 靜態服務
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # 對外穩定契約：外掛 API（plugin）與外部處理程序外掛線協定（sidecar）
├── web/                 # 管理 UI 前端工程（Vue 3 + Vite），產物建置時經 go:embed 打進二進位檔
├── configs/             # 範例設定
├── docs/ops/            # 維運文件：備份還原 / 升級 / 安全基準 / 監控
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（交流群組二維條碼）
```

***

## 貢獻

歡迎提交 Issue 與 Pull Request。本專案的立身之本是**協定相容**，因此：

- 修 bug 請說明對應的 RabbitMQ 行為（版本、用戶端、重現步驟）；
- 涉及協定細節的變更，請附上與 RabbitMQ 的對照結果；
- 提交前請確保 `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` 均通過。

***

## 授權條款

本專案採用 [Apache License 2.0](../../../LICENSE)。

允許使用、修改、散布（含商業使用），需保留著作權與授權聲明，且不提供任何擔保。

Copyright 2026 houzch（見 [NOTICE](../../../NOTICE)）

***

## 致謝

AMQP 0-9-1 協定規範與 [RabbitMQ](https://www.rabbitmq.com/) 的行為語意是本專案相容性工作的對照基準。本專案為獨立實作，與 RabbitMQ 官方無隸屬關係，未使用其程式碼。

***

## 加入交流群

掃碼加入 SwiftMQ 交流群，有問題可以在群裡直接問：

![SwiftMQ 交流群](../../../1280X1280.PNG)
