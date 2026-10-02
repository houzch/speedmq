# SwiftMQ 升級與移轉方案

> 適用版本：`1.0.0`（`broker.Version`，見 `/metrics` 的 `swiftmq_build_info`）。
> 本文所有「實測」結論均來自本機真實執行；凡未實測的，均明確標註 **【未驗證】**。
> 本機環境：Windows + PowerShell 5.1，Go 1.27.1 windows/386，臨時 `data_dir` + 非預設連接埠。

---

## 1. 移轉（從 RabbitMQ 切換到 SwiftMQ）

本專案定位是 **AMQP 0-9-1 協定級相容**，因此「移轉」以**修改連線位址**為主：

- 業務程式碼零修改，只改 `host/port/vhost`（設計文件 G3「移轉零成本」）。
- 管理工具鏈（`rabbitmqadmin`、管理 UI、監控指令碼）指向管理面連接埠即可，介面形狀對齊 RabbitMQ（`amq.default`、`%2F`、`{error, reason}` 等約定照搬）。
- 預設連接埠與 RabbitMQ 一致：AMQP `5672`、管理面 `15672`；MQTT 為 `1883`，節點間 RPC 為 `25672`。

**移轉前需自行檢查的語意差異**（均為本儲存庫刻意為之，依據 README / 設計文件）：

| 項目 | SwiftMQ 行為 | 移轉影響 |
| --- | --- | --- |
| 瞬時（非持久且非獨佔）佇列 | **拒絕宣告**（541），`auto_delete` 不豁免 | 舊用戶端若依賴該類佇列會失敗，需改為 durable 或 exclusive |
| 預設 vhost `/` | **不可刪除**（400），RabbitMQ 允許 | 自動化指令碼若刪除預設 vhost 會失敗（這是唯一主動安全限制） |
| 經典佇列資料 | **不複製**，資料只在 Owner 節點 | 需要跨節點備援請改用仲裁佇列 `x-queue-type=quorum` |
| 仲裁佇列 | 支援擴充副本，**不支援縮容** | 規劃時一次到位 |
| 外掛 | 無 Erlang 外掛生態，AMQP 1.0 / STOMP 未實作 | 用到這些協定的情境暫不可移轉 |

**資料移轉**：SwiftMQ 與 RabbitMQ 儲存格式不相容，**不提供線上/離線資料搬運工具**。
移轉方式為「新建空 SwiftMQ → 雙跑驗證 → 灰度切流」。**【未驗證】** 本文不含任何真實 RabbitMQ 資料搬運演練。

---

## 2. 升級總原則

1. **先備份**（見 `backup-restore.md`）——升級失敗的保底。
2. **先停處理程序再替換**（資料目錄有單一寫入者限制，見 §4.2）。
3. **升級後必須驗證**：處理程序能啟動、`/api/overview` 能讀取、`/metrics` 能抓取、佇列訊息數與備份前一致。
4. 叢集升級**逐節點滾動**，一次只動一個節點（見 §5）。

---

## 3. 資料目錄佈局（升級/移轉的事實依據）

以本機單機執行個體**實測**得到的 `data_dir` 佈局：

```
data/
├── meta/
│   ├── state.json        # 單機模式的中繼資料快照（vhost/交換器/佇列/綁定/使用者/權限/原則）
│   ├── users.seeded      # 引導標記：設定檔裡的 users 已播種過
│   ├── vhosts.seeded     # 引導標記：設定檔裡的 vhosts 已播種過
│   ├── raft.state        # 【叢集模式】Raft 任期/投票
│   ├── raft.log          # 【叢集模式】Raft 日誌
│   └── snapshot.json     # 【叢集模式】Raft 快照 + 成員表
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # 段檔案（訊息內文 + 屬性），記錄格式：<len u32><crc32 u32><payload>
│   └── index/000001.idx  # 佇列索引：seq-id → (段號, 段內位移, 長度, 狀態)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【叢集】仲裁佇列每個佇列一個 Raft 組（日誌/快照）
```

**注意（與直覺不同的兩點，均以程式碼/實測為準）**：

- 叢集模式下 Raft 持久化檔案**直接放在 `meta/` 下**（`raft.state` / `raft.log` / `snapshot.json`），
  **不存在 `meta/raft/` 子目錄**。依據：`internal/raft/log.go` 的檔名常數 + `internal/broker/cluster.go`
  裡 `Dir: filepath.Join(b.cfg.DataDir, "meta")`。**【叢集佈局未實測】**（本機只執行了單機執行個體）。
- 目錄名稱**不是原始的 vhost / 佇列名稱**，而是 `store.SafeDirName` 編碼：加上前綴 `q_`，非 `[A-Za-z0-9._-]` 位元組依 `%XX` 跳脫。
  實測：vhost `/` → 目錄 `q_%2F`，佇列 `persist.q` → 目錄 `q_persist.q`。
  這樣設計是為了避免路徑穿越與 Windows 保留裝置名稱（`con`/`nul` 等）。

---

## 4. 資料相容性

### 4.1 舊資料能否直接讀取——能

- **索引格式向前相容**：M8-1 在索引記錄裡加了「段號」欄位（25 位元組）；**舊格式（21 位元組，無段號）仍可原樣讀取**，
  讀取時等效於「只有一個段（seg=1）」，**升級不需要移轉指令碼**。
  依據：`internal/store/store.go` 的 `indexEntrySize` / `legacyIndexEntrySize` 常數與 `recover()` 邏輯；README M8-1。
- **當機語意不變**：每筆記錄帶長度前綴 + CRC32，還原時**捨棄尾端半寫/損壞記錄**並截斷。
  實測（見 `backup-restore.md` §6）：處理程序停止後重新啟動，durable 佇列的 5 筆持久訊息**全部還原**，
  日誌出現 `已从磁盘恢复队列消息 ... messages=5`。

### 4.2 設定中 `vhosts` / `users` 的定義（升級時最容易踩的坑）

- 二者**只在首次引導時生效**：首次啟動會把設定裡的 vhosts/users 寫進中繼資料並落下標記檔案
  `meta/vhosts.seeded` / `meta/users.seeded`；**此後以中繼資料為準**。
- 因此**升級/更換設定時，不要指望透過修改設定檔來增刪帳號或 vhost**——修改了也不生效；
  請使用管理 API 或 `swiftmqctl`。
- 反過來說，升級**不會**用設定覆蓋既有帳號：執行期改過的密碼不會因重新啟動而被改回設定裡的舊值，
  執行期刪除的帳號也不會復活。依據：`cluster.go` 的播種標記邏輯；README M8-4 / M8-7。

### 4.3 段輪替與磁碟回收

- 訊息依大小分段（預設 8 MiB），**段內訊息全部 ack 且段已封口後整段刪除**，索引隨之壓縮重寫。
- 升級不改動該行為；舊執行個體留下的單段檔案在新的段輪替邏輯下照常運作。

---

## 5. 二進位檔升級（裸機）

> 本機**未跨版本真機演練**（儲存庫目前只有一個版本 `1.0.0`，無可升級的舊二進位檔）。以下步驟為本儲存庫已具備能力的**同版本重播驗證 + 通用流程**，跨版本部分標註 **【未驗證】**。

### 5.1 步驟

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) 停止處理程序（優雅退出會收尾刷盤；見 §4「一致性」）
#    若以前景方式執行：Ctrl+C；若以服務方式：Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) 備份資料目錄（務必在處理程序停止後）
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) 替換二進位檔（把新版本 swiftmqd.exe / swiftmqctl.exe 放到原路徑）
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) 啟動
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) 驗證：處理程序存活 + 管理 API 能讀取
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 升級後驗證清單

- 啟動日誌出現 `SwiftMQ 启动中 ... version=<新版本>` 與 `管理面已启动`；
- `/api/overview` 的 `object_totals` / `queue_totals` 與備份前一致（對照 `backup-restore.md` §5）；
- `/api/queues` 中每筆 durable 佇列的 `messages` / `messages_ready` 與備份前一致；
- `/metrics` 可抓取且 `swiftmq_plugin_up{name="amqp091"} 1`、`{name="mqtt"} 1`。

---

## 6. 映像檔升級（容器）

映像檔約 13 MB（靜態連結二進位檔 + alpine），**以非 root（uid 10001）執行**，資料目錄掛載在 `/var/lib/swiftmq`。

```powershell
# 1) 拉取/建置新映像檔（tag 用新版本號，避免 old/new 混淆）
docker build -t swiftmq:1.0.0 .

# 2) 停止舊容器（compose 會保留具名磁碟區 swiftmq-data）
docker compose down

# 3) 啟動新版本（compose 檔案裡把 image 改成新 tag）
docker compose up -d

# 4) 狀態與日誌
docker compose ps
docker compose logs -f --tail 100
```

> **容器內一次性任務**（例如在容器內執行 `swiftmqctl`）：`docker compose ...` 的 `run` 在非互動環境必須加上 `-T`，
> 否則會因申請 TTY 失敗：
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

資料持久化依賴 compose 的**具名磁碟區** `swiftmq-data`，容器重建不會遺失資料（M4 起真正落盤）。
升級前若需備份磁碟區內容，等效於備份 `/var/lib/swiftmq`（見 `backup-restore.md` §3.2）。**【映像檔升級未實測】**（本機未執行 Docker）。

---

## 7. 灰度與回滾

### 7.1 單機

- **灰度**：SwiftMQ 單機沒有內建「新舊同處理程序雙版本」能力。可行的灰度是**旁路影子**：
  新版本執行個體先用**唯讀消費/影子佇列**掛到同一份上游流量上觀察，確認無誤後再切換寫入方。
- **回滾**：
  1. 停止新版本處理程序；
  2. 換回舊二進位檔；
  3. 若新版本已寫入過資料，**必須用升級前備份還原 `data_dir`**（見下）。
  **不會**出現「新版本已寫過、舊版本直接讀」的保證——跨版本降級見 §8。

### 7.2 叢集（滾動升級）

平台端不提供「一鍵滾動升級」，需依下列順序人工逐個節點操作：

1. **一次只升級一個節點**：停止該節點 → 備份其 `data_dir` → 換二進位檔 → 啟動 → 等待它重新加入並追上
   （`swiftmqctl cluster_status` / `GET /api/cluster` 查看 `role`、`commit_index`/`last_applied`）。
2. **順序建議**：先升級 **learner / 非投票成員**（對多數派無影響），再升級 **follower**，最後升級 **leader**
   （升級 leader 會觸發一次選主，期間有短暫不可寫入）。
3. **停機對多數派的影響**（關鍵）：
   - 3 節點叢集：**同時最多停 1 個**投票成員，停 2 個即失去多數派，`pause_minority` 下**整個叢集暫停服務**。
   - 2 節點叢集：停 1 個就失去多數派，**不具備滾動升級能力**（建議至少 3 節點）。
   - 因此滾動升級時**嚴禁一次停多個投票成員**。
4. **成員變更與升級不要同時進行**：成員變更**無 joint consensus**，一次只允許一個未提交的設定變更；
   升級期間請避免同時 `add_member` / `remove_member`。
5. 升級完成後核對 `GET /api/cluster` 的 `object_totals` 與升級前一致。

> **【未驗證】** 本機未做真實叢集的滾動升級演練（叢集路徑與容器均未執行）；上述順序來自訊息中介軟體
> 與 Raft 的通用限制以及本儲存庫 `pause_minority` / 成員變更的實作事實，不是本機實測結論。

---

## 8. 不支援 / 未驗證的部分（明確列出）

- **跨大版本降級：不支援、未驗證**。若新版本已用新格式/新語意寫入資料，**沒有**「回退到舊二進位檔照讀」的保證；
  回滾只能靠升級前備份。
- **設定格式不變**：仍為 JSON + `SWIFTMQ_*` 環境變數。**YAML 設定尚未支援**（需引入解析相依性，M8-17 待評估），
  升級不會帶來 YAML。
- **線上外掛/協定熱升級**：外掛隨核心編譯進來（A 形態）或依設定 `spawn` 拉起（B 形態），
  升級核心 = 重新啟動處理程序；**沒有**原地熱替換二進位檔的機制。
- **儲存引擎原地移轉**：段輪替/索引壓縮是執行期背景行為，**沒有**獨立的「資料移轉/壓縮」命令。
- **真實網路下的叢集升級**：本儲存庫只做了縮比混沌（處理程序級 kill），**未做**網路分割、磁碟寫滿下的升級演練。
- 本文檔**未包含**任何 SwiftMQ 與其他 broker（RabbitMQ）之間的資料搬運驗證。
