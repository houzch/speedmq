# SwiftMQ 備份與還原

> 本文中的「實測」結論全部來自 **Windows + PowerShell 5.1** 上的一次真實演練（臨時 `data_dir` 與臨時連接埠）。
> 演練命令與關鍵輸出原樣貼在 §6。**【未驗證】** 的部分會明確標註（叢集備份/還原、Docker 磁碟區備份等）。

---

## 1. 要備份什麼

`data_dir` 下**必須整份備份**，關鍵是下列這些（佈局見 `upgrade.md` §3）：

| 路徑 | 作用 | 遺失會怎樣 |
| --- | --- | --- |
| `meta/state.json` | 單機中繼資料快照：vhost / 交換器 / 佇列 / 綁定 / 使用者 / 權限 / 原則 | 拓撲與帳號全部遺失 |
| `meta/raft.log`、`meta/raft.state`、`meta/snapshot.json` | 【叢集】Raft 日誌 / 任期投票 / 快照+成員表 | 叢集身分與中繼資料一致性遺失 |
| `meta/users.seeded`、`meta/vhosts.seeded` | 引導標記 | 遺失會讓設定中的 users/vhosts 被**再次播種**（刪除的帳號/vhost 復活） |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | 經典佇列訊息資料與索引 | 持久訊息遺失 |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【叢集】仲裁佇列的 Raft 日誌/快照 | 仲裁佇列資料遺失 |
| 憑證檔案（設定中 `cert_file`/`key_file`/`ca_file` 指向的 PEM） | TLS 憑證 | 與 `data_dir` 分開備份，重新啟動後 TLS 起不來 |

> 軟狀態（未確認訊息、消費者、prefetch 計數）**只在記憶體**、不落盤，備份**不包含**也不應包含它們。

---

## 2. 一致性要求：**必須先停止處理程序**；熱備份**不安全**

### 2.1 結論

- ✅ **安全做法**：**停止 broker 處理程序**（優雅退出會做收尾刷盤），再複製 `data_dir`。
- ❌ **熱備份（處理程序仍在執行時直接複製檔案）：不安全，不做保證。**

### 2.2 為什麼熱備份不安全

訊息儲存是**兩個檔案**（段檔案 `*.seg` 與索引檔案 `index/*.idx`），二者**不是原子提交**：

- 還原時**以索引為準**判斷「哪些訊息還活著」，再依索引裡的 `(段號, 位移, 長度)` 去段檔案讀取。
- 熱備份時可能複製到**索引已引用、段檔案尚未寫全**（或反之）的中間狀態：
  - 索引引用了段裡不存在的記錄 → 該訊息**讀取失敗被略過**（等於遺失了已確認的持久訊息）；
  - 段裡有記錄但索引未引用 → 該訊息**不會被還原**。
- 還原雖然會用 CRC32 捨棄**尾端半寫記錄**，但那僅涵蓋「單一檔案尾端寫壞」，**無法修正索引與段之間的不同步**。

### 2.3 關於「寫入何時到磁碟」（實測觀察）

- 預設 `fsync: os` + `flush_interval_ms: 200`：訊息由後台刷盤協程在**至多約 200 ms** 內 `write()` 到作業系統
  （不 fsync），publisher confirm 也在此之後返回。
- 實測：發布持久訊息後**立即**查段檔案大小，已經能看到資料（`t=0ms seg=832`）；即「可被 OS 看到的位元組」與 confirm 基本同步。
- **注意**：這只說明「到了 OS 緩衝」，**強制終止處理程序不會遺失**（處理程序被終止不會遺失 OS 緩衝），但**斷電會遺失**。
  要「收到 confirm 即已 fsync 落盤」，請把 `storage.fsync` 改成 `batch` / `always`。**【斷電情境未實測】**

---

## 3. 備份步驟

### 3.1 單機（推薦）

```powershell
# 1) 停止處理程序（前景：Ctrl+C；背景：Stop-Process）
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) 複製整個 data_dir（附時間戳記）
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) （選用）驗證備份裡中繼資料快照可解析
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 叢集

- **每個節點各自備份自己的 `data_dir`**（中繼資料經 Raft 複製到全體，訊息資料在 Owner 節點，仲裁佇列副本在各自 Raft 目錄）。
- 停機順序：**一次只停一個節點**；不要同時停多個投票成員（見 `upgrade.md` §7.2）。
- 要得到**全叢集一致快照**，需依序停止全部節點後各自複製；生產環境中更常見的是「逐節點停/複製/啟動」。
- **【未驗證】** 本機未做真實叢集備份/還原演練。

### 3.3 Docker（具名磁碟區）

```powershell
# 停止容器後，用一次性容器把磁碟區內容封裝複製出來
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【未驗證】**（本機未執行 Docker）。

---

## 4. 還原步驟

### 4.1 單機

```powershell
# 1) 確認處理程序已停止
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) 移走（或刪除）目前 data_dir，避免新舊檔案混在一起
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) 用備份還原
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) 啟動
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

要點：
- **必須先把舊目錄移走**，不能「把備份檔案覆蓋到半殘留目錄上」；
- 還原的 `data_dir` 必須與備份時**同一 vhost/佇列集合**（目錄名稱是編碼後的，可跨機器使用）；
- **不要**趁還原之際去修改設定檔裡的 `vhosts`/`users`（只在首次引導生效，修改無效，見 `upgrade.md` §4.2）。

### 4.2 叢集

- 還原單一節點：依 §4.1 還原該節點 `data_dir` 後啟動，它會以既有成員身分重新加入並追上 Raft 日誌。
- 還原整個叢集：**先還原並啟動多數派節點**（≥ 半數投票成員），叢集才能選出 leader；再還原其餘節點。
- **【未驗證】** 叢集還原未實測。

---

## 5. 還原後的驗證方法

用管理 API 與真實用戶端交叉比對（建議全部執行）：

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) 物件總數與訊息總數（佇列數/交換器數/綁定數/使用者數；messages/ready/unacked）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) 逐佇列核對 messages / messages_ready（可對照備份前記錄）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / 使用者 / 原則 是否都在
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) 叢集（單機會回傳 enabled=false / mode=local / role=single）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **查看啟動日誌**：應出現 `已从磁盘恢复队列消息 ... messages=N` 與 `队列已恢复持久化消息 ... messages=N`；N 應與備份前一致。
- **日誌裡的警示**：若某佇列先前有訊息被消費/清空過，還原時可能出現
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` 與 `恢复时清理了无存活消息的段`。
  這些是**已結算（ack/purge）記錄**的索引殘留，屬於**已知日誌雜訊，不影響資料正確性**（見 §7）。
- **真實用戶端**：從佇列取回訊息並核對筆數/內容（見 §6 第 (7) 步）。

---

## 6. 實測演練（真實命令與輸出）

> 環境：`data_dir` 在臨時目錄，AMQP `127.0.0.1:5676`、管理面 `127.0.0.1:15677`、MQTT `127.0.0.1:1884`，
> 預設帳號 `guest/guest`。啟動日誌：
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) 建立 durable 拓撲 + 發布 5 筆持久訊息（真實用戶端 `amqp091-go`）**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) 建立使用者 / vhost / 權限 / 原則（管理 API）**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) 備份前狀態（管理 API）**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) 備份前磁碟檔案**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) 停止處理程序 → 備份 → 清空 → 還原**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) 重新啟動後的還原日誌（關鍵行）**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> 同時出現若干筆 `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`：
> 這些是本演練中**先前被 purge 掉的**訊息留下的索引殘留（已結算、段資料已回收），**不影響下列 5 筆訊息的還原**。

**(7) 還原後斷言：管理 API + 真實用戶端**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**結論**：durable 拓撲（交換器+佇列+綁定）、5 筆持久訊息、使用者、vhost、權限、原則**全部還原**，
真實用戶端能依原樣取回全部訊息。**演練通過。**

### 6.1 對照：未經歷消費/purge 的佇列還原更「安靜」

為區分上述 WARN 是否為普遍現象，另做一次**受控對照**：新建 durable 佇列 `clean.q`、發布 3 筆持久訊息、
**不消費不清空**，停止處理程序後重新啟動：

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**無任何 WARN**。說明 WARN 只出現在「索引裡還留著已結算記錄」的情境（見 §7）。

---

## 7. 已知問題與限制（如實登錄）

1. **還原日誌雜訊（真實觀察到）**：當佇列歷史上發生過消費/清空（訊息已 ack/purge），
   其索引中仍保留對已回收記錄的引用，還原時會為每筆**逐筆列印 `恢复消息失败，已跳过` WARN**，
   並新建/清理一個空的 `000000.seg`（日誌 `恢复时清理了无存活消息的段`）。
   **不影響資料正確性**（存活的未確認訊息能正確還原），但會**污染日誌**、在大型佇列/高吞吐下可能洗版。
   建議：以 `已从磁盘恢复队列消息 ... messages=N` 為準，忽略這些針對已結算記錄的 WARN；
   若日誌量無法接受，請回報給核心維護者（本文檔不修改程式碼）。
2. **熱備份不安全**（§2）：不要在處理程序執行時直接複製 `data_dir`。
3. **`fsync: os` 不保證斷電不遺失**：要「confirm 即落盤」請用 `batch` / `always`。
4. **密碼為明文**：`meta/state.json` 裡的使用者密碼是**明文**（實測可見 `"password":"drillpass"`）——
   備份檔案因此**必須視為敏感資料處理**（存取控制、加密存放）。詳見 `security-baseline.md`。
5. **【未驗證】** 叢集備份/還原、Docker 磁碟區備份/還原、斷電情境、還原過程中的並行寫入。
