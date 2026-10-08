# SpeedMQ 安全強化基準（可勾選清單）

> 原則：**只寫本儲存庫真實具備的能力**。每條給出「為什麼要做 + 如何驗證已做」，驗證命令均可執行。
> 標註 **【已驗證】** 的表示在本機（Windows + PowerShell 5.1，`1.0.0`）**真的執行過**；
> **【未驗證】** 表示未執行或目前做不到，絕不假裝。
> 所有命令以 `/bin/sh` 風格給出 curl 版本，並附上 PowerShell 版本（PowerShell 5.1 請使用
> `Invoke-WebRequest ... -UseBasicParsing`）。

---

## A. 認證與存取控制

### A-1. 修改預設帳號 `guest/guest` 【已驗證】

- **為什麼**：預設內建 `guest/guest`（標籤 `administrator`），對外暴露即等於門戶洞開。
- **怎麼做**：於執行期修改密碼 / 刪除帳號，**不要**去修改設定檔（`users` 只在首次引導生效）。

```bash
# 修改密碼
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# 或直接刪除預設帳號（先確保已建立新的管理員）
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **如何驗證**：修改後舊密碼必須 401、新密碼 200。
  **【已驗證】** 本機實測輸出：

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] 預設帳號已修改/已刪除

### A-2. 最小權限：依 vhost 的 `configure` / `write` / `read` 正規表達式 【已驗證】

- **為什麼**：三分類對齊 RabbitMQ —— `configure` 管拓撲宣告/刪除、`write` 管發布與綁定、`read` 管消費與拉取；
  越權回傳 403。給業務帳號只開它需要的。
- **怎麼做**：`PUT /api/permissions/{vhost}/{user}`，例如唯讀消費：`{"configure":"^$","write":"^$","read":".*"}`。

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **如何驗證**：用受限帳號嘗試越權操作，應得到 403 `ACCESS_REFUSED`。
  **【已驗證】** 本機用真實 AMQP 用戶端以 `configure="^$"` 的使用者宣告交換器，實測：

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] 每個業務帳號只授予必要的正規表達式，且未給予 `administrator`/`management` 標籤

### A-3. `administrator` 標籤的定義（隱含完整權限）—— 謹慎授予 【已驗證】

- **為什麼**：**`administrator` 標籤的使用者對其可見的所有 vhost 擁有完整權限、無需權限記錄**
  （對齊 RabbitMQ 實測定義，見 README / 設計 M8-7）。也就是說，只要給了這個標籤，
  權限正規表達式就不起作用了——它是最高權限。
- **怎麼做**：只有管理面/維運帳號給予 `administrator`；業務帳號一律不給標籤、只走權限正規表達式。

- **如何驗證（示範隱含權限）**：對一個**沒有任何權限記錄**的新 vhost，`administrator` 使用者應可直接使用。
  **【已驗證】** 本機實測：新建 vhost `drillvh`（未建立任何權限記錄）後，`guest`（administrator）在其上宣告拓撲成功：

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] `administrator` 標籤僅授予極少數維運帳號

### A-4. `remote_access`：限制帳號僅本機登入 【未驗證（同機無法模擬遠端來源）】

- **為什麼**：對齊 RabbitMQ，內建 `guest` 預設僅允許本機登入；對外部署時應確保特權帳號的來源受限。
- **怎麼做 / 定義（重要限制）**：
  - `remote_access` 只能寫在**設定檔**的 `users.<name>.remote_access` 裡，**僅首次引導生效**；
  - **透過管理 API / `speedmqctl` 建立的帳號一律 `remote_access=true`**（允許任意來源登入）——
    依據 `internal/broker/observe.go` 的 `UpsertUser` 註解與實測 `meta/state.json` 中
    `"remote_access":true`。也就是說 **API 目前無法把某個帳號限制為僅本機**。
- **如何驗證**：從**另一台主機**（非 `127.0.0.1`）用該帳號連線，應被 403；本機連線應成功。
  **【未驗證】**：本機環境無法構造真實遠端來源，未實測。
- [ ] 特權帳號的來源限制已依上述定義評估（注意 API 建立帳號預設開放遠端）

---

## B. 傳輸安全（TLS）

TLS 設定項（接入層與管理面**共用**同一組欄位）：`cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`。

### B-1. 啟用 TLS 且設定錯誤即拒絕啟動 【已驗證】

- **為什麼**：憑證在**啟動時**讀取並驗證——設定錯誤立刻拒絕啟動，而不是等第一個用戶端連上來才暴露。
- **怎麼做**：在 `listeners.<plugin>[].tls` 或 `management.tls` 裡提供 `cert_file` + `key_file`（**同時提供**才啟用）。

- **如何驗證**：用錯誤設定啟動，應立刻失敗。
  **【已驗證】** 本機三種錯誤設定實測，全部 `exit=1`、拒絕啟動：

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **如何驗證（正向/反向）**：TLS 用戶端能連線、明文用戶端連 TLS 連接埠會被拒。
  **【已驗證】** 本機啟動 TLS 執行個體（`amqp091` 走 TLS），用真實用戶端探針：

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] 對外協定連接埠已啟用 TLS

### B-2. `min_version` 至少 1.2 【已驗證】

- **為什麼**：停用過舊的 TLS 版本；預設即 `1.2`，可選 `1.2` / `1.3`。
- **如何驗證**：把 `min_version` 寫成 `1.0`，啟動應報錯（見 B-1 的 `badtls2` 輸出）。
- [ ] `min_version` 為 `1.2` 或 `1.3`

### B-3. 雙向認證 `client_auth: require_and_verify`（mTLS） 【部分驗證】

- **為什麼**：要求用戶端出示並驗證憑證，防止未授權用戶端接入協定連接埠。
- **怎麼做**：設定 `ca_file` + `client_auth: require_and_verify`（後兩者要求同時提供 `ca_file`）。
- **【已驗證】**：TLS 端到端與被拒路徑已用真實用戶端驗證（B-1）。**mTLS（要求並驗證用戶端憑證）本機未單獨演練**。
- [ ] 需要 mTLS 的連接埠已設定 `require_and_verify` + `ca_file`

### B-4. 管理面 TLS 【未驗證】

- **為什麼**：管理面以 Basic Auth 傳遞密碼，必須加密。
- **怎麼做**：`management.tls` 使用與協定監聽相同的欄位。
- **【未驗證】**：本機演練把管理面綁在本機明文連接埠，未單獨啟動管理面 HTTPS。
- [ ] 管理面已啟用 TLS（或嚴格限制在可信網路內）

---

## C. 暴露面收斂

### C-1. 管理面監聽範圍收斂 【已驗證（監聽位址實測）】

- **為什麼**：管理面預設 `:15672`（所有網路卡）。對外部署應綁到內網/迴環位址，或用防火牆限制來源。
- **怎麼做**：`management.addr` 設定成 `127.0.0.1:15672` 或內網位址；或將 `management.enabled=false` 徹底關閉
  （關閉後無管理連接埠，但 `speedmqctl` 也隨之不可用）。
- **如何驗證**：
  **【已驗證】** 本機把管理面設定成 `127.0.0.1:15677`，實測監聽位址確為迴環：

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] 管理面綁定位址已收斂（或已停用）

### C-2. 協定連接埠只開必要的 【未驗證】

- **為什麼**：預設同時開啟 AMQP `5672` 與 MQTT `1883`；不用 MQTT 就關掉，縮小攻擊面。
- **怎麼做**：`plugins.mqtt.enabled=false`（或從 `listeners` 裡移除）；停用走的是**關閉真實連接埠**，不只是改狀態位。
- **如何驗證**：停用後對應連接埠不再監聽（`Get-NetTCPConnection -State Listen` 看不到）。
  **【未驗證】**：本機演練兩種協定都開著，未單獨驗證關閉後連接埠消失。
- [ ] 未使用的協定外掛已停用

---

## D. 容器執行強化

儲存庫映像檔事實（`Dockerfile`）：靜態連結二進位檔 + alpine，**以非 root（uid 10001，使用者 `speedmq`）執行**，
資料目錄 `/var/lib/speedmq` 為磁碟區。`docker-compose.yml` 使用**具名磁碟區**持久化、設定**唯讀掛載**、日誌輪替。

### D-1. 非 root 執行 【未驗證（本機未執行 Docker）】

- **為什麼**：最小權限，降低容器逃逸後的影響面。
- **怎麼做**：映像檔預設已是 uid 10001；**不要**用 `--user root` 覆蓋。
- **如何驗證**：`docker compose run -T --rm broker id` 應顯示 `uid=10001`。（`run` 在非互動環境必須加上 `-T`）
- [ ] 容器以非 root 執行（未以 root 覆蓋）

### D-2. 唯讀根檔案系統 + 資源限額 + 能力裁剪（建議，儲存庫 compose 未預設開啟） 【未驗證】

- **為什麼**：唯讀根檔案系統可阻止執行期竄改二進位檔；資源限額防單一容器拖垮主機；裁剪 capabilities 可縮小核心攻擊面。
- **怎麼做**（範例，視需要合併到 compose 的 `broker` 服務）：

```yaml
services:
  broker:
    read_only: true
    tmpfs:
      - /tmp
    security_opt:
      - no-new-privileges:true
    cap_drop: ["ALL"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8g
```

- **如何驗證**：容器內嘗試寫入根路徑應失敗（唯讀）；`docker inspect` 可見資源限額。
  **【未驗證】**（本機未執行 Docker）；且**唯讀根檔案系統需確認 `data_dir` 位於可寫入的磁碟區上**，否則核心無法落盤。
- [ ] 已評估唯讀根檔案系統與資源限額（注意 `data_dir` 必須位於可寫入的磁碟區）

---

## E. 已知限制（目前確實做不到，不要指望）

以下均為**事實性缺口**，請在安全設計裡明確承認，不要假設它們存在：

1. **密碼是明文儲存與複製的**。`meta/state.json` 裡 `password` 欄位為明文（**【已驗證】** 實測可見
   `"password":"drillpass"`）；設定檔中也是明文。**沒有**密碼雜湊（雜湊與外部認證後端留給認證外掛）。
   → 後果：**資料目錄與備份檔案等同敏感憑證**，必須做好檔案權限與加密保護。
2. **沒有稽核日誌**。管理面的增刪改會輸出一般日誌（如 `管理面更新用户 actor=... user=...`），
   但**沒有**獨立、不可竄改的稽核流，也沒有「誰在什麼時候改了什麼」的合規級記錄。
3. **沒有 LDAP / OAuth2 / JWT 等外部認證**。v1 內建僅 `PLAIN` / `AMQPLAIN`
   （`auth.Store.Mechanisms()` 實測只回傳這兩個）。
4. **SASL `EXTERNAL` 未實作**：即使設定了 mTLS，協定層**仍走 PLAIN 密碼認證**
   （「用用戶端憑證免密碼」這一步並沒有）。憑證只是傳輸層驗證。
5. **`remote_access` 無法經由 API 設定**：管理 API/CLI 建立的帳號一律允許遠端登入（見 A-4），
   無法把單一帳號限制為僅本機。
6. **管理面無獨立來源白名單 / 無限流**：只能靠綁定位址、防火牆、TLS 收斂暴露面。
7. **無外掛沙箱**：A 形態外掛與核心同處理程序；B 形態外部外掛雖有處理程序隔離，但**資料面走本機連線代理**、
   且外掛可呼叫核心語意（受 vhost 與權限驗證限制），**不是**安全沙箱。
8. **管理面標籤僅有 `administrator`/`management`/`monitoring` 三種**，沒有更細的 per-resource RBAC。

---

## F. 彙總清單

- [ ] A-1 預設帳號已修改/已刪除 【已驗證流程】
- [ ] A-2 業務帳號最小權限（正規表達式），無管理員標籤 【已驗證 403 路徑】
- [ ] A-3 `administrator` 標籤僅授予維運帳號 【已驗證隱含權限定義】
- [ ] A-4 特權帳號來源限制已評估（注意 API 建立帳號預設開放遠端）
- [ ] B-1 對外連接埠啟用 TLS，設定錯誤即拒絕啟動 【已驗證】
- [ ] B-2 `min_version` ≥ 1.2 【已驗證】
- [ ] B-3 需要 mTLS 的連接埠設定 `require_and_verify` + `ca_file`
- [ ] B-4 管理面啟用 TLS
- [ ] C-1 管理面綁定位址收斂 【已驗證監聽位址】
- [ ] C-2 未使用的協定外掛停用
- [ ] D-1 容器以非 root 執行
- [ ] D-2 唯讀根檔案系統 / 資源限額 / 能力裁剪已評估
- [ ] E 已知限制（明文密碼、無稽核、無 LDAP/OAuth2、SASL EXTERNAL 未實作）已在安全設計中承認
