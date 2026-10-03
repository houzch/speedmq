# SwiftMQ 外部行程外掛開發指南 —— PHP

> **適用對象**：用 PHP 為 SwiftMQ 撰寫外部行程外掛（sidecar）的開發者。
> **先讀**：[外部行程外掛（sidecar）開發指南](plugin-development.md)（心智模型 / 設定欄位 / 線路協定總表）。
> **範例專案**：工作區 `swiftmq-plugin/php/sidecar_plugin.php`（僅標準函式庫，**無需 composer 相依**）。

---

## 1. 它跑起來是什麼樣

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三條要點：**你的行程是伺服端**（等核心來連）；**對外連接埠由核心開啟**（`protocols[].listeners`）；
**`prefix` 必須非空**（空前綴 = 不參與嗅探，連線不會被交給你；實測會被立刻中斷，≤8 位元組 ASCII）。

---

## 2. 三步跑起來

### 第一步：設定

`swiftmqd.json`（**實際設定是標準 JSON，不能帶註解**）：

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/swiftmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### 第二步：啟動

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### 第三步：驗證

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
```

---

## 3. 實作重點

### 3.1 分幀

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

PHP 用 `pack`/`unpack`：

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

資料幀酬載 = `pack('N', $streamId) . 原始位元組`。

### 3.2 握手與心跳

核心**先發 Hello**，你回 `HelloAck`；核心驗證 `name` 與 `api_version`（目前 `v1`）。
之後每 2s 一個 `Ping`，回 `Pong`。

### 3.3 並行模型：可重入的幀泵（PHP 沒有執行緒）

PHP CLI 是單執行緒阻塞式的，因此這裡不用「每條流一個執行緒」，而是：

- **讀迴圈**（`serve()`）負責握手、心跳、開流、回顯資料、處理正向呼叫；
- **回顯**不需要額外狀態機：收到 `kindData` 就立刻原樣寫回 `kindData`；
- **反向呼叫**用 `callAndWait()`：發送 `kindCall` 後，一邊讀幀一邊分發，
  直到讀到**自己的**回應（`reverse=true` 且 `id` 匹配）才返回。

```php
public function callAndWait(string $method, $params = null)
{
    $id = ++$this->nextId;
    $this->sendJson(KIND_CALL, ['id' => $id, 'method' => $method, 'reverse' => true, 'params' => $params]);
    for (;;) {
        [$kind, $payload] = $this->readFrame();
        if ($kind === KIND_REPLY) {
            $reply = json_decode($payload, true);
            if (!empty($reply['reverse']) && $reply['id'] === $id) {
                if (empty($reply['ok'])) throw new RuntimeException($reply['error'] ?? '调用失败');
                return $reply['data'] ?? null;
            }
            continue;
        }
        $this->dispatchOther($kind, $payload);   // 心跳/数据/正向调用照常处理
    }
}
```

這意味著 `dispatchOther()` **必須可重入**：它可能在一次 `callAndWait` 內部被再次呼叫
（例如處理 `session.deliver` 時又要 `session.settle`）。範例即是這麼做的。

### 3.4 語意橋（必須先認證）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` 不能省：連線的核心操作面在認證前沒有身分，直接 `session.open` 會被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

```php
$ident = $this->callAndWait('core.authenticate', [
    'stream'    => $streamId,
    'mechanism' => 'PLAIN',
    // SASL PLAIN 响应：\x00<user>\x00<password>，字节在 JSON 里走 base64
    'response'  => base64_encode("\x00{$user}\x00{$password}"),
]);
$this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
$q = $this->callAndWait('session.declare_queue', ['stream' => $streamId, 'exclusive' => true, 'auto_delete' => true]);
$this->callAndWait('session.consume', ['stream' => $streamId, 'queue' => $q['name'], 'prefetch' => 32]);
```

投遞由核心**正向回推**（`method = "session.deliver"`），處理後 `session.settle`
（`ack` / `requeue` / `reject`；投遞編號全域唯一，不帶流號）。

---

## 4. 程式碼走讀（範例專案）

`swiftmq-plugin/php/sidecar_plugin.php` 約 320 行：

| 位置 | 作用 |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | 分幀讀寫 |
| `Conn::serve()` | 主讀迴圈 |
| `Conn::dispatchOther()` | 分發非握手幀（可重入） |
| `Conn::callAndWait()` | 反向呼叫（可重入幀泵） |
| `Conn::handleHello()` | 驗證並回 HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | 正向呼叫（`session.deliver` + settle、`stats`） |
| `Conn::sessionDemo()` | 認證 + 宣告 + 發佈 + 消費 |

---

## 5. 實測（本機重現）

Windows + PHP 7.4；核心在 Docker（`swiftmq:1.1.01`），外掛在宿主機（`tcp://host.docker.internal:19021`）。

```
php -l sidecar_plugin.php  → No syntax errors detected
plugin=php-sidecar state=enabled          # /api/plugins
echo=[PHhello]                            # 客户端连内核端口 19022 发 "PHhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=php-sidecar addr=0.0.0.0:19021 version=0.1.0
内核已接入 plugin=php-sidecar peer=127.0.0.1:51006
握手完成 plugin=php-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-955afebb58d6b5307ba36e
流已打开 plugin=php-sidecar stream=1 remote=172.17.0.1:59664 local=172.17.0.2:19022
收到投递（session.deliver） queue=amq.gen-955afebb58d6b5307ba36e delivery_id=1 body=hello from php sidecar
```

涵蓋：**握手 → 認證 → 語意橋 → 投遞回推 → 結算 → 位元組流回顯**。

---

## 6. PHP 特有的注意事項

- **PHP 7.4 沒有 `mixed` 返回型別**（PHP 8.0 才有）：範例裡反向呼叫回傳「任意型別」，
  因此**不寫返回型別宣告**（用 `@return mixed` 註解）。在 7.4 上寫 `: mixed` 會直接語法錯誤。
- **JSON 的數字型別**：`json_decode($s, true)` 預設把整數解成 `int`，大整數可能變 `float`；
  投遞編號在本範例規模內沒問題，若你的編號很大，考慮 `JSON_BIGINT_AS_STRING`。
- **base64 是必須的**：`message.body` 與 `core.authenticate.response` 在 JSON 裡是 base64 字串
  （`base64_encode` / `base64_decode($s, true)`）。
- **不要用 `pcntl_fork` 做並行**：Windows 上沒有 pcntl，而且 fork 後會破壞「單連線單寫者」的假設；
  單執行緒 + 可重入幀泵已經夠用（除非你要在流上做很重的計算，那更適合放到外部服務）。
- **`stream_socket_accept` 是阻塞的**：行程生命週期由核心（`spawn`）或 supervisor 管理；
  記得處理 `fread` 回傳 `''`（EOF）→ 結束該連線並回到 accept。
- **輸出緩衝**：日誌用 `fwrite(STDOUT, …)` 並跟一個換行，便於被核心依列轉發到核心日誌。

---

## 7. 進階

- 外掛自帶管理介面：設定裡加 `console_url`（主文件 §5.8），管理後台「外掛管理」頁會出現直達入口。
- 獨立部署：`spawn: []` + `address: "tcp://<服務名稱>:19021"`，容器內監聽 `0.0.0.0`。
- 需要更高並行時，可把外掛改成常駐的 Swoole / RoadRunner 之類，但**線路協定不變**，只需保證：
  寫幀串行、讀迴圈不阻塞、反向呼叫依 `id`+`reverse` 匹配。
