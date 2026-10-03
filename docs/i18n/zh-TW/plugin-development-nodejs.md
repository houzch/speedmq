# SwiftMQ 外部行程外掛開發指南 —— Node.js

> **適用對象**：用 Node.js 為 SwiftMQ 撰寫外部行程外掛（sidecar）的開發者。
> **先讀**：[外部行程外掛（sidecar）開發指南](plugin-development.md)（心智模型 / 設定欄位 / 線路協定總表）。
> **範例專案**：工作區 `swiftmq-plugin/nodejs/index.js`（僅 Node 標準函式庫，**無需 npm 相依**）。

---

## 1. 它跑起來是什麼樣

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三條要點：**你的行程是伺服端**（等核心來連）；**對外連接埠由核心開啟**（設定 `protocols[].listeners`）；
**`prefix` 必須非空**（空前綴 = 不參與嗅探，連線不會被交給你；實測會被立刻中斷，≤8 位元組 ASCII）。

---

## 2. 三步跑起來

### 第一步：設定

`swiftmqd.json`（**實際設定是標準 JSON，不能帶註解**）：

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/swiftmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### 第二步：啟動

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### 第三步：驗證

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

Node 裡用 `Buffer`：累積收到的位元組，夠一幀就切出來處理。

```js
drain() {
  while (this.buf.length >= 5) {
    const len = this.buf.readUInt32BE(0)
    if (this.buf.length < 4 + len) return      // 帧还没收全
    const kind = this.buf.readUInt8(4)
    const payload = this.buf.subarray(5, 4 + len)
    this.buf = this.buf.subarray(4 + len)
    this.dispatch(kind, payload)
  }
}
```

一條流 = 一條客戶端連線；資料幀酬載是 `4 位元組大端流號 + 原始位元組`。

### 3.2 握手與心跳

核心**先發 Hello**，你回 `HelloAck`；核心驗證 `name` 與 `api_version`（目前 `v1`）後接入。
之後每 2s 一個 `Ping`，回 `Pong` 即可（由讀迴圈順手處理，不需要計時器）。

### 3.3 非同步模型（Node 版）

單執行緒事件迴圈，天然避免了「寫交錯」問題——但要注意**不要讓讀迴圈 await 反向呼叫**：

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` 是 `async`：它可能再 `await call('session.settle', …)`，因此絕不能寫成同步等待。

### 3.4 語意橋（必須先認證）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` 不能省：連線的核心操作面在認證前沒有身分，直接 `session.open` 會被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

```js
const ident = await call('core.authenticate', {
  stream, mechanism: 'PLAIN',
  response: Buffer.concat([Buffer.from(`\x00${user}\x00`), Buffer.from(password)]).toString('base64'),
})
await call('session.open', { stream, vhost: '/' })
const q = await call('session.declare_queue', { stream, exclusive: true, auto_delete: true })
await call('session.publish', { stream, routing_key: q.name,
  message: { body: Buffer.from('hi').toString('base64') } })
await call('session.consume', { stream, queue: q.name, prefetch: 32 })
```

投遞由核心**正向回推**（`method = "session.deliver"`），處理後 `session.settle`
（`ack` / `requeue` / `reject`；投遞編號全域唯一，不帶流號）。

---

## 4. 程式碼走讀（範例專案）

`swiftmq-plugin/nodejs/index.js` 約 330 行：

| 位置 | 作用 |
| --- | --- |
| `u32()` / `Conn.send()` | 分幀讀寫 |
| `Conn.drain()` / `dispatch()` | 依幀解析與分發 |
| `Conn.call()` | 反向呼叫（`Promise` + `pending` 表，依 `reverse=true` 與 `id` 匹配） |
| `Stream` | 流讀側：`push/end/read` 組成一個非同步佇列 |
| `handleHello` | 驗證並回 HelloAck |
| `handleForwardCall` / `handleMethod` | 正向呼叫（`session.deliver` + settle、`stats`） |
| `sessionDemo` | 認證 + 宣告 + 發佈 + 消費 |

---

## 5. 實測（本機重現）

Windows + Node v24；核心在 Docker（`swiftmq:1.1.01`），外掛在宿主機（`tcp://host.docker.internal:19011`）。

```
plugin=node-sidecar state=enabled         # /api/plugins
echo=[NDhello]                            # 客户端连内核端口 19012 发 "NDhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=node-sidecar addr=0.0.0.0:19011 version=0.1.0
内核已接入 plugin=node-sidecar peer=127.0.0.1:50987
握手完成 plugin=node-sidecar kernel=1.1.01
认证通过 user=guest
收到投递（session.deliver） queue=amq.gen-893688b01c62b2ad5763a7 delivery_id=1 body=hello from node sidecar
session 演示完成 queue=amq.gen-893688b01c62b2ad5763a7
流已打开 plugin=node-sidecar stream=1 remote=172.17.0.1:40450 local=172.17.0.2:19012
```

涵蓋：**握手 → 認證 → 語意橋 → 投遞回推 → 結算 → 位元組流回顯**。

---

## 6. Node.js 特有的注意事項

- **`socket.write` 一次寫一幀**：範例把整幀拼成一個 `Buffer` 再寫，因此不需要額外加鎖；
  若你把一幀拆成多次 `write`，就要自己保證順序。
- **`stream.on('data')` 的 chunk 邊界與幀無關**：必須自己累積緩衝（見 `drain()`）。
- **base64**：`message.body`、`core.authenticate.response` 在 JSON 裡是 base64 字串
  （`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`）。
- **不要在 `drain()` 裡 `await`**：它是同步的幀切分函式；把非同步處理交給 `handleForwardCall`。
- **ESM vs CJS**：範例用 CommonJS（`require`）以便 `node index.js` 直接跑；改 ESM 只需換 `import`。

---

## 7. 進階

- 外掛自帶管理介面：設定裡加 `console_url`（主文件 §5.8），管理後台「外掛管理」頁會出現直達入口。
- 獨立部署（K8s / systemd）：`spawn: []` + `address: "tcp://<服務名稱>:19011"`，容器內監聽 `0.0.0.0`。
- 連接埠共用：多個協定各給不同 `prefix`，核心依前綴把連線分發到各自外掛。
