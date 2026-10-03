# SwiftMQ 外部行程外掛開發指南 —— Python

> **適用對象**：用 Python 為 SwiftMQ 撰寫外部行程外掛（sidecar）的開發者。
> **先讀**：[外部行程外掛（sidecar）開發指南](plugin-development.md) —— 那裡講了心智模型、設定欄位與線路協定總表；
> 本文只講 **Python 怎麼落地**，以及本機實測過的步驟與結果。
> **範例專案**：工作區 `swiftmq-plugin/python/sidecar_plugin.py`（僅標準函式庫，零第三方相依）。

---

## 1. 它跑起來是什麼樣

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三條要點（容易搞錯，先記牢）：

1. **你的行程是伺服端**：監聽一個本機位址，等核心來連（`plugins.<名稱>.sidecar.address`）。
2. **對外業務連接埠由核心開啟**：客戶端連的是核心的連接埠，位元組被代理給你（`protocols[].listeners`）。
3. **`prefix` 必須非空**：核心依前綴嗅探決定「這條連線交給誰」。`prefix` 為空表示**不參與嗅探**，
   連線在它自己的監聽上也不會被交給你（實測：連線會被立刻中斷）。前綴長度 ≤ 8 位元組，ASCII。

---

## 2. 三步跑起來

### 第一步：在設定裡宣告外掛

`swiftmqd.json`（**實際設定是標準 JSON，不能帶註解**）：

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` 是**核心去連你的位址**（核心是客戶端，外掛是伺服端）。
- `spawn` 留空 = 核心只連不拉，行程由你自己管（systemd / supervisor / compose）。
- `prefix` **必須非空**：客戶端發來的首位元組須以此開頭（核心依前綴嗅探決定連線交給誰）。
- `listeners` 是對外連接埠，由核心開啟（客戶端連的是核心，不是你）。
- 跨容器部署時 `address` 用**服務名稱**（如 `tcp://py-sidecar:19001`），且外掛要監聽 `0.0.0.0`。

### 第二步：啟動

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### 第三步：驗證

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. 實作重點

### 3.1 分幀（唯一必須自己寫對的位元組層）

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

資料幀的酬載 = `4 位元組大端流號 + 原始位元組`；控制面酬載是 JSON。

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 握手與心跳

核心連上後**先發 Hello**，你必須回一幀 `HelloAck`；核心會驗證
`name == 設定裡的外掛名稱` 且 `api_version == 核心的 APIVersion`（目前 `v1`）。
之後核心每 2s 發一次 `Ping`，你回 `Pong` 即可（不回會被判死）。

### 3.3 邏輯流

`kindOpen` 到達 → **先回 `OpenAck`**，再開始服務；`kindData` 到達 → 原樣（或依你的協定解析後）寫回
`kindData`；處理結束 → 發 `kindClose`。一條流 = 一條客戶端連線。

### 3.4 反向呼叫與核心語意橋

外掛 → 核心的呼叫走 `kindCall` 且 `"reverse": true`，核心在同一條連線上回 `kindReply`。
**順序很重要**：

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **不能省**：連線的核心操作面在認證前沒有身分，直接 `session.open` 會被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。參數就是你協定裡解析出的 SASL 回應：

```python
plain = b"\x00" + user.encode() + b"\x00" + password.encode()
ident = call("core.authenticate", {
    "stream": stream_id, "mechanism": "PLAIN",
    "response": base64.b64encode(plain).decode(),   # 字节在 JSON 里是 base64
})
# 拿到的会话与进程内协议插件完全同一套语义
call("session.open", {"stream": stream_id, "vhost": "/"})
q = call("session.declare_queue", {"stream": stream_id, "exclusive": True, "auto_delete": True})
call("session.publish", {"stream": stream_id, "routing_key": q["name"],
                         "message": {"body": base64.b64encode(b"hi").decode()}})
call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
```

消費的投遞由核心**正向回推**（`kindCall`，`method = "session.deliver"`），處理完用
`session.settle` 結算（`ack` / `requeue` / `reject`，投遞編號全域唯一，不需要流號）。

### 3.5 並行模型（Python 版）

| 角色 | 執行緒 |
| --- | --- |
| 幀讀迴圈 | 每條核心連線一個 |
| 流處理 | 每條流一個（因此多條客戶端連線可並行） |
| 正向呼叫處理 | 每個呼叫一個 |

**必須注意**：讀迴圈裡**不能**同步等待反向呼叫回應（會死鎖）——正向呼叫（`session.deliver`）
要丟到獨立執行緒處理，因為它在處理中可能又要發起 `session.settle`。寫幀必須加鎖串行化。

---

## 4. 程式碼走讀（範例專案）

`swiftmq-plugin/python/sidecar_plugin.py` 約 320 行，重點函式：

| 位置 | 作用 |
| --- | --- |
| `read_frame` / `Conn.send` | 分幀讀寫（長度前綴 + kind） |
| `Conn.call` | 反向呼叫：編號 → 發送 → 等回應（依 `reverse=true` 與 `id` 匹配） |
| `Conn.serve` | 幀讀迴圈與分發 |
| `Conn._handle_hello` | 驗證外掛名稱/API 版本並回 HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | 流生命週期與回顯 |
| `Conn._dispatch` | 處理正向呼叫：`session.deliver`（含 settle）、`stats` |
| `Conn._session_demo` | 認證 + 宣告佇列 + 發佈 + 消費 |

---

## 5. 實測（本機重現）

環境：Windows + Python 3.12；核心跑在 Docker（`swiftmq:1.1.01`），外掛跑在宿主機，
核心用 `tcp://host.docker.internal:19001` 連它。

```
plugin=py-sidecar state=enabled           # /api/plugins
echo=[PYhello]                            # 客户端连内核端口 19002 发 "PYhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
内核已接入 plugin=py-sidecar peer=('127.0.0.1', 52864)
握手完成 plugin=py-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-9e5d0d695639bad12ce919
收到投递（session.deliver） queue=amq.gen-9e5d0d695639bad12ce919 delivery_id=1 body=hello from python sidecar
流已打开 plugin=py-sidecar stream=1 remote=172.17.0.1:43856 local=172.17.0.2:19002
```

涵蓋到的鏈路：**握手 → 認證 → 語意橋（宣告/發佈/消費）→ 投遞回推 → 結算 → 位元組流回顯**。

---

## 6. Python 特有的注意事項

- **不要用 `time.sleep` 等心跳**：讀迴圈是阻塞式的，靠核心的 Ping 維持連線即可；
  若給 socket 設了讀取逾時，記得把逾時當成「連線結束」處理（核心 `kill -9` 時 socket 可能不及時關閉）。
- **`json.dumps` 預設會加空格**：範例用 `separators=(",", ":")` 只是為了日誌好看，協定本身不要求。
- **位元組就是 base64**：`message.body`、`core.authenticate.response` 在 JSON 裡都是 base64 字串，
  別忘了 `base64.b64encode/decode`。
- **寫幀要加鎖**：心跳、回應、資料塊來自不同執行緒，交錯寫會汙染整條連線（範例用 `threading.Lock`）。
- 用 `asyncio` 也可以，但要保證「寫串行 + 讀迴圈不阻塞」，思路與執行緒版一致。

---

## 7. 進階

- 想給外掛配自己的管理介面：在設定裡加 `console_url`，管理後台的「外掛管理」頁會出現直達入口
  （見主文件 §5.8）。
- 外掛把自己做成獨立服務、由 systemd / K8s 管理：`spawn: []` + `restart: "never"`，由外部拉起。
- 需要多協定共存：在同一條監聽上讓多個外掛用不同 `prefix`，或各開專屬連接埠。
