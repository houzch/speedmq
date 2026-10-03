# SwiftMQ 外部行程外掛開發指南 —— Java

> **適用對象**：用 Java 為 SwiftMQ 撰寫外部行程外掛（sidecar）的開發者。
> **先讀**：[外部行程外掛（sidecar）開發指南](plugin-development.md)（心智模型 / 設定欄位 / 線路協定總表）。
> **範例專案**：工作區 `swiftmq-plugin/java/SidecarPlugin.java`（單一檔案、僅 JDK 標準函式庫，無需 Maven/Gradle）。

---

## 1. 它跑起來是什麼樣

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/swiftmq/classes", "SidecarPlugin",
                  "--addr", "0.0.0.0:19031", "--name", "java-sidecar"],
        "protocols": [
          { "name": "javaecho", "prefix": "JV",
            "listeners": [{ "name": "javaecho", "addr": ":19032" }] }
        ]
      }
    }
  }
}
```

### 第二步：編譯並啟動

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### 第三步：驗證

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

Java 用 `DataInputStream`/`DataOutputStream` 最省事——它們的 `readInt`/`writeInt` 就是**大端**：

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

寫側必須**串行**（心跳、回應、資料塊來自不同執行緒）：

```java
void send(int kind, byte[] payload) throws IOException {
    synchronized (writeLock) {
        out.writeInt(1 + payload.length);
        out.write(kind);
        out.write(payload);
        out.flush();
    }
}
```

資料幀酬載 = `4 位元組大端流號 + 原始位元組`。

### 3.2 握手與心跳

核心**先發 Hello**，你回 `HelloAck`；核心驗證 `name` 與 `api_version`（目前 `v1`）後接入。
之後每 2s 一個 `Ping`，回 `Pong`。

### 3.3 並行模型（Java 版）

| 角色 | 執行緒 |
| --- | --- |
| 幀讀迴圈 | 每條核心連線一個 |
| 流處理 | 每條流一個（多條客戶端連線可並行） |
| 正向呼叫處理 | 每個呼叫一個 |

**讀迴圈裡不能同步等待反向呼叫回應**（會死鎖）：`session.deliver` 的處理要丟到獨立執行緒，
因為它內部還要 `session.settle`（又是一次反向呼叫）。範例如是。

### 3.4 語意橋（必須先認證）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` 不能省：連線的核心操作面在認證前沒有身分，直接 `session.open` 會被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

```java
Map<String, Object> auth = new LinkedHashMap<>();
auth.put("stream", streamId);
auth.put("mechanism", "PLAIN");
// SASL PLAIN 响应：NUL + user + NUL + password，字节在 JSON 里走 base64
auth.put("response", Base64.getEncoder().encodeToString(plainResponse(user, password)));
Object ident = call("core.authenticate", auth, 10_000);

call("session.open", Map.of("stream", streamId, "vhost", "/"), 10_000);
Map<String, Object> q = asMap(call("session.declare_queue",
        Map.of("stream", streamId, "exclusive", true, "auto_delete", true), 10_000));
call("session.consume", Map.of("stream", streamId, "queue", q.get("name"), "prefetch", 32), 10_000);
```

投遞由核心**正向回推**（`method = "session.deliver"`），處理後 `session.settle`
（`ack` / `requeue` / `reject`；投遞編號全域唯一，不帶流號）。

---

## 4. 程式碼走讀（範例專案）

`swiftmq-plugin/java/SidecarPlugin.java` 約 470 行（含極簡 JSON）：

| 位置 | 作用 |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | 分幀讀寫（`DataInputStream` + 寫鎖） |
| `Conn.serve()` | 幀讀迴圈與分發 |
| `Conn.call()` | 反向呼叫（`pending` 表 + 阻塞佇列，逾時保護） |
| `Conn.handleHello()` | 驗證並回 HelloAck |
| `StreamState` | 流讀側（`BlockingQueue`，`STREAM_END` 表示結束） |
| `Conn.handleForwardCall()` / `handleMethod()` | 正向呼叫（`session.deliver` + settle、`stats`） |
| `Conn.sessionDemo()` | 認證 + 宣告 + 發佈 + 消費 |
| `Json`（檔案末尾） | 極簡 JSON 讀寫，僅為讓範例零相依 |

> **生產建議**：把 `Json` 換成你慣用的函式庫（Jackson / Gson），或用 `java.net.http` 之外的既有堆疊——
> 與本範例要講的東西（線路協定）無關。

---

## 5. 實測（本機重現）

Windows + JDK 25；核心在 Docker（`swiftmq:1.1.01`），外掛在宿主機（`tcp://host.docker.internal:19031`）。

```
javac -encoding UTF-8 -d classes SidecarPlugin.java   → 退出码 0
plugin=java-sidecar state=enabled          # /api/plugins
echo=[JVhello]                             # 客户端连内核端口 19032 发 "JVhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=java-sidecar addr=0.0.0.0:19031 version=0.1.0
内核已接入 plugin=java-sidecar peer=/127.0.0.1:51039
握手完成 plugin=java-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-4ce088d93638eafe830191
流已打开 plugin=java-sidecar stream=1 remote=172.17.0.1:34112 local=172.17.0.2:19032
收到投递（session.deliver） queue=amq.gen-4ce088d93638eafe830191 delivery_id=1 body=hello from java sidecar
```

涵蓋：**握手 → 認證 → 語意橋 → 投遞回推 → 結算 → 位元組流回顯**。

---

## 6. Java 特有的注意事項

- **原始碼裡的 `\uXXXX` 會被編譯器在任何位置處理**（包括註解！）。範例註解裡刻意寫成
  「NUL + 使用者名稱 + NUL + 密碼」而不是直接寫 `\u0000`，否則 javac 會報非法字元。
- **中文原始碼必須 `javac -encoding UTF-8`**，否則在 Windows 預設（GBK）下會報「編碼 GBK 的不可映射字元」。
  執行時若要正確列印中文，加 `-Dfile.encoding=UTF-8`。
- **lambda 捕獲的區域變數必須 effectively final**：範例裡 `name` 在參數解析中被重新賦值，
  因此 lambda 內用的是 `opts.name`（只賦值一次的欄位）。
- **`DataInputStream` 是阻塞的**：連線中斷時拋 `EOFException`/`IOException`，據此收尾。
- **base64**：`message.body`、`core.authenticate.response` 在 JSON 裡是 base64 字串
  （`Base64.getEncoder()/getDecoder()`）。
- **JDK 標準函式庫沒有 JSON**：範例自帶極簡實作；`Json.parse` 把整數解成 `Long`、浮點解成 `Double`，
  取 `id` 時用 `((Number) m.get("id")).longValue()`。

---

## 7. 進階

- 打包成可執行 jar（`Main-Class: SidecarPlugin`）或 `jlink` 精簡執行環境，
  再把 `spawn` 改成 `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`。
- 外掛自帶管理介面：設定裡加 `console_url`（主文件 §5.8），管理後台「外掛管理」頁會出現直達入口。
- 獨立部署：`spawn: []` + `address: "tcp://<服務名稱>:19031"`，容器內監聽 `0.0.0.0`。
