# SpeedMQ External-Process Plugin Development Guide — Java

> **Audience**: developers writing external-process plugins (sidecars) for SpeedMQ in Java.
> **Read first**: [External-Process Plugin (sidecar) Development Guide](plugin-development.md) (mental model / configuration fields / full wire-protocol table).
> **Example project**: workspace `speedmq-plugin/java/SidecarPlugin.java` (single file, JDK standard library only, no Maven/Gradle).

---

## 1. What It Looks Like When Running

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Three key points: **your process is the server** (it waits for the kernel to connect); **the external port is opened by the kernel** (`protocols[].listeners`);
**`prefix` must be non-empty** (an empty prefix = does not participate in sniffing, and the connection will not be handed to you; empirically it is dropped immediately, ≤8 bytes ASCII).

---

## 2. Running It in Three Steps

### Step 1: Configuration

`speedmqd.json` (**the actual configuration is standard JSON and cannot contain comments**):

```json
{
  "plugins": {
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/speedmq/classes", "SidecarPlugin",
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

### Step 2: Compile and Start

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Step 3: Verify

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Implementation Highlights

### 3.1 Framing

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Java is easiest with `DataInputStream`/`DataOutputStream` — their `readInt`/`writeInt` are already **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

The write side must be **serialized** (heartbeats, replies, and data chunks come from different threads):

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

Data frame payload = `4-byte big-endian stream id + raw bytes`.

### 3.2 Handshake and Heartbeat

The kernel **sends Hello first**, and you reply `HelloAck`; the kernel validates `name` and `api_version` (currently `v1`) and then connects.
After that there is a `Ping` every 2s, and you reply `Pong`.

### 3.3 Concurrency Model (Java Version)

| Role | Thread |
| --- | --- |
| Frame read loop | One per kernel connection |
| Stream handling | One per stream (multiple client connections can run concurrently) |
| Forward-call handling | One per call |

**The read loop must not synchronously wait for a reverse-call reply** (that would deadlock): handling of `session.deliver` must be dispatched to a separate thread,
because it internally needs `session.settle` (another reverse call). That is what the example does.

### 3.4 Semantic Bridge (Must Authenticate First)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` cannot be skipped: the connection's kernel operation surface has no identity before authentication, and a direct `session.open` is rejected
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

Deliveries are **pushed forward** by the kernel (`method = "session.deliver"`); after processing, `session.settle`
(`ack` / `requeue` / `reject`; the delivery number is globally unique and carries no stream number).

---

## 4. Code Walkthrough (Example Project)

`speedmq-plugin/java/SidecarPlugin.java` is about 470 lines (including a minimal JSON implementation):

| Location | Purpose |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Frame read/write (`DataInputStream` + write lock) |
| `Conn.serve()` | Frame read loop and dispatch |
| `Conn.call()` | Reverse call (`pending` table + blocking queue, with timeout protection) |
| `Conn.handleHello()` | Validate and reply HelloAck |
| `StreamState` | Stream read side (`BlockingQueue`, `STREAM_END` marks the end) |
| `Conn.handleForwardCall()` / `handleMethod()` | Forward calls (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Auth + declare + publish + consume |
| `Json` (end of file) | Minimal JSON read/write, only to keep the example dependency-free |

> **Production advice**: replace `Json` with a library you are used to (Jackson / Gson), or use an existing stack other than `java.net.http` —
> this is unrelated to what this example is about (the wire protocol).

---

## 5. Empirical Test (Reproduced Locally)

Windows + JDK 25; the kernel runs in Docker (`speedmq:1.1.01`), the plugin runs on the host (`tcp://host.docker.internal:19031`).

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

Covered: **handshake → auth → semantic bridge → delivery pushback → settle → byte-stream echo**.

---

## 6. Java-Specific Notes

- **`\uXXXX` in source is processed by the compiler anywhere** (including comments!). The example comment deliberately writes
  "NUL + user + NUL + password" rather than `\u0000` directly, otherwise javac reports an illegal character.
- **Chinese source must use `javac -encoding UTF-8`**; otherwise on Windows' default (GBK) you get "unmappable character for encoding GBK".
  To print Chinese correctly at runtime, add `-Dfile.encoding=UTF-8`.
- **Local variables captured by a lambda must be effectively final**: in the example, `name` is reassigned during argument parsing,
  so the lambda uses `opts.name` (a field assigned only once).
- **`DataInputStream` blocks**: when the connection drops it throws `EOFException`/`IOException`, which you use to wrap up.
- **base64**: `message.body` and `core.authenticate.response` are base64 strings in JSON
  (`Base64.getEncoder()/getDecoder()`).
- **The JDK standard library has no JSON**: the example ships a minimal implementation; `Json.parse` decodes integers as `Long` and floats as `Double`,
  and when reading `id` it uses `((Number) m.get("id")).longValue()`.

---

## 7. Advanced Topics

- Package it into an executable jar (`Main-Class: SidecarPlugin`) or a trimmed runtime via `jlink`,
  then change `spawn` to `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`.
- Plugin with its own admin UI: add `console_url` in the configuration (main document §5.8), and a direct entry will appear on the admin console's "Plugin Management" page.
- Standalone deployment: `spawn: []` + `address: "tcp://<service-name>:19031"`, listening on `0.0.0.0` inside the container.
