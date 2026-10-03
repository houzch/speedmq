# SwiftMQ 外部进程插件开发指南 —— Java

> 🌐 本文档提供多语言版本：[文档多语言索引](../i18n/README.md)

> **面向**：用 Java 给 SwiftMQ 写外部进程插件（sidecar）的开发者。
> **先读**：[外部进程插件（sidecar）开发指南](plugin-development.md)（心智模型 / 配置字段 / 线协议总表）。
> **示例工程**：工作区 `swiftmq-plugin/java/SidecarPlugin.java`（单文件、仅 JDK 标准库，无需 Maven/Gradle）。

---

## 1. 它跑起来是什么样

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三条要点：**你的进程是服务端**（等内核来连）；**对外端口由内核开**（`protocols[].listeners`）；
**`prefix` 必须非空**（空前缀 = 不参与嗅探，连接不会被交给你；实测会被立刻断开，≤8 字节 ASCII）。

---

## 2. 三步跑起来

### 第一步：配置

`swiftmqd.json`（**实际配置是标准 JSON，不能带注释**）：

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

### 第二步：编译并启动

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### 第三步：验证

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. 实现要点

### 3.1 分帧

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Java 用 `DataInputStream`/`DataOutputStream` 最省事——它们的 `readInt`/`writeInt` 就是**大端**：

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

写侧必须**串行**（心跳、应答、数据块来自不同线程）：

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

数据帧载荷 = `4 字节大端流号 + 原始字节`。

### 3.2 握手与心跳

内核**先发 Hello**，你回 `HelloAck`；内核校验 `name` 与 `api_version`（当前 `v1`）后接入。
之后每 2s 一个 `Ping`，回 `Pong`。

### 3.3 并发模型（Java 版）

| 角色 | 线程 |
| --- | --- |
| 帧读循环 | 每条内核连接一个 |
| 流处理 | 每条流一个（多条客户端连接可并发） |
| 正向调用处理 | 每个调用一个 |

**读循环里不能同步等待反向调用应答**（会死锁）：`session.deliver` 的处理要丢到独立线程，
因为它内部还要 `session.settle`（又是一次反向调用）。示例如是。

### 3.4 语义桥（必须先认证）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` 不能省：连接的内核操作面在认证前没有身份，直接 `session.open` 会被拒
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

投递由内核**正向回推**（`method = "session.deliver"`），处理后 `session.settle`
（`ack` / `requeue` / `reject`；投递编号全局唯一，不带流号）。

---

## 4. 代码走读（示例工程）

`swiftmq-plugin/java/SidecarPlugin.java` 约 470 行（含极简 JSON）：

| 位置 | 作用 |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | 分帧读写（`DataInputStream` + 写锁） |
| `Conn.serve()` | 帧读循环与分发 |
| `Conn.call()` | 反向调用（`pending` 表 + 阻塞队列，超时保护） |
| `Conn.handleHello()` | 校验并回 HelloAck |
| `StreamState` | 流读侧（`BlockingQueue`，`STREAM_END` 表示结束） |
| `Conn.handleForwardCall()` / `handleMethod()` | 正向调用（`session.deliver` + settle、`stats`） |
| `Conn.sessionDemo()` | 认证 + 声明 + 发布 + 消费 |
| `Json`（文件末尾） | 极简 JSON 读写，仅为让示例零依赖 |

> **生产建议**：把 `Json` 换成你惯用的库（Jackson / Gson），或用 `java.net.http` 之外的既有栈——
> 与本示例要讲的东西（线协议）无关。

---

## 5. 实测（本机复现）

Windows + JDK 25；内核在 Docker（`swiftmq:1.1.01`），插件在宿主机（`tcp://host.docker.internal:19031`）。

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

覆盖：**握手 → 认证 → 语义桥 → 投递回推 → 结算 → 字节流回显**。

---

## 6. Java 特有的注意点

- **源码里的 `\uXXXX` 会被编译器在任何位置处理**（包括注释！）。示例注释里刻意写成
  "NUL + 用户名 + NUL + 口令"而不是直接写 `\u0000`，否则 javac 会报非法字符。
- **中文源码必须 `javac -encoding UTF-8`**，否则在 Windows 默认（GBK）下会报"编码 GBK 的不可映射字符"。
  运行时若要正确打印中文，加 `-Dfile.encoding=UTF-8`。
- **lambda 捕获的局部变量必须 effectively final**：示例里 `name` 在参数解析中被重新赋值，
  因此 lambda 内用的是 `opts.name`（只赋值一次的字段）。
- **`DataInputStream` 是阻塞的**：连接断开时抛 `EOFException`/`IOException`，据此收尾。
- **base64**：`message.body`、`core.authenticate.response` 在 JSON 里是 base64 字符串
  （`Base64.getEncoder()/getDecoder()`）。
- **JDK 标准库没有 JSON**：示例自带极简实现；`Json.parse` 把整数解成 `Long`、浮点解成 `Double`，
  取 `id` 时用 `((Number) m.get("id")).longValue()`。

---

## 7. 进阶

- 打包成可执行 jar（`Main-Class: SidecarPlugin`）或 `jlink` 精简运行时，
  再把 `spawn` 改成 `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`。
- 插件自带管理界面：配置里加 `console_url`（主文档 §5.8），管理后台「插件管理」页会出现直达入口。
- 独立部署：`spawn: []` + `address: "tcp://<服务名>:19031"`，容器内监听 `0.0.0.0`。
