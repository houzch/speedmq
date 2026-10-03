# Gabay sa Pagbuo ng External-Process Plugin ng SwiftMQ —— Java

> **Para sa**: mga developer na magsusulat ng external-process plugin (sidecar) para sa SwiftMQ gamit ang Java.
> **Basahin muna**: [Gabay sa Pagbuo ng External-Process Plugin (sidecar)](plugin-development.md) (mental model / config field / pangkalahatang talahanayan ng wire protocol).
> **Halimbawang proyekto**: workspace na `swiftmq-plugin/java/SidecarPlugin.java` (single file, JDK standard library lamang, walang Maven/Gradle).

---

## 1. Ano ang Hitsura Kapag Tumakbo Ito

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tatlong pangunahing punto: **ang iyong process ay server** (naghihintay na kumonekta ang kernel); **ang panlabas na port ay binubuksan ng kernel** (`protocols[].listeners`);
**dapat hindi laman ang `prefix`** (walang laman na prefix = hindi kasali sa sniffing, hindi ibibigay sa iyo ang koneksyon; aktwal na agad itong isasara, ≤8 bytes ASCII).

---

## 2. Tatlong Hakbang para Mapatakbo

### Unang hakbang: config

`swiftmqd.json` (**ang aktwal na config ay standard JSON, hindi maaaring maglaman ng comment**):

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

### Ikalawang hakbang: i-compile at i-start

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Ikatlong hakbang: verification

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Mga Punto ng Implementasyon

### 3.1 Framing

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Ang Java ay pinakamadali gamitin ang `DataInputStream`/`DataOutputStream` —— ang kanilang `readInt`/`writeInt` ay **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Ang write side ay **dapat serial** (ang heartbeat, reply, at data block ay mula sa magkaibang thread):

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

Ang payload ng data frame = `4 na byte na big-endian na stream number + raw bytes`.

### 3.2 Handshake at heartbeat

**Nagpapadala muna ng Hello ang kernel**, at magbalik ka ng `HelloAck`; pagkatapos i-validate ng kernel ang `name` at `api_version` (kasalukuyang `v1`) ay saka ito kumokonekta.
Pagkatapos, isang `Ping` bawat 2s, at magbalik ng `Pong`.

### 3.3 Concurrency model (bersyon ng Java)

| Tungkulin | Thread |
| --- | --- |
| Frame read loop | Isa sa bawat kernel connection |
| Stream processing | Isa sa bawat stream (maaaring concurrent ang maraming client connection) |
| Forward call processing | Isa sa bawat call |

**Hindi maaaring maghintay nang synchronously sa read loop ng reverse call reply** (magdudulot ng deadlock): ang pag-handle ng `session.deliver` ay dapat itapon sa hiwalay na thread,
dahil `session.settle` pa ang kailangan nito sa loob (isa na namang reverse call). Ito ang ginagawa ng halimbawa.

### 3.4 Semantic bridge (dapat munang mag-authenticate)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Ang `core.authenticate` ay hindi maaaring laktawan: walang identity ang kernel operation surface ng koneksyon bago ang authentication, at ang direktang `session.open` ay tatanggihan
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

Ang delivery ay **itutulak pabalik nang forward** ng kernel (`method = "session.deliver"`), at pagkatapos iproseso ay `session.settle`
(`ack` / `requeue` / `reject`; globally unique ang delivery ID, walang stream number).

---

## 4. Code Walkthrough (halimbawang proyekto)

Ang `swiftmq-plugin/java/SidecarPlugin.java` ay humigit-kumulang 470 linya (may kasamang napakasimpleng JSON):

| Lokasyon | Tungkulin |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Framing read/write (`DataInputStream` + write lock) |
| `Conn.serve()` | Frame read loop at dispatch |
| `Conn.call()` | Reverse call (`pending` table + blocking queue, may timeout protection) |
| `Conn.handleHello()` | I-validate at magbalik ng HelloAck |
| `StreamState` | Stream read side (`BlockingQueue`, `STREAM_END` ang nagsasaad ng pagtatapos) |
| `Conn.handleForwardCall()` / `handleMethod()` | Forward call (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Auth + declare + publish + consume |
| `Json` (sa dulo ng file) | Napakasimpleng JSON read/write, para lamang maging zero-dependency ang halimbawa |

> **Rekomendasyon para sa production**: palitan ang `Json` ng library na kinagawian mo (Jackson / Gson), o gumamit ng umiiral nang stack maliban sa `java.net.http` ——
> walang kaugnayan ito sa nais ipaliwanag ng halimbawang ito (ang wire protocol).

---

## 5. Aktwal na Pagsusubok (na-reproduce sa makinang ito)

Windows + JDK 25; kernel sa Docker (`swiftmq:1.1.01`), plugin sa host (`tcp://host.docker.internal:19031`).

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

Nasakop: **handshake → auth → semantic bridge → pag-push pabalik ng delivery → settle → byte stream echo**.

---

## 6. Mga Puntong Dapat Tandaan na Espesipiko sa Java

- **Ang `\uXXXX` sa source ay pinoproseso ng compiler sa kahit anong posisyon** (kasama ang comment!). Ang comment sa halimbawa ay sadyang isinulat na
  "NUL + username + NUL + password" sa halip na direktang isulat ang `\u0000`, kung hindi ay mag-e-error ang javac ng illegal character.
- **Ang Chinese source ay dapat `javac -encoding UTF-8`**, kung hindi, sa Windows default (GBK) ay makakakuha ka ng "unmappable character for encoding GBK".
  Upang tama ang pag-print ng Chinese sa runtime, magdagdag ng `-Dfile.encoding=UTF-8`.
- **Ang local variable na kina-capture ng lambda ay dapat effectively final**: sa halimbawa, ang `name` ay muling ina-assign sa parameter parsing,
  kaya ang ginagamit sa loob ng lambda ay ang `opts.name` (field na isang beses lamang naa-assign).
- **Blocking ang `DataInputStream`**: kapag naputol ang koneksyon ay nagre-throw ito ng `EOFException`/`IOException`, at dito ibinabase ang pagtatapos.
- **base64**: ang `message.body` at `core.authenticate.response` ay base64 string sa JSON
  (`Base64.getEncoder()/getDecoder()`).
- **Walang JSON sa JDK standard library**: may sariling napakasimpleng implementasyon ang halimbawa; ang `Json.parse` ay nagde-decode ng integer bilang `Long` at ng floating point bilang `Double`,
  at kapag kinukuha ang `id` ay ginagamit ang `((Number) m.get("id")).longValue()`.

---

## 7. Advanced

- I-package bilang executable jar (`Main-Class: SidecarPlugin`) o `jlink` para sa pinasimpleng runtime,
  pagkatapos ay palitan ang `spawn` ng `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`.
- Sariling admin UI ng plugin: magdagdag ng `console_url` sa config (pangunahing dokumento §5.8), at lalabas ang direktang entry sa "Plugin Management" page ng admin console.
- Standalone deployment: `spawn: []` + `address: "tcp://<pangalan ng serbisyo>:19031"`, nakikinig sa `0.0.0.0` sa loob ng container.
