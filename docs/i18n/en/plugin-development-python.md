# SwiftMQ External-Process Plugin Development Guide — Python

> **Audience**: developers writing external-process plugins (sidecars) for SwiftMQ in Python.
> **Read first**: [External-Process Plugin (sidecar) Development Guide](plugin-development.md) — it covers the mental model, configuration fields, and the full wire-protocol table;
> this document only covers **how to put it into practice in Python**, along with the steps and results verified locally.
> **Example project**: workspace `swiftmq-plugin/python/sidecar_plugin.py` (standard library only, zero third-party dependencies).

---

## 1. What It Looks Like When Running

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Three key points (easy to get wrong, remember them first):

1. **Your process is the server**: it listens on a local address and waits for the kernel to connect (`plugins.<name>.sidecar.address`).
2. **The external business port is opened by the kernel**: clients connect to the kernel's port, and bytes are proxied to you (`protocols[].listeners`).
3. **`prefix` must be non-empty**: the kernel decides "who this connection goes to" by prefix sniffing. An empty `prefix` means it **does not participate in sniffing**,
   and the connection will not be handed to you even on its own listener (empirically: the connection is dropped immediately). The prefix length is ≤ 8 bytes, ASCII.

---

## 2. Running It in Three Steps

### Step 1: Declare the Plugin in Configuration

`swiftmqd.json` (**the actual configuration is standard JSON and cannot contain comments**):

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

- `address` is **the address the kernel connects to** (the kernel is the client, the plugin is the server).
- An empty `spawn` = the kernel only connects and does not launch, and you manage the process yourself (systemd / supervisor / compose).
- `prefix` **must be non-empty**: the first bytes sent by the client must start with it (the kernel decides who gets the connection by prefix sniffing).
- `listeners` are the external ports, opened by the kernel (clients connect to the kernel, not to you).
- For cross-container deployment, use the **service name** in `address` (e.g. `tcp://py-sidecar:19001`), and the plugin must listen on `0.0.0.0`.

### Step 2: Start It

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Step 3: Verify

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Implementation Highlights

### 3.1 Framing (the Only Byte Layer You Must Get Right Yourself)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

The payload of a data frame = `4-byte big-endian stream id + raw bytes`; the control-plane payload is JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake and Heartbeat

After the kernel connects, it **sends Hello first**, and you must reply with a `HelloAck` frame; the kernel validates
`name == the plugin name in config` and `api_version == the kernel's APIVersion` (currently `v1`).
After that the kernel sends a `Ping` every 2s, and you just reply `Pong` (not replying will be considered dead).

### 3.3 Logical Streams

`kindOpen` arrives → **reply `OpenAck` first**, then start serving; `kindData` arrives → write it back to
`kindData` as-is (or after parsing per your protocol); when handling ends → send `kindClose`. One stream = one client connection.

### 3.4 Reverse Calls and the Kernel Semantic Bridge

Calls from the plugin → kernel go through `kindCall` with `"reverse": true`, and the kernel replies with `kindReply` on the same connection.
**Order matters**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **cannot be skipped**: the connection's kernel operation surface has no identity before authentication, and a direct `session.open` is rejected
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). The params are the SASL response parsed from your protocol:

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

Consumer deliveries are **pushed forward** by the kernel (`kindCall`, `method = "session.deliver"`); when done processing, settle them with
`session.settle` (`ack` / `requeue` / `reject`; the delivery number is globally unique and needs no stream number).

### 3.5 Concurrency Model (Python Version)

| Role | Thread |
| --- | --- |
| Frame read loop | One per kernel connection |
| Stream handling | One per stream (so multiple client connections can run concurrently) |
| Forward-call handling | One per call |

**Must note**: the read loop **must not** synchronously wait for a reverse-call reply (that would deadlock) — forward calls (`session.deliver`)
must be dispatched to a separate thread, because during handling they may in turn initiate `session.settle`. Frame writes must be lock-serialized.

---

## 4. Code Walkthrough (Example Project)

`swiftmq-plugin/python/sidecar_plugin.py` is about 320 lines; key functions:

| Location | Purpose |
| --- | --- |
| `read_frame` / `Conn.send` | Frame read/write (length prefix + kind) |
| `Conn.call` | Reverse call: number → send → wait for reply (matched by `reverse=true` and `id`) |
| `Conn.serve` | Frame read loop and dispatch |
| `Conn._handle_hello` | Validate plugin name/API version and reply HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Stream lifecycle and echo |
| `Conn._dispatch` | Handle forward calls: `session.deliver` (including settle), `stats` |
| `Conn._session_demo` | Auth + declare queue + publish + consume |

---

## 5. Empirical Test (Reproduced Locally)

Environment: Windows + Python 3.12; the kernel runs in Docker (`swiftmq:1.1.01`), the plugin runs on the host,
and the kernel connects to it via `tcp://host.docker.internal:19001`.

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

Paths covered: **handshake → auth → semantic bridge (declare/publish/consume) → delivery pushback → settle → byte-stream echo**.

---

## 6. Python-Specific Notes

- **Do not use `time.sleep` to wait for heartbeats**: the read loop is blocking, so just rely on the kernel's Ping to keep the connection alive;
  if you set a read timeout on the socket, remember to treat the timeout as "connection ended" (when the kernel is `kill -9`ed, the socket may not close promptly).
- **`json.dumps` adds spaces by default**: the example uses `separators=(",", ":")` only for nicer logs; the protocol itself does not require it.
- **Bytes are base64**: `message.body` and `core.authenticate.response` are both base64 strings in JSON,
  so don't forget `base64.b64encode/decode`.
- **Frame writes must be locked**: heartbeats, replies, and data chunks come from different threads, and interleaved writes corrupt the whole connection (the example uses `threading.Lock`).
- `asyncio` also works, but you must ensure "serialized writes + non-blocking read loop"; the approach is the same as the threaded version.

---

## 7. Advanced Topics

- To give the plugin its own admin UI: add `console_url` in the configuration, and a direct entry will appear on the admin console's "Plugin Management" page
  (see the main document §5.8).
- To make the plugin a standalone service managed by systemd / K8s: `spawn: []` + `restart: "never"`, launched externally.
- To have multiple protocols coexist: let multiple plugins use different `prefix` values on the same listener, or open a dedicated port for each.
