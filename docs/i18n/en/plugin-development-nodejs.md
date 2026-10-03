# SwiftMQ External-Process Plugin Development Guide — Node.js

> **Audience**: developers writing external-process plugins (sidecars) for SwiftMQ in Node.js.
> **Read first**: [External-Process Plugin (sidecar) Development Guide](plugin-development.md) (mental model / configuration fields / full wire-protocol table).
> **Example project**: workspace `swiftmq-plugin/nodejs/index.js` (Node standard library only, **no npm dependencies**).

---

## 1. What It Looks Like When Running

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Three key points: **your process is the server** (it waits for the kernel to connect); **the external port is opened by the kernel** (config `protocols[].listeners`);
**`prefix` must be non-empty** (an empty prefix = does not participate in sniffing, and the connection will not be handed to you; empirically it is dropped immediately, ≤8 bytes ASCII).

---

## 2. Running It in Three Steps

### Step 1: Configuration

`swiftmqd.json` (**the actual configuration is standard JSON and cannot contain comments**):

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

### Step 2: Start It

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Step 3: Verify

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

In Node, use `Buffer`: accumulate the bytes received and cut out a frame as soon as there are enough.

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

One stream = one client connection; a data frame payload is `4-byte big-endian stream id + raw bytes`.

### 3.2 Handshake and Heartbeat

The kernel **sends Hello first**, and you reply `HelloAck`; the kernel validates `name` and `api_version` (currently `v1`) and then connects.
After that there is a `Ping` every 2s, and you just reply `Pong` (handled in passing by the read loop; no timer needed).

### 3.3 Async Model (Node Version)

A single-threaded event loop naturally avoids the "interleaved write" problem — but be careful **not to let the read loop await a reverse call**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` is `async`: it may in turn `await call('session.settle', …)`, so it must never be written as a synchronous wait.

### 3.4 Semantic Bridge (Must Authenticate First)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` cannot be skipped: the connection's kernel operation surface has no identity before authentication, and a direct `session.open` is rejected
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

Deliveries are **pushed forward** by the kernel (`method = "session.deliver"`); after processing, `session.settle`
(`ack` / `requeue` / `reject`; the delivery number is globally unique and carries no stream number).

---

## 4. Code Walkthrough (Example Project)

`swiftmq-plugin/nodejs/index.js` is about 330 lines:

| Location | Purpose |
| --- | --- |
| `u32()` / `Conn.send()` | Frame read/write |
| `Conn.drain()` / `dispatch()` | Per-frame parsing and dispatch |
| `Conn.call()` | Reverse call (`Promise` + a `pending` table, matched by `reverse=true` and `id`) |
| `Stream` | Stream read side: `push/end/read` form an async queue |
| `handleHello` | Validate and reply HelloAck |
| `handleForwardCall` / `handleMethod` | Forward calls (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Auth + declare + publish + consume |

---

## 5. Empirical Test (Reproduced Locally)

Windows + Node v24; the kernel runs in Docker (`swiftmq:1.1.01`), the plugin runs on the host (`tcp://host.docker.internal:19011`).

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

Covered: **handshake → auth → semantic bridge → delivery pushback → settle → byte-stream echo**.

---

## 6. Node.js-Specific Notes

- **`socket.write` writes one frame at a time**: the example assembles the whole frame into a single `Buffer` before writing, so no extra locking is needed;
  if you split a frame into multiple `write`s, you must guarantee the order yourself.
- **The chunk boundaries of `stream.on('data')` have nothing to do with frames**: you must accumulate the buffer yourself (see `drain()`).
- **base64**: `message.body` and `core.authenticate.response` are base64 strings in JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Do not `await` inside `drain()`**: it is a synchronous frame-splitting function; hand off async processing to `handleForwardCall`.
- **ESM vs CJS**: the example uses CommonJS (`require`) so that `node index.js` runs directly; switching to ESM just means using `import`.

---

## 7. Advanced Topics

- Plugin with its own admin UI: add `console_url` in the configuration (main document §5.8), and a direct entry will appear on the admin console's "Plugin Management" page.
- Standalone deployment (K8s / systemd): `spawn: []` + `address: "tcp://<service-name>:19011"`, listening on `0.0.0.0` inside the container.
- Port reuse: give each of multiple protocols a different `prefix`, and the kernel dispatches connections to their respective plugins by prefix.
