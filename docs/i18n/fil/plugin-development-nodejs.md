# Gabay sa Pagbuo ng External-Process Plugin ng SwiftMQ —— Node.js

> **Para sa**: mga developer na magsusulat ng external-process plugin (sidecar) para sa SwiftMQ gamit ang Node.js.
> **Basahin muna**: [Gabay sa Pagbuo ng External-Process Plugin (sidecar)](plugin-development.md) (mental model / config field / pangkalahatang talahanayan ng wire protocol).
> **Halimbawang proyekto**: workspace na `swiftmq-plugin/nodejs/index.js` (Node standard library lamang, **walang kailangang npm dependency**).

---

## 1. Ano ang Hitsura Kapag Tumakbo Ito

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tatlong pangunahing punto: **ang iyong process ay server** (naghihintay na kumonekta ang kernel); **ang panlabas na port ay binubuksan ng kernel** (config na `protocols[].listeners`);
**dapat hindi laman ang `prefix`** (walang laman na prefix = hindi kasali sa sniffing, hindi ibibigay sa iyo ang koneksyon; aktwal na agad itong isasara, ≤8 bytes ASCII).

---

## 2. Tatlong Hakbang para Mapatakbo

### Unang hakbang: config

`swiftmqd.json` (**ang aktwal na config ay standard JSON, hindi maaaring maglaman ng comment**):

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

### Ikalawang hakbang: startup

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Ikatlong hakbang: verification

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

Gumamit ng `Buffer` sa Node: ipunin ang natanggap na byte, at kapag sapat na para sa isang frame ay hiwain ito at iproseso.

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

Isang stream = isang client connection; ang payload ng data frame ay `4 na byte na big-endian na stream number + raw bytes`.

### 3.2 Handshake at heartbeat

**Nagpapadala muna ng Hello ang kernel**, at magbalik ka ng `HelloAck`; pagkatapos i-validate ng kernel ang `name` at `api_version` (kasalukuyang `v1`) ay saka ito kumokonekta.
Pagkatapos, isang `Ping` bawat 2s, at magbalik lamang ng `Pong` (hinahawakan ito ng read loop, hindi kailangan ang timer).

### 3.3 Async model (bersyon ng Node)

Single-thread event loop, natural na naiiwasan ang problemang "interlaced write" —— ngunit tandaan na **huwag hayaang mag-await ang read loop ng reverse call**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

Ang `handleForwardCall` ay `async`: maaaring muli nitong `await call('session.settle', …)`, kaya hindi ito maaaring isulat bilang synchronous na paghihintay.

### 3.4 Semantic bridge (dapat munang mag-authenticate)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Ang `core.authenticate` ay hindi maaaring laktawan: walang identity ang kernel operation surface ng koneksyon bago ang authentication, at ang direktang `session.open` ay tatanggihan
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

Ang delivery ay **itutulak pabalik nang forward** ng kernel (`method = "session.deliver"`), at pagkatapos iproseso ay `session.settle`
(`ack` / `requeue` / `reject`; globally unique ang delivery ID, walang stream number).

---

## 4. Code Walkthrough (halimbawang proyekto)

Ang `swiftmq-plugin/nodejs/index.js` ay humigit-kumulang 330 linya:

| Lokasyon | Tungkulin |
| --- | --- |
| `u32()` / `Conn.send()` | Framing read/write |
| `Conn.drain()` / `dispatch()` | Parse at dispatch ayon sa frame |
| `Conn.call()` | Reverse call (`Promise` + `pending` table, tumutugma ayon sa `reverse=true` at `id`) |
| `Stream` | Stream read side: binubuo ng `push/end/read` ang isang async queue |
| `handleHello` | I-validate at magbalik ng HelloAck |
| `handleForwardCall` / `handleMethod` | Forward call (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Auth + declare + publish + consume |

---

## 5. Aktwal na Pagsusubok (na-reproduce sa makinang ito)

Windows + Node v24; kernel sa Docker (`swiftmq:1.1.01`), plugin sa host (`tcp://host.docker.internal:19011`).

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

Nasakop: **handshake → auth → semantic bridge → pag-push pabalik ng delivery → settle → byte stream echo**.

---

## 6. Mga Puntong Dapat Tandaan na Espesipiko sa Node.js

- **Isang frame bawat `socket.write`**: pinagdurugtong ng halimbawa ang buong frame sa isang `Buffer` bago isulat, kaya hindi na kailangan ng dagdag na lock;
  kung hinati mo ang isang frame sa maraming `write`, ikaw mismo ang dapat maggarantiya ng pagkakasunod-sunod.
- **Walang kaugnayan sa frame ang chunk boundary ng `stream.on('data')`**: dapat mong ipunin mismo ang buffer (tingnan ang `drain()`).
- **base64**: ang `message.body` at `core.authenticate.response` ay base64 string sa JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Huwag mag-`await` sa loob ng `drain()`**: ito ay synchronous na frame splitting function; ipaubaya ang async processing sa `handleForwardCall`.
- **ESM vs CJS**: gumagamit ang halimbawa ng CommonJS (`require`) para direktang mapatakbo ang `node index.js`; upang gawing ESM ay palitan lamang ang `import`.

---

## 7. Advanced

- Sariling admin UI ng plugin: magdagdag ng `console_url` sa config (pangunahing dokumento §5.8), at lalabas ang direktang entry sa "Plugin Management" page ng admin console.
- Standalone deployment (K8s / systemd): `spawn: []` + `address: "tcp://<pangalan ng serbisyo>:19011"`, nakikinig sa `0.0.0.0` sa loob ng container.
- Port reuse: bigyan ng magkaibang `prefix` ang maraming protocol, at ididispatch ng kernel ang koneksyon sa kani-kanilang plugin ayon sa prefix.
