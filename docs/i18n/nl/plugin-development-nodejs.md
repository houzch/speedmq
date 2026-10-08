# Ontwikkelgids voor SpeedMQ externe-procesplugins —— Node.js

> **Doelgroep**: ontwikkelaars die met Node.js externe-procesplugins (sidecar) voor SpeedMQ schrijven.
> **Eerst lezen**: [Ontwikkelgids voor externe-procesplugins (sidecar)](plugin-development.md) (mentaal model / configuratievelden / wire-protocoltabel).
> **Voorbeeldproject**: workspace `speedmq-plugin/nodejs/index.js` (alleen de Node-standaardbibliotheek, **geen npm-afhankelijkheden**).

---

## 1. Hoe het eruitziet als het draait

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Drie kernpunten: **je proces is de server** (wacht tot de kernel verbinding maakt); **de externe poort wordt door de kernel geopend** (configuratie `protocols[].listeners`);
**`prefix` moet niet leeg zijn** (een lege prefix = niet deelnemen aan sniffing, de verbinding wordt niet aan jou gegeven; empirisch wordt deze onmiddellijk verbroken, ≤ 8 bytes ASCII).

---

## 2. In drie stappen draaien

### Stap één: configuratie

`speedmqd.json` (**de feitelijke configuratie is standaard JSON en mag geen commentaar bevatten**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/speedmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Stap twee: starten

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Stap drie: verifiëren

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. Implementatiepunten

### 3.1 Framing

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

In Node gebruik je `Buffer`: verzamel de ontvangen bytes en knip er een frame uit zodra dat compleet is.

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

Eén stream = één clientverbinding; de payload van een data-frame is `4 bytes big-endian streamnummer + ruwe bytes`.

### 3.2 Handshake en heartbeat

De kernel **stuurt eerst een Hello**, jij antwoordt met `HelloAck`; de kernel valideert `name` en `api_version` (momenteel `v1`) en verbindt dan.
Daarna elke 2s een `Ping`, antwoord met `Pong` (dit wordt meteen door de read-lus afgehandeld, er is geen timer nodig).

### 3.3 Asynchroon model (Node-versie)

Een single-threaded event loop vermijdt van nature het probleem van "interleaved schrijven" — maar let op: **laat de read-lus niet `await`en op een omgekeerde aanroep**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` is `async`: het kan weer `await call('session.settle', …)` doen, dus het mag absoluut niet synchroon wachtend worden geschreven.

### 3.4 Semantiekbrug (eerst authenticeren)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` kan niet worden overgeslagen: het kernel-operatievlak van de verbinding heeft vóór authenticatie geen identiteit, en een directe `session.open` wordt geweigerd
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

Bezorgingen worden door de kernel **in voorwaartse richting teruggeduwd** (`method = "session.deliver"`); na verwerking `session.settle`
(`ack` / `requeue` / `reject`; het bezorgingsnummer is globaal uniek en heeft geen streamnummer).

---

## 4. Codewandeling (voorbeeldproject)

`speedmq-plugin/nodejs/index.js` is ongeveer 330 regels:

| Locatie | Functie |
| --- | --- |
| `u32()` / `Conn.send()` | Framing lezen/schrijven |
| `Conn.drain()` / `dispatch()` | Parseren en dispatchen per frame |
| `Conn.call()` | Omgekeerde aanroep (`Promise` + `pending`-tabel, match op `reverse=true` en `id`) |
| `Stream` | Lees-zijde van een stream: `push/end/read` vormen een asynchrone wachtrij |
| `handleHello` | Valideren en HelloAck terugsturen |
| `handleForwardCall` / `handleMethod` | Forward calls (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Auth + declareren + publiceren + consumeren |

---

## 5. Meting (lokaal gereproduceerd)

Windows + Node v24; de kernel draait in Docker (`speedmq:1.1.01`), de plugin op de host (`tcp://host.docker.internal:19011`).

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

Bestreken: **handshake → auth → semantiekbrug → bezorging terugduwen → settle → bytestroom-echo**.

---

## 6. Node.js-specifieke aandachtspunten

- **`socket.write` schrijft één frame per keer**: het voorbeeld zet het hele frame in één `Buffer` en schrijft die, dus extra locking is niet nodig;
  als je een frame in meerdere `write`-aanroepen splitst, moet je zelf de volgorde garanderen.
- **De chunk-grenzen van `stream.on('data')` hebben niets met frames te maken**: je moet zelf bufferen (zie `drain()`).
- **base64**: `message.body` en `core.authenticate.response` zijn in JSON base64-strings
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **`await` niet in `drain()`**: het is een synchrone framesplits-functie; laat de asynchrone verwerking aan `handleForwardCall` over.
- **ESM vs CJS**: het voorbeeld gebruikt CommonJS (`require`) zodat `node index.js` direct draait; voor ESM hoef je alleen `import` te gebruiken.

---

## 7. Gevorderd

- Plugin met eigen beheer-UI: voeg `console_url` toe in de configuratie (hoofddocument §5.8), dan verschijnt op de pagina "Pluginbeheer" van de beheerconsole een directe ingang.
- Losstaande uitrol (K8s / systemd): `spawn: []` + `address: "tcp://<servicenaam>:19011"`, in de container luisteren op `0.0.0.0`.
- Poorthergebruik: geef meerdere protocollen elk een andere `prefix`, de kernel dispatchet verbindingen op prefix naar de respectieve plugins.
