# SwiftMQ Guida allo sviluppo dei plugin in processo esterno —— Node.js

> **Destinatari**: sviluppatori che scrivono plugin in processo esterno (sidecar) per SwiftMQ in Node.js.
> **Leggi prima**: [Guida allo sviluppo dei plugin in processo esterno (sidecar)](plugin-development.md) (modello mentale / campi di configurazione / tabella completa del protocollo di rete).
> **Progetto di esempio**: workspace `swiftmq-plugin/nodejs/index.js` (sola libreria standard di Node, **nessuna dipendenza npm**).

---

## 1. Che aspetto ha quando gira

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tre punti chiave: **il tuo processo è il server** (attende che il kernel si connetta); **la porta esterna è aperta dal kernel** (configurazione `protocols[].listeners`);
**`prefix` deve essere non vuoto** (un prefisso vuoto = non partecipa allo sniffing, e la connessione non ti verrà ceduta; verificato in pratica viene chiusa immediatamente, ≤8 byte ASCII).

---

## 2. Farlo girare in tre passi

### Primo passo: configurazione

`swiftmqd.json` (**la configurazione effettiva è JSON standard, non può contenere commenti**):

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

### Secondo passo: avviarlo

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Terzo passo: verificarlo

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. Punti salienti dell'implementazione

### 3.1 Suddivisione in frame

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

In Node usa `Buffer`: accumula i byte ricevuti ed estrai un frame non appena ce ne sono abbastanza.

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

Uno stream = una connessione client; il payload di un frame dati è `4 byte big-endian di stream id + byte grezzi`.

### 3.2 Handshake e heartbeat

Il kernel **invia per primo Hello**, e tu rispondi `HelloAck`; il kernel verifica `name` e `api_version` (attualmente `v1`) e poi si connette.
Poi c'è un `Ping` ogni 2s, e tu rispondi semplicemente `Pong` (gestito al volo dal ciclo di lettura, nessun timer necessario).

### 3.3 Modello asincrono (versione Node)

Un event loop a thread singolo evita naturalmente il problema delle "scritture interlacciate" —— ma attenzione a **non far attendere una chiamata inversa al ciclo di lettura**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` è `async`: può a sua volta fare `await call('session.settle', …)`, quindi non deve mai essere scritto come attesa sincrona.

### 3.4 Ponte semantico (prima si deve autenticare)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` non si può omettere: la superficie operativa kernel della connessione non ha identità prima dell'autenticazione, e un `session.open` diretto viene rifiutato
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

Le consegne sono **respinte in avanti** dal kernel (`method = "session.deliver"`); dopo la gestione `session.settle`
(`ack` / `requeue` / `reject`; il numero di consegna è globalmente univoco e non porta lo stream id).

---

## 4. Lettura guidata del codice (progetto di esempio)

`swiftmq-plugin/nodejs/index.js` è di circa 330 righe:

| Posizione | Scopo |
| --- | --- |
| `u32()` / `Conn.send()` | Lettura/scrittura dei frame |
| `Conn.drain()` / `dispatch()` | Parsing e dispatch per frame |
| `Conn.call()` | Chiamata inversa (`Promise` + tabella `pending`, abbinata per `reverse=true` e `id`) |
| `Stream` | Lato lettura dello stream: `push/end/read` formano una coda asincrona |
| `handleHello` | Verifica e risposta con HelloAck |
| `handleForwardCall` / `handleMethod` | Chiamate dirette (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Autenticazione + dichiarazione + pubblicazione + consumo |

---

## 5. Verifica pratica (riprodotta in locale)

Windows + Node v24; il kernel in Docker (`swiftmq:1.1.01`), il plugin sull'host (`tcp://host.docker.internal:19011`).

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

Coperto: **handshake → autenticazione → ponte semantico → rinvio delle consegne → liquidazione → echo del flusso di byte**.

---

## 6. Note specifiche per Node.js

- **`socket.write` scrive un frame alla volta**: l'esempio assembla l'intero frame in un unico `Buffer` prima di scrivere, quindi non serve un lock aggiuntivo;
  se dividi un frame in più `write`, devi garantire tu l'ordine.
- **I confini dei chunk di `stream.on('data')` non hanno nulla a che vedere con i frame**: devi accumulare tu il buffer (vedi `drain()`).
- **base64**: `message.body` e `core.authenticate.response` in JSON sono stringhe base64
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Non fare `await` dentro `drain()`**: è una funzione sincrona di suddivisione dei frame; affida la gestione asincrona a `handleForwardCall`.
- **ESM vs CJS**: l'esempio usa CommonJS (`require`) così `node index.js` funziona direttamente; passare a ESM significa solo usare `import`.

---

## 7. Argomenti avanzati

- Plugin con interfaccia di gestione propria: aggiungi `console_url` nella configurazione (documento principale §5.8), e nella pagina "Gestione plugin" della console di gestione comparirà una voce diretta.
- Deployment autonomo (K8s / systemd): `spawn: []` + `address: "tcp://<nome del servizio>:19011"`, ascolto su `0.0.0.0` dentro il container.
- Riuso delle porte: assegna a ciascuno dei più protocolli un `prefix` diverso, e il kernel distribuisce le connessioni ai rispettivi plugin in base al prefisso.
