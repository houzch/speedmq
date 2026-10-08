# SpeedMQ – Leitfaden zur Entwicklung externer Prozess-Plugins — Node.js

> **Zielgruppe**: Entwickler, die SpeedMQ mit Node.js um externe Prozess-Plugins (Sidecars) erweitern möchten.
> **Zuerst lesen**: [Leitfaden zur Entwicklung externer Prozess-Plugins (Sidecar)](plugin-development.md) (mentales Modell / Konfigurationsfelder / vollständige Wire-Protokoll-Tabelle).
> **Beispielprojekt**: Workspace `speedmq-plugin/nodejs/index.js` (nur Node-Standardbibliothek, **keine npm-Abhängigkeiten erforderlich**).

---

## 1. Wie es im Betrieb aussieht

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Drei Kernpunkte: **Dein Prozess ist der Server** (wartet darauf, dass der Kernel sich verbindet); **der nach außen gerichtete Port wird vom Kernel geöffnet** (Konfiguration `protocols[].listeners`);
**`prefix` darf nicht leer sein** (ein leeres Präfix = keine Teilnahme am Sniffing, die Verbindung wird nicht an dich übergeben; gemessen wird sie sofort getrennt, ≤8 Bytes ASCII).

---

## 2. In drei Schritten startklar

### Erster Schritt: Konfiguration

`speedmqd.json` (**die tatsächliche Konfiguration ist Standard-JSON und darf keine Kommentare enthalten**):

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

### Zweiter Schritt: Starten

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Dritter Schritt: Verifizieren

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. Implementierungshinweise

### 3.1 Framing

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

In Node verwendest du `Buffer`: Sammle die empfangenen Bytes, und sobald ein vollständiger Frame vorliegt, schneide ihn heraus und verarbeite ihn.

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

Ein Stream = eine Client-Verbindung; die Nutzlast eines Datenframes ist `4-Byte-Big-Endian-Streamnummer + Rohbytes`.

### 3.2 Handshake und Heartbeat

Der Kernel **sendet zuerst Hello**, du antwortest mit `HelloAck`; nach Prüfung von `name` und `api_version` (derzeit `v1`) wird die Verbindung aufgenommen.
Danach kommt alle 2 s ein `Ping`, du antwortest einfach mit `Pong` (die Leseschleife erledigt das nebenbei, ein Timer ist nicht erforderlich).

### 3.3 Asynchrones Modell (Node-Version)

Die Single-Thread-Ereignisschleife vermeidet von Natur aus das Problem „verschachtelter Schreibvorgänge" — aber achte darauf, **die Leseschleife nicht auf einen Reverse-Aufruf `await`en zu lassen**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` ist `async`: Es kann erneut `await call('session.settle', …)` ausführen, daher darf es keinesfalls als synchrones Warten geschrieben werden.

### 3.4 Semantische Brücke (Authentifizierung zwingend zuerst)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` darf nicht weggelassen werden: Die Kernel-Betriebsebene der Verbindung hat vor der Authentifizierung keine Identität, ein direktes `session.open` wird abgelehnt
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

Zustellungen werden vom Kernel **per Forward-Aufruf zurückgeschoben** (`method = "session.deliver"`); nach der Verarbeitung folgt `session.settle`
(`ack` / `requeue` / `reject`; die Zustellnummer ist global eindeutig, eine Stream-Nummer wird nicht mitgegeben).

---

## 4. Code-Durchgang (Beispielprojekt)

`speedmq-plugin/nodejs/index.js` umfasst etwa 330 Zeilen:

| Position | Zweck |
| --- | --- |
| `u32()` / `Conn.send()` | Framing-Lesen/Schreiben |
| `Conn.drain()` / `dispatch()` | Frame-Parsing und Verteilung |
| `Conn.call()` | Reverse-Aufruf (`Promise` + `pending`-Tabelle, Abgleich über `reverse=true` und `id`) |
| `Stream` | Leseseite des Streams: `push/end/read` bilden eine asynchrone Warteschlange |
| `handleHello` | Prüfen und HelloAck zurücksenden |
| `handleForwardCall` / `handleMethod` | Forward-Aufrufe (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Authentifizierung + Declare + Publish + Consume |

---

## 5. Praxistest (lokal reproduziert)

Windows + Node v24; der Kernel läuft in Docker (`speedmq:1.1.01`), das Plugin auf dem Host (`tcp://host.docker.internal:19011`).

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

Abgedeckt: **Handshake → Authentifizierung → semantische Brücke → Zustellungs-Push → Abrechnung → Byte-Stream-Echo**.

---

## 6. Node.js-spezifische Hinweise

- **`socket.write` schreibt jeweils einen Frame**: Das Beispiel fügt den gesamten Frame zu einem `Buffer` zusammen und schreibt ihn dann, daher ist kein zusätzliches Lock nötig;
  wenn du einen Frame in mehrere `write`-Aufrufe aufteilst, musst du die Reihenfolge selbst sicherstellen.
- **Die Chunk-Grenzen von `stream.on('data')` haben nichts mit Frames zu tun**: Du musst selbst puffern (siehe `drain()`).
- **base64**: `message.body` und `core.authenticate.response` sind in JSON base64-Zeichenketten
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Kein `await` in `drain()`**: Es ist eine synchrone Frame-Zerlegungsfunktion; übergib die asynchrone Verarbeitung an `handleForwardCall`.
- **ESM vs. CJS**: Das Beispiel verwendet CommonJS (`require`), damit `node index.js` direkt läuft; für ESM musst du nur auf `import` umstellen.

---

## 7. Weiterführend

- Plugin mit eigener Verwaltungsoberfläche: `console_url` in der Konfiguration ergänzen (Hauptdokument §5.8), dann erscheint auf der Seite „Plugin-Verwaltung" des Verwaltungs-Backends ein direkter Einstieg.
- Eigenständige Bereitstellung (K8s / systemd): `spawn: []` + `address: "tcp://<Dienstname>:19011"`, im Container auf `0.0.0.0` lauschen.
- Port-Wiederverwendung: Mehrere Protokolle erhalten jeweils ein unterschiedliches `prefix`, und der Kernel verteilt die Verbindungen anhand des Präfixes an die jeweiligen Plugins.
