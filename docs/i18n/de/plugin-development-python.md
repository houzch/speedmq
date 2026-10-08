# SpeedMQ – Leitfaden zur Entwicklung externer Prozess-Plugins — Python

> **Zielgruppe**: Entwickler, die SpeedMQ mit Python um externe Prozess-Plugins (Sidecars) erweitern möchten.
> **Zuerst lesen**: [Leitfaden zur Entwicklung externer Prozess-Plugins (Sidecar)](plugin-development.md) — dort werden das mentale Modell, die Konfigurationsfelder und die vollständige Wire-Protokoll-Tabelle beschrieben;
> dieses Dokument behandelt nur, **wie man das in Python umsetzt**, sowie die lokal verifizierten Schritte und Ergebnisse.
> **Beispielprojekt**: Workspace `speedmq-plugin/python/sidecar_plugin.py` (nur Standardbibliothek, keinerlei Abhängigkeiten von Drittanbietern).

---

## 1. Wie es im Betrieb aussieht

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Drei Kernpunkte (leicht zu verwechseln, daher gut einprägen):

1. **Dein Prozess ist der Server**: Er lauscht auf einer lokalen Adresse und wartet darauf, dass der Kernel sich verbindet (`plugins.<Plugin-Name>.sidecar.address`).
2. **Der nach außen gerichtete Geschäftsport wird vom Kernel geöffnet**: Clients verbinden sich mit dem Port des Kernels, und die Bytes werden an dich weitergeleitet (proxied) (`protocols[].listeners`).
3. **`prefix` darf nicht leer sein**: Der Kernel entscheidet per Präfix-Sniffing, „wem diese Verbindung übergeben wird". Ein leeres `prefix` bedeutet **keine Teilnahme am Sniffing**, und die Verbindung wird auch auf seinem eigenen Listener nicht an dich übergeben (gemessen: die Verbindung wird sofort getrennt). Die Präfixlänge beträgt ≤ 8 Bytes, ASCII.

---

## 2. In drei Schritten startklar

### Erster Schritt: Plugin in der Konfiguration deklarieren

`speedmqd.json` (**die tatsächliche Konfiguration ist Standard-JSON und darf keine Kommentare enthalten**):

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/speedmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` ist **die Adresse, mit der sich der Kernel bei dir verbindet** (der Kernel ist der Client, das Plugin der Server).
- `spawn` leer = der Kernel verbindet sich nur, startet aber nicht; den Prozess verwaltest du selbst (systemd / supervisor / compose).
- `prefix` **darf nicht leer sein**: Die ersten Bytes, die der Client sendet, müssen damit beginnen (der Kernel entscheidet per Präfix-Sniffing, wem die Verbindung übergeben wird).
- `listeners` sind die nach außen gerichteten Ports, die vom Kernel geöffnet werden (der Client verbindet sich mit dem Kernel, nicht mit dir).
- Bei einer containerübergreifenden Bereitstellung verwendet `address` den **Dienstnamen** (z. B. `tcp://py-sidecar:19001`), und das Plugin muss auf `0.0.0.0` lauschen.

### Zweiter Schritt: Starten

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Dritter Schritt: Verifizieren

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Implementierungshinweise

### 3.1 Framing (die einzige Byte-Ebene, die du selbst korrekt implementieren musst)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

Die Nutzlast eines Datenframes = `4-Byte-Big-Endian-Streamnummer + Rohbytes`; die Nutzlast der Steuerebene ist JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake und Heartbeat

Nachdem der Kernel verbunden ist, **sendet er zuerst Hello**; du musst einen `HelloAck`-Frame zurücksenden. Der Kernel prüft
`name == Plugin-Name in der Konfiguration` und `api_version == APIVersion des Kernels` (derzeit `v1`).
Danach sendet der Kernel alle 2 s ein `Ping`, du antwortest einfach mit `Pong` (keine Antwort wird als tot gewertet).

### 3.3 Logische Streams

`kindOpen` trifft ein → **zuerst mit `OpenAck` antworten**, dann mit dem Dienst beginnen; `kindData` trifft ein → unverändert (oder nach Auswertung durch dein Protokoll) als `kindData` zurückschreiben; Verarbeitung beendet → `kindClose` senden. Ein Stream = eine Client-Verbindung.

### 3.4 Reverse-Aufrufe und semantische Kernel-Brücke

Aufrufe vom Plugin → Kernel laufen über `kindCall` mit `"reverse": true`, und der Kernel antwortet auf derselben Verbindung mit `kindReply`.
**Die Reihenfolge ist wichtig**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **darf nicht weggelassen werden**: Die Kernel-Betriebsebene der Verbindung hat vor der Authentifizierung keine Identität, ein direktes `session.open` wird abgelehnt
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Die Parameter sind die aus deinem Protokoll ausgewertete SASL-Antwort:

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

Die Zustellung konsumierter Nachrichten wird vom Kernel **per Forward-Aufruf zurückgeschoben** (`kindCall`, `method = "session.deliver"`); nach der Verarbeitung wird mit
`session.settle` abgerechnet (`ack` / `requeue` / `reject`, die Zustellnummer ist global eindeutig, eine Stream-Nummer ist nicht erforderlich).

### 3.5 Nebenläufigkeitsmodell (Python-Version)

| Rolle | Thread |
| --- | --- |
| Frame-Leseschleife | eine pro Kernel-Verbindung |
| Stream-Verarbeitung | eine pro Stream (daher können mehrere Client-Verbindungen parallel laufen) |
| Forward-Call-Verarbeitung | eine pro Aufruf |

**Unbedingt beachten**: In der Leseschleife darf **nicht** synchron auf die Antwort eines Reverse-Aufrufs gewartet werden (das führt zu einem Deadlock) — Forward-Aufrufe (`session.deliver`)
müssen an einen eigenen Thread übergeben werden, da sie während der Verarbeitung möglicherweise erneut `session.settle` auslösen. Das Schreiben von Frames muss per Lock serialisiert werden.

---

## 4. Code-Durchgang (Beispielprojekt)

`speedmq-plugin/python/sidecar_plugin.py` umfasst etwa 320 Zeilen; die wichtigsten Funktionen:

| Position | Zweck |
| --- | --- |
| `read_frame` / `Conn.send` | Framing-Lesen/Schreiben (Längenpräfix + kind) |
| `Conn.call` | Reverse-Aufruf: Nummerierung → Senden → auf Antwort warten (Abgleich über `reverse=true` und `id`) |
| `Conn.serve` | Frame-Leseschleife und Verteilung |
| `Conn._handle_hello` | Plugin-Name/API-Version prüfen und HelloAck zurücksenden |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Stream-Lebenszyklus und Echo |
| `Conn._dispatch` | Forward-Aufrufe verarbeiten: `session.deliver` (inkl. settle), `stats` |
| `Conn._session_demo` | Authentifizierung + Queue deklarieren + Publish + Consume |

---

## 5. Praxistest (lokal reproduziert)

Umgebung: Windows + Python 3.12; der Kernel läuft in Docker (`speedmq:1.1.01`), das Plugin läuft auf dem Host,
und der Kernel verbindet sich über `tcp://host.docker.internal:19001` damit.

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

Abgedeckte Kette: **Handshake → Authentifizierung → semantische Brücke (Declare/Publish/Consume) → Zustellungs-Push → Abrechnung → Byte-Stream-Echo**.

---

## 6. Python-spezifische Hinweise

- **Nicht mit `time.sleep` auf Heartbeats warten**: Die Leseschleife ist blockierend; es genügt, die Verbindung über das `Ping` des Kernels aufrechtzuerhalten.
  Wenn du ein Lese-Timeout für den Socket gesetzt hast, behandle das Timeout als „Verbindungsende" (bei `kill -9` des Kernels wird der Socket möglicherweise nicht rechtzeitig geschlossen).
- **`json.dumps` fügt standardmäßig Leerzeichen hinzu**: Das Beispiel verwendet `separators=(",", ":")` nur, damit die Logs hübscher aussehen; das Protokoll selbst verlangt es nicht.
- **Bytes sind base64**: `message.body` und `core.authenticate.response` sind in JSON beides base64-Zeichenketten;
  vergiss `base64.b64encode/decode` nicht.
- **Frame-Schreiben muss gesperrt werden**: Heartbeats, Antworten und Datenblöcke kommen aus verschiedenen Threads; verschachteltes Schreiben verfälscht die gesamte Verbindung (das Beispiel verwendet `threading.Lock`).
- `asyncio` ist ebenfalls möglich, aber du musst „serielles Schreiben + nicht blockierende Leseschleife" sicherstellen; der Ansatz entspricht der Thread-Version.

---

## 7. Weiterführend

- Wenn du deinem Plugin eine eigene Verwaltungsoberfläche geben möchtest: Füge `console_url` in der Konfiguration hinzu, dann erscheint auf der Seite „Plugin-Verwaltung" des Verwaltungs-Backends ein direkter Einstieg
  (siehe Hauptdokument §5.8).
- Wenn sich das Plugin als eigenständiger Dienst darstellen und von systemd / K8s verwaltet werden soll: `spawn: []` + `restart: "never"`, extern gestartet.
- Wenn mehrere Protokolle koexistieren sollen: Lasse mehrere Plugins auf demselben Listener unterschiedliche `prefix` verwenden, oder öffne jeweils einen dedizierten Port.
