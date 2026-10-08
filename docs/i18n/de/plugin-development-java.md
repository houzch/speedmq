# SpeedMQ – Leitfaden zur Entwicklung externer Prozess-Plugins — Java

> **Zielgruppe**: Entwickler, die SpeedMQ mit Java um externe Prozess-Plugins (Sidecars) erweitern möchten.
> **Zuerst lesen**: [Leitfaden zur Entwicklung externer Prozess-Plugins (Sidecar)](plugin-development.md) (mentales Modell / Konfigurationsfelder / vollständige Wire-Protokoll-Tabelle).
> **Beispielprojekt**: Workspace `speedmq-plugin/java/SidecarPlugin.java` (eine einzige Datei, nur JDK-Standardbibliothek, kein Maven/Gradle erforderlich).

---

## 1. Wie es im Betrieb aussieht

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Drei Kernpunkte: **Dein Prozess ist der Server** (wartet darauf, dass der Kernel sich verbindet); **der nach außen gerichtete Port wird vom Kernel geöffnet** (`protocols[].listeners`);
**`prefix` darf nicht leer sein** (ein leeres Präfix = keine Teilnahme am Sniffing, die Verbindung wird nicht an dich übergeben; gemessen wird sie sofort getrennt, ≤8 Bytes ASCII).

---

## 2. In drei Schritten startklar

### Erster Schritt: Konfiguration

`speedmqd.json` (**die tatsächliche Konfiguration ist Standard-JSON und darf keine Kommentare enthalten**):

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

### Zweiter Schritt: Kompilieren und starten

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Dritter Schritt: Verifizieren

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

In Java sind `DataInputStream`/`DataOutputStream` am unkompliziertesten — ihre `readInt`/`writeInt` sind bereits **Big-Endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Die Schreibseite muss **seriell** sein (Heartbeat, Antworten und Datenblöcke kommen aus verschiedenen Threads):

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

Nutzlast eines Datenframes = `4-Byte-Big-Endian-Streamnummer + Rohbytes`.

### 3.2 Handshake und Heartbeat

Der Kernel **sendet zuerst Hello**, du antwortest mit `HelloAck`; nach Prüfung von `name` und `api_version` (derzeit `v1`) wird die Verbindung aufgenommen.
Danach kommt alle 2 s ein `Ping`, du antwortest mit `Pong`.

### 3.3 Nebenläufigkeitsmodell (Java-Version)

| Rolle | Thread |
| --- | --- |
| Frame-Leseschleife | eine pro Kernel-Verbindung |
| Stream-Verarbeitung | eine pro Stream (mehrere Client-Verbindungen können parallel laufen) |
| Forward-Call-Verarbeitung | eine pro Aufruf |

**In der Leseschleife darf nicht synchron auf die Antwort eines Reverse-Aufrufs gewartet werden** (das führt zu einem Deadlock): Die Verarbeitung von `session.deliver` muss an einen eigenen Thread übergeben werden,
da sie intern noch `session.settle` ausführt (wiederum ein Reverse-Aufruf). Das Beispiel macht es so.

### 3.4 Semantische Brücke (Authentifizierung zwingend zuerst)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` darf nicht weggelassen werden: Die Kernel-Betriebsebene der Verbindung hat vor der Authentifizierung keine Identität, ein direktes `session.open` wird abgelehnt
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

Zustellungen werden vom Kernel **per Forward-Aufruf zurückgeschoben** (`method = "session.deliver"`); nach der Verarbeitung folgt `session.settle`
(`ack` / `requeue` / `reject`; die Zustellnummer ist global eindeutig, eine Stream-Nummer wird nicht mitgegeben).

---

## 4. Code-Durchgang (Beispielprojekt)

`speedmq-plugin/java/SidecarPlugin.java` umfasst etwa 470 Zeilen (inkl. minimalem JSON):

| Position | Zweck |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Framing-Lesen/Schreiben (`DataInputStream` + Schreib-Lock) |
| `Conn.serve()` | Frame-Leseschleife und Verteilung |
| `Conn.call()` | Reverse-Aufruf (`pending`-Tabelle + blockierende Queue, Timeout-Schutz) |
| `Conn.handleHello()` | Prüfen und HelloAck zurücksenden |
| `StreamState` | Leseseite des Streams (`BlockingQueue`, `STREAM_END` bedeutet Ende) |
| `Conn.handleForwardCall()` / `handleMethod()` | Forward-Aufrufe (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Authentifizierung + Declare + Publish + Consume |
| `Json` (am Dateiende) | Minimales JSON-Lesen/Schreiben, nur damit das Beispiel ohne Abhängigkeiten auskommt |

> **Empfehlung für die Produktion**: Ersetze `Json` durch deine gewohnte Bibliothek (Jackson / Gson), oder verwende einen bestehenden Stack statt `java.net.http` —
> das hat mit dem, was dieses Beispiel vermitteln will (das Wire-Protokoll), nichts zu tun.

---

## 5. Praxistest (lokal reproduziert)

Windows + JDK 25; der Kernel läuft in Docker (`speedmq:1.1.01`), das Plugin auf dem Host (`tcp://host.docker.internal:19031`).

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

Abgedeckt: **Handshake → Authentifizierung → semantische Brücke → Zustellungs-Push → Abrechnung → Byte-Stream-Echo**.

---

## 6. Java-spezifische Hinweise

- **`\uXXXX` im Quelltext wird vom Compiler an jeder Stelle verarbeitet** (auch in Kommentaren!). Die Beispielkommentare schreiben deshalb bewusst
  „NUL + Benutzername + NUL + Passwort" statt direkt `\u0000`, sonst meldet javac ein ungültiges Zeichen.
- **Chinesischer Quelltext erfordert `javac -encoding UTF-8`**, sonst meldet Windows mit der Standardkodierung (GBK) „unmappable character for encoding GBK".
  Wenn zur Laufzeit Chinesisch korrekt ausgegeben werden soll, füge `-Dfile.encoding=UTF-8` hinzu.
- **Von einem Lambda erfasste lokale Variablen müssen effectively final sein**: Im Beispiel wird `name` bei der Argumentauswertung neu zugewiesen,
  daher wird im Lambda `opts.name` verwendet (ein Feld, das nur einmal zugewiesen wird).
- **`DataInputStream` ist blockierend**: Beim Verbindungsabbruch wird `EOFException`/`IOException` geworfen, danach wird aufgeräumt.
- **base64**: `message.body` und `core.authenticate.response` sind in JSON base64-Zeichenketten
  (`Base64.getEncoder()/getDecoder()`).
- **Die JDK-Standardbibliothek hat kein JSON**: Das Beispiel bringt eine minimale Implementierung mit; `Json.parse` zerlegt Ganzzahlen als `Long` und Gleitkommazahlen als `Double`,
  beim Auslesen von `id` verwendet man `((Number) m.get("id")).longValue()`.

---

## 7. Weiterführend

- Paketiere es als ausführbares jar (`Main-Class: SidecarPlugin`) oder nutze `jlink` für eine schlanke Laufzeit,
  und ändere dann `spawn` zu `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`.
- Plugin mit eigener Verwaltungsoberfläche: `console_url` in der Konfiguration ergänzen (Hauptdokument §5.8), dann erscheint auf der Seite „Plugin-Verwaltung" des Verwaltungs-Backends ein direkter Einstieg.
- Eigenständige Bereitstellung: `spawn: []` + `address: "tcp://<Dienstname>:19031"`, im Container auf `0.0.0.0` lauschen.
