# SwiftMQ – Leitfaden zur Entwicklung externer Prozess-Plugins — PHP

> **Zielgruppe**: Entwickler, die SwiftMQ mit PHP um externe Prozess-Plugins (Sidecars) erweitern möchten.
> **Zuerst lesen**: [Leitfaden zur Entwicklung externer Prozess-Plugins (Sidecar)](plugin-development.md) (mentales Modell / Konfigurationsfelder / vollständige Wire-Protokoll-Tabelle).
> **Beispielprojekt**: Workspace `swiftmq-plugin/php/sidecar_plugin.php` (nur Standardbibliothek, **keine composer-Abhängigkeiten erforderlich**).

---

## 1. Wie es im Betrieb aussieht

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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

`swiftmqd.json` (**die tatsächliche Konfiguration ist Standard-JSON und darf keine Kommentare enthalten**):

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/swiftmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### Zweiter Schritt: Starten

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Dritter Schritt: Verifizieren

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP verwendet `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Nutzlast eines Datenframes = `pack('N', $streamId) . Rohbytes`.

### 3.2 Handshake und Heartbeat

Der Kernel **sendet zuerst Hello**, du antwortest mit `HelloAck`; der Kernel prüft `name` und `api_version` (derzeit `v1`).
Danach kommt alle 2 s ein `Ping`, du antwortest mit `Pong`.

### 3.3 Nebenläufigkeitsmodell: wiedereintrittsfähige Frame-Pumpe (PHP kennt keine Threads)

PHP CLI ist single-threaded und blockierend, daher wird hier nicht „ein Thread pro Stream" verwendet, sondern:

- Die **Leseschleife** (`serve()`) übernimmt Handshake, Heartbeat, Stream-Öffnen, Daten-Echo und die Verarbeitung von Forward-Aufrufen;
- **Echo** benötigt keine zusätzliche Zustandsmaschine: Sobald `kindData` empfangen wird, wird es sofort unverändert als `kindData` zurückgeschrieben;
- **Reverse-Aufrufe** verwenden `callAndWait()`: Nach dem Senden von `kindCall` wird beim Lesen von Frames zugleich verteilt,
  bis **die eigene** Antwort gelesen wird (`reverse=true` und `id` stimmen überein), erst dann wird zurückgekehrt.

```php
public function callAndWait(string $method, $params = null)
{
    $id = ++$this->nextId;
    $this->sendJson(KIND_CALL, ['id' => $id, 'method' => $method, 'reverse' => true, 'params' => $params]);
    for (;;) {
        [$kind, $payload] = $this->readFrame();
        if ($kind === KIND_REPLY) {
            $reply = json_decode($payload, true);
            if (!empty($reply['reverse']) && $reply['id'] === $id) {
                if (empty($reply['ok'])) throw new RuntimeException($reply['error'] ?? '调用失败');
                return $reply['data'] ?? null;
            }
            continue;
        }
        $this->dispatchOther($kind, $payload);   // 心跳/数据/正向调用照常处理
    }
}
```

Das bedeutet, dass `dispatchOther()` **wiedereintrittsfähig** sein muss: Es kann innerhalb eines `callAndWait` erneut aufgerufen werden
(z. B. wenn bei der Verarbeitung von `session.deliver` erneut `session.settle` nötig ist). Das Beispiel macht genau das.

### 3.4 Semantische Brücke (Authentifizierung zwingend zuerst)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` darf nicht weggelassen werden: Die Kernel-Betriebsebene der Verbindung hat vor der Authentifizierung keine Identität, ein direktes `session.open` wird abgelehnt
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```php
$ident = $this->callAndWait('core.authenticate', [
    'stream'    => $streamId,
    'mechanism' => 'PLAIN',
    // SASL PLAIN 响应：\x00<user>\x00<password>，字节在 JSON 里走 base64
    'response'  => base64_encode("\x00{$user}\x00{$password}"),
]);
$this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
$q = $this->callAndWait('session.declare_queue', ['stream' => $streamId, 'exclusive' => true, 'auto_delete' => true]);
$this->callAndWait('session.consume', ['stream' => $streamId, 'queue' => $q['name'], 'prefetch' => 32]);
```

Zustellungen werden vom Kernel **per Forward-Aufruf zurückgeschoben** (`method = "session.deliver"`); nach der Verarbeitung folgt `session.settle`
(`ack` / `requeue` / `reject`; die Zustellnummer ist global eindeutig, eine Stream-Nummer wird nicht mitgegeben).

---

## 4. Code-Durchgang (Beispielprojekt)

`swiftmq-plugin/php/sidecar_plugin.php` umfasst etwa 320 Zeilen:

| Position | Zweck |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Framing-Lesen/Schreiben |
| `Conn::serve()` | Hauptleseschleife |
| `Conn::dispatchOther()` | Verteilung von Nicht-Handshake-Frames (wiedereintrittsfähig) |
| `Conn::callAndWait()` | Reverse-Aufruf (wiedereintrittsfähige Frame-Pumpe) |
| `Conn::handleHello()` | Prüfen und HelloAck zurücksenden |
| `Conn::handleForwardCall()` / `handleMethod()` | Forward-Aufrufe (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Authentifizierung + Declare + Publish + Consume |

---

## 5. Praxistest (lokal reproduziert)

Windows + PHP 7.4; der Kernel läuft in Docker (`swiftmq:1.1.01`), das Plugin auf dem Host (`tcp://host.docker.internal:19021`).

```
php -l sidecar_plugin.php  → No syntax errors detected
plugin=php-sidecar state=enabled          # /api/plugins
echo=[PHhello]                            # 客户端连内核端口 19022 发 "PHhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=php-sidecar addr=0.0.0.0:19021 version=0.1.0
内核已接入 plugin=php-sidecar peer=127.0.0.1:51006
握手完成 plugin=php-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-955afebb58d6b5307ba36e
流已打开 plugin=php-sidecar stream=1 remote=172.17.0.1:59664 local=172.17.0.2:19022
收到投递（session.deliver） queue=amq.gen-955afebb58d6b5307ba36e delivery_id=1 body=hello from php sidecar
```

Abgedeckt: **Handshake → Authentifizierung → semantische Brücke → Zustellungs-Push → Abrechnung → Byte-Stream-Echo**.

---

## 6. PHP-spezifische Hinweise

- **PHP 7.4 hat keinen `mixed`-Rückgabetyp** (erst ab PHP 8.0): Im Beispiel gibt der Reverse-Aufruf „einen beliebigen Typ" zurück,
  daher wird **keine Rückgabetypdeklaration geschrieben** (stattdessen der Kommentar `@return mixed`). Unter 7.4 führt `: mixed` direkt zu einem Syntaxfehler.
- **Zahlentypen in JSON**: `json_decode($s, true)` zerlegt Ganzzahlen standardmäßig als `int`, große Ganzzahlen können zu `float` werden;
  die Zustellnummern bereiten im Maßstab dieses Beispiels keine Probleme, bei sehr großen Nummern solltest du `JSON_BIGINT_AS_STRING` in Betracht ziehen.
- **base64 ist zwingend**: `message.body` und `core.authenticate.response` sind in JSON base64-Zeichenketten
  (`base64_encode` / `base64_decode($s, true)`).
- **Kein `pcntl_fork` für Nebenläufigkeit verwenden**: Unter Windows gibt es kein pcntl, und nach einem fork wird die Annahme „eine Verbindung, ein Schreiber" zerstört;
  Single-Thread + wiedereintrittsfähige Frame-Pumpe reichen aus (es sei denn, du musst auf einem Stream sehr aufwändige Berechnungen durchführen — dann eignet sich ein externer Dienst besser).
- **`stream_socket_accept` ist blockierend**: Der Prozesslebenszyklus wird vom Kernel (`spawn`) oder einem Supervisor verwaltet;
  denke daran, `''` als Rückgabe von `fread` (EOF) zu behandeln → diese Verbindung beenden und zum accept zurückkehren.
- **Ausgabepuffer**: Verwende für Logs `fwrite(STDOUT, …)` mit einem abschließenden Zeilenumbruch, damit der Kernel sie zeilenweise in das Kernel-Log weiterleiten kann.

---

## 7. Weiterführend

- Plugin mit eigener Verwaltungsoberfläche: `console_url` in der Konfiguration ergänzen (Hauptdokument §5.8), dann erscheint auf der Seite „Plugin-Verwaltung" des Verwaltungs-Backends ein direkter Einstieg.
- Eigenständige Bereitstellung: `spawn: []` + `address: "tcp://<Dienstname>:19021"`, im Container auf `0.0.0.0` lauschen.
- Wenn höhere Nebenläufigkeit benötigt wird, kannst du das Plugin auf ein dauerhaft laufendes Swoole / RoadRunner o. Ä. umstellen, aber **das Wire-Protokoll ändert sich nicht**; stelle lediglich sicher, dass:
  Frame-Schreiben seriell erfolgt, die Leseschleife nicht blockiert und Reverse-Aufrufe über `id`+`reverse` zugeordnet werden.
