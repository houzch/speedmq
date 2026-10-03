# Ontwikkelgids voor SwiftMQ externe-procesplugins —— PHP

> **Doelgroep**: ontwikkelaars die met PHP externe-procesplugins (sidecar) voor SwiftMQ schrijven.
> **Eerst lezen**: [Ontwikkelgids voor externe-procesplugins (sidecar)](plugin-development.md) (mentaal model / configuratievelden / wire-protocoltabel).
> **Voorbeeldproject**: workspace `swiftmq-plugin/php/sidecar_plugin.php` (alleen de standaardbibliotheek, **geen composer-afhankelijkheden**).

---

## 1. Hoe het eruitziet als het draait

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Drie kernpunten: **je proces is de server** (wacht tot de kernel verbinding maakt); **de externe poort wordt door de kernel geopend** (`protocols[].listeners`);
**`prefix` moet niet leeg zijn** (een lege prefix = niet deelnemen aan sniffing, de verbinding wordt niet aan jou gegeven; empirisch wordt deze onmiddellijk verbroken, ≤ 8 bytes ASCII).

---

## 2. In drie stappen draaien

### Stap één: configuratie

`swiftmqd.json` (**de feitelijke configuratie is standaard JSON en mag geen commentaar bevatten**):

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

### Stap twee: starten

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Stap drie: verifiëren

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP gebruikt `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

De payload van een data-frame = `pack('N', $streamId) . ruwe bytes`.

### 3.2 Handshake en heartbeat

De kernel **stuurt eerst een Hello**, jij antwoordt met `HelloAck`; de kernel valideert `name` en `api_version` (momenteel `v1`).
Daarna elke 2s een `Ping`, antwoord met `Pong`.

### 3.3 Concurrencymodel: een herintreedbare framepomp (PHP heeft geen threads)

PHP CLI is single-threaded en blokkerend, dus hier gebruiken we niet "één thread per stream", maar:

- **De read-lus** (`serve()`) regelt de handshake, heartbeat, streams openen, data echoën en forward calls afhandelen;
- **Echo** heeft geen extra toestandsmachine nodig: bij ontvangst van `kindData` wordt het direct ongewijzigd teruggeschreven als `kindData`;
- **Omgekeerde aanroepen** gebruiken `callAndWait()`: na het verzenden van `kindCall` worden al lezend frames gedispatcht,
  totdat **het eigen** antwoord wordt gelezen (`reverse=true` en `id` komt overeen), en pas dan wordt teruggekeerd.

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

Dit betekent dat `dispatchOther()` **herintreedbaar moet zijn**: het kan binnen één `callAndWait` opnieuw worden aangeroepen
(bijvoorbeeld: bij het verwerken van `session.deliver` is weer `session.settle` nodig). Het voorbeeld doet het zo.

### 3.4 Semantiekbrug (eerst authenticeren)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` kan niet worden overgeslagen: het kernel-operatievlak van de verbinding heeft vóór authenticatie geen identiteit, en een directe `session.open` wordt geweigerd
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

Bezorgingen worden door de kernel **in voorwaartse richting teruggeduwd** (`method = "session.deliver"`); na verwerking `session.settle`
(`ack` / `requeue` / `reject`; het bezorgingsnummer is globaal uniek en heeft geen streamnummer).

---

## 4. Codewandeling (voorbeeldproject)

`swiftmq-plugin/php/sidecar_plugin.php` is ongeveer 320 regels:

| Locatie | Functie |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Framing lezen/schrijven |
| `Conn::serve()` | Hoofd-read-lus |
| `Conn::dispatchOther()` | Dispatcht niet-handshake-frames (herintreedbaar) |
| `Conn::callAndWait()` | Omgekeerde aanroep (herintreedbare framepomp) |
| `Conn::handleHello()` | Valideren en HelloAck terugsturen |
| `Conn::handleForwardCall()` / `handleMethod()` | Forward calls (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Auth + declareren + publiceren + consumeren |

---

## 5. Meting (lokaal gereproduceerd)

Windows + PHP 7.4; de kernel draait in Docker (`swiftmq:1.1.01`), de plugin op de host (`tcp://host.docker.internal:19021`).

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

Bestreken: **handshake → auth → semantiekbrug → bezorging terugduwen → settle → bytestroom-echo**.

---

## 6. PHP-specifieke aandachtspunten

- **PHP 7.4 heeft geen `mixed`-retourtype** (dat bestaat pas in PHP 8.0): in het voorbeeld retourneert een omgekeerde aanroep "elk type",
  dus **wordt er geen retourtypedeclaratie geschreven** (een `@return mixed`-commentaar wordt gebruikt). Op 7.4 geeft `: mixed` direct een syntaxfout.
- **JSON-numerieke typen**: `json_decode($s, true)` decodeert gehele getallen standaard als `int`, maar grote gehele getallen kunnen `float` worden;
  het bezorgingsnummer is bij de schaal van dit voorbeeld geen probleem, maar als je nummers groot zijn, overweeg dan `JSON_BIGINT_AS_STRING`.
- **base64 is verplicht**: `message.body` en `core.authenticate.response` zijn in JSON base64-strings
  (`base64_encode` / `base64_decode($s, true)`).
- **Gebruik `pcntl_fork` niet voor concurrency**: op Windows is er geen pcntl, en fork zou de aanname "één schrijver per verbinding" breken;
  single-threaded + een herintreedbare framepomp is al voldoende (tenzij je erg zware berekeningen op de stream wilt doen, dan past dat beter in een externe service).
- **`stream_socket_accept` is blokkerend**: de proceslevenscyclus wordt beheerd door de kernel (`spawn`) of een supervisor;
  vergeet niet `fread` die `''` (EOF) retourneert af te handelen → beëindig die verbinding en keer terug naar accept.
- **Uitvoerbuffering**: gebruik voor logs `fwrite(STDOUT, …)` met een newline erachter, zodat de kernel ze regel voor regel naar het kernellog kan doorsturen.

---

## 7. Gevorderd

- Plugin met eigen beheer-UI: voeg `console_url` toe in de configuratie (hoofddocument §5.8), dan verschijnt op de pagina "Pluginbeheer" van de beheerconsole een directe ingang.
- Losstaande uitrol: `spawn: []` + `address: "tcp://<servicenaam>:19021"`, in de container luisteren op `0.0.0.0`.
- Heb je hogere concurrency nodig, dan kun je de plugin omzetten naar iets als een permanent draaiende Swoole / RoadRunner, maar **het wire-protocol verandert niet**; zorg alleen dat:
  frames geserialiseerd worden geschreven, de read-lus niet blokkeert, en omgekeerde aanroepen op `id` + `reverse` matchen.
