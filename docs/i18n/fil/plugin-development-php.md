# Gabay sa Pagbuo ng External-Process Plugin ng SpeedMQ —— PHP

> **Para sa**: mga developer na magsusulat ng external-process plugin (sidecar) para sa SpeedMQ gamit ang PHP.
> **Basahin muna**: [Gabay sa Pagbuo ng External-Process Plugin (sidecar)](plugin-development.md) (mental model / config field / pangkalahatang talahanayan ng wire protocol).
> **Halimbawang proyekto**: workspace na `speedmq-plugin/php/sidecar_plugin.php` (standard library lamang, **walang kailangang composer dependency**).

---

## 1. Ano ang Hitsura Kapag Tumakbo Ito

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tatlong pangunahing punto: **ang iyong process ay server** (naghihintay na kumonekta ang kernel); **ang panlabas na port ay binubuksan ng kernel** (`protocols[].listeners`);
**dapat hindi laman ang `prefix`** (walang laman na prefix = hindi kasali sa sniffing, hindi ibibigay sa iyo ang koneksyon; aktwal na agad itong isasara, ≤8 bytes ASCII).

---

## 2. Tatlong Hakbang para Mapatakbo

### Unang hakbang: config

`speedmqd.json` (**ang aktwal na config ay standard JSON, hindi maaaring maglaman ng comment**):

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/speedmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### Ikalawang hakbang: startup

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Ikatlong hakbang: verification

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

Gumamit ng `pack`/`unpack` ang PHP:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Ang payload ng data frame = `pack('N', $streamId) . raw bytes`.

### 3.2 Handshake at heartbeat

**Nagpapadala muna ng Hello ang kernel**, at magbalik ka ng `HelloAck`; iva-validate ng kernel ang `name` at `api_version` (kasalukuyang `v1`).
Pagkatapos, isang `Ping` bawat 2s, at magbalik ng `Pong`.

### 3.3 Concurrency model: reentrant na frame pump (walang thread ang PHP)

Ang PHP CLI ay single-threaded na blocking, kaya hindi ginagamit dito ang "isang thread sa bawat stream", kundi:

- Ang **read loop** (`serve()`) ang responsable sa handshake, heartbeat, pagbukas ng stream, pag-echo ng data, at pag-handle ng forward call;
- Ang **echo** ay hindi nangangailangan ng dagdag na state machine: kapag natanggap ang `kindData`, agad itong isinusulat pabalik nang gayon din sa `kindData`;
- Ang **reverse call** ay gumagamit ng `callAndWait()`: pagkatapos magpadala ng `kindCall`, sabay itong nagbabasa ng frame at nagdi-dispatch,
  hanggang sa mabasa ang **sariling** reply (`reverse=true` at tumutugmang `id`) saka lamang ito nagbabalik.

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

Ibig sabihin nito, ang `dispatchOther()` ay **dapat reentrant**: maaaring muli itong tawagin sa loob ng isang `callAndWait`
(halimbawa, kapag nagpoproseso ng `session.deliver` ay kailangan pang `session.settle`). Ito ang ginagawa ng halimbawa.

### 3.4 Semantic bridge (dapat munang mag-authenticate)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Ang `core.authenticate` ay hindi maaaring laktawan: walang identity ang kernel operation surface ng koneksyon bago ang authentication, at ang direktang `session.open` ay tatanggihan
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

Ang delivery ay **itutulak pabalik nang forward** ng kernel (`method = "session.deliver"`), at pagkatapos iproseso ay `session.settle`
(`ack` / `requeue` / `reject`; globally unique ang delivery ID, walang stream number).

---

## 4. Code Walkthrough (halimbawang proyekto)

Ang `speedmq-plugin/php/sidecar_plugin.php` ay humigit-kumulang 320 linya:

| Lokasyon | Tungkulin |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Framing read/write |
| `Conn::serve()` | Pangunahing read loop |
| `Conn::dispatchOther()` | Dispatch ng non-handshake frame (reentrant) |
| `Conn::callAndWait()` | Reverse call (reentrant na frame pump) |
| `Conn::handleHello()` | I-validate at magbalik ng HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Forward call (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Auth + declare + publish + consume |

---

## 5. Aktwal na Pagsusubok (na-reproduce sa makinang ito)

Windows + PHP 7.4; kernel sa Docker (`speedmq:1.1.01`), plugin sa host (`tcp://host.docker.internal:19021`).

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

Nasakop: **handshake → auth → semantic bridge → pag-push pabalik ng delivery → settle → byte stream echo**.

---

## 6. Mga Puntong Dapat Tandaan na Espesipiko sa PHP

- **Walang `mixed` return type ang PHP 7.4** (nasa PHP 8.0 pa lamang ito): sa halimbawa, ang reverse call ay nagbabalik ng "anumang uri",
  kaya **hindi naglalagay ng return type declaration** (gumagamit ng `@return mixed` comment). Ang pagsulat ng `: mixed` sa 7.4 ay direktang syntax error.
- **Ang numeric type ng JSON**: ang `json_decode($s, true)` ay bilang default na nagde-decode ng integer bilang `int`, at ang malaking integer ay maaaring maging `float`;
  walang problema ang delivery ID sa sukat ng halimbawang ito, ngunit kung napakalaki ng iyong ID, isaalang-alang ang `JSON_BIGINT_AS_STRING`.
- **Kailangan ang base64**: ang `message.body` at `core.authenticate.response` ay base64 string sa JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **Huwag gumamit ng `pcntl_fork` para sa concurrency**: walang pcntl sa Windows, at sisira ang fork sa assumption na "isang writer sa isang koneksyon";
  sapat na ang single thread + reentrant frame pump (maliban kung may napakabigat na computation sa stream, na mas angkop ilagay sa external service).
- **Blocking ang `stream_socket_accept`**: ang lifecycle ng process ay pinamamahalaan ng kernel (`spawn`) o supervisor;
  tandaan na hawakan ang `fread` na nagbabalik ng `''` (EOF) → tapusin ang koneksyong iyon at bumalik sa accept.
- **Output buffering**: gumamit ng `fwrite(STDOUT, …)` para sa log at sundan ng newline, para madaling maipasa ng kernel nang linya-linya sa kernel log.

---

## 7. Advanced

- Sariling admin UI ng plugin: magdagdag ng `console_url` sa config (pangunahing dokumento §5.8), at lalabas ang direktang entry sa "Plugin Management" page ng admin console.
- Standalone deployment: `spawn: []` + `address: "tcp://<pangalan ng serbisyo>:19021"`, nakikinig sa `0.0.0.0` sa loob ng container.
- Kapag kailangan ng mas mataas na concurrency, maaaring gawing residente ang plugin tulad ng Swoole / RoadRunner, ngunit **hindi nagbabago ang wire protocol**; siguraduhin lamang:
  serial ang pagsulat ng frame, hindi blocking ang read loop, at tumutugma ang reverse call ayon sa `id`+`reverse`.
