# Ontwikkelgids voor SwiftMQ externe-procesplugins —— Python

> **Doelgroep**: ontwikkelaars die met Python externe-procesplugins (sidecar) voor SwiftMQ schrijven.
> **Eerst lezen**: [Ontwikkelgids voor externe-procesplugins (sidecar)](plugin-development.md) — daar worden het mentale model, de configuratievelden en de wire-protocoltabel behandeld;
> dit document behandelt alleen **hoe je het in Python realiseert**, plus de op deze machine empirisch geteste stappen en resultaten.
> **Voorbeeldproject**: workspace `swiftmq-plugin/python/sidecar_plugin.py` (alleen standaardbibliotheek, geen externe afhankelijkheden).

---

## 1. Hoe het eruitziet als het draait

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Drie kernpunten (gemakkelijk fout te doen, onthoud ze goed):

1. **Je proces is de server**: het luistert op een lokaal adres en wacht tot de kernel verbinding maakt (`plugins.<naam>.sidecar.address`).
2. **De externe bedrijfspoort wordt door de kernel geopend**: clients verbinden met de poort van de kernel, en de bytes worden naar jou geproxyd (`protocols[].listeners`).
3. **`prefix` moet niet leeg zijn**: de kernel bepaalt via prefix-sniffing "aan wie deze verbinding wordt gegeven". Een lege `prefix` betekent **niet deelnemen aan sniffing**,
   en dan wordt de verbinding ook op zijn eigen listener niet aan jou gegeven (empirisch: de verbinding wordt onmiddellijk verbroken). De prefixlengte is ≤ 8 bytes, ASCII.

---

## 2. In drie stappen draaien

### Stap één: de plugin in de configuratie declareren

`swiftmqd.json` (**de feitelijke configuratie is standaard JSON en mag geen commentaar bevatten**):

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` is **het adres waarmee de kernel verbinding met je maakt** (de kernel is de client, de plugin is de server).
- `spawn` leeg laten = de kernel maakt alleen verbinding en start niet, het proces beheer je zelf (systemd / supervisor / compose).
- `prefix` **moet niet leeg zijn**: de eerste bytes die de client stuurt moeten ermee beginnen (de kernel bepaalt via prefix-sniffing aan wie de verbinding wordt gegeven).
- `listeners` zijn de externe poorten, door de kernel geopend (clients verbinden met de kernel, niet met jou).
- Bij uitrol in aparte containers gebruik je voor `address` een **servicenaam** (zoals `tcp://py-sidecar:19001`), en de plugin moet op `0.0.0.0` luisteren.

### Stap twee: starten

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Stap drie: verifiëren

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Implementatiepunten

### 3.1 Framing (het enige byte-niveau dat je zelf correct moet schrijven)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

De payload van een data-frame = `4 bytes big-endian streamnummer + ruwe bytes`; de payload van het besturingsvlak is JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake en heartbeat

Nadat de kernel verbinding heeft gemaakt, stuurt deze **eerst een Hello**, en jij moet één `HelloAck`-frame terugsturen; de kernel valideert
`name == de pluginnaam in de config` en `api_version == de APIVersion van de kernel` (momenteel `v1`).
Daarna stuurt de kernel elke 2s een `Ping`, en jij antwoordt met `Pong` (niet antwoorden wordt als dood beschouwd).

### 3.3 Logische stream

`kindOpen` arriveert → **stuur eerst `OpenAck`**, begin dan met bedienen; `kindData` arriveert → schrijf het ongewijzigd (of na parsing volgens je eigen protocol) terug als
`kindData`; verwerking klaar → stuur `kindClose`. Eén stream = één clientverbinding.

### 3.4 Omgekeerde aanroepen en de kernelsemantiekbrug

Aanroepen van plugin → kernel gaan via `kindCall` met `"reverse": true`, en de kernel antwoordt met `kindReply` op dezelfde verbinding.
**De volgorde is belangrijk**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **kan niet worden overgeslagen**: het kernel-operatievlak van de verbinding heeft vóór authenticatie geen identiteit, en een directe `session.open` wordt geweigerd
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). De parameter is de SASL-respons die uit je protocol is geparseerd:

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

Consumptiebezorgingen worden door de kernel **in voorwaartse richting teruggeduwd** (`kindCall`, `method = "session.deliver"`); na verwerking settle je met
`session.settle` (`ack` / `requeue` / `reject`; het bezorgingsnummer is globaal uniek en heeft geen streamnummer nodig).

### 3.5 Concurrencymodel (Python-versie)

| Rol | Thread |
| --- | --- |
| Frameread-lus | één per kernelverbinding |
| Streamverwerking | één per stream (meerdere clientverbindingen kunnen dus parallel) |
| Afhandeling van forward calls | één per aanroep |

**Let op**: in de read-lus **mag je niet** synchroon op een antwoord van een omgekeerde aanroep wachten (dat zou deadlocken) — een forward call (`session.deliver`)
moet naar een aparte thread worden gestuurd, omdat deze tijdens de verwerking mogelijk weer `session.settle` moet initiëren. Het schrijven van frames moet met een lock worden geserialiseerd.

---

## 4. Codewandeling (voorbeeldproject)

`swiftmq-plugin/python/sidecar_plugin.py` is ongeveer 320 regels; belangrijkste functies:

| Locatie | Functie |
| --- | --- |
| `read_frame` / `Conn.send` | Framing lezen/schrijven (lengteprefix + kind) |
| `Conn.call` | Omgekeerde aanroep: nummer → verzenden → op antwoord wachten (match op `reverse=true` en `id`) |
| `Conn.serve` | Frameread-lus en dispatch |
| `Conn._handle_hello` | Valideert pluginnaam/API-versie en stuurt HelloAck terug |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Streamlevenscyclus en echo |
| `Conn._dispatch` | Verwerkt forward calls: `session.deliver` (inclusief settle), `stats` |
| `Conn._session_demo` | Auth + wachtrij declareren + publiceren + consumeren |

---

## 5. Meting (lokaal gereproduceerd)

Omgeving: Windows + Python 3.12; de kernel draait in Docker (`swiftmq:1.1.01`), de plugin draait op de host,
en de kernel maakt verbinding via `tcp://host.docker.internal:19001`.

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

Bestreken keten: **handshake → auth → semantiekbrug (declareren/publiceren/consumeren) → bezorging terugduwen → settle → bytestroom-echo**.

---

## 6. Python-specifieke aandachtspunten

- **Gebruik geen `time.sleep` om op de heartbeat te wachten**: de read-lus is blokkerend; laat de verbinding in stand houden door de Ping van de kernel;
  als je een read-timeout op de socket instelt, behandel de timeout dan als "einde van de verbinding" (bij `kill -9` van de kernel wordt de socket mogelijk niet tijdig gesloten).
- **`json.dumps` voegt standaard spaties toe**: het voorbeeld gebruikt `separators=(",", ":")` alleen voor een mooiere log; het protocol zelf vereist dit niet.
- **Bytes zijn base64**: `message.body` en `core.authenticate.response` zijn in JSON base64-strings,
  vergeet `base64.b64encode/decode` niet.
- **Het schrijven van frames moet met een lock**: heartbeat, antwoorden en datablokken komen uit verschillende threads; interleaved schrijven vervuilt de hele verbinding (het voorbeeld gebruikt `threading.Lock`).
- Met `asyncio` kan het ook, maar zorg dat "schrijven geserialiseerd + read-lus niet blokkerend" blijft; de aanpak is dezelfde als de threadversie.

---

## 7. Gevorderd

- Wil je de plugin een eigen beheer-UI geven: voeg `console_url` toe in de configuratie, dan verschijnt op de pagina "Pluginbeheer" van de beheerconsole een directe ingang
  (zie hoofddocument §5.8).
- Maak je de plugin tot een losstaande service, beheerd door systemd / K8s: `spawn: []` + `restart: "never"`, extern gestart.
- Meerdere protocollen naast elkaar: geef meerdere plugins verschillende `prefix` op dezelfde listener, of open elk een toegewijde poort.
