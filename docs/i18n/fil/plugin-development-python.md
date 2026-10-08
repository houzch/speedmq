# Gabay sa Pagbuo ng External-Process Plugin ng SpeedMQ —— Python

> **Para sa**: mga developer na magsusulat ng external-process plugin (sidecar) para sa SpeedMQ gamit ang Python.
> **Basahin muna**: [Gabay sa Pagbuo ng External-Process Plugin (sidecar)](plugin-development.md) —— doon nakasaad ang mental model, config field, at pangkalahatang talahanayan ng wire protocol;
> tinatalakay lamang ng dokumentong ito ang **kung paano ipatupad sa Python**, at ang mga hakbang at resultang aktwal na na-test sa makinang ito.
> **Halimbawang proyekto**: workspace na `speedmq-plugin/python/sidecar_plugin.py` (standard library lamang, walang third-party dependency).

---

## 1. Ano ang Hitsura Kapag Tumakbo Ito

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tatlong pangunahing punto (madaling mapagkamalan, tandaan muna):

1. **Ang iyong process ay server**: nakikinig sa isang lokal na address, hinihintay na kumonekta ang kernel (`plugins.<pangalan ng plugin>.sidecar.address`).
2. **Ang panlabas na business port ay binubuksan ng kernel**: ang port ng kernel ang kinokonektahan ng client, at ang byte ay ni-proxy sa iyo (`protocols[].listeners`).
3. **Dapat hindi laman ang `prefix`**: nag-sniff ang kernel ayon sa prefix upang magpasya "kanino ibibigay ang koneksyong ito". Ang walang laman na `prefix` ay nangangahulugang **hindi kasali sa sniffing**,
   at hindi rin ito ibibigay sa iyo kahit sa sarili nitong listener (aktwal: agad na isasara ang koneksyon). Ang haba ng prefix ay ≤ 8 bytes, ASCII.

---

## 2. Tatlong Hakbang para Mapatakbo

### Unang hakbang: ideklara ang plugin sa config

`speedmqd.json` (**ang aktwal na config ay standard JSON, hindi maaaring maglaman ng comment**):

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

- Ang `address` ay **ang address na kinokonektahan ng kernel sa iyo** (kernel ang client, plugin ang server).
- Ang pag-iwan na walang laman sa `spawn` = konekta lamang ang kernel, hindi nag-la-launch, ikaw ang mamamahala ng process (systemd / supervisor / compose).
- Ang `prefix` ay **dapat hindi laman**: ang unang byte na ipinadala ng client ay dapat magsimula rito (nag-sniff ang kernel ayon sa prefix upang magpasya kung kanino ibibigay ang koneksyon).
- Ang `listeners` ay ang panlabas na port, binubuksan ng kernel (ang kernel ang kinokonektahan ng client, hindi ikaw).
- Sa cross-container deployment, gamitin ang **service name** para sa `address` (tulad ng `tcp://py-sidecar:19001`), at dapat nakikinig ang plugin sa `0.0.0.0`.

### Ikalawang hakbang: startup

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Ikatlong hakbang: verification

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Mga Punto ng Implementasyon

### 3.1 Framing (ang tanging byte layer na dapat mong isulat nang tama)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

Ang payload ng data frame = `4 na byte na big-endian na stream number + raw bytes`; ang control plane payload ay JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake at heartbeat

Pagkatapos kumonekta, **nagpapadala muna ang kernel ng Hello**, at dapat kang magbalik ng isang `HelloAck` frame; iva-validate ng kernel
na `name == ang plugin name sa config` at `api_version == APIVersion ng kernel` (kasalukuyang `v1`).
Pagkatapos, nagpapadala ang kernel ng `Ping` bawat 2s, at magbalik ka lamang ng `Pong` (kapag hindi ka nagbalik, ituturing kang patay).

### 3.3 Logical stream

Pagdating ng `kindOpen` → **magbalik muna ng `OpenAck`**, pagkatapos simulan ang serbisyo; pagdating ng `kindData` → isulat pabalik nang gayon din (o pagkatapos i-parse ayon sa iyong protocol) sa
`kindData`; pagkatapos ng pagproseso → magpadala ng `kindClose`. Isang stream = isang client connection.

### 3.4 Reverse call at kernel semantic bridge

Ang tawag mula plugin → kernel ay dumadaan sa `kindCall` na may `"reverse": true`, at ang kernel ay nagbabalik ng `kindReply` sa parehong koneksyon.
**Mahalaga ang pagkakasunod-sunod**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Ang `core.authenticate` ay **hindi maaaring laktawan**: walang identity ang kernel operation surface ng koneksyon bago ang authentication, at ang direktang `session.open` ay tatanggihan
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Ang params ay ang SASL response na na-parse mula sa iyong protocol:

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

Ang delivery ng consumer ay **itutulak pabalik nang forward** ng kernel (`kindCall`, `method = "session.deliver"`), at pagkatapos iproseso ay i-settle gamit ang
`session.settle` (`ack` / `requeue` / `reject`, globally unique ang delivery ID, hindi kailangan ang stream number).

### 3.5 Concurrency model (bersyon ng Python)

| Tungkulin | Thread |
| --- | --- |
| Frame read loop | Isa sa bawat kernel connection |
| Stream processing | Isa sa bawat stream (kaya maaaring concurrent ang maraming client connection) |
| Forward call processing | Isa sa bawat call |

**Dapat tandaan**: sa read loop ay **hindi maaaring** maghintay nang synchronously ng reverse call reply (magdudulot ng deadlock) —— ang forward call (`session.deliver`)
ay dapat itapon sa hiwalay na thread, dahil maaaring muling magsimula ito ng `session.settle` habang nagpoproseso. Ang pagsulat ng frame ay dapat may lock upang maging serial.

---

## 4. Code Walkthrough (halimbawang proyekto)

Ang `speedmq-plugin/python/sidecar_plugin.py` ay humigit-kumulang 320 linya, mga pangunahing function:

| Lokasyon | Tungkulin |
| --- | --- |
| `read_frame` / `Conn.send` | Framing read/write (length prefix + kind) |
| `Conn.call` | Reverse call: ID → send → hintayin ang reply (tumutugma ayon sa `reverse=true` at `id`) |
| `Conn.serve` | Frame read loop at dispatch |
| `Conn._handle_hello` | I-validate ang plugin name/API version at magbalik ng HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Stream lifecycle at echo |
| `Conn._dispatch` | I-handle ang forward call: `session.deliver` (may settle), `stats` |
| `Conn._session_demo` | Auth + declare queue + publish + consume |

---

## 5. Aktwal na Pagsusubok (na-reproduce sa makinang ito)

Environment: Windows + Python 3.12; tumatakbo ang kernel sa Docker (`speedmq:1.1.01`), tumatakbo ang plugin sa host,
at kinokonekta ito ng kernel gamit ang `tcp://host.docker.internal:19001`.

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

Mga nasakop na chain: **handshake → auth → semantic bridge (declare/publish/consume) → pag-push pabalik ng delivery → settle → byte stream echo**.

---

## 6. Mga Puntong Dapat Tandaan na Espesipiko sa Python

- **Huwag gamitin ang `time.sleep` para maghintay ng heartbeat**: blocking ang read loop, sapat na ang Ping ng kernel upang mapanatili ang koneksyon;
  kung nagtakda ka ng read timeout sa socket, tandaan na ituring ang timeout bilang "pagtatapos ng koneksyon" (kapag `kill -9` ang kernel, maaaring hindi agad isara ang socket).
- **Nagdadagdag ng space ang `json.dumps` bilang default**: ang paggamit ng `separators=(",", ":")` sa halimbawa ay para lamang maging maganda ang log, hindi ito kinakailangan ng protocol.
- **Ang byte ay base64**: ang `message.body` at `core.authenticate.response` ay base64 string sa JSON,
  huwag kalimutan ang `base64.b64encode/decode`.
- **Kailangang may lock ang pagsulat ng frame**: ang heartbeat, reply, at data block ay mula sa magkaibang thread, at ang interlaced na pagsulat ay masisira ang buong koneksyon (gumagamit ng `threading.Lock` ang halimbawa).
- Maaari ring gumamit ng `asyncio`, ngunit siguraduhing "serial ang pagsulat + hindi blocking ang read loop"; pareho ang pag-iisip sa thread version.

---

## 7. Advanced

- Gusto mong maglagay ng sariling admin UI para sa plugin: magdagdag ng `console_url` sa config, at lalabas ang direktang entry sa "Plugin Management" page ng admin console
  (tingnan ang pangunahing dokumento §5.8).
- Gawing standalone service ang plugin, pinamamahalaan ng systemd / K8s: `spawn: []` + `restart: "never"`, panlabas na mag-la-launch.
- Kailangan ng multi-protocol coexistence: hayaang gumamit ang maraming plugin ng magkaibang `prefix` sa parehong listener, o magbukas ng sariling dedikadong port ang bawat isa.
