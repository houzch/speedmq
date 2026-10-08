# SpeedMQ Guida allo sviluppo dei plugin in processo esterno —— Python

> **Destinatari**: sviluppatori che scrivono plugin in processo esterno (sidecar) per SpeedMQ in Python.
> **Leggi prima**: [Guida allo sviluppo dei plugin in processo esterno (sidecar)](plugin-development.md) —— copre il modello mentale, i campi di configurazione e la tabella completa del protocollo di rete;
> questo documento copre solo **come metterlo in pratica in Python**, con i passaggi e i risultati verificati in locale.
> **Progetto di esempio**: workspace `speedmq-plugin/python/sidecar_plugin.py` (sola libreria standard, zero dipendenze di terze parti).

---

## 1. Che aspetto ha quando gira

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tre punti chiave (facili da sbagliare, tienili a mente prima di tutto):

1. **Il tuo processo è il server**: ascolta su un indirizzo locale e attende che il kernel si connetta (`plugins.<nome>.sidecar.address`).
2. **La porta di servizio esterna è aperta dal kernel**: i client si connettono alla porta del kernel, e i byte ti vengono proxati (`protocols[].listeners`).
3. **`prefix` deve essere non vuoto**: il kernel decide "a chi va questa connessione" tramite lo sniffing del prefisso. Un `prefix` vuoto significa **non partecipa allo sniffing**,
   e la connessione non ti verrà ceduta nemmeno sul suo listener (verificato in pratica: la connessione viene chiusa immediatamente). La lunghezza del prefisso è ≤ 8 byte, ASCII.

---

## 2. Farlo girare in tre passi

### Primo passo: dichiarare il plugin nella configurazione

`speedmqd.json` (**la configurazione effettiva è JSON standard, non può contenere commenti**):

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

- `address` è **l'indirizzo a cui il kernel si connette** (il kernel è il client, il plugin è il server).
- `spawn` vuoto = il kernel si connette soltanto e non avvia, il processo lo gestisci tu (systemd / supervisor / compose).
- `prefix` **deve essere non vuoto**: i primi byte inviati dal client devono iniziare con esso (il kernel decide a chi va la connessione tramite lo sniffing del prefisso).
- `listeners` sono le porte esterne, aperte dal kernel (i client si connettono al kernel, non a te).
- Per un deployment tra container usa il **nome del servizio** in `address` (es. `tcp://py-sidecar:19001`), e il plugin deve ascoltare su `0.0.0.0`.

### Secondo passo: avviarlo

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Terzo passo: verificarlo

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Punti salienti dell'implementazione

### 3.1 Suddivisione in frame (l'unico livello a byte che devi scrivere correttamente da te)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

Il payload di un frame dati = `4 byte big-endian di stream id + byte grezzi`; il payload del piano di controllo è JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake e heartbeat

Dopo che il kernel si connette, **invia per primo Hello**, e tu devi rispondere con un frame `HelloAck`; il kernel verifica
`name == il nome del plugin nella configurazione` e `api_version == l'APIVersion del kernel` (attualmente `v1`).
Poi il kernel invia un `Ping` ogni 2s, e tu rispondi `Pong` (se non rispondi vieni considerato morto).

### 3.3 Stream logici

Arriva `kindOpen` → **rispondi prima `OpenAck`**, poi inizia a servire; arriva `kindData` → scrivilo di nuovo
come `kindData` così com'è (o dopo averlo interpretato secondo il tuo protocollo); quando la gestione termina → invia `kindClose`. Uno stream = una connessione client.

### 3.4 Chiamate inverse e ponte semantico del kernel

Le chiamate plugin → kernel passano per `kindCall` con `"reverse": true`, e il kernel risponde con `kindReply` sulla stessa connessione.
**L'ordine è importante**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **non si può omettere**: la superficie operativa kernel della connessione non ha identità prima dell'autenticazione, e un `session.open` diretto viene rifiutato
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). I parametri sono la risposta SASL estratta dal tuo protocollo:

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

Le consegne di consumo sono **respinte in avanti** dal kernel (`kindCall`, `method = "session.deliver"`); quando hai finito usa
`session.settle` per liquidarle (`ack` / `requeue` / `reject`, il numero di consegna è globalmente univoco e non richiede lo stream id).

### 3.5 Modello di concorrenza (versione Python)

| Ruolo | Thread |
| --- | --- |
| Ciclo di lettura dei frame | Uno per connessione kernel |
| Gestione degli stream | Uno per stream (quindi più connessioni client possono essere concorrenti) |
| Gestione delle chiamate dirette | Uno per chiamata |

**Da notare assolutamente**: il ciclo di lettura **non deve** attendere in modo sincrono la risposta di una chiamata inversa (si avrebbe un deadlock) —— le chiamate dirette (`session.deliver`)
vanno dispacciate su un thread separato, perché durante la gestione possono a loro volta avviare `session.settle`. La scrittura dei frame deve essere serializzata con un lock.

---

## 4. Lettura guidata del codice (progetto di esempio)

`speedmq-plugin/python/sidecar_plugin.py` è di circa 320 righe; funzioni chiave:

| Posizione | Scopo |
| --- | --- |
| `read_frame` / `Conn.send` | Lettura/scrittura dei frame (prefisso di lunghezza + kind) |
| `Conn.call` | Chiamata inversa: numero → invio → attesa della risposta (abbinata per `reverse=true` e `id`) |
| `Conn.serve` | Ciclo di lettura dei frame e dispatch |
| `Conn._handle_hello` | Verifica nome plugin/versione API e risponde con HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Ciclo di vita dello stream ed echo |
| `Conn._dispatch` | Gestione delle chiamate dirette: `session.deliver` (incluso settle), `stats` |
| `Conn._session_demo` | Autenticazione + dichiarazione coda + pubblicazione + consumo |

---

## 5. Verifica pratica (riprodotta in locale)

Ambiente: Windows + Python 3.12; il kernel gira in Docker (`speedmq:1.1.01`), il plugin gira sull'host,
e il kernel si connette ad esso con `tcp://host.docker.internal:19001`.

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

Percorsi coperti: **handshake → autenticazione → ponte semantico (dichiarazione/pubblicazione/consumo) → rinvio delle consegne → liquidazione → echo del flusso di byte**.

---

## 6. Note specifiche per Python

- **Non usare `time.sleep` per attendere gli heartbeat**: il ciclo di lettura è bloccante, affidati semplicemente al Ping del kernel per mantenere viva la connessione;
  se hai impostato un timeout di lettura sul socket, ricorda di trattare il timeout come "connessione terminata" (quando il kernel riceve `kill -9`, il socket potrebbe non chiudersi prontamente).
- **`json.dumps` aggiunge spazi per impostazione predefinita**: l'esempio usa `separators=(",", ":")` solo per log più leggibili; il protocollo in sé non lo richiede.
- **I byte sono base64**: `message.body` e `core.authenticate.response` in JSON sono stringhe base64,
  quindi non dimenticare `base64.b64encode/decode`.
- **La scrittura dei frame deve essere protetta da lock**: heartbeat, risposte e blocchi dati provengono da thread diversi, e scritture interlacciate corrompono l'intera connessione (l'esempio usa `threading.Lock`).
- Funziona anche `asyncio`, ma devi garantire "scritture serializzate + ciclo di lettura non bloccante"; l'approccio è lo stesso della versione a thread.

---

## 7. Argomenti avanzati

- Per dare al plugin una propria interfaccia di gestione: aggiungi `console_url` nella configurazione, e nella pagina "Gestione plugin" della console di gestione comparirà una voce diretta
  (vedi il documento principale §5.8).
- Per rendere il plugin un servizio autonomo gestito da systemd / K8s: `spawn: []` + `restart: "never"`, avviato dall'esterno.
- Per far coesistere più protocolli: fai usare a più plugin `prefix` diversi sullo stesso listener, oppure apri una porta dedicata per ciascuno.
