# SwiftMQ Guida allo sviluppo dei plugin in processo esterno —— PHP

> **Destinatari**: sviluppatori che scrivono plugin in processo esterno (sidecar) per SwiftMQ in PHP.
> **Leggi prima**: [Guida allo sviluppo dei plugin in processo esterno (sidecar)](plugin-development.md) (modello mentale / campi di configurazione / tabella completa del protocollo di rete).
> **Progetto di esempio**: workspace `swiftmq-plugin/php/sidecar_plugin.php` (sola libreria standard, **nessuna dipendenza composer**).

---

## 1. Che aspetto ha quando gira

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tre punti chiave: **il tuo processo è il server** (attende che il kernel si connetta); **la porta esterna è aperta dal kernel** (`protocols[].listeners`);
**`prefix` deve essere non vuoto** (un prefisso vuoto = non partecipa allo sniffing, e la connessione non ti verrà ceduta; verificato in pratica viene chiusa immediatamente, ≤8 byte ASCII).

---

## 2. Farlo girare in tre passi

### Primo passo: configurazione

`swiftmqd.json` (**la configurazione effettiva è JSON standard, non può contenere commenti**):

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

### Secondo passo: avviarlo

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Terzo passo: verificarlo

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

In PHP si usano `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Il payload di un frame dati = `pack('N', $streamId) . byte grezzi`.

### 3.2 Handshake e heartbeat

Il kernel **invia per primo Hello**, e tu rispondi `HelloAck`; il kernel verifica `name` e `api_version` (attualmente `v1`).
Poi c'è un `Ping` ogni 2s, e tu rispondi `Pong`.

### 3.3 Modello di concorrenza: una pompa di frame rientrante (PHP non ha thread)

La CLI di PHP è a thread singolo e bloccante, quindi invece di "un thread per stream" qui si usa:

- Il **ciclo di lettura** (`serve()`) gestisce l'handshake, l'heartbeat, l'apertura degli stream, l'echo dei dati e la gestione delle chiamate dirette;
- L'**echo** non necessita di una macchina a stati aggiuntiva: alla ricezione di `kindData` lo si riscrive immediatamente come `kindData`;
- Le **chiamate inverse** usano `callAndWait()`: dopo aver inviato `kindCall`, legge e dispaccia i frame
  finché non legge **la propria** risposta (`reverse=true` e `id` corrispondente), poi ritorna.

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

Questo significa che `dispatchOther()` **deve essere rientrante**: può essere richiamato di nuovo dentro un `callAndWait`
(ad esempio, gestire `session.deliver` richiede a sua volta `session.settle`). È esattamente ciò che fa l'esempio.

### 3.4 Ponte semantico (prima si deve autenticare)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` non si può omettere: la superficie operativa kernel della connessione non ha identità prima dell'autenticazione, e un `session.open` diretto viene rifiutato
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

Le consegne sono **respinte in avanti** dal kernel (`method = "session.deliver"`); dopo la gestione `session.settle`
(`ack` / `requeue` / `reject`; il numero di consegna è globalmente univoco e non porta lo stream id).

---

## 4. Lettura guidata del codice (progetto di esempio)

`swiftmq-plugin/php/sidecar_plugin.php` è di circa 320 righe:

| Posizione | Scopo |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Lettura/scrittura dei frame |
| `Conn::serve()` | Ciclo di lettura principale |
| `Conn::dispatchOther()` | Dispatch dei frame non di handshake (rientrante) |
| `Conn::callAndWait()` | Chiamata inversa (pompa di frame rientrante) |
| `Conn::handleHello()` | Verifica e risposta con HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Chiamate dirette (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Autenticazione + dichiarazione + pubblicazione + consumo |

---

## 5. Verifica pratica (riprodotta in locale)

Windows + PHP 7.4; il kernel in Docker (`swiftmq:1.1.01`), il plugin sull'host (`tcp://host.docker.internal:19021`).

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

Coperto: **handshake → autenticazione → ponte semantico → rinvio delle consegne → liquidazione → echo del flusso di byte**.

---

## 6. Note specifiche per PHP

- **PHP 7.4 non ha il tipo di ritorno `mixed`** (arrivato con PHP 8.0): nell'esempio le chiamate inverse restituiscono "qualsiasi tipo",
  quindi **non viene scritta alcuna dichiarazione di tipo di ritorno** (si usa un commento `@return mixed`). Scrivere `: mixed` su 7.4 è un errore di sintassi diretto.
- **Tipi numerici JSON**: `json_decode($s, true)` decodifica gli interi come `int` per impostazione predefinita, e gli interi grandi possono diventare `float`;
  i numeri di consegna vanno bene a questa scala d'esempio, ma se i tuoi numeri sono molto grandi considera `JSON_BIGINT_AS_STRING`.
- **base64 è obbligatorio**: `message.body` e `core.authenticate.response` in JSON sono stringhe base64
  (`base64_encode` / `base64_decode($s, true)`).
- **Non usare `pcntl_fork` per la concorrenza**: su Windows non esiste pcntl, e il fork romperebbe l'assunzione "un solo scrittore per connessione";
  thread singolo + una pompa di frame rientrante è già sufficiente (a meno che tu non debba fare calcoli molto pesanti su uno stream, che è meglio collocare in un servizio esterno).
- **`stream_socket_accept` è bloccante**: il ciclo di vita del processo è gestito dal kernel (`spawn`) o da un supervisor;
  ricorda di gestire il `fread` che restituisce `''` (EOF) → termina quella connessione e torna ad accept.
- **Buffering dell'output**: registra i log con `fwrite(STDOUT, …)` seguito da un newline, così il kernel può inoltrarli riga per riga nel log del kernel.

---

## 7. Argomenti avanzati

- Plugin con interfaccia di gestione propria: aggiungi `console_url` nella configurazione (documento principale §5.8), e nella pagina "Gestione plugin" della console di gestione comparirà una voce diretta.
- Deployment autonomo: `spawn: []` + `address: "tcp://<nome del servizio>:19021"`, ascolto su `0.0.0.0` dentro il container.
- Quando serve maggiore concorrenza, puoi trasformare il plugin in qualcosa come Swoole / RoadRunner a lunga esecuzione, ma il **protocollo di rete resta invariato**; devi solo garantire:
  scritture dei frame serializzate, ciclo di lettura non bloccante, chiamate inverse abbinate per `id` + `reverse`.
