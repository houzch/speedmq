# SwiftMQ Guida allo sviluppo dei plugin in processo esterno —— Java

> **Destinatari**: sviluppatori che scrivono plugin in processo esterno (sidecar) per SwiftMQ in Java.
> **Leggi prima**: [Guida allo sviluppo dei plugin in processo esterno (sidecar)](plugin-development.md) (modello mentale / campi di configurazione / tabella completa del protocollo di rete).
> **Progetto di esempio**: workspace `swiftmq-plugin/java/SidecarPlugin.java` (file singolo, sola libreria standard del JDK, senza Maven/Gradle).

---

## 1. Che aspetto ha quando gira

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/swiftmq/classes", "SidecarPlugin",
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

### Secondo passo: compilare e avviare

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Terzo passo: verificarlo

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

In Java è più comodo con `DataInputStream`/`DataOutputStream` —— i loro `readInt`/`writeInt` sono già **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Il lato scrittura deve essere **serializzato** (heartbeat, risposte e blocchi dati provengono da thread diversi):

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

Il payload di un frame dati = `4 byte big-endian di stream id + byte grezzi`.

### 3.2 Handshake e heartbeat

Il kernel **invia per primo Hello**, e tu rispondi `HelloAck`; il kernel verifica `name` e `api_version` (attualmente `v1`) e poi si connette.
Poi c'è un `Ping` ogni 2s, e tu rispondi `Pong`.

### 3.3 Modello di concorrenza (versione Java)

| Ruolo | Thread |
| --- | --- |
| Ciclo di lettura dei frame | Uno per connessione kernel |
| Gestione degli stream | Uno per stream (più connessioni client possono essere concorrenti) |
| Gestione delle chiamate dirette | Uno per chiamata |

**Il ciclo di lettura non deve attendere in modo sincrono la risposta di una chiamata inversa** (si avrebbe un deadlock): la gestione di `session.deliver` va dispacciata su un thread separato,
perché al suo interno necessita di `session.settle` (un'altra chiamata inversa). È ciò che fa l'esempio.

### 3.4 Ponte semantico (prima si deve autenticare)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` non si può omettere: la superficie operativa kernel della connessione non ha identità prima dell'autenticazione, e un `session.open` diretto viene rifiutato
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

Le consegne sono **respinte in avanti** dal kernel (`method = "session.deliver"`); dopo la gestione `session.settle`
(`ack` / `requeue` / `reject`; il numero di consegna è globalmente univoco e non porta lo stream id).

---

## 4. Lettura guidata del codice (progetto di esempio)

`swiftmq-plugin/java/SidecarPlugin.java` è di circa 470 righe (incluso un JSON minimale):

| Posizione | Scopo |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Lettura/scrittura dei frame (`DataInputStream` + lock di scrittura) |
| `Conn.serve()` | Ciclo di lettura dei frame e dispatch |
| `Conn.call()` | Chiamata inversa (tabella `pending` + coda bloccante, con protezione di timeout) |
| `Conn.handleHello()` | Verifica e risposta con HelloAck |
| `StreamState` | Lato lettura dello stream (`BlockingQueue`, `STREAM_END` indica la fine) |
| `Conn.handleForwardCall()` / `handleMethod()` | Chiamate dirette (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Autenticazione + dichiarazione + pubblicazione + consumo |
| `Json` (in fondo al file) | JSON minimale di lettura/scrittura, solo per rendere l'esempio senza dipendenze |

> **Consiglio per la produzione**: sostituisci `Json` con una libreria a te familiare (Jackson / Gson), oppure usa uno stack esistente diverso da `java.net.http` ——
> non ha nulla a che vedere con l'argomento di questo esempio (il protocollo di rete).

---

## 5. Verifica pratica (riprodotta in locale)

Windows + JDK 25; il kernel in Docker (`swiftmq:1.1.01`), il plugin sull'host (`tcp://host.docker.internal:19031`).

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

Coperto: **handshake → autenticazione → ponte semantico → rinvio delle consegne → liquidazione → echo del flusso di byte**.

---

## 6. Note specifiche per Java

- **I `\uXXXX` nel sorgente vengono elaborati dal compilatore in qualsiasi posizione** (commenti inclusi!). Nell'esempio il commento scrive deliberatamente
  "NUL + user + NUL + password" invece di `\u0000` diretto, altrimenti javac segnala un carattere illegale.
- **Il sorgente in cinese deve usare `javac -encoding UTF-8`**, altrimenti sul default di Windows (GBK) si ottiene "unmappable character for encoding GBK".
  Per stampare correttamente caratteri non ASCII a runtime, aggiungi `-Dfile.encoding=UTF-8`.
- **Le variabili locali catturate da una lambda devono essere effectively final**: nell'esempio `name` viene riassegnato durante il parsing degli argomenti,
  quindi la lambda usa `opts.name` (un campo assegnato una sola volta).
- **`DataInputStream` è bloccante**: quando la connessione cade solleva `EOFException`/`IOException`, con cui concludi l'elaborazione.
- **base64**: `message.body` e `core.authenticate.response` in JSON sono stringhe base64
  (`Base64.getEncoder()/getDecoder()`).
- **La libreria standard del JDK non ha JSON**: l'esempio include un'implementazione minimale; `Json.parse` decodifica gli interi come `Long` e i decimali come `Double`,
  e nel leggere `id` usa `((Number) m.get("id")).longValue()`.

---

## 7. Argomenti avanzati

- Impacchettalo in un jar eseguibile (`Main-Class: SidecarPlugin`) o in un runtime ridotto con `jlink`,
  poi cambia `spawn` in `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`.
- Plugin con interfaccia di gestione propria: aggiungi `console_url` nella configurazione (documento principale §5.8), e nella pagina "Gestione plugin" della console di gestione comparirà una voce diretta.
- Deployment autonomo: `spawn: []` + `address: "tcp://<nome del servizio>:19031"`, ascolto su `0.0.0.0` dentro il container.
