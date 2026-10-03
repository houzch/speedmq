# Ontwikkelgids voor SwiftMQ externe-procesplugins —— Java

> **Doelgroep**: ontwikkelaars die met Java externe-procesplugins (sidecar) voor SwiftMQ schrijven.
> **Eerst lezen**: [Ontwikkelgids voor externe-procesplugins (sidecar)](plugin-development.md) (mentaal model / configuratievelden / wire-protocoltabel).
> **Voorbeeldproject**: workspace `swiftmq-plugin/java/SidecarPlugin.java` (één bestand, alleen de JDK-standaardbibliotheek, geen Maven/Gradle nodig).

---

## 1. Hoe het eruitziet als het draait

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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

### Stap twee: compileren en starten

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Stap drie: verifiëren

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

In Java zijn `DataInputStream`/`DataOutputStream` het eenvoudigst — hun `readInt`/`writeInt` zijn **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

De schrijfzijde moet **geserialiseerd** zijn (heartbeat, antwoorden en datablokken komen uit verschillende threads):

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

De payload van een data-frame = `4 bytes big-endian streamnummer + ruwe bytes`.

### 3.2 Handshake en heartbeat

De kernel **stuurt eerst een Hello**, jij antwoordt met `HelloAck`; de kernel valideert `name` en `api_version` (momenteel `v1`) en verbindt dan.
Daarna elke 2s een `Ping`, antwoord met `Pong`.

### 3.3 Concurrencymodel (Java-versie)

| Rol | Thread |
| --- | --- |
| Frameread-lus | één per kernelverbinding |
| Streamverwerking | één per stream (meerdere clientverbindingen kunnen parallel) |
| Afhandeling van forward calls | één per aanroep |

**In de read-lus mag je niet synchroon op een antwoord van een omgekeerde aanroep wachten** (dat zou deadlocken): de verwerking van `session.deliver` moet naar een aparte thread,
omdat deze intern weer `session.settle` doet (nog een omgekeerde aanroep). Het voorbeeld doet het zo.

### 3.4 Semantiekbrug (eerst authenticeren)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` kan niet worden overgeslagen: het kernel-operatievlak van de verbinding heeft vóór authenticatie geen identiteit, en een directe `session.open` wordt geweigerd
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

Bezorgingen worden door de kernel **in voorwaartse richting teruggeduwd** (`method = "session.deliver"`); na verwerking `session.settle`
(`ack` / `requeue` / `reject`; het bezorgingsnummer is globaal uniek en heeft geen streamnummer).

---

## 4. Codewandeling (voorbeeldproject)

`swiftmq-plugin/java/SidecarPlugin.java` is ongeveer 470 regels (inclusief minimale JSON):

| Locatie | Functie |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Framing lezen/schrijven (`DataInputStream` + schrijflock) |
| `Conn.serve()` | Frameread-lus en dispatch |
| `Conn.call()` | Omgekeerde aanroep (`pending`-tabel + blokkerende wachtrij, met timeout-beveiliging) |
| `Conn.handleHello()` | Valideren en HelloAck terugsturen |
| `StreamState` | Lees-zijde van een stream (`BlockingQueue`, `STREAM_END` betekent einde) |
| `Conn.handleForwardCall()` / `handleMethod()` | Forward calls (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Auth + declareren + publiceren + consumeren |
| `Json` (einde van het bestand) | Minimale JSON-lezer/schrijver, alleen om het voorbeeld zonder afhankelijkheden te houden |

> **Productie-advies**: vervang `Json` door je gebruikelijke library (Jackson / Gson), of gebruik een bestaande stack buiten `java.net.http` —
> dat staat los van wat dit voorbeeld wil uitleggen (het wire-protocol).

---

## 5. Meting (lokaal gereproduceerd)

Windows + JDK 25; de kernel draait in Docker (`swiftmq:1.1.01`), de plugin op de host (`tcp://host.docker.internal:19031`).

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

Bestreken: **handshake → auth → semantiekbrug → bezorging terugduwen → settle → bytestroom-echo**.

---

## 6. Java-specifieke aandachtspunten

- **`\uXXXX` in de broncode wordt door de compiler op elke plek verwerkt** (ook in commentaar!). In de commentaren van het voorbeeld is daarom bewust
  "NUL + gebruikersnaam + NUL + wachtwoord" geschreven in plaats van direct `\u0000`, anders meldt javac een illegaal teken.
- **Chinese broncode vereist `javac -encoding UTF-8`**, anders treedt op de Windows-standaard (GBK) de fout "unmappable character for encoding GBK" op.
  Voeg bij het draaien `-Dfile.encoding=UTF-8` toe als Chinese tekst correct moet worden afgedrukt.
- **Lokale variabelen die door een lambda worden vastgelegd, moeten effectively final zijn**: in het voorbeeld wordt `name` tijdens het parsen van de argumenten opnieuw toegewezen,
  dus binnen de lambda wordt `opts.name` gebruikt (een veld dat slechts één keer wordt toegewezen).
- **`DataInputStream` is blokkerend**: bij een verbroken verbinding wordt `EOFException`/`IOException` gegooid; rond daarmee af.
- **base64**: `message.body` en `core.authenticate.response` zijn in JSON base64-strings
  (`Base64.getEncoder()/getDecoder()`).
- **De JDK-standaardbibliotheek heeft geen JSON**: het voorbeeld levert een minimale implementatie mee; `Json.parse` decodeert gehele getallen als `Long` en floats als `Double`,
  dus gebruik bij het ophalen van `id` `((Number) m.get("id")).longValue()`.

---

## 7. Gevorderd

- Verpak het als een uitvoerbare jar (`Main-Class: SidecarPlugin`) of een `jlink`-gekrompen runtime,
  en wijzig `spawn` dan in `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`.
- Plugin met eigen beheer-UI: voeg `console_url` toe in de configuratie (hoofddocument §5.8), dan verschijnt op de pagina "Pluginbeheer" van de beheerconsole een directe ingang.
- Losstaande uitrol: `spawn: []` + `address: "tcp://<servicenaam>:19031"`, in de container luisteren op `0.0.0.0`.
