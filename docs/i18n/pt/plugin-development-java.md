# Guia de desenvolvimento de plugins de processo externo do SpeedMQ —— Java

> **Público-alvo**: desenvolvedores que escrevem plugins de processo externo (sidecar) para o SpeedMQ em Java.
> **Leia primeiro**: [Guia de desenvolvimento de plugins de processo externo (sidecar)](plugin-development.md) (modelo mental / campos de configuração / tabela geral do protocolo de linha).
> **Projeto de exemplo**: no workspace `speedmq-plugin/java/SidecarPlugin.java` (arquivo único, somente biblioteca padrão do JDK, sem Maven/Gradle).

---

## 1. Como fica quando roda

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Três pontos essenciais: **seu processo é o servidor** (espera o núcleo se conectar); **a porta externa é aberta pelo núcleo** (`protocols[].listeners`);
**`prefix` deve ser não vazio** (prefixo vazio = não participa da sondagem, a conexão não será entregue a você; na prática é derrubada imediatamente, ≤8 bytes ASCII).

---

## 2. Três passos para rodar

### Passo 1: configuração

`speedmqd.json` (**a configuração real é JSON padrão e não pode conter comentários**):

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

### Passo 2: compilar e iniciar

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Passo 3: verificar

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Pontos de implementação

### 3.1 Fragmentação

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

O Java com `DataInputStream`/`DataOutputStream` é o mais simples — seus `readInt`/`writeInt` já são **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

O lado de escrita deve ser **serializado** (heartbeat, respostas e blocos de dados vêm de threads diferentes):

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

Payload do frame de dados = `4 bytes de id de stream em big-endian + bytes brutos`.

### 3.2 Handshake e heartbeat

O núcleo **envia Hello primeiro**, você responde `HelloAck`; o núcleo valida `name` e `api_version` (atualmente `v1`) e então conecta.
Depois disso, um `Ping` a cada 2s, responda `Pong`.

### 3.3 Modelo de concorrência (versão Java)

| Papel | Thread |
| --- | --- |
| Loop de leitura de frames | Uma por conexão do núcleo |
| Tratamento de stream | Um por stream (várias conexões de cliente podem rodar em paralelo) |
| Tratamento de chamadas diretas | Uma por chamada |

**Não se pode aguardar de forma síncrona a resposta de uma chamada reversa dentro do loop de leitura** (causaria deadlock): o tratamento de `session.deliver` deve ser despachado para uma thread separada,
porque internamente ele ainda faz `session.settle` (outra chamada reversa). O exemplo faz assim.

### 3.4 Ponte semântica (é preciso autenticar primeiro)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` não pode ser omitido: a superfície de operação do núcleo da conexão não tem identidade antes da autenticação, e um `session.open` direto é rejeitado
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

Entregas são **empurradas de volta** pelo núcleo (`method = "session.deliver"`); após processar, `session.settle`
(`ack` / `requeue` / `reject`; o número da entrega é globalmente único, sem número de stream).

---

## 4. Passo a passo do código (projeto de exemplo)

`speedmq-plugin/java/SidecarPlugin.java` tem cerca de 470 linhas (incluindo um JSON minimalista):

| Local | Função |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Leitura/escrita de frames (`DataInputStream` + lock de escrita) |
| `Conn.serve()` | Loop de leitura de frames e despacho |
| `Conn.call()` | Chamada reversa (tabela `pending` + fila bloqueante, proteção por timeout) |
| `Conn.handleHello()` | Valida e responde HelloAck |
| `StreamState` | Lado de leitura do stream (`BlockingQueue`, `STREAM_END` indica o fim) |
| `Conn.handleForwardCall()` / `handleMethod()` | Chamadas diretas (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Autenticação + declaração + publicação + consumo |
| `Json`（final do arquivo） | Leitura/escrita JSON minimalista, apenas para deixar o exemplo sem dependências |

> **Recomendação para produção**: troque o `Json` pela biblioteca que você costuma usar (Jackson / Gson), ou por uma stack existente além de `java.net.http` ——
> isso não tem relação com o que este exemplo quer explicar (o protocolo de linha).

---

## 5. Teste prático (reprodução local)

Windows + JDK 25; o núcleo no Docker (`speedmq:1.1.01`), o plugin no host (`tcp://host.docker.internal:19031`).

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

Cobertura: **handshake → autenticação → ponte semântica → devolução de entrega → liquidação → eco do fluxo de bytes**.

---

## 6. Observações específicas de Java

- **`\uXXXX` no código-fonte é processado pelo compilador em qualquer posição** (inclusive em comentários!). No exemplo, o comentário foi escrito deliberadamente como
  "NUL + nome de usuário + NUL + senha" em vez de `\u0000` direto, senão o javac reporta caractere ilegal.
- **Código-fonte com caracteres não ASCII exige `javac -encoding UTF-8`**, senão no padrão do Windows (GBK) aparece o erro "caracteres não mapeáveis no encoding GBK".
  Em tempo de execução, se quiser imprimir corretamente, adicione `-Dfile.encoding=UTF-8`.
- **Variáveis locais capturadas por lambda precisam ser effectively final**: no exemplo, `name` é reatribuído durante a análise de parâmetros,
  portanto dentro do lambda usa-se `opts.name` (campo atribuído uma única vez).
- **`DataInputStream` é bloqueante**: na desconexão ele lança `EOFException`/`IOException`, e a partir disso você faz o encerramento.
- **base64**: `message.body`, `core.authenticate.response` são strings base64 em JSON
  (`Base64.getEncoder()/getDecoder()`).
- **A biblioteca padrão do JDK não tem JSON**: o exemplo traz uma implementação minimalista; `Json.parse` converte inteiros em `Long` e pontos flutuantes em `Double`,
  e ao obter `id` usa-se `((Number) m.get("id")).longValue()`.

---

## 7. Avançado

- Empacote em um jar executável (`Main-Class: SidecarPlugin`) ou use `jlink` para enxugar o runtime,
  e depois troque `spawn` por `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`.
- Interface de gerenciamento própria do plugin: adicione `console_url` na configuração (documento principal §5.8), e uma entrada direta aparecerá na página "Gerenciamento de plugins" do console de gerenciamento.
- Implantação independente: `spawn: []` + `address: "tcp://<nome do serviço>:19031"`, escutando `0.0.0.0` dentro do contêiner.
