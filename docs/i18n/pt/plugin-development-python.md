# Guia de desenvolvimento de plugins de processo externo do SpeedMQ —— Python

> **Público-alvo**: desenvolvedores que escrevem plugins de processo externo (sidecar) para o SpeedMQ em Python.
> **Leia primeiro**: [Guia de desenvolvimento de plugins de processo externo (sidecar)](plugin-development.md) —— lá estão o modelo mental, os campos de configuração e a tabela geral do protocolo de linha;
> este documento trata apenas de **como implementar em Python**, além dos passos e resultados verificados na máquina local.
> **Projeto de exemplo**: no workspace `speedmq-plugin/python/sidecar_plugin.py` (somente biblioteca padrão, zero dependências de terceiros).

---

## 1. Como fica quando roda

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Três pontos essenciais (fáceis de errar, memorize primeiro):

1. **Seu processo é o servidor**: escuta um endereço local e espera o núcleo se conectar (`plugins.<nome>.sidecar.address`).
2. **A porta de negócio externa é aberta pelo núcleo**: os clientes se conectam à porta do núcleo, e os bytes são intermediados até você (`protocols[].listeners`).
3. **`prefix` deve ser não vazio**: o núcleo usa a sondagem por prefixo para decidir "a quem esta conexão será entregue". Um `prefix` vazio significa **não participar da sondagem**,
   e a conexão não será entregue a você nem no seu próprio listener (na prática: a conexão é derrubada imediatamente). O comprimento do prefixo é ≤ 8 bytes, ASCII.

---

## 2. Três passos para rodar

### Passo 1: declarar o plugin na configuração

`speedmqd.json` (**a configuração real é JSON padrão e não pode conter comentários**):

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

- `address` é **o endereço ao qual o núcleo se conecta** (o núcleo é o cliente, o plugin é o servidor).
- `spawn` vazio = o núcleo apenas conecta e não inicia, e você gerencia o processo por conta própria (systemd / supervisor / compose).
- `prefix` **deve ser não vazio**: os primeiros bytes enviados pelo cliente devem começar com ele (o núcleo decide a quem entregar a conexão pela sondagem de prefixo).
- `listeners` são as portas externas, abertas pelo núcleo (os clientes se conectam ao núcleo, não a você).
- Em implantação entre contêineres, use o **nome do serviço** em `address` (como `tcp://py-sidecar:19001`), e o plugin deve escutar em `0.0.0.0`.

### Passo 2: iniciar

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Passo 3: verificar

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Pontos de implementação

### 3.1 Fragmentação (a única camada de bytes que você precisa acertar sozinho)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

O payload de um frame de dados = `4 bytes de id de stream em big-endian + bytes brutos`; o payload do plano de controle é JSON.

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

Depois que o núcleo se conecta, ele **envia Hello primeiro**, e você deve responder com um frame `HelloAck`; o núcleo valida
`name == o nome do plugin na configuração` e `api_version == a APIVersion do núcleo` (atualmente `v1`).
Depois disso o núcleo envia um `Ping` a cada 2s, e basta você responder `Pong` (não responder será considerado morto).

### 3.3 Streams lógicos

`kindOpen` chega → **responda `OpenAck` primeiro**, depois comece a atender; `kindData` chega → escreva de volta em
`kindData` como está (ou após analisar conforme seu protocolo); quando o processamento termina → envie `kindClose`. Um stream = uma conexão de cliente.

### 3.4 Chamadas reversas e a ponte semântica do núcleo

As chamadas do plugin → núcleo passam por `kindCall` com `"reverse": true`, e o núcleo responde com `kindReply` na mesma conexão.
**A ordem importa**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **não pode ser omitido**: a superfície de operação do núcleo da conexão não tem identidade antes da autenticação, e um `session.open` direto é rejeitado
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Os parâmetros são a resposta SASL analisada do seu protocolo:

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

As entregas do consumidor são **empurradas de volta** pelo núcleo (`kindCall`, `method = "session.deliver"`); ao terminar o processamento, liquide-as com
`session.settle` (`ack` / `requeue` / `reject`; o número da entrega é globalmente único e não precisa de número de stream).

### 3.5 Modelo de concorrência (versão Python)

| Papel | Thread |
| --- | --- |
| Loop de leitura de frames | Uma por conexão do núcleo |
| Tratamento de stream | Um por stream (portanto várias conexões de cliente podem rodar em paralelo) |
| Tratamento de chamadas diretas | Uma por chamada |

**Atenção**: o loop de leitura **não pode** aguardar de forma síncrona a resposta de uma chamada reversa (isso causaria deadlock) — chamadas diretas (`session.deliver`)
devem ser despachadas para uma thread separada, porque durante o processamento elas podem, por sua vez, iniciar `session.settle`. As escritas de frame devem ser serializadas com lock.

---

## 4. Passo a passo do código (projeto de exemplo)

`speedmq-plugin/python/sidecar_plugin.py` tem cerca de 320 linhas; funções principais:

| Local | Função |
| --- | --- |
| `read_frame` / `Conn.send` | Leitura/escrita de frames (prefixo de comprimento + kind) |
| `Conn.call` | Chamada reversa: numeração → envio → espera da resposta (correspondida por `reverse=true` e `id`) |
| `Conn.serve` | Loop de leitura de frames e despacho |
| `Conn._handle_hello` | Valida nome do plugin/versão de API e responde HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Ciclo de vida do stream e eco |
| `Conn._dispatch` | Trata chamadas diretas: `session.deliver` (incluindo settle), `stats` |
| `Conn._session_demo` | Autenticação + declaração de fila + publicação + consumo |

---

## 5. Teste prático (reprodução local)

Ambiente: Windows + Python 3.12; o núcleo roda no Docker (`speedmq:1.1.01`), o plugin roda no host,
e o núcleo se conecta a ele via `tcp://host.docker.internal:19001`.

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

Caminhos cobertos: **handshake → autenticação → ponte semântica (declarar/publicar/consumir) → devolução de entrega → liquidação → eco do fluxo de bytes**.

---

## 6. Observações específicas de Python

- **Não use `time.sleep` para esperar o heartbeat**: o loop de leitura é bloqueante, então basta confiar no Ping do núcleo para manter a conexão viva;
  se você definir um timeout de leitura no socket, lembre-se de tratar o timeout como "conexão encerrada" (quando o núcleo é `kill -9`ado, o socket pode não fechar prontamente).
- **`json.dumps` adiciona espaços por padrão**: o exemplo usa `separators=(",", ":")` apenas para deixar o log mais bonito; o protocolo em si não exige isso.
- **Bytes são base64**: `message.body` e `core.authenticate.response` são ambos strings base64 em JSON,
  então não esqueça `base64.b64encode/decode`.
- **As escritas de frame precisam de lock**: heartbeat, respostas e blocos de dados vêm de threads diferentes, e escritas intercaladas corrompem a conexão inteira (o exemplo usa `threading.Lock`).
- `asyncio` também funciona, mas é preciso garantir "escritas serializadas + loop de leitura não bloqueante"; a abordagem é a mesma da versão com threads.

---

## 7. Avançado

- Para dar ao plugin sua própria interface de gerenciamento: adicione `console_url` na configuração, e uma entrada direta aparecerá na página "Gerenciamento de plugins" do console de gerenciamento
  (ver o documento principal §5.8).
- Para transformar o plugin em um serviço independente gerenciado por systemd / K8s: `spawn: []` + `restart: "never"`, iniciado externamente.
- Para ter múltiplos protocolos coexistindo: faça vários plugins usarem `prefix` diferentes no mesmo listener, ou abra uma porta dedicada para cada um.
