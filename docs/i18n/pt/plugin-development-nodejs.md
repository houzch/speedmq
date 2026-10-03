# Guia de desenvolvimento de plugins de processo externo do SwiftMQ —— Node.js

> **Público-alvo**: desenvolvedores que escrevem plugins de processo externo (sidecar) para o SwiftMQ em Node.js.
> **Leia primeiro**: [Guia de desenvolvimento de plugins de processo externo (sidecar)](plugin-development.md) (modelo mental / campos de configuração / tabela geral do protocolo de linha).
> **Projeto de exemplo**: no workspace `swiftmq-plugin/nodejs/index.js` (somente biblioteca padrão do Node, **sem dependências npm**).

---

## 1. Como fica quando roda

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Três pontos essenciais: **seu processo é o servidor** (espera o núcleo se conectar); **a porta externa é aberta pelo núcleo** (configuração `protocols[].listeners`);
**`prefix` deve ser não vazio** (prefixo vazio = não participa da sondagem, a conexão não será entregue a você; na prática é derrubada imediatamente, ≤8 bytes ASCII).

---

## 2. Três passos para rodar

### Passo 1: configuração

`swiftmqd.json` (**a configuração real é JSON padrão e não pode conter comentários**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/swiftmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Passo 2: iniciar

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Passo 3: verificar

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

Em Node, use `Buffer`: acumule os bytes recebidos e, quando houver um frame completo, recorte-o para processar.

```js
drain() {
  while (this.buf.length >= 5) {
    const len = this.buf.readUInt32BE(0)
    if (this.buf.length < 4 + len) return      // 帧还没收全
    const kind = this.buf.readUInt8(4)
    const payload = this.buf.subarray(5, 4 + len)
    this.buf = this.buf.subarray(4 + len)
    this.dispatch(kind, payload)
  }
}
```

Um stream = uma conexão de cliente; o payload do frame de dados é `4 bytes de id de stream em big-endian + bytes brutos`.

### 3.2 Handshake e heartbeat

O núcleo **envia Hello primeiro**, você responde `HelloAck`; o núcleo valida `name` e `api_version` (atualmente `v1`) e então conecta.
Depois disso, um `Ping` a cada 2s, basta responder `Pong` (tratado pelo loop de leitura, sem precisar de timer).

### 3.3 Modelo assíncrono (versão Node)

Loop de eventos de thread única, que evita naturalmente o problema de "escritas intercaladas" — mas cuidado para **não deixar o loop de leitura aguardar (`await`) uma chamada reversa**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` é `async`: ele pode, por sua vez, `await call('session.settle', …)`, portanto nunca o escreva como espera síncrona.

### 3.4 Ponte semântica (é preciso autenticar primeiro)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` não pode ser omitido: a superfície de operação do núcleo da conexão não tem identidade antes da autenticação, e um `session.open` direto é rejeitado
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```js
const ident = await call('core.authenticate', {
  stream, mechanism: 'PLAIN',
  response: Buffer.concat([Buffer.from(`\x00${user}\x00`), Buffer.from(password)]).toString('base64'),
})
await call('session.open', { stream, vhost: '/' })
const q = await call('session.declare_queue', { stream, exclusive: true, auto_delete: true })
await call('session.publish', { stream, routing_key: q.name,
  message: { body: Buffer.from('hi').toString('base64') } })
await call('session.consume', { stream, queue: q.name, prefetch: 32 })
```

Entregas são **empurradas de volta** pelo núcleo (`method = "session.deliver"`); após processar, `session.settle`
(`ack` / `requeue` / `reject`; o número da entrega é globalmente único, sem número de stream).

---

## 4. Passo a passo do código (projeto de exemplo)

`swiftmq-plugin/nodejs/index.js` tem cerca de 330 linhas:

| Local | Função |
| --- | --- |
| `u32()` / `Conn.send()` | Leitura/escrita de frames |
| `Conn.drain()` / `dispatch()` | Análise e despacho por frame |
| `Conn.call()` | Chamada reversa (tabela `Promise` + `pending`, correspondida por `reverse=true` e `id`) |
| `Stream` | Lado de leitura do stream: `push/end/read` formam uma fila assíncrona |
| `handleHello` | Valida e responde HelloAck |
| `handleForwardCall` / `handleMethod` | Chamadas diretas (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Autenticação + declaração + publicação + consumo |

---

## 5. Teste prático (reprodução local)

Windows + Node v24; o núcleo no Docker (`swiftmq:1.1.01`), o plugin no host (`tcp://host.docker.internal:19011`).

```
plugin=node-sidecar state=enabled         # /api/plugins
echo=[NDhello]                            # 客户端连内核端口 19012 发 "NDhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=node-sidecar addr=0.0.0.0:19011 version=0.1.0
内核已接入 plugin=node-sidecar peer=127.0.0.1:50987
握手完成 plugin=node-sidecar kernel=1.1.01
认证通过 user=guest
收到投递（session.deliver） queue=amq.gen-893688b01c62b2ad5763a7 delivery_id=1 body=hello from node sidecar
session 演示完成 queue=amq.gen-893688b01c62b2ad5763a7
流已打开 plugin=node-sidecar stream=1 remote=172.17.0.1:40450 local=172.17.0.2:19012
```

Cobertura: **handshake → autenticação → ponte semântica → devolução de entrega → liquidação → eco do fluxo de bytes**.

---

## 6. Observações específicas de Node.js

- **`socket.write` escreve um frame por vez**: o exemplo monta o frame inteiro em um `Buffer` antes de escrever, portanto não precisa de lock adicional;
  se você dividir um frame em várias chamadas `write`, terá de garantir a ordem por conta própria.
- **Os limites de chunk de `stream.on('data')` não têm relação com os frames**: você deve acumular o buffer por conta própria (ver `drain()`).
- **base64**: `message.body`, `core.authenticate.response` são strings base64 em JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Não use `await` dentro de `drain()`**: é uma função síncrona de recorte de frames; entregue o processamento assíncrono ao `handleForwardCall`.
- **ESM vs CJS**: o exemplo usa CommonJS (`require`) para que `node index.js` rode diretamente; para mudar para ESM, basta trocar por `import`.

---

## 7. Avançado

- Interface de gerenciamento própria do plugin: adicione `console_url` na configuração (documento principal §5.8), e uma entrada direta aparecerá na página "Gerenciamento de plugins" do console de gerenciamento.
- Implantação independente (K8s / systemd): `spawn: []` + `address: "tcp://<nome do serviço>:19011"`, escutando `0.0.0.0` dentro do contêiner.
- Reutilização de portas: dê um `prefix` diferente para cada protocolo; o núcleo distribui as conexões para cada plugin conforme o prefixo.
