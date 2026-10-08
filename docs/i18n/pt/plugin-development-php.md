# Guia de desenvolvimento de plugins de processo externo do SpeedMQ —— PHP

> **Público-alvo**: desenvolvedores que escrevem plugins de processo externo (sidecar) para o SpeedMQ em PHP.
> **Leia primeiro**: [Guia de desenvolvimento de plugins de processo externo (sidecar)](plugin-development.md) (modelo mental / campos de configuração / tabela geral do protocolo de linha).
> **Projeto de exemplo**: no workspace `speedmq-plugin/php/sidecar_plugin.php` (somente biblioteca padrão, **sem dependências do composer**).

---

## 1. Como fica quando roda

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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

### Passo 2: iniciar

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Passo 3: verificar

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

O PHP usa `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Payload do frame de dados = `pack('N', $streamId) . bytes brutos`.

### 3.2 Handshake e heartbeat

O núcleo **envia Hello primeiro**, você responde `HelloAck`; o núcleo valida `name` e `api_version` (atualmente `v1`).
Depois disso, um `Ping` a cada 2s, responda `Pong`.

### 3.3 Modelo de concorrência: bomba de frames reentrante (PHP não tem threads)

O PHP CLI é de thread única e bloqueante, então aqui não se usa "uma thread por stream", mas sim:

- **Loop de leitura** (`serve()`): responsável pelo handshake, heartbeat, abertura de stream, eco de dados e tratamento de chamadas diretas;
- **Eco** não precisa de máquina de estados adicional: ao receber `kindData`, escreva imediatamente de volta em `kindData` como está;
- **Chamadas reversas** usam `callAndWait()`: após enviar `kindCall`, lê frames e despacha ao mesmo tempo,
  até ler **a sua própria** resposta (`reverse=true` e `id` correspondente) e então retornar.

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

Isso significa que `dispatchOther()` **deve ser reentrante**: ele pode ser chamado novamente dentro de um `callAndWait`
(por exemplo, ao tratar `session.deliver`, precisar de `session.settle` de novo). O exemplo faz exatamente assim.

### 3.4 Ponte semântica (é preciso autenticar primeiro)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` não pode ser omitido: a superfície de operação do núcleo da conexão não tem identidade antes da autenticação, e um `session.open` direto é rejeitado
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

Entregas são **empurradas de volta** pelo núcleo (`method = "session.deliver"`); após processar, `session.settle`
(`ack` / `requeue` / `reject`; o número da entrega é globalmente único, sem número de stream).

---

## 4. Passo a passo do código (projeto de exemplo)

`speedmq-plugin/php/sidecar_plugin.php` tem cerca de 320 linhas:

| Local | Função |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Leitura/escrita de frames |
| `Conn::serve()` | Loop de leitura principal |
| `Conn::dispatchOther()` | Despacha frames que não são de handshake (reentrante) |
| `Conn::callAndWait()` | Chamada reversa (bomba de frames reentrante) |
| `Conn::handleHello()` | Valida e responde HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Chamadas diretas (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Autenticação + declaração + publicação + consumo |

---

## 5. Teste prático (reprodução local)

Windows + PHP 7.4; o núcleo no Docker (`speedmq:1.1.01`), o plugin no host (`tcp://host.docker.internal:19021`).

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

Cobertura: **handshake → autenticação → ponte semântica → devolução de entrega → liquidação → eco do fluxo de bytes**.

---

## 6. Observações específicas de PHP

- **O PHP 7.4 não tem tipo de retorno `mixed`** (só existe a partir do PHP 8.0): no exemplo, a chamada reversa retorna "qualquer tipo",
  portanto **não escreva a declaração de tipo de retorno** (use o comentário `@return mixed`). Escrever `: mixed` no 7.4 causa erro de sintaxe direto.
- **Tipos numéricos em JSON**: `json_decode($s, true)` por padrão converte inteiros em `int`, e inteiros grandes podem virar `float`;
  o número da entrega não é problema na escala deste exemplo, mas se o seu número for muito grande, considere `JSON_BIGINT_AS_STRING`.
- **base64 é obrigatório**: `message.body` e `core.authenticate.response` são strings base64 em JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **Não use `pcntl_fork` para concorrência**: não há pcntl no Windows, e o fork quebra a premissa de "um único escritor por conexão";
  thread única + bomba de frames reentrante já basta (a menos que você faça cálculos muito pesados no stream, o que é mais adequado a um serviço externo).
- **`stream_socket_accept` é bloqueante**: o ciclo de vida do processo é gerenciado pelo núcleo (`spawn`) ou por um supervisor;
  lembre-se de tratar `fread` retornando `''` (EOF) → encerrar aquela conexão e voltar ao accept.
- **Buffer de saída**: use `fwrite(STDOUT, …)` nos logs, com uma quebra de linha, para facilitar o encaminhamento linha a linha pelo núcleo ao log do núcleo.

---

## 7. Avançado

- Interface de gerenciamento própria do plugin: adicione `console_url` na configuração (documento principal §5.8), e uma entrada direta aparecerá na página "Gerenciamento de plugins" do console de gerenciamento.
- Implantação independente: `spawn: []` + `address: "tcp://<nome do serviço>:19021"`, escutando `0.0.0.0` dentro do contêiner.
- Quando precisar de mais concorrência, dá para transformar o plugin em algo residente como Swoole / RoadRunner, mas **o protocolo de linha não muda**; basta garantir:
  escritas de frame serializadas, loop de leitura não bloqueante, chamadas reversas correspondidas por `id`+`reverse`.
