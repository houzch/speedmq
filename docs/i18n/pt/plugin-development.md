# Guia de desenvolvimento de plugins de processo externo (sidecar) do SpeedMQ

> **Público-alvo**: desenvolvedores que não querem fazer fork / recompilar o núcleo, mas desejam estender o SpeedMQ em **qualquer linguagem**.
> **Escopo**: este documento cobre apenas uma forma de plugin — o **plugin de processo externo** (termo do núcleo `sidecar`). Plugins de protocolo embutidos no núcleo (AMQP 0-9-1 / MQTT) estão fora do escopo.
> **Como ler**: as seções 1–2 constroem o modelo mental, a seção 3 mostra como escrever código, e **a seção 5 trata de "como integrá-lo para rodar junto e atender clientes depois de terminar o desenvolvimento"**;
> **para outras linguagens (Python / Node.js / PHP / Java), veja os guias por linguagem na §4** (cada um traz um projeto de exemplo completo e verificado na prática).
> O código no documento é um esqueleto mínimo executável, que você pode copiar como ponto de partida. O chinês simplificado é o idioma de origem.

---

## 1. O que é

Um **processo independente** que implementa certo "protocolo" (analisa o fluxo de bytes do cliente) dentro do próprio processo,
e o núcleo o hospeda conforme a configuração: **as portas são abertas pelo núcleo e as conexões são intermediadas pelo núcleo**, e registro / início-parada / auditoria / isolamento reutilizam todos os mecanismos já existentes no núcleo.

Antes de tudo, estabeleça três modelos mentais corretos (os pontos mais fáceis de errar):

1. **O processo do plugin é um "serviço local"**: ele escuta apenas um **endereço local** (TCP ou unix socket) e espera **que o núcleo se conecte**.
   A direção da conexão é **núcleo (cliente) → plugin (servidor)**, e o handshake também é enviado primeiro pelo núcleo.
2. **A porta de negócio externa não é aberta pelo plugin**: ela é criada pelo **núcleo** conforme `protocols[].listeners` na configuração e mapeada para os clientes.
   Os clientes se conectam à **porta do núcleo**, e os bytes são intermediados pelo núcleo até o processo do plugin. O processo do plugin **não precisa** abrir a porta de negócio por conta própria.
3. **A semântica é opcional**: o plugin pode apenas "transportar bytes" (implementando o protocolo inteiramente por conta própria),
   ou pode alcançar a semântica do núcleo (filas / roteamento / permissões / confirmação) por meio de **chamadas reversas** a `session.*`,
   compartilhando **exatamente o mesmo conjunto de semânticas** dos plugins de protocolo embutidos (assim vhost, permissões, roteamento e comportamento das filas não divergem).

| Benefícios | Custos |
| --- | --- |
| Estender sem alterar nem recompilar o núcleo | Uma cópia local adicional de bytes no plano de dados (intermediação do núcleo, sem passagem de fd, consistente entre plataformas) |
| Implementação em qualquer linguagem (basta implementar o protocolo de linha) | Um RPC local adicional a cada chamada reversa (codificação/decodificação JSON + cópia) |
| O plugin pode ser publicado / atualizado / reiniciado de forma independente | A sondagem (sniffing) fica no lado do núcleo: só pode ser reconhecido por "prefixo" ou "porta dedicada" |
| Uma falha afeta apenas aquele plugin: o núcleo o marca como `down`, sem sair nem travar | Apenas a capacidade `net.listen` realmente tem efeito; os demais valores de capacidade são reservados (ver §7) |

---

## 2. Como funciona

Criação da conexão (**o núcleo é o cliente, o plugin é o servidor**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

Handshake:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

Início do atendimento:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Ciclo de vida** (host `internal/plugin/sidecar` no lado do núcleo):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Semântica de estados** (visível via `speedmqctl plugins show`):

| Estado | Significado | Ação do operador |
| --- | --- | --- |
| `enabled` | Conectado e atendendo | — |
| `failed` | **Não subiu na inicialização** (configuração incorreta / handshake rejeitado / processo não pôde ser iniciado) | Ver `RuntimeNote` e o log do núcleo, corrigir a configuração ou o plugin; o núcleo só tenta de novo após reiniciar |
| `down` | **Já subiu antes, mas agora não está presente** (processo travou / conexão caiu) | Suba o processo do plugin; o núcleo se recupera automaticamente conforme a política `restart` |
| `disabled` | `enabled=false` na configuração, ou desativado a quente pelo operador | Recupere com `plugins enable <nome>` |

---

## 3. Desenvolvimento (Go)

### 3.1 Criar o projeto

O plugin é um **módulo Go independente** que depende apenas de dois pacotes de contrato públicos:

- `github.com/houzch/speedmq/pkg/sidecar` — o protocolo de linha e a implementação do lado do plugin (**obrigatório**)
- `github.com/houzch/speedmq/pkg/plugin` — apenas quando você precisar de tipos como `plugin.Message` / `plugin.Error` (opcional)

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/speedmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/speedmq@v1.1.05
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/speedmq=../speedmq
```

> Ao usar `replace` para depuração conjunta, o plugin e o núcleo devem usar **a mesma árvore de código-fonte**; caso contrário, embora a versão de API (`v1`) coincida, os tipos podem ser diferentes.

### 3.2 Implementar o `Handler` (três métodos)

Toda a superfície de negócio do processo do plugin é o `Hello` / `Call` / `Open` de `sidecar.Handler`.
Handshake, heartbeat, multiplexação e fragmentação são todos tratados por `pkg/sidecar`, e você não precisa mexer em frames.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/houzch/speedmq/pkg/sidecar"
)

const (
	pluginName = "my-sidecar" // 必须与内核配置里的插件名一致
	version    = "0.1.0"
	apiVersion = "v1" // 必须等于 sidecar / plugin 的 APIVersion
	protocol   = "myproto" // 应与配置里 protocols[].name 一致
)

func main() {
	addr := ":19001" // 内核来连的本机地址；可用 -addr 覆盖
	handler := &handler{}

	srv, err := sidecar.NewServer(handler, sidecar.ServerOptions{
		Address: "tcp://" + addr,
		Logger:  stdLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 监听失败: %v\n", err)
		os.Exit(1)
	}
	// Address() 能读回实际地址（配置里写 :0 时有用）。
	log.Printf("my-sidecar 已启动 name=%s version=%s addr=%s", pluginName, version, srv.Address())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 服务退出: %v\n", err)
		os.Exit(1)
	}
}

type handler struct {
	streams atomic.Int64
}

var _ sidecar.Handler = (*handler)(nil)

// Hello 处理握手：返回的 HelloAck 发给内核。返回 error 即拒绝接入（内核会明确隔离该插件）。
func (h *handler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	// 内核版本过旧时可以拒绝，避免带着不兼容跑起来。
	if hello.Plugin != pluginName {
		return sidecar.HelloAck{}, fmt.Errorf("插件名不匹配：内核声明 %q，本插件是 %q", hello.Plugin, pluginName)
	}
	if hello.APIVersion != apiVersion {
		return sidecar.HelloAck{}, fmt.Errorf("插件 API 版本不匹配：内核 %q，插件 %q", hello.APIVersion, apiVersion)
	}
	return sidecar.HelloAck{
		Name:         pluginName,
		Version:      version,
		APIVersion:   apiVersion,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{protocol}, // 展示用；真正生效的是内核配置里的 protocols
		Methods:      []string{"stats"},  // 展示用；内核不提供通用调用入口
	}, nil
}

// Call 处理方法调用。约定的系统方法 session.deliver 是"内核把消费投递回推给插件"，
// 必须在实现里处理（见 §3.4）。
func (h *handler) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		return h.handleDeliver(ctx, params)
	case "stats":
		return map[string]any{"streams": h.streams.Load()}, nil
	default:
		// 未知方法必须明确报错：静默成功会让调用方以为生效了。
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

// Open 处理一条新打开的流：一条流 = 内核侧的一条客户端连接。
// 通常**阻塞处理到流结束**再返回；返回后该流即结束（内核会关闭对应的客户端连接）。
func (h *handler) Open(ctx context.Context, stream *sidecar.Stream, meta sidecar.Open) error {
	h.streams.Add(1)
	// meta.Remote / meta.Local 是两端地址，meta.Peek 是嗅探阶段读到的前缀字节，可用于更细的分支判断。
	return h.serve(stream)
}

// stdLogger 把 pkg/sidecar 的最小日志接口接到标准库日志。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Info(msg string, args ...any) {
	s.l.Printf("%s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
func (s stdLogger) Warn(msg string, args ...any) {
	s.l.Printf("WARN %s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
```

### 3.3 Plano de dados: ler e escrever um `Stream`

`sidecar.Stream` implementa `io.ReadWriteCloser`, então basta tratá-lo como "uma conexão":

```go
func (h *handler) serve(stream *sidecar.Stream) error {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			// 你的协议解析在这里；示例先原样回显。
			if _, werr := stream.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			return nil // 对端关闭
		}
	}
}
```

Pontos-chave:

- Cada conexão de cliente = um stream; o plugin pode começar a enviar/receber imediatamente dentro de `Handler.Open`.
- Mensagens grandes são fragmentadas automaticamente pela biblioteca (cada frame ≤ 64 KiB), e **o uso de memória é independente do tamanho da mensagem**.
- Backpressure: o buffer de recepção de um único stream tem limite máximo; quando o buffer enche, a **goroutine de despacho** daquela conexão é bloqueada (todos os streams esperam juntos) —
  este é o trade-off entre "memória previsível" e "limitação de taxa por stream", veja a §7 para detalhes.

### 3.4 Usar a ponte semântica do núcleo (`session.*`)

Se você quer que o plugin reutilize a semântica de filas / roteamento / permissões / confirmação do núcleo (em vez de criar um conjunto próprio), use **chamadas reversas**.
Em um stream, use-as na ordem `session.open` → outros `session.*` → (`session.close`):

```go
import (
	"errors"

	"github.com/houzch/speedmq/pkg/plugin"
	"github.com/houzch/speedmq/pkg/sidecar"
)

func (h *handler) runDemo(ctx context.Context, stream *sidecar.Stream) error {
	br, ok := sidecar.BridgeFromContext(ctx) // 当前流的内核桥（内核在建流时注入 ctx）
	if !ok {
		return errors.New("ctx 中没有内核桥")
	}
	streamID := stream.ID()

	// 1) 认证：连接的内核操作面在认证前没有身份，会话一定打不开。
	//    response 就是你自己协议里的凭据（这里以 SASL PLAIN 为例）。
	plain := append([]byte("\x00guest\x00"), []byte("guest")...)
	var ident sidecar.AuthIdentityDTO
	if err := br.Call(ctx, sidecar.MethodCoreAuthenticate, sidecar.CoreAuthenticateParams{
		Stream: streamID, Mechanism: "PLAIN", Response: plain,
	}, &ident); err != nil {
		return err
	}

	// 2) 打开会话（内核会做与内置协议插件相同的权限校验）
	if err := br.Call(ctx, sidecar.MethodSessionOpen,
		sidecar.SessionOpenParams{Stream: streamID, VHost: "/"}, nil); err != nil {
		return err
	}

	// 3) 声明一个临时队列
	var q sidecar.QueueInfoResult
	if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue, sidecar.QueueDeclareParams{
		Stream: streamID, Exclusive: true, AutoDelete: true,
	}, &q); err != nil {
		return err
	}

	// 4) 发布一条消息（持久化等待在内核应答前已完成：调用返回即已按 fsync 档位落盘）
	if err := br.Call(ctx, sidecar.MethodSessionPublish, sidecar.PublishParams{
		Stream: streamID, RoutingKey: q.Name,
		Message: sidecar.MessageDTO{Body: []byte("hello")},
	}, nil); err != nil {
		return err
	}

	// 5) 注册消费者；投递随后以正向调用 session.deliver 到达 Handler.Call
	var c sidecar.ConsumeResult
	return br.Call(ctx, sidecar.MethodSessionConsume, sidecar.ConsumeParams{
		Stream: streamID, Queue: q.Name, Prefetch: 32,
	}, &c)
}

// 处理内核回推的投递并结算
func (h *handler) handleDeliver(ctx context.Context, params json.RawMessage) (any, error) {
	var p sidecar.DeliverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		return nil, errors.New("ctx 中没有内核桥")
	}
	// 结算不需要流号：投递编号在整条连接上全局唯一。
	// 动作三选一：Ack（消费完成）/ Requeue（重新入队）/ Reject（丢弃，可能进死信）。
	return nil, br.Call(ctx, sidecar.MethodSessionSettle, sidecar.SettleParams{
		DeliveryID: p.DeliveryID, Action: sidecar.SettleActionAck,
	}, nil)
}
```

**Chamadas reversas em resumo** (constantes em `pkg/sidecar/bridge.go` → nomes de linha):

| Grupo | Nome de linha (constante) | Descrição |
| --- | --- | --- |
| Autenticação | `core.authenticate`（`MethodCoreAuthenticate`） | **Deve ser feito primeiro**: entregue as credenciais do seu protocolo ao núcleo para verificação; parâmetros `{stream, mechanism, response}`, retorna `{user}` |
| Sessão | `session.open`（`MethodSessionOpen`） | Abre uma sessão para um vhost no stream; só pode ser feito **após a autenticação** |
| | `session.close`（`MethodSessionClose`） | Libera a sessão no stream (cancela consumidores, exclui filas exclusivas) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Criar / excluir (declarar passivamente algo inexistente → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Bindings de exchange para exchange |
| Fila | `session.declare_queue` / `session.delete_queue` | Criar / excluir; o servidor gera quando `name` está vazio |
| | `session.bind_queue` / `session.unbind_queue` | Bindings de fila para exchange |
| | `session.purge_queue` | Limpa as mensagens prontas (excluindo as não confirmadas) |
| Publicação | `session.publish` | Retorna `{routed, rejected}`; a persistência é concluída antes da resposta |
| Obtenção | `session.get` | Busca ativamente uma mensagem; `found=false` indica fila vazia |
| Consumo | `session.consume` / `session.cancel` | Registrar / cancelar consumidores |
| Liquidação | `session.settle` | Liquida uma entrega (`ack` / `requeue` / `reject`) |
| **Direta** | `session.deliver` | **Núcleo → plugin**: devolve a entrega (tratada no seu `Handler.Call`) |

**Quatro regras que você deve seguir**:

1. **`core.authenticate` primeiro**: a superfície de operação do núcleo da conexão não tem identidade antes da autenticação,
   então `session.open` é rejeitado nesse momento (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   O plugin é responsável por extrair as credenciais do seu próprio protocolo; a lógica de autenticação e a tabela de usuários continuam no núcleo, e o plugin nunca toca no armazenamento de senhas.
2. **Depois `session.open`**: chamar outros métodos sem uma sessão aberta faz o núcleo retornar `KindPreconditionFailed` ("o stream N ainda não abriu uma sessão").
3. **Liquide cada entrega exatamente uma vez**: escolha uma entre `Ack` / `Requeue` / `Reject`.
   Tanto `Ack` quanto `Reject` descartam a mensagem, mas **apenas `Reject` vai para a dead letter**.
4. **Entregas não liquidadas não se perdem**: quando um stream termina (o cliente desconecta / `Handler.Open` retorna) ou a conexão do plugin cai,
   o núcleo trata todas as entregas não liquidadas **como "requeue"**, evitando que mensagens fiquem presas.

**Restauração de erros**: o `*plugin.Error` do núcleo chega pela ponte como um `*sidecar.RPCError` (campos `Kind` / `Text`),
e pode ser restaurado em um `plugin.Error` categorizado, em vez de perder a categoria dentro de uma string:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Mapeamento de `Kind` comuns para os protocolos embutidos (para você decidir como devolver erros ao cliente):

| `plugin.ErrorKind` | Semântica | Mapeamento AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | Objeto não existe | 404 NOT_FOUND (fecha o Channel) |
| `KindPreconditionFailed` | Parâmetros inconsistentes com um objeto existente / sessão não aberta | 406 PRECONDITION_FAILED (fecha o Channel) |
| `KindAccessRefused` | Permissão insuficiente / nome reservado | 403 ACCESS_REFUSED (fecha o Channel) |
| `KindResourceLocked` | Recurso exclusivo ocupado | 405 RESOURCE_LOCKED (fecha o Channel) |
| `KindInvalidPath` | vhost não existe | 402 INVALID_PATH (fecha a conexão) |
| `KindNotImplemented` | Capacidade não implementada | 540 NOT_IMPLEMENTED (fecha a conexão) |
| `KindInternal` | Erro interno do núcleo | 541 INTERNAL_ERROR (fecha a conexão) |

**Limite de fidelidade de tipos de mensagem**: a tabela de propriedades (`MessageDTO.Properties.Headers`) é retransmitida via JSON,
então **informações de tipo numérico que distinguem `int32` / `double`, como em um field-table do AMQP, não estão disponíveis**.
Quando for necessária fidelidade estrita de tipos, carregue você mesmo esse tipo de informação no corpo da mensagem (bytes brutos).

### 3.5 Estado e logging do lado do plugin

- O **estado de um plugin externo é determinado por a conexão estar viva ou não**, e o plugin não o reporta por conta própria (o `StateReporter` dos plugins embutidos não se aplica a processos externos).
- Logging: `sidecar.ServerOptions.Logger` grava em stdout/stderr do processo do plugin;
  **quando iniciado pelo núcleo via `spawn`, essas saídas são encaminhadas pelo núcleo para o log do núcleo** (marcadas com `plugin`), o que facilita a coleta centralizada.
- Em uma implantação independente (sem spawn), colete os logs do plugin do seu próprio jeito.

### 3.6 Testar sem o núcleo

`Handler` é uma interface Go comum; em um teste unitário você pode instanciá-lo diretamente e chamar `Hello` / `Call` / `Open` para cobrir a lógica de negócio, sem subir nenhuma rede:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Para verificação ponta a ponta, veja a §5.

---

## 4. Desenvolver em outras linguagens (especificação do protocolo de linha)

`pkg/sidecar` é um contrato público sem dependências; o protocolo de linha em si é simples e pode ser implementado em qualquer linguagem.
Para integrar, você precisa implementar as seguintes convenções "em nível de byte" (o código-fonte está em `pkg/sidecar/frame.go`, `proto.go`).

> **Guias por linguagem com projetos de exemplo completos já estão disponíveis** (os exemplos passaram na prática por handshake → autenticação → ponte semântica → entrega/liquidação → fluxo de bytes):
>
> | Linguagem | Guia | Projeto de exemplo (no workspace `speedmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (somente biblioteca padrão) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (somente biblioteca padrão) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (somente biblioteca padrão) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (arquivo único, somente JDK) |
>
> Para a implementação de referência completa em Go, veja o projeto de teste independente `speedmq-test/test/integration/echosidecar/` (ele usa `pkg/sidecar.Server` diretamente,
> portanto você não precisa se preocupar com os detalhes em nível de byte abaixo).

**Formato do frame** (uniforme para todos os frames):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Tipos de frame `kind`**:

| kind | Nome | Direção | Carga útil |
| --- | --- | --- | --- |
| 1 | Hello | núcleo → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → núcleo | JSON `HelloAck` |
| 3 | Ping | núcleo → plugin | vazio |
| 4 | Pong | plugin → núcleo | vazio |
| 5 | Call | bidirecional | JSON `Call` |
| 6 | Reply | bidirecional | JSON `Reply` |
| 7 | Open | núcleo → plugin | JSON `Open` |
| 8 | OpenAck | plugin → núcleo | JSON `OpenAck` |
| 9 | Data | bidirecional | `u32 BE stream` + bytes brutos |
| 10 | Close | bidirecional | JSON `Close` |

**Estruturas JSON do plano de controle** (nomes de campo iguais aos de `proto.go`).

> Esta seção é um **exemplo de mensagens do protocolo de linha** (várias mensagens são dadas em ordem em um mesmo bloco, por isso os separadores `//` para explicação),
> e **não é configuração que possa ser escrita diretamente no `speedmqd.json`**.

```jsonc
// Hello（内核 → 插件）
{ "plugin": "my-sidecar", "protocol_version": "v1", "kernel_version": "1.1.01", "api_version": "v1" }
// HelloAck（插件 → 内核）；deny 非空表示拒绝服务
{ "name": "my-sidecar", "version": "0.1.0", "api_version": "v1",
  "capabilities": ["net.listen"], "protocols": ["myproto"], "methods": ["stats"], "deny": "" }
// Call / Reply：方向字段 reverse 区分"内核→插件"（false）与"插件→内核"（true）
{ "id": 1, "method": "session.open", "reverse": false, "params": { } }
{ "id": 1, "reverse": false, "ok": true, "error": "", "data": { } }
// Open（内核 → 插件）／OpenAck（插件 → 内核）／Close（双向）
{ "stream": 7, "remote": "1.2.3.4:5000", "local": "0.0.0.0:19002", "peek": "" }
{ "stream": 7, "ok": true, "error": "" }
{ "stream": 7, "reason": "closed by peer" }
```

**Semânticas que você deve seguir**:

- **Handshake**: o núcleo envia `Hello` primeiro, e o plugin deve responder com um frame `HelloAck`.
  O núcleo valida `HelloAck.name == o nome do plugin na configuração` e `HelloAck.api_version == a APIVersion do núcleo`;
  um `deny` não vazio é tratado como recusa de conexão (o plugin é isolado).
- **Heartbeat**: por padrão o núcleo envia `Ping` a cada 2s, e o plugin deve responder `Pong` em até 8s; do lado do plugin, se nenhum frame for recebido em 24s, ele pode fechar a conexão por conta própria.
- **Dois espaços de ID**: o `reverse` de `Call` distingue a direção, e cada direção incrementa a partir de 1 de forma independente,
  portanto o `Reply` **deve carregar o `reverse` de volta**, senão a resposta será entregue ao esperador errado.
- **Sem base64 no plano de dados**: dados grandes, como corpos de mensagem, vão diretamente na carga útil do frame `Data` (`stream` + bytes brutos), fragmentados conforme necessário.

> Se você usar Go, basta usar `pkg/sidecar` diretamente e não precisará implementar nenhum dos detalhes acima.

---

## 5. Integrar para rodar junto: conexão, atendimento a clientes, empacotamento ★

Esta seção responde a "como integrar ao SpeedMQ e como atender clientes depois de terminar o desenvolvimento".

### 5.1 Declarar o plugin na configuração

Um plugin externo é **totalmente gerenciado pela configuração**, e o núcleo não precisa de nenhuma alteração de código para ele. Adicione uma entrada à seção `plugins` do `speedmqd.json`
(**a configuração real é JSON padrão e não pode conter comentários**):

```json
{
  "listeners": {
    "myproto": [{ "addr": ":19002" }]
  },
  "plugins": {
    "my-sidecar": {
      "builtin": false,
      "enabled": true,
      "required": false,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["/usr/local/bin/my-sidecar", "-addr", "tcp://127.0.0.1:19001"],
        "restart": "always",
        "protocols": [
          {
            "name": "myproto",
            "prefix": "MP",
            "listeners": [{ "name": "myproto", "addr": ":19002" }]
          }
        ]
      }
    }
  }
}
```

Explicação item a item (a lista de campos está na tabela abaixo):

- O nome da chave de `plugins.<nome do plugin>` **deve corresponder ao `HelloAck.name` autorreportado pelo plugin**, senão o handshake é rejeitado.
- `builtin: false`: declara explicitamente um plugin externo (se omitido, o plano de gerenciamento o exibe como embutido).
- `enabled`: desativá-lo = não iniciar processo, não criar listener.
- Com `required: true`, uma falha de inicialização bloqueia a inicialização do núcleo —— não habilite isso para plugins externos.
- `address` é **o endereço ao qual o núcleo se conecta** (o núcleo é o cliente); quando `spawn` não está vazio, o núcleo inicia o processo em seu nome.
- `protocols[].prefix` **deve ser não vazio** (as regras de sondagem estão na §5.3).
- `listeners` são as **portas externas desse protocolo, abertas pelo núcleo** (os clientes se conectam ao núcleo).

Lista de campos:

| Campo | Obrigatório | Descrição |
| --- | --- | --- |
| `sidecar.address` | ✅ | Endereço do processo do plugin: `tcp://host:port` ou `unix:///path` |
| `sidecar.spawn` | ✕ | Linha de comando que o núcleo inicia em seu nome (o primeiro elemento é o executável); **vazio = o núcleo apenas conecta e não inicia**, e você gerencia o processo por conta própria |
| `sidecar.restart` | ✕ | `always` (padrão, recupera automaticamente após desconexão/falha) ou `never` (apenas marca `down` e aguarda intervenção do operador) |
| `sidecar.protocols[].name` | ✅ | Nome do protocolo (globalmente único, participa da prioridade de sondagem) |
| `sidecar.protocols[].prefix` | ✕ | Prefixo de sondagem (ASCII); **vazio = não participa da sondagem** |
| `sidecar.protocols[].listeners[]` | ✕ | Listener externo desse protocolo (`name` + `addr`), criado pelo núcleo |
| `sidecar.handshake_timeout_seconds` | ✕ | Substitui o timeout do handshake (padrão 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Substitui o intervalo de heartbeat (padrão 2s) |

> **Nome do plugin vs. nome do protocolo**: os dois **podem ser diferentes** (por exemplo, o plugin `my-sidecar` fornece o protocolo `myproto`).
> A desativação a quente primeiro encontra todos os protocolos registrados pelo nome do plugin e depois fecha as portas desses protocolos, portanto não há necessidade de usar deliberadamente o mesmo nome.

### 5.2 Três modos de integração

| Modo | Configuração | Quando usar |
| --- | --- | --- |
| **Mesmo host + núcleo inicia (spawn)** | `spawn: [...]`, `address` aponta para o endereço em que ele escuta | Implantação na mesma máquina, contêiner único; o mais simples, o núcleo cuida de iniciar e recolher |
| **Mesmo host + autogerenciado (dial)** | `spawn: []`, `address` aponta para um processo já em execução | Gerenciar o ciclo de vida do plugin com systemd / supervisor |
| **Host cruzado / contêiner cruzado (dial, obrigatoriamente tcp)** | `spawn: []`, `address: "tcp://<nome do serviço>:19001"` | Plugin e núcleo implantados em contêineres / máquinas separados |

Escolha do endereço:

- **Na mesma máquina, recomenda-se um unix socket** (`unix:///tmp/my-sidecar.sock`): não ocupa uma porta TCP e não é afetado pela ocupação de portas do host.
  Observe que o caminho do socket precisa ser gravável pelo processo do núcleo (em um contêiner, o usuário não-root `speedmq`).
- **Entre contêineres é obrigatório TCP**, e o processo do plugin deve escutar em `0.0.0.0`, com `address` usando **o nome do serviço na rede de contêineres**.

> Não inverta a direção: **o endereço em que o plugin escuta** = `address`; **a porta exposta aos clientes** = `protocols[].listeners`.

### 5.3 Atender clientes: reconhecido pelo `prefix`

Quando a camada de acesso distribui conexões, ela **olha apenas para o resultado da sondagem**: para cada protocolo habilitado ela pergunta `Sniff(peek)` na ordem de registro (peek é no máximo 8 bytes),
e o que corresponder assume a conexão. Portanto:

1. **`prefix` deve ser não vazio** (ASCII, ≤ 8 bytes). Só quando os primeiros bytes enviados pelo cliente corresponderem a ele a conexão será entregue ao seu plugin.
   Exemplo: `"prefix": "PY"` → os primeiros bytes do cliente devem ser `PY` (você pode tratar o prefixo como o cabeçalho mágico do seu protocolo).
2. **Um `prefix` vazio significa que não participa da sondagem**: essas conexões **não** serão entregues ao plugin (na prática: conexões na porta de escuta são derrubadas imediatamente).
   Portanto um `prefix` vazio só serve para o cenário "outro protocolo fará o encaminhamento por você na mesma porta"; **não** o use para criar uma porta dedicada.
3. `listeners[].addr` decide "em qual porta atender externamente", e `prefix` decide "se esta conexão é sua" ——
   os dois devem ser usados em conjunto: **uma porta dedicada também precisa de um `prefix` não vazio** (é também por isso que, na configuração de exemplo do núcleo,
   o `echo-sidecar` escreve tanto `prefix: "ECHO"` quanto `listeners: [":1885"]`).
4. A sondagem corresponde na ordem de registro dos protocolos, e **a primeira correspondência vence**: quando vários plugins coexistem, os prefixos precisam ser distinguíveis (por exemplo, todos começando com o mesmo byte vão se ofuscar mutuamente).

### 5.4 Substituir endereços de escuta e TLS

- O endereço de escuta externo pode ser dado em **dois lugares**: `sidecar.protocols[].listeners[].addr` (padrão) e
  `listeners.<nome do protocolo>` (substitui integralmente pelo nome do protocolo). Quando ambos existem, `listeners.<nome do protocolo>` tem precedência.
- Quando TLS for necessário, forneça o certificado em `listeners.<nome do protocolo>[i].tls` (campos idênticos aos dos protocolos embutidos).
  Abaixo está um trecho de `listeners` (**JSON padrão, não pode conter comentários**): o item 1 é texto claro, o item 2 usa TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/speedmq/tls/cert.pem",
                                 "key_file":  "/etc/speedmq/tls/key.pem" } }
  ]
}
```

> O TLS é terminado pelo **núcleo** no lado do listener; o processo do plugin recebe um fluxo em texto claro, então o plugin não precisa tratar TLS.

### 5.5 Empacotamento: fazer o plugin rodar junto com o núcleo

**Opção A — Embutir na mesma imagem** (recomendado para plugins "publicados junto com o núcleo"): adicione uma linha ao estágio de runtime do `speedmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Depois, na configuração, defina `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
com `address` no mesmo valor. O núcleo o inicia na inicialização.

**Opção B — Montar o binário** (sem alterar a imagem, bom para depuração conjunta):

```yaml
services:
  speedmq:
    image: houzch/speedmq:1.1.05
    command: ["-config", "/etc/speedmq/speedmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/speedmqd.json:/etc/speedmq/speedmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

A configuração usa um unix socket (para evitar ocupar uma porta extra). Abaixo está o trecho `sidecar` dentro de `plugins.my-sidecar`
(**JSON padrão, não pode conter comentários**; `prefix` ainda deve ser não vazio, ver §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Opção C — Contêiner separado** (plugin publicado separadamente / escalado de forma independente):

```yaml
services:
  speedmq:
    image: houzch/speedmq:1.1.05
    volumes: ["./configs/speedmqd.json:/etc/speedmq/speedmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

Na configuração, `spawn: []` (o núcleo apenas conecta, não inicia) e `address: "tcp://my-sidecar:19001"` (o nome de serviço do compose).

### 5.6 Inicialização e verificação

```bash
# 1) 内核日志里应能看到握手与接入
docker compose logs speedmq | grep -E "外部插件已接入|外部插件进程"

# 2) 经 CLI 看插件状态（state=enabled 且 RuntimeNote 为空）
./bin/speedmqctl plugins list
./bin/speedmqctl plugins show my-sidecar

# 3) 经管理 API 看（等价入口）
curl -u guest:guest http://127.0.0.1:15672/api/plugins/my-sidecar

# 4) 实测对外服务：你的协议端口由内核监听，直接连它
#    若你的协议是文本行协议，可以这样冒烟：
printf 'hello\n' | nc 127.0.0.1 19002
```

Em `plugins show`, preste atenção especial a `state` e `RuntimeNote`:
`failed` vem com o motivo da falha (handshake rejeitado / porta não pôde ser aberta…); `down` vem com o motivo da desconexão (processo travou / conexão caiu).

### 5.7 Operações em tempo de execução

| Operação | Comando / API | Efeito |
| --- | --- | --- |
| Desativação a quente | `speedmqctl plugins disable my-sidecar` ou `PUT /api/plugins/my-sidecar/disable` | **Fecha os listeners externos do plugin** (desativação em nível de capacidade); o núcleo e os outros plugins não são afetados |
| Ativação a quente | `speedmqctl plugins enable my-sidecar` | Reabre seus listeners; se uma inicialização anterior falhou, tenta mais uma vez |
| Ver estado | `speedmqctl plugins list/show` | Estado + motivo de falha/desconexão |
| Saída do núcleo | — | Desconecta do plugin, recolhe as sessões na ponte, **termina os processos filhos iniciados pelo núcleo via `spawn`** |

> A desativação a quente apenas fecha a "capacidade" (portas de escuta) e **não** mata o processo do plugin iniciado via `spawn`; o recolhimento do processo acontece quando o núcleo sai.

### 5.8 Fornecer uma entrada no console de gerenciamento (opcional)

Quando o plugin traz sua própria interface, adicione um `console_url` (o endereço da interface de gerenciamento; os demais campos estão na §5.1) à seção `plugins.<nome do plugin>`.
Abaixo apenas a entrada `plugins.my-sidecar` é mostrada (**JSON padrão, não pode conter comentários**; o conteúdo da seção `sidecar` é o mesmo da §5.1):

```json
"plugins": {
  "my-sidecar": {
    "builtin": false,
    "enabled": true,
    "console_url": "http://127.0.0.1:19003/",
    "sidecar": { }
  }
}
```

- A página **Gerenciamento de plugins** do console de gerenciamento (os dados vêm do campo `console_url` de `GET /api/plugins`) exibe
  um botão "Abrir interface de gerenciamento" para esses plugins, que **abre em uma nova aba**.
- Quando `console_url` não é declarado, o botão fica indisponível e uma dica flutuante diz "Este plugin não fornece interface de gerenciamento".
- É apenas **metadados escritos na configuração pelo implantador**: não faz parte da API de plugins (`pkg/plugin`), não participa da ativação/desativação do plugin,
  e a interface em si é hospedada pelo plugin (pode estar no próprio processo do plugin ou em qualquer serviço independente).

---

## 6. Ciclo de vida e matriz de tolerância a falhas

| Cenário | Comportamento do núcleo | Impacto no lado do plugin |
| --- | --- | --- |
| Processo do plugin não iniciado / handshake rejeitado | Tenta reconectar em até 8s; se ainda falhar, marca `failed` e isola (não bloqueia a inicialização do núcleo) | Nenhum |
| Processo iniciado por `spawn` termina | Registra no log; marca `down`; faz backoff e reconecta / reinicia conforme a política `restart` | O novo processo refaz o handshake |
| Processo do plugin trava (em tempo de execução) | O núcleo não é afetado; `down` + reconexão com backoff | `pkg/sidecar.Server` fecha aquela conexão |
| Núcleo é `kill -9`ado | — | O lado do plugin recolhe a conexão por conta própria via timeout de ociosidade (nenhum frame por 24s por padrão), sem deixar zumbis |
| Saída normal do núcleo | Chama `Stop`: desconecta, recolhe entregas não liquidadas (como requeue), `Kill` nos processos filhos | Recebe SIGKILL |
| Entregas enquanto a conexão do plugin está caída | Entregas não liquidadas são sempre **reenfileiradas**, nunca se perdem | — |
| Cliente desconecta / `Open` retorna | Fecha o stream correspondente e libera a sessão e os consumidores daquele stream | `Stream.Read` retorna EOF |

---

## 7. Linhas vermelhas e limites conhecidos

**Linhas vermelhas**

1. Um plugin só pode depender de `pkg/sidecar` (e, opcionalmente, `pkg/plugin`); ele **não deve** depender de `internal/**` do núcleo.
2. O nome do plugin deve corresponder à configuração e a `APIVersion` deve corresponder à do núcleo, senão não é possível conectar (isso evita "rodar em silêncio sem fazer efeito").
3. Ao usar `session.*`: **`core.authenticate` primeiro, depois `session.open`**, e liquide cada entrega **exatamente uma vez**.
4. `protocols[].prefix` deve ser não vazio, senão as conexões não serão entregues ao plugin (ver §5.3).
5. Uma rejeição no `Hello` deve **retornar explicitamente um erro** (não fique em silêncio) —— senão o núcleo só vê "conexão fechada" e não consegue localizar a causa.

**Limites conhecidos**

- **A sondagem fica no lado do núcleo**: plugins externos não podem definir funções de sondagem personalizadas e só podem corresponder por `prefix` (ASCII, ≤ 8 bytes);
  um `prefix` vazio significa "nenhuma conexão pode ser obtida" (ver §5.3).
- **O plano de dados passa por intermediação local**: não há passagem de fd (o Windows não tem `SCM_RIGHTS`), adicionando uma cópia de memória a mais em relação ao in-process;
  cada chamada reversa também adiciona um RPC local a mais.
- **Os tipos da tabela de propriedades degradam**: `Properties.Headers` é retransmitido via JSON, então distinções como `int32` / `double` se perdem (ver §3.4).
- **O backpressure de um único stream afeta a conexão inteira**: quando o buffer de recepção de um stream enche, ele bloqueia a goroutine de despacho da conexão; a limitação de taxa por stream é uma otimização futura.
- **Falhas de autenticação passam apenas como texto**: uma falha de autenticação do núcleo é um `*plugin.AuthError` (uma classificação diferente de `plugin.ErrorKind`),
  e apenas o texto chega ao plugin pela ponte; o plugin precisa mapeá-lo para códigos de erro de protocolo conforme sua própria convenção.
- **Apenas a capacidade `net.listen` realmente tem efeito**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` são **espaços reservados**; declará-los apenas participa da auditoria (ver a exibição de governança na §5.6), e atualmente não há ponto de extensão correspondente.

---

## 8. FAQ de solução de problemas

| Sintoma | Causa e tratamento |
| --- | --- |
| Estado `failed`, motivo contém "plugin name mismatch" | O nome do plugin configurado ≠ `HelloAck.name`; iguale-os |
| Estado `failed`, motivo contém "API version mismatch" | `HelloAck.api_version` ≠ a `APIVersion` do núcleo; iguale-os |
| Estado `failed`, motivo contém "handshake rejected" | O `Hello` do plugin retornou um erro (`deny`); ver a saída do plugin encaminhada no log do núcleo |
| Estado `failed`, motivo contém "failed to connect to external plugin" | O processo não subiu / `address` está errado / o caminho do socket não é gravável (atenção às permissões do usuário `speedmq` em contêineres) |
| Estado `down` | O processo do plugin travou ou a conexão caiu; `restart=always` reconecta automaticamente, enquanto `never` exige reinício manual |
| Porta não aberta / cliente não consegue conectar | `protocols[].listeners` não está configurado ou seu endereço foi substituído por `listeners.<nome do protocolo>`; verifique os dois lugares |
| Cliente conecta em outra porta e é desconectado imediatamente | Essa porta não corresponde ao seu protocolo (`prefix` vazio ou prefixo não correspondente); configure um `prefix` não vazio para o protocolo (ver §5.3) |
| Erro `ACCESS_REFUSED - ... for user ''` | Não houve **autenticação** antes da ponte semântica; chame `core.authenticate` antes de `session.open` |
| A chamada reversa reporta "o stream N ainda não abriu uma sessão" | Faça `core.authenticate` primeiro, depois `session.open`, e só então chame outros `session.*` |
| Nenhuma entrega de consumo recebida | As entregas chegam ao seu `Call` como uma **chamada direta** `session.deliver`; confirme que esse método é tratado |
| Plugin fora do contêiner, núcleo dentro, não conecta | Use `address: tcp://host.docker.internal:<port>` (ou coloque o plugin no contêiner também e use o nome de serviço); o plugin deve escutar em `0.0.0.0` |

---

## 9. Referência (índice do código-fonte)

| O que você quer ver | Arquivo |
| --- | --- |
| Protocolo de linha e implementações dos dois lados (**leitura obrigatória para o desenvolvimento**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frames), `proto.go` (mensagens), `server.go` (lado do plugin), `client.go` (lado do núcleo), `bridge.go` (contrato `session.*`), `stream.go` (streams) |
| Host sidecar do lado do núcleo (integração/reconexão/proxy/estado) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Ponte semântica do lado do núcleo (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Tipos da superfície de operações de sessão do núcleo (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Escuta, sondagem, start-stop a quente por plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Ciclo de vida e governança do plugin (isolamento/estado/auditoria) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Itens de configuração e exemplos (incluindo a seção sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/speedmqd.json`](../../../configs/speedmqd.json) |
| Montagem do processo (como o sidecar é integrado ao núcleo) | [`cmd/speedmqd/main.go`](../../../cmd/speedmqd/main.go) |
| Implementação de referência em Go (usa `pkg/sidecar.Server`, com a ponte `session.*` e `core.authenticate`) | Projeto de teste independente `speedmq-test/test/integration/echosidecar/` |
| **Guias por linguagem + projetos de exemplo** | `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md` deste diretório; os exemplos estão no **workspace** `speedmq-plugin/{python,nodejs,php,java}/` |
