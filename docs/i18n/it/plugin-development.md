# SwiftMQ Guida allo sviluppo dei plugin in processo esterno (sidecar)

> **Destinatari**: sviluppatori che non vogliono fare il fork / ricompilare il kernel ma desiderano estendere SwiftMQ con **qualsiasi linguaggio**.
> **Ambito**: questo documento tratta un solo tipo di plugin —— il **plugin in processo esterno** (termine del kernel `sidecar`). I plugin di protocollo integrati nel kernel (AMQP 0-9-1 / MQTT) non rientrano nell'ambito.
> **Come leggerlo**: le sezioni 1–2 costruiscono il modello mentale, la sezione 3 mostra il codice, e **la sezione 5 spiega "una volta terminato lo sviluppo, come collegarlo ed eseguirlo assieme per erogare il servizio ai client"**;
> **per altri linguaggi (Python / Node.js / PHP / Java) consulta le guide per linguaggio del §4** (ciascuna con un progetto di esempio completo e verificato in pratica).
> Il codice nel documento è uno scheletro minimo eseguibile, da copiare come punto di partenza. Il cinese semplificato è la lingua di origine.

---

## 1. Cos'è

Un **processo indipendente** che implementa un certo "protocollo" (analizza il flusso di byte dei client) nel proprio processo,
il kernel lo ospita in base alla configurazione: **le porte sono aperte dal kernel, le connessioni sono proxate dal kernel**, e registrazione / avvio-arresto / audit / isolamento riutilizzano tutti i meccanismi esistenti del kernel.

Prima stabilisci tre modelli mentali corretti (i punti più facili da sbagliare):

1. **Il processo plugin è un "servizio locale"**: ascolta solo su un **indirizzo locale** (TCP o unix socket) e attende che **il kernel si connetta**.
   La direzione della connessione è **kernel (client) → plugin (server)**, e anche l'handshake è inviato per primo dal kernel.
2. **La porta di servizio esterna non è aperta dal plugin**: è creata dal **kernel** in base a `protocols[].listeners` nella configurazione, e mappata verso l'esterno per i client.
   I client si connettono **alla porta del kernel**, e i byte sono proxati dal kernel al processo plugin. Il processo plugin **non ha bisogno** di aprire da sé una porta di servizio.
3. **La semantica è opzionale**: il plugin può solo "trasportare byte" (implementando il protocollo interamente da sé),
   oppure raggiungere la semantica del kernel (code / routing / permessi / conferme) tramite **chiamate inverse** a `session.*`,
   condividendo **esattamente la stessa semantica** dei plugin di protocollo integrati (vhost, permessi, routing e comportamento delle code quindi non divergono).

| Vantaggi | Costi |
| --- | --- |
| Estendibile senza modificare né ricompilare il kernel | Un passaggio di copia byte locale in più sul piano dati (proxing del kernel, nessun passaggio di fd, coerente tra piattaforme) |
| Implementabile in qualsiasi linguaggio (basta implementare il protocollo di rete) | Una RPC locale in più per ogni chiamata inversa (codifica/decodifica JSON + copia) |
| Il plugin può essere pubblicato / aggiornato / riavviato in modo indipendente | Lo sniffing resta lato kernel: può essere riconosciuto solo per "prefisso" o "porta dedicata" |
| Un crash influisce solo su quel plugin: il kernel lo marca `down`, senza uscire né andare in crash | Solo la capacità `net.listen` ha realmente effetto; altri valori di capacità sono riservati (vedi §7) |

---

## 2. Come funziona

Stabilimento della connessione (**il kernel è il client, il plugin è il server**):

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

Inizio del servizio:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Ciclo di vita** (host `internal/plugin/sidecar` lato kernel):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Semantica degli stati** (visibile con `swiftmqctl plugins show`):

| Stato | Significato | Azione operativa |
| --- | --- | --- |
| `enabled` | Connesso e in servizio | — |
| `failed` | **Non è mai partito all'avvio** (configurazione errata / handshake rifiutato / processo non avviabile) | Controlla `RuntimeNote` e il log del kernel, correggi la configurazione o il plugin; il kernel riprova solo dopo un riavvio |
| `down` | **Era partito, ora non c'è più** (processo andato in crash / connessione caduta) | Vai ad avviare il processo plugin; il kernel recupererà automaticamente secondo la policy `restart` |
| `disabled` | `enabled=false` nella configurazione, oppure disattivato a caldo dall'operatore | Recupera con `plugins enable <nome>` |

---

## 3. Sviluppo (Go)

### 3.1 Creare il progetto

Il plugin è un **modulo Go indipendente** che dipende solo da due pacchetti di contratto pubblici:

- `github.com/houzch/swiftmq/pkg/sidecar` —— il protocollo di rete e l'implementazione lato plugin (**obbligatorio**)
- `github.com/houzch/swiftmq/pkg/plugin` —— solo se ti servono tipi come `plugin.Message` / `plugin.Error` (opzionale)

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/swiftmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/swiftmq@v1.1.03
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/swiftmq=../swiftmq
```

> Quando usi `replace` per il debug congiunto, plugin e kernel devono usare **lo stesso albero dei sorgenti**; altrimenti la versione dell'API (`v1`) può coincidere ma i tipi differire.

### 3.2 Implementare `Handler` (tre metodi)

L'intera superficie di business del processo plugin è `Hello` / `Call` / `Open` di `sidecar.Handler`.
Handshake, heartbeat, multiplexing e suddivisione in blocchi sono gestiti da `pkg/sidecar`; non devi mai toccare i frame.

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

	"github.com/houzch/swiftmq/pkg/sidecar"
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

### 3.3 Piano dati: leggere e scrivere uno `Stream`

`sidecar.Stream` implementa `io.ReadWriteCloser`, quindi trattalo semplicemente come "una connessione":

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

Punti chiave:

- Ogni connessione client = uno stream; il plugin può iniziare subito a inviare/ricevere dentro `Handler.Open`.
- I messaggi grandi sono suddivisi automaticamente dalla libreria (ogni frame ≤ 64 KiB), e **l'occupazione di memoria è indipendente dalla dimensione del messaggio**.
- Backpressure: il buffer di ricezione di un singolo stream ha un limite massimo; quando il buffer è pieno, viene bloccata la **goroutine di dispatch** di quella connessione (tutti gli stream attendono assieme) ——
  questo è il compromesso tra "memoria prevedibile" e "limitazione di velocità per stream", vedi §7.

### 3.4 Usare il ponte semantico del kernel (`session.*`)

Se vuoi che il plugin riutilizzi la semantica di code / routing / permessi / conferme del kernel (invece di costruirne una propria), usa le **chiamate inverse**.
Su uno stream usale nell'ordine `session.open` → altri `session.*` → (`session.close`):

```go
import (
	"errors"

	"github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
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

**Chiamate inverse a colpo d'occhio** (costanti in `pkg/sidecar/bridge.go` → nomi sul filo):

| Gruppo | Nome sul filo (costante) | Descrizione |
| --- | --- | --- |
| Autenticazione | `core.authenticate`（`MethodCoreAuthenticate`） | **Va fatto per primo**: consegna le credenziali del tuo protocollo al kernel per la verifica; parametri `{stream, mechanism, response}`, restituisce `{user}` |
| Sessione | `session.open`（`MethodSessionOpen`） | Apre una sessione per un certo vhost sullo stream; si può fare **solo dopo l'autenticazione** |
| | `session.close`（`MethodSessionClose`） | Rilascia la sessione sullo stream (annulla i consumer, elimina le code esclusive) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Crea / elimina (dichiarazione passiva di uno inesistente → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Binding exchange-to-exchange |
| Coda | `session.declare_queue` / `session.delete_queue` | Crea / elimina; se `name` è vuoto il server ne genera uno |
| | `session.bind_queue` / `session.unbind_queue` | Binding coda-exchange |
| | `session.purge_queue` | Svuota i messaggi pronti (esclusi quelli non confermati) |
| Pubblicazione | `session.publish` | Restituisce `{routed, rejected}`; la persistenza si completa prima della risposta |
| Get | `session.get` | Preleva attivamente un messaggio; `found=false` significa coda vuota |
| Consumo | `session.consume` / `session.cancel` | Registra / annulla consumer |
| Liquidazione | `session.settle` | Liquida una consegna (`ack` / `requeue` / `reject`) |
| **Diretta** | `session.deliver` | **Kernel → plugin**: rinvio di una consegna (da gestire nel tuo `Handler.Call`) |

**Quattro regole da rispettare**:

1. **Prima `core.authenticate`**: la superficie operativa kernel della connessione non ha identità prima dell'autenticazione,
   quindi `session.open` viene rifiutato (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Il plugin è responsabile di estrarre le credenziali dal proprio protocollo; la logica di autenticazione e la tabella utenti restano nel kernel, il plugin non tocca mai l'archivio delle password.
2. **Poi `session.open`**: chiamare altri metodi senza aver aperto la sessione fa restituire dal kernel `KindPreconditionFailed` ("stream N has not opened a session").
3. **Liquidare ogni consegna esattamente una volta**: scegli uno tra `Ack` / `Requeue` / `Reject`.
   Sia `Ack` che `Reject` scartano il messaggio, ma **solo `Reject` va nel percorso dei messaggi morti**.
4. **Le consegne non liquidate non vanno perse**: quando uno stream termina (il client si disconnette / `Handler.Open` ritorna) o la connessione del plugin cade,
   il kernel tratta tutte le consegne non liquidate **come "rimesse in coda"**, evitando che i messaggi restino bloccati.

**Ripristino degli errori**: il `*plugin.Error` del kernel arriva attraverso il ponte come `*sidecar.RPCError` (campi `Kind` / `Text`),
e può essere ripristinato in un `plugin.Error` categorizzato, invece di perdere la categoria in una stringa:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Mappatura dei `Kind` comuni verso i protocolli integrati (per aiutarti a decidere come restituire l'errore al client):

| `plugin.ErrorKind` | Semantica | Mappatura AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | L'oggetto non esiste | 404 NOT_FOUND (chiudi Channel) |
| `KindPreconditionFailed` | Parametri incoerenti con un oggetto esistente / sessione non aperta | 406 PRECONDITION_FAILED (chiudi Channel) |
| `KindAccessRefused` | Permessi insufficienti / nome riservato | 403 ACCESS_REFUSED (chiudi Channel) |
| `KindResourceLocked` | Risorsa esclusiva occupata | 405 RESOURCE_LOCKED (chiudi Channel) |
| `KindInvalidPath` | Il vhost non esiste | 402 INVALID_PATH (chiudi connessione) |
| `KindNotImplemented` | Capacità non implementata | 540 NOT_IMPLEMENTED (chiudi connessione) |
| `KindInternal` | Errore interno del kernel | 541 INTERNAL_ERROR (chiudi connessione) |

**Confini della fedeltà dei tipi dei messaggi**: la tabella delle proprietà (`MessageDTO.Properties.Headers`) passa attraverso JSON,
quindi **le informazioni sul tipo numerico che distinguono `int32` / `double`, come in un field-table AMQP, non sono disponibili**.
Quando serve una fedeltà di tipo rigorosa, porta queste informazioni da te nel corpo del messaggio (byte grezzi).

### 3.5 Stato e log lato plugin

- Lo **stato di un plugin esterno è determinato dal fatto che la connessione sia viva**, e il plugin non deve auto-dichiararlo (lo `StateReporter` dei plugin integrati non si applica ai processi esterni).
- Log: `sidecar.ServerOptions.Logger` scrive su stdout/stderr del processo plugin;
  **quando viene avviato dal kernel tramite `spawn`, questi output vengono inoltrati dal kernel nel log del kernel** (con tag `plugin`), facilitando la raccolta centralizzata.
- In un deployment autonomo (senza spawn), raccogli i log del plugin a modo tuo.

### 3.6 Autotest senza il kernel

`Handler` è una normale interfaccia Go; in uno unit test puoi istanziarla direttamente e chiamare `Hello` / `Call` / `Open` per coprire la logica di business, senza avviare alcuna rete:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Per la verifica end-to-end vedi §5.

---

## 4. Sviluppo in altri linguaggi (specifica del protocollo di rete)

`pkg/sidecar` è un contratto pubblico a dipendenze zero; il protocollo di rete in sé è semplice e può essere implementato in qualsiasi linguaggio.
Per integrarti, devi implementare le seguenti convenzioni "a livello di byte" (il sorgente è in `pkg/sidecar/frame.go`, `proto.go`).

> **Sono disponibili guide per linguaggio con progetti di esempio completi** (gli esempi hanno tutti superato la verifica pratica: handshake → autenticazione → ponte semantico → consegna/liquidazione → flusso di byte):
>
> | Linguaggio | Guida | Progetto di esempio (workspace `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (sola libreria standard) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (sola libreria standard) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (sola libreria standard) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (file singolo, solo JDK) |
>
> Per l'implementazione di riferimento completa in Go, vedi il progetto di test autonomo `swiftmq-test/test/integration/echosidecar/` (usa direttamente `pkg/sidecar.Server`,
> quindi non devi preoccuparti dei dettagli a livello di byte riportati sotto).

**Formato dei frame** (uniforme per tutti i frame):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Tipi di frame `kind`**:

| kind | Nome | Direzione | Payload |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | vuoto |
| 4 | Pong | plugin → kernel | vuoto |
| 5 | Call | bidirezionale | JSON `Call` |
| 6 | Reply | bidirezionale | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | bidirezionale | `u32 BE stream` + byte grezzi |
| 10 | Close | bidirezionale | JSON `Close` |

**Strutture JSON del piano di controllo** (i nomi dei campi coincidono con `proto.go`).

> Questa parte è un **esempio di messaggi del protocollo di rete** (più messaggi sono dati in ordine nello stesso blocco, perciò i separatori `//` servono da spiegazione),
> e **non è una configurazione scrivibile direttamente in `swiftmqd.json`**.

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

**Semantiche da rispettare**:

- **Handshake**: il kernel invia per primo `Hello`, e il plugin deve rispondere con un frame `HelloAck`.
  Il kernel verifica `HelloAck.name == il nome del plugin nella configurazione` e `HelloAck.api_version == l'APIVersion del kernel`;
  un `deny` non vuoto è considerato un rifiuto della connessione (il plugin viene isolato).
- **Heartbeat**: per impostazione predefinita il kernel invia `Ping` ogni 2s, e il plugin deve rispondere `Pong` entro 8s; lato plugin, se non riceve alcun frame entro 24s può chiudere da sé la connessione.
- **Due spazi di ID**: il `reverse` di `Call` distingue la direzione, e ciascuna direzione incrementa da 1 in modo indipendente,
  quindi `Reply` **deve riportare `reverse`**, altrimenti la risposta verrà consegnata al waiter sbagliato.
- **Nessun base64 sul piano dati**: i dati di grandi dimensioni come i corpi dei messaggi vanno direttamente nel payload del frame `Data` (`stream` + byte grezzi), suddivisi in blocchi secondo necessità.

> Se usi Go, usa direttamente `pkg/sidecar` e non dovrai implementare nessuno dei dettagli sopra.

---

## 5. Metterlo in funzione assieme: integrazione, servizio ai client, packaging ★

Questa sezione risponde a "una volta terminato lo sviluppo, come integrarlo in SwiftMQ e come erogare il servizio ai client".

### 5.1 Dichiarare il plugin nella configurazione

Un plugin esterno è **interamente gestito dalla configurazione**, e il kernel non necessita di alcuna modifica al codice per esso. Aggiungi una voce alla sezione `plugins` di `swiftmqd.json`
(**la configurazione effettiva è JSON standard, non può contenere commenti**):

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

Spiegazione voce per voce (l'elenco dei campi è nella tabella sotto):

- Il nome della chiave di `plugins.<nome del plugin>` **deve coincidere con l'`HelloAck.name` auto-dichiarato dal plugin**, altrimenti l'handshake viene rifiutato.
- `builtin: false`: dichiara esplicitamente un plugin esterno (se omesso, il piano di gestione lo mostra come integrato).
- `enabled`: disattivarlo = non avviare il processo, non creare il listener.
- Con `required: true`, un fallimento all'avvio blocca l'avvio del kernel —— non attivarlo per i plugin esterni.
- `address` è **l'indirizzo a cui il kernel si connette** (il kernel è il client); quando `spawn` è non vuoto, il kernel avvia il processo al posto tuo.
- `protocols[].prefix` **deve essere non vuoto** (regole di sniffing vedi §5.3).
- `listeners` sono le **porte esterne di quel protocollo, aperte dal kernel** (i client si connettono al kernel).

Elenco dei campi:

| Campo | Obbligatorio | Descrizione |
| --- | --- | --- |
| `sidecar.address` | ✅ | Indirizzo del processo plugin: `tcp://host:port` o `unix:///path` |
| `sidecar.spawn` | ✕ | Riga di comando che il kernel avvia al posto tuo (il primo elemento è l'eseguibile); **vuoto = il kernel si connette soltanto e non avvia**, il processo lo gestisci tu |
| `sidecar.restart` | ✕ | `always` (predefinito, recupero automatico dopo disconnessione/crash) oppure `never` (solo marca `down`, in attesa dell'intervento dell'operatore) |
| `sidecar.protocols[].name` | ✅ | Nome del protocollo (globalmente univoco, partecipa alla priorità di sniffing) |
| `sidecar.protocols[].prefix` | ✕ | Prefisso di sniffing (ASCII); **vuoto = non partecipa allo sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Listener esterno di quel protocollo (`name` + `addr`), creato dal kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | Sovrascrive il timeout dell'handshake (predefinito 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Sovrascrive l'intervallo di heartbeat (predefinito 2s) |

> **Nome del plugin e nome del protocollo**: i due **possono differire** (ad esempio il plugin `my-sidecar` fornisce il protocollo `myproto`).
> La disattivazione a caldo dapprima individua tutti i protocolli registrati con il nome del plugin, poi chiude le porte di quei protocolli, quindi non serve usare deliberatamente lo stesso nome.

### 5.2 Tre modalità di integrazione

| Modalità | Configurazione | Quando usarla |
| --- | --- | --- |
| **Stessa macchina + avvio da parte del kernel (spawn)** | `spawn: [...]`, `address` punta all'indirizzo su cui ascolta | Deployment sulla stessa macchina, container singolo; la più semplice, il kernel si occupa dell'avvio e della bonifica |
| **Stessa macchina + gestione autonoma (dial)** | `spawn: []`, `address` punta a un processo già in esecuzione | Gestisci il ciclo di vita del plugin con systemd / supervisor |
| **Tra host / tra container (dial, deve essere tcp)** | `spawn: []`, `address: "tcp://<nome del servizio>:19001"` | Plugin e kernel distribuiti in container / macchine separati |

Scelta dell'indirizzo:

- **Sulla stessa macchina si consiglia un unix socket** (`unix:///tmp/my-sidecar.sock`): non occupa una porta TCP e non è influenzato dall'occupazione delle porte dell'host.
  Nota che il percorso del socket deve essere scrivibile dal processo kernel (in un container, l'utente `swiftmq` non root).
- **Tra container è obbligatorio TCP**, e il processo plugin deve ascoltare su `0.0.0.0`, con `address` che usa **il nome del servizio nella rete del container**.

> Non invertire la direzione: **l'indirizzo su cui il plugin ascolta** = `address`; **la porta esposta ai client** = `protocols[].listeners`.

### 5.3 Erogare il servizio ai client: riconoscimento tramite `prefix`

Quando il livello di accesso distribuisce le connessioni, **guarda solo il risultato dello sniffing**: per ogni protocollo abilitato interroga `Sniff(peek)` nell'ordine di registrazione (peek è al massimo 8 byte),
e chi corrisponde prende in carico la connessione. Quindi:

1. **`prefix` deve essere non vuoto** (ASCII, ≤ 8 byte). Solo quando i primi byte inviati dal client coincidono con esso la connessione viene ceduta al tuo plugin.
   Esempio: `"prefix": "PY"` → il primo byte del client deve essere `PY` (puoi trattare il prefisso come l'header magico del tuo protocollo).
2. **Un `prefix` vuoto significa che non partecipa allo sniffing**: tali connessioni **non** vengono cedute al plugin (verificato in pratica: le connessioni sulla porta di ascolto vengono chiuse immediatamente).
   Quindi un `prefix` vuoto è adatto solo allo scenario "un altro protocollo farà il forwarding al posto tuo sulla stessa porta"; **non** usarlo per costruire una porta dedicata.
3. `listeners[].addr` decide "su quale porta erogare il servizio verso l'esterno", e `prefix` decide "se questa connessione è tua" ——
   i due vanno usati assieme: **anche una porta dedicata necessita di un `prefix` non vuoto** (è anche il motivo per cui nella configurazione di esempio del kernel
   `echo-sidecar` scrive sia `prefix: "ECHO"` sia `listeners: [":1885"]`).
4. Lo sniffing corrisponde nell'ordine di registrazione dei protocolli, e **il primo che corrisponde vince**: quando più plugin coesistono, i prefissi devono essere distinguibili (ad esempio, tutti che iniziano con lo stesso byte si oscurerebbero a vicenda).

### 5.4 Sovrascrivere gli indirizzi di ascolto e TLS

- L'indirizzo di ascolto esterno può essere dato in **due posti**: `sidecar.protocols[].listeners[].addr` (predefinito) e
  `listeners.<nome del protocollo>` (sovrascrittura complessiva per nome del protocollo). Quando entrambi esistono, prevale `listeners.<nome del protocollo>`.
- Quando serve TLS, fornisci il certificato in `listeners.<nome del protocollo>[i].tls` (campi identici ai protocolli integrati).
  Sotto c'è un frammento di `listeners` (**JSON standard, non può contenere commenti**): la voce 1 è in chiaro, la voce 2 usa TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS è terminato dal **kernel** lato listener; il processo plugin riceve uno stream in chiaro —— il plugin non deve gestire TLS.

### 5.5 Packaging: far girare il plugin assieme al kernel

**Approccio A —— includerlo nella stessa immagine** (consigliato per i plugin "rilasciati assieme al kernel"): aggiungi una riga alla fase runtime di `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Poi nella configurazione imposta `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
con `address` allo stesso valore. Il kernel lo avvierà all'avvio.

**Approccio B —— montare il binario** (senza modificare l'immagine, adatto al debug congiunto):

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.03
    command: ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

La configurazione usa un unix socket (per evitare di occupare una porta aggiuntiva). Sotto c'è il frammento `sidecar` dentro `plugins.my-sidecar`
(**JSON standard, non può contenere commenti**; `prefix` deve comunque essere non vuoto, vedi §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Approccio C —— container separato** (plugin rilasciato separatamente / scalato in modo indipendente):

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.03
    volumes: ["./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

Nella configurazione `spawn: []` (il kernel si connette soltanto, non avvia) e `address: "tcp://my-sidecar:19001"` (il nome del servizio compose).

### 5.6 Avvio e verifica

```bash
# 1) 内核日志里应能看到握手与接入
docker compose logs swiftmq | grep -E "外部插件已接入|外部插件进程"

# 2) 经 CLI 看插件状态（state=enabled 且 RuntimeNote 为空）
./bin/swiftmqctl plugins list
./bin/swiftmqctl plugins show my-sidecar

# 3) 经管理 API 看（等价入口）
curl -u guest:guest http://127.0.0.1:15672/api/plugins/my-sidecar

# 4) 实测对外服务：你的协议端口由内核监听，直接连它
#    若你的协议是文本行协议，可以这样冒烟：
printf 'hello\n' | nc 127.0.0.1 19002
```

In `plugins show`, presta particolare attenzione a `state` e `RuntimeNote`:
`failed` riporta la causa del fallimento (handshake rifiutato / porta non avviabile…); `down` riporta la causa della disconnessione (processo andato in crash / connessione caduta).

### 5.7 Operazioni a runtime

| Operazione | Comando / API | Effetto |
| --- | --- | --- |
| Disattivazione a caldo | `swiftmqctl plugins disable my-sidecar` oppure `PUT /api/plugins/my-sidecar/disable` | **Chiude i listener esterni di quel plugin** (disattivazione a livello di capacità); il kernel e gli altri plugin non ne sono influenzati |
| Attivazione a caldo | `swiftmqctl plugins enable my-sidecar` | Riapre i suoi listener; se un precedente avvio era fallito, ritenta una volta |
| Visualizza stato | `swiftmqctl plugins list/show` | Stato + causa di fallimento/disconnessione |
| Uscita del kernel | — | Disconnette dal plugin, bonifica le sessioni sul ponte, **termina i processi figli avviati dal kernel tramite `spawn`** |

> La disattivazione a caldo chiude solo la "capacità" (le porte di ascolto) e **non** uccide il processo plugin avviato con `spawn`; la bonifica del processo avviene all'uscita del kernel.

### 5.8 Fornire una voce nella console di gestione (opzionale)

Quando il plugin è dotato di una propria interfaccia, aggiungi un `console_url` (l'indirizzo dell'interfaccia di gestione; per gli altri campi vedi §5.1) nella sezione `plugins.<nome del plugin>`.
Sotto è mostrata solo la voce `plugins.my-sidecar` (**JSON standard, non può contenere commenti**; il contenuto della sezione `sidecar` è lo stesso del §5.1):

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

- La pagina **Gestione plugin** della console di gestione (i dati provengono dal campo `console_url` di `GET /api/plugins`) mostra per questi plugin
  un pulsante "Apri interfaccia di gestione", che **si apre in una nuova scheda**.
- Se `console_url` non è dichiarato, il pulsante non è disponibile e il tooltip dice "Questo plugin non fornisce un'interfaccia di gestione".
- È solo **metadato scritto nella configurazione dall'installatore**: non fa parte dell'API del plugin (`pkg/plugin`), non partecipa all'avvio/arresto del plugin,
  e l'interfaccia stessa è ospitata dal plugin (può stare nel processo plugin o in qualsiasi servizio autonomo).

---

## 6. Ciclo di vita e matrice di tolleranza ai guasti

| Scenario | Comportamento del kernel | Impatto sul lato plugin |
| --- | --- | --- |
| Processo plugin non avviato / handshake rifiutato | Ritenta la connessione entro 8s; se fallisce ancora lo marca `failed` e lo isola (non blocca l'avvio del kernel) | Nessuno |
| Un processo avviato da `spawn` termina | Lo registra nel log; marca `down`; con backoff riconnette / riavvia secondo la policy `restart` | Il nuovo processo esegue di nuovo l'handshake |
| Il processo plugin va in crash (a runtime) | Il kernel non ne è influenzato; `down` + riconnessione con backoff | `pkg/sidecar.Server` chiuderà quella connessione |
| Il kernel riceve `kill -9` | — | Il lato plugin bonifica la connessione da sé tramite timeout di inattività (nessun frame per 24s di default), senza lasciare zombie |
| Il kernel esce normalmente | Chiama `Stop`: disconnette, bonifica le consegne non liquidate (come requeue), `Kill` dei processi figli | Riceve SIGKILL |
| Consegne durante la disconnessione del plugin | Le consegne non liquidate vengono sempre **rimesse in coda**, mai perse | — |
| Il client si disconnette / `Open` ritorna | Chiude lo stream corrispondente e rilascia la sessione e i consumer di quello stream | `Stream.Read` restituisce EOF |

---

## 7. Linee rosse e confini noti

**Linee rosse**

1. Un plugin può dipendere solo da `pkg/sidecar` (e opzionalmente da `pkg/plugin`); **non deve** dipendere da `internal/**` del kernel.
2. Il nome del plugin deve coincidere con la configurazione e `APIVersion` deve coincidere con il kernel, altrimenti non può connettersi (questo previene "l'esecuzione silenziosa senza effetto").
3. Quando usi `session.*`: **prima `core.authenticate`, poi `session.open`**, e liquida ogni consegna **esattamente una volta**.
4. `protocols[].prefix` deve essere non vuoto, altrimenti le connessioni non verranno cedute al plugin (vedi §5.3).
5. Un rifiuto in `Hello` deve **restituire esplicitamente un errore** (non restare silenzioso) —— altrimenti il kernel vede solo "connessione chiusa" e non riesce a individuarne la causa.

**Confini noti**

- **Lo sniffing è lato kernel**: i plugin esterni non possono definire funzioni di sniffing personalizzate e possono solo corrispondere tramite `prefix` (ASCII, ≤ 8 byte);
  un `prefix` vuoto significa "nessuna connessione ottenibile" (vedi §5.3).
- **Il piano dati passa per il proxing locale**: nessun passaggio di fd (Windows non ha `SCM_RIGHTS`), una copia di memoria in più rispetto all'in-process;
  anche ogni chiamata inversa aggiunge una RPC locale in più.
- **I tipi della tabella delle proprietà degradano**: `Properties.Headers` passa attraverso JSON, quindi distinzioni come `int32` / `double` vanno perse (vedi §3.4).
- **La backpressure di un singolo stream influisce sull'intera connessione**: quando il buffer di ricezione di uno stream è pieno, blocca la goroutine di dispatch di quella connessione; la limitazione di velocità per stream è un'ottimizzazione futura.
- **I fallimenti di autenticazione passano solo come testo**: un fallimento di autenticazione del kernel è un `*plugin.AuthError` (una classificazione diversa da `plugin.ErrorKind`),
  e attraverso il ponte al plugin arriva solo il testo; il plugin deve mapparlo in codici di errore di protocollo secondo la propria convenzione.
- **Solo la capacità `net.listen` ha realmente effetto**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` sono **slot riservati**; dichiararli partecipa solo all'audit (vedi la visualizzazione di governance al §5.6), e attualmente non esiste un punto di estensione corrispondente.

---

## 8. FAQ di troubleshooting

| Sintomo | Causa e gestione |
| --- | --- |
| Stato `failed`, motivo contiene "nome del plugin non corrispondente" | Il nome del plugin configurato ≠ `HelloAck.name`; allineali |
| Stato `failed`, motivo contiene "versione API non corrispondente" | `HelloAck.api_version` ≠ `APIVersion` del kernel; allineali |
| Stato `failed`, motivo contiene "handshake rifiutato" | Il `Hello` del plugin ha restituito un errore (`deny`); controlla l'output del plugin inoltrato nel log del kernel |
| Stato `failed`, motivo contiene "connessione al plugin esterno fallita" | Il processo non è partito / `address` errato / percorso del socket non scrivibile (nei container attenzione ai permessi dell'utente `swiftmq`) |
| Stato `down` | Il processo plugin è andato in crash o la connessione è caduta; `restart=always` riconnette automaticamente, `never` richiede l'avvio manuale |
| Porta non aperta / il client non riesce a connettersi | `protocols[].listeners` non configurato o il suo indirizzo è sovrascritto da `listeners.<nome del protocollo>`; controlla entrambi i posti |
| Il client si connette a un'altra porta e viene disconnesso subito | Quella porta non corrisponde al tuo protocollo (`prefix` vuoto o prefisso non corrispondente); configura un `prefix` non vuoto per il protocollo (vedi §5.3) |
| Errore `ACCESS_REFUSED - ... for user ''` | **Nessuna autenticazione** prima del ponte semantico; chiama `core.authenticate` prima di `session.open` |
| La chiamata inversa riporta "stream N has not opened a session" | Fai prima `core.authenticate`, poi `session.open`, e solo dopo chiama gli altri `session.*` |
| Nessuna consegna di consumo ricevuta | Le consegne arrivano al tuo `Call` come **chiamata diretta** `session.deliver`; verifica che quel metodo sia gestito |
| Il plugin è fuori dal container, il kernel dentro, non riesce a connettersi | Usa `address: tcp://host.docker.internal:<port>` (oppure metti anche il plugin nel container e usa il nome del servizio); il plugin deve ascoltare su `0.0.0.0` |

---

## 9. Riferimenti (indice dei sorgenti)

| Cosa vuoi vedere | File |
| --- | --- |
| Protocollo di rete e implementazioni dei due lati (**lettura obbligatoria per lo sviluppo**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frame), `proto.go` (messaggi), `server.go` (lato plugin), `client.go` (lato kernel), `bridge.go` (contratto `session.*`), `stream.go` (stream) |
| Host sidecar lato kernel (integrazione/riconnessione/proxing/stato) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Ponte semantico lato kernel (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Tipi della superficie operativa di sessione del kernel (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Ascolto, sniffing, avvio/arresto a caldo per plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Ciclo di vita e governance del plugin (isolamento/stato/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Voci di configurazione ed esempi (inclusa la sezione sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Assemblaggio del processo (come il sidecar viene collegato al kernel) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Implementazione di riferimento Go (usa `pkg/sidecar.Server`, con il ponte `session.*` e `core.authenticate`) | Progetto di test autonomo `swiftmq-test/test/integration/echosidecar/` |
| **Guide per linguaggio + progetti di esempio** | In questa directory `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; gli esempi sono nel **workspace** `swiftmq-plugin/{python,nodejs,php,java}/` |
