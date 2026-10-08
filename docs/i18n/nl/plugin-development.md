# Ontwikkelgids voor SpeedMQ externe-procesplugins (sidecar)

> **Doelgroep**: ontwikkelaars die de kernel niet willen forken / hercompileren, maar SpeedMQ in **elke taal** willen uitbreiden.
> **Scope**: dit document behandelt slechts één pluginvorm — de **externe-procesplugin** (kernterm `sidecar`). Ingebouwde protocolplugins van de kernel (AMQP 0-9-1 / MQTT) vallen buiten dit document.
> **Hoe te lezen**: secties 1–2 bouwen het mentale model op, sectie 3 gaat over code schrijven, en **sectie 5 gaat over "hoe je het na ontwikkeling aansluit en meelaat draaien om clients te bedienen"**;
> **voor andere talen (Python / Node.js / PHP / Java) zie de taalspecifieke gidsen in §4** (elk met een volledig, empirisch getest voorbeeldproject).
> De code in dit document is een minimaal uitvoerbaar skelet dat je direct als startpunt kunt kopiëren. Vereenvoudigd Chinees is de brontaal.

---

## 1. Wat het is

Een **losstaand proces** dat in zijn eigen proces een bepaald "protocol" implementeert (de bytestroom van clients parseert),
en dat de kernel volgens de configuratie beheert: **poorten worden door de kernel geopend, verbindingen worden door de kernel geproxyd**, en registratie / start-stop / auditing / isolatie hergebruiken allemaal bestaande kernelmechanismen.

Stel eerst drie correcte mentale modellen vast (de punten die het gemakkelijkst fout gaan):

1. **Het pluginproces is een "lokale service"**: het luistert alleen op een **lokaal adres** (TCP of unix socket) en wacht tot **de kernel verbinding maakt**.
   De verbindingsrichting is **kernel (client) → plugin (server)**, en ook de handshake wordt door de kernel als eerste verzonden.
2. **De externe bedrijfspoort wordt niet door de plugin geopend**: die wordt door **de kernel** aangemaakt volgens `protocols[].listeners` in de configuratie en naar clients gemapt.
   Clients verbinden met **de poort van de kernel**, en de bytes worden door de kernel naar het pluginproces geproxyd. Het pluginproces **hoeft** zelf geen bedrijfspoort te openen.
3. **Semantiek is optioneel**: de plugin kan alleen "bytes verplaatsen" (het protocol volledig zelf implementeren),
   of via **omgekeerde aanroepen** naar `session.*` de kernelsemantiek bereiken (wachtrijen / routing / rechten / bevestiging),
   met **exact dezelfde semantiek** als ingebouwde protocolplugins (waardoor vhost, rechten, routing en wachtrijgedrag niet uiteenlopen).

| Voordelen | Kosten |
| --- | --- |
| Uitbreiden zonder de kernel te wijzigen of te hercompileren | Eén extra lokale byte-kopie op het datavlak (kernel-proxying, geen fd-doorgifte, platformonafhankelijk consistent) |
| In elke taal te implementeren (je hoeft alleen het wire-protocol te implementeren) | Eén extra lokale RPC per omgekeerde aanroep (JSON-codering/decodering + kopie) |
| De plugin kan onafhankelijk worden uitgebracht / geüpgraded / herstart | Sniffing blijft aan de kernelzijde: alleen herkenbaar via "prefix" of een "toegewijde poort" |
| Een crash treft alleen die plugin: de kernel markeert `down` en stopt of crasht niet | Alleen de capaciteit `net.listen` werkt echt; andere capaciteitswaarden zijn gereserveerd (zie §7) |

---

## 2. Hoe het werkt

Verbinding tot stand brengen (**de kernel is de client, de plugin is de server**):

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

Service start:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Levenscyclus** (de host `internal/plugin/sidecar` aan de kernelzijde):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Statussemantiek** (zichtbaar via `speedmqctl plugins show`):

| Status | Betekenis | Beheerdersactie |
| --- | --- | --- |
| `enabled` | Verbonden en in bedrijf | — |
| `failed` | **Kwam bij het opstarten niet op** (configuratiefout / handshake geweigerd / proces kon niet worden gestart) | Bekijk `RuntimeNote` en het kernellog, pas de configuratie aan of repareer de plugin; de kernel probeert het pas opnieuw na een herstart |
| `down` | **Was eerder op, is nu weg** (proces gecrasht / verbinding verbroken) | Start het pluginproces op; de kernel herstelt automatisch volgens het `restart`-beleid |
| `disabled` | `enabled=false` in de configuratie, of hot uitgeschakeld door de beheerder | Herstel met `plugins enable <naam>` |

---

## 3. Ontwikkeling (Go)

### 3.1 Project aanmaken

De plugin is een **losstaande Go-module** die slechts van twee publieke contractpakketten afhankelijk is:

- `github.com/houzch/speedmq/pkg/sidecar` — het wire-protocol en de plugin-zijde-implementatie (**verplicht**)
- `github.com/houzch/speedmq/pkg/plugin` — alleen wanneer je typen zoals `plugin.Message` / `plugin.Error` nodig hebt (optioneel)

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

> Bij gebruik van `replace` voor gezamenlijk debuggen moeten de plugin en de kernel **dezelfde broncode** gebruiken, anders komt de API-versie (`v1`) wel overeen, maar kunnen de typen verschillen.

### 3.2 `Handler` implementeren (drie methoden)

Het volledige zakelijke oppervlak van het pluginproces is `sidecar.Handler` met `Hello` / `Call` / `Open`.
Handshake, heartbeat, multiplexing en chunking worden allemaal door `pkg/sidecar` afgehandeld; je hoeft nooit frames aan te raken.

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

### 3.3 Datavlak: een `Stream` lezen en schrijven

`sidecar.Stream` implementeert `io.ReadWriteCloser`, dus behandel het gewoon als "een verbinding":

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

Belangrijke punten:

- Elke clientverbinding = één stream; de plugin kan direct in `Handler.Open` beginnen met verzenden/ontvangen.
- Grote berichten worden automatisch door de library gechunkt (elk frame ≤ 64 KiB), en **het geheugengebruik is onafhankelijk van de berichtgrootte**.
- Backpressure: de ontvangstbuffer van een enkele stream heeft een bovengrens; wanneer de buffer vol is, wordt de **dispatch-goroutine** van die verbinding geblokkeerd (alle streams wachten samen) —
  dit is de afweging tussen "voorspelbaar geheugen" en "snelheidsbeperking per stream", zie §7 voor details.

### 3.4 De kernelsemantiekbrug gebruiken (`session.*`)

Wil je dat de plugin de wachtrij- / routing- / rechten- / bevestigingssemantiek van de kernel hergebruikt (in plaats van zelf iets te bouwen), gebruik dan **omgekeerde aanroepen**.
Gebruik ze op een stream in de volgorde `session.open` → andere `session.*` → (`session.close`):

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

**Omgekeerde aanroepen in één oogopslag** (constanten in `pkg/sidecar/bridge.go` → wire-namen):

| Groep | Wire-naam (constante) | Beschrijving |
| --- | --- | --- |
| Auth | `core.authenticate` (`MethodCoreAuthenticate`) | **Moet eerst**: geef de inloggegevens uit je protocol aan de kernel ter verificatie; parameters `{stream, mechanism, response}`, retourneert `{user}` |
| Sessie | `session.open` (`MethodSessionOpen`) | Opent een sessie voor een vhost op de stream; kan alleen **na authenticatie** |
| | `session.close` (`MethodSessionClose`) | Geeft de sessie op de stream vrij (annuleert consumers, verwijdert exclusieve wachtrijen) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Toevoegen / verwijderen (passief declareren van een niet-bestaande → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Exchange-naar-exchange-bindingen |
| Wachtrij | `session.declare_queue` / `session.delete_queue` | Toevoegen / verwijderen; de server genereert er een als `name` leeg is |
| | `session.bind_queue` / `session.unbind_queue` | Wachtrij-naar-exchange-bindingen |
| | `session.purge_queue` | Maakt gereedstaande berichten leeg (exclusief onbevestigde) |
| Publiceren | `session.publish` | Retourneert `{routed, rejected}`; persistentie voltooit vóór het antwoord |
| Ophalen | `session.get` | Haalt actief één bericht op; `found=false` betekent dat de wachtrij leeg is |
| Consume | `session.consume` / `session.cancel` | Registreren / annuleren van consumers |
| Settle | `session.settle` | Settelt een bezorging (`ack` / `requeue` / `reject`) |
| **Forward** | `session.deliver` | **Kernel → plugin**: duwt een bezorging terug (afgehandeld in je `Handler.Call`) |

**Vier regels die je moet volgen**:

1. **Eerst `core.authenticate`**: het kernel-operatievlak van de verbinding heeft vóór authenticatie geen identiteit,
   dus `session.open` wordt dan geweigerd (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   De plugin is verantwoordelijk voor het ophalen van de inloggegevens uit zijn eigen protocol; de authenticatielogica en de gebruikerstabel blijven in de kernel, en de plugin raakt de wachtwoordopslag nooit aan.
2. **Daarna `session.open`**: als je andere methoden aanroept zonder een geopende sessie, retourneert de kernel `KindPreconditionFailed` ("stream N heeft nog geen sessie geopend").
3. **Settle elke bezorging precies één keer**: kies één van `Ack` / `Requeue` / `Reject`.
   Zowel `Ack` als `Reject` gooit het bericht weg, maar **alleen `Reject` gaat naar het dead-letterpad**.
4. **Onbeslechte bezorgingen gaan niet verloren**: wanneer een stream eindigt (client verbreekt / `Handler.Open` retourneert) of de pluginverbinding wegvalt,
   behandelt de kernel alle onbeslechte bezorgingen **als "requeue"**, zodat berichten niet blijven hangen.

**Foutherstel**: de `*plugin.Error` van de kernel komt via de brug binnen als een `*sidecar.RPCError` (velden `Kind` / `Text`),
en kan worden hersteld naar een gecategoriseerde `plugin.Error` in plaats van de categorie in een string te verliezen:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Veelvoorkomende `Kind`-waarden en hun mapping naar ingebouwde protocollen (om je te helpen beslissen hoe je fouten naar clients terugstuurt):

| `plugin.ErrorKind` | Semantiek | AMQP 0-9-1-mapping |
| --- | --- | --- |
| `KindNotFound` | Object bestaat niet | 404 NOT_FOUND (Channel sluiten) |
| `KindPreconditionFailed` | Parameters inconsistent met een bestaand object / sessie niet geopend | 406 PRECONDITION_FAILED (Channel sluiten) |
| `KindAccessRefused` | Onvoldoende rechten / gereserveerde naam | 403 ACCESS_REFUSED (Channel sluiten) |
| `KindResourceLocked` | Exclusieve bron in gebruik | 405 RESOURCE_LOCKED (Channel sluiten) |
| `KindInvalidPath` | vhost bestaat niet | 402 INVALID_PATH (verbinding sluiten) |
| `KindNotImplemented` | Capaciteit niet geïmplementeerd | 540 NOT_IMPLEMENTED (verbinding sluiten) |
| `KindInternal` | Interne kernelfout | 541 INTERNAL_ERROR (verbinding sluiten) |

**Grens van berichttypegetrouwheid**: de eigenschappentabel (`MessageDTO.Properties.Headers`) wordt via JSON doorgegeven,
dus **numerieke type-informatie die `int32` / `double` onderscheidt zoals in een AMQP field-table is niet beschikbaar**.
Wanneer strikte typegetrouwheid vereist is, draag zulke informatie dan zelf in de berichtbody (ruwe bytes).

### 3.5 Plugin-zijde status en logging

- De **status van een externe plugin wordt bepaald door of de verbinding in leven is**, en de plugin rapporteert die niet zelf (de `StateReporter` van ingebouwde plugins geldt niet voor externe processen).
- Logging: `sidecar.ServerOptions.Logger` schrijft naar stdout/stderr van het pluginproces;
  **wanneer de kernel het via `spawn` start, worden deze uitvoer door de kernel naar het kernellog doorgestuurd** (met tag `plugin`), wat gecentraliseerde verzameling vergemakkelijkt.
- Bij een losstaande uitrol (geen spawn) verzamel je de pluginlogs op je eigen manier.

### 3.6 Zelf testen zonder de kernel

`Handler` is een gewone Go-interface; in een unit test kun je hem direct instantiëren en `Hello` / `Call` / `Open` aanroepen om de bedrijfslogica te dekken, zonder netwerk te starten:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Voor end-to-end verificatie, zie §5.

---

## 4. In andere talen ontwikkelen (wire-protocolspecificatie)

`pkg/sidecar` is een publiek contract zonder afhankelijkheden; het wire-protocol zelf is eenvoudig en in elke taal te implementeren.
Om te integreren moet je de volgende "byte-niveau"-afspraken implementeren (zie `pkg/sidecar/frame.go`, `proto.go` voor de bron).

> **Er zijn taalspecifieke gidsen met volledige voorbeeldprojecten** (de voorbeelden zijn allemaal empirisch geslaagd: handshake → auth → semantiekbrug → bezorging/settle → bytestroom):
>
> | Taal | Gids | Voorbeeldproject (in workspace `speedmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (alleen standaardbibliotheek) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (alleen standaardbibliotheek) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (alleen standaardbibliotheek) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (één bestand, alleen JDK) |
>
> Voor de volledige Go-referentie-implementatie, zie het losstaande testproject `speedmq-test/test/integration/echosidecar/` (het gebruikt `pkg/sidecar.Server` direct,
> dus je hoeft je niet met de byte-niveau-details hieronder bezig te houden).

**Frameformaat** (uniform voor alle frames):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Frametypen `kind`**:

| kind | Naam | Richting | Payload |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | leeg |
| 4 | Pong | plugin → kernel | leeg |
| 5 | Call | bidirectioneel | JSON `Call` |
| 6 | Reply | bidirectioneel | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | bidirectioneel | `u32 BE stream` + ruwe bytes |
| 10 | Close | bidirectioneel | JSON `Close` |

**JSON-structuren van het besturingsvlak** (veldnamen komen overeen met `proto.go`).

> Dit gedeelte is een **voorbeeld van wire-protocolberichten** (meerdere berichten worden op volgorde in één blok gegeven, vandaar de `//`-scheidingstekens ter toelichting),
> en **het is geen configuratie die direct in `speedmqd.json` kan worden geschreven**.

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

**Semantiek die je moet volgen**:

- **Handshake**: de kernel stuurt eerst `Hello`, en de plugin moet een `HelloAck`-frame terugsturen.
  De kernel valideert `HelloAck.name == de pluginnaam in de config` en `HelloAck.api_version == de APIVersion van de kernel`;
  een niet-lege `deny` wordt beschouwd als weigering om te verbinden (de plugin wordt geïsoleerd).
- **Heartbeat**: standaard stuurt de kernel elke 2s een `Ping`, en de plugin moet binnen 8s met `Pong` antwoorden; aan de pluginzijde mag de verbinding zelf worden gesloten als er binnen 24s geen frame is ontvangen.
- **Twee ID-ruimtes**: `reverse` van `Call` onderscheidt de richting, en elke richting telt onafhankelijk vanaf 1 op,
  dus `Reply` **moet `reverse` terugsturen**, anders wordt het antwoord aan de verkeerde wachtende bezorgd.
- **Geen base64 op het datavlak**: grote data zoals berichtbodies gaan direct in de payload van het `Data`-frame (`stream` + ruwe bytes), gechunkt waar nodig.

> Als je Go gebruikt, gebruik dan direct `pkg/sidecar` en hoef je geen van de details hierboven te implementeren.

---

## 5. Samen laten draaien: integratie, clients bedienen, packaging ★

Deze sectie beantwoordt "hoe je het na ontwikkeling in SpeedMQ integreert en hoe je clients bedient".

### 5.1 De plugin in de configuratie declareren

Een externe plugin wordt **volledig door de configuratie beheerd**, en de kernel hoeft er geen code voor te wijzigen. Voeg een item toe aan de sectie `plugins` van `speedmqd.json`
(**de feitelijke configuratie is standaard JSON en mag geen commentaar bevatten**):

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

Item-voor-item uitleg (zie de tabel hieronder voor de veldenlijst):

- De sleutelnaam van `plugins.<pluginnaam>` **moet overeenkomen met de door de plugin zelf gerapporteerde `HelloAck.name`**, anders wordt de handshake geweigerd.
- `builtin: false`: verklaart expliciet een externe plugin (als je het weglaat, toont het beheervlak het als ingebouwd).
- `enabled`: uitschakelen = geen proces starten, geen listener aanmaken.
- Met `required: true` blokkeert een opstartfout het opstarten van de kernel — schakel dit niet in voor externe plugins.
- `address` is **het adres waarmee de kernel verbinding maakt** (de kernel is de client); als `spawn` niet leeg is, start de kernel het proces voor je.
- `protocols[].prefix` **moet niet leeg zijn** (zie §5.3 voor de sniffing-regels).
- `listeners` zijn de **externe poorten van het protocol, door de kernel geopend** (clients verbinden met de kernel).

Veldenoverzicht:

| Veld | Verplicht | Beschrijving |
| --- | --- | --- |
| `sidecar.address` | ✅ | Adres van het pluginproces: `tcp://host:port` of `unix:///path` |
| `sidecar.spawn` | ✕ | Commandoregel die de kernel voor je start (eerste element is het uitvoerbare bestand); **leeg = de kernel maakt alleen verbinding en start niet**, en jij beheert het proces zelf |
| `sidecar.restart` | ✕ | `always` (standaard, automatisch herstel na een verbreking/crash) of `never` (alleen `down` markeren en wachten op beheerder) |
| `sidecar.protocols[].name` | ✅ | Protocolnaam (globaal uniek, neemt deel aan de sniffing-prioriteit) |
| `sidecar.protocols[].prefix` | ✕ | Sniffing-prefix (ASCII); **leeg = neemt niet deel aan sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Externe listener voor dit protocol (`name` + `addr`), aangemaakt door de kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | Overschrijf de handshake-timeout (standaard 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Overschrijf het heartbeat-interval (standaard 2s) |

> **Pluginnaam vs. protocolnaam**: deze **mogen verschillen** (bijvoorbeeld plugin `my-sidecar` levert protocol `myproto`).
> Hot uitschakelen vindt eerst alle protocollen die door de pluginnaam zijn geregistreerd en sluit daarna de poorten van die protocollen, dus je hoeft niet bewust dezelfde naam te gebruiken.

### 5.2 Drie integratiewijzen

| Wijze | Configuratie | Wanneer te gebruiken |
| --- | --- | --- |
| **Zelfde host + kernel start (spawn)** | `spawn: [...]`, `address` wijst naar het adres waarop het luistert | Uitrol op dezelfde machine, één container; het minste werk, de kernel regelt starten en opruimen |
| **Zelfde host + zelf beheren (dial)** | `spawn: []`, `address` wijst naar een al draaiend proces | Beheer de pluginlevenscyclus met systemd / supervisor |
| **Cross-host / cross-container (dial, moet tcp zijn)** | `spawn: []`, `address: "tcp://<servicenaam>:19001"` | Plugin en kernel in aparte containers / machines uitgerold |

Adreskeuze:

- **Gebruik op dezelfde machine bij voorkeur een unix socket** (`unix:///tmp/my-sidecar.sock`): deze bezet geen TCP-poort en is niet gevoelig voor bezetting van hostpoorten.
  Let op dat het socketpad schrijfbaar moet zijn voor het kernelproces (in een container de niet-root `speedmq`-gebruiker).
- **Cross-container vereist TCP**, en het pluginproces moet op `0.0.0.0` luisteren, met `address` dat **de servicenaam in het containernetwerk** gebruikt.

> Draai de richting niet om: **het adres waarop de plugin luistert** = `address`; **de poort die naar clients wordt blootgesteld** = `protocols[].listeners`.

### 5.3 Clients bedienen: herkend worden via `prefix`

Wanneer de toeganglaag verbindingen dispatcht, kijkt deze **alleen naar het sniffing-resultaat**: voor elk ingeschakeld protocol vraagt het in registratievolgorde `Sniff(peek)` (peek is maximaal 8 bytes),
en degene die matcht neemt de verbinding over. Daarom:

1. **`prefix` moet niet leeg zijn** (ASCII, ≤ 8 bytes). Alleen wanneer de eerste paar bytes die de client stuurt overeenkomen, wordt de verbinding aan je plugin gegeven.
   Voorbeeld: `"prefix": "PY"` → de eerste bytes van de client moeten `PY` zijn (je kunt de prefix beschouwen als de magic header van je protocol).
2. **Een lege `prefix` betekent dat deze niet deelneemt aan sniffing**: zulke verbindingen worden **niet** aan de plugin gegeven (empirisch: verbindingen op de luisterpoort worden onmiddellijk verbroken).
   Daarom is een lege `prefix` alleen geschikt voor het scenario "een ander protocol stuurt op dezelfde poort voor je door"; **gebruik het niet** om een toegewijde poort te bouwen.
3. `listeners[].addr` bepaalt "op welke poort extern wordt bediend", en `prefix` bepaalt "of deze verbinding de jouwe is" —
   de twee moeten samen worden gebruikt: **een toegewijde poort heeft ook een niet-lege `prefix` nodig** (dit is ook waarom in de voorbeeldconfiguratie van de kernel
   `echo-sidecar` zowel `prefix: "ECHO"` als `listeners: [":1885"]` schrijft).
4. Sniffing matcht in protocolregistratievolgorde, en **de eerste match wint**: wanneer meerdere plugins naast elkaar bestaan, moeten prefixen te onderscheiden zijn (bijvoorbeeld: allemaal met dezelfde byte beginnen zal elkaar overschaduwen).

### 5.4 Luisteradressen en TLS overschrijven

- Het externe luisteradres kan op **twee plaatsen** worden gegeven: `sidecar.protocols[].listeners[].addr` (standaard) en
  `listeners.<protocolnaam>` (overschrijft in zijn geheel op protocolnaam). Wanneer beide bestaan, heeft `listeners.<protocolnaam>` voorrang.
- Wanneer TLS nodig is, geef je het certificaat in `listeners.<protocolnaam>[i].tls` (velden identiek aan ingebouwde protocollen).
  Hieronder staat een `listeners`-fragment (**standaard JSON, mag geen commentaar bevatten**): item 1 is plaintext, item 2 gebruikt TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/speedmq/tls/cert.pem",
                                 "key_file":  "/etc/speedmq/tls/key.pem" } }
  ]
}
```

> TLS wordt door **de kernel** aan de luisterzijde getermineerd; het pluginproces ontvangt een plaintext-stroom, dus de plugin hoeft geen TLS te verwerken.

### 5.5 Packaging: de plugin samen met de kernel laten draaien

**Optie A — In dezelfde image bakken** (aanbevolen voor plugins die "met de kernel worden uitgebracht"): voeg één regel toe aan de runtime-stage van `speedmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Stel daarna in de configuratie `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]` in,
met `address` op dezelfde waarde. De kernel start het bij het opstarten.

**Optie B — De binary mounten** (geen imagewijzigingen, goed voor gezamenlijk debuggen):

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

De configuratie gebruikt een unix socket (om een extra poort te vermijden). Hieronder staat het `sidecar`-fragment binnen `plugins.my-sidecar`
(**standaard JSON, mag geen commentaar bevatten**; `prefix` moet nog steeds niet leeg zijn, zie §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Optie C — Aparte container** (plugin apart uitgebracht / onafhankelijk geschaald):

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

In de configuratie `spawn: []` (de kernel maakt alleen verbinding, start niet) en `address: "tcp://my-sidecar:19001"` (de compose-servicenaam).

### 5.6 Opstarten en verifiëren

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

Let in `plugins show` vooral op `state` en `RuntimeNote`:
`failed` komt met de faalreden (handshake geweigerd / poort kon niet worden geopend…); `down` komt met de verbrekingsreden (proces gecrasht / verbinding verbroken).

### 5.7 Operationeel beheer tijdens runtime

| Operatie | Commando / API | Effect |
| --- | --- | --- |
| Hot uitschakelen | `speedmqctl plugins disable my-sidecar` of `PUT /api/plugins/my-sidecar/disable` | **Sluit de externe listeners van de plugin** (uitschakeling op capaciteitsniveau); de kernel en andere plugins worden niet beïnvloed |
| Hot inschakelen | `speedmqctl plugins enable my-sidecar` | Heropent zijn listeners; als een eerdere start is mislukt, wordt één keer opnieuw geprobeerd |
| Status bekijken | `speedmqctl plugins list/show` | Status + faal-/verbrekingsreden |
| Kernel afsluiten | — | Verbreekt de verbinding met de plugin, recupereert overbrugde sessies, **beëindigt de door de kernel via `spawn` gestarte subprocessen** |

> Hot uitschakelen sluit alleen de "capaciteit" (luisterpoorten) en **doodt niet** het via `spawn` gestarte pluginproces; het opruimen van processen gebeurt bij het afsluiten van de kernel.

### 5.8 Een ingang in de beheerconsole bieden (optioneel)

Wanneer de plugin zijn eigen UI meelevert, voeg je een `console_url` toe (het adres van de beheer-UI; zie §5.1 voor de overige velden) aan de sectie `plugins.<pluginnaam>`.
Hieronder wordt alleen het item `plugins.my-sidecar` getoond (**standaard JSON, mag geen commentaar bevatten**; de inhoud van de sectie `sidecar` is hetzelfde als in §5.1):

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

- De pagina **Pluginbeheer** van de beheerconsole (gegevens komen uit het veld `console_url` van `GET /api/plugins`) toont voor zulke plugins een
  knop "Beheer-UI openen", die **in een nieuw tabblad opent**.
- Wanneer `console_url` niet is gedeclareerd, is de knop niet beschikbaar en zegt een tooltip "Deze plugin biedt geen beheer-UI".
- Het is slechts **metadata die de uitrolpartij in de configuratie schrijft**: het maakt geen deel uit van de plugin-API (`pkg/plugin`), neemt niet deel aan plugin start-stop,
  en de UI zelf wordt door de plugin gehost (deze kan in het pluginproces leven of in een willekeurige losstaande service).

---

## 6. Levenscyclus- en fouttolerantiematrix

| Scenario | Kernelgedrag | Impact aan de pluginzijde |
| --- | --- | --- |
| Pluginproces niet gestart / handshake geweigerd | Probeert de verbinding binnen 8s opnieuw; als het nog steeds mislukt, markeert `failed` en isoleert (blokkeert het opstarten van de kernel niet) | Geen |
| Een via `spawn` gestart proces stopt | Logt het; markeert `down`; backoff en herverbinden / herstarten volgens het `restart`-beleid | Het nieuwe proces voert de handshake opnieuw uit |
| Pluginproces crasht (tijdens runtime) | De kernel wordt niet beïnvloed; `down` + backoff herverbinden | `pkg/sidecar.Server` sluit die verbinding |
| Kernel wordt `kill -9`ed | — | De pluginzijde recupereert de verbinding zelf via idle-timeout (standaard 24s geen frames), laat geen zombies achter |
| Kernel sluit normaal af | Roept `Stop` aan: verbreekt, recupereert onbeslechte bezorgingen (als requeue), `Kill`t subprocessen | Ontvangt SIGKILL |
| Bezorgingen terwijl de pluginverbinding weg is | Onbeslechte bezorgingen worden altijd **geherqueued** en gaan nooit verloren | — |
| Client verbreekt / `Open` retourneert | Sluit de betreffende stream en geeft de sessie en consumers van die stream vrij | `Stream.Read` retourneert EOF |

---

## 7. Rode lijnen en bekende grenzen

**Rode lijnen**

1. Een plugin mag alleen afhankelijk zijn van `pkg/sidecar` (en optioneel `pkg/plugin`); hij **mag niet** afhankelijk zijn van de kernel `internal/**`.
2. De pluginnaam moet overeenkomen met de configuratie en `APIVersion` moet overeenkomen met de kernel, anders kan er geen verbinding worden gemaakt (dit voorkomt "stil draaien maar niet werken").
3. Bij gebruik van `session.*`: **eerst `core.authenticate`, dan `session.open`**, en settle elke bezorging **precies één keer**.
4. `protocols[].prefix` moet niet leeg zijn, anders worden verbindingen niet aan de plugin gegeven (zie §5.3).
5. Een weigering in `Hello` moet **expliciet een error retourneren** (blijf niet stil) — anders ziet de kernel alleen "verbinding gesloten" en kan de oorzaak niet worden gevonden.

**Bekende grenzen**

- **Sniffing bevindt zich aan de kernelzijde**: externe plugins kunnen geen aangepaste sniff-functies definiëren en kunnen alleen matchen via `prefix` (ASCII, ≤ 8 bytes);
  een lege `prefix` betekent "geen verbinding kunnen krijgen" (zie §5.3).
- **Het datavlak loopt via lokale proxying**: er is geen fd-doorgifte (Windows heeft geen `SCM_RIGHTS`), wat één extra geheugenkopie toevoegt in vergelijking met in-process;
  elke omgekeerde aanroep voegt ook één extra lokale RPC toe.
- **Eigenschappentabeltypen degraderen**: `Properties.Headers` wordt via JSON doorgegeven, dus onderscheidingen zoals `int32` / `double` gaan verloren (zie §3.4).
- **Backpressure van één stream beïnvloedt de hele verbinding**: wanneer de ontvangstbuffer van één stream vol is, blokkeert dit de dispatch-goroutine van die verbinding; snelheidsbeperking per stream is een toekomstige optimalisatie.
- **Auth-fouten geven alleen tekst door**: een kernel-authfout is een `*plugin.AuthError` (een andere classificatie dan `plugin.ErrorKind`),
  en alleen de tekst bereikt de plugin via de brug; de plugin moet deze volgens zijn eigen conventie naar protocolfoutcodes mappen.
- **Alleen de capaciteit `net.listen` werkt echt**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` zijn **gereserveerde slots**; ze declareren doet alleen mee aan auditing (zie de governance-weergave in §5.6), en er is momenteel geen bijbehorend uitbreidingspunt.

---

## 8. Probleemoplossing FAQ

| Symptoom | Oorzaak en behandeling |
| --- | --- |
| Status `failed`, reden bevat "pluginnaam komt niet overeen" | De geconfigureerde pluginnaam ≠ `HelloAck.name`; maak ze gelijk |
| Status `failed`, reden bevat "API-versie komt niet overeen" | `HelloAck.api_version` ≠ de `APIVersion` van de kernel; maak ze gelijk |
| Status `failed`, reden bevat "handshake geweigerd" | De `Hello` van de plugin retourneerde een error (`deny`); bekijk de doorgestuurde pluginuitvoer in het kernellog |
| Status `failed`, reden bevat "verbinden met externe plugin mislukt" | Het proces is niet opgekomen / `address` is fout / het socketpad is niet schrijfbaar (let op de rechten van de `speedmq`-gebruiker in containers) |
| Status `down` | Het pluginproces is gecrasht of de verbinding is verbroken; `restart=always` herverbindt automatisch, `never` vereist handmatig starten |
| Poort niet open / client kan niet verbinden | `protocols[].listeners` is niet geconfigureerd of het adres is overschreven door `listeners.<protocolnaam>`; controleer beide plaatsen |
| Client verbindt met een andere poort en wordt onmiddellijk verbroken | Die poort komt niet overeen met je protocol (`prefix` is leeg of de prefix komt niet overeen); configureer een niet-lege `prefix` voor het protocol (zie §5.3) |
| Fout `ACCESS_REFUSED - ... for user ''` | Er was **geen authenticatie** vóór de semantiekbrug; roep `core.authenticate` aan vóór `session.open` |
| Omgekeerde aanroep meldt "stream N heeft nog geen sessie geopend" | Doe eerst `core.authenticate`, dan `session.open`, en pas daarna andere `session.*` |
| Geen consumer-bezorgingen ontvangen | Bezorgingen komen bij je `Call` binnen als een **forward call** `session.deliver`; bevestig dat die methode wordt afgehandeld |
| Plugin buiten de container, kernel binnen, kan niet verbinden | Gebruik `address: tcp://host.docker.internal:<port>` (of zet de plugin ook in de container en gebruik de servicenaam); de plugin moet op `0.0.0.0` luisteren |

---

## 9. Referentie (bronindex)

| Wat je wilt zien | Bestand |
| --- | --- |
| Wire-protocol en implementaties aan beide zijden (**verplichte literatuur voor ontwikkeling**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frames), `proto.go` (berichten), `server.go` (pluginzijde), `client.go` (kernelzijde), `bridge.go` (`session.*`-contract), `stream.go` (streams) |
| Kernel-zijde sidecar-host (integratie/herverbinding/proxy/status) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Kernel-zijde semantiekbrug (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Kernel-sessie-operatievlaktypen (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Luisteren, sniffen, hot start-stop per plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Pluginlevenscyclus en governance (isolatie/status/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Configuratie-items en voorbeelden (inclusief het sidecar-gedeelte) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/speedmqd.json`](../../../configs/speedmqd.json) |
| Procesassemblage (hoe de sidecar in de kernel wordt geassembleerd) | [`cmd/speedmqd/main.go`](../../../cmd/speedmqd/main.go) |
| Go-referentie-implementatie (gebruikt `pkg/sidecar.Server`, met de `session.*`-brug en `core.authenticate`) | Losstaand testproject `speedmq-test/test/integration/echosidecar/` |
| **Taalspecifieke gidsen + voorbeeldprojecten** | In deze map `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; voorbeelden staan in de **workspace** `speedmq-plugin/{python,nodejs,php,java}/` |
