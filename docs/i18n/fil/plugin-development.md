# Gabay sa Pagbuo ng External-Process Plugin (sidecar) ng SwiftMQ

> **Para sa**: mga developer na ayaw mag-fork / mag-recompile ng kernel, pero gustong mag-extend ng kakayahan ng SwiftMQ gamit ang **anumang wika**.
> **Saklaw**: ang dokumentong ito ay tumatalakay lamang sa isang uri ng plugin —— ang **external-process plugin** (kernel term na `sidecar`). Ang built-in na protocol plugin ng kernel (AMQP 0-9-1 / MQTT) ay wala sa saklaw ng dokumentong ito.
> **Paano basahin**: itinatayo ng Seksyon 1–2 ang mental model, ang Seksyon 3 ay ang pagsulat ng code, **ang Seksyon 5 ang "pagkatapos ng development, paano isasaksak at patakbuhin nang sabay, at magbigay ng serbisyo sa labas"**;
> **para sa ibang wika (Python / Node.js / PHP / Java), tingnan ang per-language na gabay sa §4** (bawat isa ay may kasamang kumpleto at aktwal na na-verify na halimbawang proyekto).
> Ang code sa dokumentong ito ay minimal na runnable skeleton, maaari mong direktang kopyahin bilang panimulang punto. Ang Simplified Chinese ang source language.

---

## 1. Ano Ito

Isang **hiwalay na process** na nagpapatupad ng isang "protocol" (nag-parse ng byte stream mula sa client) sa loob ng sarili nitong process,
hina-host ito ng kernel ayon sa config: **ang port ay binubuksan ng kernel, ang koneksyon ay ni-proxy ng kernel**, at ang registration / start-stop / auditing / isolation ay gumagamit pa rin ng umiiral nang mekanismo ng kernel.

Itatag muna ang tatlong tamang mental model (ang pinakamadalas mapagkamalan):

1. **Ang plugin process ay isang "lokal na serbisyo"**: nakikinig lamang ito sa isang **lokal na address** (TCP o unix socket), at hinihintay na **kumonekta ang kernel**.
   Ang direksyon ng koneksyon ay **kernel (client) → plugin (server)**, at ang handshake ay ang kernel din ang naunang nagpapadala.
2. **Ang panlabas na business port ay hindi binubuksan ng plugin**: ito ay ginagawa ng **kernel** ayon sa `protocols[].listeners` sa config, at ini-map pabalik sa client.
   Ang kinokonektahan ng client ay **ang port ng kernel**, at ang byte ay ni-proxy ng kernel papunta sa plugin process. Ang plugin process ay **hindi kailangan** na magbukas mismo ng business port.
3. **Opsyonal ang semantics**: maaaring "maglipat lamang ng byte" ang plugin (ganap mong sariling implementasyon ang protocol),
   o maaari itong umabot sa kernel semantics (queue / routing / permission / acknowledgement) sa pamamagitan ng **reverse call** sa `session.*`,
   na **kaparehong-kapareho ng semantics** ng built-in protocol plugin (kaya ang vhost, permission, routing, at gawi ng queue ay hindi magkakaiba).

| Benepisyo | Gastos |
| --- | --- |
| Maaaring mag-extend nang hindi binabago o ni-recompile ang kernel | Isang dagdag na lokal na byte copy sa data plane (kernel proxy, walang fd passing, pare-pareho sa lahat ng platform) |
| Implementasyon sa kahit anong wika (kailangan lang na maipatupad ang wire protocol) | Isang dagdag na lokal na RPC sa bawat reverse call (JSON encoding/decoding + copy) |
| Maaaring i-release / i-upgrade / i-restart nang hiwalay ang plugin | Nasa kernel side ang sniffing: makikilala lamang sa pamamagitan ng "prefix" o "dedikadong port" |
| Ang crash ay nakakaapekto lamang sa plugin na iyon: minamarkahan ito ng kernel na `down`, hindi ito bumabagsak o nag-e-exit | Ang `net.listen` capability lamang ang tunay na nagiging epektibo; ang ibang capability value ay reserba (tingnan ang §7) |

---

## 2. Paano Ito Gumagana

Pagtatatag ng koneksyon (**ang kernel ang client, ang plugin ang server**):

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

Simula ng serbisyo:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Lifecycle** (ang `internal/plugin/sidecar` host sa kernel side):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Semantics ng estado** (makikita sa `swiftmqctl plugins show`):

| Estado | Kahulugan | Aksyon ng operator |
| --- | --- | --- |
| `enabled` | Nakakonekta at nagseserbisyo | — |
| `failed` | **Hindi umandar noong startup** (maling config / tinanggihan ang handshake / hindi ma-launch ang process) | Tingnan ang `RuntimeNote` at kernel log, ayusin ang config o ang plugin; magre-retry lamang ang kernel pagkatapos ng restart |
| `down` | **Umandar dati, wala na ngayon** (nag-crash ang process / naputol ang koneksyon) | Paandarin ang plugin process; awtomatikong magre-recover ang kernel ayon sa `restart` policy |
| `disabled` | `enabled=false` sa config, o hot-disable ng operator | Ibalik gamit ang `plugins enable <pangalan>` |

---

## 3. Pag-develop (Go)

### 3.1 Gumawa ng Proyekto

Ang plugin ay isang **hiwalay na Go module** na umaasa lamang sa dalawang public contract package:

- `github.com/houzch/swiftmq/pkg/sidecar` —— ang wire protocol at implementasyon sa plugin side (**kailangan**)
- `github.com/houzch/swiftmq/pkg/plugin` —— kailangan lamang kapag gagamit ka ng mga type tulad ng `plugin.Message` / `plugin.Error` (opsyonal)

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

> Kapag gumamit ng `replace` para sa joint debugging, ang plugin at ang kernel ay dapat **kaparehong source**, kung hindi, kahit pareho ang API version (`v1`), maaaring magkaiba ang type.

### 3.2 Ipatupad ang `Handler` (tatlong method)

Ang buong business surface ng plugin process ay ang `Hello` / `Call` / `Open` ng `sidecar.Handler`.
Ang handshake, heartbeat, multiplexing, at chunking ay hinahawakan ng `pkg/sidecar`, hindi mo kailangang hawakan ang frame.

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

### 3.3 Data Plane: Pagbasa at Pagsulat ng `Stream`

Ang `sidecar.Stream` ay nagpapatupad ng `io.ReadWriteCloser`, ituring na lamang itong "isang koneksyon":

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

Mga pangunahing punto:

- Bawat client connection = isang stream; maaaring magsimulang magpadala/tumanggap agad ang plugin sa loob ng `Handler.Open`.
- Ang malalaking mensahe ay awtomatikong hinahati-hati ng library (bawat frame ≤ 64 KiB), at **hindi umaasa sa laki ng mensahe ang paggamit ng memory**.
- Backpressure: may upper bound ang receive buffer ng isang stream; kapag napuno ang buffer, nahaharang ang **dispatch goroutine** ng koneksyong iyon (sabay na naghihintay ang lahat ng stream) ——
  ito ang trade-off sa pagitan ng "predictable memory" at "per-stream rate limiting", tingnan ang §7.

### 3.4 Paggamit ng Kernel Semantic Bridge (`session.*`)

Kung gusto mong gamitin ng plugin ang queue / routing / permission / acknowledgement semantics ng kernel (sa halip na gumawa ng sarili), gumamit ng **reverse call**.
Sa isang stream, gamitin ang mga ito sa pagkakasunod-sunod na `session.open` → iba pang `session.*` → (`session.close`):

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

**Isang sulyap sa mga reverse-call method** (mga constant sa `pkg/sidecar/bridge.go` → wire name):

| Grupo | Wire name (constant) | Paglalarawan |
| --- | --- | --- |
| Auth | `core.authenticate`（`MethodCoreAuthenticate`） | **Kailangang gawin muna**: ibigay ang credentials mula sa iyong protocol sa kernel para sa verification; params `{stream, mechanism, response}`, returns `{user}` |
| Session | `session.open`（`MethodSessionOpen`） | Magbukas ng session para sa isang vhost sa stream; **pagkatapos ng authentication** lamang ito maaaring gawin |
| | `session.close`（`MethodSessionClose`） | I-release ang session sa stream (kanselahin ang consumer, burahin ang exclusive queue) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Dagdag / bura (passive declaration ng wala → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Binding ng exchange papunta sa exchange |
| Queue | `session.declare_queue` / `session.delete_queue` | Dagdag / bura; kapag walang laman ang `name` ay ang server ang gagawa |
| | `session.bind_queue` / `session.unbind_queue` | Binding ng queue papunta sa exchange |
| | `session.purge_queue` | Linisin ang ready messages (hindi kasama ang unacknowledged) |
| Publish | `session.publish` | Returns `{routed, rejected}`; nakumpleto ang persistence bago ang reply |
| Get | `session.get` | Aktibong kumuha ng isa; `found=false` ay nangangahulugang walang laman ang queue |
| Consume | `session.consume` / `session.cancel` | Register / kanselahin ang consumer |
| Settle | `session.settle` | I-settle ang isang delivery (`ack` / `requeue` / `reject`) |
| **Forward** | `session.deliver` | **Kernel → plugin**: pag-push pabalik ng delivery (hina-handle sa iyong `Handler.Call`) |

**Apat na kasunduan na dapat sundin**:

1. **`core.authenticate` muna**: walang identity ang kernel operation surface ng koneksyon bago ang authentication,
   kaya tatanggihan ang `session.open` (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Ang plugin ang responsable sa pagkuha ng credentials mula sa sarili nitong protocol; ang authentication logic at user table ay nasa kernel pa rin, hindi hinahawakan ng plugin ang password store.
2. **Pagkatapos ay `session.open`**: kapag tumawag ng ibang method nang hindi pa nakabukas ang session, ibabalik ng kernel ang `KindPreconditionFailed` ("stream N ay hindi pa nagbukas ng session").
3. **Bawat delivery ay eksaktong isang beses na ini-settle**: pumili ng isa sa `Ack` / `Requeue` / `Reject`.
   Parehong itinatapon ng `Ack` at `Reject` ang mensahe, **ang `Reject` lamang ang dumadaan sa dead-letter**.
4. **Hindi nawawala ang hindi pa na-se-settle na delivery**: kapag natapos ang stream (nadiskonekta ang client / bumalik ang `Handler.Open`) o naputol ang koneksyon ng plugin,
   itinuturing ng kernel ang lahat ng hindi pa na-se-settle na delivery **bilang "requeue"**, para maiwasan ang naiiwang mensahe.

**Error restoration**: ang `*plugin.Error` ng kernel ay dumarating sa pamamagitan ng bridge bilang `*sidecar.RPCError` (mga field na `Kind` / `Text`),
at maaaring ibalik sa categorized na `plugin.Error`, sa halip na itapon ang category sa string:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Karaniwang `Kind` at ang mapping sa built-in protocol (para makatulong sa pagdesisyon kung paano ibabalik ang error sa client):

| `plugin.ErrorKind` | Semantics | AMQP 0-9-1 mapping |
| --- | --- | --- |
| `KindNotFound` | Wala ang object | 404 NOT_FOUND (isara ang Channel) |
| `KindPreconditionFailed` | Hindi tugma ang params sa umiiral nang object / hindi nakabukas ang session | 406 PRECONDITION_FAILED (isara ang Channel) |
| `KindAccessRefused` | Kulang ang permission / reserved name | 403 ACCESS_REFUSED (isara ang Channel) |
| `KindResourceLocked` | Abala ang exclusive resource | 405 RESOURCE_LOCKED (isara ang Channel) |
| `KindInvalidPath` | Wala ang vhost | 402 INVALID_PATH (isara ang koneksyon) |
| `KindNotImplemented` | Hindi pa naipatupad ang capability | 540 NOT_IMPLEMENTED (isara ang koneksyon) |
| `KindInternal` | Panloob na error ng kernel | 541 INTERNAL_ERROR (isara ang koneksyon) |

**Hangganan ng message type fidelity**: ang property table (`MessageDTO.Properties.Headers`) ay dumadaan sa JSON,
kaya **hindi makukuha ang numeric type information na tumutukoy sa `int32` / `double`** tulad ng sa AMQP field-table.
Kapag kailangan ang mahigpit na type fidelity, ilagay ang ganitong impormasyon sa message body (raw bytes) upang ikaw mismo ang magdala nito.

### 3.5 Estado at Log sa Plugin Side

- Ang **estado ng external plugin ay natutukoy sa kung buhay ang koneksyon**, at hindi kailangang i-report ito ng plugin mismo (ang `StateReporter` ng built-in plugin ay hindi naaangkop sa external process).
- Log: ang `sidecar.ServerOptions.Logger` ay nag-o-output sa stdout/stderr ng plugin process;
  **kapag ini-launch ng kernel sa pamamagitan ng `spawn`, ang mga output na ito ay ipapasa ng kernel sa kernel log** (may tag na `plugin`), para sa pinag-isang koleksyon.
- Sa standalone deployment (hindi spawn), kolektahin ang log ng plugin sa sarili mong paraan.

### 3.6 Maaaring Mag-Self-Test Kahit Walang Kernel

Ang `Handler` ay ordinaryong Go interface; sa unit test, direktang i-instantiate ito at tawagin ang `Hello` / `Call` / `Open` upang masakop ang business logic, hindi na kailangang magsimula ng network:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Para sa end-to-end verification, tingnan ang §5.

---

## 4. Pag-develop sa Ibang Wika (wire protocol spec)

Ang `pkg/sidecar` ay zero-dependency na public contract; ang wire protocol mismo ay simple, kayang ipatupad ng kahit anong wika.
Upang maka-integrate, kailangan mong ipatupad ang mga sumusunod na "byte-level" na kasunduan (tingnan ang `pkg/sidecar/frame.go`, `proto.go` para sa source).

> **May mga per-language na gabay na may kumpletong halimbawang proyekto** (lahat ng halimbawa ay aktwal na naipasa: handshake → auth → semantic bridge → delivery/settle → byte stream):
>
> | Wika | Gabay | Halimbawang proyekto (workspace na `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (standard library lamang) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (standard library lamang) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (standard library lamang) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (single file, JDK lamang) |
>
> Para sa kumpletong Go reference implementation, tingnan ang hiwalay na test project na `swiftmq-test/test/integration/echosidecar/` (direktang gumagamit ito ng `pkg/sidecar.Server`,
> kaya hindi mo kailangang alalahanin ang byte-level na detalye sa ibaba).

**Frame format** (pareho sa lahat ng frame):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Mga uri ng frame `kind`**:

| kind | Pangalan | Direksyon | Payload |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | walang laman |
| 4 | Pong | plugin → kernel | walang laman |
| 5 | Call | bidirectional | JSON `Call` |
| 6 | Reply | bidirectional | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | bidirectional | `u32 BE stream` + raw bytes |
| 10 | Close | bidirectional | JSON `Close` |

**Control-plane JSON structures** (tugma ang mga field name sa `proto.go`).

> Ang seksyong ito ay **halimbawa ng wire-protocol message** (maraming message sa isang bloke sa pagkakasunod-sunod, kaya may `//` separator para sa paliwanag),
> **hindi ito config na maaaring direktang isulat sa `swiftmqd.json`**.

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

**Mga semantics na dapat sundin**:

- **Handshake**: ang kernel ang naunang nagpapadala ng `Hello`, at dapat magbalik ang plugin ng isang `HelloAck` frame.
  Iva-validate ng kernel na `HelloAck.name == ang plugin name sa config` at `HelloAck.api_version == APIVersion ng kernel`;
  ang hindi laman na `deny` ay itinuturing na pagtanggi sa koneksyon (ini-isolate ang plugin).
- **Heartbeat**: default na nagpapadala ang kernel ng `Ping` bawat 2s, at dapat magbalik ang plugin ng `Pong` sa loob ng 8s; sa plugin side, kung walang natanggap na anumang frame sa loob ng 24s ay maaari nitong isara ang koneksyon.
- **Dalawang ID space**: ang `reverse` ng `Call` ay tumutukoy sa direksyon, at ang bawat direksyon ay nag-i-increment nang hiwalay mula sa 1,
  kaya ang `Reply` ay **dapat magdala pabalik ng `reverse`**, kung hindi ay maihahatid ang tugon sa maling naghihintay.
- **Walang base64 sa data plane**: ang malalaking data tulad ng message body ay direktang inilalagay sa `Data` frame payload (`stream` + raw bytes), hinahati-hati kung kinakailangan.

> Kung gumagamit ka ng Go, direktang gamitin ang `pkg/sidecar`, at hindi mo na kailangang ipatupad ang mga detalye sa itaas.

---

## 5. Isasaksak at Patakbuhin Nang Sabay: Integration, Paglilingkod sa Client, Packaging ★

Sinasagot ng seksyong ito ang "paano i-integrate sa SwiftMQ at paano magbigay ng serbisyo sa client pagkatapos ng development".

### 5.1 Pagdedeklara ng Plugin sa Config

Ang external plugin ay **ganap na pinamamahalaan ng config**, at hindi kailangang baguhin ng kernel ang anumang code para dito. Magdagdag ng isang entry sa `plugins` section ng `swiftmqd.json`
(**ang aktwal na config ay standard JSON, hindi maaaring maglaman ng comment**):

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

Paliwanag sa bawat item (tingnan ang talahanayan sa ibaba para sa listahan ng field):

- Ang key name ng `plugins.<pangalan ng plugin>` ay **dapat tumugma sa `HelloAck.name` na ini-report ng plugin mismo**, kung hindi ay tatanggihan ang handshake.
- `builtin: false`: tahasang idinedeklara ang external plugin (kapag hindi isinulat, ipapakita ito ng management plane bilang built-in).
- `enabled`: kapag pinatay ito = hindi mag-la-launch ng process, hindi gagawa ng listener.
- Sa `required: true`, ang pagka-fail sa startup ay haharang sa startup ng kernel —— huwag itong buksan para sa external plugin.
- Ang `address` ay **ang address na kinokonektahan ng kernel** (ang kernel ang client); kapag hindi laman ang `spawn`, ang kernel ang mag-la-launch ng process.
- Ang `protocols[].prefix` ay **dapat hindi laman** (tingnan ang §5.3 para sa sniffing rules).
- Ang `listeners` ay ang **panlabas na port ng protocol na ito, binubuksan ng kernel** (ang kernel ang kinokonektahan ng client).

Listahan ng field:

| Field | Kailangan | Paglalarawan |
| --- | --- | --- |
| `sidecar.address` | ✅ | Address ng plugin process: `tcp://host:port` o `unix:///path` |
| `sidecar.spawn` | ✕ | Command line na ila-launch ng kernel (ang unang elemento ay ang executable); **walang laman = konekta lamang ang kernel, hindi nag-la-launch**, ikaw ang mamamahala ng process |
| `sidecar.restart` | ✕ | `always` (default, awtomatikong nagre-recover pagkatapos ng disconnect/crash) o `never` (minamarkahan lamang na `down`, naghihintay ng operator) |
| `sidecar.protocols[].name` | ✅ | Pangalan ng protocol (globally unique, kasama sa sniffing priority) |
| `sidecar.protocols[].prefix` | ✕ | Sniffing prefix (ASCII); **walang laman = hindi kasali sa sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Panlabas na listener ng protocol na ito (`name` + `addr`), ginagawa ng kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | I-override ang handshake timeout (default 5s) |
| `sidecar.heartbeat_seconds` | ✕ | I-override ang heartbeat interval (default 2s) |

> **Plugin name vs. protocol name**: **maaaring magkaiba** ang dalawa (halimbawa, ang plugin na `my-sidecar` ay nagbibigay ng protocol na `myproto`).
> Ang hot-disable ay hahanapin muna ang lahat ng protocol na nairehistro sa ilalim ng plugin name, at pagkatapos ay isasara ang mga port ng mga protocol na iyon, kaya hindi kailangang sadyang gamitin ang parehong pangalan.

### 5.2 Tatlong Paraan ng Integration

| Paraan | Config | Kailan gamitin |
| --- | --- | --- |
| **Parehong host + kernel ang nag-la-launch (spawn)** | `spawn: [...]`, tinuturo ng `address` ang address na pinakikinggan nito | Deploy sa parehong makina, isang container; pinakamadali, ang kernel ang namamahala sa pag-launch at pag-reclaim |
| **Parehong host + sariling pamamahala (dial)** | `spawn: []`, tinuturo ng `address` ang umaandar nang process | Pamahalaan ang lifecycle ng plugin gamit ang systemd / supervisor |
| **Cross-host / cross-container (dial, dapat tcp)** | `spawn: []`, `address: "tcp://<pangalan ng serbisyo>:19001"` | Nakadeploy ang plugin at kernel sa magkaibang container / makina |

Pagpili ng address:

- **Sa parehong makina, inirerekomenda ang unix socket** (`unix:///tmp/my-sidecar.sock`): hindi ito kumukuha ng TCP port at hindi apektado ng pagkaokupa ng host port.
  Tandaan na ang socket path ay dapat na maisusulat ng kernel process (sa container, ang non-root na `swiftmq` user).
- **Cross-container ay dapat TCP**, at ang plugin process ay dapat nakikinig sa `0.0.0.0`, gamit ang **service name sa container network** para sa `address`.

> Huwag ibaligtad ang direksyon: **ang address na pinakikinggan ng plugin** = `address`; **ang port na nakabukas sa client** = `protocols[].listeners`.

### 5.3 Paglilingkod sa Client: Nakikilala sa Pamamagitan ng `prefix`

Kapag nagdi-dispatch ang access layer ng koneksyon, **ang sniffing result lamang ang tinitingnan nito**: para sa bawat naka-enable na protocol ay tinatanong nito ang `Sniff(peek)` sa pagkakasunod-sunod ng registration (peek ay hanggang 8 bytes),
at ang tumutugma ang kumukuha ng koneksyon. Kaya:

1. **Dapat hindi laman ang `prefix`** (ASCII, ≤ 8 bytes). Kapag ang mga unang byte na ipinadala ng client ay tumugma rito, saka lamang ibibigay sa iyong plugin ang koneksyon.
   Halimbawa: `"prefix": "PY"` → ang unang byte ng client ay dapat `PY` (maaaring ituring ang prefix na magic header ng iyong protocol).
2. **Ang walang laman na `prefix` ay nangangahulugang hindi kasali sa sniffing**: ang ganitong koneksyon ay **hindi** ibibigay sa plugin (aktwal: ang koneksyon sa listening port ay agad na isasara).
   Kaya ang walang laman na `prefix` ay angkop lamang sa sitwasyong "may ibang protocol na magfo-forward para sa iyo sa parehong port", **huwag** itong gamitin para sa dedikadong port.
3. Ang `listeners[].addr` ang nagtatakda ng "sa aling port maglilingkod sa labas", at ang `prefix` ang nagtatakda ng "kung sa iyo ba ang koneksyong ito" ——
   dapat gamitin nang magkasama ang dalawa: **kailangan din ng dedikadong port ng hindi laman na `prefix`** (ito rin ang dahilan kung bakit sa kernel example config,
   ang `echo-sidecar` ay nagsusulat ng parehong `prefix: "ECHO"` at `listeners: [":1885"]`).
4. Ang sniffing ay tumutugma ayon sa pagkakasunod-sunod ng registration ng protocol, **ang naunang tumugma ang epektibo**: kapag maraming plugin ang magkakasabay, dapat may pagkakaiba ang prefix (halimbawa, ang lahat ay nagsisimula sa parehong byte ay magsasapawan sa isa't isa).

### 5.4 Pag-override ng Listener Address at TLS

- Ang panlabas na listener address ay maaaring ibigay sa **dalawang lugar**: `sidecar.protocols[].listeners[].addr` (default) at
  `listeners.<pangalan ng protocol>` (pangkalahatang override ayon sa protocol name). Kapag pareho silang umiiral, `listeners.<pangalan ng protocol>` ang masusunod.
- Kapag kailangan ang TLS, ibigay ang certificate sa `listeners.<pangalan ng protocol>[i].tls` (pareho ang field sa built-in protocol).
  Nasa ibaba ang isang `listeners` snippet (**standard JSON, hindi maaaring maglaman ng comment**): item 1 ay plaintext, item 2 ay gumagamit ng TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> Ang TLS ay tinatapos ng **kernel** sa listener side; ang natatanggap ng plugin process ay plaintext stream —— hindi kailangang hawakan ng plugin ang TLS.

### 5.5 Packaging: Pagpapatakbo ng Plugin Kasabay ng Kernel

**Paraan A —— Isama sa parehong image** (inirerekomenda para sa plugin na "inilalabas kasabay ng kernel"): magdagdag ng isang linya sa runtime stage ng `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Pagkatapos, sa config, itakda ang `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
at pareho ang `address`. Ila-launch ito ng kernel sa startup.

**Paraan B —— I-mount ang binary** (walang pagbabago sa image, angkop para sa joint debugging):

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

Ang config ay gumagamit ng unix socket (para maiwasan ang dagdag na port). Nasa ibaba ang `sidecar` snippet sa loob ng `plugins.my-sidecar`
(**standard JSON, hindi maaaring maglaman ng comment**; kailangan pa ring hindi laman ang `prefix`, tingnan ang §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Paraan C —— Hiwalay na container** (hiwalay na release / independiyenteng scaling ng plugin):

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

Sa config, `spawn: []` (konekta lamang ang kernel, hindi nag-la-launch), `address: "tcp://my-sidecar:19001"` (compose service name).

### 5.6 Startup at Verification

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

Sa `plugins show`, bigyang-pansin ang `state` at `RuntimeNote`:
Ang `failed` ay may kasamang dahilan ng pagkabigo (tinanggihan ang handshake / hindi mabuksan ang port…); ang `down` ay may kasamang dahilan ng pagkawala ng koneksyon (nag-crash ang process / naputol ang koneksyon).

### 5.7 Runtime Operations

| Operasyon | Command / API | Epekto |
| --- | --- | --- |
| Hot-disable | `swiftmqctl plugins disable my-sidecar` o `PUT /api/plugins/my-sidecar/disable` | **Isinasara ang panlabas na listener ng plugin** (capability-level disable); hindi apektado ang kernel at ibang plugin |
| Hot-enable | `swiftmqctl plugins enable my-sidecar` | Muling binubuksan ang listener nito; kung nabigo ang nakaraang startup, magre-retry nang isang beses |
| Tingnan ang estado | `swiftmqctl plugins list/show` | Estado + dahilan ng pagkabigo/pagkawala ng koneksyon |
| Kernel exit | — | Kinakalas ang koneksyon sa plugin, nire-reclaim ang bridged session, **tinatapos ang child process na ini-launch ng kernel sa pamamagitan ng `spawn`** |

> Ang hot-disable ay nagsasara lamang ng "capability" (listening port), **hindi** pinapatay ang plugin process na ini-launch gamit ang `spawn`; nangyayari ang pag-reclaim ng process kapag nag-exit ang kernel.

### 5.8 Pagbibigay ng Entry sa Admin Console (opsyonal)

Kapag may sariling UI ang plugin, magdagdag ng `console_url` (address ng admin UI, tingnan ang §5.1 para sa ibang field) sa `plugins.<pangalan ng plugin>` section.
Nasa ibaba ang ipinapakitang `plugins.my-sidecar` entry lamang (**standard JSON, hindi maaaring maglaman ng comment**; pareho ang nilalaman ng `sidecar` section sa §5.1):

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

- Ang **Plugin Management** page ng admin console (ang data ay mula sa `console_url` field ng `GET /api/plugins`) ay magpapakita ng
  "Buksan ang Admin UI" na button para sa ganitong plugin, na **binubuksan sa bagong tab**.
- Kapag hindi idineklara ang `console_url`, hindi available ang button, at may tooltip na "walang admin UI ang plugin na ito".
- Ito ay **metadata lamang na isinulat ng deployer sa config**: hindi ito bahagi ng plugin API (`pkg/plugin`), hindi kasali sa start-stop ng plugin,
  at ang UI mismo ay hina-host ng plugin (maaaring nasa plugin process, o sa anumang standalone service).

---

## 6. Lifecycle at Fault-Tolerance Matrix

| Sitwasyon | Gawi ng kernel | Epekto sa plugin side |
| --- | --- | --- |
| Hindi nagsimula ang plugin process / tinanggihan ang handshake | Nagre-retry ng koneksyon sa loob ng 8s; kung nabigo pa rin, minamarkahan na `failed` at ini-isolate (hindi hinaharang ang startup ng kernel) | Wala |
| Nag-exit ang process na ini-launch ng `spawn` | Nilo-log ito; minamarkahan na `down`; nagba-backoff at muling kumokonekta / nagre-relaunch ayon sa `restart` policy | Muling nagha-handshake ang bagong process |
| Nag-crash ang plugin process (sa runtime) | Hindi apektado ang kernel; `down` + backoff reconnect | Isinasara ng `pkg/sidecar.Server` ang koneksyong iyon |
| Na-`kill -9` ang kernel | — | Nire-reclaim ng plugin side ang koneksyon sa pamamagitan ng idle timeout (default na 24s na walang frame), walang naiiwang zombie |
| Normal na pag-exit ng kernel | Tinatawag ang `Stop`: kinakalas ang koneksyon, nire-reclaim ang hindi pa na-se-settle na delivery (bilang requeue), `Kill` ang child process | Tumanggap ng SIGKILL |
| Mga delivery habang patay ang koneksyon ng plugin | Ang lahat ng hindi pa na-se-settle na delivery ay **irerequeue**, hindi mawawala | — |
| Nadiskonekta ang client / bumalik ang `Open` | Isinasara ang kaukulang stream, pine-release ang session at consumer ng stream na iyon | Nagbabalik ng EOF ang `Stream.Read` |

---

## 7. Red Lines at Kilalang Hangganan

**Red lines**

1. Ang plugin ay pinapayagang umasa lamang sa `pkg/sidecar` (at opsyonal na `pkg/plugin`); **hindi dapat** itong umasa sa `internal/**` ng kernel.
2. Dapat tumugma ang plugin name sa config at ang `APIVersion` sa kernel, kung hindi ay hindi ito makakakonekta (ito ang pumipigil sa "tahimik na tumatakbo pero hindi epektibo").
3. Kapag gumagamit ng `session.*`: **`core.authenticate` muna, pagkatapos `session.open`**, at i-settle ang bawat delivery **nang eksaktong isang beses**.
4. Dapat hindi laman ang `protocols[].prefix`, kung hindi ay hindi ibibigay sa plugin ang koneksyon (tingnan ang §5.3).
5. Ang pagtanggi sa `Hello` ay dapat **tahasang magbalik ng error** (huwag manatiling tahimik) —— kung hindi, ang makikita lamang ng kernel ay "sarado ang koneksyon", at hindi nito matutukoy ang dahilan.

**Mga kilalang hangganan**

- **Nasa kernel side ang sniffing**: hindi maaaring mag-customize ng sniff function ang external plugin, maaari lamang tumugma sa `prefix` (ASCII, ≤ 8 bytes);
  ang walang laman na `prefix` ay "walang makukuhang koneksyon" (tingnan ang §5.3).
- **Dumadaan sa lokal na proxy ang data plane**: walang fd passing (walang `SCM_RIGHTS` sa Windows), isang dagdag na memory copy kumpara sa in-process;
  isang dagdag na lokal na RPC din ang bawat reverse call.
- **Nagde-degrade ang type ng property table**: ang `Properties.Headers` ay dumadaan sa JSON, nawawala ang pagkakaiba tulad ng `int32` / `double` (tingnan ang §3.4).
- **Ang backpressure ng isang stream ay nakakaapekto sa buong koneksyon**: kapag napuno ang receive buffer ng isang stream, nahaharang ang dispatch goroutine ng koneksyong iyon; ang per-stream rate limiting ay susunod na optimization.
- **Text lamang ang ipinapasa kapag nabigo ang auth**: ang pagkabigo ng auth sa kernel ay isang `*plugin.AuthError` (ibang klasipikasyon kaysa sa `plugin.ErrorKind`),
  text lamang ang dumarating sa plugin sa pamamagitan ng bridge, at kailangang i-map ito ng plugin sa protocol error codes ayon sa sarili nitong convention.
- **Ang `net.listen` capability lamang ang tunay na epektibo**: ang `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` ay **reserved slots**; ang pagdedeklara sa mga ito ay kasali lamang sa auditing (tingnan ang governance display sa §5.6), at kasalukuyang walang kaukulang extension point.

---

## 8. Troubleshooting FAQ

| Sintomas | Dahilan at paghawak |
| --- | --- |
| Estado `failed`, may dahilang "hindi tugma ang plugin name" | Ang naka-config na plugin name ≠ `HelloAck.name`; pagtugmain ang mga ito |
| Estado `failed`, may dahilang "hindi tugma ang API version" | `HelloAck.api_version` ≠ `APIVersion` ng kernel; pagtugmain ang mga ito |
| Estado `failed`, may dahilang "tinanggihan ang handshake" | Nagbalik ng error ang `Hello` ng plugin (`deny`); tingnan ang output ng plugin na ipinasa sa kernel log |
| Estado `failed`, may dahilang "nabigong kumonekta sa external plugin" | Hindi umandar ang process / mali ang `address` / hindi maisusulat ang socket path (sa container, pansinin ang permission ng `swiftmq` user) |
| Estado `down` | Nag-crash ang plugin process o naputol ang koneksyon; ang `restart=always` ay awtomatikong muling kumokonekta, ang `never` ay nangangailangan ng manu-manong pag-launch |
| Hindi nakabukas ang port / hindi makakonekta ang client | Hindi naka-configure ang `protocols[].listeners` o na-override ng `listeners.<pangalan ng protocol>` ang address nito; tsekan ang dalawang lugar |
| Nakakonekta ang client sa ibang port at agad na nadiskonekta | Hindi tumutugma sa iyong protocol ang port na iyon (walang laman ang `prefix` o hindi tugma ang prefix); mag-configure ng hindi laman na `prefix` para sa protocol (tingnan ang §5.3) |
| Error na `ACCESS_REFUSED - ... for user ''` | **Walang authentication** bago ang semantic bridge; tawagin muna ang `core.authenticate` bago ang `session.open` |
| Nagre-report ang reverse call ng "stream N ay hindi pa nagbukas ng session" | Gawin muna ang `core.authenticate`, pagkatapos `session.open`, at saka lamang tumawag ng ibang `session.*` |
| Walang natatanggap na consumer delivery | Ang delivery ay dumarating sa iyong `Call` bilang **forward call** na `session.deliver`; kumpirmahin na hina-handle ang method na iyon |
| Ang plugin ay nasa labas ng container, ang kernel ay nasa loob, hindi makakonekta | Gamitin ang `address: tcp://host.docker.internal:<port>` (o ilagay din ang plugin sa container at gamitin ang service name); dapat nakikinig ang plugin sa `0.0.0.0` |

---

## 9. Reference (Source Index)

| Ang gusto mong tingnan | File |
| --- | --- |
| Wire protocol at implementasyon sa magkabilang panig (**required reading para sa development**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frame), `proto.go` (message), `server.go` (plugin side), `client.go` (kernel side), `bridge.go` (`session.*` contract), `stream.go` (stream) |
| Kernel-side sidecar host (integration/reconnect/proxy/state) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Kernel-side semantic bridge (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Kernel session operation-surface types (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Listening, sniffing, per-plugin hot start-stop | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Plugin lifecycle at governance (isolation/state/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Mga config item at halimbawa (kasama ang sidecar section) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Process wiring (paano isinasaksak ang sidecar sa kernel) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Go reference implementation (gumagamit ng `pkg/sidecar.Server`, may `session.*` bridge at `core.authenticate`) | Hiwalay na test project na `swiftmq-test/test/integration/echosidecar/` |
| **Per-language guides + halimbawang proyekto** | `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md` sa direktoryong ito; ang mga halimbawa ay nasa **workspace** na `swiftmq-plugin/{python,nodejs,php,java}/` |
