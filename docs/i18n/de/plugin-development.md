# SwiftMQ – Leitfaden zur Entwicklung externer Prozess-Plugins (Sidecar)

> **Zielgruppe**: Entwickler, die den Kernel nicht forken / neu kompilieren wollen, sondern SwiftMQ in **beliebiger Sprache** erweitern möchten.
> **Umfang**: Dieses Dokument behandelt nur eine Plugin-Form — das **externe Prozess-Plugin** (Kernel-Begriff `sidecar`). Die im Kernel eingebauten Protokoll-Plugins (AMQP 0-9-1 / MQTT) liegen außerhalb des Umfangs dieses Dokuments.
> **Leseempfehlung**: Die Abschnitte 1–2 vermitteln das mentale Modell, Abschnitt 3 behandelt das Schreiben von Code, **Abschnitt 5 erklärt „wie man es nach der Entwicklung einbindet und zusammen laufen lässt, um nach außen Dienste anzubieten"**;
> **für andere Sprachen (Python / Node.js / PHP / Java) siehe die sprachspezifischen Leitfäden in §4** (jeder mit einem vollständig praxiserprobten Beispielprojekt).
> Der Code in diesem Dokument ist ein minimales lauffähiges Skelett, das du direkt als Ausgangspunkt kopieren kannst. Vereinfachtes Chinesisch ist die Ausgangssprache.

---

## 1. Was es ist

Ein **eigenständiger Prozess**, der in seinem eigenen Prozess ein bestimmtes „Protokoll" implementiert (den Byte-Stream des Clients parst);
der Kernel hostet ihn gemäß der Konfiguration: **Ports werden vom Kernel geöffnet, Verbindungen werden vom Kernel proxied**, und Registrierung / Start-Stopp / Auditierung / Isolation nutzen allesamt die bestehenden Mechanismen des Kernels.

Zunächst drei korrekte mentale Modelle aufbauen (die am leichtesten falsch verstandenen Punkte):

1. **Der Plugin-Prozess ist ein „lokaler Dienst"**: Er lauscht nur auf einer **lokalen Adresse** (TCP oder unix socket) und wartet darauf, dass **der Kernel sich verbindet**.
   Die Verbindungsrichtung ist **Kernel (Client) → Plugin (Server)**, und auch den Handshake sendet zuerst der Kernel.
2. **Der nach außen gerichtete Geschäftsport wird nicht vom Plugin geöffnet**: Er wird von **dem Kernel** gemäß `protocols[].listeners` in der Konfiguration erstellt und für die Clients nach außen abgebildet.
   Die Clients verbinden sich mit **dem Port des Kernels**, und die Bytes werden vom Kernel zum Plugin-Prozess weitergeleitet. Der Plugin-Prozess **muss keinen** eigenen Geschäftsport öffnen.
3. **Semantik ist optional**: Das Plugin kann nur „Bytes bewegen" (das Protokoll vollständig selbst implementieren),
   oder über **Reverse-Aufrufe** auf `session.*` die Kernel-Semantik erreichen (Warteschlangen / Routing / Berechtigungen / Bestätigungen),
   und zwar mit **genau derselben Semantik** wie die eingebauten Protokoll-Plugins (vhost, Berechtigungen, Routing und Warteschlangenverhalten weichen dadurch nicht voneinander ab).

| Vorteile | Kosten |
| --- | --- |
| Erweiterbar ohne Änderung oder Neukompilierung des Kernels | Eine zusätzliche lokale Byte-Kopie auf der Datenebene (Kernel-Proxying, keine fd-Übergabe, plattformübergreifend konsistent) |
| Implementierung in beliebiger Sprache (es genügt, das Wire-Protokoll zu implementieren) | Ein zusätzlicher lokaler RPC pro Reverse-Aufruf (JSON-Kodierung/-Dekodierung + Kopie) |
| Das Plugin kann unabhängig veröffentlicht / aktualisiert / neu gestartet werden | Das Sniffing bleibt auf der Kernel-Seite: Erkennung nur über „Präfix" oder „dedizierten Port" |
| Ein Absturz betrifft nur dieses Plugin: Der Kernel markiert es als `down` und beendet nicht / stürzt nicht ab | Nur die Fähigkeit `net.listen` wird tatsächlich wirksam; andere Fähigkeitswerte sind reserviert (siehe §7) |

---

## 2. Funktionsweise

Verbindungsaufbau (**der Kernel ist der Client, das Plugin der Server**):

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

Beginn des Dienstes:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Lebenszyklus** (Host auf der Kernel-Seite `internal/plugin/sidecar`):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Status-Semantik** (sichtbar in `swiftmqctl plugins show`):

| Status | Bedeutung | Betriebsmaßnahme |
| --- | --- | --- |
| `enabled` | Eingebunden und im Dienst | — |
| `failed` | **Beim Start nicht hochgekommen** (Konfigurationsfehler / Handshake abgelehnt / Prozess lässt sich nicht starten) | `RuntimeNote` und Kernel-Logs prüfen, Konfiguration ändern oder Plugin reparieren; ein Neustart des Kernels ist nötig, um es erneut zu versuchen |
| `down` | **War schon oben, ist jetzt weg** (Prozess abgestürzt / Verbindung getrennt) | Den Plugin-Prozess wieder starten; der Kernel stellt gemäß der `restart`-Strategie automatisch wieder her |
| `disabled` | `enabled=false` in der Konfiguration oder vom Betrieb zur Laufzeit deaktiviert | Mit `plugins enable <Name>` wiederherstellen |

---

## 3. Entwicklung (Go)

### 3.1 Projekt anlegen

Das Plugin ist ein **eigenständiges Go-Modul** und hängt nur von zwei öffentlichen Vertragspaketen ab:

- `github.com/houzch/swiftmq/pkg/sidecar` — Wire-Protokoll und plugin-seitige Implementierung (**erforderlich**)
- `github.com/houzch/swiftmq/pkg/plugin` — nur wenn du Typen wie `plugin.Message` / `plugin.Error` verwenden willst (optional)

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

> Beim gemeinsamen Testen mit `replace` müssen Plugin und Kernel **denselben Quelltext** verwenden; sonst kann es sein, dass die API-Version (`v1`) zwar übereinstimmt, die Typen aber unterschiedlich sind.

### 3.2 `Handler` implementieren (drei Methoden)

Die gesamte fachliche Oberfläche des Plugin-Prozesses besteht aus `Hello` / `Call` / `Open` von `sidecar.Handler`.
Handshake, Heartbeat, Multiplexing und Fragmentierung werden von `pkg/sidecar` übernommen; du musst die Frames nicht anfassen.

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

### 3.3 Datenebene: `Stream` lesen und schreiben

`sidecar.Stream` implementiert `io.ReadWriteCloser` und kann direkt als „eine Verbindung" verwendet werden:

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

Kernpunkte:

- Jede Client-Verbindung = ein Stream; das Plugin kann in `Handler.Open` sofort mit Senden/Empfangen beginnen.
- Große Nachrichten werden von der Bibliothek automatisch fragmentiert (pro Frame ≤ 64 KiB), **der Speicherverbrauch ist unabhängig von der Nachrichtengröße**.
- Backpressure: Der Empfangspuffer eines einzelnen Streams hat eine Obergrenze; ist der Puffer voll, blockiert die **Verteilungs-Goroutine** dieser Verbindung (alle Streams warten gemeinsam) —
  das ist der Kompromiss zwischen „vorhersagbarem Speicher" und „Geschwindigkeitsbegrenzung pro Stream", siehe §7.

### 3.4 Die Kernel-Semantikbrücke verwenden (`session.*`)

Wenn das Plugin die Warteschlangen- / Routing- / Berechtigungs- / Bestätigungssemantik des Kernels wiederverwenden soll (statt eine eigene zu bauen), verwendet man **Reverse-Aufrufe**.
Auf einem Stream wird die Reihenfolge `session.open` → weitere `session.*` → (`session.close`) eingehalten:

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

**Übersicht der Reverse-Aufrufmethoden** (Konstanten in `pkg/sidecar/bridge.go` → Wire-Namen):

| Gruppe | Wire-Name (Konstante) | Beschreibung |
| --- | --- | --- |
| Authentifizierung | `core.authenticate` (`MethodCoreAuthenticate`) | **Muss zuerst erfolgen**: Übergibt die Anmeldeinformationen aus dem Protokoll zur Prüfung an den Kernel; Parameter `{stream, mechanism, response}`, Rückgabe `{user}` |
| Sitzung | `session.open` (`MethodSessionOpen`) | Öffnet auf dem Stream eine Sitzung für einen vhost; erst **nach der Authentifizierung** möglich |
| | `session.close` (`MethodSessionClose`) | Gibt die Sitzung auf dem Stream frei (Consumer abbestellen, exklusive Warteschlangen löschen) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Anlegen/Löschen (passives Deklarieren eines nicht vorhandenen → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Bindung von Exchange zu Exchange |
| Warteschlange | `session.declare_queue` / `session.delete_queue` | Anlegen/Löschen; ist `name` leer, generiert der Server ihn |
| | `session.bind_queue` / `session.unbind_queue` | Bindung von Warteschlange zu Exchange |
| | `session.purge_queue` | Leert die bereitliegenden Nachrichten (ohne unbestätigte) |
| Publish | `session.publish` | Rückgabe `{routed, rejected}`; die Persistierung erfolgt vor der Antwort |
| Get | `session.get` | Aktiv eine Nachricht abholen; `found=false` bedeutet, die Warteschlange ist leer |
| Consume | `session.consume` / `session.cancel` | Consumer registrieren / abbestellen |
| Abrechnung | `session.settle` | Eine Zustellung abrechnen (`ack` / `requeue` / `reject`) |
| **Forward** | `session.deliver` | **Kernel → Plugin**: Zustellungs-Push (in deinem `Handler.Call` behandeln) |

**Vier einzuhaltende Konventionen**:

1. **Zuerst `core.authenticate`**: Die Kernel-Betriebsebene der Verbindung hat vor der Authentifizierung keine Identität,
   ein `session.open` zu diesem Zeitpunkt wird abgelehnt (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Das Plugin ist dafür zuständig, die Anmeldeinformationen aus seinem eigenen Protokoll zu entnehmen; die Authentifizierungslogik und die Benutzertabelle bleiben im Kernel, das Plugin berührt die Passwortdatenbank nicht.
2. **Dann `session.open`**: Ruft man andere Methoden auf, ohne eine Sitzung geöffnet zu haben, gibt der Kernel `KindPreconditionFailed` zurück („Stream N hat noch keine Sitzung geöffnet").
3. **Jede Zustellung genau einmal abrechnen**: Eine von `Ack` / `Requeue` / `Reject`.
   Sowohl `Ack` als auch `Reject` verwerfen die Nachricht, **nur `Reject` geht in die Dead-Letter-Queue**.
4. **Nicht abgerechnete Zustellungen gehen nicht verloren**: Wenn der Stream endet (Client getrennt / `Handler.Open` kehrt zurück) oder die Plugin-Verbindung abbricht,
   behandelt der Kernel alle nicht abgerechneten Zustellungen **als „zurück in die Warteschlange"**, um ein Verharren der Nachrichten zu vermeiden.

**Fehler-Rückgewinnung**: Der `*plugin.Error` des Kernels erreicht dich über die Brücke als `*sidecar.RPCError` (Felder `Kind` / `Text`) und kann in einen klassifizierten `plugin.Error` zurückverwandelt werden, statt die Klassifizierung in einer Zeichenkette zu verlieren:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Häufige Zuordnung von `Kind` zu den eingebauten Protokollen (damit du entscheiden kannst, wie du dem Client den Fehler zurückmeldest):

| `plugin.ErrorKind` | Semantik | AMQP 0-9-1-Zuordnung |
| --- | --- | --- |
| `KindNotFound` | Objekt existiert nicht | 404 NOT_FOUND (Channel schließen) |
| `KindPreconditionFailed` | Parameter stimmen nicht mit vorhandenem Objekt überein / Sitzung nicht geöffnet | 406 PRECONDITION_FAILED (Channel schließen) |
| `KindAccessRefused` | Unzureichende Berechtigung / reservierter Name | 403 ACCESS_REFUSED (Channel schließen) |
| `KindResourceLocked` | Exklusive Ressource ist belegt | 405 RESOURCE_LOCKED (Channel schließen) |
| `KindInvalidPath` | vhost existiert nicht | 402 INVALID_PATH (Verbindung schließen) |
| `KindNotImplemented` | Fähigkeit nicht implementiert | 540 NOT_IMPLEMENTED (Verbindung schließen) |
| `KindInternal` | Interner Kernel-Fehler | 541 INTERNAL_ERROR (Verbindung schließen) |

**Grenzen der Typentreue von Nachrichten**: Die Attributtabelle (`MessageDTO.Properties.Headers`) läuft über JSON,
sodass die Zahlentypinformationen, die `int32` / `double` unterscheiden — wie bei einer AMQP field-table — **nicht erhalten werden können**.
Wenn strikte Typentreue benötigt wird, lege diese Informationen in den Nachrichtenkörper (Rohbytes) und trage sie selbst.

### 3.5 Plugin-seitiger Status und Logging

- Der **Status eines externen Plugins wird durch „ob die Verbindung lebt" bestimmt**; das Plugin muss ihn nicht selbst melden (der `StateReporter` eingebauter Plugins gilt nicht für externe Prozesse).
- Logging: `sidecar.ServerOptions.Logger` gibt auf stdout/stderr des Plugin-Prozesses aus;
  **wenn der Kernel den Prozess per `spawn` startet, leitet der Kernel diese Ausgaben in das Kernel-Log weiter** (mit dem Tag `plugin`), was eine einheitliche Erfassung erleichtert.
- Bei eigenständiger Bereitstellung (kein spawn) erfasst du die Plugin-Logs auf deine eigene Weise.

### 3.6 Auch ohne Kernel-Anbindung selbst testen

`Handler` ist ein gewöhnliches Go-Interface; in Unit-Tests kannst du es direkt instanziieren und `Hello` / `Call` / `Open` aufrufen, um die Geschäftslogik abzudecken, ohne Netzwerk starten zu müssen:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Die End-to-End-Verifizierung erfolgt in §5.

---

## 4. Entwicklung mit anderen Sprachen (Wire-Protokoll-Spezifikation)

`pkg/sidecar` ist ein abhängigkeitsfreier öffentlicher Vertrag; das Wire-Protokoll selbst ist sehr einfach und in jeder Sprache implementierbar.
Zur Anbindung musst du die folgenden „Byte-Ebene"-Konventionen implementieren (Quelltext siehe `pkg/sidecar/frame.go`, `proto.go`).

> **Es stehen sprachspezifische Leitfäden mit vollständigen Beispielprojekten zur Verfügung** (die Beispiele haben alle praxiserprobt den Ablauf Handshake → Authentifizierung → semantische Brücke → Zustellung/Abrechnung → Byte-Stream durchlaufen):
>
> | Sprache | Leitfaden | Beispielprojekt (Workspace `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (nur Standardbibliothek) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (nur Standardbibliothek) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (nur Standardbibliothek) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (eine Datei, nur JDK) |
>
> Die vollständige Go-Referenzimplementierung findest du im eigenständigen Testprojekt `swiftmq-test/test/integration/echosidecar/` (es verwendet direkt `pkg/sidecar.Server`
> und muss sich nicht um die Byte-Ebenen-Details unten kümmern).

**Frame-Format** (für alle Frames einheitlich):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Frame-Typ `kind`**:

| kind | Name | Richtung | Nutzlast |
| --- | --- | --- | --- |
| 1 | Hello | Kernel → Plugin | JSON `Hello` |
| 2 | HelloAck | Plugin → Kernel | JSON `HelloAck` |
| 3 | Ping | Kernel → Plugin | leer |
| 4 | Pong | Plugin → Kernel | leer |
| 5 | Call | bidirektional | JSON `Call` |
| 6 | Reply | bidirektional | JSON `Reply` |
| 7 | Open | Kernel → Plugin | JSON `Open` |
| 8 | OpenAck | Plugin → Kernel | JSON `OpenAck` |
| 9 | Data | bidirektional | `u32 BE stream` + Rohbytes |
| 10 | Close | bidirektional | JSON `Close` |

**JSON-Struktur der Steuerebene** (Feldnamen stimmen mit `proto.go` überein).

> Dieser Abschnitt ist ein **Beispiel für Wire-Protokoll-Nachrichten** (in demselben Block werden mehrere Nachrichten der Reihe nach angegeben, daher mit `//` getrennt erläutert),
> **keine Konfiguration, die direkt in `swiftmqd.json` geschrieben werden kann**.

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

**Zwingend einzuhaltende Semantik**:

- **Handshake**: Der Kernel sendet zuerst `Hello`, das Plugin muss einen `HelloAck`-Frame zurücksenden.
  Der Kernel prüft `HelloAck.name == Plugin-Name in der Konfiguration` und `HelloAck.api_version == APIVersion des Kernels`;
  ein nicht leeres `deny` gilt als Ablehnung der Verbindung (das Plugin wird isoliert).
- **Heartbeat**: Der Kernel sendet standardmäßig alle 2 s ein `Ping`, das Plugin muss innerhalb von 8 s mit `Pong` antworten; wenn auf der Plugin-Seite innerhalb von 24 s kein Frame empfangen wird, darf sie die Verbindung selbst schließen.
- **Zwei ID-Räume**: `reverse` im `Call` unterscheidet die Richtung; beide Richtungen zählen jeweils ab 1 hoch, daher **muss `Reply` das `reverse` mitführen**, sonst wird die Antwort an den falschen Wartenden übergeben.
- **Datenebene ohne base64**: Große Datenblöcke wie der Nachrichtenkörper werden direkt in die Nutzlast des `Data`-Frames gelegt (`stream` + Rohbytes) und bei Bedarf fragmentiert.

> Bei Verwendung von Go genügt `pkg/sidecar` direkt; die oben genannten Details musst du nicht selbst implementieren.

---

## 5. Einbinden und gemeinsam betreiben: Anbindung, Dienstanbot nach außen, Paketierung ★

Dieser Abschnitt beantwortet „wie man es nach der Entwicklung in SwiftMQ einbindet und wie man nach außen Dienste anbietet".

### 5.1 Plugin in der Konfiguration deklarieren

Externe Plugins werden **vollständig durch die Konfiguration verwaltet**; der Kernel muss dafür keinerlei Code ändern. Füge im `plugins`-Abschnitt von `swiftmqd.json` einen Eintrag hinzu
(**die tatsächliche Konfiguration ist Standard-JSON und darf keine Kommentare enthalten**):

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

Erläuterung Punkt für Punkt (Feldliste siehe Tabelle unten):

- Der Schlüsselname von `plugins.<Plugin-Name>` **muss mit dem vom Plugin selbst gemeldeten `HelloAck.name` übereinstimmen**, sonst wird der Handshake abgelehnt.
- `builtin: false`: explizite Deklaration für externe Plugins (ohne Angabe wird es in der Verwaltungsoberfläche als eingebaut dargestellt).
- `enabled`: Ausschalten = Prozess wird nicht gestartet, kein Listener erstellt.
- Bei `required: true` blockiert ein Startfehler den Start des Kernels — bei externen Plugins nicht aktivieren.
- `address` ist **die Adresse, mit der der Kernel sich verbindet** (der Kernel ist der Client); ist `spawn` nicht leer, startet der Kernel den Prozess stellvertretend.
- `protocols[].prefix` **darf nicht leer sein** (Sniffing-Regeln siehe §5.3).
- `listeners` ist der **nach außen gerichtete Port** dieses Protokolls, **der vom Kernel geöffnet wird** (der Client verbindet sich mit dem Kernel).

Feldübersicht:

| Feld | Erforderlich | Beschreibung |
| --- | --- | --- |
| `sidecar.address` | ✅ | Adresse des Plugin-Prozesses: `tcp://host:port` oder `unix:///path` |
| `sidecar.spawn` | ✕ | Kommandozeile, die der Kernel stellvertretend startet (erstes Element ist die ausführbare Datei); **leer = der Kernel verbindet sich nur, startet nicht**, den Prozess verwaltest du selbst |
| `sidecar.restart` | ✕ | `always` (Standard, automatische Wiederherstellung nach Verbindungsabbruch/Absturz) oder `never` (nur als `down` markieren, auf Eingreifen des Betriebs warten) |
| `sidecar.protocols[].name` | ✅ | Protokollname (global eindeutig, nimmt an der Sniffing-Priorität teil) |
| `sidecar.protocols[].prefix` | ✕ | Sniffing-Präfix (ASCII); **leer = keine Teilnahme am Sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Der nach außen gerichtete Listener dieses Protokolls (`name` + `addr`), vom Kernel erstellt |
| `sidecar.handshake_timeout_seconds` | ✕ | Überschreibt das Handshake-Timeout (Standard 5 s) |
| `sidecar.heartbeat_seconds` | ✕ | Überschreibt das Heartbeat-Intervall (Standard 2 s) |

> **Plugin-Name und Protokollname**: Beide **dürfen unterschiedlich sein** (z. B. stellt das Plugin `my-sidecar` das Protokoll `myproto` bereit).
> Ein Hot-Disable sucht zuerst anhand des Plugin-Namens alle von ihm registrierten Protokolle und schließt dann die Ports dieser Protokolle; es ist daher nicht nötig, sie bewusst gleich zu benennen.

### 5.2 Drei Anbindungsarten

| Anbindungsart | Konfiguration | Geeignet für |
| --- | --- | --- |
| **Gleicher Host + Start durch den Kernel (spawn)** | `spawn: [...]`, `address` zeigt auf die von ihm gelauschte Adresse | Bereitstellung auf demselben Rechner, einzelner Container; am einfachsten, der Kernel übernimmt Start und Aufräumen |
| **Gleicher Host + Selbstverwaltung (dial)** | `spawn: []`, `address` zeigt auf den bereits laufenden Prozess | Verwaltung des Plugin-Lebenszyklus mit systemd / supervisor |
| **Hostübergreifend / containerübergreifend (dial, zwingend tcp)** | `spawn: []`, `address: "tcp://<Dienstname>:19001"` | Plugin und Kernel in getrennten Containern / auf getrennten Rechnern bereitgestellt |

Adresswahl:

- **Auf demselben Rechner wird unix socket empfohlen** (`unix:///tmp/my-sidecar.sock`): belegt keinen TCP-Port und ist von belegten Host-Ports unabhängig.
  Beachte, dass der socket-Pfad für den Kernel-Prozess (im Container der Nicht-root-Benutzer `swiftmq`) beschreibbar sein muss.
- **Containerübergreifend ist TCP zwingend erforderlich**, und der Plugin-Prozess muss auf `0.0.0.0` lauschen; `address` verwendet den **Dienstnamen im Container-Netzwerk**.

> Die Richtung nicht verwechseln: **Die vom Plugin gelauschte Adresse** = `address`; **der nach außen für Clients geöffnete Port** = `protocols[].listeners`.

### 5.3 Dienstanbot nach außen: Erkennung über `prefix`

Die Zugangsschicht verteilt Verbindungen **allein anhand des Sniffing-Ergebnisses**: Sie fragt für jedes aktivierte Protokoll in Registrierungsreihenfolge `Sniff(peek)` ab (peek maximal 8 Bytes); wer trifft, übernimmt diese Verbindung. Daher:

1. **`prefix` darf nicht leer sein** (ASCII, ≤ 8 Bytes). Nur wenn die ersten vom Client gesendeten Bytes damit übereinstimmen, wird die Verbindung an dein Plugin übergeben.
   Beispiel: `"prefix": "PY"` → die ersten Bytes des Clients müssen `PY` sein (du kannst das Präfix als Magic-Header deines Protokolls betrachten).
2. **Ein leeres `prefix` bedeutet keine Teilnahme am Sniffing**: Verbindungen dieser Art werden **nicht** an das Plugin übergeben (gemessen: Verbindungen auf dem Listener werden sofort getrennt).
   Ein leeres `prefix` eignet sich daher nur für das Szenario „ein anderes Protokoll leitet auf demselben Port für dich weiter"; verwende es **nicht**, um einen dedizierten Port zu realisieren.
3. `listeners[].addr` bestimmt „auf welchem Port nach außen geöffnet wird", `prefix` bestimmt „ob diese Verbindung dir gehört" —
   beide müssen zusammen verwendet werden: **Auch ein dedizierter Port braucht ein nicht leeres `prefix`** (das ist auch der Grund, warum in der Beispielkonfiguration des Kernels
   `echo-sidecar` sowohl `prefix: "ECHO"` als auch `listeners: [":1885"]` angibt).
4. Das Sniffing gleicht in Protokoll-Registrierungsreihenfolge ab, **der erste Treffer gilt**: Wenn mehrere Plugins koexistieren, müssen die Präfixe unterscheidbar sein (z. B. verdecken sie sich gegenseitig, wenn sie alle mit demselben Byte beginnen).

### 5.4 Lauschadresse und TLS überschreiben

- Die nach außen gerichtete Lauschadresse kann an **zwei Stellen** angegeben werden: `sidecar.protocols[].listeners[].addr` (Standard) und
  `listeners.<Protokollname>` (vollständige Überschreibung nach Protokollname). Sind beide vorhanden, gilt `listeners.<Protokollname>`.
- Wenn TLS benötigt wird, gib das Zertifikat in `listeners.<Protokollname>[i].tls` an (Felder wie bei eingebauten Protokollen).
  Das Folgende ist ein `listeners`-Fragment (**Standard-JSON, keine Kommentare erlaubt**): Der erste Eintrag ist Klartext, der zweite verwendet TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS wird von **dem Kernel** auf der Listener-Seite terminiert; der Plugin-Prozess erhält einen Klartext-Stream — das Plugin muss sich nicht um TLS kümmern.

### 5.5 Paketierung: Plugin und Kernel zusammen laufen lassen

**Vorgehen A — In dasselbe Image packen** (empfohlen für Plugins, die „mit dem Kernel veröffentlicht" werden): Füge in der Laufzeitphase von `swiftmq/Dockerfile` eine Zeile hinzu:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Setze dann in der Konfiguration `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
wobei `address` denselben Wert hat. Beim Start des Kernels wird es gestartet.

**Vorgehen B — Binärdatei mounten** (ohne das Image zu ändern, geeignet für die gemeinsame Entwicklung):

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

Die Konfiguration verwendet einen unix socket (um keinen zusätzlichen Port zu belegen). Das Folgende ist das `sidecar`-Fragment in `plugins.my-sidecar`
(**Standard-JSON, keine Kommentare erlaubt**; `prefix` muss weiterhin nicht leer sein, siehe §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Vorgehen C — Eigenständiger Container** (Plugin separat veröffentlichen / unabhängig skalieren):

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

In der Konfiguration `spawn: []` (der Kernel verbindet sich nur, startet nicht), `address: "tcp://my-sidecar:19001"` (compose-Dienstname).

### 5.6 Start und Verifikation

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

Achte in `plugins show` besonders auf `state` und `RuntimeNote`:
`failed` liefert den Fehlergrund (Handshake abgelehnt / Port lässt sich nicht starten …); `down` liefert den Trennungsgrund (Prozess abgestürzt / Verbindung getrennt).

### 5.7 Betrieb zur Laufzeit

| Vorgang | Befehl / Schnittstelle | Wirkung |
| --- | --- | --- |
| Hot-Disable | `swiftmqctl plugins disable my-sidecar` oder `PUT /api/plugins/my-sidecar/disable` | **Schließt den nach außen gerichteten Listener dieses Plugins** (Deaktivierung auf Fähigkeitsebene); Kernel und andere Plugins sind nicht betroffen |
| Hot-Enable | `swiftmqctl plugins enable my-sidecar` | Öffnet seinen Listener erneut; bei einem vorherigen Startfehler wird einmal erneut versucht |
| Status anzeigen | `swiftmqctl plugins list/show` | Status + Fehler-/Trennungsgrund |
| Kernel-Beendigung | — | Trennt die Verbindung zum Plugin, räumt die Sitzungen auf der Brücke ab, **beendet die vom Kernel per `spawn` gestarteten Kindprozesse** |

> Ein Hot-Disable schließt nur die „Fähigkeit" (den Listener), **beendet aber nicht** den per `spawn` gestarteten Plugin-Prozess; das Aufräumen des Prozesses erfolgt beim Beenden des Kernels.

### 5.8 Einen Einstieg im Verwaltungs-Backend bereitstellen (optional)

Wenn das Plugin eine eigene Bedienoberfläche mitbringt, füge im Abschnitt `plugins.<Plugin-Name>` ein `console_url` hinzu (Adresse der Verwaltungsoberfläche, die übrigen Felder siehe §5.1).
Das Folgende zeigt nur den Eintrag `plugins.my-sidecar` (**Standard-JSON, keine Kommentare erlaubt**; der Inhalt des `sidecar`-Abschnitts ist derselbe wie in §5.1):

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

- Die Seite **Plugin-Verwaltung** des Verwaltungs-Backends (Daten stammen aus dem Feld `console_url` von `GET /api/plugins`) zeigt für solche Plugins
  die Schaltfläche „Verwaltungsoberfläche öffnen" an, **die in einem neuen Tab geöffnet wird**.
- Ohne deklariertes `console_url` ist die Schaltfläche nicht verfügbar; ein Mouseover-Hinweis lautet „Dieses Plugin stellt keine Verwaltungsoberfläche bereit".
- Es handelt sich lediglich um **Metadaten, die der Bereitsteller in die Konfiguration schreibt**: Sie gehören nicht zur Plugin-API (`pkg/plugin`), nehmen nicht am Start/Stopp des Plugins teil,
  und die Oberfläche selbst wird vom Plugin eigenständig gehostet (sie kann sich direkt im Plugin-Prozess befinden oder in einem beliebigen eigenständigen Dienst).

---

## 6. Lebenszyklus- und Fehlertoleranzmatrix

| Situation | Kernel-Verhalten | Auswirkung auf die Plugin-Seite |
| --- | --- | --- |
| Plugin-Prozess nicht gestartet / Handshake abgelehnt | Wiederholt die Verbindung innerhalb von 8 s; bei weiterem Fehlschlag als `failed` markiert und isoliert (blockiert den Kernel-Start nicht) | keine |
| Der per `spawn` gestartete Prozess beendet sich | Log wird geschrieben; als `down` markiert; gemäß `restart`-Strategie mit Backoff neu verbinden / neu starten | Der neue Prozess führt den Handshake erneut aus |
| Plugin-Prozess stürzt ab (Laufzeit) | Kernel nicht betroffen; `down` + Neuverbindung mit Backoff | `pkg/sidecar.Server` schließt diese Verbindung |
| Kernel wird per `kill -9` beendet | — | Die Plugin-Seite räumt die Verbindung über den Idle-Timeout (standardmäßig 24 s ohne Frame) selbst auf; es bleiben keine Zombies zurück |
| Kernel wird ordnungsgemäß beendet | Ruft `Stop` auf: Verbindung trennen, nicht abgerechnete Zustellungen aufräumen (zurück in die Warteschlange), Kindprozesse `Kill` | Empfängt SIGKILL |
| Zustellungen während einer getrennten Plugin-Verbindung | Nicht abgerechnete Zustellungen werden ausnahmslos **zurück in die Warteschlange** gestellt und gehen nicht verloren | — |
| Client trennt / `Open` kehrt zurück | Schließt den entsprechenden Stream, gibt die Sitzung und Consumer dieses Streams frei | `Stream.Read` gibt EOF zurück |

---

## 7. Rote Linien und bekannte Grenzen

**Rote Linien**

1. Das Plugin darf nur von `pkg/sidecar` (und optional `pkg/plugin`) abhängen; es darf **nicht** vom Kernel `internal/**` abhängen.
2. Der Plugin-Name muss mit der Konfiguration übereinstimmen und `APIVersion` muss mit dem Kernel übereinstimmen, sonst ist keine Anbindung möglich (dies verhindert „still laufen, aber nicht wirksam sein").
3. Bei Verwendung von `session.*`: **zuerst `core.authenticate`, dann `session.open`**, und jede Zustellung **genau einmal abrechnen**.
4. `protocols[].prefix` darf nicht leer sein, sonst wird die Verbindung nicht an das Plugin übergeben (siehe §5.3).
5. Eine Ablehnung in `Hello` muss **ausdrücklich mit einem error beantwortet werden** (nicht stillschweigend) — sonst sieht der Kernel nur „Verbindung geschlossen" und kann die Ursache nicht lokalisieren.

**Bekannte Grenzen**

- **Sniffing auf der Kernel-Seite**: Externe Plugins können keine eigene Sniffing-Funktion definieren, sondern nur über `prefix` (ASCII, ≤ 8 Bytes) abgleichen;
  ein leeres `prefix` bedeutet „bekommt keine Verbindung" (siehe §5.3).
- **Datenebene über lokales Proxying**: keine fd-Übergabe (Windows hat kein `SCM_RIGHTS`), eine zusätzliche Speicherkopie gegenüber dem In-Process-Fall;
  auch jeder Reverse-Aufruf kostet einen zusätzlichen lokalen RPC.
- **Attributtabellen-Typen degenerieren**: `Properties.Headers` läuft über JSON, die Unterscheidung von `int32` / `double` u. Ä. geht verloren (siehe §3.4).
- **Backpressure eines einzelnen Streams betrifft die gesamte Verbindung**: Ist der Empfangspuffer eines Streams voll, blockiert die Verteilungs-Goroutine dieser Verbindung; eine Geschwindigkeitsbegrenzung pro Stream ist eine spätere Optimierung.
- **Bei Authentifizierungsfehlern wird nur Text durchgereicht**: Ein Authentifizierungsfehler des Kernels ist ein `*plugin.AuthError` (nicht dieselbe Klassifizierung wie `plugin.ErrorKind`);
  über die Brücke erreicht das Plugin nur Text, und das Plugin muss ihn nach seiner eigenen Konvention auf einen Protokoll-Fehlercode abbilden.
- **Nur die Fähigkeit `net.listen` wird tatsächlich wirksam**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` sind **reservierte Plätze**; nach der Deklaration nehmen sie nur am Audit teil (siehe die Governance-Darstellung in §5.6), es gibt derzeit keine entsprechenden Erweiterungspunkte.

---

## 8. Fehlerbehebungs-FAQ

| Symptom | Ursache und Vorgehen |
| --- | --- |
| Status `failed`, Grund enthält „Plugin-Name stimmt nicht überein" | Der konfigurierte Plugin-Name ≠ `HelloAck.name`; beide angleichen |
| Status `failed`, Grund enthält „API-Version stimmt nicht überein" | `HelloAck.api_version` ≠ Kernel `APIVersion`; beide angleichen |
| Status `failed`, Grund enthält „Handshake abgelehnt" | Das Plugin `Hello` hat einen error zurückgegeben (`deny`); sieh dir die im Kernel-Log weitergeleiteten Plugin-Ausgaben an |
| Status `failed`, Grund enthält „Verbindung zum externen Plugin fehlgeschlagen" | Der Prozess wurde nicht gestartet / `address` falsch geschrieben / socket-Pfad nicht beschreibbar (im Container auf die Rechte des Benutzers `swiftmq` achten) |
| Status `down` | Der Plugin-Prozess ist abgestürzt oder die Verbindung ist getrennt; `restart=always` verbindet automatisch neu, bei `never` ist ein manueller Start nötig |
| Port nicht geöffnet / Client kann keine Verbindung herstellen | `protocols[].listeners` nicht konfiguriert oder die Adresse wurde von `listeners.<Protokollname>` überschrieben; beide Stellen prüfen |
| Client verbindet sich mit einem anderen Port und wird sofort getrennt | Dieser Port passt nicht zu deinem Protokoll (`prefix` leer oder Präfix stimmt nicht); konfiguriere ein nicht leeres `prefix` für das Protokoll (siehe §5.3) |
| Meldung `ACCESS_REFUSED - ... for user ''` | Vor der semantischen Brücke **wurde nicht authentifiziert**; rufe zuerst `core.authenticate` und dann `session.open` auf |
| Reverse-Aufruf meldet „Stream N hat noch keine Sitzung geöffnet" | Zuerst `core.authenticate`, dann `session.open`, erst danach dürfen andere `session.*` aufgerufen werden |
| Es kommen keine Konsum-Zustellungen an | Zustellungen erreichen dein `Call` als **Forward-Aufruf** `session.deliver`; stelle sicher, dass diese Methode behandelt wird |
| Das Plugin ist außerhalb des Containers, der Kernel darin, keine Verbindung möglich | Verwende für `address` `tcp://host.docker.internal:<port>` (oder packe das Plugin ebenfalls in den Container und verwende den Dienstnamen); das Plugin muss auf `0.0.0.0` lauschen |

---

## 9. Referenz (Quelltextindex)

| Was du sehen möchtest | Datei |
| --- | --- |
| Wire-Protokoll und beidseitige Implementierung (**Pflichtlektüre für die Entwicklung**) | [`pkg/sidecar/`](../../../pkg/sidecar/)：`frame.go` (Frames)、`proto.go` (Nachrichten)、`server.go` (Plugin-Seite)、`client.go` (Kernel-Seite)、`bridge.go` (`session.*`-Vertrag)、`stream.go` (Streams) |
| Sidecar-Host auf der Kernel-Seite (Anbindung/Neuverbindung/Proxying/Status) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Semantische Brücke auf der Kernel-Seite (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Typen der Kernel-Sitzungsbetriebsebene (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Listener, Sniffing, Hot-Start/Stop pro Plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Plugin-Lebenszyklus und Governance (Isolation/Status/Audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go)、[`registry.go`](../../../internal/plugin/registry.go) |
| Konfigurationsoptionen und Beispiele (inkl. sidecar-Abschnitt) | [`internal/config/config.go`](../../../internal/config/config.go)、[`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Prozess-Assemblierung (wie sidecar in den Kernel eingebunden wird) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Go-Referenzimplementierung (verwendet `pkg/sidecar.Server`, inkl. `session.*`-Brücke und `core.authenticate`) | Eigenständiges Testprojekt `swiftmq-test/test/integration/echosidecar/` |
| **Sprachspezifische Leitfäden + Beispielprojekte** | In diesem Verzeichnis `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; Beispiele im **Workspace** `swiftmq-plugin/{python,nodejs,php,java}/` |
