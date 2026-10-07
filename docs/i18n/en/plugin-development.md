# SwiftMQ External-Process Plugin (sidecar) Development Guide

> **Audience**: developers who do not want to fork / recompile the kernel but want to extend SwiftMQ in **any language**.
> **Scope**: this document covers only one plugin form — the **external-process plugin** (kernel term `sidecar`). Kernel built-in protocol plugins (AMQP 0-9-1 / MQTT) are out of scope.
> **How to read**: Sections 1–2 build the mental model, Section 3 is about writing code, and **Section 5 covers "how to wire it in and run it together to serve clients once development is done"**;
> **for other languages (Python / Node.js / PHP / Java), see the per-language guides in §4** (each ships with a complete, empirically verified example project).
> The code in this document is a minimal runnable skeleton that you can copy as a starting point. Simplified Chinese is the source language.

---

## 1. What It Is

A **standalone process** that implements some "protocol" (parses client byte streams) in its own process.
The kernel hosts it per configuration: **ports are opened by the kernel and connections are proxied by the kernel**, and registration / start-stop / auditing / isolation all reuse the kernel's existing mechanisms.

First, establish three correct mental models (the most easily mistaken points):

1. **The plugin process is a "local service"**: it listens only on a **local address** (TCP or unix socket) and waits for **the kernel to connect**.
   The connection direction is **kernel (client) → plugin (server)**, and the kernel also sends the handshake first.
2. **The external business port is not opened by the plugin**: it is created by **the kernel** according to `protocols[].listeners` in the configuration and mapped out to clients.
   Clients connect to **the kernel's port**, and bytes are proxied by the kernel to the plugin process. The plugin process **does not need** to open a business port itself.
3. **Semantics are optional**: the plugin can merely "move bytes" (implementing the protocol entirely by yourself),
   or it can reach kernel semantics (queues / routing / permissions / acknowledgement) via **reverse calls** to `session.*`,
   sharing **exactly the same semantics** as built-in protocol plugins (so vhost, permissions, routing, and queue behavior will not diverge).

| Benefits | Costs |
| --- | --- |
| Extend without changing or recompiling the kernel | One extra local byte copy on the data plane (kernel proxying, no fd passing, cross-platform consistent) |
| Implement in any language (you only need to implement the wire protocol) | One extra local RPC per reverse call (JSON encoding/decoding + copy) |
| The plugin can be released / upgraded / restarted independently | Sniffing stays on the kernel side: it can only be recognized by "prefix" or a "dedicated port" |
| A crash only affects that plugin: the kernel marks it `down` without exiting or crashing | Only the `net.listen` capability truly takes effect; other capability values are reserved (see §7) |

---

## 2. How It Works

Connection setup (**the kernel is the client, the plugin is the server**):

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

Serving begins:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Lifecycle** (the `internal/plugin/sidecar` host on the kernel side):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**State semantics** (visible via `swiftmqctl plugins show`):

| State | Meaning | Operator action |
| --- | --- | --- |
| `enabled` | Connected and serving | — |
| `failed` | **Never came up at startup** (bad configuration / handshake rejected / process could not be launched) | Check `RuntimeNote` and the kernel log, fix the configuration or the plugin; the kernel only retries after a restart |
| `down` | **It came up before but is gone now** (process crashed / connection dropped) | Go launch the plugin process; the kernel will recover automatically per the `restart` policy |
| `disabled` | `enabled=false` in the configuration, or hot-disabled by an operator | Recover with `plugins enable <name>` |

---

## 3. Development (Go)

### 3.1 Create the Project

The plugin is a **standalone Go module** that depends on only two public contract packages:

- `github.com/houzch/swiftmq/pkg/sidecar` — the wire protocol and the plugin-side implementation (**required**)
- `github.com/houzch/swiftmq/pkg/plugin` — only when you need types such as `plugin.Message` / `plugin.Error` (optional)

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

> When using `replace` for joint debugging, the plugin and the kernel must use **the same source tree**; otherwise the API version (`v1`) may match while the types differ.

### 3.2 Implement `Handler` (Three Methods)

The entire business surface of the plugin process is `sidecar.Handler`'s `Hello` / `Call` / `Open`.
Handshake, heartbeat, multiplexing, and chunking are all handled by `pkg/sidecar`; you never need to touch frames.

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

### 3.3 Data Plane: Reading and Writing a `Stream`

`sidecar.Stream` implements `io.ReadWriteCloser`, so just treat it as "a connection":

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

Key points:

- Each client connection = one stream; the plugin can start sending/receiving immediately inside `Handler.Open`.
- Large messages are chunked automatically by the library (each frame ≤ 64 KiB), and **memory usage is independent of message size**.
- Backpressure: the receive buffer of a single stream has an upper bound; when the buffer is full, the connection's **dispatch goroutine** is blocked (all streams wait together) —
  this is the trade-off between "predictable memory" and "per-stream rate limiting"; see §7 for details.

### 3.4 Using the Kernel Semantic Bridge (`session.*`)

If you want the plugin to reuse the kernel's queue / routing / permission / acknowledgement semantics (rather than building its own), use **reverse calls**.
On a stream, use them in the order `session.open` → other `session.*` → (`session.close`):

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

**Reverse-call methods at a glance** (constants in `pkg/sidecar/bridge.go` → wire names):

| Group | Wire name (constant) | Description |
| --- | --- | --- |
| Auth | `core.authenticate`（`MethodCoreAuthenticate`） | **Must be done first**: hand the credentials from your protocol to the kernel for verification; params `{stream, mechanism, response}`, returns `{user}` |
| Session | `session.open`（`MethodSessionOpen`） | Open a session for a vhost on the stream; can only be done **after authentication** |
| | `session.close`（`MethodSessionClose`） | Release the session on the stream (cancel consumers, delete exclusive queues) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Create / delete (passively declaring a missing one → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Exchange-to-exchange bindings |
| Queue | `session.declare_queue` / `session.delete_queue` | Create / delete; the server generates one when `name` is empty |
| | `session.bind_queue` / `session.unbind_queue` | Queue-to-exchange bindings |
| | `session.purge_queue` | Purge ready messages (excluding unacknowledged) |
| Publish | `session.publish` | Returns `{routed, rejected}`; persistence completes before the reply |
| Get | `session.get` | Actively fetch one message; `found=false` means the queue is empty |
| Consume | `session.consume` / `session.cancel` | Register / cancel consumers |
| Settle | `session.settle` | Settle a delivery (`ack` / `requeue` / `reject`) |
| **Forward** | `session.deliver` | **Kernel → plugin**: push a delivery back (handled in your `Handler.Call`) |

**Four rules you must follow**:

1. **`core.authenticate` first**: the connection's kernel operation surface has no identity before authentication,
   so `session.open` is rejected at that point (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   The plugin is responsible for extracting credentials from its own protocol; the authentication logic and the user table stay in the kernel, and the plugin never touches the password store.
2. **Then `session.open`**: calling other methods without an open session makes the kernel return `KindPreconditionFailed` ("stream N has not opened a session").
3. **Settle each delivery exactly once**: choose one of `Ack` / `Requeue` / `Reject`.
   Both `Ack` and `Reject` discard the message, but **only `Reject` goes to the dead-letter path**.
4. **Unsettled deliveries are not lost**: when a stream ends (client disconnects / `Handler.Open` returns) or the plugin connection drops,
   the kernel handles all unsettled deliveries **as "requeue"**, preventing messages from lingering.

**Error restoration**: the kernel's `*plugin.Error` arrives through the bridge as a `*sidecar.RPCError` (fields `Kind` / `Text`),
and can be restored into a categorized `plugin.Error` instead of losing the category in a string:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Mapping of common `Kind` values to built-in protocols (to help you decide how to return errors to clients):

| `plugin.ErrorKind` | Semantics | AMQP 0-9-1 mapping |
| --- | --- | --- |
| `KindNotFound` | Object does not exist | 404 NOT_FOUND (close Channel) |
| `KindPreconditionFailed` | Parameters inconsistent with an existing object / session not open | 406 PRECONDITION_FAILED (close Channel) |
| `KindAccessRefused` | Insufficient permission / reserved name | 403 ACCESS_REFUSED (close Channel) |
| `KindResourceLocked` | Exclusive resource occupied | 405 RESOURCE_LOCKED (close Channel) |
| `KindInvalidPath` | vhost does not exist | 402 INVALID_PATH (close connection) |
| `KindNotImplemented` | Capability not implemented | 540 NOT_IMPLEMENTED (close connection) |
| `KindInternal` | Internal kernel error | 541 INTERNAL_ERROR (close connection) |

**Boundary of message type fidelity**: the property table (`MessageDTO.Properties.Headers`) is relayed through JSON,
so **numeric type information distinguishing `int32` / `double` as in an AMQP field-table is not available**.
When strict type fidelity is required, carry such information yourself in the message body (raw bytes).

### 3.5 Plugin-Side State and Logging

- An external plugin's **state is determined by whether the connection is alive**, and the plugin does not report it itself (the built-in plugin's `StateReporter` does not apply to external processes).
- Logging: `sidecar.ServerOptions.Logger` writes to the plugin process's stdout/stderr;
  **when launched by the kernel via `spawn`, these outputs are forwarded by the kernel into the kernel log** (tagged with `plugin`), which makes centralized collection easier.
- In a standalone deployment (not spawn), collect the plugin logs your own way.

### 3.6 Self-Testing Without the Kernel

`Handler` is an ordinary Go interface; in a unit test you can instantiate it directly and call `Hello` / `Call` / `Open` to cover the business logic, without starting any network:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

For end-to-end verification, see §5.

---

## 4. Developing in Other Languages (Wire Protocol Spec)

`pkg/sidecar` is a zero-dependency public contract; the wire protocol itself is simple and can be implemented in any language.
To integrate, you need to implement the following "byte-level" conventions (see `pkg/sidecar/frame.go`, `proto.go` for the source).

> **Per-language guides with complete example projects are provided** (the examples all passed empirically: handshake → auth → semantic bridge → delivery/settle → byte stream):
>
> | Language | Guide | Example project (in workspace `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (standard library only) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (standard library only) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (standard library only) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (single file, JDK only) |
>
> For the complete Go reference implementation, see the standalone test project `swiftmq-test/test/integration/echosidecar/` (it uses `pkg/sidecar.Server` directly,
> so you don't need to care about the byte-level details below).

**Frame format** (uniform for all frames):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Frame types `kind`**:

| kind | Name | Direction | Payload |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | empty |
| 4 | Pong | plugin → kernel | empty |
| 5 | Call | bidirectional | JSON `Call` |
| 6 | Reply | bidirectional | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | bidirectional | `u32 BE stream` + raw bytes |
| 10 | Close | bidirectional | JSON `Close` |

**Control-plane JSON structures** (field names match `proto.go`).

> This section is a **wire-protocol message example** (multiple messages are given in order in one block, hence the `//` separators for explanation),
> and **it is not configuration that can be written directly into `swiftmqd.json`**.

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

**Semantics you must follow**:

- **Handshake**: the kernel sends `Hello` first, and the plugin must reply with a `HelloAck` frame.
  The kernel validates `HelloAck.name == the plugin name in config` and `HelloAck.api_version == the kernel's APIVersion`;
  a non-empty `deny` is treated as refusing to connect (the plugin gets isolated).
- **Heartbeat**: by default the kernel sends `Ping` every 2s, and the plugin must reply `Pong` within 8s; on the plugin side, if no frame is received within 24s it may close the connection itself.
- **Two ID spaces**: `Call`'s `reverse` distinguishes direction, and each direction increments from 1 independently,
  so `Reply` **must carry `reverse` back**, otherwise the reply will be delivered to the wrong waiter.
- **No base64 on the data plane**: large data such as message bodies goes directly into the `Data` frame payload (`stream` + raw bytes), chunked as needed.

> If you use Go, just use `pkg/sidecar` directly and you won't have to implement any of the details above.

---

## 5. Running It Together: Integration, Serving Clients, Packaging ★

This section answers "how to integrate into SwiftMQ and how to serve clients once development is done".

### 5.1 Declaring the Plugin in Configuration

An external plugin is **fully managed by configuration**, and no code changes to the kernel are needed for it. Add an entry to the `plugins` section of `swiftmqd.json`
(**the actual configuration is standard JSON and cannot contain comments**):

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

Item-by-item explanation (see the table below for the field list):

- The key name of `plugins.<plugin-name>` **must match the plugin's self-reported `HelloAck.name`**, otherwise the handshake is rejected.
- `builtin: false`: explicitly declares an external plugin (if omitted, the management plane displays it as built-in).
- `enabled`: turning it off = no process launched, no listener created.
- With `required: true`, a startup failure blocks the kernel startup — do not enable it for external plugins.
- `address` is **the address the kernel connects to** (the kernel is the client); when `spawn` is non-empty, the kernel launches the process on your behalf.
- `protocols[].prefix` **must be non-empty** (see §5.3 for the sniffing rules).
- `listeners` are the protocol's **external ports, opened by the kernel** (clients connect to the kernel).

Field list:

| Field | Required | Description |
| --- | --- | --- |
| `sidecar.address` | ✅ | Plugin process address: `tcp://host:port` or `unix:///path` |
| `sidecar.spawn` | ✕ | Command line the kernel launches on your behalf (first element is the executable); **empty = the kernel only connects and does not launch**, and you manage the process yourself |
| `sidecar.restart` | ✕ | `always` (default, auto-recover after a disconnect/crash) or `never` (only mark `down` and wait for operator intervention) |
| `sidecar.protocols[].name` | ✅ | Protocol name (globally unique, participates in sniffing priority) |
| `sidecar.protocols[].prefix` | ✕ | Sniffing prefix (ASCII); **empty = does not participate in sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | External listener for this protocol (`name` + `addr`), created by the kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | Override the handshake timeout (default 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Override the heartbeat interval (default 2s) |

> **Plugin name vs. protocol name**: the two **may differ** (for example, plugin `my-sidecar` provides protocol `myproto`).
> Hot-disable first finds all protocols registered by the plugin name and then closes those protocols' ports, so there is no need to deliberately use the same name.

### 5.2 Three Integration Modes

| Mode | Configuration | When to use |
| --- | --- | --- |
| **Same host + kernel launches (spawn)** | `spawn: [...]`, `address` points to the address it listens on | Same-machine deployment, single container; the least effort, the kernel handles launching and reclamation |
| **Same host + self-managed (dial)** | `spawn: []`, `address` points to an already-running process | Manage the plugin lifecycle with systemd / supervisor |
| **Cross-host / cross-container (dial, must be tcp)** | `spawn: []`, `address: "tcp://<service-name>:19001"` | Plugin and kernel deployed in separate containers / machines |

Address choice:

- **On the same machine, a unix socket is recommended** (`unix:///tmp/my-sidecar.sock`): it does not occupy a TCP port and is unaffected by host port occupation.
  Note that the socket path must be writable by the kernel process (in a container, the non-root `swiftmq` user).
- **Cross-container requires TCP**, and the plugin process must listen on `0.0.0.0`, with `address` using **the service name in the container network**.

> Don't get the direction backwards: **the address the plugin listens on** = `address`; **the port exposed to clients** = `protocols[].listeners`.

### 5.3 Serving Clients: Recognized by `prefix`

When the access layer dispatches connections, it **only looks at the sniffing result**: for each enabled protocol it asks `Sniff(peek)` in registration order (peek is at most 8 bytes),
and the one that matches takes over the connection. Therefore:

1. **`prefix` must be non-empty** (ASCII, ≤ 8 bytes). Only when the first few bytes sent by the client match it will the connection be handed to your plugin.
   Example: `"prefix": "PY"` → the client's first bytes must be `PY` (you can treat the prefix as the magic header of your protocol).
2. **An empty `prefix` means it does not participate in sniffing**: such connections will **not** be handed to the plugin (empirically: connections on the listening port are dropped immediately).
   Therefore an empty `prefix` is only suitable for the scenario "another protocol will forward for you on the same port"; **do not** use it to build a dedicated port.
3. `listeners[].addr` decides "on which port to serve externally", and `prefix` decides "whether this connection is yours" —
   the two must be used together: **a dedicated port also needs a non-empty `prefix`** (this is also why in the kernel's example config
   `echo-sidecar` writes both `prefix: "ECHO"` and `listeners: [":1885"]`).
4. Sniffing matches in protocol registration order, and **the first match wins**: when multiple plugins coexist, prefixes must be distinguishable (for example, all starting with the same byte will shadow each other).

### 5.4 Overriding Listener Addresses and TLS

- The external listener address can be given in **two places**: `sidecar.protocols[].listeners[].addr` (default) and
  `listeners.<protocol-name>` (overrides wholesale by protocol name). When both exist, `listeners.<protocol-name>` takes precedence.
- When TLS is needed, provide the certificate in `listeners.<protocol-name>[i].tls` (fields identical to built-in protocols).
  Below is a `listeners` snippet (**standard JSON, cannot contain comments**): item 1 is plaintext, item 2 uses TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS is terminated by **the kernel** on the listener side; the plugin process receives a plaintext stream, so the plugin does not need to handle TLS.

### 5.5 Packaging: Making the Plugin Run Alongside the Kernel

**Option A — Bake it into the same image** (recommended for plugins "released with the kernel"): add one line to the runtime stage of `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Then in the configuration set `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
with `address` set to the same value. The kernel launches it at startup.

**Option B — Mount the binary** (no image changes, good for joint debugging):

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

The configuration uses a unix socket (to avoid occupying an extra port). Below is the `sidecar` snippet inside `plugins.my-sidecar`
(**standard JSON, cannot contain comments**; `prefix` must still be non-empty, see §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Option C — Separate container** (plugin released separately / scaled independently):

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

In the configuration, `spawn: []` (the kernel only connects, does not launch) and `address: "tcp://my-sidecar:19001"` (the compose service name).

### 5.6 Startup and Verification

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

In `plugins show`, pay special attention to `state` and `RuntimeNote`:
`failed` comes with the failure reason (handshake rejected / port could not be opened…); `down` comes with the disconnect reason (process crashed / connection dropped).

### 5.7 Runtime Operations

| Operation | Command / API | Effect |
| --- | --- | --- |
| Hot-disable | `swiftmqctl plugins disable my-sidecar` or `PUT /api/plugins/my-sidecar/disable` | **Closes the plugin's external listeners** (capability-level disable); the kernel and other plugins are unaffected |
| Hot-enable | `swiftmqctl plugins enable my-sidecar` | Reopens its listeners; if a previous startup failed, it retries once |
| View state | `swiftmqctl plugins list/show` | State + failure/disconnect reason |
| Kernel exit | — | Disconnects from the plugin, reclaims bridged sessions, **terminates the child processes launched by the kernel via `spawn`** |

> Hot-disable only closes the "capability" (listening ports) and **does not** kill the plugin process launched via `spawn`; process reclamation happens when the kernel exits.

### 5.8 Providing an Entry in the Admin Console (Optional)

When the plugin ships with its own UI, add a `console_url` (the admin UI address; see §5.1 for the other fields) to the `plugins.<plugin-name>` section.
Below only the `plugins.my-sidecar` entry is shown (**standard JSON, cannot contain comments**; the `sidecar` section content is the same as §5.1):

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

- The admin console's **Plugin Management** page (data comes from the `console_url` field of `GET /api/plugins`) displays an
  "Open Admin UI" button for such plugins, which **opens in a new tab**.
- When `console_url` is not declared, the button is unavailable and a tooltip says "This plugin does not provide an admin UI".
- It is merely **metadata written into the configuration by the deployer**: it is not part of the plugin API (`pkg/plugin`), does not participate in plugin start-stop,
  and the UI itself is hosted by the plugin (it can live in the plugin process or in any standalone service).

---

## 6. Lifecycle and Fault-Tolerance Matrix

| Scenario | Kernel behavior | Impact on the plugin side |
| --- | --- | --- |
| Plugin process not started / handshake rejected | Retries the connection within 8s; if it still fails, marks `failed` and isolates it (does not block kernel startup) | None |
| A process launched by `spawn` exits | Logs it; marks `down`; backs off and reconnects / relaunches per the `restart` policy | The new process performs the handshake again |
| Plugin process crashes (at runtime) | The kernel is unaffected; `down` + backoff reconnect | `pkg/sidecar.Server` closes that connection |
| Kernel is `kill -9`ed | — | The plugin side reclaims the connection itself via idle timeout (no frames for 24s by default), leaving no zombies |
| Kernel exits normally | Calls `Stop`: disconnects, reclaims unsettled deliveries (as requeue), `Kill`s child processes | Receives SIGKILL |
| Deliveries while the plugin connection is down | Unsettled deliveries are always **requeued** and never lost | — |
| Client disconnects / `Open` returns | Closes the corresponding stream and releases that stream's session and consumers | `Stream.Read` returns EOF |

---

## 7. Red Lines and Known Boundaries

**Red lines**

1. A plugin may only depend on `pkg/sidecar` (and optionally `pkg/plugin`); it **must not** depend on the kernel's `internal/**`.
2. The plugin name must match the configuration and `APIVersion` must match the kernel, otherwise it cannot connect (this prevents "silently running but not taking effect").
3. When using `session.*`: **`core.authenticate` first, then `session.open`**, and settle each delivery **exactly once**.
4. `protocols[].prefix` must be non-empty, otherwise connections will not be handed to the plugin (see §5.3).
5. A rejection in `Hello` must **explicitly return an error** (do not stay silent) — otherwise the kernel only sees "connection closed" and cannot locate the cause.

**Known boundaries**

- **Sniffing is on the kernel side**: external plugins cannot define custom sniff functions and can only match by `prefix` (ASCII, ≤ 8 bytes);
  an empty `prefix` means "no connection can be obtained" (see §5.3).
- **The data plane goes through local proxying**: there is no fd passing (Windows has no `SCM_RIGHTS`), adding one extra memory copy compared with in-process;
  each reverse call also adds one extra local RPC.
- **Property table types degrade**: `Properties.Headers` is relayed through JSON, so distinctions such as `int32` / `double` are lost (see §3.4).
- **Single-stream backpressure affects the whole connection**: when one stream's receive buffer is full, it blocks the connection's dispatch goroutine; per-stream rate limiting is a future optimization.
- **Auth failures pass through text only**: a kernel auth failure is a `*plugin.AuthError` (a different classification from `plugin.ErrorKind`),
  and only the text reaches the plugin through the bridge; the plugin must map it to protocol error codes per its own convention.
- **Only the `net.listen` capability truly takes effect**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` are **reserved slots**; declaring them only participates in auditing (see the governance display in §5.6), and there is currently no corresponding extension point.

---

## 8. Troubleshooting FAQ

| Symptom | Cause and handling |
| --- | --- |
| State `failed`, reason contains "plugin name mismatch" | The configured plugin name ≠ `HelloAck.name`; make them match |
| State `failed`, reason contains "API version mismatch" | `HelloAck.api_version` ≠ the kernel's `APIVersion`; make them match |
| State `failed`, reason contains "handshake rejected" | The plugin's `Hello` returned an error (`deny`); check the plugin output forwarded in the kernel log |
| State `failed`, reason contains "failed to connect to external plugin" | The process did not come up / `address` is wrong / the socket path is not writable (mind the `swiftmq` user permissions inside containers) |
| State `down` | The plugin process crashed or the connection dropped; `restart=always` reconnects automatically, while `never` requires manual relaunch |
| Port not open / client cannot connect | `protocols[].listeners` is not configured or its address is overridden by `listeners.<protocol-name>`; check both places |
| Client connects to another port and is immediately disconnected | That port does not match your protocol (`prefix` is empty or the prefix does not match); configure a non-empty `prefix` for the protocol (see §5.3) |
| Error `ACCESS_REFUSED - ... for user ''` | There was **no authentication** before the semantic bridge; call `core.authenticate` before `session.open` |
| Reverse call reports "stream N has not opened a session" | Do `core.authenticate` first, then `session.open`, and only then call other `session.*` |
| No consumer deliveries received | Deliveries arrive at your `Call` as a **forward call** `session.deliver`; confirm that method is handled |
| Plugin outside the container, kernel inside, cannot connect | Use `address: tcp://host.docker.internal:<port>` (or put the plugin in the container too and use the service name); the plugin must listen on `0.0.0.0` |

---

## 9. Reference (Source Index)

| What you want to see | File |
| --- | --- |
| Wire protocol and both-side implementations (**required reading for development**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frames), `proto.go` (messages), `server.go` (plugin side), `client.go` (kernel side), `bridge.go` (`session.*` contract), `stream.go` (streams) |
| Kernel-side sidecar host (integration/reconnect/proxy/state) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Kernel-side semantic bridge (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Kernel session operation-surface types (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Listening, sniffing, per-plugin hot start-stop | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Plugin lifecycle and governance (isolation/state/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Configuration items and examples (including the sidecar section) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Process wiring (how the sidecar is wired into the kernel) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Go reference implementation (uses `pkg/sidecar.Server`, with the `session.*` bridge and `core.authenticate`) | Standalone test project `swiftmq-test/test/integration/echosidecar/` |
| **Per-language guides + example projects** | This directory's `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; examples are in the **workspace** `swiftmq-plugin/{python,nodejs,php,java}/` |
