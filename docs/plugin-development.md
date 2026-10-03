# SwiftMQ 外部进程插件（sidecar）开发指南

> **面向**：不想 fork / 重编内核，希望用**任意语言**给 SwiftMQ 扩展能力的开发者。
> **范围**：本文只讲一种插件形态 —— **外部进程插件**（内核术语 `sidecar`）。内核内置协议插件（AMQP 0-9-1 / MQTT）不在本文范围。
> **读法**：第 1–2 节建立心智模型，第 3 节写代码，**第 5 节是"开发完怎么接进来一起跑、对外提供服务"**；
> **用其它语言（Python / Node.js / PHP / Java）请看 §4 的分语言指南**（各带一个实测跑通的完整示例工程）。
> 文中代码是最小可运行骨架，可直接复制为起点。简体中文为源语言。

---

## 1. 它是什么

一个**独立进程**，在自己的进程里实现某个"协议"（解析客户端字节流），
内核按配置把它托管起来：**端口由内核开、连接由内核代理**，注册 / 启停 / 审计 / 隔离全部复用内核既有机制。

先建立三条正确的心智模型（最容易搞错的地方）：

1. **插件进程是一个"本机服务"**：它只监听一个**本机地址**（TCP 或 unix socket），等**内核来连**。
   连接方向是 **内核（客户端）→ 插件（服务端）**，握手也由内核先发。
2. **对外业务端口不由插件开**：由**内核**按配置里的 `protocols[].listeners` 创建，并映射出来给客户端。
   客户端连的是**内核的端口**，字节被内核代理到插件进程。插件进程**不需要**自己开业务端口。
3. **语义可选**：插件可以只"搬运字节"（协议完全由你自己实现），
   也可以经**反向调用** `session.*` 触达内核语义（队列 / 路由 / 权限 / 确认），
   与内置协议插件**完全同一套语义**（vhost、权限、路由、队列行为因此不会分叉）。

| 好处 | 代价 |
| --- | --- |
| 不改内核、不重编内核即可扩展 | 数据面多一次本机字节拷贝（内核代理，无 fd 传递，跨平台一致） |
| 任意语言实现（只需要能实现线协议） | 反向调用每次多一次本机 RPC（JSON 编解码 + 拷贝） |
| 插件可独立发布 / 升级 / 重启 | 嗅探留在内核侧：只能按"前缀"或"专属端口"被识别 |
| 崩溃只影响该插件：内核标记 `down`，不退不崩 | 只有 `net.listen` 能力真正生效，其它能力值为预留（见 §7） |

---

## 2. 工作原理

连接建立（**内核是客户端，插件是服务端**）：

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

握手：

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

开始服务：

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**生命周期**（内核侧 `internal/plugin/sidecar` 宿主）：

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**状态语义**（`swiftmqctl plugins show` 可见）：

| 状态 | 含义 | 运维动作 |
| --- | --- | --- |
| `enabled` | 已接入并服务中 | — |
| `failed` | **启动就没起来**（配置错误 / 握手被拒 / 进程拉不起来） | 看 `RuntimeNote` 与内核日志，改配置或修插件；重启内核才会重试 |
| `down` | **起来过、现在不在**（进程崩了 / 连接断了） | 去拉起插件进程；内核会按 `restart` 策略自动恢复 |
| `disabled` | 配置里 `enabled=false`，或运维热停用 | `plugins enable <名>` 恢复 |

---

## 3. 开发（Go）

### 3.1 建工程

插件是**独立 Go module**，只依赖两个对外契约包：

- `github.com/houzch/swiftmq/pkg/sidecar` —— 线协议与插件侧实现（**必需**）
- `github.com/houzch/swiftmq/pkg/plugin` —— 仅当你要用 `plugin.Message` / `plugin.Error` 等类型时（可选）

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/swiftmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/swiftmq@v1.1.01
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/swiftmq=../swiftmq
```

> 用 `replace` 联调时，插件与内核必须**同一份源码**，否则 API 版本（`v1`）虽一致、类型却可能不同。

### 3.2 实现 `Handler`（三个方法）

插件进程的全部业务面就是 `sidecar.Handler` 的 `Hello` / `Call` / `Open`。
握手、心跳、多路复用、分块都由 `pkg/sidecar` 处理，你不需要碰帧。

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

### 3.3 数据面：读写 `Stream`

`sidecar.Stream` 实现 `io.ReadWriteCloser`，直接当成"一条连接"用即可：

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

要点：

- 每条客户端连接 = 一条流；插件可以在 `Handler.Open` 里立即开始收发。
- 大消息由库自动分块（每帧 ≤ 64 KiB），**内存占用与消息大小无关**。
- 背压：单条流的接收缓冲有上限，缓冲满时阻塞该连接的**分发协程**（所有流一起等）——
  这是"内存可预测"与"单流限速"的取舍，详见 §7。

### 3.4 用内核语义桥（`session.*`）

想让插件复用内核的队列 / 路由 / 权限 / 确认语义（而不是自己造一套），就用**反向调用**。
在流上按 `session.open` → 其他 `session.*` → （`session.close`）的顺序使用：

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

**反向调用方法一览**（`pkg/sidecar/bridge.go` 的常量 → 线名字）：

| 分组 | 线名字（常量） | 说明 |
| --- | --- | --- |
| 认证 | `core.authenticate`（`MethodCoreAuthenticate`） | **必须先做**：把协议里的凭据交给内核校验；参数 `{stream, mechanism, response}`，返回 `{user}` |
| 会话 | `session.open`（`MethodSessionOpen`） | 在流上打开某 vhost 的会话；**认证之后**才能做 |
| | `session.close`（`MethodSessionClose`） | 释放该流上的会话（取消消费者、删独占队列） |
| 交换机 | `session.declare_exchange` / `session.delete_exchange` | 增删（被动声明不存在 → `KindNotFound`） |
| | `session.bind_exchange` / `session.unbind_exchange` | 交换机到交换机的绑定 |
| 队列 | `session.declare_queue` / `session.delete_queue` | 增删；`name` 为空时服务端生成 |
| | `session.bind_queue` / `session.unbind_queue` | 队列到交换机的绑定 |
| | `session.purge_queue` | 清空就绪消息（不含未确认） |
| 发布 | `session.publish` | 返回 `{routed, rejected}`；持久化在应答前完成 |
| 拉取 | `session.get` | 主动拉一条；`found=false` 表示队列为空 |
| 消费 | `session.consume` / `session.cancel` | 注册 / 取消消费者 |
| 结算 | `session.settle` | 结算一条投递（`ack` / `requeue` / `reject`） |
| **正向** | `session.deliver` | **内核 → 插件**：投递回推（在你的 `Handler.Call` 里处理） |

**必须遵守的四条约定**：

1. **先 `core.authenticate`**：连接的内核操作面在认证前没有身份，
   此时 `session.open` 会被拒（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。
   插件负责从自己的协议里取凭据；认证逻辑与用户表仍在内核，插件不接触口令库。
2. **再 `session.open`**：未开会话就调其他方法，内核返回 `KindPreconditionFailed`（"流 N 尚未打开会话"）。
3. **每条投递恰好结算一次**：`Ack` / `Requeue` / `Reject` 三选一。
   `Ack` 与 `Reject` 都丢弃消息，**只有 `Reject` 走死信**。
4. **未结算的投递不会丢**：流结束（客户端断开 / `Handler.Open` 返回）或插件连接断开时，
   内核把所有未结算投递**按"回队"处理**，避免消息滞留。

**错误还原**：内核的 `*plugin.Error` 经桥以 `*sidecar.RPCError`（字段 `Kind` / `Text`）到达，
可还原成带分类的 `plugin.Error`，而不是把分类丢在字符串里：

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

常见 `Kind` 与内置协议的映射（供你决定怎么回错给客户端）：

| `plugin.ErrorKind` | 语义 | AMQP 0-9-1 映射 |
| --- | --- | --- |
| `KindNotFound` | 对象不存在 | 404 NOT_FOUND（关 Channel） |
| `KindPreconditionFailed` | 参数与已存在对象不一致 / 会话未打开 | 406 PRECONDITION_FAILED（关 Channel） |
| `KindAccessRefused` | 权限不足 / 保留名 | 403 ACCESS_REFUSED（关 Channel） |
| `KindResourceLocked` | 独占资源被占用 | 405 RESOURCE_LOCKED（关 Channel） |
| `KindInvalidPath` | vhost 不存在 | 402 INVALID_PATH（关连接） |
| `KindNotImplemented` | 能力未实现 | 540 NOT_IMPLEMENTED（关连接） |
| `KindInternal` | 内核内部错误 | 541 INTERNAL_ERROR（关连接） |

**消息类型保真的边界**：属性表（`MessageDTO.Properties.Headers`）经 JSON 中转，
像 AMQP field-table 那样**区分 `int32` / `double` 的数值类型信息拿不到**。
需要严格类型保真时，把这类信息放进消息体（原始字节）自行承载。

### 3.5 插件侧状态与日志

- 外部插件的**状态由"连接是否存活"决定**，插件无需自报（内置插件的 `StateReporter` 不适用于外部进程）。
- 日志：`sidecar.ServerOptions.Logger` 输出到插件进程的 stdout/stderr；
  **由内核 `spawn` 拉起时，这些输出会被内核转发进内核日志**（带 `plugin` 标签），便于统一采集。
- 独立部署（非 spawn）时，插件日志按你自己的方式采集。

### 3.6 不接内核也能自测

`Handler` 是普通 Go 接口，单测里直接实例化、调用 `Hello` / `Call` / `Open` 即可覆盖业务逻辑，无需起网络：

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

端到端验证走 §5。

---

## 4. 用其它语言开发（线协议规格）

`pkg/sidecar` 是零依赖的对外契约；线协议本身很简单，任何语言都能实现。
要对接，你需要实现下面这些"字节级"约定（源码见 `pkg/sidecar/frame.go`、`proto.go`）。

> **已提供带完整示例工程的分语言指南**（示例都实测跑通过握手 → 认证 → 语义桥 → 投递/结算 → 字节流）：
>
> | 语言 | 指南 | 示例工程（工作区 `swiftmq-plugin/`） |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py`（仅标准库） |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js`（仅标准库） |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php`（仅标准库） |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java`（单文件，仅 JDK） |
>
> Go 的完整参考实现见独立测试工程 `swiftmq-test/test/integration/echosidecar/`（它直接用 `pkg/sidecar.Server`，
> 无需关心下面的字节层细节）。

**帧格式**（所有帧统一）：

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**帧类型 `kind`**：

| kind | 名称 | 方向 | 载荷 |
| --- | --- | --- | --- |
| 1 | Hello | 内核 → 插件 | JSON `Hello` |
| 2 | HelloAck | 插件 → 内核 | JSON `HelloAck` |
| 3 | Ping | 内核 → 插件 | 空 |
| 4 | Pong | 插件 → 内核 | 空 |
| 5 | Call | 双向 | JSON `Call` |
| 6 | Reply | 双向 | JSON `Reply` |
| 7 | Open | 内核 → 插件 | JSON `Open` |
| 8 | OpenAck | 插件 → 内核 | JSON `OpenAck` |
| 9 | Data | 双向 | `u32 BE stream` + 原始字节 |
| 10 | Close | 双向 | JSON `Close` |

**控制面 JSON 结构**（字段名与 `proto.go` 一致）：

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

**必须遵守的语义**：

- **握手**：内核先发 `Hello`，插件必须回一帧 `HelloAck`。
  内核会校验 `HelloAck.name == 配置里的插件名` 且 `HelloAck.api_version == 内核的 APIVersion`；
  `deny` 非空视为拒绝接入（插件被隔离）。
- **心跳**：内核默认每 2s 发 `Ping`，插件须在 8s 内回 `Pong`；插件侧若 24s 内没有收到任何帧可自行关闭连接。
- **两个 ID 空间**：`Call` 的 `reverse` 区分方向，两个方向各自从 1 自增，
  因此 `Reply` **必须回带 `reverse`**，否则应答会被投给错误的等待者。
- **数据面不 base64**：消息体等大块数据直接放 `Data` 帧载荷（`stream` + 原始字节），按需分块。

> 若用 Go，直接用 `pkg/sidecar`，上述细节都不用自己实现。

---

## 5. 放进来一起运行：接入、对外服务、打包 ★

这一节回答"开发完怎么接进 SwiftMQ、怎么对外提供服务"。

### 5.1 在配置里声明插件

外部插件**完全由配置托管**，内核不需要为它改任何代码。在 `swiftmqd.json` 的 `plugins` 段加一项：

```jsonc
{
  "listeners": {
    // 可选：按“协议名”覆盖对外监听地址（见 §5.4）
    "myproto": [{ "addr": ":19002" }]
  },
  "plugins": {
    "my-sidecar": {                     // ← 插件名：必须与插件自报的 HelloAck.name 一致
      "builtin": false,                 // 外部插件：显式声明 false（否则管理面会当内置展示）
      "enabled": true,                  // 关掉它 = 不拉进程、不建监听
      "required": false,                // true 则启动失败会阻塞内核启动（外部插件不要开）
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",  // 内核去连的地址（内核是客户端）
        "spawn": ["/usr/local/bin/my-sidecar", "-addr", "tcp://127.0.0.1:19001"],
        "restart": "always",
        "protocols": [
          {
            "name": "myproto",          // 协议名：参与嗅探优先级；也是 listeners 覆盖的键
            "prefix": "",               // 空 = 不参与嗅探，只在专属端口服务
            "listeners": [{ "name": "myproto", "addr": ":19002" }]  // 对外端口，由内核打开
          }
        ]
      }
    }
  }
}
```

字段一览：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `sidecar.address` | ✅ | 插件进程地址：`tcp://host:port` 或 `unix:///path` |
| `sidecar.spawn` | ✕ | 内核代为启动的命令行（首个元素是可执行文件）；**留空 = 内核只连不拉**，进程由你自行管理 |
| `sidecar.restart` | ✕ | `always`（默认，断线/崩溃后自动恢复）或 `never`（只标记 `down`，等运维介入） |
| `sidecar.protocols[].name` | ✅ | 协议名（全局唯一，参与嗅探优先级） |
| `sidecar.protocols[].prefix` | ✕ | 嗅探前缀（ASCII）；**空 = 不参与嗅探** |
| `sidecar.protocols[].listeners[]` | ✕ | 该协议的对外监听（`name` + `addr`），由内核创建 |
| `sidecar.handshake_timeout_seconds` | ✕ | 覆盖握手超时（默认 5s） |
| `sidecar.heartbeat_seconds` | ✕ | 覆盖心跳间隔（默认 2s） |

> **插件名与协议名**：二者**可以不同**（例如插件 `my-sidecar` 提供协议 `myproto`）。
> 热停用会先按插件名找到它注册的全部协议、再关闭这些协议的端口，因此不需要刻意取同名。

### 5.2 三种接法

| 接法 | 配置 | 适用 |
| --- | --- | --- |
| **同主机 + 内核代拉（spawn）** | `spawn: [...]`，`address` 指向它监听的地址 | 同机部署、单容器；最省事，内核负责拉起与回收 |
| **同主机 + 自行管理（dial）** | `spawn: []`，`address` 指向已运行的进程 | 用 systemd / supervisor 管理插件生命周期 |
| **跨主机 / 跨容器（dial，必须 tcp）** | `spawn: []`，`address: "tcp://<服务名>:19001"` | 插件与内核分容器 / 分机器部署 |

地址选择：

- **同机推荐 unix socket**（`unix:///tmp/my-sidecar.sock`）：不占 TCP 端口、不受宿主端口占用影响。
  注意 socket 路径要对内核进程（容器里是非 root 的 `swiftmq` 用户）可写。
- **跨容器必须 TCP**，且插件进程要监听 `0.0.0.0`，`address` 用**容器网络里的服务名**。

> 方向别搞反：**插件监听的地址** = `address`；**对外开放给客户端的端口** = `protocols[].listeners`。

### 5.3 对外提供服务：靠 `prefix` 被识别

接入层分发连接时**只看嗅探结果**：它对每个已启用协议按注册顺序问 `Sniff(peek)`（peek 最多 8 字节），
命中者接管这条连接。因此：

1. **`prefix` 必须非空**（ASCII，≤ 8 字节）。客户端发来的前几个字节等于它，连接才会交给你的插件。
   例：`"prefix": "PY"` → 客户端首字节须是 `PY`（可以把前缀当作你协议的魔法头）。
2. **`prefix` 为空表示不参与嗅探**：这类连接**不会**被交给插件（实测：监听端口上的连接会被立刻断开）。
   因此空 `prefix` 只适合"另有协议会在同端口上帮你转发"的场景，**不要**用它来做专属端口。
3. `listeners[].addr` 决定"在哪个端口上对外开放"，`prefix` 决定"这条连接算不算你的"——
   两者要配套使用：**专属端口也要给一个非空 `prefix`**（这也是内核示例配置里
   `echo-sidecar` 同时写 `prefix: "ECHO"` 与 `listeners: [":1885"]` 的原因）。
4. 嗅探按协议注册顺序匹配，**先匹配者生效**：多个插件共存时，前缀要有区分度（例如都以同一字节开头会互相遮挡）。

### 5.4 覆盖监听地址与 TLS

- 对外监听地址可在**两处**给：`sidecar.protocols[].listeners[].addr`（默认）与
  `listeners.<协议名>`（按协议名整体覆盖）。二者同时存在时以 `listeners.<协议名>` 为准。
- 需要 TLS 时，在 `listeners.<协议名>[i].tls` 里给证书（字段与内置协议一致）：

```jsonc
"listeners": {
  "myproto": [
    { "addr": ":19002" },                                   // 明文
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS 由**内核**在监听侧终结，插件进程拿到的是明文流——插件不必处理 TLS。

### 5.5 打包：让插件与内核一起跑

**做法 A —— 打进同一个镜像**（推荐给"随内核发布"的插件）：在 `swiftmq/Dockerfile` 的运行阶段加一行：

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

然后配置里 `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`，
`address` 同值。内核启动时会拉起它。

**做法 B —— 挂载二进制**（不改镜像，适合联调）：

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.01
    command: ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

配置用 unix socket（避免额外占端口）：

```jsonc
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**做法 C —— 独立容器**（插件单独发布 / 独立扩缩容）：

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.01
    volumes: ["./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

配置里 `spawn: []`（内核只连不拉），`address: "tcp://my-sidecar:19001"`（compose 服务名）。

### 5.6 启动与验证

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

`plugins show` 里重点关注 `state` 与 `RuntimeNote`：
`failed` 会带失败原因（握手被拒 / 端口拉不起来…）；`down` 会带断开原因（进程崩了 / 连接断了）。

### 5.7 运行期运维

| 操作 | 命令 / 接口 | 效果 |
| --- | --- | --- |
| 热停用 | `swiftmqctl plugins disable my-sidecar` 或 `PUT /api/plugins/my-sidecar/disable` | **关闭该插件的对外监听**（能力级停用）；内核与其它插件不受影响 |
| 热启用 | `swiftmqctl plugins enable my-sidecar` | 重新打开它的监听；若之前启动失败会重试一次 |
| 看状态 | `swiftmqctl plugins list/show` | 状态 + 失败/断开原因 |
| 内核退出 | — | 断开与插件的连接、回收桥上会话、**终止由内核 `spawn` 的子进程** |

> 热停用只关"能力"（监听端口），**不会**杀掉用 `spawn` 拉起的插件进程；进程的回收发生在内核退出时。

### 5.8 在管理后台提供入口（可选）

插件自带操作界面时，在 `plugins.<插件名>` 段里加一个 `console_url`（管理界面地址，其余字段见 §5.1）：

```jsonc
"plugins": {
  "my-sidecar": {
    "builtin": false,
    "enabled": true,
    "console_url": "http://127.0.0.1:19003/",   // ← 管理后台「插件管理」页据此提供直达入口
    "sidecar": { /* 同 §5.1 */ }
  }
}
```

- 管理后台的**插件管理**页（数据来自 `GET /api/plugins` 的 `console_url` 字段）会为这类插件显示
  「打开管理界面」按钮，**在新标签页打开**。
- 未声明 `console_url` 时按钮不可用，悬浮提示"该插件未提供管理界面"。
- 它只是**部署方写在配置里的元数据**：不属于插件 API（`pkg/plugin`）、不参与插件启停，
  界面本身由插件自行托管（可以就在插件进程里，或任意独立服务）。

---

## 6. 生命周期与容错矩阵

| 情形 | 内核行为 | 插件侧影响 |
| --- | --- | --- |
| 插件进程未启动 / 握手被拒 | 8s 内重试连接，仍失败则标记 `failed` 并隔离（不阻塞内核启动） | 无 |
| `spawn` 拉起的进程退出 | 记录日志；标记 `down`；按 `restart` 策略退避重连 / 重拉 | 新进程重新握手 |
| 插件进程崩溃（运行期） | 内核不受影响；`down` + 退避重连 | `pkg/sidecar.Server` 会关闭该连接 |
| 内核被 `kill -9` | — | 插件侧靠空闲超时（默认 24s 无帧）自行回收连接，不留僵尸 |
| 内核正常退出 | 调用 `Stop`：断连接、回收未结算投递（按回队）、`Kill` 子进程 | 收到 SIGKILL |
| 插件连接断开期间的投递 | 未结算投递一律**回队**，不会丢 | — |
| 客户端断开 / `Open` 返回 | 关闭对应流，释放该流的会话与消费者 | `Stream.Read` 返回 EOF |

---

## 7. 红线与已知边界

**红线**

1. 插件只允许依赖 `pkg/sidecar`（以及可选的 `pkg/plugin`）；**不得**依赖内核 `internal/**`。
2. 插件名必须与配置一致、`APIVersion` 必须与内核一致，否则无法接入（这是防"静默跑着不生效"）。
3. 用 `session.*` 时：**先 `core.authenticate`、再 `session.open`**，每条投递**恰好结算一次**。
4. `protocols[].prefix` 必须非空，否则连接不会被交给插件（见 §5.3）。
5. `Hello` 里拒绝要**明确回 error**（不要静默）——否则内核只能看到"连接被关闭"，定位不到原因。

**已知边界**

- **嗅探在内核侧**：外部插件不能自定义嗅探函数，只能靠 `prefix`（ASCII，≤ 8 字节）匹配；
  `prefix` 为空即"拿不到连接"（见 §5.3）。
- **数据面走本机代理**：无 fd 传递（Windows 无 `SCM_RIGHTS`），比进程内多一次内存拷贝；
  反向调用每次也多一次本机 RPC。
- **属性表类型会退化**：`Properties.Headers` 经 JSON 中转，`int32` / `double` 之类区分丢失（见 §3.4）。
- **单流背压影响整条连接**：一条流的接收缓冲满时会阻塞该连接的分发协程；按流限速属后续优化。
- **认证失败只透传文本**：内核认证失败是 `*plugin.AuthError`（与 `plugin.ErrorKind` 不是同一套分类），
  经桥到达插件时只有文本，插件需按自己的约定映射成协议错误码。
- **只有 `net.listen` 能力真正生效**：`store.read/write`、`http.route`、`cluster.metadata.write`、
  `auth.verify` 是**预留位**，声明后仅参与审计（见 §5.6 的治理展示），当前没有对应扩展点。

---

## 8. 排错 FAQ

| 现象 | 原因与处理 |
| --- | --- |
| 状态 `failed`，原因含"插件名不一致" | 配置的插件名 ≠ `HelloAck.name`；改齐 |
| 状态 `failed`，原因含"API 版本不匹配" | `HelloAck.api_version` ≠ 内核 `APIVersion`；改齐 |
| 状态 `failed`，原因含"拒绝握手" | 插件 `Hello` 返回了 error（`deny`）；看内核日志里转发的插件输出 |
| 状态 `failed`，原因含"连接外部插件失败" | 进程没起来 / `address` 写错 / socket 路径不可写（容器里注意 `swiftmq` 用户权限） |
| 状态 `down` | 插件进程崩了或连接断了；`restart=always` 会自动重连，`never` 需人工拉起 |
| 端口没开 / 客户端连不上 | `protocols[].listeners` 没配或地址被 `listeners.<协议名>` 覆盖掉了；核对两处 |
| 客户端连上别的端口后立刻断开 | 该端口不匹配你的协议（`prefix` 为空或前缀不符）；给协议配一个非空 `prefix`（见 §5.3） |
| 报 `ACCESS_REFUSED - ... for user ''` | 语义桥前**没有认证**；先调 `core.authenticate` 再 `session.open` |
| 反向调用报"流 N 尚未打开会话" | 先 `core.authenticate`、再 `session.open`，然后才能调其他 `session.*` |
| 收不到消费投递 | 投递以**正向调用** `session.deliver` 到达你的 `Call`；确认已处理该方法 |
| 插件在容器外、内核在容器内，连不上 | `address` 用 `tcp://host.docker.internal:<port>`（或把插件也放进容器、用服务名）；插件需监听 `0.0.0.0` |

---

## 9. 参考（源码索引）

| 想看什么 | 文件 |
| --- | --- |
| 线协议与两端实现（**开发必读**） | [`pkg/sidecar/`](../pkg/sidecar/)：`frame.go`（帧）、`proto.go`（报文）、`server.go`（插件侧）、`client.go`（内核侧）、`bridge.go`（`session.*` 契约）、`stream.go`（流） |
| 内核侧 sidecar 宿主（接入/重连/代理/状态） | [`internal/plugin/sidecar/sidecar.go`](../internal/plugin/sidecar/sidecar.go) |
| 内核侧语义桥（`session.*` → `plugin.Session`） | [`internal/plugin/sidecar/bridge.go`](../internal/plugin/sidecar/bridge.go) |
| 内核会话操作面类型（`Message` / `Delivery` / `ErrorKind`） | [`pkg/plugin/session.go`](../pkg/plugin/session.go) |
| 监听、嗅探、按插件热启停 | [`internal/transport/server.go`](../internal/transport/server.go) |
| 插件生命周期与治理（隔离/状态/审计） | [`internal/plugin/manager.go`](../internal/plugin/manager.go)、[`registry.go`](../internal/plugin/registry.go) |
| 配置项与示例（含 sidecar 段） | [`internal/config/config.go`](../internal/config/config.go)、[`configs/swiftmqd.json`](../configs/swiftmqd.json) |
| 进程装配（sidecar 如何被装配进内核） | [`cmd/swiftmqd/main.go`](../cmd/swiftmqd/main.go) |
| Go 参考实现（用 `pkg/sidecar.Server`，含 `session.*` 桥与 `core.authenticate`） | 独立测试工程 `swiftmq-test/test/integration/echosidecar/` |
| **分语言指南 + 示例工程** | 本目录 `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`；示例在**工作区** `swiftmq-plugin/{python,nodejs,php,java}/` |
