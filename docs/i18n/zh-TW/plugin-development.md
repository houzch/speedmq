# SwiftMQ 外部行程外掛（sidecar）開發指南

> **適用對象**：不想 fork / 重編核心，希望用**任意語言**為 SwiftMQ 擴充能力的開發者。
> **範圍**：本文只講一種外掛形態 —— **外部行程外掛**（核心術語 `sidecar`）。核心內建協定外掛（AMQP 0-9-1 / MQTT）不在本文範圍。
> **讀法**：第 1–2 節建立心智模型，第 3 節寫程式碼，**第 5 節是「開發完怎麼接進來一起跑、對外提供服務」**；
> **用其他語言（Python / Node.js / PHP / Java）請看 §4 的分語言指南**（各帶一個實測跑通的完整範例專案）。
> 文中程式碼是最小可執行骨架，可直接複製為起點。簡體中文為來源語言。

---

## 1. 它是什麼

一個**獨立行程**，在自己的行程裡實作某個「協定」（解析客戶端位元組流），
核心依設定把它託管起來：**連接埠由核心開啟、連線由核心代理**，註冊 / 啟停 / 稽核 / 隔離全部沿用核心既有機制。

先建立三條正確的心智模型（最容易搞錯的地方）：

1. **外掛行程是一個「本機服務」**：它只監聽一個**本機位址**（TCP 或 unix socket），等**核心來連**。
   連線方向是 **核心（客戶端）→ 外掛（伺服端）**，握手也由核心先發。
2. **對外業務連接埠不由外掛開啟**：由**核心**依設定裡的 `protocols[].listeners` 建立，並映射出來給客戶端。
   客戶端連的是**核心的連接埠**，位元組被核心代理到外掛行程。外掛行程**不需要**自己開業務連接埠。
3. **語意可選**：外掛可以只「搬運位元組」（協定完全由你自己實作），
   也可以經**反向呼叫** `session.*` 觸達核心語意（佇列 / 路由 / 權限 / 確認），
   與內建協定外掛**完全同一套語意**（vhost、權限、路由、佇列行為因此不會分叉）。

| 好處 | 代價 |
| --- | --- |
| 不改核心、不重編核心即可擴充 | 資料面多一次本機位元組複製（核心代理，無 fd 傳遞，跨平台一致） |
| 任意語言實作（只需要能實作線路協定） | 反向呼叫每次多一次本機 RPC（JSON 編解碼 + 複製） |
| 外掛可獨立發佈 / 升級 / 重啟 | 嗅探留在核心側：只能按「前綴」或「專屬連接埠」被識別 |
| 崩潰只影響該外掛：核心標記 `down`，不退不崩 | 只有 `net.listen` 能力真正生效，其他能力值為保留（見 §7） |

---

## 2. 運作原理

連線建立（**核心是客戶端，外掛是伺服端**）：

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

開始服務：

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**生命週期**（核心側 `internal/plugin/sidecar` 宿主）：

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**狀態語意**（`swiftmqctl plugins show` 可見）：

| 狀態 | 含義 | 維運動作 |
| --- | --- | --- |
| `enabled` | 已接入並服務中 | — |
| `failed` | **啟動就沒起來**（設定錯誤 / 握手被拒 / 行程拉不起來） | 看 `RuntimeNote` 與核心日誌，改設定或修外掛；重啟核心才會重試 |
| `down` | **起來過、現在不在**（行程崩了 / 連線斷了） | 去拉起外掛行程；核心會依 `restart` 策略自動恢復 |
| `disabled` | 設定裡 `enabled=false`，或維運熱停用 | `plugins enable <名稱>` 恢復 |

---

## 3. 開發（Go）

### 3.1 建立專案

外掛是**獨立的 Go module**，只相依兩個對外契約套件：

- `github.com/houzch/swiftmq/pkg/sidecar` —— 線路協定與外掛側實作（**必需**）
- `github.com/houzch/swiftmq/pkg/plugin` —— 僅當你要用 `plugin.Message` / `plugin.Error` 等型別時（選用）

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/swiftmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/swiftmq@v1.1.02
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/swiftmq=../swiftmq
```

> 用 `replace` 聯調時，外掛與核心必須**同一份原始碼**，否則 API 版本（`v1`）雖一致、型別卻可能不同。

### 3.2 實作 `Handler`（三個方法）

外掛行程的全部業務面就是 `sidecar.Handler` 的 `Hello` / `Call` / `Open`。
握手、心跳、多工、分塊都由 `pkg/sidecar` 處理，你不需要碰幀。

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

### 3.3 資料面：讀寫 `Stream`

`sidecar.Stream` 實作 `io.ReadWriteCloser`，直接當成「一條連線」用即可：

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

重點：

- 每條客戶端連線 = 一條流；外掛可以在 `Handler.Open` 裡立即開始收發。
- 大訊息由函式庫自動分塊（每幀 ≤ 64 KiB），**記憶體佔用與訊息大小無關**。
- 背壓：單條流的接收緩衝區有上限，緩衝區滿時阻塞該連線的**分發協程**（所有流一起等）——
  這是「記憶體可預測」與「單流限速」的取捨，詳見 §7。

### 3.4 使用核心語意橋（`session.*`）

想讓外掛重用核心的佇列 / 路由 / 權限 / 確認語意（而不是自己造一套），就用**反向呼叫**。
在流上依 `session.open` → 其他 `session.*` → （`session.close`）的順序使用：

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

**反向呼叫方法一覽**（`pkg/sidecar/bridge.go` 的常數 → 線路名稱）：

| 分組 | 線路名稱（常數） | 說明 |
| --- | --- | --- |
| 認證 | `core.authenticate`（`MethodCoreAuthenticate`） | **必須先做**：把協定裡的憑據交給核心驗證；參數 `{stream, mechanism, response}`，回傳 `{user}` |
| 會話 | `session.open`（`MethodSessionOpen`） | 在流上開啟某 vhost 的會話；**認證之後**才能做 |
| | `session.close`（`MethodSessionClose`） | 釋放該流上的會話（取消消費者、刪除獨佔佇列） |
| 交換器 | `session.declare_exchange` / `session.delete_exchange` | 新增/刪除（被動宣告不存在 → `KindNotFound`） |
| | `session.bind_exchange` / `session.unbind_exchange` | 交換器到交換器的綁定 |
| 佇列 | `session.declare_queue` / `session.delete_queue` | 新增/刪除；`name` 為空時由伺服端產生 |
| | `session.bind_queue` / `session.unbind_queue` | 佇列到交換器的綁定 |
| | `session.purge_queue` | 清空就緒訊息（不含未確認） |
| 發佈 | `session.publish` | 回傳 `{routed, rejected}`；持久化在回應前完成 |
| 拉取 | `session.get` | 主動拉一條；`found=false` 表示佇列為空 |
| 消費 | `session.consume` / `session.cancel` | 註冊 / 取消消費者 |
| 結算 | `session.settle` | 結算一條投遞（`ack` / `requeue` / `reject`） |
| **正向** | `session.deliver` | **核心 → 外掛**：投遞回推（在你的 `Handler.Call` 裡處理） |

**必須遵守的四條約定**：

1. **先 `core.authenticate`**：連線的核心操作面在認證前沒有身分，
   此時 `session.open` 會被拒（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。
   外掛負責從自己的協定裡取憑據；認證邏輯與使用者表仍在核心，外掛不接觸密碼庫。
2. **再 `session.open`**：未開啟會話就呼叫其他方法，核心回傳 `KindPreconditionFailed`（「流 N 尚未開啟會話」）。
3. **每條投遞恰好結算一次**：`Ack` / `Requeue` / `Reject` 三選一。
   `Ack` 與 `Reject` 都丟棄訊息，**只有 `Reject` 走死信**。
4. **未結算的投遞不會遺失**：流結束（客戶端斷線 / `Handler.Open` 回傳）或外掛連線中斷時，
   核心把所有未結算投遞**以「重新入列」處理**，避免訊息滯留。

**錯誤還原**：核心的 `*plugin.Error` 經橋以 `*sidecar.RPCError`（欄位 `Kind` / `Text`）抵達，
可還原成帶分類的 `plugin.Error`，而不是把分類丟在字串裡：

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

常見 `Kind` 與內建協定的對應（供你決定怎麼回錯給客戶端）：

| `plugin.ErrorKind` | 語意 | AMQP 0-9-1 對應 |
| --- | --- | --- |
| `KindNotFound` | 物件不存在 | 404 NOT_FOUND（關閉 Channel） |
| `KindPreconditionFailed` | 參數與已存在物件不一致 / 會話未開啟 | 406 PRECONDITION_FAILED（關閉 Channel） |
| `KindAccessRefused` | 權限不足 / 保留名稱 | 403 ACCESS_REFUSED（關閉 Channel） |
| `KindResourceLocked` | 獨佔資源被佔用 | 405 RESOURCE_LOCKED（關閉 Channel） |
| `KindInvalidPath` | vhost 不存在 | 402 INVALID_PATH（關閉連線） |
| `KindNotImplemented` | 能力未實作 | 540 NOT_IMPLEMENTED（關閉連線） |
| `KindInternal` | 核心內部錯誤 | 541 INTERNAL_ERROR（關閉連線） |

**訊息型別保真的邊界**：屬性表（`MessageDTO.Properties.Headers`）經 JSON 中轉，
像 AMQP field-table 那樣**區分 `int32` / `double` 的數值型別資訊拿不到**。
需要嚴格型別保真時，把這類資訊放進訊息內文（原始位元組）自行承載。

### 3.5 外掛側狀態與日誌

- 外部外掛的**狀態由「連線是否存活」決定**，外掛無須自報（內建外掛的 `StateReporter` 不適用於外部行程）。
- 日誌：`sidecar.ServerOptions.Logger` 輸出到外掛行程的 stdout/stderr；
  **由核心 `spawn` 拉起時，這些輸出會被核心轉發進核心日誌**（帶 `plugin` 標籤），便於統一蒐集。
- 獨立部署（非 spawn）時，外掛日誌依你自己的方式蒐集。

### 3.6 不接核心也能自測

`Handler` 是普通 Go 介面，單元測試裡直接實例化、呼叫 `Hello` / `Call` / `Open` 即可涵蓋業務邏輯，無需啟動網路：

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

端到端驗證走 §5。

---

## 4. 用其他語言開發（線路協定規格）

`pkg/sidecar` 是零相依的對外契約；線路協定本身很簡單，任何語言都能實作。
要對接，你需要實作下面這些「位元組級」約定（原始碼見 `pkg/sidecar/frame.go`、`proto.go`）。

> **已提供帶完整範例專案的分語言指南**（範例都實測跑通過握手 → 認證 → 語意橋 → 投遞/結算 → 位元組流）：
>
> | 語言 | 指南 | 範例專案（工作區 `swiftmq-plugin/`） |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py`（僅標準函式庫） |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js`（僅標準函式庫） |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php`（僅標準函式庫） |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java`（單一檔案，僅 JDK） |
>
> Go 的完整參考實作見獨立測試專案 `swiftmq-test/test/integration/echosidecar/`（它直接用 `pkg/sidecar.Server`，
> 無需關心下面的位元組層細節）。

**幀格式**（所有幀統一）：

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**幀型別 `kind`**：

| kind | 名稱 | 方向 | 酬載 |
| --- | --- | --- | --- |
| 1 | Hello | 核心 → 外掛 | JSON `Hello` |
| 2 | HelloAck | 外掛 → 核心 | JSON `HelloAck` |
| 3 | Ping | 核心 → 外掛 | 空 |
| 4 | Pong | 外掛 → 核心 | 空 |
| 5 | Call | 雙向 | JSON `Call` |
| 6 | Reply | 雙向 | JSON `Reply` |
| 7 | Open | 核心 → 外掛 | JSON `Open` |
| 8 | OpenAck | 外掛 → 核心 | JSON `OpenAck` |
| 9 | Data | 雙向 | `u32 BE stream` + 原始位元組 |
| 10 | Close | 雙向 | JSON `Close` |

**控制面 JSON 結構**（欄位名稱與 `proto.go` 一致）。

> 這一段是**線路協定報文範例**（同一區塊裡依序給出多個報文，故用 `//` 分隔說明），
> **不是能直接寫進 `swiftmqd.json` 的設定**。

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

**必須遵守的語意**：

- **握手**：核心先發 `Hello`，外掛必須回一幀 `HelloAck`。
  核心會驗證 `HelloAck.name == 設定裡的外掛名稱` 且 `HelloAck.api_version == 核心的 APIVersion`；
  `deny` 非空視為拒絕接入（外掛被隔離）。
- **心跳**：核心預設每 2s 發 `Ping`，外掛須在 8s 內回 `Pong`；外掛側若 24s 內沒有收到任何幀可自行關閉連線。
- **兩個 ID 空間**：`Call` 的 `reverse` 區分方向，兩個方向各自從 1 遞增，
  因此 `Reply` **必須回帶 `reverse`**，否則回應會被投給錯誤的等待者。
- **資料面不 base64**：訊息內文等大塊資料直接放 `Data` 幀酬載（`stream` + 原始位元組），視需要分塊。

> 若用 Go，直接用 `pkg/sidecar`，上述細節都不用自己實作。

---

## 5. 放進來一起運行：接入、對外服務、打包 ★

這一節回答「開發完怎麼接進 SwiftMQ、怎麼對外提供服務」。

### 5.1 在設定裡宣告外掛

外部外掛**完全由設定託管**，核心不需要為它改任何程式碼。在 `swiftmqd.json` 的 `plugins` 段加一項
（**實際設定是標準 JSON，不能帶註解**）：

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

逐項說明（欄位清單見下表）：

- `plugins.<外掛名稱>` 的鍵名**必須與外掛自報的 `HelloAck.name` 一致**，否則握手被拒。
- `builtin: false`：外部外掛顯式宣告（不寫會被管理面當內建顯示）。
- `enabled`：關掉它 = 不拉行程、不建監聽。
- `required: true` 時啟動失敗會阻塞核心啟動 —— 外部外掛不要開。
- `address` 是**核心去連的位址**（核心是客戶端）；`spawn` 非空時由核心代為拉起行程。
- `protocols[].prefix` **必須非空**（嗅探規則見 §5.3）。
- `listeners` 是該協定的**對外連接埠，由核心開啟**（客戶端連的是核心）。

欄位一覽：

| 欄位 | 必填 | 說明 |
| --- | --- | --- |
| `sidecar.address` | ✅ | 外掛行程位址：`tcp://host:port` 或 `unix:///path` |
| `sidecar.spawn` | ✕ | 核心代為啟動的命令列（首個元素是可執行檔）；**留空 = 核心只連不拉**，行程由你自行管理 |
| `sidecar.restart` | ✕ | `always`（預設，斷線/崩潰後自動恢復）或 `never`（只標記 `down`，等維運介入） |
| `sidecar.protocols[].name` | ✅ | 協定名稱（全域唯一，參與嗅探優先序） |
| `sidecar.protocols[].prefix` | ✕ | 嗅探前綴（ASCII）；**空 = 不參與嗅探** |
| `sidecar.protocols[].listeners[]` | ✕ | 該協定的對外監聽（`name` + `addr`），由核心建立 |
| `sidecar.handshake_timeout_seconds` | ✕ | 覆寫握手逾時（預設 5s） |
| `sidecar.heartbeat_seconds` | ✕ | 覆寫心跳間隔（預設 2s） |

> **外掛名稱與協定名稱**：二者**可以不同**（例如外掛 `my-sidecar` 提供協定 `myproto`）。
> 熱停用會先依外掛名稱找到它註冊的全部協定、再關閉這些協定的連接埠，因此不需要刻意取同名。

### 5.2 三種接法

| 接法 | 設定 | 適用 |
| --- | --- | --- |
| **同主機 + 核心代拉（spawn）** | `spawn: [...]`，`address` 指向它監聽的位址 | 同機部署、單一容器；最省事，核心負責拉起與回收 |
| **同主機 + 自行管理（dial）** | `spawn: []`，`address` 指向已運行的行程 | 用 systemd / supervisor 管理外掛生命週期 |
| **跨主機 / 跨容器（dial，必須 tcp）** | `spawn: []`，`address: "tcp://<服務名稱>:19001"` | 外掛與核心分容器 / 分機器部署 |

位址選擇：

- **同機建議 unix socket**（`unix:///tmp/my-sidecar.sock`）：不佔 TCP 連接埠、不受宿主連接埠佔用影響。
  注意 socket 路徑要對核心行程（容器裡是非 root 的 `swiftmq` 使用者）可寫。
- **跨容器必須 TCP**，且外掛行程要監聽 `0.0.0.0`，`address` 用**容器網路裡的服務名稱**。

> 方向別搞反：**外掛監聽的位址** = `address`；**對外開放給客戶端的連接埠** = `protocols[].listeners`。

### 5.3 對外提供服務：靠 `prefix` 被識別

接入層分發連線時**只看嗅探結果**：它對每個已啟用協定依註冊順序問 `Sniff(peek)`（peek 最多 8 位元組），
命中者接管這條連線。因此：

1. **`prefix` 必須非空**（ASCII，≤ 8 位元組）。客戶端發來的前幾個位元組等於它，連線才會交給你的外掛。
   例：`"prefix": "PY"` → 客戶端首位元組須是 `PY`（可以把前綴當作你協定的魔法標頭）。
2. **`prefix` 為空表示不參與嗅探**：這類連線**不會**被交給外掛（實測：監聽連接埠上的連線會被立刻中斷）。
   因此空 `prefix` 只適合「另有協定會在同連接埠上幫你轉發」的情境，**不要**用它來做專屬連接埠。
3. `listeners[].addr` 決定「在哪個連接埠上對外開放」，`prefix` 決定「這條連線算不算你的」——
   兩者要配套使用：**專屬連接埠也要給一個非空 `prefix`**（這也是核心範例設定裡
   `echo-sidecar` 同時寫 `prefix: "ECHO"` 與 `listeners: [":1885"]` 的原因）。
4. 嗅探依協定註冊順序匹配，**先匹配者生效**：多個外掛共存時，前綴要有區辨度（例如都以同一位元組開頭會互相遮蔽）。

### 5.4 覆寫監聽位址與 TLS

- 對外監聽位址可在**兩處**給：`sidecar.protocols[].listeners[].addr`（預設）與
  `listeners.<協定名稱>`（依協定名稱整體覆寫）。二者同時存在時以 `listeners.<協定名稱>` 為準。
- 需要 TLS 時，在 `listeners.<協定名稱>[i].tls` 裡給憑證（欄位與內建協定一致）。
  下面是一個 `listeners` 片段（**標準 JSON，不能帶註解**）：第 1 項明文，第 2 項走 TLS。

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS 由**核心**在監聽側終結，外掛行程拿到的是明文流——外掛不必處理 TLS。

### 5.5 打包：讓外掛與核心一起跑

**做法 A —— 打進同一個映像**（建議給「隨核心發佈」的外掛）：在 `swiftmq/Dockerfile` 的執行階段加一行：

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

然後設定裡 `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`，
`address` 同值。核心啟動時會拉起它。

**做法 B —— 掛載二進位檔**（不改映像，適合聯調）：

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.02
    command: ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

設定用 unix socket（避免額外佔連接埠）。下面是 `plugins.my-sidecar` 裡的 `sidecar` 片段
（**標準 JSON，不能帶註解**；`prefix` 仍要非空，見 §5.3）：

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**做法 C —— 獨立容器**（外掛單獨發佈 / 獨立擴縮容）：

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.02
    volumes: ["./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

設定裡 `spawn: []`（核心只連不拉），`address: "tcp://my-sidecar:19001"`（compose 服務名稱）。

### 5.6 啟動與驗證

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

在 `plugins show` 裡特別關注 `state` 與 `RuntimeNote`：
`failed` 會帶失敗原因（握手被拒 / 連接埠拉不起來…）；`down` 會帶中斷原因（行程崩了 / 連線斷了）。

### 5.7 運行期維運

| 操作 | 指令 / 介面 | 效果 |
| --- | --- | --- |
| 熱停用 | `swiftmqctl plugins disable my-sidecar` 或 `PUT /api/plugins/my-sidecar/disable` | **關閉該外掛的對外監聽**（能力級停用）；核心與其他外掛不受影響 |
| 熱啟用 | `swiftmqctl plugins enable my-sidecar` | 重新打開它的監聽；若之前啟動失敗會重試一次 |
| 看狀態 | `swiftmqctl plugins list/show` | 狀態 + 失敗/中斷原因 |
| 核心退出 | — | 斷開與外掛的連線、回收橋上會話、**終止由核心 `spawn` 的子行程** |

> 熱停用只關「能力」（監聽連接埠），**不會**殺掉用 `spawn` 拉起的外掛行程；行程的回收發生在核心退出時。

### 5.8 在管理後台提供入口（選用）

外掛自帶操作介面時，在 `plugins.<外掛名稱>` 段裡加一個 `console_url`（管理介面位址，其餘欄位見 §5.1）。
下面只畫出 `plugins.my-sidecar` 這一項（**標準 JSON，不能帶註解**；`sidecar` 段內容同 §5.1）：

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

- 管理後台的**外掛管理**頁（資料來自 `GET /api/plugins` 的 `console_url` 欄位）會為這類外掛顯示
  「打開管理介面」按鈕，**在新分頁開啟**。
- 未宣告 `console_url` 時按鈕不可用，懸浮提示「該外掛未提供管理介面」。
- 它只是**部署方寫在設定裡的中繼資料**：不屬於外掛 API（`pkg/plugin`）、不參與外掛啟停，
  介面本身由外掛自行託管（可以就在外掛行程裡，或任意獨立服務）。

---

## 6. 生命週期與容錯矩陣

| 情形 | 核心行為 | 外掛側影響 |
| --- | --- | --- |
| 外掛行程未啟動 / 握手被拒 | 8s 內重試連線，仍失敗則標記 `failed` 並隔離（不阻塞核心啟動） | 無 |
| `spawn` 拉起的行程退出 | 記錄日誌；標記 `down`；依 `restart` 策略退避重連 / 重拉 | 新行程重新握手 |
| 外掛行程崩潰（運行期） | 核心不受影響；`down` + 退避重連 | `pkg/sidecar.Server` 會關閉該連線 |
| 核心被 `kill -9` | — | 外掛側靠閒置逾時（預設 24s 無幀）自行回收連線，不留殭屍 |
| 核心正常退出 | 呼叫 `Stop`：中斷連線、回收未結算投遞（以重新入列）、`Kill` 子行程 | 收到 SIGKILL |
| 外掛連線中斷期間的投遞 | 未結算投遞一律**重新入列**，不會遺失 | — |
| 客戶端斷線 / `Open` 回傳 | 關閉對應流，釋放該流的會話與消費者 | `Stream.Read` 回傳 EOF |

---

## 7. 紅線與已知邊界

**紅線**

1. 外掛只允許相依 `pkg/sidecar`（以及選用的 `pkg/plugin`）；**不得**相依核心 `internal/**`。
2. 外掛名稱必須與設定一致、`APIVersion` 必須與核心一致，否則無法接入（這是防「靜默跑著不生效」）。
3. 用 `session.*` 時：**先 `core.authenticate`、再 `session.open`**，每條投遞**恰好結算一次**。
4. `protocols[].prefix` 必須非空，否則連線不會被交給外掛（見 §5.3）。
5. `Hello` 裡拒絕要**明確回 error**（不要靜默）——否則核心只能看到「連線被關閉」，定位不到原因。

**已知邊界**

- **嗅探在核心側**：外部外掛不能自訂嗅探函式，只能靠 `prefix`（ASCII，≤ 8 位元組）匹配；
  `prefix` 為空即「拿不到連線」（見 §5.3）。
- **資料面走本機代理**：無 fd 傳遞（Windows 無 `SCM_RIGHTS`），比行程內多一次記憶體複製；
  反向呼叫每次也多一次本機 RPC。
- **屬性表型別會退化**：`Properties.Headers` 經 JSON 中轉，`int32` / `double` 之類區分遺失（見 §3.4）。
- **單流背壓影響整條連線**：一條流的接收緩衝區滿時會阻塞該連線的分發協程；依流限速屬後續最佳化。
- **認證失敗只透傳文字**：核心認證失敗是 `*plugin.AuthError`（與 `plugin.ErrorKind` 不是同一套分類），
  經橋到達外掛時只有文字，外掛需依自己的約定對應成協定錯誤碼。
- **只有 `net.listen` 能力真正生效**：`store.read/write`、`http.route`、`cluster.metadata.write`、
  `auth.verify` 是**保留位**，宣告後僅參與稽核（見 §5.6 的治理展示），目前沒有對應擴充點。

---

## 8. 疑難排解 FAQ

| 現象 | 原因與處理 |
| --- | --- |
| 狀態 `failed`，原因含「外掛名稱不一致」 | 設定的外掛名稱 ≠ `HelloAck.name`；改齊 |
| 狀態 `failed`，原因含「API 版本不匹配」 | `HelloAck.api_version` ≠ 核心 `APIVersion`；改齊 |
| 狀態 `failed`，原因含「拒絕握手」 | 外掛 `Hello` 回傳了 error（`deny`）；看核心日誌裡轉發的外掛輸出 |
| 狀態 `failed`，原因含「連接外部外掛失敗」 | 行程沒起來 / `address` 寫錯 / socket 路徑不可寫（容器裡注意 `swiftmq` 使用者權限） |
| 狀態 `down` | 外掛行程崩了或連線斷了；`restart=always` 會自動重連，`never` 需人工拉起 |
| 連接埠沒開 / 客戶端連不上 | `protocols[].listeners` 沒配或位址被 `listeners.<協定名稱>` 覆寫掉了；核對兩處 |
| 客戶端連上別的連接埠後立刻中斷 | 該連接埠不匹配你的協定（`prefix` 為空或前綴不符）；給協定配一個非空 `prefix`（見 §5.3） |
| 報 `ACCESS_REFUSED - ... for user ''` | 語意橋前**沒有認證**；先呼叫 `core.authenticate` 再 `session.open` |
| 反向呼叫報「流 N 尚未開啟會話」 | 先 `core.authenticate`、再 `session.open`，然後才能呼叫其他 `session.*` |
| 收不到消費投遞 | 投遞以**正向呼叫** `session.deliver` 到達你的 `Call`；確認已處理該方法 |
| 外掛在容器外、核心在容器內，連不上 | `address` 用 `tcp://host.docker.internal:<port>`（或把外掛也放進容器、用服務名稱）；外掛需監聽 `0.0.0.0` |

---

## 9. 參考（原始碼索引）

| 想看什麼 | 檔案 |
| --- | --- |
| 線路協定與兩端實作（**開發必讀**） | [`pkg/sidecar/`](../../../pkg/sidecar/)：`frame.go`（幀）、`proto.go`（報文）、`server.go`（外掛側）、`client.go`（核心側）、`bridge.go`（`session.*` 契約）、`stream.go`（流） |
| 核心側 sidecar 宿主（接入/重連/代理/狀態） | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| 核心側語意橋（`session.*` → `plugin.Session`） | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| 核心會話操作面型別（`Message` / `Delivery` / `ErrorKind`） | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| 監聽、嗅探、依外掛熱啟停 | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| 外掛生命週期與治理（隔離/狀態/稽核） | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go)、[`registry.go`](../../../internal/plugin/registry.go) |
| 設定項與範例（含 sidecar 段） | [`internal/config/config.go`](../../../internal/config/config.go)、[`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| 行程組裝（sidecar 如何被組裝進核心） | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Go 參考實作（用 `pkg/sidecar.Server`，含 `session.*` 橋與 `core.authenticate`） | 獨立測試專案 `swiftmq-test/test/integration/echosidecar/` |
| **分語言指南 + 範例專案** | 本目錄 `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`；範例在**工作區** `swiftmq-plugin/{python,nodejs,php,java}/` |
