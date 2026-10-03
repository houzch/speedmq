# Hướng dẫn phát triển plugin tiến trình ngoài (sidecar) cho SwiftMQ

> **Đối tượng**: các nhà phát triển không muốn fork / biên dịch lại kernel mà muốn dùng **bất kỳ ngôn ngữ nào** để mở rộng năng lực cho SwiftMQ.
> **Phạm vi**: tài liệu này chỉ bàn về một dạng plugin —— **plugin tiến trình ngoài** (thuật ngữ kernel là `sidecar`). Plugin giao thức tích hợp sẵn của kernel (AMQP 0-9-1 / MQTT) không thuộc phạm vi tài liệu này.
> **Cách đọc**: mục 1–2 xây dựng mô hình tư duy, mục 3 viết mã, **mục 5 là "sau khi phát triển xong thì tích hợp vào chạy chung và cung cấp dịch vụ ra bên ngoài như thế nào"**;
> **dùng ngôn ngữ khác (Python / Node.js / PHP / Java) thì xem hướng dẫn theo từng ngôn ngữ ở §4** (mỗi phần kèm một dự án ví dụ hoàn chỉnh đã chạy thực tế).
> Mã trong tài liệu là bộ khung tối giản có thể chạy được, có thể sao chép trực tiếp làm điểm bắt đầu. Tiếng Trung giản thể là ngôn ngữ nguồn.

---

## 1. Nó là gì

Một **tiến trình độc lập**, tự hiện thực một "giao thức" nào đó (phân tích luồng byte của client) ngay trong tiến trình của chính nó;
kernel đảm nhận việc quản lý nó theo cấu hình: **cổng do kernel mở, kết nối do kernel proxy**, còn đăng ký / khởi động-dừng / kiểm toán / cô lập đều tái sử dụng các cơ chế sẵn có của kernel.

Trước tiên hãy thiết lập ba mô hình tư duy đúng đắn (những điểm dễ hiểu nhầm nhất):

1. **Tiến trình plugin là một "dịch vụ cục bộ"**: nó chỉ lắng nghe một **địa chỉ cục bộ** (TCP hoặc unix socket), chờ **kernel kết nối tới**.
   Chiều kết nối là **kernel (client) → plugin (server)**, và kernel cũng là bên gửi bắt tay trước.
2. **Cổng nghiệp vụ hướng ngoại không do plugin mở**: nó do **kernel** tạo theo `protocols[].listeners` trong cấu hình rồi ánh xạ ra cho client.
   Client kết nối tới **cổng của kernel**, byte được kernel proxy tới tiến trình plugin. Tiến trình plugin **không cần** tự mở cổng nghiệp vụ.
3. **Ngữ nghĩa là tùy chọn**: plugin có thể chỉ "vận chuyển byte" (giao thức do bạn tự hiện thực hoàn toàn),
   hoặc cũng có thể chạm tới ngữ nghĩa của kernel (hàng đợi / định tuyến / quyền / xác nhận) thông qua **lời gọi ngược** `session.*`,
   với **cùng một bộ ngữ nghĩa hoàn toàn** như plugin giao thức tích hợp sẵn (nhờ đó vhost, quyền, định tuyến, hành vi hàng đợi sẽ không bị phân kỳ).

| Lợi ích | Cái giá phải trả |
| --- | --- |
| Mở rộng mà không cần sửa kernel, không cần biên dịch lại kernel | Mặt phẳng dữ liệu tốn thêm một lần sao chép byte cục bộ (kernel proxy, không truyền fd, nhất quán trên mọi nền tảng) |
| Hiện thực bằng bất kỳ ngôn ngữ nào (chỉ cần hiện thực được giao thức đường dây) | Mỗi lời gọi ngược tốn thêm một RPC cục bộ (mã hóa/giải mã JSON + sao chép) |
| Plugin có thể phát hành / nâng cấp / khởi động lại độc lập | Việc sniffing nằm lại phía kernel: chỉ có thể được nhận diện theo "tiền tố" hoặc "cổng riêng" |
| Sự cố chỉ ảnh hưởng tới plugin đó: kernel đánh dấu `down`, không thoát cũng không sập | Chỉ có năng lực `net.listen` thực sự có hiệu lực, các giá trị năng lực khác là dự phòng (xem §7) |

---

## 2. Nguyên lý hoạt động

Thiết lập kết nối (**kernel là client, plugin là server**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

Bắt tay:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

Bắt đầu phục vụ:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Vòng đời** (host `internal/plugin/sidecar` phía kernel):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Ngữ nghĩa trạng thái** (thấy được qua `swiftmqctl plugins show`):

| Trạng thái | Ý nghĩa | Thao tác vận hành |
| --- | --- | --- |
| `enabled` | Đã kết nối và đang phục vụ | — |
| `failed` | **Khởi động đã không lên được** (cấu hình sai / bắt tay bị từ chối / không kéo được tiến trình) | Xem `RuntimeNote` và log kernel, sửa cấu hình hoặc sửa plugin; phải khởi động lại kernel mới thử lại |
| `down` | **Đã từng lên, giờ thì không còn** (tiến trình sập / kết nối đứt) | Đi kéo tiến trình plugin lên; kernel sẽ tự khôi phục theo chính sách `restart` |
| `disabled` | `enabled=false` trong cấu hình, hoặc bị vận hành viên tắt nóng | Khôi phục bằng `plugins enable <tên>` |

---

## 3. Phát triển (Go)

### 3.1 Tạo dự án

Plugin là một **Go module độc lập**, chỉ phụ thuộc vào hai gói hợp đồng hướng ngoại:

- `github.com/houzch/swiftmq/pkg/sidecar` —— giao thức đường dây và hiện thực phía plugin (**bắt buộc**)
- `github.com/houzch/swiftmq/pkg/plugin` —— chỉ khi bạn cần dùng các kiểu như `plugin.Message` / `plugin.Error` (tùy chọn)

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

> Khi dùng `replace` để liên kết gỡ lỗi, plugin và kernel phải dùng **cùng một bộ mã nguồn**, nếu không thì dù phiên bản API (`v1`) có khớp, kiểu dữ liệu vẫn có thể khác nhau.

### 3.2 Hiện thực `Handler` (ba phương thức)

Toàn bộ mặt nghiệp vụ của tiến trình plugin chính là `Hello` / `Call` / `Open` của `sidecar.Handler`.
Việc bắt tay, nhịp tim, dồn kênh, chia khối đều do `pkg/sidecar` xử lý, bạn không cần chạm tới frame.

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

### 3.3 Mặt phẳng dữ liệu: đọc và ghi `Stream`

`sidecar.Stream` hiện thực `io.ReadWriteCloser`, cứ dùng nó như "một kết nối" là được:

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

Những điểm chính:

- Mỗi kết nối client = một luồng; plugin có thể bắt đầu gửi/nhận ngay trong `Handler.Open`.
- Message lớn được thư viện tự động chia khối (mỗi frame ≤ 64 KiB), **mức chiếm dụng bộ nhớ không liên quan tới kích thước message**.
- Backpressure: bộ đệm nhận của một luồng có giới hạn trên, khi bộ đệm đầy sẽ chặn **goroutine phân phối** của kết nối đó (mọi luồng cùng chờ) ——
  đây là sự đánh đổi giữa "bộ nhớ dự đoán được" và "giới hạn tốc độ từng luồng", xem chi tiết ở §7.

### 3.4 Dùng cầu ngữ nghĩa của kernel (`session.*`)

Nếu muốn plugin tái sử dụng ngữ nghĩa hàng đợi / định tuyến / quyền / xác nhận của kernel (thay vì tự dựng một bộ riêng), hãy dùng **lời gọi ngược**.
Trên một luồng, dùng theo thứ tự `session.open` → các `session.*` khác → (`session.close`):

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

**Danh sách phương thức gọi ngược** (hằng số trong `pkg/sidecar/bridge.go` → tên trên đường dây):

| Nhóm | Tên đường dây (hằng số) | Mô tả |
| --- | --- | --- |
| Xác thực | `core.authenticate`（`MethodCoreAuthenticate`） | **Phải làm trước tiên**: giao thông tin xác thực từ giao thức của bạn cho kernel kiểm tra; tham số `{stream, mechanism, response}`, trả về `{user}` |
| Phiên | `session.open`（`MethodSessionOpen`） | Mở phiên cho một vhost trên luồng; chỉ làm được **sau khi xác thực** |
| | `session.close`（`MethodSessionClose`） | Giải phóng phiên trên luồng (hủy consumer, xóa hàng đợi độc quyền) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Thêm/xóa (khai báo thụ động một cái không tồn tại → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Ràng buộc exchange tới exchange |
| Hàng đợi | `session.declare_queue` / `session.delete_queue` | Thêm/xóa; khi `name` rỗng thì server tự sinh |
| | `session.bind_queue` / `session.unbind_queue` | Ràng buộc hàng đợi tới exchange |
| | `session.purge_queue` | Xóa sạch message đã sẵn sàng (không gồm message chưa xác nhận) |
| Publish | `session.publish` | Trả về `{routed, rejected}`; việc lưu bền hoàn tất trước khi phản hồi |
| Get | `session.get` | Chủ động lấy một message; `found=false` nghĩa là hàng đợi rỗng |
| Consume | `session.consume` / `session.cancel` | Đăng ký / hủy consumer |
| Settle | `session.settle` | Quyết toán một delivery (`ack` / `requeue` / `reject`) |
| **Forward** | `session.deliver` | **Kernel → plugin**: đẩy delivery về (xử lý trong `Handler.Call` của bạn) |

**Bốn quy ước bắt buộc phải tuân thủ**:

1. **`core.authenticate` trước**: mặt thao tác kernel của kết nối chưa có danh tính trước khi xác thực,
   lúc đó `session.open` sẽ bị từ chối (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Plugin chịu trách nhiệm lấy thông tin xác thực từ giao thức của chính nó; logic xác thực và bảng người dùng vẫn nằm ở kernel, plugin không chạm tới kho mật khẩu.
2. **`session.open` sau đó**: nếu chưa mở phiên mà gọi các phương thức khác, kernel trả về `KindPreconditionFailed` ("luồng N chưa mở phiên").
3. **Mỗi delivery được quyết toán đúng một lần**: chọn một trong ba `Ack` / `Requeue` / `Reject`.
   Cả `Ack` lẫn `Reject` đều loại bỏ message, **chỉ `Reject` mới đi vào dead-letter**.
4. **Delivery chưa quyết toán sẽ không bị mất**: khi luồng kết thúc (client ngắt kết nối / `Handler.Open` trả về) hoặc kết nối plugin đứt,
   kernel xử lý tất cả delivery chưa quyết toán **như "đưa trở lại hàng đợi"**, tránh để message tồn đọng.

**Khôi phục lỗi**: `*plugin.Error` của kernel đến qua cầu dưới dạng `*sidecar.RPCError` (trường `Kind` / `Text`),
có thể khôi phục thành `plugin.Error` có phân loại, thay vì để mất phân loại trong chuỗi:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Ánh xạ giữa các `Kind` thường gặp và giao thức tích hợp sẵn (để bạn quyết định cách trả lỗi cho client):

| `plugin.ErrorKind` | Ngữ nghĩa | Ánh xạ AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | Đối tượng không tồn tại | 404 NOT_FOUND (đóng Channel) |
| `KindPreconditionFailed` | Tham số không khớp với đối tượng đã tồn tại / phiên chưa mở | 406 PRECONDITION_FAILED (đóng Channel) |
| `KindAccessRefused` | Không đủ quyền / tên dành riêng | 403 ACCESS_REFUSED (đóng Channel) |
| `KindResourceLocked` | Tài nguyên độc quyền đang bị chiếm | 405 RESOURCE_LOCKED (đóng Channel) |
| `KindInvalidPath` | vhost không tồn tại | 402 INVALID_PATH (đóng kết nối) |
| `KindNotImplemented` | Năng lực chưa được hiện thực | 540 NOT_IMPLEMENTED (đóng kết nối) |
| `KindInternal` | Lỗi nội bộ kernel | 541 INTERNAL_ERROR (đóng kết nối) |

**Ranh giới bảo toàn kiểu dữ liệu của message**: bảng thuộc tính (`MessageDTO.Properties.Headers`) được trung chuyển qua JSON,
nên **không lấy được thông tin kiểu số phân biệt `int32` / `double`** như trong field-table của AMQP.
Khi cần bảo toàn kiểu nghiêm ngặt, hãy đặt loại thông tin đó vào phần thân message (byte thô) để tự mang theo.

### 3.5 Trạng thái và log phía plugin

- **Trạng thái của plugin tiến trình ngoài được quyết định bởi "kết nối còn sống hay không"**, plugin không cần tự báo cáo (`StateReporter` của plugin tích hợp sẵn không áp dụng cho tiến trình ngoài).
- Log: `sidecar.ServerOptions.Logger` xuất ra stdout/stderr của tiến trình plugin;
  **khi được kernel `spawn` kéo lên, những output này sẽ được kernel chuyển tiếp vào log kernel** (gắn nhãn `plugin`), thuận tiện cho việc thu thập tập trung.
- Khi triển khai độc lập (không spawn), thu thập log plugin theo cách của riêng bạn.

### 3.6 Tự kiểm thử mà không cần kết nối kernel

`Handler` là một interface Go thông thường, trong unit test chỉ cần khởi tạo trực tiếp rồi gọi `Hello` / `Call` / `Open` là có thể bao phủ logic nghiệp vụ, không cần dựng mạng:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Kiểm thử đầu-cuối xem §5.

---

## 4. Phát triển bằng ngôn ngữ khác (đặc tả giao thức đường dây)

`pkg/sidecar` là hợp đồng hướng ngoại không phụ thuộc gì; bản thân giao thức đường dây rất đơn giản, ngôn ngữ nào cũng hiện thực được.
Để kết nối, bạn cần hiện thực các quy ước "cấp byte" dưới đây (mã nguồn xem ở `pkg/sidecar/frame.go`, `proto.go`).

> **Đã cung cấp hướng dẫn theo từng ngôn ngữ kèm dự án ví dụ hoàn chỉnh** (các ví dụ đều đã chạy thực tế qua: bắt tay → xác thực → cầu ngữ nghĩa → delivery/quyết toán → luồng byte):
>
> | Ngôn ngữ | Hướng dẫn | Dự án ví dụ (trong workspace `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (chỉ thư viện chuẩn) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (chỉ thư viện chuẩn) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (chỉ thư viện chuẩn) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (một tệp, chỉ JDK) |
>
> Bản hiện thực tham chiếu đầy đủ bằng Go xem ở dự án kiểm thử độc lập `swiftmq-test/test/integration/echosidecar/` (nó dùng trực tiếp `pkg/sidecar.Server`,
> không cần bận tâm tới các chi tiết tầng byte bên dưới).

**Định dạng frame** (mọi frame đều thống nhất):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Loại frame `kind`**:

| kind | Tên | Hướng | Payload |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | rỗng |
| 4 | Pong | plugin → kernel | rỗng |
| 5 | Call | hai chiều | JSON `Call` |
| 6 | Reply | hai chiều | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | hai chiều | `u32 BE stream` + byte thô |
| 10 | Close | hai chiều | JSON `Close` |

**Cấu trúc JSON mặt điều khiển** (tên trường khớp với `proto.go`).

> Đoạn này là **ví dụ bản tin của giao thức đường dây** (trong cùng một khối đưa ra nhiều bản tin theo thứ tự, nên dùng `//` để phân tách và giải thích),
> **không phải cấu hình có thể ghi thẳng vào `swiftmqd.json`**.

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

**Ngữ nghĩa bắt buộc phải tuân thủ**:

- **Bắt tay**: kernel gửi `Hello` trước, plugin phải trả về một frame `HelloAck`.
  Kernel sẽ kiểm tra `HelloAck.name == tên plugin trong cấu hình` và `HelloAck.api_version == APIVersion của kernel`;
  `deny` khác rỗng được coi là từ chối kết nối (plugin bị cô lập).
- **Nhịp tim**: mặc định kernel gửi `Ping` mỗi 2s, plugin phải trả `Pong` trong vòng 8s; phía plugin nếu trong 24s không nhận được frame nào thì có thể tự đóng kết nối.
- **Hai không gian ID**: `reverse` của `Call` phân biệt hướng, hai hướng tự tăng từ 1 độc lập,
  do đó `Reply` **bắt buộc phải mang theo `reverse`**, nếu không phản hồi sẽ bị gửi cho nhầm bên đang chờ.
- **Mặt dữ liệu không dùng base64**: dữ liệu khối lớn như phần thân message được đặt thẳng vào payload của frame `Data` (`stream` + byte thô), chia khối khi cần.

> Nếu dùng Go, cứ dùng thẳng `pkg/sidecar`, mọi chi tiết trên đều không cần tự hiện thực.

---

## 5. Đưa vào chạy chung: tích hợp, cung cấp dịch vụ ra ngoài, đóng gói ★

Mục này trả lời câu hỏi "sau khi phát triển xong thì tích hợp vào SwiftMQ thế nào, cung cấp dịch vụ ra ngoài ra sao".

### 5.1 Khai báo plugin trong cấu hình

Plugin tiến trình ngoài **hoàn toàn do cấu hình quản lý**, kernel không cần sửa bất kỳ dòng mã nào vì nó. Thêm một mục vào đoạn `plugins` của `swiftmqd.json`
(**cấu hình thực tế là JSON chuẩn, không được có chú thích**):

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

Giải thích từng mục (danh sách trường xem bảng dưới):

- Tên khóa của `plugins.<tên plugin>` **phải khớp với `HelloAck.name` mà plugin tự báo**, nếu không bắt tay sẽ bị từ chối.
- `builtin: false`: khai báo tường minh đây là plugin tiến trình ngoài (không ghi thì mặt quản trị sẽ hiển thị như loại tích hợp sẵn).
- `enabled`: tắt nó = không kéo tiến trình, không tạo listener.
- Khi `required: true`, khởi động thất bại sẽ chặn kernel khởi động —— plugin tiến trình ngoài đừng bật.
- `address` là **địa chỉ mà kernel kết nối tới** (kernel là client); khi `spawn` khác rỗng thì kernel sẽ kéo tiến trình lên thay bạn.
- `protocols[].prefix` **phải khác rỗng** (quy tắc sniffing xem §5.3).
- `listeners` là **cổng hướng ngoại của giao thức đó, do kernel mở** (client kết nối tới kernel).

Danh sách trường:

| Trường | Bắt buộc | Mô tả |
| --- | --- | --- |
| `sidecar.address` | ✅ | Địa chỉ tiến trình plugin: `tcp://host:port` hoặc `unix:///path` |
| `sidecar.spawn` | ✕ | Dòng lệnh mà kernel khởi động thay bạn (phần tử đầu là tệp thực thi); **để trống = kernel chỉ kết nối không kéo lên**, tiến trình do bạn tự quản lý |
| `sidecar.restart` | ✕ | `always` (mặc định, tự khôi phục sau khi đứt kết nối/sập) hoặc `never` (chỉ đánh dấu `down`, chờ vận hành can thiệp) |
| `sidecar.protocols[].name` | ✅ | Tên giao thức (duy nhất toàn cục, tham gia độ ưu tiên sniffing) |
| `sidecar.protocols[].prefix` | ✕ | Tiền tố sniffing (ASCII); **rỗng = không tham gia sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Listener hướng ngoại của giao thức đó (`name` + `addr`), do kernel tạo |
| `sidecar.handshake_timeout_seconds` | ✕ | Ghi đè timeout bắt tay (mặc định 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Ghi đè khoảng cách nhịp tim (mặc định 2s) |

> **Tên plugin và tên giao thức**: hai cái **có thể khác nhau** (ví dụ plugin `my-sidecar` cung cấp giao thức `myproto`).
> Thao tác tắt nóng sẽ trước tiên tìm theo tên plugin ra toàn bộ giao thức do nó đăng ký, rồi đóng các cổng của những giao thức đó, nên không cần cố ý đặt cùng tên.

### 5.2 Ba cách tích hợp

| Cách tích hợp | Cấu hình | Phù hợp với |
| --- | --- | --- |
| **Cùng máy + kernel kéo lên thay bạn (spawn)** | `spawn: [...]`, `address` trỏ tới địa chỉ nó lắng nghe | Triển khai cùng máy, một container; nhàn nhất, kernel lo việc kéo lên và thu hồi |
| **Cùng máy + tự quản lý (dial)** | `spawn: []`, `address` trỏ tới tiến trình đang chạy | Dùng systemd / supervisor để quản lý vòng đời plugin |
| **Khác máy / khác container (dial, bắt buộc tcp)** | `spawn: []`, `address: "tcp://<tên dịch vụ>:19001"` | Plugin và kernel triển khai ở container / máy khác nhau |

Chọn địa chỉ:

- **Cùng máy thì khuyến nghị dùng unix socket** (`unix:///tmp/my-sidecar.sock`): không chiếm cổng TCP, không bị ảnh hưởng bởi việc cổng máy chủ đã bị chiếm.
  Lưu ý đường dẫn socket phải ghi được bởi tiến trình kernel (trong container là người dùng `swiftmq` không phải root).
- **Khác container bắt buộc dùng TCP**, và tiến trình plugin phải lắng nghe `0.0.0.0`, `address` dùng **tên dịch vụ trong mạng container**.

> Đừng nhầm ngược chiều: **địa chỉ plugin lắng nghe** = `address`; **cổng mở ra cho client bên ngoài** = `protocols[].listeners`.

### 5.3 Cung cấp dịch vụ ra ngoài: được nhận diện nhờ `prefix`

Khi phân phối kết nối, tầng truy cập **chỉ nhìn kết quả sniffing**: nó hỏi `Sniff(peek)` với từng giao thức đã bật theo thứ tự đăng ký (peek tối đa 8 byte),
bên khớp sẽ tiếp nhận kết nối đó. Do đó:

1. **`prefix` phải khác rỗng** (ASCII, ≤ 8 byte). Chỉ khi vài byte đầu tiên client gửi tới khớp với nó thì kết nối mới được giao cho plugin của bạn.
   Ví dụ: `"prefix": "PY"` → byte đầu của client phải là `PY` (có thể coi tiền tố như magic header của giao thức bạn).
2. **`prefix` rỗng nghĩa là không tham gia sniffing**: những kết nối như vậy **sẽ không** được giao cho plugin (thực tế: kết nối tới cổng lắng nghe sẽ bị ngắt ngay lập tức).
   Do đó `prefix` rỗng chỉ phù hợp với tình huống "có giao thức khác trên cùng cổng sẽ chuyển tiếp giúp bạn", **đừng** dùng nó để làm cổng riêng.
3. `listeners[].addr` quyết định "mở ra bên ngoài ở cổng nào", `prefix` quyết định "kết nối này có tính là của bạn hay không" ——
   hai cái phải dùng kèm nhau: **cổng riêng cũng phải cho một `prefix` khác rỗng** (đây cũng là lý do trong cấu hình ví dụ của kernel,
   `echo-sidecar` ghi đồng thời `prefix: "ECHO"` và `listeners: [":1885"]`).
4. Sniffing khớp theo thứ tự đăng ký giao thức, **bên khớp trước được hiệu lực**: khi nhiều plugin cùng tồn tại, tiền tố phải có độ phân biệt (ví dụ đều bắt đầu bằng cùng một byte sẽ che khuất lẫn nhau).

### 5.4 Ghi đè địa chỉ listener và TLS

- Địa chỉ listener hướng ngoại có thể đặt ở **hai nơi**: `sidecar.protocols[].listeners[].addr` (mặc định) và
  `listeners.<tên giao thức>` (ghi đè toàn bộ theo tên giao thức). Khi cả hai cùng tồn tại thì lấy `listeners.<tên giao thức>` làm chuẩn.
- Khi cần TLS, cấp chứng chỉ trong `listeners.<tên giao thức>[i].tls` (trường giống với giao thức tích hợp sẵn).
  Dưới đây là một đoạn `listeners` (**JSON chuẩn, không được có chú thích**): mục 1 là văn bản thuần, mục 2 dùng TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS do **kernel** kết thúc ở phía listener, tiến trình plugin nhận được là luồng văn bản thuần —— plugin không cần xử lý TLS.

### 5.5 Đóng gói: để plugin chạy cùng kernel

**Cách A —— Đóng gói vào cùng một image** (khuyến nghị cho plugin "phát hành kèm kernel"): thêm một dòng vào giai đoạn runtime của `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Sau đó trong cấu hình đặt `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
`address` cùng giá trị. Kernel sẽ kéo nó lên khi khởi động.

**Cách B —— Mount binary** (không đổi image, phù hợp để liên kết gỡ lỗi):

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

Cấu hình dùng unix socket (tránh chiếm thêm cổng). Dưới đây là đoạn `sidecar` trong `plugins.my-sidecar`
(**JSON chuẩn, không được có chú thích**; `prefix` vẫn phải khác rỗng, xem §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Cách C —— Container độc lập** (plugin phát hành riêng / mở rộng thu nhỏ độc lập):

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

Trong cấu hình `spawn: []` (kernel chỉ kết nối không kéo lên), `address: "tcp://my-sidecar:19001"` (tên dịch vụ compose).

### 5.6 Khởi động và kiểm chứng

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

Trong `plugins show` hãy chú ý đặc biệt tới `state` và `RuntimeNote`:
`failed` sẽ kèm lý do thất bại (bắt tay bị từ chối / không mở được cổng…); `down` sẽ kèm lý do đứt kết nối (tiến trình sập / kết nối đứt).

### 5.7 Vận hành trong thời gian chạy

| Thao tác | Lệnh / giao diện | Hiệu quả |
| --- | --- | --- |
| Tắt nóng | `swiftmqctl plugins disable my-sidecar` hoặc `PUT /api/plugins/my-sidecar/disable` | **Đóng listener hướng ngoại của plugin đó** (vô hiệu hóa ở mức năng lực); kernel và các plugin khác không bị ảnh hưởng |
| Bật nóng | `swiftmqctl plugins enable my-sidecar` | Mở lại listener của nó; nếu trước đó khởi động thất bại sẽ thử lại một lần |
| Xem trạng thái | `swiftmqctl plugins list/show` | Trạng thái + lý do thất bại/đứt kết nối |
| Kernel thoát | — | Ngắt kết nối với plugin, thu hồi phiên trên cầu, **chấm dứt tiến trình con do kernel `spawn`** |

> Tắt nóng chỉ đóng "năng lực" (cổng lắng nghe), **không** giết tiến trình plugin được kéo lên bằng `spawn`; việc thu hồi tiến trình diễn ra khi kernel thoát.

### 5.8 Cung cấp lối vào trong UI quản trị (tùy chọn)

Khi plugin có sẵn giao diện thao tác, thêm một `console_url` (địa chỉ UI quản trị, các trường còn lại xem §5.1) vào đoạn `plugins.<tên plugin>`.
Dưới đây chỉ vẽ mục `plugins.my-sidecar` (**JSON chuẩn, không được có chú thích**; nội dung đoạn `sidecar` giống §5.1):

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

- Trang **Quản lý plugin** của UI quản trị (dữ liệu đến từ trường `console_url` của `GET /api/plugins`) sẽ hiển thị
  nút「Mở giao diện quản trị」cho những plugin như vậy, **mở trong tab mới**.
- Khi chưa khai báo `console_url`, nút không khả dụng, khi rê chuột sẽ hiện gợi ý「Plugin này không cung cấp giao diện quản trị」.
- Nó chỉ là **siêu dữ liệu do bên triển khai ghi vào cấu hình**: không thuộc API plugin (`pkg/plugin`), không tham gia việc khởi động-dừng plugin,
  bản thân giao diện do plugin tự host (có thể ngay trong tiến trình plugin, hoặc ở bất kỳ dịch vụ độc lập nào).

---

## 6. Vòng đời và ma trận chịu lỗi

| Tình huống | Hành vi của kernel | Ảnh hưởng phía plugin |
| --- | --- | --- |
| Tiến trình plugin chưa khởi động / bắt tay bị từ chối | Thử lại kết nối trong 8s, nếu vẫn thất bại thì đánh dấu `failed` và cô lập (không chặn kernel khởi động) | Không |
| Tiến trình do `spawn` kéo lên thoát | Ghi log; đánh dấu `down`; thử lại kết nối / kéo lại theo chính sách `restart` với backoff | Tiến trình mới bắt tay lại |
| Tiến trình plugin sập (thời gian chạy) | Kernel không bị ảnh hưởng; `down` + kết nối lại với backoff | `pkg/sidecar.Server` sẽ đóng kết nối đó |
| Kernel bị `kill -9` | — | Phía plugin dựa vào timeout nhàn rỗi (mặc định 24s không có frame) để tự thu hồi kết nối, không để lại tiến trình zombie |
| Kernel thoát bình thường | Gọi `Stop`: ngắt kết nối, thu hồi delivery chưa quyết toán (theo hướng đưa về hàng đợi), `Kill` tiến trình con | Nhận SIGKILL |
| Delivery trong lúc kết nối plugin bị đứt | Delivery chưa quyết toán đều được **đưa trở lại hàng đợi**, không bị mất | — |
| Client ngắt kết nối / `Open` trả về | Đóng luồng tương ứng, giải phóng phiên và consumer của luồng đó | `Stream.Read` trả về EOF |

---

## 7. Ranh giới đỏ và giới hạn đã biết

**Ranh giới đỏ**

1. Plugin chỉ được phép phụ thuộc vào `pkg/sidecar` (và `pkg/plugin` tùy chọn); **không được** phụ thuộc vào `internal/**` của kernel.
2. Tên plugin phải khớp với cấu hình, `APIVersion` phải khớp với kernel, nếu không thì không thể kết nối (điều này để phòng "chạy âm thầm mà không có hiệu lực").
3. Khi dùng `session.*`: **`core.authenticate` trước, `session.open` sau**, mỗi delivery **được quyết toán đúng một lần**.
4. `protocols[].prefix` phải khác rỗng, nếu không kết nối sẽ không được giao cho plugin (xem §5.3).
5. Việc từ chối trong `Hello` phải **trả về error rõ ràng** (đừng im lặng) —— nếu không kernel chỉ thấy "kết nối bị đóng", không định vị được nguyên nhân.

**Giới hạn đã biết**

- **Sniffing nằm ở phía kernel**: plugin tiến trình ngoài không thể tự định nghĩa hàm sniff, chỉ có thể khớp bằng `prefix` (ASCII, ≤ 8 byte);
  `prefix` rỗng tức là "không lấy được kết nối" (xem §5.3).
- **Mặt dữ liệu đi qua proxy cục bộ**: không truyền fd (Windows không có `SCM_RIGHTS`), tốn thêm một lần sao chép bộ nhớ so với trong tiến trình;
  mỗi lời gọi ngược cũng tốn thêm một RPC cục bộ.
- **Kiểu của bảng thuộc tính bị suy giảm**: `Properties.Headers` được trung chuyển qua JSON, sự phân biệt kiểu như `int32` / `double` bị mất (xem §3.4).
- **Backpressure của một luồng ảnh hưởng cả kết nối**: khi bộ đệm nhận của một luồng đầy sẽ chặn goroutine phân phối của kết nối đó; giới hạn tốc độ theo luồng là tối ưu về sau.
- **Xác thực thất bại chỉ truyền qua văn bản**: lỗi xác thực của kernel là `*plugin.AuthError` (không cùng một bộ phân loại với `plugin.ErrorKind`),
  khi đến plugin qua cầu chỉ có văn bản, plugin cần ánh xạ thành mã lỗi giao thức theo quy ước của riêng nó.
- **Chỉ có năng lực `net.listen` thực sự có hiệu lực**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` là **vị trí dự phòng**, khai báo vào chỉ tham gia kiểm toán (xem phần hiển thị quản trị ở §5.6), hiện chưa có điểm mở rộng tương ứng.

---

## 8. FAQ xử lý sự cố

| Hiện tượng | Nguyên nhân và cách xử lý |
| --- | --- |
| Trạng thái `failed`, lý do có "tên plugin không khớp" | Tên plugin trong cấu hình ≠ `HelloAck.name`; sửa cho khớp |
| Trạng thái `failed`, lý do có "phiên bản API không khớp" | `HelloAck.api_version` ≠ `APIVersion` của kernel; sửa cho khớp |
| Trạng thái `failed`, lý do có "từ chối bắt tay" | `Hello` của plugin trả về error (`deny`); xem output plugin được chuyển tiếp trong log kernel |
| Trạng thái `failed`, lý do có "kết nối plugin tiến trình ngoài thất bại" | Tiến trình chưa lên / `address` ghi sai / đường dẫn socket không ghi được (trong container chú ý quyền của người dùng `swiftmq`) |
| Trạng thái `down` | Tiến trình plugin sập hoặc kết nối đứt; `restart=always` sẽ tự kết nối lại, `never` cần kéo lên thủ công |
| Cổng không mở / client không kết nối được | `protocols[].listeners` chưa cấu hình hoặc địa chỉ bị `listeners.<tên giao thức>` ghi đè; đối chiếu cả hai nơi |
| Client kết nối tới cổng khác rồi bị ngắt ngay | Cổng đó không khớp giao thức của bạn (`prefix` rỗng hoặc tiền tố không khớp); cấu hình `prefix` khác rỗng cho giao thức (xem §5.3) |
| Báo `ACCESS_REFUSED - ... for user ''` | Trước cầu ngữ nghĩa **chưa xác thực**; gọi `core.authenticate` trước rồi `session.open` |
| Gọi ngược báo "luồng N chưa mở phiên" | Làm `core.authenticate` trước, rồi `session.open`, sau đó mới gọi được các `session.*` khác |
| Không nhận được delivery tiêu thụ | Delivery đến `Call` của bạn dưới dạng **lời gọi xuôi** `session.deliver`; xác nhận đã xử lý phương thức đó |
| Plugin ở ngoài container, kernel ở trong container, không kết nối được | `address` dùng `tcp://host.docker.internal:<port>` (hoặc đưa plugin vào container luôn, dùng tên dịch vụ); plugin cần lắng nghe `0.0.0.0` |

---

## 9. Tham khảo (chỉ mục mã nguồn)

| Muốn xem gì | Tệp |
| --- | --- |
| Giao thức đường dây và hiện thực hai phía (**bắt buộc đọc khi phát triển**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frame), `proto.go` (bản tin), `server.go` (phía plugin), `client.go` (phía kernel), `bridge.go` (hợp đồng `session.*`), `stream.go` (luồng) |
| Host sidecar phía kernel (tích hợp/kết nối lại/proxy/trạng thái) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Cầu ngữ nghĩa phía kernel (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Kiểu mặt thao tác phiên của kernel (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Lắng nghe, sniffing, bật/tắt nóng theo plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Vòng đời và quản trị plugin (cô lập/trạng thái/kiểm toán) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Mục cấu hình và ví dụ (gồm đoạn sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Lắp ghép tiến trình (sidecar được lắp vào kernel như thế nào) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Hiện thực tham chiếu bằng Go (dùng `pkg/sidecar.Server`, gồm cầu `session.*` và `core.authenticate`) | Dự án kiểm thử độc lập `swiftmq-test/test/integration/echosidecar/` |
| **Hướng dẫn theo ngôn ngữ + dự án ví dụ** | Thư mục này `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; ví dụ ở **workspace** `swiftmq-plugin/{python,nodejs,php,java}/` |
