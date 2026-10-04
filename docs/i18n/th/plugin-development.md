# คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก (sidecar) ของ SwiftMQ

> **กลุ่มเป้าหมาย**: นักพัฒนาที่ไม่ต้องการ fork หรือคอมไพล์เคอร์เนลใหม่ แต่ต้องการขยายความสามารถให้ SwiftMQ ด้วย**ภาษาใดก็ได้**
> **ขอบเขต**: เอกสารนี้อธิบายรูปแบบปลั๊กอินเพียงแบบเดียว —— **ปลั๊กอินโปรเซสภายนอก** (คำศัพท์ของเคอร์เนลคือ `sidecar`) ปลั๊กอินโปรโตคอลที่บิลต์อินมากับเคอร์เนล (AMQP 0-9-1 / MQTT) ไม่อยู่ในขอบเขตของเอกสารนี้
> **วิธีอ่าน**: ส่วนที่ 1–2 สร้างแบบจำลองทางความคิด ส่วนที่ 3 คือการเขียนโค้ด และ**ส่วนที่ 5 คือ "หลังพัฒนาเสร็จจะเชื่อมเข้ามารันร่วมกันและให้บริการภายนอกได้อย่างไร"**
> **หากใช้ภาษาอื่น (Python / Node.js / PHP / Java) โปรดดูคู่มือแยกตามภาษาใน §4** (แต่ละฉบับมีโปรเจกต์ตัวอย่างที่ทดสอบรันผ่านจริงครบถ้วน)
> โค้ดในเอกสารนี้เป็นโครงร่างขั้นต่ำที่รันได้ สามารถคัดลอกเป็นจุดเริ่มต้นได้เลย ภาษาจีนตัวย่อเป็นภาษาต้นฉบับ

---

## 1. มันคืออะไร

**โปรเซสอิสระ** ตัวหนึ่ง ที่ implement "โปรโตคอล" บางอย่าง (แยกวิเคราะห์สตรีมไบต์จากไคลเอนต์) ภายในโปรเซสของตัวเอง
เคอร์เนลทำหน้าที่เป็นโฮสต์ให้ตามคอนฟิก: **พอร์ตเปิดโดยเคอร์เนล การเชื่อมต่อถูกพร็อกซีโดยเคอร์เนล** ส่วนการลงทะเบียน / เริ่ม-หยุด / ตรวจสอบ / แยกตัว ล้วนใช้กลไกเดิมที่มีอยู่ในเคอร์เนล

ก่อนอื่น ให้สร้างแบบจำลองทางความคิดที่ถูกต้องสามข้อ (จุดที่มักเข้าใจผิดมากที่สุด):

1. **โปรเซสปลั๊กอินคือ "บริการภายในเครื่อง"**: มันรับฟังเพียง**ที่อยู่ภายในเครื่อง** (TCP หรือ unix socket) และรอ**ให้เคอร์เนลมาเชื่อมต่อ**
   ทิศทางของการเชื่อมต่อคือ **เคอร์เนล (ไคลเอนต์) → ปลั๊กอิน (เซิร์ฟเวอร์)** และการจับมือก็เป็นฝ่ายเคอร์เนลที่ส่งก่อนเช่นกัน
2. **พอร์ตธุรกิจภายนอกไม่ได้เปิดโดยปลั๊กอิน**: เปิดโดย**เคอร์เนล**ตาม `protocols[].listeners` ในคอนฟิก แล้วแมปออกมาให้ไคลเอนต์
   ไคลเอนต์เชื่อมต่อกับ**พอร์ตของเคอร์เนล** ไบต์ถูกพร็อกซีโดยเคอร์เนลไปยังโปรเซสปลั๊กอิน โปรเซสปลั๊กอิน**ไม่จำเป็น**ต้องเปิดพอร์ตธุรกิจเอง
3. **ความหมายเชิงความหมายเป็นทางเลือก**: ปลั๊กอินอาจเพียง "ขนย้ายไบต์" (implement โปรโตคอลเองทั้งหมด)
   หรือจะเข้าถึงความหมายเชิงความหมายของเคอร์เนล (คิว / การกำหนดเส้นทาง / สิทธิ์ / การยืนยัน) ผ่าน**การเรียกย้อนกลับ** `session.*` ก็ได้
   ซึ่งเป็น**ชุดความหมายเชิงความหมายเดียวกันทั้งหมด**กับปลั๊กอินโปรโตคอลที่บิลต์อิน (ดังนั้น vhost สิทธิ์ การกำหนดเส้นทาง และพฤติกรรมของคิวจะไม่แตกแขนง)

| ข้อดี | ต้นทุน |
| --- | --- |
| ขยายได้โดยไม่แก้เคอร์เนล ไม่ต้องคอมไพล์เคอร์เนลใหม่ | ระนาบข้อมูลมีการคัดลอกไบต์ภายในเครื่องเพิ่มขึ้นหนึ่งครั้ง (พร็อกซีโดยเคอร์เนล ไม่มีการส่ง fd สอดคล้องกันข้ามแพลตฟอร์ม) |
| implement ด้วยภาษาใดก็ได้ (เพียงต้อง implement โปรโตคอลสายได้) | การเรียกย้อนกลับแต่ละครั้งมี RPC ภายในเครื่องเพิ่มขึ้นหนึ่งครั้ง (เข้ารหัส/ถอดรหัส JSON + คัดลอก) |
| ปลั๊กอินเผยแพร่ / อัปเกรด / รีสตาร์ทได้อย่างอิสระ | การดักจับ (sniff) อยู่ฝั่งเคอร์เนล: ถูกระบุได้เพียงตาม "คำนำหน้า" หรือ "พอร์ตเฉพาะ" |
| การคราชส่งผลกระทบเพียงปลั๊กอินนั้น: เคอร์เนลทำเครื่องหมาย `down` ไม่ออกไม่คราช | มีเพียงความสามารถ `net.listen` ที่มีผลจริง ค่าความสามารถอื่นเป็นช่องสงวน (ดู §7) |

---

## 2. หลักการทำงาน

การสร้างการเชื่อมต่อ (**เคอร์เนลคือไคลเอนต์ ปลั๊กอินคือเซิร์ฟเวอร์**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

การจับมือ:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

เริ่มให้บริการ:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**วงจรชีวิต** (โฮสต์ `internal/plugin/sidecar` ฝั่งเคอร์เนล):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**ความหมายของสถานะ** (ดูได้ด้วย `swiftmqctl plugins show`):

| สถานะ | ความหมาย | การดำเนินการของฝ่ายปฏิบัติการ |
| --- | --- | --- |
| `enabled` | เชื่อมต่อแล้วและกำลังให้บริการ | — |
| `failed` | **ตอนสตาร์ทก็ไม่ขึ้นมาเลย** (คอนฟิกผิด / การจับมือถูกปฏิเสธ / ดึงโปรเซสขึ้นมาไม่ได้) | ดู `RuntimeNote` และล็อกของเคอร์เนล แก้คอนฟิกหรือซ่อมปลั๊กอิน; ต้องรีสตาร์ทเคอร์เนลจึงจะลองใหม่ |
| `down` | **เคยขึ้นมาแล้ว ตอนนี้ไม่อยู่** (โปรเซสคราช / การเชื่อมต่อขาด) | ไปดึงโปรเซสปลั๊กอินขึ้นมา; เคอร์เนลจะกู้คืนอัตโนมัติตามนโยบาย `restart` |
| `disabled` | ในคอนฟิก `enabled=false` หรือฝ่ายปฏิบัติการปิดแบบ hot | กู้คืนด้วย `plugins enable <ชื่อ>` |

---

## 3. การพัฒนา (Go)

### 3.1 สร้างโปรเจกต์

ปลั๊กอินเป็น **Go module อิสระ** พึ่งพาเพียงสองแพ็กเกจสัญญาภายนอก:

- `github.com/houzch/swiftmq/pkg/sidecar` —— โปรโตคอลสายและการ implement ฝั่งปลั๊กอิน (**จำเป็น**)
- `github.com/houzch/swiftmq/pkg/plugin` —— ใช้เฉพาะเมื่อคุณต้องใช้ชนิดอย่าง `plugin.Message` / `plugin.Error` (ทางเลือก)

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

> เมื่อใช้ `replace` ในการดีบักร่วม ปลั๊กอินกับเคอร์เนลต้องเป็น**ซอร์สโค้ดชุดเดียวกัน** มิฉะนั้นเวอร์ชัน API (`v1`) แม้ตรงกัน แต่ชนิดอาจไม่เหมือนกัน

### 3.2 implement `Handler` (สามเมธอด)

พื้นผิวธุรกิจทั้งหมดของโปรเซสปลั๊กอินคือ `Hello` / `Call` / `Open` ของ `sidecar.Handler`
การจับมือ ฮาร์ตบีต มัลติเพล็กซ์ และการแบ่งชังก์ ล้วนจัดการโดย `pkg/sidecar` คุณไม่จำเป็นต้องแตะเฟรม

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

### 3.3 ระนาบข้อมูล: อ่าน-เขียน `Stream`

`sidecar.Stream` implement `io.ReadWriteCloser` ใช้เป็น "หนึ่งการเชื่อมต่อ" ได้เลย:

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

ประเด็นสำคัญ:

- การเชื่อมต่อไคลเอนต์แต่ละรายการ = หนึ่งสตรีม; ปลั๊กอินสามารถเริ่มส่ง-รับได้ทันทีใน `Handler.Open`
- ข้อความขนาดใหญ่ถูกแบ่งชังก์อัตโนมัติโดยไลบรารี (แต่ละเฟรม ≤ 64 KiB) **การใช้หน่วยความจำไม่ขึ้นกับขนาดข้อความ**
- แบ็กเพรสเชอร์: บัฟเฟอร์รับของสตรีมเดียวมีขีดจำกัด เมื่อบัฟเฟอร์เต็มจะบล็อก**โกโรทีนกระจายงาน**ของการเชื่อมต่อนั้น (ทุกสตรีมรอพร้อมกัน) ——
  นี่คือการแลกระหว่าง "หน่วยความจำที่คาดเดาได้" กับ "การจำกัดความเร็วต่อสตรีม" ดูรายละเอียดที่ §7

### 3.4 ใช้บริดจ์ความหมายเชิงความหมายของเคอร์เนล (`session.*`)

หากต้องการให้ปลั๊กอินนำความหมายเชิงความหมายของคิว / การกำหนดเส้นทาง / สิทธิ์ / การยืนยัน ของเคอร์เนลกลับมาใช้ซ้ำ (แทนที่จะสร้างชุดของตัวเอง) ให้ใช้**การเรียกย้อนกลับ**
บนสตรีม ให้ใช้ตามลำดับ `session.open` → `session.*` อื่น ๆ → (`session.close`):

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

**สรุปเมธอดการเรียกย้อนกลับ** (ค่าคงที่ใน `pkg/sidecar/bridge.go` → ชื่อบนสาย):

| กลุ่ม | ชื่อบนสาย (ค่าคงที่) | คำอธิบาย |
| --- | --- | --- |
| การยืนยันตัวตน | `core.authenticate` (`MethodCoreAuthenticate`) | **ต้องทำก่อน**: ส่งข้อมูลรับรองจากโปรโตคอลให้เคอร์เนลตรวจสอบ; พารามิเตอร์ `{stream, mechanism, response}` คืนค่า `{user}` |
| เซสชัน | `session.open` (`MethodSessionOpen`) | เปิดเซสชันของ vhost หนึ่งบนสตรีม; ทำได้**หลังการยืนยันตัวตน**เท่านั้น |
| | `session.close` (`MethodSessionClose`) | ปล่อยเซสชันบนสตรีมนั้น (ยกเลิกผู้บริโภค ลบคิวแบบเอกสิทธิ์) |
| เอ็กซ์เชนจ์ | `session.declare_exchange` / `session.delete_exchange` | เพิ่ม/ลบ (ประกาศแบบ passive ที่ไม่มีอยู่ → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | การผูกเอ็กซ์เชนจ์กับเอ็กซ์เชนจ์ |
| คิว | `session.declare_queue` / `session.delete_queue` | เพิ่ม/ลบ; เมื่อ `name` ว่างเซิร์ฟเวอร์จะสร้างให้ |
| | `session.bind_queue` / `session.unbind_queue` | การผูกคิวกับเอ็กซ์เชนจ์ |
| | `session.purge_queue` | ล้างข้อความที่พร้อม (ไม่รวมที่ยังไม่ยืนยัน) |
| เผยแพร่ | `session.publish` | คืนค่า `{routed, rejected}`; การทำให้คงทนเสร็จสิ้นก่อนการตอบกลับ |
| การดึง | `session.get` | ดึงหนึ่งข้อความเชิงรุก; `found=false` หมายถึงคิวว่าง |
| การบริโภค | `session.consume` / `session.cancel` | ลงทะเบียน / ยกเลิกผู้บริโภค |
| การเคลียร์ | `session.settle` | เคลียร์การนำส่งหนึ่งรายการ (`ack` / `requeue` / `reject`) |
| **ไปข้างหน้า** | `session.deliver` | **เคอร์เนล → ปลั๊กอิน**: ดันการนำส่งกลับ (จัดการใน `Handler.Call` ของคุณ) |

**สี่ข้อตกลงที่ต้องปฏิบัติตาม**:

1. **`core.authenticate` ก่อน**: ระนาบการดำเนินการของเคอร์เนลของการเชื่อมต่อไม่มีตัวตนก่อนการยืนยันตัวตน
   ตอนนี้ `session.open` จะถูกปฏิเสธ (`ACCESS_REFUSED - access to vhost '/' refused for user ''`)
   ปลั๊กอินมีหน้าที่ดึงข้อมูลรับรองจากโปรโตคอลของตัวเอง; ลอจิกการยืนยันตัวตนและตารางผู้ใช้ยังอยู่ที่เคอร์เนล ปลั๊กอินไม่แตะต้องที่เก็บรหัสผ่าน
2. **จากนั้น `session.open`**: หากยังไม่เปิดเซสชันแล้วเรียกเมธอดอื่น เคอร์เนลจะคืน `KindPreconditionFailed` ("สตรีม N ยังไม่ได้เปิดเซสชัน")
3. **การนำส่งแต่ละรายการเคลียร์เพียงครั้งเดียวพอดี**: เลือกหนึ่งใน `Ack` / `Requeue` / `Reject`
   `Ack` และ `Reject` ต่างทิ้งข้อความ **มีเพียง `Reject` ที่ไปเส้นทางเดดเลตเทอร์**
4. **การนำส่งที่ยังไม่เคลียร์จะไม่หาย**: เมื่อสตรีมสิ้นสุด (ไคลเอนต์ตัดการเชื่อมต่อ / `Handler.Open` คืนค่า) หรือการเชื่อมต่อปลั๊กอินขาด
   เคอร์เนลจะจัดการการนำส่งที่ยังไม่เคลียร์ทั้งหมด**เป็น "กลับเข้าคิว"** เพื่อหลีกเลี่ยงข้อความค้างคา

**การคืนสภาพข้อผิดพลาด**: `*plugin.Error` ของเคอร์เนลมาถึงผ่านบริดจ์ในรูป `*sidecar.RPCError` (ฟิลด์ `Kind` / `Text`)
สามารถคืนสภาพเป็น `plugin.Error` ที่มีหมวดหมู่ได้ แทนที่จะทิ้งหมวดหมู่ไว้ในสตริง:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

การแมป `Kind` ที่พบบ่อยกับโปรโตคอลที่บิลต์อิน (เพื่อให้คุณตัดสินใจว่าจะตอบข้อผิดพลาดให้ไคลเอนต์อย่างไร):

| `plugin.ErrorKind` | ความหมายเชิงความหมาย | การแมป AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | ออบเจ็กต์ไม่มีอยู่ | 404 NOT_FOUND (ปิด Channel) |
| `KindPreconditionFailed` | พารามิเตอร์ไม่สอดคล้องกับออบเจ็กต์ที่มีอยู่ / เซสชันยังไม่เปิด | 406 PRECONDITION_FAILED (ปิด Channel) |
| `KindAccessRefused` | สิทธิ์ไม่เพียงพอ / ชื่อสงวน | 403 ACCESS_REFUSED (ปิด Channel) |
| `KindResourceLocked` | ทรัพยากรแบบเอกสิทธิ์ถูกครอบครอง | 405 RESOURCE_LOCKED (ปิด Channel) |
| `KindInvalidPath` | vhost ไม่มีอยู่ | 402 INVALID_PATH (ปิดการเชื่อมต่อ) |
| `KindNotImplemented` | ความสามารถยังไม่ถูก implement | 540 NOT_IMPLEMENTED (ปิดการเชื่อมต่อ) |
| `KindInternal` | ข้อผิดพลาดภายในเคอร์เนล | 541 INTERNAL_ERROR (ปิดการเชื่อมต่อ) |

**ขอบเขตความเที่ยงตรงของชนิดข้อความ**: ตารางคุณสมบัติ (`MessageDTO.Properties.Headers`) ถูกส่งต่อผ่าน JSON
จึง**ไม่ได้ข้อมูลชนิดตัวเลขที่แยก `int32` / `double`** เหมือน AMQP field-table
เมื่อต้องการความเที่ยงตรงของชนิดอย่างเข้มงวด ให้ใส่ข้อมูลประเภทนี้ไว้ในเนื้อข้อความ (ไบต์ดิบ) แล้วรับผิดชอบเอง

### 3.5 สถานะและล็อกฝั่งปลั๊กอิน

- **สถานะของปลั๊กอินภายนอกถูกกำหนดโดย "การเชื่อมต่อยังอยู่หรือไม่"** ปลั๊กอินไม่ต้องรายงานเอง (`StateReporter` ของปลั๊กอินบิลต์อินใช้กับโปรเซสภายนอกไม่ได้)
- ล็อก: `sidecar.ServerOptions.Logger` ส่งออกไปยัง stdout/stderr ของโปรเซสปลั๊กอิน;
  **เมื่อถูกดึงขึ้นมาโดย `spawn` ของเคอร์เนล เอาต์พุตเหล่านี้จะถูกเคอร์เนลส่งต่อไปยังล็อกของเคอร์เนล** (มีแท็ก `plugin`) สะดวกต่อการเก็บรวบรวมแบบรวมศูนย์
- เมื่อดีพลอยแบบแยกส่วน (ไม่ใช่ spawn) ให้เก็บล็อกปลั๊กอินตามวิธีของคุณเอง

### 3.6 ทดสอบด้วยตัวเองได้โดยไม่ต้องต่อกับเคอร์เนล

`Handler` เป็นอินเทอร์เฟซ Go ธรรมดา ในยูนิตเทสต์สามารถสร้างอินสแตนซ์และเรียก `Hello` / `Call` / `Open` ได้โดยตรงเพื่อครอบคลุมลอจิกธุรกิจ โดยไม่ต้องเปิดเครือข่าย:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

การตรวจสอบแบบ end-to-end ให้ดู §5

---

## 4. การพัฒนาด้วยภาษาอื่น (สเปกโปรโตคอลสาย)

`pkg/sidecar` เป็นสัญญาภายนอกที่ไม่มีการพึ่งพา; ตัวโปรโตคอลสายนั้นเรียบง่าย ภาษาใดก็ implement ได้
หากต้องการเชื่อมต่อ คุณต้อง implement ข้อตกลง "ระดับไบต์" ต่อไปนี้ (ดูซอร์สโค้ดที่ `pkg/sidecar/frame.go`, `proto.go`)

> **มีคู่มือแยกตามภาษาพร้อมโปรเจกต์ตัวอย่างครบถ้วนให้แล้ว** (ตัวอย่างทั้งหมดทดสอบรันผ่านจริง: การจับมือ → การยืนยันตัวตน → บริดจ์ความหมายเชิงความหมาย → การนำส่ง/เคลียร์ → สตรีมไบต์):
>
> | ภาษา | คู่มือ | โปรเจกต์ตัวอย่าง (เวิร์กสเปซ `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (เฉพาะไลบรารีมาตรฐาน) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (เฉพาะไลบรารีมาตรฐาน) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (เฉพาะไลบรารีมาตรฐาน) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (ไฟล์เดียว เฉพาะ JDK) |
>
> สำหรับการ implement อ้างอิงฉบับเต็มในภาษา Go ดูโปรเจกต์ทดสอบแยกต่างหาก `swiftmq-test/test/integration/echosidecar/` (มันใช้ `pkg/sidecar.Server` โดยตรง
> ไม่ต้องสนใจรายละเอียดระดับไบต์ด้านล่าง)

**รูปแบบเฟรม** (ทุกเฟรมเหมือนกัน):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**ชนิดเฟรม `kind`**:

| kind | ชื่อ | ทิศทาง | เพย์โหลด |
| --- | --- | --- | --- |
| 1 | Hello | เคอร์เนล → ปลั๊กอิน | JSON `Hello` |
| 2 | HelloAck | ปลั๊กอิน → เคอร์เนล | JSON `HelloAck` |
| 3 | Ping | เคอร์เนล → ปลั๊กอิน | ว่าง |
| 4 | Pong | ปลั๊กอิน → เคอร์เนล | ว่าง |
| 5 | Call | สองทิศทาง | JSON `Call` |
| 6 | Reply | สองทิศทาง | JSON `Reply` |
| 7 | Open | เคอร์เนล → ปลั๊กอิน | JSON `Open` |
| 8 | OpenAck | ปลั๊กอิน → เคอร์เนล | JSON `OpenAck` |
| 9 | Data | สองทิศทาง | `u32 BE stream` + ไบต์ดิบ |
| 10 | Close | สองทิศทาง | JSON `Close` |

**โครงสร้าง JSON ของระนาบควบคุม** (ชื่อฟิลด์ตรงกับ `proto.go`)

> ส่วนนี้เป็น**ตัวอย่างข้อความโปรโตคอลสาย** (ในบล็อกเดียวกันให้ข้อความหลายรายการตามลำดับ จึงใช้ `//` คั่นคำอธิบาย)
> **ไม่ใช่คอนฟิกที่เขียนลง `swiftmqd.json` ได้โดยตรง**

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

**ความหมายเชิงความหมายที่ต้องปฏิบัติตาม**:

- **การจับมือ**: เคอร์เนลส่ง `Hello` ก่อน ปลั๊กอินต้องตอบกลับหนึ่งเฟรม `HelloAck`
  เคอร์เนลจะตรวจสอบ `HelloAck.name == ชื่อปลั๊กอินในคอนฟิก` และ `HelloAck.api_version == APIVersion ของเคอร์เนล`;
  `deny` ที่ไม่ว่างถือเป็นการปฏิเสธการเชื่อมต่อ (ปลั๊กอินถูกแยก)
- **ฮาร์ตบีต**: โดยค่าเริ่มต้นเคอร์เนลส่ง `Ping` ทุก 2 วินาที ปลั๊กอินต้องตอบ `Pong` ภายใน 8 วินาที; ฝั่งปลั๊กอินหากไม่ได้รับเฟรมใด ๆ ภายใน 24 วินาทีสามารถปิดการเชื่อมต่อเองได้
- **สองเนมสเปซของ ID**: `reverse` ของ `Call` แยกทิศทาง สองทิศทางเพิ่มขึ้นจาก 1 อย่างอิสระต่อกัน
  ดังนั้น `Reply` **ต้องแนบ `reverse` กลับมาด้วย** มิฉะนั้นการตอบกลับจะถูกส่งไปยังผู้รอที่ผิด
- **ระนาบข้อมูลไม่ใช้ base64**: ข้อมูลก้อนใหญ่เช่นเนื้อข้อความใส่ลงเพย์โหลดของเฟรม `Data` โดยตรง (`stream` + ไบต์ดิบ) แบ่งชังก์ตามต้องการ

> หากใช้ Go ให้ใช้ `pkg/sidecar` โดยตรง รายละเอียดข้างต้นไม่ต้อง implement เอง

---

## 5. นำมารันร่วมกัน: การเชื่อมต่อ การให้บริการภายนอก การแพ็กเกจ ★

ส่วนนี้อธิบาย "หลังพัฒนาเสร็จจะเชื่อมเข้า SwiftMQ และให้บริการภายนอกได้อย่างไร"

### 5.1 ประกาศปลั๊กอินในคอนฟิก

ปลั๊กอินภายนอก**ถูกดูแลจัดการด้วยคอนฟิกทั้งหมด** เคอร์เนลไม่ต้องแก้โค้ดใด ๆ เพื่อมัน ให้เพิ่มรายการในส่วน `plugins` ของ `swiftmqd.json`
(**คอนฟิกจริงเป็น JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**):

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

คำอธิบายรายข้อ (รายการฟิลด์ดูตารางด้านล่าง):

- ชื่อคีย์ของ `plugins.<ชื่อปลั๊กอิน>` **ต้องตรงกับ `HelloAck.name` ที่ปลั๊กอินรายงานเอง** มิฉะนั้นการจับมือจะถูกปฏิเสธ
- `builtin: false`: ประกาศปลั๊กอินภายนอกอย่างชัดเจน (ถ้าไม่เขียน ฝ่ายจัดการจะแสดงเป็นบิลต์อิน)
- `enabled`: ปิดมัน = ไม่ดึงโปรเซส ไม่สร้างตัวรับฟัง
- เมื่อ `required: true` การสตาร์ทล้มเหลวจะบล็อกการสตาร์ทเคอร์เนล —— ปลั๊กอินภายนอกไม่ควรเปิด
- `address` คือ**ที่อยู่ที่เคอร์เนลไปเชื่อมต่อ** (เคอร์เนลเป็นไคลเอนต์); เมื่อ `spawn` ไม่ว่าง เคอร์เนลจะดึงโปรเซสขึ้นมาแทน
- `protocols[].prefix` **ต้องไม่ว่าง** (กฎการดักจับดู §5.3)
- `listeners` คือ**พอร์ตภายนอกของโปรโตคอลนั้น เปิดโดยเคอร์เนล** (ไคลเอนต์เชื่อมต่อกับเคอร์เนล)

รายการฟิลด์:

| ฟิลด์ | จำเป็น | คำอธิบาย |
| --- | --- | --- |
| `sidecar.address` | ✅ | ที่อยู่โปรเซสปลั๊กอิน: `tcp://host:port` หรือ `unix:///path` |
| `sidecar.spawn` | ✕ | บรรทัดคำสั่งที่เคอร์เนลรันแทน (รายการแรกคือไฟล์เรียกทำงาน); **เว้นว่าง = เคอร์เนลเชื่อมต่ออย่างเดียวไม่ดึงขึ้นมา** โปรเซสคุณจัดการเอง |
| `sidecar.restart` | ✕ | `always` (ค่าเริ่มต้น กู้คืนอัตโนมัติหลังขาดการเชื่อมต่อ/คราช) หรือ `never` (ทำเครื่องหมาย `down` เท่านั้น รอการแทรกแซงจากฝ่ายปฏิบัติการ) |
| `sidecar.protocols[].name` | ✅ | ชื่อโปรโตคอล (ไม่ซ้ำทั้งระบบ มีส่วนในลำดับความสำคัญของการดักจับ) |
| `sidecar.protocols[].prefix` | ✕ | คำนำหน้าสำหรับการดักจับ (ASCII); **ว่าง = ไม่มีส่วนในการดักจับ** |
| `sidecar.protocols[].listeners[]` | ✕ | ตัวรับฟังภายนอกของโปรโตคอลนั้น (`name` + `addr`) สร้างโดยเคอร์เนล |
| `sidecar.handshake_timeout_seconds` | ✕ | เขียนทับเวลาหมดอายุการจับมือ (ค่าเริ่มต้น 5s) |
| `sidecar.heartbeat_seconds` | ✕ | เขียนทับช่วงห่างฮาร์ตบีต (ค่าเริ่มต้น 2s) |

> **ชื่อปลั๊กอินกับชื่อโปรโตคอล**: ทั้งสอง**สามารถต่างกันได้** (เช่นปลั๊กอิน `my-sidecar` ให้โปรโตคอล `myproto`)
> การปิดแบบ hot จะค้นหาโปรโตคอลทั้งหมดที่ลงทะเบียนไว้ด้วยชื่อปลั๊กอินก่อน แล้วจึงปิดพอร์ตของโปรโตคอลเหล่านั้น จึงไม่จำเป็นต้องตั้งชื่อให้เหมือนกันโดยเจตนา

### 5.2 สามวิธีเชื่อมต่อ

| วิธีเชื่อมต่อ | คอนฟิก | เหมาะกับ |
| --- | --- | --- |
| **โฮสต์เดียวกัน + เคอร์เนลดึงขึ้นมา (spawn)** | `spawn: [...]`, `address` ชี้ไปยังที่อยู่ที่มันรับฟัง | ดีพลอยเครื่องเดียวกัน คอนเทนเนอร์เดียว; ง่ายที่สุด เคอร์เนลรับผิดชอบดึงขึ้นมาและเก็บคืน |
| **โฮสต์เดียวกัน + จัดการเอง (dial)** | `spawn: []`, `address` ชี้ไปยังโปรเซสที่รันอยู่แล้ว | ใช้ systemd / supervisor จัดการวงจรชีวิตปลั๊กอิน |
| **ข้ามโฮสต์ / ข้ามคอนเทนเนอร์ (dial ต้องเป็น tcp)** | `spawn: []`, `address: "tcp://<ชื่อบริการ>:19001"` | ปลั๊กอินกับเคอร์เนลดีพลอยแยกคอนเทนเนอร์ / แยกเครื่อง |

การเลือกที่อยู่:

- **เครื่องเดียวกันแนะนำ unix socket** (`unix:///tmp/my-sidecar.sock`): ไม่ยึดครองพอร์ต TCP ไม่ได้รับผลกระทบจากการที่พอร์ตโฮสต์ถูกใช้อยู่
  ระวังว่าเส้นทาง socket ต้องเขียนได้โดยโปรเซสเคอร์เนล (ในคอนเทนเนอร์คือผู้ใช้ `swiftmq` ที่ไม่ใช่ root)
- **ข้ามคอนเทนเนอร์ต้องเป็น TCP** และโปรเซสปลั๊กอินต้องรับฟังบน `0.0.0.0` โดย `address` ใช้**ชื่อบริการในเครือข่ายคอนเทนเนอร์**

> อย่าสลับทิศทาง: **ที่อยู่ที่ปลั๊กอินรับฟัง** = `address`; **พอร์ตที่เปิดให้ไคลเอนต์ภายนอก** = `protocols[].listeners`

### 5.3 การให้บริการภายนอก: ถูกระบุตัวตนด้วย `prefix`

เมื่อชั้นเชื่อมต่อกระจายการเชื่อมต่อ **ดูแค่ผลการดักจับ**: มันถาม `Sniff(peek)` กับทุกโปรโตคอลที่เปิดใช้งานตามลำดับการลงทะเบียน (peek สูงสุด 8 ไบต์)
ตัวที่ตรงจะเข้าครอบครองการเชื่อมต่อนี้ ดังนั้น:

1. **`prefix` ต้องไม่ว่าง** (ASCII, ≤ 8 ไบต์) เมื่อไม่กี่ไบต์แรกที่ไคลเอนต์ส่งมาตรงกับมัน การเชื่อมต่อจึงจะถูกส่งให้ปลั๊กอินของคุณ
   ตัวอย่าง: `"prefix": "PY"` → ไบต์แรกของไคลเอนต์ต้องเป็น `PY` (ถือคำนำหน้าเป็น magic header ของโปรโตคอลคุณได้)
2. **`prefix` ว่างหมายถึงไม่มีส่วนในการดักจับ**: การเชื่อมต่อประเภทนี้**จะไม่**ถูกส่งให้ปลั๊กอิน (ทดสอบจริง: การเชื่อมต่อบนพอร์ตที่รับฟังจะถูกตัดทันที)
   ดังนั้น `prefix` ว่างเหมาะเฉพาะกรณี "มีโปรโตคอลอื่นช่วยส่งต่อให้บนพอร์ตเดียวกัน" **อย่า**ใช้มันทำพอร์ตเฉพาะ
3. `listeners[].addr` กำหนด "เปิดให้ภายนอกบนพอร์ตใด" ส่วน `prefix` กำหนด "การเชื่อมต่อนี้ถือเป็นของคุณหรือไม่" ——
   ทั้งสองต้องใช้คู่กัน: **พอร์ตเฉพาะก็ต้องให้ `prefix` ที่ไม่ว่างด้วย** (นี่ก็เป็นเหตุผลที่ในคอนฟิกตัวอย่างของเคอร์เนล
   `echo-sidecar` เขียนทั้ง `prefix: "ECHO"` และ `listeners: [":1885"]`)
4. การดักจับจับคู่ตามลำดับการลงทะเบียนโปรโตคอล **ตัวที่ตรงก่อนมีผล**: เมื่อมีปลั๊กอินหลายตัวอยู่ร่วมกัน คำนำหน้าต้องมีความแตกต่าง (เช่นขึ้นต้นด้วยไบต์เดียวกันจะบดบังกัน)

### 5.4 เขียนทับที่อยู่รับฟังและ TLS

- ที่อยู่รับฟังภายนอกให้ได้**สองที่**: `sidecar.protocols[].listeners[].addr` (ค่าเริ่มต้น) และ
  `listeners.<ชื่อโปรโตคอล>` (เขียนทับทั้งหมดตามชื่อโปรโตคอล) เมื่อมีทั้งสองที่ ให้ยึด `listeners.<ชื่อโปรโตคอล>` เป็นหลัก
- เมื่อต้องใช้ TLS ให้ระบุใบรับรองใน `listeners.<ชื่อโปรโตคอล>[i].tls` (ฟิลด์เหมือนกับโปรโตคอลบิลต์อิน)
  ด้านล่างเป็นท่อน `listeners` (**JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**): รายการที่ 1 เป็นข้อความธรรมดา รายการที่ 2 ใช้ TLS

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS ถูกสิ้นสุดโดย**เคอร์เนล**ที่ฝั่งรับฟัง โปรเซสปลั๊กอินได้สตรีมข้อความธรรมดา —— ปลั๊กอินไม่จำเป็นต้องจัดการ TLS

### 5.5 การแพ็กเกจ: ให้ปลั๊กอินรันร่วมกับเคอร์เนล

**วิธี A —— บิลต์เข้าไปในอิมเมจเดียวกัน** (แนะนำสำหรับปลั๊กอินที่ "เผยแพร่พร้อมเคอร์เนล"): เพิ่มหนึ่งบรรทัดในสเตจรันไทม์ของ `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

จากนั้นในคอนฟิกใส่ `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`
`address` ค่าเดียวกัน เคอร์เนลจะดึงมันขึ้นมาตอนสตาร์ท

**วิธี B —— เมานต์ไบนารี** (ไม่แก้ไขอิมเมจ เหมาะกับการดีบักร่วม):

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

คอนฟิกใช้ unix socket (เพื่อเลี่ยงการยึดครองพอร์ตเพิ่มเติม) ด้านล่างเป็นท่อน `sidecar` ใน `plugins.my-sidecar`
(**JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**; `prefix` ยังต้องไม่ว่าง ดู §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**วิธี C —— คอนเทนเนอร์แยก** (ปลั๊กอินเผยแพร่แยก / ขยาย-ย่ออิสระ):

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

ในคอนฟิก `spawn: []` (เคอร์เนลเชื่อมต่ออย่างเดียวไม่ดึงขึ้นมา) `address: "tcp://my-sidecar:19001"` (ชื่อบริการของ compose)

### 5.6 การสตาร์ทและการตรวจสอบ

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

ใน `plugins show` ให้สนใจ `state` กับ `RuntimeNote` เป็นพิเศษ:
`failed` จะมาพร้อมสาเหตุความล้มเหลว (การจับมือถูกปฏิเสธ / ดึงพอร์ตขึ้นมาไม่ได้…); `down` จะมาพร้อมสาเหตุการขาดการเชื่อมต่อ (โปรเซสคราช / การเชื่อมต่อขาด)

### 5.7 การปฏิบัติการระหว่างรัน

| การดำเนินการ | คำสั่ง / อินเทอร์เฟซ | ผลลัพธ์ |
| --- | --- | --- |
| ปิดแบบ hot | `swiftmqctl plugins disable my-sidecar` หรือ `PUT /api/plugins/my-sidecar/disable` | **ปิดการรับฟังภายนอกของปลั๊กอินนั้น** (ปิดในระดับความสามารถ); เคอร์เนลและปลั๊กอินอื่นไม่ได้รับผลกระทบ |
| เปิดแบบ hot | `swiftmqctl plugins enable my-sidecar` | เปิดการรับฟังของมันใหม่; หากก่อนหน้านี้สตาร์ทล้มเหลวจะลองอีกครั้ง |
| ดูสถานะ | `swiftmqctl plugins list/show` | สถานะ + สาเหตุความล้มเหลว/การขาดการเชื่อมต่อ |
| เคอร์เนลออก | — | ตัดการเชื่อมต่อกับปลั๊กอิน เก็บคืนเซสชันบนบริดจ์ **ยุติโปรเซสลูกที่เคอร์เนล `spawn` ขึ้นมา** |

> การปิดแบบ hot ปิดเพียง "ความสามารถ" (พอร์ตรับฟัง) **ไม่**ฆ่าโปรเซสปลั๊กอินที่ดึงขึ้นมาด้วย `spawn`; การเก็บคืนโปรเซสเกิดตอนเคอร์เนลออก

### 5.8 ให้ทางเข้าในแบ็กเอนด์จัดการ (ทางเลือก)

เมื่อปลั๊กอินมีอินเทอร์เฟซดำเนินการของตัวเอง ให้เพิ่ม `console_url` (ที่อยู่หน้าจอจัดการ ฟิลด์อื่นดู §5.1) ในส่วน `plugins.<ชื่อปลั๊กอิน>`
ด้านล่างวาดเฉพาะรายการ `plugins.my-sidecar` (**JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**; เนื้อหาส่วน `sidecar` เหมือน §5.1):

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

- หน้า **การจัดการปลั๊กอิน** ของแบ็กเอนด์จัดการ (ข้อมูลมาจากฟิลด์ `console_url` ของ `GET /api/plugins`) จะแสดง
  ปุ่ม "เปิดหน้าจอจัดการ" สำหรับปลั๊กอินประเภทนี้ โดย**เปิดในแท็บใหม่**
- เมื่อไม่ได้ประกาศ `console_url` ปุ่มจะใช้งานไม่ได้ ทูลทิปขึ้นว่า "ปลั๊กอินนี้ไม่มีหน้าจอจัดการ"
- มันเป็นเพียง**เมตาดาตาที่ผู้ดีพลอยเขียนไว้ในคอนฟิก**: ไม่ใช่ส่วนหนึ่งของ API ปลั๊กอิน (`pkg/plugin`) ไม่มีส่วนในการเริ่ม-หยุดปลั๊กอิน
  ตัวอินเทอร์เฟซถูกโฮสต์โดยปลั๊กอินเอง (จะอยู่ในโปรเซสปลั๊กอินเลย หรือบริการอิสระใดก็ได้)

---

## 6. วงจรชีวิตและเมทริกซ์การทนทานต่อข้อผิดพลาด

| กรณี | พฤติกรรมของเคอร์เนล | ผลกระทบฝั่งปลั๊กอิน |
| --- | --- | --- |
| โปรเซสปลั๊กอินยังไม่เริ่ม / การจับมือถูกปฏิเสธ | ลองเชื่อมต่อใหม่ภายใน 8s หากยังล้มเหลวจะทำเครื่องหมาย `failed` และแยก (ไม่บล็อกการสตาร์ทเคอร์เนล) | ไม่มี |
| โปรเซสที่ `spawn` ดึงขึ้นมาออก | บันทึกล็อก; ทำเครื่องหมาย `down`; ถอยกลับเชื่อมต่อใหม่ / ดึงใหม่ตามนโยบาย `restart` | โปรเซสใหม่จับมือใหม่ |
| โปรเซสปลั๊กอินคราช (ระหว่างรัน) | เคอร์เนลไม่ได้รับผลกระทบ; `down` + ถอยกลับเชื่อมต่อใหม่ | `pkg/sidecar.Server` จะปิดการเชื่อมต่อนั้น |
| เคอร์เนลถูก `kill -9` | — | ฝั่งปลั๊กอินเก็บคืนการเชื่อมต่อเองด้วยการหมดอายุเมื่อว่าง (ค่าเริ่มต้น 24s ไม่มีเฟรม) ไม่ทิ้งซอมบี้ |
| เคอร์เนลออกตามปกติ | เรียก `Stop`: ตัดการเชื่อมต่อ เก็บคืนการนำส่งที่ยังไม่เคลียร์ (เป็นกลับเข้าคิว) `Kill` โปรเซสลูก | ได้รับ SIGKILL |
| การนำส่งระหว่างที่การเชื่อมต่อปลั๊กอินขาด | การนำส่งที่ยังไม่เคลียร์ทั้งหมด**กลับเข้าคิว** ไม่หาย | — |
| ไคลเอนต์ตัดการเชื่อมต่อ / `Open` คืนค่า | ปิดสตรีมที่เกี่ยวข้อง ปล่อยเซสชันและผู้บริโภคของสตรีมนั้น | `Stream.Read` คืนค่า EOF |

---

## 7. ข้อห้ามเด็ดขาดและขอบเขตที่ทราบ

**ข้อห้ามเด็ดขาด**

1. ปลั๊กอินอนุญาตให้พึ่งพา `pkg/sidecar` เท่านั้น (และ `pkg/plugin` ที่เป็นทางเลือก); **ห้าม**พึ่งพา `internal/**` ของเคอร์เนล
2. ชื่อปลั๊กอินต้องตรงกับคอนฟิก `APIVersion` ต้องตรงกับเคอร์เนล มิฉะนั้นจะเชื่อมต่อไม่ได้ (นี่คือการกัน "รันอยู่เงียบ ๆ แต่ไม่มีผล")
3. เมื่อใช้ `session.*`: **`core.authenticate` ก่อน แล้ว `session.open`** การนำส่งแต่ละรายการ**เคลียร์เพียงครั้งเดียวพอดี**
4. `protocols[].prefix` ต้องไม่ว่าง มิฉะนั้นการเชื่อมต่อจะไม่ถูกส่งให้ปลั๊กอิน (ดู §5.3)
5. การปฏิเสธใน `Hello` ต้อง**คืน error อย่างชัดเจน** (อย่าเงียบ) —— มิฉะนั้นเคอร์เนลจะเห็นแค่ "การเชื่อมต่อถูกปิด" หาสาเหตุไม่ได้

**ขอบเขตที่ทราบ**

- **การดักจับอยู่ฝั่งเคอร์เนล**: ปลั๊กอินภายนอกกำหนดฟังก์ชันดักจับเองไม่ได้ ทำได้เพียงจับคู่ด้วย `prefix` (ASCII, ≤ 8 ไบต์);
  `prefix` ว่างคือ "ไม่ได้การเชื่อมต่อ" (ดู §5.3)
- **ระนาบข้อมูลผ่านพร็อกซีภายในเครื่อง**: ไม่มีการส่ง fd (Windows ไม่มี `SCM_RIGHTS`) มีการคัดลอกหน่วยความจำเพิ่มขึ้นหนึ่งครั้งเมื่อเทียบกับในโปรเซส;
  การเรียกย้อนกลับแต่ละครั้งก็มี RPC ภายในเครื่องเพิ่มขึ้นหนึ่งครั้ง
- **ชนิดในตารางคุณสมบัติจะเสื่อมสภาพ**: `Properties.Headers` ถูกส่งต่อผ่าน JSON การแยกชนิดอย่าง `int32` / `double` จึงสูญหาย (ดู §3.4)
- **แบ็กเพรสเชอร์ของสตรีมเดียวกระทบทั้งการเชื่อมต่อ**: เมื่อบัฟเฟอร์รับของสตรีมหนึ่งเต็ม จะบล็อกโกโรทีนกระจายงานของการเชื่อมต่อนั้น; การจำกัดความเร็วต่อสตรีมเป็นการปรับปรุงในอนาคต
- **การยืนยันตัวตนล้มเหลวส่งผ่านเพียงข้อความ**: การยืนยันตัวตนของเคอร์เนลล้มเหลวเป็น `*plugin.AuthError` (ไม่ใช่ชุดหมวดหมู่เดียวกับ `plugin.ErrorKind`)
  เมื่อมาถึงปลั๊กอินผ่านบริดจ์มีเพียงข้อความ ปลั๊กอินต้องแมปเป็นรหัสข้อผิดพลาดของโปรโตคอลตามข้อตกลงของตัวเอง
- **มีเพียงความสามารถ `net.listen` ที่มีผลจริง**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` เป็น**ช่องสงวน** เมื่อประกาศแล้วมีส่วนเพียงการตรวจสอบ (ดูการแสดงผลการกำกับดูแลใน §5.6) ปัจจุบันไม่มีจุดขยายที่รองรับ

---

## 8. การแก้ไขปัญหา FAQ

| อาการ | สาเหตุและการจัดการ |
| --- | --- |
| สถานะ `failed` สาเหตุมี "ชื่อปลั๊กอินไม่ตรงกัน" | ชื่อปลั๊กอินในคอนฟิก ≠ `HelloAck.name`; แก้ให้ตรง |
| สถานะ `failed` สาเหตุมี "เวอร์ชัน API ไม่ตรงกัน" | `HelloAck.api_version` ≠ `APIVersion` ของเคอร์เนล; แก้ให้ตรง |
| สถานะ `failed` สาเหตุมี "ปฏิเสธการจับมือ" | `Hello` ของปลั๊กอินคืน error (`deny`); ดูเอาต์พุตปลั๊กอินที่ถูกส่งต่อในล็อกเคอร์เนล |
| สถานะ `failed` สาเหตุมี "เชื่อมต่อปลั๊กอินภายนอกล้มเหลว" | โปรเซสไม่ขึ้นมา / `address` เขียนผิด / เส้นทาง socket เขียนไม่ได้ (ในคอนเทนเนอร์ระวังสิทธิ์ผู้ใช้ `swiftmq`) |
| สถานะ `down` | โปรเซสปลั๊กอินคราชหรือการเชื่อมต่อขาด; `restart=always` จะเชื่อมต่อใหม่เอง `never` ต้องดึงขึ้นมาด้วยมือ |
| พอร์ตไม่เปิด / ไคลเอนต์เชื่อมต่อไม่ได้ | `protocols[].listeners` ไม่ได้ตั้งค่าหรือที่อยู่ถูก `listeners.<ชื่อโปรโตคอล>` เขียนทับไป; ตรวจสอบทั้งสองที่ |
| ไคลเอนต์เชื่อมต่อพอร์ตอื่นแล้วถูกตัดทันที | พอร์ตนั้นไม่ตรงกับโปรโตคอลของคุณ (`prefix` ว่างหรือคำนำหน้าไม่ตรง); ตั้ง `prefix` ที่ไม่ว่างให้โปรโตคอล (ดู §5.3) |
| ขึ้น `ACCESS_REFUSED - ... for user ''` | ก่อนบริดจ์ความหมายเชิงความหมาย**ยังไม่มีการยืนยันตัวตน**; เรียก `core.authenticate` ก่อนแล้ว `session.open` |
| การเรียกย้อนกลับขึ้น "สตรีม N ยังไม่ได้เปิดเซสชัน" | `core.authenticate` ก่อน แล้ว `session.open` จากนั้นจึงเรียก `session.*` อื่นได้ |
| ไม่ได้รับการนำส่งจากการบริโภค | การนำส่งมาถึง `Call` ของคุณด้วย**การเรียกไปข้างหน้า** `session.deliver`; ยืนยันว่าจัดการเมธอดนั้นแล้ว |
| ปลั๊กอินนอกคอนเทนเนอร์ เคอร์เนลในคอนเทนเนอร์ เชื่อมต่อไม่ได้ | `address` ใช้ `tcp://host.docker.internal:<port>` (หรือใส่ปลั๊กอินลงคอนเทนเนอร์ด้วยและใช้ชื่อบริการ); ปลั๊กอินต้องรับฟัง `0.0.0.0` |

---

## 9. อ้างอิง (ดัชนีซอร์สโค้ด)

| อยากดูอะไร | ไฟล์ |
| --- | --- |
| โปรโตคอลสายและการ implement ทั้งสองฝั่ง (**ต้องอ่านสำหรับการพัฒนา**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (เฟรม), `proto.go` (ข้อความ), `server.go` (ฝั่งปลั๊กอิน), `client.go` (ฝั่งเคอร์เนล), `bridge.go` (สัญญา `session.*`), `stream.go` (สตรีม) |
| โฮสต์ sidecar ฝั่งเคอร์เนล (เชื่อมต่อ/เชื่อมต่อใหม่/พร็อกซี/สถานะ) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| บริดจ์ความหมายเชิงความหมายฝั่งเคอร์เนล (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| ชนิดระนาบการดำเนินการของเซสชันเคอร์เนล (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| การรับฟัง การดักจับ การเปิด-ปิดแบบ hot ตามปลั๊กอิน | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| วงจรชีวิตและการกำกับดูแลปลั๊กอิน (แยก/สถานะ/ตรวจสอบ) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| รายการคอนฟิกและตัวอย่าง (รวมส่วน sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| การประกอบโปรเซส (sidecar ถูกประกอบเข้าเคอร์เนลอย่างไร) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| การ implement อ้างอิงภาษา Go (ใช้ `pkg/sidecar.Server` รวมบริดจ์ `session.*` และ `core.authenticate`) | โปรเจกต์ทดสอบแยกต่างหาก `swiftmq-test/test/integration/echosidecar/` |
| **คู่มือแยกตามภาษา + โปรเจกต์ตัวอย่าง** | ไดเรกทอรีนี้ `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; ตัวอย่างอยู่ใน**เวิร์กสเปซ** `swiftmq-plugin/{python,nodejs,php,java}/` |
