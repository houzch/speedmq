# دليل تطوير المكوّن الإضافي كعملية خارجية (sidecar) في SwiftMQ

> **الجمهور**: المطوّرون الذين لا يرغبون في عمل fork / إعادة ترجمة النواة، ويريدون توسيع قدرات SwiftMQ بـ**أي لغة**.
> **النطاق**: يشرح هذا المستند شكلًا واحدًا فقط من المكوّنات الإضافية —— **المكوّن الإضافي كعملية خارجية** (مصطلح النواة `sidecar`). ومكوّنات البروتوكول المدمجة في النواة (AMQP 0-9-1 / MQTT) خارج نطاق هذا المستند.
> **طريقة القراءة**: يبني القسمان 1–2 النموذج الذهني، والقسم 3 لكتابة الشيفرة، و**القسم 5 هو "كيفية إدخاله ليعمل معنا وتقديم الخدمة للعملاء بعد اكتمال التطوير"**؛
> **للاستخدام بلغات أخرى (Python / Node.js / PHP / Java) انظر الأدلة الخاصة بكل لغة في §4** (كل منها يأتي بمشروع نموذجي كامل ومُختبَر فعليًا).
> الشيفرة في المستند هي هيكل تشغيلي أدنى، ويمكن نسخها مباشرة كنقطة انطلاق. الصينية المبسّطة هي لغة المصدر.

---

## 1. ما هو

**عملية مستقلة** تُنفّذ «بروتوكولًا» ما داخل عمليتها الخاصة (تحليل تدفّق بايتات العميل)،
وتتولّى النواة استضافتها وفق الإعداد: **المنافذ تفتحها النواة، والاتصالات تتوسّطها النواة**، بينما تُعاد آلية التسجيل / التشغيل والإيقاف / التدقيق / العزل كلها باستخدام آليات النواة القائمة.

لنُرسِخ أولًا ثلاثة نماذج ذهنية صحيحة (أكثر المواضع عرضةً للخطأ):

1. **عملية المكوّن الإضافي هي "خدمة محلية"**: تستمع فقط إلى **عنوان محلي** (TCP أو unix socket)، وتنتظر **اتصال النواة**.
   واتجاه الاتصال هو **النواة (عميل) → المكوّن الإضافي (خادم)**، والمصافحة ترسلها النواة أيضًا أولًا.
2. **منفذ الأعمال الخارجي لا يفتحه المكوّن الإضافي**: بل تُنشئه **النواة** وفق `protocols[].listeners` في الإعداد، وتُخرجه للعملاء.
   يتصل العملاء بـ**منفذ النواة**، وتتوسّط النواة البايتات إلى عملية المكوّن الإضافي. ولا **تحتاج** عملية المكوّن الإضافي إلى فتح منفذ الأعمال بنفسها.
3. **الدلالات اختيارية**: يمكن للمكوّن الإضافي أن "ينقل البايتات" فقط (تنفيذ البروتوكول بالكامل من جهتك)،
   ويمكنه أيضًا الوصول إلى دلالات النواة (الطوابير / التوجيه / الصلاحيات / التأكيد) عبر **الاستدعاءات العكسية** `session.*`،
   بـ**نفس مجموعة الدلالات تمامًا** التي تستخدمها مكوّنات البروتوكول المدمجة (وبذلك لا تتفرّع سلوكيات vhost والصلاحيات والتوجيه والطوابير).

| الفوائد | التكاليف |
| --- | --- |
| التوسيع دون تغيير النواة أو إعادة ترجمتها | نسخة إضافية واحدة من البايتات محليًا على مستوى البيانات (توسّط النواة، بلا تمرير fd، ومتّسق عبر المنصّات) |
| التنفيذ بأي لغة (يكفي أن تنفّذ بروتوكول السلك) | استدعاء RPC محلي إضافي في كل استدعاء عكسي (ترميز/فك ترميز JSON + نسخ) |
| إمكانية إصدار المكوّن الإضافي / ترقيته / إعادة تشغيله بشكل مستقل | يبقى الاستكشاف على جهة النواة: لا يُتعرّف إليه إلا عبر "البادئة" أو "منفذ مخصّص" |
| الانهيار يؤثر على هذا المكوّن فقط: تضع النواة العلامة `down`، دون انصراف أو انهيار | قدرة `net.listen` وحدها هي التي تسري فعليًا، أما بقية قيم القدرات فمحجوزة (انظر §7) |

---

## 2. كيف يعمل

إنشاء الاتصال (**النواة هي العميل، والمكوّن الإضافي هو الخادم**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

المصافحة:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

بدء الخدمة:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**دورة الحياة** (المضيف `internal/plugin/sidecar` على جهة النواة):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**دلالات الحالة** (ظاهرة عبر `swiftmqctl plugins show`):

| الحالة | المعنى | إجراء العمليات |
| --- | --- | --- |
| `enabled` | متّصل ويقدّم الخدمة | — |
| `failed` | **لم يبدأ من الأساس** (إعداد خاطئ / رفض المصافحة / تعذّر إطلاق العملية) | افحص `RuntimeNote` وسجل النواة، وصحّح الإعداد أو أصلح المكوّن الإضافي؛ ولا يُعاد المحاولة إلا بإعادة تشغيل النواة |
| `down` | **كان قائمًا وليس كذلك الآن** (انهارت العملية / انقطع الاتصال) | أبطل تشغيل عملية المكوّن الإضافي؛ وستستعيدها النواة تلقائيًا وفق سياسة `restart` |
| `disabled` | `enabled=false` في الإعداد، أو إيقاف ساخن من العمليات | الاستعادة عبر `plugins enable <الاسم>` |

---

## 3. التطوير (Go)

### 3.1 إنشاء المشروع

المكوّن الإضافي هو **Go module مستقل**، ويعتمد على حزمتَي عقد خارجيتين فقط:

- `github.com/houzch/swiftmq/pkg/sidecar` —— بروتوكول السلك والتنفيذ على جهة المكوّن الإضافي (**إلزامي**)
- `github.com/houzch/swiftmq/pkg/plugin` —— فقط عندما تحتاج إلى أنواع مثل `plugin.Message` / `plugin.Error` (اختياري)

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

> عند استخدام `replace` للتشغيل المشترك، يجب أن يشترك المكوّن الإضافي والنواة في **نفس الشيفرة المصدرية**، وإلا فقد يتطابق إصدار الـ API (`v1`) بينما تختلف الأنواع.

### 3.2 تنفيذ `Handler` (ثلاث طرق)

كامل السطح الوظيفي لعملية المكوّن الإضافي هو `Hello` / `Call` / `Open` في `sidecar.Handler`.
وتتولّى `pkg/sidecar` المصافحة ونبضات القلب وتعدّد الإرسال والتقسيم إلى كتل، فلا تحتاج إلى لمس الإطارات.

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

### 3.3 مستوى البيانات: قراءة وكتابة `Stream`

تُنفّذ `sidecar.Stream` الواجهة `io.ReadWriteCloser`، فيكفي التعامل معها مباشرةً كأنها "اتصال واحد":

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

النقاط الأساسية:

- كل اتصال عميل = تدفّق واحد؛ ويمكن للمكوّن الإضافي بدء الإرسال والاستقبال فورًا داخل `Handler.Open`.
- تُقسَّم الرسائل الكبيرة تلقائيًا بواسطة المكتبة (كل إطار ≤ 64 KiB)، و**استهلاك الذاكرة مستقل عن حجم الرسالة**.
- الضغط العكسي: لمخزن الاستقبال في التدفّق الواحد حدّ أعلى، وعند امتلائه يُحجب **خيط التوزيع** لذلك الاتصال (تنتظر كل التدفّقات معًا) ——
  وهذه مقايضة بين "ذاكرة قابلة للتنبؤ" و"تحديد معدّل لكل تدفّق"، وتفصيلها في §7.

### 3.4 استخدام جسر دلالات النواة (`session.*`)

إذا أردت أن يعيد المكوّن الإضافي استخدام دلالات الطوابير / التوجيه / الصلاحيات / التأكيد الخاصة بالنواة (بدلًا من بناء مجموعة خاصة به)، فاستخدم **الاستدعاءات العكسية**.
واستخدمها على التدفّق بالترتيب `session.open` → بقية `session.*` → (`session.close`):

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

**نظرة عامة على طرق الاستدعاء العكسي** (ثوابت `pkg/sidecar/bridge.go` → أسماء السلك):

| المجموعة | اسم السلك (الثابت) | الوصف |
| --- | --- | --- |
| المصادقة | `core.authenticate` (`MethodCoreAuthenticate`) | **يجب أن يكون أولًا**: سلّم بيانات الاعتماد من بروتوكولك إلى النواة للتحقّق؛ المعاملات `{stream, mechanism, response}`، ويعيد `{user}` |
| الجلسة | `session.open` (`MethodSessionOpen`) | افتح جلسة لمضيف افتراضي (vhost) على التدفّق؛ ولا يكون ذلك إلا **بعد المصادقة** |
| | `session.close` (`MethodSessionClose`) | حرّر الجلسة على التدفّق (إلغاء المستهلكين، وحذف الطوابير الحصرية) |
| المبادل | `session.declare_exchange` / `session.delete_exchange` | الإنشاء / الحذف (الإعلان السلبي عن غير موجود → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | ربط مبادل بمبادل |
| الطابور | `session.declare_queue` / `session.delete_queue` | الإنشاء / الحذف؛ وعندما يكون `name` فارغًا يولّده الخادم |
| | `session.bind_queue` / `session.unbind_queue` | ربط طابور بمبادل |
| | `session.purge_queue` | إفراغ الرسائل الجاهزة (باستثناء غير المؤكَّدة) |
| النشر | `session.publish` | يعيد `{routed, rejected}`؛ وتكتمل الاستدامة قبل الرد |
| السحب | `session.get` | اسحب رسالة واحدة بنفسك؛ و`found=false` تعني أن الطابور فارغ |
| الاستهلاك | `session.consume` / `session.cancel` | تسجيل / إلغاء المستهلكين |
| التسوية | `session.settle` | سوِّ تسليمًا واحدًا (`ack` / `requeue` / `reject`) |
| **أمامي** | `session.deliver` | **النواة → المكوّن الإضافي**: إعادة دفع التسليم (يُعالج داخل `Handler.Call` لديك) |

**أربعة تعهّدات يجب الالتزام بها**:

1. **`core.authenticate` أولًا**: لا هوية لسطح عمليات النواة للاتصال قبل المصادقة،
   وعندئذٍ سيُرفض `session.open` (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   ويُتولّى المكوّن الإضافي استخراج بيانات الاعتماد من بروتوكوله؛ أما منطق المصادقة وجدول المستخدمين فيبقيان في النواة، ولا يلمس المكوّن الإضافي مخزن كلمات المرور.
2. **ثم `session.open`**: إذا استدعيت طرقًا أخرى دون فتح جلسة، تُعيد النواة `KindPreconditionFailed` ("التدفّق N لم يفتح جلسة بعد").
3. **كل تسليم يُسوّى مرة واحدة بالضبط**: اختر واحدًا من `Ack` / `Requeue` / `Reject`.
   وكلٌّ من `Ack` و`Reject` يُسقط الرسالة، و**`Reject` وحده هو الذي يمرّ بمسار الرسائل الميتة**.
4. **التسليمات غير المسوّاة لا تُفقد**: عند انتهاء التدفّق (انقطاع العميل / إرجاع `Handler.Open`) أو انقطاع اتصال المكوّن الإضافي،
   تتعامل النواة مع كل التسليمات غير المسوّاة **على أنها "إعادة إلى الطابور"**، تفاديًا لبقاء الرسائل عالقة.

**استعادة الأخطاء**: يصل `*plugin.Error` من النواة عبر الجسر على شكل `*sidecar.RPCError` (الحقلان `Kind` / `Text`)،
ويمكن استعادته إلى `plugin.Error` مصنَّف، بدلًا من إضاعة التصنيف داخل نص:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

مقابلة قيم `Kind` الشائعة بالبروتوكولات المدمجة (لتقرّر كيف تُعيد الخطأ إلى العميل):

| `plugin.ErrorKind` | الدلالة | مقابلة AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | الكائن غير موجود | 404 NOT_FOUND (إغلاق Channel) |
| `KindPreconditionFailed` | المعاملات غير متوافقة مع كائن موجود / الجلسة غير مفتوحة | 406 PRECONDITION_FAILED (إغلاق Channel) |
| `KindAccessRefused` | صلاحيات غير كافية / اسم محجوز | 403 ACCESS_REFUSED (إغلاق Channel) |
| `KindResourceLocked` | مورد حصري مشغول | 405 RESOURCE_LOCKED (إغلاق Channel) |
| `KindInvalidPath` | vhost غير موجود | 402 INVALID_PATH (إغلاق الاتصال) |
| `KindNotImplemented` | القدرة غير منفَّذة | 540 NOT_IMPLEMENTED (إغلاق الاتصال) |
| `KindInternal` | خطأ داخلي في النواة | 541 INTERNAL_ERROR (إغلاق الاتصال) |

**حدود أمانة أنواع الرسائل**: يُمرَّر جدول الخصائص (`MessageDTO.Properties.Headers`) عبر JSON،
فلا يتوفّر **تمييز معلومات النوع الرقمي بين `int32` / `double` كما في جدول حقول AMQP field-table**.
وعند الحاجة إلى أمانة أنواع صارمة، ضَع هذا النوع من المعلومات في جسم الرسالة (بايتات خام) لتحمله بنفسك.

### 3.5 حالة المكوّن الإضافي والسجلات

- **تتحدّد حالة** المكوّن الإضافي الخارجي **ببقاء الاتصال قائمًا أو لا**، ولا يحتاج المكوّن إلى الإبلاغ عن نفسه (فـ`StateReporter` الخاص بالمكوّنات المدمجة لا ينطبق على العمليات الخارجية).
- السجلات: يُخرج `sidecar.ServerOptions.Logger` إلى stdout/stderr لعملية المكوّن الإضافي؛
  **وعند إطلاقه من النواة عبر `spawn`، تُمرِّر النواة هذه المخرجات إلى سجل النواة** (بوسم `plugin`)، تسهيلًا للجمع الموحّد.
- في النشر المستقل (بلا spawn)، اجمع سجلات المكوّن الإضافي بالطريقة التي تناسبك.

### 3.6 الاختبار الذاتي دون الاتصال بالنواة

`Handler` واجهة Go عادية، ويمكن في اختبار الوحدة إنشاء نسخة منها مباشرةً واستدعاء `Hello` / `Call` / `Open` لتغطية منطق العمل، دون تشغيل أي شبكة:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

أما التحقّق من الطرف إلى الطرف فانظر §5.

---

## 4. التطوير بلغات أخرى (مواصفة بروتوكول السلك)

`pkg/sidecar` عقد خارجي بلا اعتماديات؛ وبروتوكول السلك نفسه بسيط، ويمكن تنفيذه بأي لغة.
وللتكامل، عليك تنفيذ التعهّدات "على مستوى البايت" التالية (الشيفرة المصدرية في `pkg/sidecar/frame.go` و`proto.go`).

> **تتوفّر أدلة خاصة بكل لغة مع مشاريع نموذجية كاملة** (وقد اجتازت الأمثلة فعليًا: المصافحة → المصادقة → جسر الدلالات → التسليم/التسوية → تدفّق البايتات):
>
> | اللغة | الدليل | المشروع النموذجي (في مساحة العمل `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (المكتبة القياسية فقط) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (المكتبة القياسية فقط) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (المكتبة القياسية فقط) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (ملف واحد، JDK فقط) |
>
> أما التنفيذ المرجعي الكامل بلغة Go فانظر المشروع الاختباري المستقل `swiftmq-test/test/integration/echosidecar/` (يستخدم `pkg/sidecar.Server` مباشرةً،
> ولا حاجة للاهتمام بتفاصيل طبقة البايت أدناه).

**تنسيق الإطار** (موحّد لكل الإطارات):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**أنواع الإطارات `kind`**:

| kind | الاسم | الاتجاه | الحمل |
| --- | --- | --- | --- |
| 1 | Hello | النواة → المكوّن الإضافي | JSON `Hello` |
| 2 | HelloAck | المكوّن الإضافي → النواة | JSON `HelloAck` |
| 3 | Ping | النواة → المكوّن الإضافي | فارغ |
| 4 | Pong | المكوّن الإضافي → النواة | فارغ |
| 5 | Call | في الاتجاهين | JSON `Call` |
| 6 | Reply | في الاتجاهين | JSON `Reply` |
| 7 | Open | النواة → المكوّن الإضافي | JSON `Open` |
| 8 | OpenAck | المكوّن الإضافي → النواة | JSON `OpenAck` |
| 9 | Data | في الاتجاهين | `u32 BE stream` + بايتات خام |
| 10 | Close | في الاتجاهين | JSON `Close` |

**بنى JSON لمستوى التحكّم** (أسماء الحقول مطابقة لـ `proto.go`).

> هذا المقطع **مثال على رسائل بروتوكول السلك** (تُعطى رسائل متعددة بالترتيب في الكتلة نفسها، ولذلك يُستخدم `//` للفصل والشرح)،
> و**ليس إعدادًا يمكن كتابته مباشرة في `swiftmqd.json`**.

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

**دلالات يجب الالتزام بها**:

- **المصافحة**: ترسل النواة `Hello` أولًا، ويجب على المكوّن الإضافي الردّ بإطار `HelloAck`.
  وتتحقّق النواة من `HelloAck.name == اسم المكوّن الإضافي في الإعداد` ومن `HelloAck.api_version == APIVersion الخاص بالنواة`؛
  و`deny` غير الفارغ يُعدّ رفضًا للاتصال (فيُعزل المكوّن الإضافي).
- **نبضات القلب**: ترسل النواة افتراضيًا `Ping` كل 2s، ويجب على المكوّن الإضافي الردّ بـ`Pong` خلال 8s؛ وعلى جهة المكوّن الإضافي، إن لم يصل أي إطار خلال 24s فله أن يغلق الاتصال بنفسه.
- **مساحتا مُعرّفات**: يميّز `reverse` في `Call` الاتجاه، ويزيد كل اتجاه من 1 بشكل مستقل،
  ولذلك **يجب أن يحمل `Reply` قيمة `reverse`**، وإلا سيُسلَّم الردّ إلى المنتظر الخاطئ.
- **لا base64 على مستوى البيانات**: تُوضع البيانات الكبيرة مثل جسم الرسالة مباشرة في حمل إطار `Data` (`stream` + بايتات خام)، مع التقسيم عند الحاجة.

> إن كنت تستخدم Go، فاستخدم `pkg/sidecar` مباشرةً، ولن تحتاج إلى تنفيذ أي من التفاصيل أعلاه بنفسك.

---

## 5. إدخاله ليعمل معنا: الربط وتقديم الخدمة والتغليف ★

يجيب هذا القسم عن "كيفية ربطه بـSwiftMQ وكيفية تقديم الخدمة للعملاء بعد اكتمال التطوير".

### 5.1 الإعلان عن المكوّن الإضافي في الإعداد

المكوّن الإضافي الخارجي **مُدار بالكامل عبر الإعداد**، ولا تحتاج النواة إلى أي تعديل شيفرة لأجله. أضف عنصرًا في مقطع `plugins` داخل `swiftmqd.json`
(**الإعداد الفعلي هو JSON قياسي، ولا يقبل التعليقات**):

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

شرح بند بند (قائمة الحقول في الجدول أدناه):

- اسم المفتاح `plugins.<اسم الإضافة>` **يجب أن يطابق `HelloAck.name` الذي يعلن عنه المكوّن الإضافي**، وإلا رُفضت المصافحة.
- `builtin: false`: إعلان صريح بأنه مكوّن إضافي خارجي (إن لم تكتبه، ستعرضه واجهة الإدارة كمكوّن مدمج).
- `enabled`: إيقافه = عدم إطلاق العملية وعدم إنشاء مستمع.
- عند `required: true` سيؤدي فشل البدء إلى حجب بدء النواة —— فلا تفعّله للمكوّنات الإضافية الخارجية.
- `address` هو **العنوان الذي تتصل به النواة** (النواة هي العميل)؛ وعندما يكون `spawn` غير فارغ تتولّى النواة إطلاق العملية نيابةً عنك.
- `protocols[].prefix` **يجب أن يكون غير فارغ** (قواعد الاستكشاف في §5.3).
- `listeners` هي **المنافذ الخارجية لهذا البروتوكول، وتفتحها النواة** (العملاء يتصلون بالنواة).

نظرة عامة على الحقول:

| الحقل | إلزامي | الوصف |
| --- | --- | --- |
| `sidecar.address` | ✅ | عنوان عملية المكوّن الإضافي: `tcp://host:port` أو `unix:///path` |
| `sidecar.spawn` | ✕ | سطر الأوامر الذي تُشغّله النواة نيابةً عنك (العنصر الأول هو الملف التنفيذي)؛ **اتركه فارغًا = النواة تتصل فقط ولا تُطلق**، وتدير العملية بنفسك |
| `sidecar.restart` | ✕ | `always` (الافتراضي، استعادة تلقائية بعد انقطاع/انهيار) أو `never` (تضع العلامة `down` فقط وتنتظر تدخّل العمليات) |
| `sidecar.protocols[].name` | ✅ | اسم البروتوكول (فريد عالميًا، ويشارك في أولوية الاستكشاف) |
| `sidecar.protocols[].prefix` | ✕ | بادئة الاستكشاف (ASCII)؛ **فارغة = لا يشارك في الاستكشاف** |
| `sidecar.protocols[].listeners[]` | ✕ | المستمع الخارجي لهذا البروتوكول (`name` + `addr`)، وتُنشئه النواة |
| `sidecar.handshake_timeout_seconds` | ✕ | تجاوز مهلة المصافحة (الافتراضي 5s) |
| `sidecar.heartbeat_seconds` | ✕ | تجاوز فترة نبضات القلب (الافتراضي 2s) |

> **اسم المكوّن الإضافي واسم البروتوكول**: **يمكن أن يختلفا** (مثلًا المكوّن الإضافي `my-sidecar` يقدّم البروتوكول `myproto`).
> فالإيقاف الساخن يجد أولًا كل البروتوكولات المسجّلة باسم المكوّن الإضافي، ثم يغلق منافذ هذه البروتوكولات، فلا حاجة إلى توحيد الاسم عن قصد.

### 5.2 ثلاث طرق للربط

| الطريقة | الإعداد | متى تُستخدم |
| --- | --- | --- |
| **نفس المضيف + إطلاق من النواة (spawn)** | `spawn: [...]`، و`address` يشير إلى العنوان الذي يستمع عليه | نشر على نفس الجهاز، حاوية واحدة؛ الأقل عناءً، وتتولّى النواة الإطلاق والاسترجاع |
| **نفس المضيف + إدارة ذاتية (dial)** | `spawn: []`، و`address` يشير إلى عملية قيد التشغيل | إدارة دورة حياة المكوّن الإضافي بـsystemd / supervisor |
| **عبر مضيفين / حاويات (dial، ويجب أن يكون tcp)** | `spawn: []`، و`address: "tcp://<اسم الخدمة>:19001"` | نشر المكوّن الإضافي والنواة في حاويات / أجهزة منفصلة |

اختيار العنوان:

- **على نفس الجهاز يُفضَّل unix socket** (`unix:///tmp/my-sidecar.sock`): لا يشغل منفذ TCP ولا يتأثر بشغل منافذ المضيف.
  وانتبه إلى أن مسار الـsocket يجب أن يكون قابلًا للكتابة من عملية النواة (داخل الحاوية المستخدم غير الجذري `swiftmq`).
- **عبر الحاويات يلزم TCP**، ويجب أن تستمع عملية المكوّن الإضافي إلى `0.0.0.0`، وأن يستخدم `address` **اسم الخدمة في شبكة الحاويات**.

> لا تعكس الاتجاه: **العنوان الذي يستمع عليه المكوّن الإضافي** = `address`؛ **المنفذ المفتوح للعملاء** = `protocols[].listeners`.

### 5.3 تقديم الخدمة للعملاء: التعرّف عبر `prefix`

عند توزيع الاتصالات **لا تنظر طبقة الوصول إلا إلى نتيجة الاستكشاف**: فهي تسأل كل بروتوكول مفعّل بالترتيب `Sniff(peek)` (وpeek بحد أقصى 8 بايتات)،
ومن يطابق يتولّى هذا الاتصال. ولذلك:

1. **يجب أن يكون `prefix` غير فارغ** (ASCII، ≤ 8 بايتات). ولا يُسلَّم الاتصال إلى مكوّنك الإضافي إلا إذا طابقت البايتات الأولى القادمة من العميل هذه البادئة.
   مثال: `"prefix": "PY"` → يجب أن تكون البايتة الأولى للعميل هي `PY` (ويمكنك اعتبار البادئة الترويسة السحرية لبروتوكولك).
2. **`prefix` الفارغ يعني عدم المشاركة في الاستكشاف**: ولن تُسلَّم مثل هذه الاتصالات إلى المكوّن الإضافي (عمليًا: تُقطع الاتصالات على منفذ الاستماع فورًا).
   لذلك لا يصلح `prefix` الفارغ إلا لسيناريو "بروتوكول آخر سيمرّر لك على المنفذ نفسه"، و**لا** تستخدمه لبناء منفذ مخصّص.
3. يحدّد `listeners[].addr` "على أي منفذ يُقدَّم الخدمة للخارج"، ويحدّد `prefix` "هل يُعدّ هذا الاتصال لك" ——
   ويجب استخدامهما معًا: **فالمنفذ المخصّص يحتاج أيضًا بادئة `prefix` غير فارغة** (وهذا أيضًا سبب كتابة
   `echo-sidecar` في إعداد النواة النموذجي لكلٍّ من `prefix: "ECHO"` و`listeners: [":1885"]`).
4. تُطابَق البادئات بترتيب تسجيل البروتوكولات، و**الأسبق في المطابقة هو الذي يسري**: فعند تعدد المكوّنات الإضافية، يجب أن تكون البادئات متمايزة (فمثلًا بدء الجميع بالبايتة نفسها يجعلها تحجب بعضها).

### 5.4 تجاوز عنوان الاستماع وTLS

- يمكن إعطاء عنوان الاستماع الخارجي في **موضعين**: `sidecar.protocols[].listeners[].addr` (الافتراضي) و
  `listeners.<اسم البروتوكول>` (تجاوز كامل باسم البروتوكول). وعند وجودهما معًا يُرجَّح `listeners.<اسم البروتوكول>`.
- وعند الحاجة إلى TLS، أعطِ الشهادة في `listeners.<اسم البروتوكول>[i].tls` (الحقول مطابقة للبروتوكولات المدمجة).
  وفيما يلي مقطع `listeners` (**JSON قياسي، ولا يقبل التعليقات**): البند الأول نص صريح، والثاني عبر TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> يُنهي **النواة** اتصال TLS على جهة الاستماع، وتستلم عملية المكوّن الإضافي تدفّقًا صريحًا —— فلا يلزم المكوّن الإضافي التعامل مع TLS.

### 5.5 التغليف: تشغيل المكوّن الإضافي مع النواة

**الخيار A —— تضمينه في الصورة نفسها** (مُوصى به للمكوّنات الإضافية "التي تُنشر مع النواة"): أضف سطرًا في مرحلة التشغيل من `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

ثم في الإعداد ضع `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`،
و`address` بالقيمة نفسها. وستُطلقه النواة عند بدء التشغيل.

**الخيار B —— تركيب الملف التنفيذي (mount)** (دون تعديل الصورة، مناسب للتشغيل المشترك):

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

يستخدم الإعداد unix socket (لتفادي شغل منفذ إضافي). وفيما يلي مقطع `sidecar` داخل `plugins.my-sidecar`
(**JSON قياسي، ولا يقبل التعليقات**؛ ويبقى `prefix` غير فارغ، انظر §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**الخيار C —— حاوية مستقلة** (المكوّن الإضافي يُنشر منفردًا / يتوسّع ويقلّص باستقلال):

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

في الإعداد `spawn: []` (النواة تتصل فقط ولا تُطلق)، و`address: "tcp://my-sidecar:19001"` (اسم خدمة compose).

### 5.6 التشغيل والتحقّق

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

في `plugins show` ركّز على `state` و`RuntimeNote`:
فـ`failed` يأتي مع سبب الفشل (رفض المصافحة / تعذّر فتح المنفذ…)؛ و`down` يأتي مع سبب الانقطاع (انهيار العملية / انقطاع الاتصال).

### 5.7 عمليات التشغيل

| العملية | الأمر / الواجهة | الأثر |
| --- | --- | --- |
| إيقاف ساخن | `swiftmqctl plugins disable my-sidecar` أو `PUT /api/plugins/my-sidecar/disable` | **إغلاق المستمعات الخارجية لهذا المكوّن الإضافي** (تعطيل على مستوى القدرة)؛ ولا يتأثر النواة ولا المكوّنات الإضافية الأخرى |
| تشغيل ساخن | `swiftmqctl plugins enable my-sidecar` | إعادة فتح مستمعاته؛ وإن كان قد فشل في البدء سابقًا يُعاد المحاولة مرة واحدة |
| عرض الحالة | `swiftmqctl plugins list/show` | الحالة + سبب الفشل/الانقطاع |
| خروج النواة | — | قطع الاتصال بالمكوّن الإضافي، واسترجاع جلسات الجسر، و**إنهاء العمليات الفرعية التي أطلقتها النواة عبر `spawn`** |

> الإيقاف الساخن يغلق "القدرة" فقط (منافذ الاستماع)، و**لا** يقتل عملية المكوّن الإضافي التي أُطلقت بـ`spawn`؛ ويحدث استرجاع العملية عند خروج النواة.

### 5.8 توفير مدخل في لوحة الإدارة (اختياري)

عندما يأتي المكوّن الإضافي بواجهة تشغيل خاصة به، أضف `console_url` (عنوان واجهة الإدارة، وبقية الحقول انظر §5.1) في مقطع `plugins.<اسم الإضافة>`.
وفيما يلي نرسم البند `plugins.my-sidecar` وحده (**JSON قياسي، ولا يقبل التعليقات**؛ ومحتوى مقطع `sidecar` كما في §5.1):

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

- تعرض صفحة **إدارة المكوّنات الإضافية** في لوحة الإدارة (بياناتها من الحقل `console_url` في `GET /api/plugins`) لهذه المكوّنات
  زر «فتح واجهة الإدارة»، و**يُفتح في تبويب جديد**.
- وعند عدم الإعلان عن `console_url` يكون الزر غير متاح، مع تلميح عند التحويم يقول "هذا المكوّن الإضافي لا يوفّر واجهة إدارة".
- وهو مجرّد **بيانات وصفية يكتبها الناشر في الإعداد**: لا ينتمي إلى واجهة برمجة المكوّنات الإضافية (`pkg/plugin`)، ولا يشارك في تشغيلها/إيقافها،
  وتستضيف الواجهة نفسها بنفسها (يمكن أن تكون داخل عملية المكوّن الإضافي، أو أي خدمة مستقلة).

---

## 6. دورة الحياة ومصفوفة تحمّل الأخطاء

| الحالة | سلوك النواة | الأثر على جهة المكوّن الإضافي |
| --- | --- | --- |
| لم تبدأ عملية المكوّن الإضافي / رُفضت المصافحة | يُعاد محاولة الاتصال خلال 8s، وإن استمر الفشل توضع العلامة `failed` ويُعزل (دون حجب بدء النواة) | لا شيء |
| خروج عملية أطلقتها `spawn` | يُسجَّل ذلك؛ وتوضع العلامة `down`؛ ويُعاد الاتصال / الإطلاق بتراجع وفق سياسة `restart` | تعيد العملية الجديدة المصافحة |
| انهيار عملية المكوّن الإضافي (أثناء التشغيل) | لا تتأثر النواة؛ `down` + إعادة اتصال بتراجع | تُغلق `pkg/sidecar.Server` ذلك الاتصال |
| تلقّي النواة `kill -9` | — | تستعيد جهة المكوّن الإضافي الاتصال بنفسها عبر مهلة الخمول (افتراضيًا 24s بلا إطار)، دون ترك عمليات زومبي |
| خروج النواة بشكل طبيعي | استدعاء `Stop`: قطع الاتصال، واسترجاع التسليمات غير المسوّاة (كإعادة إلى الطابور)، و`Kill` العمليات الفرعية | يستقبل SIGKILL |
| التسليمات أثناء انقطاع اتصال المكوّن الإضافي | التسليمات غير المسوّاة **تُعاد دائمًا إلى الطابور** ولا تُفقد | — |
| انقطاع العميل / إرجاع `Open` | إغلاق التدفّق المقابل، وتحرير جلسة ذلك التدفّق ومستهلكيه | يُرجع `Stream.Read` قيمة EOF |

---

## 7. الخطوط الحمراء والحدود المعروفة

**الخطوط الحمراء**

1. لا يجوز للمكوّن الإضافي أن يعتمد إلا على `pkg/sidecar` (واختياريًا `pkg/plugin`)؛ و**لا يجوز** له الاعتماد على `internal/**` في النواة.
2. يجب أن يطابق اسم المكوّن الإضافي الإعداد، ويجب أن يطابق `APIVersion` النواة، وإلا تعذّر الاتصال (وهذا لمنع "العمل بصمت دون سريان").
3. عند استخدام `session.*`: **`core.authenticate` أولًا ثم `session.open`**، و**تسوِّ كل تسليم مرة واحدة بالضبط**.
4. يجب أن يكون `protocols[].prefix` غير فارغ، وإلا لن تُسلَّم الاتصالات إلى المكوّن الإضافي (انظر §5.3).
5. يجب أن يُعيد الرفض في `Hello` **خطًا صريحًا** (لا تصمت) —— وإلا فلن ترى النواة سوى "أُغلق الاتصال" ولم تستطع تحديد السبب.

**الحدود المعروفة**

- **الاستكشاف على جهة النواة**: لا يمكن للمكوّن الإضافي الخارجي تعريف دالة استكشاف خاصة، ولا يطابق إلا بـ`prefix` (ASCII، ≤ 8 بايتات)؛
  و`prefix` الفارغ يعني "عدم الحصول على الاتصال" (انظر §5.3).
- **مستوى البيانات يمرّ عبر وسيط محلي**: لا تمرير fd (فلا يوجد `SCM_RIGHTS` على Windows)، ما يضيف نسخة ذاكرة واحدة مقارنةً بالتنفيذ داخليًا؛
  ويضيف كل استدعاء عكسي أيضًا RPC محليًا إضافيًا.
- **أنواع جدول الخصائص تتدهور**: يُمرَّر `Properties.Headers` عبر JSON، فيضيع التمييز بين `int32` / `double` ونحوه (انظر §3.4).
- **الضغط العكسي لتدفّق واحد يؤثر على الاتصال كله**: عند امتلاء مخزن استقبال تدفّق واحد يُحجب خيط التوزيع في ذلك الاتصال؛ وتحديد المعدّل لكل تدفّق تحسين لاحق.
- **فشل المصادقة يمرّ كنص فقط**: فشل مصادقة النواة هو `*plugin.AuthError` (تصنيف مختلف عن `plugin.ErrorKind`)،
  ولا يصل إلى المكوّن الإضافي عبر الجسر سوى النص، فعلى المكوّن الإضافي أن يقابله برموز أخطاء البروتوكول وفق تعهّده الخاص.
- **قدرة `net.listen` وحدها هي التي تسري فعليًا**: أما `store.read/write` و`http.route` و`cluster.metadata.write` و
  `auth.verify` فهي **مواضع محجوزة**، ولا يشارك الإعلان عنها إلا في التدقيق (انظر عرض الحوكمة في §5.6)، ولا توجد حاليًا نقطة توسيع مقابلة.

---

## 8. الأسئلة الشائعة لحل المشكلات

| العَرَض | السبب والمعالجة |
| --- | --- |
| الحالة `failed` والسبب يتضمّن "عدم تطابق اسم المكوّن الإضافي" | اسم المكوّن الإضافي في الإعداد ≠ `HelloAck.name`؛ طابقهما |
| الحالة `failed` والسبب يتضمّن "عدم تطابق إصدار الـ API" | `HelloAck.api_version` ≠ `APIVersion` الخاص بالنواة؛ طابقهما |
| الحالة `failed` والسبب يتضمّن "رفض المصافحة" | أعاد `Hello` في المكوّن الإضافي خطأ (`deny`)؛ راجع مخرجات المكوّن الإضافي المُمرَّرة في سجل النواة |
| الحالة `failed` والسبب يتضمّن "فشل الاتصال بالمكوّن الإضافي الخارجي" | لم تبدأ العملية / `address` خاطئ / مسار الـsocket غير قابل للكتابة (انتبه لصلاحيات المستخدم `swiftmq` داخل الحاوية) |
| الحالة `down` | انهارت عملية المكوّن الإضافي أو انقطع الاتصال؛ و`restart=always` يُعيد الاتصال تلقائيًا، بينما `never` يتطلّب إطلاقًا يدويًا |
| المنفذ غير مفتوح / تعذّر اتصال العميل | `protocols[].listeners` غير مضبوط أو عُوِّض عنوانه بـ`listeners.<اسم البروتوكول>`؛ راجع الموضعين |
| يتصل العميل بمنفذ آخر ثم يُقطع فورًا | هذا المنفذ لا يطابق بروتوكولك (`prefix` فارغ أو البادئة غير مطابقة)؛ اضبط للمكوّن الإضافي `prefix` غير فارغ (انظر §5.3) |
| ظهور `ACCESS_REFUSED - ... for user ''` | **لا مصادقة** قبل جسر الدلالات؛ استدعِ `core.authenticate` قبل `session.open` |
| يعيد الاستدعاء العكسي "التدفّق N لم يفتح جلسة بعد" | `core.authenticate` أولًا ثم `session.open`، وبعدها فقط يمكن استدعاء بقية `session.*` |
| لا تصل تسليمات الاستهلاك | تصل التسليمات إلى `Call` لديك كـ**استدعاء أمامي** `session.deliver`؛ تأكّد من معالجة هذه الطريقة |
| المكوّن الإضافي خارج الحاوية والنواة داخلها، وتعذّر الاتصال | استخدم `tcp://host.docker.internal:<المنفذ>` في `address` (أو ضَع المكوّن الإضافي في الحاوية أيضًا واستخدم اسم الخدمة)؛ ويجب أن يستمع المكوّن الإضافي إلى `0.0.0.0` |

---

## 9. المراجع (فهرس الشيفرة المصدرية)

| ما تريد رؤيته | الملف |
| --- | --- |
| بروتوكول السلك والتنفيذ على الجانبين (**قراءة إلزامية للتطوير**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (الإطارات)، `proto.go` (الرسائل)، `server.go` (جهة المكوّن الإضافي)، `client.go` (جهة النواة)، `bridge.go` (عقد `session.*`)، `stream.go` (التدفّقات) |
| مضيف sidecar على جهة النواة (الربط/إعادة الاتصال/التوسّط/الحالة) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| جسر دلالات النواة (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| أنواع سطح عمليات الجلسة في النواة (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| الاستماع والاستكشاف والتشغيل/الإيقاف الساخن حسب المكوّن الإضافي | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| دورة حياة المكوّن الإضافي وحوكمته (العزل/الحالة/التدقيق) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go)، [`registry.go`](../../../internal/plugin/registry.go) |
| عناصر الإعداد والأمثلة (بما فيها مقطع sidecar) | [`internal/config/config.go`](../../../internal/config/config.go)، [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| تجميع العملية (كيف يُدمج sidecar في النواة) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| التنفيذ المرجعي بلغة Go (باستخدام `pkg/sidecar.Server`، مع جسر `session.*` و`core.authenticate`) | المشروع الاختباري المستقل `swiftmq-test/test/integration/echosidecar/` |
| **أدلة كل لغة + المشاريع النموذجية** | في هذا الدليل `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`؛ والأمثلة في **مساحة العمل** `swiftmq-plugin/{python,nodejs,php,java}/` |
