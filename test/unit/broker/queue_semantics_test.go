package broker_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M3 的内核语义：TTL、死信、长度限制、优先级、队列过期与权限。
//
// 队列参数与死信原因的取值一律写字面量（"x-message-ttl"、"expired" 等）：
// 它们是协议级契约，锁字面量才能在实现里的常量被改错时暴露问题。

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", desc)
}

// queueCount 通过被动声明读取队列中的就绪消息数。
//
// 必须把队列的 x-* 参数原样带上：RabbitMQ 的等价性检查对被动声明同样生效，
// 参数不一致会直接 406，读到的就永远是 0。
func queueCount(t *testing.T, sess plugin.Session, name string, args map[string]any) (uint32, bool) {
	t.Helper()
	info, err := sess.DeclareQueue(plugin.QueueDeclare{Name: name, Passive: true, Arguments: args})
	if err != nil {
		return 0, false
	}
	return info.MessageCount, true
}

// requireCount 断言队列当前消息数。
func requireCount(t *testing.T, sess plugin.Session, name string, args map[string]any, want uint32) {
	t.Helper()
	got, ok := queueCount(t, sess, name, args)
	if !ok {
		t.Fatalf("读取队列 %s 统计失败（被动声明被拒）", name)
	}
	if got != want {
		t.Fatalf("队列 %s 消息数 = %d, want %d", name, got, want)
	}
}

// declareDLX 建立"死信交换机 + 死信队列"的固定拓扑：
// 死信一律投到 dlq，绑定键即为 dlq 的名字。
func declareDLX(t *testing.T, sess plugin.Session, dlx, dlq string) {
	t.Helper()
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{Name: dlx, Type: plugin.ExchangeDirect}); err != nil {
		t.Fatalf("声明死信交换机失败: %v", err)
	}
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: dlq}); err != nil {
		t.Fatalf("声明死信队列失败: %v", err)
	}
	if err := sess.BindQueue(dlq, dlx, dlq, nil); err != nil {
		t.Fatalf("绑定死信队列失败: %v", err)
	}
}

// dlxArgs 返回把消息送入 dlx/dlq 的队列参数。
func dlxArgs(dlx, dlq string, extra map[string]any) map[string]any {
	args := map[string]any{
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": dlq,
	}
	for k, v := range extra {
		args[k] = v
	}
	return args
}

// checkDeath 校验 x-death 头的首项。
func checkDeath(t *testing.T, msg *plugin.Message, wantReason, wantQueue string) {
	t.Helper()
	arr, ok := msg.Properties.Headers["x-death"].([]any)
	if !ok || len(arr) == 0 {
		t.Fatalf("缺少 x-death 头: %#v", msg.Properties.Headers)
	}
	first, ok := arr[0].(map[string]any)
	if !ok {
		t.Fatalf("x-death 首项类型错误: %#v", arr[0])
	}
	if first["reason"] != wantReason {
		t.Fatalf("x-death reason = %v, want %q", first["reason"], wantReason)
	}
	if first["queue"] != wantQueue {
		t.Fatalf("x-death queue = %v, want %q", first["queue"], wantQueue)
	}
}

func isKind(err error, want plugin.ErrorKind) bool {
	ke, ok := err.(*plugin.Error)
	return ok && ke.Kind == want
}

// TestMessageTTLToDeadLetter 覆盖"延迟队列"的经典构造：
// 队列级 TTL 到期后消息自动转入死信，且不需要任何消费者。
func TestMessageTTLToDeadLetter(t *testing.T) {
	b := newTestBroker(t)
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	declareDLX(t, sess, "ttl.dlx", "ttl.dlq")

	qArgs := dlxArgs("ttl.dlx", "ttl.dlq", map[string]any{"x-message-ttl": int32(50)}) // 50ms
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "ttl.q", Arguments: qArgs})
	if _, err := sess.Publish(&plugin.Message{Body: []byte("expire-me")}, "", q.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	waitFor(t, 3*time.Second, "过期消息进入死信队列", func() bool {
		n, ok := queueCount(t, sess, "ttl.dlq", nil)
		return ok && n == 1
	})
	requireCount(t, sess, q.Name, qArgs, 0)

	d, ok, err := sess.Get("ttl.dlq", true)
	if err != nil || !ok {
		t.Fatalf("从死信队列取消息失败: ok=%v err=%v", ok, err)
	}
	if string(d.Message.Body) != "expire-me" {
		t.Fatalf("死信内容错误: %q", d.Message.Body)
	}
	checkDeath(t, d.Message, "expired", q.Name)
}

// TestRejectToDeadLetter 覆盖 basic.reject/nack(requeue=false) 进死信。
func TestRejectToDeadLetter(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")
	declareDLX(t, sess, "rej.dlx", "rej.dlq")

	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{
		Name:      "rej.q",
		Arguments: dlxArgs("rej.dlx", "rej.dlq", nil),
	})
	if _, err := sess.Publish(&plugin.Message{Body: []byte("bad-msg")}, "", q.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	d, ok, err := sess.Get(q.Name, false)
	if err != nil || !ok {
		t.Fatalf("取消息失败: ok=%v err=%v", ok, err)
	}
	d.Settle(plugin.SettleReject)

	waitFor(t, 3*time.Second, "被拒绝的消息进入死信队列", func() bool {
		n, ok := queueCount(t, sess, "rej.dlq", nil)
		return ok && n == 1
	})
	dl, ok, _ := sess.Get("rej.dlq", true)
	if !ok {
		t.Fatalf("死信队列为空")
	}
	checkDeath(t, dl.Message, "rejected", q.Name)
}

// TestAckDoesNotDeadLetter 是回归测试：确认消费（ack）绝不能被当成拒绝而复制进死信。
func TestAckDoesNotDeadLetter(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")
	declareDLX(t, sess, "ok.dlx", "ok.dlq")

	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{
		Name:      "ok.q",
		Arguments: dlxArgs("ok.dlx", "ok.dlq", nil),
	})
	if _, err := sess.Publish(&plugin.Message{Body: []byte("good")}, "", q.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	d, ok, _ := sess.Get(q.Name, false)
	if !ok {
		t.Fatalf("取消息失败")
	}
	d.Settle(plugin.SettleAck)

	// 给后台派发器足够时间去犯错
	time.Sleep(300 * time.Millisecond)
	if n, ok := queueCount(t, sess, "ok.dlq", nil); ok && n != 0 {
		t.Fatalf("正常 ack 的消息不应进死信，死信队列却有 %d 条", n)
	}
}

// TestMaxLengthDropHead 覆盖 drop-head：超出长度限制时挤出最老的消息。
func TestMaxLengthDropHead(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")
	declareDLX(t, sess, "len.dlx", "len.dlq")

	qArgs := dlxArgs("len.dlx", "len.dlq", map[string]any{"x-max-length": int32(2)})
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "len.q", Arguments: qArgs})
	for i := 0; i < 3; i++ {
		body := []byte{byte('0' + i)}
		if _, err := sess.Publish(&plugin.Message{Body: body}, "", q.Name, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}

	requireCount(t, sess, q.Name, qArgs, 2)
	// drop-head 挤出的是最老的一条，且它应进死信
	waitFor(t, 3*time.Second, "被挤出的消息进入死信队列", func() bool {
		n, ok := queueCount(t, sess, "len.dlq", nil)
		return ok && n == 1
	})
	dl, ok, _ := sess.Get("len.dlq", true)
	if !ok || string(dl.Message.Body) != "0" {
		t.Fatalf("被挤出的应是最老的 \"0\"，实际 %q", dl.Message.Body)
	}
	checkDeath(t, dl.Message, "maxlen", q.Name)

	// 队列中应保留最后两条，且顺序不变
	for _, want := range []string{"1", "2"} {
		d, ok, _ := sess.Get(q.Name, true)
		if !ok || string(d.Message.Body) != want {
			t.Fatalf("队列剩余内容错误：期望 %q，实际 %q", want, d.Message.Body)
		}
	}
}

// TestMaxLengthRejectPublish 覆盖 reject-publish：超出限制时拒绝新消息，
// 并通过发布结果告诉协议层（confirm 模式下据此回 basic.nack）。
func TestMaxLengthRejectPublish(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	qArgs := map[string]any{"x-max-length": int32(1), "x-overflow": "reject-publish"}
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "rejlen.q", Arguments: qArgs})
	res, err := sess.Publish(&plugin.Message{Body: []byte("a")}, "", q.Name, false)
	if err != nil || !res.Routed || res.Rejected {
		t.Fatalf("第一条应被接受: %+v err=%v", res, err)
	}
	res, err = sess.Publish(&plugin.Message{Body: []byte("b")}, "", q.Name, false)
	if err != nil {
		t.Fatalf("发布不应报错: %v", err)
	}
	if !res.Rejected {
		t.Fatalf("第二条应被拒绝，实际 %+v", res)
	}
	if n, ok := queueCount(t, sess, q.Name, qArgs); !ok || n != 1 {
		t.Fatalf("队列应只有 1 条，实际 %d（读取成功=%v）", n, ok)
	}
}

// TestPriorityQueueOrdering 覆盖 x-max-priority：高优先级先投递。
func TestPriorityQueueOrdering(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{
		Name:      "prio.q",
		Arguments: map[string]any{"x-max-priority": int32(10)},
	})
	for _, p := range []uint8{1, 5, 3} {
		if _, err := sess.Publish(&plugin.Message{
			Properties: plugin.Properties{Priority: p},
			Body:       []byte{byte('0' + p)},
		}, "", q.Name, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}
	for _, want := range []string{"5", "3", "1"} {
		d, ok, _ := sess.Get(q.Name, true)
		if !ok || string(d.Message.Body) != want {
			t.Fatalf("优先级顺序错误：期望 %q，实际 %q", want, d.Message.Body)
		}
	}
}

// TestQueueExpires 覆盖 x-expires：空闲队列到期后被删除。
func TestQueueExpires(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	mustDeclareQueue(t, sess, plugin.QueueDeclare{
		Name:      "expiring.q",
		Arguments: map[string]any{"x-expires": int32(100)}, // 100ms
	})

	waitFor(t, 3*time.Second, "空闲队列按 x-expires 被删除", func() bool {
		_, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "expiring.q", Passive: true})
		return err != nil // 已不存在 → 被动声明报 404
	})
}

// TestQueueTypeValidation 覆盖队列类型的声明校验：
// 未实现的类型必须明确报错（而不是静默建成 classic），
// 已实现的 quorum 必须满足它的语义前提（durable / 非独占 / 非自动删除 / 有名字）。
//
// quorum 队列的行为本身由 test/unit/broker/quorum_test.go 在真实集群上验证。
func TestQueueTypeValidation(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	// stream 尚未实现：明确报 NOT_IMPLEMENTED，而不是悄悄建成 classic
	_, err := sess.DeclareQueue(plugin.QueueDeclare{
		Name:      "stream.q",
		Arguments: map[string]any{"x-queue-type": "stream"},
	})
	if ke, ok := err.(*plugin.Error); !ok || ke.Kind != plugin.KindNotImplemented {
		t.Fatalf("未实现的队列类型应报 KindNotImplemented，实际 %v", err)
	}

	// quorum 需要 durable：不满足时 406，而不是建成一条没有复制保证的队列
	_, err = sess.DeclareQueue(plugin.QueueDeclare{
		Name:      "quorum.q",
		Arguments: map[string]any{"x-queue-type": "quorum"},
	})
	if ke, ok := err.(*plugin.Error); !ok || ke.Kind != plugin.KindPreconditionFailed {
		t.Fatalf("非 durable 的 quorum 队列应报 406，实际 %v", err)
	}

	// quorum 不支持的参数也要明确拒绝（x-expires / x-max-priority）
	_, err = sess.DeclareQueue(plugin.QueueDeclare{
		Name:    "quorum.ttl.q",
		Durable: true,
		Arguments: map[string]any{
			"x-queue-type": "quorum",
			"x-expires":    60000,
		},
	})
	if ke, ok := err.(*plugin.Error); !ok || ke.Kind != plugin.KindPreconditionFailed {
		t.Fatalf("quorum 队列的 x-expires 应报 406，实际 %v", err)
	}

	// classic 显式声明应被接受
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{
		Name:      "classic.q",
		Arguments: map[string]any{"x-queue-type": "classic"},
	}); err != nil {
		t.Fatalf("classic 队列应被接受: %v", err)
	}
}

// TestPermissionEnforcement 覆盖越权访问返回 403。
func TestPermissionEnforcement(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	// 受限用户：只能操作 app. 前缀的资源
	cfg.Users["limited"] = config.User{
		Password: "secret",
		Permissions: map[string]config.Permission{
			"/": {Configure: `^app\.`, Write: `^app\.`, Read: `^app\.`},
		},
	}
	b := mustBroker(t, cfg)
	t.Cleanup(b.Close)

	sess, err := testSessionOf(t, b, "limited", "secret")
	if err != nil {
		t.Fatalf("受限用户应能登录并打开 vhost: %v", err)
	}

	// 允许：声明并绑定 app. 前缀资源
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{Name: "app.ex", Type: plugin.ExchangeDirect}); err != nil {
		t.Fatalf("声明 app.ex 应被允许: %v", err)
	}
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "app.q"}); err != nil {
		t.Fatalf("声明 app.q 应被允许: %v", err)
	}
	if err := sess.BindQueue("app.q", "app.ex", "app.q", nil); err != nil {
		t.Fatalf("绑定应被允许: %v", err)
	}
	if _, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "app.ex", "app.q", false); err != nil {
		t.Fatalf("发布到 app.ex 应被允许: %v", err)
	}
	if _, ok, err := sess.Get("app.q", true); err != nil || !ok {
		t.Fatalf("读取 app.q 应被允许: ok=%v err=%v", ok, err)
	}

	// 拒绝：configure 越权
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "other.q"}); !isKind(err, plugin.KindAccessRefused) {
		t.Fatalf("声明 other.q 应 403，实际 %v", err)
	}
	// 拒绝：write 越权（默认交换机名字为空串，不匹配 ^app\.）
	if _, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "", "app.q", false); !isKind(err, plugin.KindAccessRefused) {
		t.Fatalf("发布到默认交换机应 403，实际 %v", err)
	}
	// 拒绝：read 越权（权限检查先于存在性检查，避免泄露资源是否存在）
	if _, _, err := sess.Get("other.q", true); !isKind(err, plugin.KindAccessRefused) {
		t.Fatalf("读取 other.q 应 403，实际 %v", err)
	}
	// 拒绝：绑定到无权写入的交换机
	if err := sess.BindQueue("app.q", "amq.direct", "k", nil); !isKind(err, plugin.KindAccessRefused) {
		t.Fatalf("绑定到 amq.direct 应 403，实际 %v", err)
	}
}
