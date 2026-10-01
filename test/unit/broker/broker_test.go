// Package broker_test 从包外驱动内核：只使用 internal/broker 的导出 API，
// 断言一律落在对外可观察的行为上（路由、FIFO、确认、独占、保留名）。
//
// 文件位置与包形式的约定见 AGENTS.md §10.1。
package broker_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// mustBroker 构造内核；失败时终止用例。
//
// 集群/元数据层让 broker.New 有了失败路径（成员表配置错误、端口占用、日志损坏），
// 测试里遇到即视为用例失败。
func mustBroker(t *testing.T, cfg *config.Config) *broker.Broker {
	t.Helper()
	b, err := broker.New(discardLogger(), cfg)
	if err != nil {
		t.Fatalf("构造内核失败: %v", err)
	}
	return b
}

// newTestBroker 返回带后台协程的内核（TTL 扫描、死信派发与水位检查依赖它）。
//
// 数据目录指向测试专属临时目录：durable 队列会真的落盘，绝不能写进仓库工作区。
func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载默认配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	b := mustBroker(t, cfg)
	t.Cleanup(b.Close)
	return b
}

// testSessionOf 走完整的"认证 → 打开 vhost"路径取会话，以便覆盖权限逻辑。
func testSessionOf(t *testing.T, b *broker.Broker, user, pass string) (plugin.Session, error) {
	t.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := b.NewSession(remote, local)
	resp := append([]byte("\x00"+user+"\x00"), []byte(pass)...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	return core.Session("/")
}

// newTestSession 以内置 guest 用户打开默认 vhost 的会话。
//
// guest 在 config.Load("") 后自动补齐全部 vhost 的完全权限，因此这里无需额外授权。
func newTestSession(t *testing.T, b *broker.Broker) plugin.Session {
	t.Helper()
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	return sess
}

func mustDeclareQueue(t *testing.T, sess plugin.Session, req plugin.QueueDeclare) plugin.QueueInfo {
	t.Helper()
	// 测试里要的"普通临时队列"补成会话私有队列：瞬时非独占队列已被禁止（见 asSessionQueue）。
	info, err := sess.DeclareQueue(asSessionQueue(req))
	if err != nil {
		t.Fatalf("声明队列失败: %v", err)
	}
	return info
}

// TestDefaultExchangeRoutesByQueueName 覆盖最常用的发布方式：
// 不写交换机、直接以队列名当 routing key（默认交换机的隐式绑定）。
func TestDefaultExchangeRoutesByQueueName(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})

	res, err := sess.Publish(&plugin.Message{Body: []byte("hi")}, "", q.Name, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatalf("默认交换机未按队列名 %q 路由", q.Name)
	}

	d, ok, err := sess.Get(q.Name, true)
	if err != nil {
		t.Fatalf("get 失败: %v", err)
	}
	if !ok {
		t.Fatalf("队列为空：消息没有进入队列")
	}
	if string(d.Message.Body) != "hi" {
		t.Fatalf("消息内容错误: %q", d.Message.Body)
	}
}

// TestDefaultExchangeUnknownQueue 未命中时不应报错，只是没路由出去。
func TestDefaultExchangeUnknownQueue(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	res, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "", "no.such.queue", false)
	if err != nil {
		t.Fatalf("发布不应报错: %v", err)
	}
	if res.Routed {
		t.Fatalf("发布到不存在的队列名竟然被路由了")
	}
}

// TestFanoutRoutingToMultipleQueues 验证 fanout 广播与 basic.get 取回。
func TestFanoutRoutingToMultipleQueues(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{
		Name: "test.fanout", Type: plugin.ExchangeFanout,
	}); err != nil {
		t.Fatalf("声明交换机失败: %v", err)
	}
	q1 := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})
	q2 := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})
	for _, q := range []string{q1.Name, q2.Name} {
		if err := sess.BindQueue(q, "test.fanout", "ignored", nil); err != nil {
			t.Fatalf("绑定失败: %v", err)
		}
	}

	res, err := sess.Publish(&plugin.Message{Body: []byte("broadcast")}, "test.fanout", "", false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatalf("fanout 未路由到任何队列")
	}
	for i, q := range []string{q1.Name, q2.Name} {
		d, ok, err := sess.Get(q, true)
		if err != nil {
			t.Fatalf("get q%d 失败: %v", i+1, err)
		}
		if !ok {
			t.Fatalf("fanout 未投递到队列 %d", i+1)
		}
		if string(d.Message.Body) != "broadcast" {
			t.Fatalf("队列 %d 内容错误: %q", i+1, d.Message.Body)
		}
	}
}

// TestTopicRoutingAndDedup 验证通配匹配与"同一队列多条绑定只收一份"。
func TestTopicRoutingAndDedup(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{
		Name: "test.topic", Type: plugin.ExchangeTopic,
	}); err != nil {
		t.Fatalf("声明交换机失败: %v", err)
	}
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})
	for _, key := range []string{"a.*.c", "a.#", "x.#"} {
		if err := sess.BindQueue(q.Name, "test.topic", key, nil); err != nil {
			t.Fatalf("绑定 %q 失败: %v", key, err)
		}
	}

	if _, err := sess.Publish(&plugin.Message{Body: []byte("t")}, "test.topic", "a.b.c", false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if _, ok, _ := sess.Get(q.Name, true); !ok {
		t.Fatalf("topic 未命中 a.b.c")
	}
	if _, ok, _ := sess.Get(q.Name, true); ok {
		t.Fatalf("同一队列的多条绑定未去重")
	}
}

// TestTopicMatch 把原先对内部函数 topicMatch(pattern, key) 的直测改写为行为化表驱动：
// 每个用例声明一个专属队列绑定到 topic 交换机，发布后断言"该不该收到"。
//
// 这样锁住的是 RabbitMQ 兼容的可见路由行为，实现换成 Trie / 索引也不会失效。
func TestTopicMatch(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	const exchange = "test.topicmatch"
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{
		Name: exchange, Type: plugin.ExchangeTopic,
	}); err != nil {
		t.Fatalf("声明交换机失败: %v", err)
	}

	cases := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"a.b.c", "a.b.c", true},
		{"a.b.c", "a.b.d", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.x.c", false},
		{"a.#", "a.b.c", true},
		{"a.#", "a", true},
		{"#", "a.b.c", true},
		{"#", "", true},
		{"a.#.c", "a.b.x.c", true},
		{"a.#.c", "a.c", true},
		{"*.b", "a.b", true},
		{"*.b", "a.x.b", false},
		{"", "", true},
		{"a.b", "", false},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/%s", c.pattern, c.key), func(t *testing.T) {
			q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})
			if err := sess.BindQueue(q.Name, exchange, c.pattern, nil); err != nil {
				t.Fatalf("绑定 %q 失败: %v", c.pattern, err)
			}

			// 发布结果里的 Routed 表示"整台交换机是否有命中"，会被先前用例残留的
			// 绑定（如 "#"）污染，因此这里只用本用例专属队列的收信情况判定。
			if _, err := sess.Publish(&plugin.Message{Body: []byte("t")}, exchange, c.key, false); err != nil {
				t.Fatalf("发布失败: %v", err)
			}
			if _, got, err := sess.Get(q.Name, true); err != nil {
				t.Fatalf("get 失败: %v", err)
			} else if got != c.want {
				t.Fatalf("pattern=%q key=%q: 队列收到=%v, want %v", c.pattern, c.key, got, c.want)
			}
		})
	}
}

// TestQueueFIFOWithPrefetch 验证 FIFO 顺序与 prefetch 闸门。
func TestQueueFIFOWithPrefetch(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})

	for i := 0; i < 3; i++ {
		if _, err := sess.Publish(&plugin.Message{Body: []byte{byte('0' + i)}}, "", q.Name, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}

	var got []string
	for i := 0; i < 3; i++ {
		d, ok, err := sess.Get(q.Name, false)
		if err != nil || !ok {
			t.Fatalf("第 %d 次 get 失败: ok=%v err=%v", i, ok, err)
		}
		got = append(got, string(d.Message.Body))
		d.Settle(plugin.SettleAck) // 确认
	}
	want := []string{"0", "1", "2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FIFO 顺序错误: got %v want %v", got, want)
		}
	}
}

// TestRequeueMarksRedelivered 验证重新入队会置 redelivered 并回到队首。
func TestRequeueMarksRedelivered(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})

	if _, err := sess.Publish(&plugin.Message{Body: []byte("retry")}, "", q.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	d, ok, err := sess.Get(q.Name, false)
	if err != nil || !ok {
		t.Fatalf("get 失败: ok=%v err=%v", ok, err)
	}
	if d.Redelivered {
		t.Fatalf("首次投递不应是 redelivered")
	}
	d.Settle(plugin.SettleRequeue) // 重新入队

	d2, ok, err := sess.Get(q.Name, false)
	if err != nil || !ok {
		t.Fatalf("重投后 get 失败: ok=%v err=%v", ok, err)
	}
	if !d2.Redelivered {
		t.Fatalf("重新入队的消息应置 redelivered=true")
	}
}

// TestConsumeDispatchAndAck 验证消费者投递、prefetch 与确认后继续投递。
func TestConsumeDispatchAndAck(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Exclusive: true})

	delivered := make(chan *plugin.Delivery, 8)
	_, err := sess.Consume(plugin.Subscription{
		Queue:    q.Name,
		Prefetch: 1,
		Deliver: func(d *plugin.Delivery) error {
			delivered <- d
			return nil
		},
	})
	if err != nil {
		t.Fatalf("消费失败: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := sess.Publish(&plugin.Message{Body: []byte{byte('a' + i)}}, "", q.Name, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}

	// prefetch=1：必须逐条确认后才能收到下一条
	for i := 0; i < 3; i++ {
		select {
		case d := <-delivered:
			if string(d.Message.Body) != string([]byte{byte('a' + i)}) {
				t.Fatalf("第 %d 条内容错误: %q", i, d.Message.Body)
			}
			d.Settle(plugin.SettleAck)
		default:
			t.Fatalf("第 %d 条未被投递（prefetch 闸门或投递循环有问题）", i)
		}
	}
}

// TestReservedNameAndEquivalence 验证保留名与声明等价性检查。
func TestReservedNameAndEquivalence(t *testing.T) {
	sess := newTestSession(t, newTestBroker(t))

	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "amq.illegal"}); err == nil {
		t.Fatalf("保留名声明应被拒绝")
	} else if ke, ok := err.(*plugin.Error); !ok || ke.Kind != plugin.KindAccessRefused {
		t.Fatalf("保留名应返回 KindAccessRefused，实际: %v", err)
	}

	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "dur.q", Durable: true}); err != nil {
		t.Fatalf("首次声明失败: %v", err)
	}
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "dur.q"}); err == nil {
		t.Fatalf("参数不一致重声明应被拒绝")
	} else if ke, ok := err.(*plugin.Error); !ok || ke.Kind != plugin.KindPreconditionFailed {
		t.Fatalf("应返回 KindPreconditionFailed，实际: %v", err)
	}
}

// TestExclusiveQueueOwnership 验证独占队列被其他会话使用时报 405。
//
// 两个会话必须来自两次独立的 NewSession（即两条不同连接），
// 否则会话标识相同，独占归属检查不会触发。
func TestExclusiveQueueOwnership(t *testing.T) {
	b := newTestBroker(t)
	owner := newTestSession(t, b)
	q := mustDeclareQueue(t, owner, plugin.QueueDeclare{Name: "excl.q", Exclusive: true})

	other := newTestSession(t, b)
	if _, err := other.DeclareQueue(plugin.QueueDeclare{Name: q.Name, Exclusive: true}); err == nil {
		t.Fatalf("其他会话声明同一独占队列应被拒绝")
	} else if ke, ok := err.(*plugin.Error); !ok || ke.Kind != plugin.KindResourceLocked {
		t.Fatalf("应返回 KindResourceLocked，实际: %v", err)
	}
}

// TestSessionCloseDeletesExclusiveQueue 验证会话关闭时独占队列被清理。
func TestSessionCloseDeletesExclusiveQueue(t *testing.T) {
	b := newTestBroker(t)
	sess := newTestSession(t, b)
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "gone.q", Exclusive: true})

	sess.Close()

	if _, ok := b.QueueSnapshot("/", q.Name); ok {
		t.Fatalf("会话关闭后独占队列应被删除")
	}
}
