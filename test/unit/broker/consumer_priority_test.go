package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M8-14 消费者优先级（订阅的 Priority，对应 AMQP 的 `x-priority`）在内核侧的分配规则。
//
// 规则来自**实测 RabbitMQ 4.3**（见 README 的 M8-14 条目）：
//   - 只有多个消费者**同时还有投递额度**时优先级才起作用：优先级高的先拿消息；
//   - 同优先级之间轮询（两个默认优先级的消费者各拿一半）；
//   - 高优先级消费者把额度（prefetch）用尽后，低优先级消费者照常收到消息 —— 不会被饿死。

// priorityCounter 统计某消费者收到的消息，并保留未确认投递供用例按需结算。
type priorityCounter struct {
	mu      sync.Mutex
	n       int
	pending []*plugin.Delivery
	autoAck bool
}

func (c *priorityCounter) deliver(d *plugin.Delivery) error {
	c.mu.Lock()
	c.n++
	auto := c.autoAck
	if !auto {
		c.pending = append(c.pending, d)
	}
	c.mu.Unlock()
	// 结算必须在锁外做：Settle 会回到内核的派发路径，持锁调用会造成自锁
	if auto {
		d.Settle(plugin.SettleAck)
	}
	return nil
}

func (c *priorityCounter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// settleOne 结算最早一条未确认投递，返回是否确有可结算的投递。
func (c *priorityCounter) settleOne() bool {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return false
	}
	d := c.pending[0]
	c.pending = c.pending[1:]
	c.mu.Unlock()
	d.Settle(plugin.SettleAck)
	return true
}

// subscribePriority 以给定优先级注册一个 no-ack 消费者（次数统计进 c）。
func subscribePriority(t *testing.T, sess plugin.Session, tag, queue string, prio int, c *priorityCounter) {
	t.Helper()
	_, err := sess.Consume(plugin.Subscription{
		Tag:      tag,
		Queue:    queue,
		NoAck:    true,
		Priority: prio,
		Deliver:  c.deliver,
		Cancel:   func(string) {},
	})
	if err != nil {
		t.Fatalf("注册消费者 %s 失败: %v", tag, err)
	}
}

// publishN 向队列发布 n 条消息。
func publishN(t *testing.T, sess plugin.Session, queue string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := sess.Publish(&plugin.Message{Body: []byte("m")}, "", queue, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}
}

func TestConsumerPriorityHigherPriorityGetsMessages(t *testing.T) {
	// 先注册低优先级、再注册高优先级，然后才灌消息：
	// 结果必须由**优先级**决定，而不是注册顺序。
	b := newTestBroker(t)
	sess := newTestSession(t, b)
	const qname = "py.prio.high"
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: qname, Durable: true})

	low := &priorityCounter{autoAck: true}
	high := &priorityCounter{autoAck: true}
	subscribePriority(t, sess, "low", qname, 1, low)
	subscribePriority(t, sess, "high", qname, 10, high)

	const total = 40
	publishN(t, sess, qname, total)

	waitFor(t, time.Second, "消息全部投出", func() bool { return low.total()+high.total() == total })
	if got := high.total(); got != total {
		t.Fatalf("高优先级消费者应拿到全部 %d 条，实际 %d 条", total, got)
	}
	if got := low.total(); got != 0 {
		t.Fatalf("低优先级消费者在有更高优先级消费者可用时不应拿到消息，实际 %d 条", got)
	}
}

func TestConsumerPriorityEqualPriorityRoundRobin(t *testing.T) {
	b := newTestBroker(t)
	sess := newTestSession(t, b)
	const qname = "py.prio.equal"
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: qname, Durable: true})

	a := &priorityCounter{autoAck: true}
	c := &priorityCounter{autoAck: true}
	subscribePriority(t, sess, "a", qname, 0, a) // 未显式给优先级 = 0
	subscribePriority(t, sess, "b", qname, 0, c)

	const total = 40
	publishN(t, sess, qname, total)

	waitFor(t, time.Second, "消息全部投出", func() bool { return a.total()+c.total() == total })
	if a.total() == 0 || c.total() == 0 {
		t.Fatalf("同优先级必须轮询，实际分配 %d / %d", a.total(), c.total())
	}
	if diff := a.total() - c.total(); diff > 2 || diff < -2 {
		t.Fatalf("同优先级的分配应基本均衡，实际 %d / %d", a.total(), c.total())
	}
}

func TestConsumerPriorityDoesNotStarveSaturatedHigherPriorityConsumer(t *testing.T) {
	// prefetch=1 且**暂不确认**：高优先级消费者占满 1 条额度后，
	// 下一条必须落到低优先级消费者上（同优先级与低优先级都不会被饿死）。
	// 之后谁确认了，额度释放给谁 —— 这是"优先级只在有额度时起作用"的直接体现。
	b := newTestBroker(t)
	sess := newTestSession(t, b)
	const qname = "py.prio.saturate"
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: qname, Durable: true})

	high := &priorityCounter{}
	low := &priorityCounter{}
	if _, err := sess.Consume(plugin.Subscription{
		Tag: "high", Queue: qname, Prefetch: 1, Priority: 10, Deliver: high.deliver, Cancel: func(string) {},
	}); err != nil {
		t.Fatalf("注册高优先级消费者失败: %v", err)
	}
	if _, err := sess.Consume(plugin.Subscription{
		Tag: "low", Queue: qname, Prefetch: 1, Priority: 1, Deliver: low.deliver, Cancel: func(string) {},
	}); err != nil {
		t.Fatalf("注册低优先级消费者失败: %v", err)
	}

	const total = 10
	publishN(t, sess, qname, total)

	waitFor(t, time.Second, "两个消费者各拿到一条", func() bool {
		return high.total() == 1 && low.total() == 1
	})

	// 低优先级消费者确认后额度释放：仍然只有它能收（高优先级仍占满），证明它没被优先级挡住
	if !low.settleOne() {
		t.Fatal("低优先级消费者应持有未确认投递")
	}
	waitFor(t, time.Second, "低优先级消费者在额度释放后继续收到消息", func() bool { return low.total() == 2 })

	// 高优先级消费者确认后，下一条回到它手里 —— 优先级仍然生效
	if !high.settleOne() {
		t.Fatal("高优先级消费者应持有未确认投递")
	}
	waitFor(t, time.Second, "高优先级消费者在额度释放后继续收到消息", func() bool { return high.total() == 2 })

	left, ok := queueCount(t, sess, qname, nil)
	if !ok {
		t.Fatal("读取队列统计失败")
	}
	if want := uint32(total - high.total() - low.total()); left != want {
		t.Fatalf("队列里应剩 %d 条未投出，实际 %d 条", want, left)
	}
}
