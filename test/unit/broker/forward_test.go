package broker_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M6b 的跨节点消息转发：
//   - 在非 Owner 节点上发布 → 消息落到 Owner 的队列里（含消息头类型保真）；
//   - 在非 Owner 节点上消费（推送）→ Owner 把投递推回来，确认后队列深度归零；
//   - 在非 Owner 节点上主动拉取（basic.get）→ 未确认消息在 Owner 侧持有，结算回传；
//   - 客户端断开 / 代理节点进程消失 → 代理消费者被摘除，未确认消息回到队头（不丢）；
//   - 清空与删除也跨节点生效。
//
// 集群一律用真实 TCP 端口（见 cluster_test.go 的 startTestCluster），
// 因为跨节点转发的错往往只在线路上才暴露（M6 的编解码 bug 就是这么发现的）。

// deliverCollector 是"把投递送进 channel"的消费者替身。
type deliverCollector struct {
	ch chan *plugin.Delivery
}

func newCollector(queue string, prefetch uint16, noAck bool) (plugin.Subscription, *deliverCollector) {
	c := &deliverCollector{ch: make(chan *plugin.Delivery, 16)}
	sub := plugin.Subscription{
		Queue:    queue,
		NoAck:    noAck,
		Prefetch: prefetch,
		Deliver: func(d *plugin.Delivery) error {
			select {
			case c.ch <- d:
				return nil
			case <-time.After(10 * time.Second):
				return fmt.Errorf("测试投递缓冲已满")
			}
		},
		Cancel: func(string) {},
	}
	return sub, c
}

func (c *deliverCollector) next(t *testing.T, desc string) *plugin.Delivery {
	t.Helper()
	select {
	case d := <-c.ch:
		return d
	case <-time.After(10 * time.Second):
		t.Fatalf("等待投递超时: %s", desc)
		return nil
	}
}

// forwardFixture 把"队列 Owner、消费节点、发布节点"三者的角色固定下来，
// 避免用例里到处出现 nodes[0]/nodes[1] 这种靠位置猜角色的写法。
type forwardFixture struct {
	nodes     []*clusterNode
	owner     *clusterNode
	consumer  *clusterNode
	publisher *clusterNode
}

func newForwardFixture(t *testing.T) *forwardFixture {
	t.Helper()
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	return &forwardFixture{nodes: nodes, owner: nodes[0], consumer: nodes[1], publisher: nodes[2]}
}

// declareDurableOn 在 Owner 节点上声明一个 durable 队列，并等到它在消费节点可见。
func (f *forwardFixture) declareDurableOn(t *testing.T, name string) {
	t.Helper()
	sess, err := testSessionOf(t, f.owner.b, "guest", "guest")
	if err != nil {
		t.Fatalf("在 Owner 节点打开会话失败: %v", err)
	}
	defer sess.Close()
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: name, Durable: true})
	waitFor(t, 5*time.Second, "队列复制到消费节点", func() bool {
		_, ok := f.consumer.b.QueueSnapshot("/", name)
		return ok
	})
}

// session 在指定节点上以 guest 打开会话。
func session(t *testing.T, node *clusterNode) plugin.Session {
	t.Helper()
	sess, err := testSessionOf(t, node.b, "guest", "guest")
	if err != nil {
		t.Fatalf("在节点 %s 上打开会话失败: %v", node.id, err)
	}
	return sess
}

// ownerQueue 返回 Owner 节点上该队列的本地快照（直接读，不经过转发）。
func ownerQueue(t *testing.T, f *forwardFixture, name string) broker.QueueSnapshot {
	t.Helper()
	q, ok := f.owner.b.QueueSnapshot("/", name)
	if !ok {
		t.Fatalf("Owner 节点上找不到队列 %s", name)
	}
	return q
}

// TestForwardPublishAndConsumeAcrossNodes 是跨节点转发的核心用例：
// 发布在 C、队列数据在 A、消费在 B，三者互不相同。
func TestForwardPublishAndConsumeAcrossNodes(t *testing.T) {
	const queue = "m6b.push.q"
	f := newForwardFixture(t)
	f.declareDurableOn(t, queue)

	consSess := session(t, f.consumer)
	defer consSess.Close()
	sub, collector := newCollector(queue, 5, false)
	if _, err := consSess.Consume(sub); err != nil {
		t.Fatalf("在非 Owner 节点上消费失败: %v", err)
	}
	// Owner 侧应当出现一个代理消费者。
	waitFor(t, 5*time.Second, "Owner 上出现代理消费者", func() bool {
		return ownerQueue(t, f, queue).ConsumerCount == 1
	})

	pubSess := session(t, f.publisher)
	defer pubSess.Close()
	// 带上一个 int32 消息头：跨节点后类型必须原样保留（这正是消息编解码器存在的理由）。
	res, err := pubSess.Publish(&plugin.Message{
		Properties: plugin.Properties{Headers: map[string]any{"x-count": int32(7)}},
		Body:       []byte("across-nodes"),
	}, "", queue, false)
	if err != nil || !res.Routed {
		t.Fatalf("在非 Owner 节点上发布失败: routed=%v err=%v", res.Routed, err)
	}

	d := collector.next(t, "跨节点投递")
	if string(d.Message.Body) != "across-nodes" {
		t.Fatalf("消息体不一致: got %q", d.Message.Body)
	}
	if got, ok := d.Message.Properties.Headers["x-count"]; !ok || got != int32(7) {
		t.Fatalf("消息头未保真: got %#v (%T)", got, got)
	}
	d.Settle(plugin.SettleAck)

	// 确认之后 Owner 的队列应当回到全空。
	waitFor(t, 5*time.Second, "Owner 队列的未确认消息被确认掉", func() bool {
		q := ownerQueue(t, f, queue)
		return q.Ready == 0 && q.Unacked == 0
	})
}

// TestForwardGetAcrossNodes 覆盖跨节点 basic.get：未确认消息在 Owner 侧持有，结算回报。
func TestForwardGetAcrossNodes(t *testing.T) {
	const queue = "m6b.get.q"
	f := newForwardFixture(t)
	f.declareDurableOn(t, queue)

	pubSess := session(t, f.publisher)
	defer pubSess.Close()
	if _, err := pubSess.Publish(&plugin.Message{Body: []byte("pulled")}, "", queue, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	consSess := session(t, f.consumer)
	defer consSess.Close()
	d, ok, err := consSess.Get(queue, false)
	if err != nil || !ok {
		t.Fatalf("跨节点拉取失败: ok=%v err=%v", ok, err)
	}
	if string(d.Message.Body) != "pulled" {
		t.Fatalf("消息体不一致: got %q", d.Message.Body)
	}
	// 拉取未确认期间，Owner 侧应把它计入未确认。
	waitFor(t, 5*time.Second, "Owner 记录到未确认消息", func() bool {
		return ownerQueue(t, f, queue).Unacked == 1
	})
	d.Settle(plugin.SettleAck)
	waitFor(t, 5*time.Second, "结算回报到 Owner", func() bool {
		q := ownerQueue(t, f, queue)
		return q.Ready == 0 && q.Unacked == 0
	})
}

// TestForwardConsumerCancelOnClientDisconnect：客户端断开（会话关闭）时，
// 代理消费者必须被摘除，它未确认的消息回到队头 —— 断连不丢消息。
func TestForwardConsumerCancelOnClientDisconnect(t *testing.T) {
	const queue = "m6b.cancel.q"
	f := newForwardFixture(t)
	f.declareDurableOn(t, queue)

	consSess := session(t, f.consumer)
	sub, collector := newCollector(queue, 1, false)
	if _, err := consSess.Consume(sub); err != nil {
		t.Fatalf("消费失败: %v", err)
	}

	pubSess := session(t, f.publisher)
	defer pubSess.Close()
	if _, err := pubSess.Publish(&plugin.Message{Body: []byte("held")}, "", queue, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	d := collector.next(t, "投递")
	// 故意不结算，直接断开客户端：消息应当回到 Owner 的队头。
	consSess.Close()

	waitFor(t, 5*time.Second, "代理消费者被摘除且消息回到队头", func() bool {
		q := ownerQueue(t, f, queue)
		return q.ConsumerCount == 0 && q.Ready == 1 && q.Unacked == 0
	})
	_ = d
}

// TestForwardProxyNodeDeathRequeues：代理节点整个进程消失（不只是客户端断开）时，
// Owner 通过投递失败立刻摘掉消费者并把消息放回队列，而不是让消息永远卡在未确认里。
func TestForwardProxyNodeDeathRequeues(t *testing.T) {
	const queue = "m6b.death.q"
	f := newForwardFixture(t)
	f.declareDurableOn(t, queue)

	consSess := session(t, f.consumer)
	sub, _ := newCollector(queue, 1, false)
	if _, err := consSess.Consume(sub); err != nil {
		t.Fatalf("消费失败: %v", err)
	}
	waitFor(t, 5*time.Second, "Owner 上出现代理消费者", func() bool {
		return ownerQueue(t, f, queue).ConsumerCount == 1
	})

	// 消费节点整个下线：它不会再续租，也不可能发取消通知。
	f.consumer.b.Close()

	pubSess := session(t, f.publisher)
	defer pubSess.Close()
	for i := 0; i < 3; i++ {
		if _, err := pubSess.Publish(&plugin.Message{Body: []byte(fmt.Sprintf("m%d", i))}, "", queue, false); err != nil {
			t.Fatalf("发布第 %d 条失败: %v", i, err)
		}
	}

	waitFor(t, 20*time.Second, "代理消费者被摘除且消息全部回到队列", func() bool {
		q := ownerQueue(t, f, queue)
		return q.ConsumerCount == 0 && q.Ready == 3 && q.Unacked == 0
	})
}

// TestForwardPurgeAndDeleteAcrossNodes：清空与删除同样跨节点生效（删除经集群元数据）。
func TestForwardPurgeAndDeleteAcrossNodes(t *testing.T) {
	const queue = "m6b.purge.q"
	f := newForwardFixture(t)
	f.declareDurableOn(t, queue)

	pubSess := session(t, f.publisher)
	defer pubSess.Close()
	for i := 0; i < 2; i++ {
		if _, err := pubSess.Publish(&plugin.Message{Body: []byte("x")}, "", queue, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}
	waitFor(t, 5*time.Second, "消息到达 Owner", func() bool {
		return ownerQueue(t, f, queue).Ready == 2
	})

	consSess := session(t, f.consumer)
	defer consSess.Close()
	// 统计也要来自 Owner：非 Owner 节点不能"看起来是空队列"。
	if q, ok := f.consumer.b.QueueSnapshot("/", queue); !ok || q.Ready != 2 {
		t.Fatalf("非 Owner 节点的队列统计应取自 Owner: ok=%v ready=%d", ok, q.Ready)
	}
	purged, err := consSess.PurgeQueue(queue)
	if err != nil || purged != 2 {
		t.Fatalf("跨节点清空失败: purged=%d err=%v", purged, err)
	}
	waitFor(t, 5*time.Second, "Owner 队列被清空", func() bool {
		return ownerQueue(t, f, queue).Ready == 0
	})

	// 删除也走集群元数据：Owner 上的队列对象与其磁盘数据一并消失。
	if _, err := consSess.DeleteQueue(queue, false, false); err != nil {
		t.Fatalf("跨节点删除失败: %v", err)
	}
	waitFor(t, 10*time.Second, "Owner 上的队列被删除", func() bool {
		_, ok := f.owner.b.QueueSnapshot("/", queue)
		return !ok
	})
}
