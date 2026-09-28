package broker_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M4 的持久化与流控语义：
// 重启恢复、瞬时消息不落盘、fsync=none 不落盘，以及内存/磁盘水位阻塞生产者。

// m4Config 返回开启持久化的测试配置。
//
// 数据目录是临时目录且会被"重启"用例复用，因此不能是 t.TempDir() 的每调用一次新目录。
func m4Config(t *testing.T, fsync string) *config.Config {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	cfg.Storage.Fsync = fsync
	cfg.Storage.FlushIntervalMS = 10
	return cfg
}

// publishPersistent 发布一条持久消息，并像 confirm 模式那样等待落盘完成。
func publishPersistent(t *testing.T, sess plugin.Session, queue, body string) {
	t.Helper()
	res, err := sess.Publish(&plugin.Message{
		Properties: plugin.Properties{DeliveryMode: 2},
		Body:       []byte(body),
	}, "", queue, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatalf("消息未路由到队列 %s", queue)
	}
	if res.Durable == nil {
		t.Fatalf("durable 队列 + 持久消息必须返回落盘等待函数")
	}
	if err := res.Durable(); err != nil {
		t.Fatalf("等待落盘失败: %v", err)
	}
}

// drain 取出队列中的全部消息并返回消息体与 redelivered 标记。
func drain(t *testing.T, sess plugin.Session, queue string) ([]string, []bool) {
	t.Helper()
	var bodies []string
	var redelivered []bool
	for {
		d, ok, err := sess.Get(queue, true)
		if err != nil {
			t.Fatalf("读取队列 %s 失败: %v", queue, err)
		}
		if !ok {
			return bodies, redelivered
		}
		bodies = append(bodies, string(d.Message.Body))
		redelivered = append(redelivered, d.Redelivered)
	}
}

// TestDurableQueueRecoveryAfterRestart 是 M4 的核心验收用例：
// 已确认的消息不丢失，未确认的消息回到队头并标记 redelivered。
func TestDurableQueueRecoveryAfterRestart(t *testing.T) {
	cfg := m4Config(t, "always")
	const queue = "m4.recover.q"

	b := mustBroker(t, cfg)
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: queue, Durable: true})
	for _, body := range []string{"m1", "m2", "m3"} {
		publishPersistent(t, sess, queue, body)
	}

	// m1：完整消费并确认 —— 重启后不应再出现
	d1, ok, err := sess.Get(queue, false)
	if err != nil || !ok {
		t.Fatalf("取第一条失败: ok=%v err=%v", ok, err)
	}
	if string(d1.Message.Body) != "m1" {
		t.Fatalf("FIFO 顺序错误: 首条为 %q", d1.Message.Body)
	}
	d1.Settle(plugin.SettleAck)

	// m2：已投递但未确认（模拟消费者在 ack 前掉线）—— 重启后必须回到队头
	d2, ok, err := sess.Get(queue, false)
	if err != nil || !ok {
		t.Fatalf("取第二条失败: ok=%v err=%v", ok, err)
	}
	if string(d2.Message.Body) != "m2" {
		t.Fatalf("FIFO 顺序错误: 第二条为 %q", d2.Message.Body)
	}
	// 故意不结算 d2

	b.Close() // 收尾刷盘

	// 重启：复用同一数据目录
	b2 := mustBroker(t, cfg)
	t.Cleanup(b2.Close)
	sess2, err := testSessionOf(t, b2, "guest", "guest")
	if err != nil {
		t.Fatalf("重启后打开会话失败: %v", err)
	}
	info := mustDeclareQueue(t, sess2, plugin.QueueDeclare{Name: queue, Durable: true})
	// 恢复出来的消息必须体现在声明响应里：客户端常据此判断"队列里是否还有存量"
	if info.MessageCount != 2 {
		t.Fatalf("重启后声明返回的消息数 = %d, want 2", info.MessageCount)
	}

	bodies, redelivered := drain(t, sess2, queue)
	want := []string{"m2", "m3"}
	if len(bodies) != len(want) {
		t.Fatalf("恢复的消息数 = %d (%v), want %d (%v)", len(bodies), bodies, len(want), want)
	}
	for i := range want {
		if bodies[i] != want[i] {
			t.Fatalf("恢复顺序错误: got %v want %v", bodies, want)
		}
		if !redelivered[i] {
			t.Fatalf("恢复的消息必须置 redelivered=true: %q", bodies[i])
		}
	}
}

// TestTransientMessageNotRecovered 覆盖"只有持久消息才落盘"：
// durable 队列里的 delivery-mode=1 消息重启后应当消失。
func TestTransientMessageNotRecovered(t *testing.T) {
	cfg := m4Config(t, "always")
	const queue = "m4.transient.q"

	b := mustBroker(t, cfg)
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: queue, Durable: true})

	res, err := sess.Publish(&plugin.Message{Body: []byte("transient")}, "", queue, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatalf("消息未路由到队列")
	}
	if res.Durable != nil {
		t.Fatalf("瞬时消息不应进入持久化路径")
	}
	b.Close()

	b2 := mustBroker(t, cfg)
	t.Cleanup(b2.Close)
	sess2, err := testSessionOf(t, b2, "guest", "guest")
	if err != nil {
		t.Fatalf("重启后打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess2, plugin.QueueDeclare{Name: queue, Durable: true})
	if _, ok, _ := sess2.Get(queue, true); ok {
		t.Fatalf("瞬时消息不应被恢复")
	}
}

// TestFsyncNoneDisablesPersistence 覆盖 none 档位：完全不落盘，消息只存在于内存。
func TestFsyncNoneDisablesPersistence(t *testing.T) {
	cfg := m4Config(t, "none")
	const queue = "m4.none.q"

	b := mustBroker(t, cfg)
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: queue, Durable: true})

	res, err := sess.Publish(&plugin.Message{
		Properties: plugin.Properties{DeliveryMode: 2},
		Body:       []byte("no-disk"),
	}, "", queue, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if res.Durable != nil {
		t.Fatalf("none 档位不应有落盘等待函数")
	}
	b.Close()

	b2 := mustBroker(t, cfg)
	t.Cleanup(b2.Close)
	sess2, err := testSessionOf(t, b2, "guest", "guest")
	if err != nil {
		t.Fatalf("重启后打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess2, plugin.QueueDeclare{Name: queue, Durable: true})
	if _, ok, _ := sess2.Get(queue, true); ok {
		t.Fatalf("none 档位不应有任何持久化数据")
	}
}

// TestDurableQueueDeleteRemovesStore 覆盖队列删除后的磁盘回收：
// 删掉队列再重启，同名队列不应"复活"旧消息。
func TestDurableQueueDeleteRemovesStore(t *testing.T) {
	cfg := m4Config(t, "always")
	const queue = "m4.deleted.q"

	b := mustBroker(t, cfg)
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: queue, Durable: true})
	publishPersistent(t, sess, queue, "gone")
	if _, err := sess.DeleteQueue(queue, false, false); err != nil {
		t.Fatalf("删除队列失败: %v", err)
	}
	b.Close()

	b2 := mustBroker(t, cfg)
	t.Cleanup(b2.Close)
	sess2, err := testSessionOf(t, b2, "guest", "guest")
	if err != nil {
		t.Fatalf("重启后打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess2, plugin.QueueDeclare{Name: queue, Durable: true})
	if bodies, _ := drain(t, sess2, queue); len(bodies) != 0 {
		t.Fatalf("已删除队列的消息不应残留: %v", bodies)
	}
}

// TestFlowControlBlocksAndUnblocks 覆盖内存/磁盘水位：
// 触发时阻塞生产者并广播 Connection.Blocked，恢复后解除阻塞并广播 Unblocked。
func TestFlowControlBlocksAndUnblocks(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	// 磁盘下限设成 1 TiB：任何测试机的可用空间都会低于它，必然触发水位
	cfg.Storage.DiskFreeLimit = 1 << 40
	cfg.Storage.MemoryHighWatermark = 0

	b := mustBroker(t, cfg)
	t.Cleanup(b.Close)

	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := b.NewSession(remote, local)
	t.Cleanup(core.Close)
	notify := core.Notifications()

	// 水位检查是秒级的，等待它把内核切换为阻塞
	waitFor(t, 5*time.Second, "内核进入资源水位阻塞", func() bool { return b.BlockedState() })

	select {
	case n := <-notify:
		if !n.Blocked {
			t.Fatalf("期望阻塞通知，实际 %+v", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("未收到 Connection.Blocked 通知")
	}

	// 通过 Core 认证后取得 vhost 会话（Publish 才会走到水位闸门）
	resp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	sess, err := core.Session("/")
	if err != nil {
		t.Fatalf("打开 vhost 会话失败: %v", err)
	}

	// 阻塞期间发布必须挂起（而不是丢弃消息）
	done := make(chan error, 1)
	go func() {
		_, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "amq.direct", "k", false)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("阻塞期间发布不应返回: err=%v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// 解除水位：发布必须恢复
	b.UpdateLimits(0, 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("解除阻塞后发布失败: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("解除阻塞后发布仍未返回")
	}

	select {
	case n := <-notify:
		if n.Blocked {
			t.Fatalf("期望解除阻塞通知，实际 %+v", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("未收到 Connection.Unblocked 通知")
	}
}
