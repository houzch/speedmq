// 本文件是 M7d 的性能基线：可复现的吞吐基准，作为"优化前后对比"的依据。
//
// 只在 `go test -bench` 下运行，不参与常规 `go test`（`-run '^$'` 即可跳过功能用例）。
// 基准走的是**真实的内核路径**（Session → 路由 → 队列 → 投递 → 结算），
// 不含协议编解码与网络 IO —— 因此它衡量的是内核自身，而不是某个客户端的实现质量。
package broker_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

func benchBroker(b *testing.B) *broker.Broker {
	b.Helper()
	cfg, err := config.Load("")
	if err != nil {
		b.Fatalf("加载默认配置失败: %v", err)
	}
	cfg.DataDir = b.TempDir()
	br, err := broker.New(discardLogger(), cfg)
	if err != nil {
		b.Fatalf("构造内核失败: %v", err)
	}
	b.Cleanup(br.Close)
	return br
}

func benchSession(b *testing.B, br *broker.Broker) plugin.Session {
	b.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := br.NewSession(remote, local)
	resp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		b.Fatalf("认证失败: %v", err)
	}
	sess, err := core.Session("/")
	if err != nil {
		b.Fatalf("打开会话失败: %v", err)
	}
	return sess
}

// benchRoundTrip 建立"发布 b.N 条 → 消费并结算 b.N 条"的端到端基准，返回已投递计数。
//
// 用 NoAck 消费者把结算成本压到最小，专注测"路由 + 入队 + 投递"这段主链路。
func benchRoundTrip(b *testing.B, body []byte) {
	b.Helper()
	br := benchBroker(b)
	sess := benchSession(b, br)
	info, err := sess.DeclareQueue(plugin.QueueDeclare{Exclusive: true})
	if err != nil {
		b.Fatalf("声明队列失败: %v", err)
	}

	var received atomic.Int64
	done := make(chan struct{})
	if _, err := sess.Consume(plugin.Subscription{
		Queue: info.Name,
		NoAck: true,
		Deliver: func(*plugin.Delivery) error {
			if received.Add(1) == int64(b.N) {
				close(done)
			}
			return nil
		},
	}); err != nil {
		b.Fatalf("注册消费者失败: %v", err)
	}

	msg := &plugin.Message{Body: body}
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sess.Publish(msg, "", info.Name, false); err != nil {
			b.Fatalf("发布失败: %v", err)
		}
	}
	<-done
	b.StopTimer()
}

// BenchmarkPublishConsume 是最常用路径的基线：小消息（256 B）的发布→消费闭环。
func BenchmarkPublishConsume(b *testing.B) {
	benchRoundTrip(b, make([]byte, 256))
}

// BenchmarkPublishConsumeLargeBody 覆盖大消息路径：1 MiB 消息体。
//
// 它用来暴露"大消息是否被整体拷贝多次 / 是否被当作小消息处理"这类问题。
func BenchmarkPublishConsumeLargeBody(b *testing.B) {
	benchRoundTrip(b, make([]byte, 1<<20))
}

// BenchmarkSessionOpen 覆盖连接规模：一次"认证 + 打开 vhost"的成本。
//
// 连接数上限由单连接成本与 goroutine/内存开销决定，这里给出单连接成本的基线。
func BenchmarkSessionOpen(b *testing.B) {
	br := benchBroker(b)
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	resp := append([]byte("\x00guest\x00"), []byte("guest")...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		core := br.NewSession(remote, local)
		if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
			b.Fatalf("认证失败: %v", err)
		}
		sess, err := core.Session("/")
		if err != nil {
			b.Fatalf("打开会话失败: %v", err)
		}
		sess.Close()
		core.Close()
	}
}
