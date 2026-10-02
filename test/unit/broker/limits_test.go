package broker_test

import (
	"context"
	"net"
	"testing"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 vhost 级限制（max-connections / max-queues）的**运行期语义**：
// 限制要能落库、读回、删除，并且**真的拦住**新建队列与新建连接 ——
// 只存下来不生效的限制是运维最怕的那种"看起来配了"。

// openSessionWithPort 用指定的远端端口打开一个 guest 会话。
//
// 端口必须各不相同：连接登记表以"远端 → 本地"的连接名做键，同名会被后一条覆盖，
// 那样的用例根本验证不到"连接数超限"。
func openSessionWithPort(t *testing.T, b *broker.Broker, port int) (plugin.Session, error) {
	t.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := b.NewSession(remote, local)
	resp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	return core.Session("/")
}

// declareExclusive 声明一个会话私有的普通队列（测试里最常用的那种）。
func declareExclusive(t *testing.T, sess plugin.Session, name string) error {
	t.Helper()
	_, err := sess.DeclareQueue(plugin.QueueDeclare{Name: name, Exclusive: true})
	return err
}

// TestVHostQueueLimitBlocksNewQueue 覆盖 max-queues：达到上限后新队列被拒，
// 而**已存在**的队列重声明照常成功（限制是容量上限，不是既成事实的撤销开关）。
func TestVHostQueueLimitBlocksNewQueue(t *testing.T) {
	b := newTestBroker(t)
	sess := newTestSession(t, b)

	if err := b.SetVHostLimit("/", broker.LimitMaxQueues, 1); err != nil {
		t.Fatalf("设置 max-queues 失败: %v", err)
	}
	if err := declareExclusive(t, sess, "lim.q1"); err != nil {
		t.Fatalf("第一条队列应当被放行: %v", err)
	}
	if err := declareExclusive(t, sess, "lim.q2"); !isKind(err, plugin.KindAccessRefused) {
		t.Fatalf("超过 max-queues 后声明新队列应 403，实际 %v", err)
	}
	// 已存在的队列重声明走等价性检查那条路，不受容量限制影响。
	if err := declareExclusive(t, sess, "lim.q1"); err != nil {
		t.Fatalf("重声明已存在的队列不应受限: %v", err)
	}

	// 拆掉限制后又能建新队列：限制是"当前生效"的，不是一次性开关。
	if _, err := b.DeleteVHostLimit("/", broker.LimitMaxQueues); err != nil {
		t.Fatalf("删除 max-queues 失败: %v", err)
	}
	if err := declareExclusive(t, sess, "lim.q3"); err != nil {
		t.Fatalf("删除限制后应当可以继续建队列: %v", err)
	}
}

// TestVHostConnectionLimitBlocksNewConnection 覆盖 max-connections：
// 第一条连接放行，第二条被拒。
func TestVHostConnectionLimitBlocksNewConnection(t *testing.T) {
	b := newTestBroker(t)

	if err := b.SetVHostLimit("/", broker.LimitMaxConnections, 1); err != nil {
		t.Fatalf("设置 max-connections 失败: %v", err)
	}
	if _, err := openSessionWithPort(t, b, 20001); err != nil {
		t.Fatalf("第一条连接应当被放行: %v", err)
	}
	if _, err := openSessionWithPort(t, b, 20002); !isKind(err, plugin.KindAccessRefused) {
		t.Fatalf("超过 max-connections 后第二条连接应 403，实际 %v", err)
	}
}

// TestVHostLimitValidation 覆盖写路径的校验：非法限制名、非正数值、不存在的 vhost
// 都必须在写下去之前被拒 —— 否则会留下一条永远不生效、也查不出原因的记录。
func TestVHostLimitValidation(t *testing.T) {
	b := newTestBroker(t)

	if err := b.SetVHostLimit("/", "max-foo", 1); err == nil {
		t.Fatal("未知的限制名应当被拒")
	}
	if err := b.SetVHostLimit("/", broker.LimitMaxQueues, 0); err == nil {
		t.Fatal("限制值为 0 应当被拒")
	}
	if err := b.SetVHostLimit("/", broker.LimitMaxQueues, -1); err == nil {
		t.Fatal("限制值为负数应当被拒")
	}
	if err := b.SetVHostLimit("/nope", broker.LimitMaxQueues, 1); err == nil {
		t.Fatal("不存在的 vhost 应当被拒")
	}
	if _, err := b.DeleteVHostLimit("/", "max-foo"); err == nil {
		t.Fatal("删除未知限制名应当被拒")
	}
}

// TestVHostLimitLifecycle 覆盖"设置 → 读回 → 删除"的完整闭环。
func TestVHostLimitLifecycle(t *testing.T) {
	b := newTestBroker(t)

	if err := b.SetVHostLimit("/", broker.LimitMaxQueues, 7); err != nil {
		t.Fatalf("设置 max-queues 失败: %v", err)
	}
	snap, ok := b.VHostLimit("/", broker.LimitMaxQueues)
	if !ok || snap.Value != 7 {
		t.Fatalf("读回的 max-queues 应为 7，实际 ok=%t %+v", ok, snap)
	}
	if got := len(b.VHostLimitsOf("/")); got != 1 {
		t.Fatalf("该 vhost 应有 1 条限制，实际 %d", got)
	}
	if got := len(b.VHostLimits()); got != 1 {
		t.Fatalf("全局应有 1 条限制，实际 %d", got)
	}

	hit, err := b.DeleteVHostLimit("/", broker.LimitMaxQueues)
	if err != nil || !hit {
		t.Fatalf("删除 max-queues 应命中，实际 hit=%t err=%v", hit, err)
	}
	if _, ok := b.VHostLimit("/", broker.LimitMaxQueues); ok {
		t.Fatal("删除后不应再读到该限制")
	}
	// 再删一次：返回"未命中"，而不是报错（对齐 RabbitMQ 的幂等删除）。
	hit, err = b.DeleteVHostLimit("/", broker.LimitMaxQueues)
	if err != nil || hit {
		t.Fatalf("重复删除应幂等返回未命中，实际 hit=%t err=%v", hit, err)
	}
}

// TestDeleteVHostRemovesLimits 覆盖级联：删 vhost 时它的限制不能留在元数据里变悬空记录。
func TestDeleteVHostRemovesLimits(t *testing.T) {
	b := newTestBroker(t)

	if err := b.CreateVHost("limvh"); err != nil {
		t.Fatalf("新建 vhost 失败: %v", err)
	}
	if err := b.SetVHostLimit("limvh", broker.LimitMaxQueues, 3); err != nil {
		t.Fatalf("设置 max-queues 失败: %v", err)
	}
	hit, err := b.DeleteVHost("limvh")
	if err != nil || !hit {
		t.Fatalf("删除 vhost 应命中，实际 hit=%t err=%v", hit, err)
	}
	if _, ok := b.VHostLimit("limvh", broker.LimitMaxQueues); ok {
		t.Fatal("vhost 删除后它的限制应被一并清除")
	}
}
