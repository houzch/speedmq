package broker_test

import (
	"net"
	"testing"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖特性开关与弃用特性。
//
// 核心要求是"开关真的有效"：关掉之后对应操作必须被拒、能力声明必须同步收敛，
// 重新打开后一切恢复 —— 一个只会改页面显示的开关等于没有。

// newTestCore 打开一条连接级操作面（无需认证），用于读取 ServerProperties。
func newTestCore(t *testing.T, b *broker.Broker) plugin.Core {
	t.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	return b.NewSession(remote, local)
}

// TestFeatureFlagsDefaultEnabled 覆盖注册表本身：每个开关都要有状态、说明与提供方。
func TestFeatureFlagsDefaultEnabled(t *testing.T) {
	b := newTestBroker(t)
	flags := b.FeatureFlags()
	if len(flags) == 0 {
		t.Fatal("特性开关注册表不应为空")
	}
	for _, f := range flags {
		if f.State != "enabled" {
			t.Fatalf("开关 %s 默认应为 enabled，实际 %s", f.Name, f.State)
		}
		if f.Desc == "" || f.ProvidedBy == "" {
			t.Fatalf("开关 %s 缺少说明或提供方", f.Name)
		}
	}
	if err := b.SetFeatureFlag("no_such_flag", true); err == nil {
		t.Fatal("未知开关应当被拒")
	}
}

// TestQuorumQueueFeatureFlagGate 覆盖 quorum_queue 开关：
// 关闭后新声明仲裁队列被拒，已存在的队列不受影响，重新打开后恢复。
func TestQuorumQueueFeatureFlagGate(t *testing.T) {
	const flag = "quorum_queue"
	b := newTestBroker(t)
	sess := newTestSession(t, b)

	quorum := func(name string) error {
		_, err := sess.DeclareQueue(plugin.QueueDeclare{
			Name: name, Durable: true,
			Arguments: map[string]any{"x-queue-type": "quorum"},
		})
		return err
	}
	if err := quorum("ff.quorum.ok"); err != nil {
		t.Fatalf("开关默认开启时应允许声明仲裁队列: %v", err)
	}

	if err := b.SetFeatureFlag(flag, false); err != nil {
		t.Fatalf("关闭 %s 失败: %v", flag, err)
	}
	if snap, _ := b.FeatureFlag(flag); snap.State != "disabled" {
		t.Fatalf("关闭后状态应为 disabled，实际 %s", snap.State)
	}
	if err := quorum("ff.quorum.denied"); !isKind(err, plugin.KindPreconditionFailed) {
		t.Fatalf("开关关闭时声明仲裁队列应 406，实际 %v", err)
	}
	// 已存在的队列重声明走等价性校验，不该被开关拦住。
	if err := quorum("ff.quorum.ok"); err != nil {
		t.Fatalf("开关关闭不该影响已存在的仲裁队列: %v", err)
	}

	if err := b.SetFeatureFlag(flag, true); err != nil {
		t.Fatalf("重新打开 %s 失败: %v", flag, err)
	}
	if err := quorum("ff.quorum.again"); err != nil {
		t.Fatalf("重新打开后应恢复声明能力: %v", err)
	}
}

// TestExchangeBindingsFeatureFlagGate 覆盖 exchange_exchange_bindings 开关：
// 关闭后 exchange.bind 被拒（NOT_IMPLEMENTED），且 capabilities 声明同步收敛为 false。
func TestExchangeBindingsFeatureFlagGate(t *testing.T) {
	const flag = "exchange_exchange_bindings"
	b := newTestBroker(t)
	sess := newTestSession(t, b)

	for _, name := range []string{"ff.src", "ff.dst"} {
		if err := sess.DeclareExchange(plugin.ExchangeDeclare{
			Name: name, Type: plugin.ExchangeFanout, Durable: true,
		}); err != nil {
			t.Fatalf("声明交换机 %s 失败: %v", name, err)
		}
	}
	if err := sess.BindExchange("ff.dst", "ff.src", "", nil); err != nil {
		t.Fatalf("开关默认开启时应允许交换机间绑定: %v", err)
	}
	if err := sess.UnbindExchange("ff.dst", "ff.src", "", nil); err != nil {
		t.Fatalf("解绑失败: %v", err)
	}

	if err := b.SetFeatureFlag(flag, false); err != nil {
		t.Fatalf("关闭 %s 失败: %v", flag, err)
	}
	if err := sess.BindExchange("ff.dst", "ff.src", "", nil); !isKind(err, plugin.KindNotImplemented) {
		t.Fatalf("开关关闭时交换机间绑定应 NOT_IMPLEMENTED，实际 %v", err)
	}

	// 能力声明必须与判定点一致：否则客户端看到 true 会继续发 exchange.bind。
	core := newTestCore(t, b)
	capabilities, _ := core.ServerProperties()["capabilities"].(map[string]any)
	if capabilities["exchange_exchange_bindings"] != false {
		t.Fatalf("开关关闭时 capabilities.exchange_exchange_bindings 应为 false，实际 %v",
			capabilities["exchange_exchange_bindings"])
	}

	if err := b.SetFeatureFlag(flag, true); err != nil {
		t.Fatalf("重新打开 %s 失败: %v", flag, err)
	}
	if err := sess.BindExchange("ff.dst", "ff.src", "", nil); err != nil {
		t.Fatalf("重新打开后应恢复绑定能力: %v", err)
	}
	core = newTestCore(t, b)
	capabilities, _ = core.ServerProperties()["capabilities"].(map[string]any)
	if capabilities["exchange_exchange_bindings"] != true {
		t.Fatalf("开关打开时 capabilities.exchange_exchange_bindings 应为 true，实际 %v",
			capabilities["exchange_exchange_bindings"])
	}
}

// TestDeprecatedFeatureRegistry 覆盖弃用特性清单：它如实反映本实现的最终态度，
// 每条都要有状态与弃用阶段。
func TestDeprecatedFeatureRegistry(t *testing.T) {
	b := newTestBroker(t)
	items := b.DeprecatedFeatures()
	if len(items) == 0 {
		t.Fatal("弃用特性清单不应为空")
	}
	states := map[string]string{}
	for _, item := range items {
		if item.State != "denied" && item.State != "permitted" {
			t.Fatalf("弃用特性 %s 的状态非法: %q", item.Name, item.State)
		}
		if item.DeprecationPhase == "" || item.Desc == "" {
			t.Fatalf("弃用特性 %s 缺少阶段或说明", item.Name)
		}
		states[item.Name] = item.State
	}
	// 本实现确实拒绝瞬时非独占队列与 global QoS（见 newQueueIn / channel_methods.go），
	// 清单必须如实写成 denied，否则页面会与行为相反。
	for _, name := range []string{"transient_nonexcl_queues", "global_qos"} {
		if states[name] != "denied" {
			t.Fatalf("弃用特性 %s 的状态应为 denied，实际 %q", name, states[name])
		}
	}
}
