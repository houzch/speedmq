package broker_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M8-6（策略 / policy）的**运行期语义**：
// 按名称匹配把一批配置施加到队列与交换机上，并且对**已存在**的对象立即生效 ——
// "改完策略要等队列重建才生效"等于没实现（RabbitMQ 是即时生效的）。

// setPolicy 是设置策略的测试helper：任何一次失败都直接终止用例。
func setPolicy(t *testing.T, b *broker.Broker, name, pattern, applyTo string, def map[string]any, priority int) {
	t.Helper()
	if err := b.SetPolicy(broker.PolicySnapshot{
		VHost: "/", Name: name, Pattern: pattern,
		ApplyTo: applyTo, Definition: def, Priority: priority,
	}); err != nil {
		t.Fatalf("设置策略 %s 失败: %v", name, err)
	}
}

// policyOfQueue 返回队列命中的策略名与展示用的策略定义。
func policyOfQueue(t *testing.T, b *broker.Broker, name string) (string, map[string]any) {
	t.Helper()
	snap, ok := b.QueueSnapshot("/", name)
	if !ok {
		t.Fatalf("队列 %s 的快照不存在", name)
	}
	return snap.Policy, snap.EffectivePolicyDefinition
}

// TestPolicyAppliesToExistingQueue 覆盖"策略对已存在的队列立即生效"，
// 以及删除策略后行为回到原状。
func TestPolicyAppliesToExistingQueue(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	// 先建一个没有任何限制的队列，并塞满 3 条。
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pol.q", Durable: true})
	for i := 0; i < 3; i++ {
		if _, err := sess.Publish(&plugin.Message{Body: []byte{byte('0' + i)}}, "", q.Name, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}
	requireCount(t, sess, q.Name, nil, 3)

	// 上策略：长度限制 1（默认 drop-head）。**不重建队列**。
	setPolicy(t, b, "plen", `^pol\.`, broker.ApplyToQueues, map[string]any{"max-length": 1}, 0)

	// 立即生效：再发一条就会挤出队首，队列始终只有 1 条。
	if _, err := sess.Publish(&plugin.Message{Body: []byte("new")}, "", q.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	requireCount(t, sess, q.Name, nil, 1)

	// 管理面必须能回答"这条限制是哪条策略给的"。
	name, def := policyOfQueue(t, b, "pol.q")
	if name != "plen" {
		t.Fatalf("队列应显示命中策略 plen，实际 %q", name)
	}
	if got := fmt.Sprint(def["max-length"]); got != "1" {
		t.Fatalf("effective_policy_definition 应含 max-length=1，实际 %v", def)
	}

	// 删掉策略：限制必须**立刻**解除（回到无策略状态）。
	ok, err := b.DeletePolicy("/", "plen")
	if err != nil || !ok {
		t.Fatalf("删除策略失败: ok=%v err=%v", ok, err)
	}
	if name, _ := policyOfQueue(t, b, "pol.q"); name != "" {
		t.Fatalf("策略删除后队列不应再显示策略名，实际 %q", name)
	}
	for i := 0; i < 2; i++ {
		if _, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "", q.Name, false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}
	requireCount(t, sess, q.Name, nil, 3)
}

// TestPolicyMessageTTL 覆盖 TTL 的作用时机：TTL 在**发布时**按当时的生效参数算，
// 因此策略变更只影响之后发布的消息（与 RabbitMQ 一致）。
//
// 两个队列分别验证两半：
//   - A：策略设置**之后**发布 → 按策略 TTL 过期；
//   - B：策略设置**之前**发布 → 不受影响（TTL 不回溯），消息仍在。
//
// 之所以要分两个队列：TTL 只在**队首**判定（对齐 RabbitMQ 的队首过期语义），
// 同一条队列里"无 TTL 的头消息"会挡住后面消息的过期，混在一起测就分不清是哪一半。
func TestPolicyMessageTTL(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	qA := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pol.ttl.a", Durable: true})
	qB := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pol.ttl.b", Durable: true})
	// B 的消息在策略之前发布：不带过期时间
	if _, err := sess.Publish(&plugin.Message{Body: []byte("before")}, "", qB.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	setPolicy(t, b, "pttl", `^pol\.ttl`, broker.ApplyToQueues, map[string]any{"message-ttl": 50}, 0)

	// A 的消息在策略之后发布：应按 50ms 过期，队列随之清空
	if _, err := sess.Publish(&plugin.Message{Body: []byte("after")}, "", qA.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	waitFor(t, 3*time.Second, "策略生效后发布的消息过期", func() bool {
		n, ok := queueCount(t, sess, qA.Name, nil)
		return ok && n == 0
	})

	// B 的消息不受影响：TTL 不回溯（RabbitMQ 同样如此）
	requireCount(t, sess, qB.Name, nil, 1)
}

// TestPolicyPriorityAndPattern 覆盖匹配规则：priority 高者生效、pattern 不命中则不生效、
// apply-to 决定对象类型。
func TestPolicyPriorityAndPattern(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pp.q", Durable: true})

	setPolicy(t, b, "plow", `^pp\.`, broker.ApplyToQueues, map[string]any{"max-length": 5}, 0)
	if name, _ := policyOfQueue(t, b, "pp.q"); name != "plow" {
		t.Fatalf("应命中 plow，实际 %q", name)
	}

	// 更高优先级的策略赢
	setPolicy(t, b, "phigh", `^pp\.`, broker.ApplyToQueues, map[string]any{"max-length": 1}, 10)
	if name, _ := policyOfQueue(t, b, "pp.q"); name != "phigh" {
		t.Fatalf("优先级高的应赢，实际 %q", name)
	}

	// 把高优先级那条的 pattern 改成不命中 → 回落到 plow
	setPolicy(t, b, "phigh", `^no-match-here$`, broker.ApplyToQueues, map[string]any{"max-length": 1}, 10)
	if name, _ := policyOfQueue(t, b, "pp.q"); name != "plow" {
		t.Fatalf("pattern 不命中时应回落到 plow，实际 %q", name)
	}

	// apply-to 决定对象类型：只作用于交换机的策略不该影响队列
	setPolicy(t, b, "pex", `^pp\.`, broker.ApplyToExchanges, map[string]any{"alternate-exchange": "pp.alt"}, 99)
	if name, _ := policyOfQueue(t, b, "pp.q"); name != "plow" {
		t.Fatalf("apply-to=exchanges 的策略不该作用于队列，实际命中 %q", name)
	}
}

// TestPolicyApplyToQueueKind 覆盖 classic_queues / quorum_queues 的区分：
// 只作用于仲裁队列的策略不得落到经典队列上（反之亦然）。
func TestPolicyApplyToQueueKind(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pk.q", Durable: true})

	setPolicy(t, b, "pquorum", `^pk\.`, broker.ApplyToQuorumQueues, map[string]any{"max-length": 1}, 0)
	if name, _ := policyOfQueue(t, b, "pk.q"); name != "" {
		t.Fatalf("经典队列不该命中 quorum_queues 策略，实际 %q", name)
	}

	setPolicy(t, b, "pclassic", `^pk\.`, broker.ApplyToClassicQueues, map[string]any{"max-length": 2}, 0)
	if name, _ := policyOfQueue(t, b, "pk.q"); name != "pclassic" {
		t.Fatalf("经典队列应命中 classic_queues 策略，实际 %q", name)
	}
}

// TestPolicyQueueArgumentWins 覆盖优先级：队列显式声明的参数**覆盖**策略。
func TestPolicyQueueArgumentWins(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	// 队列自己声明 TTL=10 秒；策略给 1ms。若策略赢了，消息会立刻消失。
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{
		Name: "pw.q", Durable: true,
		Arguments: map[string]any{"x-message-ttl": int32(10000)},
	})
	setPolicy(t, b, "pw", `^pw\.q$`, broker.ApplyToQueues, map[string]any{"message-ttl": 1}, 0)

	if _, err := sess.Publish(&plugin.Message{Body: []byte("keep")}, "", q.Name, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // 远大于策略里的 1ms
	requireCount(t, sess, q.Name, nil, 1)
	// 策略命中信息仍然要展示（它确实生效了，只是被队列自己的声明覆盖）
	if name, _ := policyOfQueue(t, b, "pw.q"); name != "pw" {
		t.Fatalf("队列应仍显示命中策略 pw，实际 %q", name)
	}
}

// TestPolicyAlternateExchange 覆盖交换机侧的策略：alternate-exchange。
func TestPolicyAlternateExchange(t *testing.T) {
	b := newTestBroker(t)
	sess, _ := testSessionOf(t, b, "guest", "guest")

	if err := sess.DeclareExchange(plugin.ExchangeDeclare{Name: "pae.main", Type: plugin.ExchangeDirect}); err != nil {
		t.Fatalf("声明主交换机失败: %v", err)
	}
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{Name: "pae.alt", Type: plugin.ExchangeFanout}); err != nil {
		t.Fatalf("声明备用交换机失败: %v", err)
	}
	q := mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pae.q", Durable: true})
	if err := sess.BindQueue(q.Name, "pae.alt", "", nil); err != nil {
		t.Fatalf("绑定失败: %v", err)
	}

	// 还没上策略：主交换机没有绑定 → 未路由
	res, err := sess.Publish(&plugin.Message{Body: []byte("m1")}, "pae.main", "k", false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if res.Routed {
		t.Fatal("主交换机没有绑定时不应路由成功")
	}

	// 上策略：主交换机未路由时改投备用交换机
	setPolicy(t, b, "pae", `^pae\.main$`, broker.ApplyToExchanges,
		map[string]any{"alternate-exchange": "pae.alt"}, 0)

	res, err = sess.Publish(&plugin.Message{Body: []byte("m2")}, "pae.main", "k", false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatal("上了 alternate-exchange 之后应路由成功")
	}
	requireCount(t, sess, q.Name, nil, 1)

	// 交换机快照要能看出它是被哪条策略管的
	ex, ok := b.ExchangeSnapshot("/", "pae.main")
	if !ok {
		t.Fatal("交换机快照缺失")
	}
	if ex.Policy != "pae" || fmt.Sprint(ex.EffectivePolicyDefinition["alternate-exchange"]) != "pae.alt" {
		t.Fatalf("交换机应显示策略 pae 与 alternate-exchange，实际 %q %v", ex.Policy, ex.EffectivePolicyDefinition)
	}

	// 删掉策略 → 回到未路由
	if ok, err := b.DeletePolicy("/", "pae"); err != nil || !ok {
		t.Fatalf("删除策略失败: ok=%v err=%v", ok, err)
	}
	res, err = sess.Publish(&plugin.Message{Body: []byte("m3")}, "pae.main", "k", false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if res.Routed {
		t.Fatal("策略删除后不应再走备用交换机")
	}
}

// TestPolicyReplicatesAcrossCluster 覆盖策略的集群复制：策略是集群级元数据，
// 在任意节点上创建后，全体节点都要看到并**作用到各自的队列**上。
func TestPolicyReplicatesAcrossCluster(t *testing.T) {
	nodes := startTestCluster(t, 3)
	leader := waitClusterLeader(t, nodes)

	var follower, other *clusterNode
	for _, node := range nodes {
		if node.id == leader.id {
			continue
		}
		if follower == nil {
			follower = node
		} else {
			other = node
		}
	}

	// 在另一个节点上先建一条 durable 队列（策略是事后施加的，必须作用到它）
	sess, err := testSessionOf(t, other.b, "guest", "guest")
	if err != nil {
		t.Fatalf("在节点 %s 上打开会话失败: %v", other.id, err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "pol.cluster.q", Durable: true})

	// 在 follower 上创建策略（写请求会被转发给 leader 提交）
	setPolicy(t, follower.b, "pcluster", `^pol\.cluster\.`, broker.ApplyToQueues,
		map[string]any{"max-length": 1}, 0)

	waitFor(t, 5*time.Second, "策略复制到全部节点", func() bool {
		for _, node := range nodes {
			if _, ok := node.b.Policy("/", "pcluster"); !ok {
				return false
			}
		}
		return true
	})

	// 关键：**别的节点**上那条已存在的队列也要立即受限（不是只有创建策略的节点生效）
	waitFor(t, 5*time.Second, "策略在另一个节点上生效", func() bool {
		for i := 0; i < 2; i++ {
			if _, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "", "pol.cluster.q", false); err != nil {
				return false
			}
		}
		n, ok := queueCount(t, sess, "pol.cluster.q", nil)
		return ok && n == 1
	})

	// 删除策略同样要复制并解除限制
	if ok, err := follower.b.DeletePolicy("/", "pcluster"); err != nil || !ok {
		t.Fatalf("删除策略失败: ok=%v err=%v", ok, err)
	}
	waitFor(t, 5*time.Second, "策略删除复制到全部节点", func() bool {
		for _, node := range nodes {
			if _, ok := node.b.Policy("/", "pcluster"); ok {
				return false
			}
		}
		return true
	})
}

// TestPolicyValidation 覆盖写路径校验：不支持的键与取值必须在**创建时**被拒，
// 而不是存下来当摆设。
func TestPolicyValidation(t *testing.T) {
	b := newTestBroker(t)

	cases := []struct {
		desc   string
		policy broker.PolicySnapshot
	}{
		{"空 pattern", policySnap("v", "", broker.ApplyToQueues, map[string]any{"max-length": 1})},
		{"非法正则", policySnap("v", "([", broker.ApplyToQueues, map[string]any{"max-length": 1})},
		{"apply-to 为空", policySnap("v", "^a", "", map[string]any{"max-length": 1})},
		{"apply-to 非法", policySnap("v", "^a", "classic", map[string]any{"max-length": 1})},
		{"未知键", policySnap("v", "^a", broker.ApplyToQueues, map[string]any{"max-lenght": 1})},
		{"队列不支持 max-priority", policySnap("v", "^a", broker.ApplyToQueues, map[string]any{"max-priority": 5})},
		{"队列不支持 queue-type", policySnap("v", "^a", broker.ApplyToQueues, map[string]any{"queue-type": "quorum"})},
		{"交换机不支持 max-length", policySnap("v", "^a", broker.ApplyToExchanges, map[string]any{"max-length": 1})},
		{"队列不支持 alternate-exchange", policySnap("v", "^a", broker.ApplyToQueues, map[string]any{"alternate-exchange": "x"})},
		{"max-length 取值非法", policySnap("v", "^a", broker.ApplyToQueues, map[string]any{"max-length": -1})},
		{"overflow 取值非法", policySnap("v", "^a", broker.ApplyToQueues, map[string]any{"overflow": "whatever"})},
		{"priority 为负", broker.PolicySnapshot{
			VHost: "/", Name: "v", Pattern: "^a", ApplyTo: broker.ApplyToQueues, Priority: -1,
		}},
	}
	for _, c := range cases {
		c.policy.VHost = "/"
		if err := b.SetPolicy(c.policy); err == nil {
			t.Fatalf("%s：应当被拒绝", c.desc)
		}
	}

	// 合法的策略必须能建上（否则上面的用例可能只是"什么都拒绝"）
	setPolicy(t, b, "ok", `^a`, broker.ApplyToQueues, map[string]any{
		"max-length": 1, "message-ttl": 100, "overflow": "reject-publish",
		"dead-letter-exchange": "x", "dead-letter-routing-key": "k",
	}, 3)
	if pol, ok := b.Policy("/", "ok"); !ok || pol.Priority != 3 {
		t.Fatalf("合法策略应被写入，实际 ok=%v %+v", ok, pol)
	}
}

func policySnap(name, pattern, applyTo string, def map[string]any) broker.PolicySnapshot {
	return broker.PolicySnapshot{
		VHost: "/", Name: name, Pattern: pattern, ApplyTo: applyTo, Definition: def,
	}
}
