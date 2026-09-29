package broker_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件在真实 TCP 三节点集群上验证仲裁队列（Quorum Queue）：
//   - 声明即建立 Raft 组，消息复制到多数派后才确认；
//   - 任意节点都能发布/消费（服务节点是组的 leader，非 leader 节点经转发层代理）；
//   - **leader 宕机后已确认的消息不丢**（新 leader 从日志里拿到全部消息）；
//   - leader 变更后未确认的消息被重新投递（at-least-once）；
//   - 清空与删除跨节点生效。
//
// 这些性质正是仲裁队列存在的理由，因此必须用真实进程/端口验证，而不是只看单测。

// declareQuorumOn 在指定节点上声明一条仲裁队列。
func declareQuorumOn(t *testing.T, node *clusterNode, name string) {
	t.Helper()
	sess := session(t, node)
	defer sess.Close()
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{
		Name:    name,
		Durable: true,
		Arguments: map[string]any{
			"x-queue-type": "quorum",
		},
	}); err != nil {
		t.Fatalf("声明仲裁队列失败: %v", err)
	}
}

// waitQuorumLeader 等到该仲裁队列的 Raft 组选出 leader，返回 leader 所在节点。
//
// 队列快照里的 Owner 就是"当前服务节点"，也就是组 leader —— 管理面与测试用的是同一个口径。
func waitQuorumLeader(t *testing.T, nodes []*clusterNode, queue string) *clusterNode {
	t.Helper()
	var leader *clusterNode
	waitFor(t, 15*time.Second, "仲裁队列选出 leader", func() bool {
		for _, n := range nodes {
			if n.b.ClusterStatus().Role == "shutdown" {
				continue
			}
			s, ok := n.b.QueueSnapshot("/", queue)
			if !ok || s.Owner == "" {
				continue
			}
			for _, cand := range nodes {
				if cand.id == s.Owner {
					leader = cand
					return true
				}
			}
		}
		return false
	})
	return leader
}

// publishConfirmed 发布并等待持久化完成。
//
// 允许 res.Durable 为 nil：那是"发布被转发到服务节点、由它先等完复制再应答"的路径，
// 确认语义同样成立（见 forwardPublish）。服务在本地的路径由 publishOnLeaderConfirmed 单独验证。
func publishConfirmed(t *testing.T, sess plugin.Session, queue, body string) {
	t.Helper()
	res, err := sess.Publish(&plugin.Message{Body: []byte(body)}, "", queue, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatalf("消息未路由到队列 %s", queue)
	}
	if res.Durable != nil {
		if err := res.Durable(); err != nil {
			t.Fatalf("等待持久化失败: %v", err)
		}
	}
}

// publishOnLeaderConfirmed 在服务节点上发布，并要求返回"等复制到多数派"的凭据。
func publishOnLeaderConfirmed(t *testing.T, sess plugin.Session, queue, body string) {
	t.Helper()
	res, err := sess.Publish(&plugin.Message{Body: []byte(body)}, "", queue, false)
	if err != nil {
		t.Fatalf("发布失败: %v", err)
	}
	if !res.Routed {
		t.Fatalf("消息未路由到队列 %s", queue)
	}
	if res.Durable == nil {
		t.Fatalf("在服务节点上发布仲裁队列必须返回「复制到多数派」的等待凭据")
	}
	if err := res.Durable(); err != nil {
		t.Fatalf("等待复制到多数派失败: %v", err)
	}
}

// TestQuorumQueueReplicatesAcrossNodes 覆盖基本闭环：任意节点发布、任意节点消费。
func TestQuorumQueueReplicatesAcrossNodes(t *testing.T) {
	const queue = "m6c.quorum.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumOn(t, nodes[0], queue)

	// 元数据复制：三个节点都应当看到这条队列，且类型是 quorum。
	waitFor(t, 10*time.Second, "仲裁队列复制到全部节点", func() bool {
		for _, n := range nodes {
			s, ok := n.b.QueueSnapshot("/", queue)
			if !ok || s.QueueType != "quorum" {
				return false
			}
		}
		return true
	})
	leader := waitQuorumLeader(t, nodes, queue)
	// 等所有节点都从心跳里学到 leader：否则客户端连到"还没听说 leader"的节点会短暂报错。
	waitFor(t, 15*time.Second, "所有节点都记录到组 leader", func() bool {
		for _, n := range nodes {
			if s, ok := n.b.QueueSnapshot("/", queue); !ok || s.Owner != leader.id {
				return false
			}
		}
		return true
	})

	// 在服务节点（组 leader）上发布：这条路径由 Raft 直接受理，confirm 必须等复制到多数派。
	pubSess := session(t, leader)
	defer pubSess.Close()
	for i := 0; i < 3; i++ {
		publishOnLeaderConfirmed(t, pubSess, queue, fmt.Sprintf("q-%d", i))
	}

	// 在另一个节点消费（经转发层把消费者注册到服务节点）。
	consNode := nodes[0]
	if consNode == leader {
		consNode = nodes[1]
	}
	consSess := session(t, consNode)
	defer consSess.Close()
	sub, collector := newCollector(queue, 5, false)
	if _, err := consSess.Consume(sub); err != nil {
		t.Fatalf("在上消费仲裁队列失败: %v", err)
	}
	for i := 0; i < 3; i++ {
		d := collector.next(t, fmt.Sprintf("第 %d 条投递", i))
		if got := string(d.Message.Body); got != fmt.Sprintf("q-%d", i) {
			t.Fatalf("第 %d 条内容错误: %q", i, got)
		}
		d.Settle(plugin.SettleAck)
	}
	waitFor(t, 10*time.Second, "确认后队列清空", func() bool {
		s, ok := nodes[0].b.QueueSnapshot("/", queue)
		return ok && s.Ready == 0 && s.Unacked == 0
	})
}

// TestQuorumQueueSurvivesLeaderLoss 是仲裁队列的核心保证：
// 已确认的消息在 leader 宕机后必须仍然存在（副本里有日志）。
func TestQuorumQueueSurvivesLeaderLoss(t *testing.T) {
	const queue = "m6c.quorum.ha.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumOn(t, nodes[0], queue)
	leader := waitQuorumLeader(t, nodes, queue)

	pubSess := session(t, nodes[1])
	// 从非 leader 节点发布：经转发层交给 leader，再由它的 Raft 组复制到多数派。
	for i := 0; i < 3; i++ {
		publishConfirmed(t, pubSess, queue, fmt.Sprintf("ha-%d", i))
	}
	pubSess.Close()

	// 干掉组 leader：剩下 2/3 仍是多数派，服务必须继续且消息不能丢。
	leader.b.Close()
	waitFor(t, 20*time.Second, "剩余节点选出新的组 leader", func() bool {
		for _, n := range nodes {
			if n == leader {
				continue
			}
			if n.b.ClusterStatus().Role == "shutdown" {
				continue
			}
			s, ok := n.b.QueueSnapshot("/", queue)
			if ok && s.Owner != "" && s.Owner != leader.id {
				return true
			}
		}
		return false
	})

	// 在幸存节点上消费：3 条已确认的消息必须都在。
	var survivor *clusterNode
	for _, n := range nodes {
		if n != leader {
			survivor = n
			break
		}
	}
	consSess := session(t, survivor)
	defer consSess.Close()
	sub, collector := newCollector(queue, 10, false)
	if _, err := consSess.Consume(sub); err != nil {
		t.Fatalf("在幸存节点上消费失败: %v", err)
	}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		d := collector.next(t, fmt.Sprintf("leader 宕机后的第 %d 条投递", i))
		seen[string(d.Message.Body)] = true
		d.Settle(plugin.SettleAck)
	}
	for i := 0; i < 3; i++ {
		if !seen[fmt.Sprintf("ha-%d", i)] {
			t.Fatalf("leader 宕机后丢失了已确认的消息 ha-%d（收到 %v）", i, seen)
		}
	}
}

// TestQuorumQueueRedeliversUnackedAfterLeaderChange 覆盖 at-least-once：
// 已投递但未确认的消息在 leader 变更后必须被重新投递（而不是随着旧 leader 一起消失）。
func TestQuorumQueueRedeliversUnackedAfterLeaderChange(t *testing.T) {
	const queue = "m6c.quorum.redeliver.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumOn(t, nodes[0], queue)
	leader := waitQuorumLeader(t, nodes, queue)

	var other *clusterNode
	for _, n := range nodes {
		if n != leader {
			other = n
			break
		}
	}
	pubSess := session(t, other)
	publishConfirmed(t, pubSess, queue, "once")
	pubSess.Close()

	// 在非 leader 节点上拉取但不确认，然后干掉 leader。
	consSess := session(t, other)
	d, ok, err := consSess.Get(queue, false)
	if err != nil || !ok {
		t.Fatalf("跨节点拉取失败: ok=%v err=%v", ok, err)
	}
	if string(d.Message.Body) != "once" {
		t.Fatalf("拉取内容错误: %q", d.Message.Body)
	}
	consSess.Close() // 客户端断开 → 未确认消息回到日志语义的"未投递"
	leader.b.Close()

	// 等新 leader 选出后重新拉取：同一条消息必须回来且 redelivered=true。
	waitFor(t, 20*time.Second, "剩余节点选出新的组 leader", func() bool {
		s, ok := other.b.QueueSnapshot("/", queue)
		return ok && s.Owner != "" && s.Owner != leader.id
	})
	consSess2 := session(t, other)
	defer consSess2.Close()
	waitFor(t, 20*time.Second, "未确认的消息被重新投递", func() bool {
		d, ok, err := consSess2.Get(queue, true)
		return err == nil && ok && string(d.Message.Body) == "once"
	})
}

// TestQuorumQueueMinorityCannotPublish：失去多数派时仲裁队列不可写。
//
// 期望"失败"而不是"成功"：仲裁队列的承诺是"确认即已在多数派落盘"，
// 少数派下无法兑现这个承诺，就必须让客户端看到失败（而不是静默接受）。
func TestQuorumQueueMinorityCannotPublish(t *testing.T) {
	const queue = "m6c.quorum.minority.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumOn(t, nodes[0], queue)
	waitQuorumLeader(t, nodes, queue)

	survivor := nodes[0]
	for _, n := range nodes {
		if n != survivor {
			n.b.Close()
		}
	}
	// 少数派：要么被 pause_minority 拒绝，要么 Raft 组选不出 leader —— 两条路都是"不可写"。
	sess := session(t, survivor)
	defer sess.Close()
	waitFor(t, 20*time.Second, "少数派下的发布被拒绝", func() bool {
		_, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "", queue, false)
		return err != nil
	})
}

// TestQuorumQueuePurgeAndDelete 覆盖清空与删除（都经日志/元数据，跨节点生效）。
func TestQuorumQueuePurgeAndDelete(t *testing.T) {
	const queue = "m6c.quorum.purge.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumOn(t, nodes[0], queue)
	waitQuorumLeader(t, nodes, queue)

	pubSess := session(t, nodes[1])
	defer pubSess.Close()
	for i := 0; i < 3; i++ {
		publishConfirmed(t, pubSess, queue, fmt.Sprintf("p-%d", i))
	}
	waitFor(t, 10*time.Second, "消息进入仲裁队列", func() bool {
		s, ok := nodes[0].b.QueueSnapshot("/", queue)
		return ok && s.Ready == 3
	})

	consSess := session(t, nodes[2])
	defer consSess.Close()
	purged, err := consSess.PurgeQueue(queue)
	if err != nil || purged != 3 {
		t.Fatalf("跨节点清空仲裁队列失败: purged=%d err=%v", purged, err)
	}
	waitFor(t, 10*time.Second, "仲裁队列被清空", func() bool {
		s, ok := nodes[0].b.QueueSnapshot("/", queue)
		return ok && s.Ready == 0
	})

	if _, err := consSess.DeleteQueue(queue, false, false); err != nil {
		t.Fatalf("删除仲裁队列失败: %v", err)
	}
	waitFor(t, 15*time.Second, "全部节点上的仲裁队列被删除", func() bool {
		for _, n := range nodes {
			if _, ok := n.b.QueueSnapshot("/", queue); ok {
				return false
			}
		}
		return true
	})
}
