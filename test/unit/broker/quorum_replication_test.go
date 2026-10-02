package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件在真实 TCP 三节点集群上验证 M8-15 的仲裁队列扩副本（grow）与再平衡（rebalance）：
//   - 声明时的 x-quorum-initial-group-size 决定初始副本集（投票成员数）；
//   - grow 只把目标节点从 learner 提升为 voter，副本集落进元数据、重启后仍是 N 副本；
//   - 不可缩容、不可超过集群规模、非仲裁队列明确报错、单机模式报 NOT_IMPLEMENTED；
//   - 扩到 3 副本后杀掉一台机器，队列仍然可写可读（这正是"副本数变 3"的意义）。

// declareQuorumSized 在指定节点上声明一条指定初始副本数的仲裁队列。
func declareQuorumSized(t *testing.T, node *clusterNode, name string, size int) {
	t.Helper()
	sess := session(t, node)
	defer sess.Close()
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{
		Name:    name,
		Durable: true,
		Arguments: map[string]any{
			"x-queue-type":                "quorum",
			"x-quorum-initial-group-size": size,
		},
	}); err != nil {
		t.Fatalf("声明 %d 副本的仲裁队列失败: %v", size, err)
	}
}

// quorumInfoOn 读取某节点上该队列的副本集视图。
func quorumInfoOn(t *testing.T, node *clusterNode, queue string) broker.QuorumQueueInfo {
	t.Helper()
	info, ok := node.b.QuorumQueueInfo("/", queue)
	if !ok {
		t.Fatalf("节点 %s 上没有队列 %s 的副本集视图", node.id, queue)
	}
	return info
}

// waitQuorumVoters 等全部节点看到同一份投票成员集合。
func waitQuorumVoters(t *testing.T, nodes []*clusterNode, queue string, want []string) {
	t.Helper()
	waitFor(t, 30*time.Second, fmt.Sprintf("全部节点看到 %s 的投票成员为 %v", queue, want), func() bool {
		for _, n := range nodes {
			info, ok := n.b.QuorumQueueInfo("/", queue)
			if !ok || !equalStrings(info.Voters, want) {
				return false
			}
		}
		return true
	})
}

// TestQuorumGrowIncreasesFaultTolerance 覆盖 grow 的完整语义闭环。
func TestQuorumGrowIncreasesFaultTolerance(t *testing.T) {
	const queue = "m8.quorum.grow.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)

	// 1 副本：只有 n1 是投票成员，另外两个是 learner（照常复制，只是不投票）。
	declareQuorumSized(t, nodes[0], queue, 1)
	waitQuorumVoters(t, nodes, queue, []string{"n1"})
	for _, n := range nodes {
		info := quorumInfoOn(t, n, queue)
		if !equalStrings(info.Replicas, []string{"n1"}) {
			t.Fatalf("节点 %s 记录的副本集应为 [n1]，实际 %v", n.id, info.Replicas)
		}
		if len(info.Learners) != 2 {
			t.Fatalf("节点 %s 应看到 2 个 learner，实际 %v", n.id, info.Learners)
		}
	}

	// 扩副本前先放一条消息：扩副本不应影响已有数据。
	pubSess := session(t, nodes[0])
	res, err := pubSess.Publish(&plugin.Message{Body: []byte("before-grow")}, "", queue, false)
	if err != nil || !res.Routed {
		t.Fatalf("扩副本前发布失败: routed=%v err=%v", res.Routed, err)
	}
	if res.Durable != nil {
		if err := res.Durable(); err != nil {
			t.Fatalf("等待复制到多数派失败: %v", err)
		}
	}
	pubSess.Close()

	// grow 到 3：在**非组 leader** 的节点上发起，验证请求会被转发给组 leader。
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := nodes[1].b.GrowQuorumQueue(ctx, "/", queue, 3); err != nil {
		t.Fatalf("在节点 %s 上扩副本失败: %v", nodes[1].id, err)
	}
	waitQuorumVoters(t, nodes, queue, []string{"n1", "n2", "n3"})

	// 副本集落进元数据：每个节点看到的目标副本集都是 3 个节点。
	for _, n := range nodes {
		info := quorumInfoOn(t, n, queue)
		if len(info.Replicas) != 3 {
			t.Fatalf("节点 %s 记录的副本集应为 3 个，实际 %v", n.id, info.Replicas)
		}
		if len(info.Learners) != 0 {
			t.Fatalf("扩到 3 副本后不应再有 learner，节点 %s 实际 %v", n.id, info.Learners)
		}
	}

	// 幂等：再扩一次同一目标值不报错，也不改变结果。
	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", queue, 3); err != nil {
		t.Fatalf("重复扩到同一副本数应幂等成功，实际 %v", err)
	}
	waitQuorumVoters(t, nodes, queue, []string{"n1", "n2", "n3"})

	// 3 副本的容错：干掉一台机器，剩下 2/3 仍是多数派，队列照样可读可写。
	var victim *clusterNode
	for _, n := range nodes {
		if n.id != "n1" { // n1 持有初始消息的 leader 身份更可能，但杀哪台都应是 2/3
			victim = n
			break
		}
	}
	victim.b.Close()

	survivors := make([]*clusterNode, 0, 2)
	for _, n := range nodes {
		if n != victim {
			survivors = append(survivors, n)
		}
	}
	var writer *clusterNode
	waitFor(t, 30*time.Second, "剩余节点选出新的组 leader", func() bool {
		for _, n := range survivors {
			if n.b.ClusterStatus().Role == "shutdown" {
				continue
			}
			if info, ok := n.b.QuorumQueueInfo("/", queue); ok && info.Leader != "" && info.Leader != victim.id {
				writer = n
				return true
			}
		}
		return false
	})

	// 新写入必须能复制到多数派；扩副本前那条消息也必须还在。
	sess := session(t, writer)
	defer sess.Close()
	res, err = sess.Publish(&plugin.Message{Body: []byte("after-failure")}, "", queue, false)
	if err != nil || !res.Routed {
		t.Fatalf("杀掉一台机器后发布失败: routed=%v err=%v", res.Routed, err)
	}
	if res.Durable != nil {
		if err := res.Durable(); err != nil {
			t.Fatalf("杀掉一台机器后等待复制到多数派失败: %v", err)
		}
	}

	seen := map[string]bool{}
	waitFor(t, 30*time.Second, "两条消息都还在（含扩副本前的存量）", func() bool {
		d, ok, err := sess.Get(queue, true)
		if err != nil || !ok {
			return false
		}
		seen[string(d.Message.Body)] = true
		return seen["before-grow"] && seen["after-failure"]
	})
}

// TestQuorumGrowBounds 覆盖 grow 的边界：扩大上限、禁止缩容、非仲裁队列、单机模式。
func TestQuorumGrowBounds(t *testing.T) {
	const queue = "m8.quorum.bounds.q"
	const classic = "m8.classic.bounds.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)

	sess := session(t, nodes[0])
	defer sess.Close()
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{
		Name: queue, Durable: true,
		Arguments: map[string]any{"x-queue-type": "quorum", "x-quorum-initial-group-size": 1},
	}); err != nil {
		t.Fatalf("声明仲裁队列失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: classic, Durable: true})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", queue, 4); err == nil {
		t.Fatalf("扩到超过集群成员数（4 > 3）应当报错")
	}
	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", "no.such.queue", 2); err == nil {
		t.Fatalf("对不存在的队列扩副本应当报错")
	}
	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", classic, 2); err == nil {
		t.Fatalf("对经典队列扩副本应当报 406（没有副本集）")
	}
	// 先扩到 3，再尝试缩到 2：本期明确不支持缩容。
	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", queue, 3); err != nil {
		t.Fatalf("扩到 3 副本失败: %v", err)
	}
	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", queue, 2); err == nil {
		t.Fatalf("缩容应当报错（本期只实现 grow）")
	}
}

// TestQuorumGrowPersistsAcrossRestart：扩副本的结论落在 Raft 组自身与元数据两处，
// 节点重启后必须仍是 N 副本。
func TestQuorumGrowPersistsAcrossRestart(t *testing.T) {
	const queue = "m8.quorum.persist.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumSized(t, nodes[0], queue, 1)
	waitQuorumVoters(t, nodes, queue, []string{"n1"})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := nodes[0].b.GrowQuorumQueue(ctx, "/", queue, 3); err != nil {
		t.Fatalf("扩到 3 副本失败: %v", err)
	}
	waitQuorumVoters(t, nodes, queue, []string{"n1", "n2", "n3"})

	// 重启 n3（它的组状态、元数据都要靠自身持久化恢复）。
	peers := map[string]string{}
	for _, n := range nodes {
		peers[n.id] = fmt.Sprintf("127.0.0.1:%d", n.port)
	}
	victim := nodes[2]
	dir, port := victim.dir, victim.port
	victim.b.Close()

	restarted, err := broker.New(discardLogger(), clusterConfig(t, dir, victim.id, port, peers))
	if err != nil {
		t.Fatalf("重启节点 %s 失败: %v", victim.id, err)
	}
	restarted.SetMessageCodec(spec.NewMessageCodec())
	t.Cleanup(restarted.Close)
	victim.b = restarted

	waitQuorumVoters(t, nodes, queue, []string{"n1", "n2", "n3"})
	if info := quorumInfoOn(t, victim, queue); len(info.Replicas) != 3 {
		t.Fatalf("重启后副本集应为 3 个，实际 %v", info.Replicas)
	}
}

// TestQuorumRebalanceContract：rebalance 的行为契约（最小实现的边界）。
//
// 断言的是"可观察结果"，不依赖具体的选举时序：
//   - 有 leader 时调用必须成功；
//   - 报告迁移了 leader 时，目标节点必须是该队列的投票成员，且随后确实成为 leader；
//   - 没迁移时必须给出原因。
func TestQuorumRebalanceContract(t *testing.T) {
	const queue = "m8.quorum.rebalance.q"
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)
	declareQuorumSized(t, nodes[0], queue, 3)
	waitQuorumVoters(t, nodes, queue, []string{"n1", "n2", "n3"})

	// 等组 leader 选出来。
	waitFor(t, 30*time.Second, "仲裁队列选出组 leader", func() bool {
		info, ok := nodes[0].b.QuorumQueueInfo("/", queue)
		return ok && info.Leader != ""
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := nodes[0].b.RebalanceQuorumQueue(ctx, "/", queue)
	if err != nil {
		t.Fatalf("rebalance 失败: %v", err)
	}
	if res.From == "" {
		t.Fatalf("rebalance 应报告当前 leader，实际为空")
	}
	if !res.Moved {
		if res.Reason == "" {
			t.Fatalf("未迁移 leader 时必须给出原因")
		}
		t.Logf("rebalance 未迁移 leader：%s（from=%s）", res.Reason, res.From)
		return
	}
	t.Logf("rebalance 把 leader 从 %s 迁到 %s", res.From, res.To)
	if res.To == "" || res.To == res.From {
		t.Fatalf("迁移目标 %q 非法（from=%q）", res.To, res.From)
	}
	// 目标必须确实是投票成员。
	info, _ := nodes[0].b.QuorumQueueInfo("/", queue)
	if !containsID(info.Voters, res.To) {
		t.Fatalf("迁移目标 %s 不是该队列的投票成员（%v）", res.To, info.Voters)
	}
	// 断言的是**迁移的目的**：leader 不再是被判定为"太忙"的那个节点，且新 leader 是该组的投票成员。
	//
	// 不写成"必须等于 res.To"：换届是选举，谁来当 leader 由 Raft 决定，
	// 也可能选到第三个节点（尤其在机器被全量测试压满时）。迁走 + 仍是合法成员才是语义。
	waitFor(t, 30*time.Second, "leader 已从 "+res.From+" 迁走", func() bool {
		cur, ok := nodes[0].b.QuorumQueueInfo("/", queue)
		return ok && cur.Leader != "" && cur.Leader != res.From && containsID(cur.Voters, cur.Leader)
	})

	// 迁移后队列仍然可写可读（换届不该影响可用性）。
	sess := session(t, nodes[0])
	defer sess.Close()
	pub, err := sess.Publish(&plugin.Message{Body: []byte("after-rebalance")}, "", queue, false)
	if err != nil || !pub.Routed {
		t.Fatalf("rebalance 后发布失败: routed=%v err=%v", pub.Routed, err)
	}
}

// containsID 判断切片里是否有该 ID。
func containsID(ids []string, id string) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

// TestQuorumReplicationRejectedOnStandalone：单机模式下扩副本与再平衡都必须报 NOT_IMPLEMENTED。
func TestQuorumReplicationRejectedOnStandalone(t *testing.T) {
	b := newTestBroker(t)
	sess, err := testSessionOf(t, b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{
		Name: "m8.quorum.single.q", Durable: true,
		Arguments: map[string]any{"x-queue-type": "quorum"},
	}); err != nil {
		t.Fatalf("单机模式声明仲裁队列失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.GrowQuorumQueue(ctx, "/", "m8.quorum.single.q", 3); err == nil {
		t.Fatalf("单机模式扩副本应当报 NOT_IMPLEMENTED")
	}
	if _, err := b.RebalanceQuorumQueue(ctx, "/", "m8.quorum.single.q"); err == nil {
		t.Fatalf("单机模式再平衡应当报 NOT_IMPLEMENTED")
	}
}
