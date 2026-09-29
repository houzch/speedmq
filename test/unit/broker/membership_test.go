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

// 本文件在真实 TCP 三节点集群上验证 M6d 的动态成员变更：
//   - 新节点以 learner 身份启动 → 被集群接纳为 learner → 追平 → 提升为投票成员；
//   - 成员变更经 Raft 复制到全体节点，并且**不需要改任何节点的配置文件**；
//   - 移除成员后多数派按新配置计算（无需重启节点）；
//   - 在非 leader 节点上发起成员变更会自动转发给 leader；
//   - 单机模式下明确报 NOT_IMPLEMENTED，而不是假装成功。

// startJoinNode 以"加入模式"启动一个新节点：它的地址簿包含全体成员，但它自己只是 learner。
func startJoinNode(t *testing.T, id string, peers map[string]string, ownPort int) *clusterNode {
	t.Helper()
	cfg := clusterConfig(t, t.TempDir(), id, ownPort, peers)
	// 加入模式：本节点不参与投票、不发起竞选，等待被 leader 提升。
	cfg.Cluster.Join = true
	b, err := broker.New(discardLogger(), cfg)
	if err != nil {
		t.Fatalf("启动加入节点 %s 失败: %v", id, err)
	}
	b.SetMessageCodec(spec.NewMessageCodec())
	t.Cleanup(b.Close)
	return &clusterNode{id: id, dir: cfg.DataDir, port: ownPort, b: b}
}

// membersOf 读取某节点的成员划分。
func membersOf(node *clusterNode) (voters, learners []string) {
	m := node.b.ClusterMembers()
	return m.Voters, m.Learners
}

// waitMembershipOnAll 等全部节点看到同一份成员划分。
func waitMembershipOnAll(t *testing.T, nodes []*clusterNode, wantVoters, wantLearners []string) {
	t.Helper()
	waitFor(t, 20*time.Second, fmt.Sprintf("全部节点成员表收敛为 voters=%v learners=%v", wantVoters, wantLearners), func() bool {
		for _, n := range nodes {
			v, l := membersOf(n)
			if !equalStrings(v, wantVoters) || !equalStrings(l, wantLearners) {
				return false
			}
		}
		return true
	})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestClusterDynamicMembership：加入新节点（learner → voter）、移除成员，全程不改配置文件。
func TestClusterDynamicMembership(t *testing.T) {
	nodes := startTestCluster(t, 3)
	leader := waitClusterLeader(t, nodes)

	// 先在集群里放一条 durable 拓扑：新节点追平后应当也能看到它。
	sess, err := testSessionOf(t, nodes[0].b, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "m6d.membership.q", Durable: true})

	// 第 4 个节点：地址簿含全体成员，但以 learner 身份启动（不在任何人的投票成员表里）。
	allPeers := map[string]string{}
	for _, n := range nodes {
		allPeers[n.id] = fmt.Sprintf("127.0.0.1:%d", n.port)
	}
	port4 := freePort(t)
	allPeers["n4"] = fmt.Sprintf("127.0.0.1:%d", port4)
	n4 := startJoinNode(t, "n4", allPeers, port4)
	all := append(append([]*clusterNode(nil), nodes...), n4)

	// 加入前：n4 自己是 learner，既有节点根本不知道 n4。
	voters, learners := membersOf(n4)
	if !equalStrings(voters, []string{"n1", "n2", "n3"}) || !equalStrings(learners, []string{"n4"}) {
		t.Fatalf("n4 启动时应是 learner，实际 voters=%v learners=%v", voters, learners)
	}
	for _, n := range nodes {
		if v, _ := membersOf(n); !equalStrings(v, []string{"n1", "n2", "n3"}) {
			t.Fatalf("加入前节点 %s 的投票成员应为 [n1 n2 n3]，实际 %v", n.id, v)
		}
	}

	// 在一个**非 leader** 节点上发起加入：验证请求会被转发给 leader。
	var notLeader *clusterNode
	for _, n := range nodes {
		if n.id != leader.id {
			notLeader = n
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := notLeader.b.AddClusterMember(ctx, "n4", allPeers["n4"]); err != nil {
		t.Fatalf("在非 leader 节点 %s 上加入 n4 失败: %v", notLeader.id, err)
	}

	// 加入后：全体（含 n4）都应看到 4 个投票成员、0 个 learner。
	waitMembershipOnAll(t, all, []string{"n1", "n2", "n3", "n4"}, nil)

	// n4 追平了元数据：那条 durable 队列在它上面也可见。
	waitFor(t, 20*time.Second, "新节点追平元数据（能看到既有队列）", func() bool {
		_, ok := n4.b.QueueSnapshot("/", "m6d.membership.q")
		return ok
	})

	// 新节点能真正参与共识：在它上面打开会话并声明一条 durable 队列，
	// 声明必须经 leader 提交后复制到全体。
	sess4, err := testSessionOf(t, n4.b, "guest", "guest")
	if err != nil {
		t.Fatalf("在新节点上打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess4, plugin.QueueDeclare{Name: "m6d.from.n4.q", Durable: true})
	waitFor(t, 20*time.Second, "新节点提交的拓扑复制到全体", func() bool {
		for _, n := range all {
			if _, ok := n.b.QueueSnapshot("/", "m6d.from.n4.q"); !ok {
				return false
			}
		}
		return true
	})

	// 移除一个成员：多数派按新配置（3 成员 → 2）计算。
	leader = waitClusterLeader(t, all)
	remover := leader
	if err := remover.b.RemoveClusterMember(ctx, "n2"); err != nil {
		t.Fatalf("移除 n2 失败: %v", err)
	}
	survivors := []*clusterNode{}
	for _, n := range all {
		if n.id != "n2" {
			survivors = append(survivors, n)
		}
	}
	waitMembershipOnAll(t, survivors, []string{"n1", "n3", "n4"}, nil)

	// 移除是幂等的：再移除一次同一个节点不应报错。
	leader = waitClusterLeader(t, survivors)
	if err := leader.b.RemoveClusterMember(ctx, "n2"); err != nil {
		t.Fatalf("重复移除同一成员应幂等成功，实际 %v", err)
	}

	// 缩容后集群仍能正常提交：随便挑一个幸存节点声明队列。
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "m6d.after.shrink.q", Durable: true})
	waitFor(t, 20*time.Second, "缩容后仍能复制拓扑", func() bool {
		for _, n := range survivors {
			if _, ok := n.b.QueueSnapshot("/", "m6d.after.shrink.q"); !ok {
				return false
			}
		}
		return true
	})
}

// TestMemberChangeRejectedOnStandalone：单机模式下成员变更必须明确报 NOT_IMPLEMENTED。
func TestMemberChangeRejectedOnStandalone(t *testing.T) {
	b := newTestBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.AddClusterMember(ctx, "n9", "127.0.0.1:19999"); err == nil {
		t.Fatalf("单机模式下加入成员应报错")
	}
	if err := b.RemoveClusterMember(ctx, "n9"); err == nil {
		t.Fatalf("单机模式下移除成员应报错")
	}
	if m := b.ClusterMembers(); len(m.Voters) != 0 || len(m.Learners) != 0 {
		t.Fatalf("单机模式的成员划分应为空，实际 %+v", m)
	}
}
