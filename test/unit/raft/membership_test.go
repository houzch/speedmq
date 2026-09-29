package raft_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/raft"
)

// 本文件覆盖 M6d 的成员变更语义：learner 加入 → 追平 → 提升为 voter，
// 以及移除成员、成员表持久化、learner 不参与表决。
//
// 时间口径与 raft_test.go 一致：轮询 + 超时，不依赖固定睡眠假装同步。

// startClusterWithLearner 起一个集群，其中 learners 里的节点以 Options.Learners 引导
// （即"新节点先以 learner 身份加入"）。
func startClusterWithLearner(t *testing.T, voters []string, learners []string) *cluster {
	t.Helper()
	cfg := clusterConfig{electionTimeout: testElectionTimeout, heartbeat: testHeartbeat}
	return startClusterFull(t, cfg, voters, learners)
}

// startClusterFull 是全量构造：voters 是投票成员，learners 是非投票成员。
func startClusterFull(t *testing.T, cfg clusterConfig, voters, learners []string) *cluster {
	t.Helper()
	if cfg.electionTimeout == 0 {
		cfg.electionTimeout = testElectionTimeout
	}
	if cfg.heartbeat == 0 {
		cfg.heartbeat = testHeartbeat
	}
	ids := append(append([]string(nil), voters...), learners...)
	c := &cluster{
		ids:   ids,
		net:   raft.NewMemNetwork(),
		nodes: make(map[string]*raft.Node, len(ids)),
		fsms:  make(map[string]*testFSM, len(ids)),
	}
	for _, id := range ids {
		c.fsms[id] = &testFSM{}
	}
	for _, id := range ids {
		// 成员表的引导规则：既有节点只知道投票成员；只有新节点自己
		// 以 learner 身份启动（否则既有节点会"提前"认为新节点已是成员）。
		var nodeLearners []string
		for _, l := range learners {
			if l == id {
				nodeLearners = []string{id}
			}
		}
		n, err := raft.New(raft.Options{
			ID:                id,
			Peers:             voters,
			Learners:          nodeLearners,
			Dir:               t.TempDir(),
			Transport:         c.net.Transport(id),
			FSM:               c.fsms[id],
			ElectionTimeout:   cfg.electionTimeout,
			HeartbeatInterval: cfg.heartbeat,
			Logger:            raft.NewLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		})
		if err != nil {
			t.Fatalf("构造节点 %s 失败: %v", id, err)
		}
		c.nodes[id] = n
	}
	for _, id := range ids {
		if err := c.nodes[id].Start(); err != nil {
			t.Fatalf("启动节点 %s 失败: %v", id, err)
		}
	}
	t.Cleanup(func() {
		for _, id := range ids {
			c.nodes[id].Stop()
		}
	})
	return c
}

// changeMembership 在 leader 上提交一次成员变更。
func (c *cluster) changeMembership(t *testing.T, leader string, cc raft.ConfChange) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	return c.nodes[leader].ChangeMembership(ctx, cc)
}

// addVoter 走完整流程把一个已经在集群里的 learner 提升为投票成员：
// 加为 learner（幂等）→ 等追平 → 提升。
func (c *cluster) addVoter(t *testing.T, leader, id string) {
	t.Helper()
	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfAddLearner, ID: id}); err != nil {
		t.Fatalf("把 %s 加为 learner 失败: %v", id, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := c.nodes[leader].AwaitCatchUp(ctx, id); err != nil {
		t.Fatalf("等待 %s 追平失败: %v", id, err)
	}
	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfPromote, ID: id}); err != nil {
		t.Fatalf("提升 %s 为投票成员失败: %v", id, err)
	}
}

// waitMembership 等待某个节点的成员划分满足断言。
func (c *cluster) waitMembership(t *testing.T, id string, ok func(raft.Membership) bool) {
	t.Helper()
	waitFor(t, fmt.Sprintf("节点 %s 的成员划分达到期望", id), func() bool {
		return ok(c.nodes[id].Membership())
	})
}

// TestAddLearnerThenPromote：把一个以 learner 引导的节点提升为投票成员，
// 之后它开始参与表决，且全体节点对成员表达成一致。
func TestAddLearnerThenPromote(t *testing.T) {
	c := startClusterWithLearner(t, []string{"n1", "n2", "n3"}, []string{"n4"})
	leader := c.stableLeader(t, []string{"n1", "n2", "n3"})

	// 加入前：n4 是 learner，不投票、不竞选；集群多数派仍是 2/3。
	if m := c.nodes["n4"].Membership(); len(m.Learners) != 1 || m.Learners[0] != "n4" {
		t.Fatalf("n4 初始应为 learner，实际 %+v", m)
	}
	if c.nodes["n4"].IsLeader() {
		t.Fatalf("learner 不应成为领导者")
	}

	c.addVoter(t, leader, "n4")

	// 全体（含新成员）都必须看到 n4 进入投票成员表。
	for _, id := range c.ids {
		c.waitMembership(t, id, func(m raft.Membership) bool {
			return len(m.Voters) == 4 && len(m.Learners) == 0
		})
	}

	// 提升后 n4 参与表决：4 成员的新配置下，停掉一个节点仍是多数派。
	c.net.Down("n1")
	leader = c.proposeOnLeader(t, []string{"n2", "n3", "n4"}, "after-promote")
	waitFor(t, "提升后 3/4 仍能提交", func() bool { return c.fsms[leader].hasValue("after-promote") })
}

// TestPromotedMemberCountsTowardQuorum：提升为 voter 后，它计入多数派 ——
// 4 节点集群里 3 个节点（含新成员）能继续提交，这一点在旧配置下是不同的。
func TestPromotedMemberCountsTowardQuorum(t *testing.T) {
	c := startClusterWithLearner(t, []string{"n1", "n2", "n3"}, []string{"n4"})
	leader := c.stableLeader(t, []string{"n1", "n2", "n3"})
	c.addVoter(t, leader, "n4")

	// 全体一致后重新找 leader（成员变更可能引起一次换届）。
	//
	// 这里不用 stableLeader：它要求"状态机进度 >= 提交点"，而成员变更条目
	// 不进状态机，因此该条件在发生过成员变更的集群上永远不成立。
	alive := []string{"n1", "n2", "n3", "n4"}
	for _, id := range alive {
		c.waitMembership(t, id, func(m raft.Membership) bool { return len(m.Voters) == 4 })
	}
	leader = c.waitLeader(t, alive)

	// 停掉 leader，剩余 3/4 仍是多数派，必须能选出新 leader 并提交。
	c.net.Down(leader)
	rest := others(alive, leader)
	newLeader := c.proposeOnLeader(t, rest, "after-grow")
	if newLeader == leader {
		t.Fatalf("新 leader 不应是已下线的节点")
	}
	waitFor(t, "新 leader 已应用提案", func() bool { return c.fsms[newLeader].hasValue("after-grow") })
}

// TestRemoveMember：把一个投票成员移除后，剩下 2 个节点仍能提交（2/3 多数派）。
func TestRemoveMember(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")
	leader := c.stableLeader(t, c.ids)
	victim := others(c.ids, leader)[0]

	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfRemove, ID: victim}); err != nil {
		t.Fatalf("移除 %s 失败: %v", victim, err)
	}
	for _, id := range others(c.ids, victim) {
		c.waitMembership(t, id, func(m raft.Membership) bool {
			return equalString(m.Voters, others(c.ids, victim))
		})
	}

	// 被移除的节点不再被计入多数派：把剩下的 3 个里再停掉 1 个，
	// 剩余 2 个（新配置的多数派）仍能提交。
	c.net.Down(victim)
	leader = c.proposeOnLeader(t, others(c.ids, victim), "after-shrink")
	waitFor(t, "缩容后仍能提交", func() bool { return c.fsms[leader].hasValue("after-shrink") })
}

// TestMembershipSurvivesRestart：成员变更必须落盘 —— 重启后不得退回初始成员表。
func TestMembershipSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	voters := []string{"n1"}
	cfg := clusterConfig{electionTimeout: testElectionTimeout, heartbeat: testHeartbeat}

	// 每次启动都用全新的内存网络与状态机：等价于"进程重启"。
	// 复用同一个网络会撞上"RPC 方法重复注册"（旧端点仍持有处理器）。
	start := func() *raft.Node {
		n, err := raft.New(raft.Options{
			ID: "n1", Peers: voters, Dir: dir, Transport: raft.NewMemNetwork().Transport("n1"),
			FSM:             &testFSM{},
			ElectionTimeout: cfg.electionTimeout, HeartbeatInterval: cfg.heartbeat,
			Logger: raft.NewLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		})
		if err != nil {
			t.Fatalf("构造 n1 失败: %v", err)
		}
		return n
	}

	// 单节点集群（自己就是多数派）才能独立完成一次成员变更：把 n2 加为 learner。
	n1 := start()
	if err := n1.Start(); err != nil {
		t.Fatalf("启动 n1 失败: %v", err)
	}
	waitFor(t, "n1 成为 leader", n1.IsLeader)
	if err := n1.ChangeMembership(context.Background(),
		raft.ConfChange{Op: raft.ConfAddLearner, ID: "n2", Addr: "127.0.0.1:9999"}); err != nil {
		t.Fatalf("加入 learner n2 失败: %v", err)
	}
	want := n1.Membership()
	n1.Stop()

	// 重启：必须记得 n2 是 learner（而不是退回"只有 n1"）。
	n1b := start()
	if err := n1b.Start(); err != nil {
		t.Fatalf("重启 n1 失败: %v", err)
	}
	t.Cleanup(n1b.Stop)
	got := n1b.Membership()
	if !equalString(got.Voters, want.Voters) || !equalString(got.Learners, want.Learners) {
		t.Fatalf("重启后成员表丢失: 期望 %+v，实际 %+v", want, got)
	}
	if len(got.Learners) != 1 || got.Learners[0] != "n2" {
		t.Fatalf("重启后 n2 应仍是 learner，实际 %+v", got)
	}
}

// TestLearnerDoesNotVoteNorCount：learner 不参与表决 —— 3 投票成员 + 1 learner 时，
// 停掉 2 个投票成员即失去多数派（learner 的两票不算数）。
func TestLearnerDoesNotVoteNorCount(t *testing.T) {
	c := startClusterWithLearner(t, []string{"n1", "n2", "n3"}, []string{"n4"})
	leader := c.stableLeader(t, []string{"n1", "n2", "n3"})

	// 保留 leader，下线其余两个投票成员；n4（learner）在线也不算多数派。
	rest := others([]string{"n1", "n2", "n3"}, leader)
	for _, id := range rest {
		c.net.Down(id)
	}
	waitFor(t, "leader 判定失去多数派", func() bool { return !c.nodes[leader].HasQuorum() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.nodes[leader].Propose(ctx, []byte("should-fail")); err == nil {
		t.Fatalf("learner 不应帮助凑出多数派：Propose 必须失败")
	}
	if c.fsms[leader].hasValue("should-fail") {
		t.Fatalf("失去多数派时不得应用提案")
	}
}

// TestMembershipValidation：非 leader 提交成员变更必须被拒绝；重复加入/提升不存在者同样被拒。
func TestMembershipValidation(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")
	leader := c.stableLeader(t, c.ids)
	follower := others(c.ids, leader)[0]

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.nodes[follower].ChangeMembership(ctx, raft.ConfChange{Op: raft.ConfAddLearner, ID: "n9"})
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("follower 提交成员变更应返回 ErrNotLeader，实际 %v", err)
	}

	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfAddLearner, ID: leader}); !errors.Is(err, raft.ErrMemberExists) {
		t.Fatalf("重复加入已存在成员应返回 ErrMemberExists，实际 %v", err)
	}
	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfPromote, ID: "nope"}); !errors.Is(err, raft.ErrUnknownMember) {
		t.Fatalf("提升不存在的成员应返回 ErrUnknownMember，实际 %v", err)
	}
	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfPromote, ID: follower}); !errors.Is(err, raft.ErrNotLearner) {
		t.Fatalf("提升非 learner 应返回 ErrNotLearner，实际 %v", err)
	}
	if err := c.changeMembership(t, leader, raft.ConfChange{Op: raft.ConfRemove, ID: "nope"}); !errors.Is(err, raft.ErrUnknownMember) {
		t.Fatalf("移除不存在的成员应返回 ErrUnknownMember，实际 %v", err)
	}
}
