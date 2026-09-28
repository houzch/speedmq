package raft_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/raft"
)

// 本文件覆盖自研 Raft 的关键语义：选举、日志复制、多数派提交、少数派拒绝、
// 故障切换、重启恢复、落后节点追赶、崩溃尾部截断，以及内存网络本身的分区语义。
//
// 时间口径：一律"轮询 + 超时"（testTimeout 留足余量），不依赖真实时间精度，
// 也不用固定睡眠来假装同步。

const (
	testElectionTimeout = 200 * time.Millisecond
	testHeartbeat       = 25 * time.Millisecond
	testTimeout         = 5 * time.Second
	testPollInterval    = 5 * time.Millisecond
)

// testFSM 是观测型状态机：如实记录每条 Apply 的索引与载荷，
// 用于断言"顺序应用、不跳号、不重复"以及各节点状态是否一致。
type testFSM struct {
	mu      sync.Mutex
	applied []uint64
	values  []string
}

// fsmState 是 testFSM 的可比较快照（也是 Snapshot 的序列化形式）。
type fsmState struct {
	Applied []uint64 `json:"applied"`
	Values  []string `json:"values"`
}

func (f *testFSM) Apply(index uint64, data []byte) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, index)
	f.values = append(f.values, string(data))
	return string(data), nil
}

func (f *testFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(fsmState{Applied: append([]uint64(nil), f.applied...), Values: append([]string(nil), f.values...)})
}

func (f *testFSM) Restore(data []byte) error {
	var s fsmState
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("解析测试状态机快照失败: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = s.Applied
	f.values = s.Values
	return nil
}

func (f *testFSM) state() fsmState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fsmState{
		Applied: append([]uint64(nil), f.applied...),
		Values:  append([]string(nil), f.values...),
	}
}

func (f *testFSM) lastIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.applied) == 0 {
		return 0
	}
	return f.applied[len(f.applied)-1]
}

func (f *testFSM) hasValue(v string) bool { return f.hasValues([]string{v}) }

func (f *testFSM) hasValues(vals []string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, want := range vals {
		found := false
		for _, got := range f.values {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// assertContiguous 断言应用序列恰为 1,2,3,...（不跳号、不重复）。
func (f *testFSM) assertContiguous(t *testing.T, who string) {
	t.Helper()
	s := f.state()
	for i, idx := range s.Applied {
		if idx != uint64(i+1) {
			t.Fatalf("%s: 应用序列不连续/有重复，第 %d 项为 %d（期望 %d）: %v", who, i, idx, i+1, s.Applied)
		}
	}
}

// cluster 是内存网络上的一个测试集群。
type cluster struct {
	ids   []string
	net   raft.MemNetwork
	nodes map[string]*raft.Node
	fsms  map[string]*testFSM
}

type clusterConfig struct {
	electionTimeout   time.Duration
	heartbeat         time.Duration
	snapshotThreshold uint64
}

func startCluster(t *testing.T, ids ...string) *cluster {
	t.Helper()
	return startClusterCfg(t, clusterConfig{}, ids...)
}

func startClusterCfg(t *testing.T, cfg clusterConfig, ids ...string) *cluster {
	t.Helper()
	if cfg.electionTimeout == 0 {
		cfg.electionTimeout = testElectionTimeout
	}
	if cfg.heartbeat == 0 {
		cfg.heartbeat = testHeartbeat
	}
	c := &cluster{
		ids:   ids,
		net:   raft.NewMemNetwork(),
		nodes: make(map[string]*raft.Node, len(ids)),
		fsms:  make(map[string]*testFSM, len(ids)),
	}
	dirs := make(map[string]string, len(ids))
	for _, id := range ids {
		c.fsms[id] = &testFSM{}
		dirs[id] = t.TempDir()
	}
	for _, id := range ids {
		c.nodes[id] = newNodeIn(t, id, ids, dirs[id], c.net, c.fsms[id], cfg)
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

// newNodeIn 走公开装配路径构造一个节点。
func newNodeIn(t *testing.T, id string, peers []string, dir string, net raft.MemNetwork, fsm raft.FSM, cfg clusterConfig) *raft.Node {
	t.Helper()
	n, err := raft.New(raft.Options{
		ID:                id,
		Peers:             peers,
		Dir:               dir,
		Transport:         net.Transport(id),
		FSM:               fsm,
		ElectionTimeout:   cfg.electionTimeout,
		HeartbeatInterval: cfg.heartbeat,
		SnapshotThreshold: cfg.snapshotThreshold,
		Logger:            raft.NewLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	})
	if err != nil {
		t.Fatalf("构造节点 %s 失败: %v", id, err)
	}
	return n
}

func (c *cluster) propose(id, value string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	return c.nodes[id].Propose(ctx, []byte(value))
}

// proposeOnLeader 在当前 leader 上提案；若等待期间发生换届（ErrNotLeader），
// 重新发现 leader 后重试 —— 集群中"领导者可变"是常态，客户端必须容忍，
// 测试也不应把"某一瞬间的 leader 永久不变"当成前提。
func (c *cluster) proposeOnLeader(t *testing.T, ids []string, value string) string {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		leader := c.waitLeader(t, ids)
		if _, err := c.propose(leader, value); err == nil {
			return leader
		} else if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("Propose(%s) 在 leader %s 上失败: %v", value, leader, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("反复换届，未能在超时内完成提案 %s", value)
		}
	}
}

// stableLeader 等到一个"已完成本任期首次提交"的 leader。
//
// 提交意味着它已拿到多数派应答，其心跳也已被 followers 收到（选举计时被重置），
// 因此接下来一段时间不会再有peer 先超时换届 —— 需要"下线某个节点"这类
// 一次性动作的用例必须先拿到这样的 leader，否则动作可能落在换届窗口里。
func (c *cluster) stableLeader(t *testing.T, ids []string) string {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		leader := c.waitLeader(t, ids)
		st := c.nodes[leader].Status()
		// 还要求状态机追平提交点：leader 上任时的 no-op 在提交后可能有短暂应用延迟，
		// 若用例在"应用之前"记录基线，就会把随后到来的应用误判成"分区期间的状态变化"。
		if st.CommitIndex > 0 && c.fsms[leader].lastIndex() >= st.CommitIndex {
			return leader
		}
		if time.Now().After(deadline) {
			t.Fatalf("未能在超时内得到稳定的 leader")
		}
		time.Sleep(testPollInterval)
	}
}

// waitLeader 等给定节点集合收敛到"恰好一个 leader"，返回其 ID。
func (c *cluster) waitLeader(t *testing.T, ids []string) string {
	t.Helper()
	var leader string
	waitFor(t, "集群收敛到唯一领导者", func() bool {
		found := ""
		for _, id := range ids {
			if c.nodes[id].IsLeader() {
				if found != "" {
					return false
				}
				found = id
			}
		}
		if found == "" {
			return false
		}
		leader = found
		return true
	})
	return leader
}

func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(testPollInterval) // 轮询间隔，不是"睡够时间就当完成"
	}
	t.Fatalf("等待超时: %s", desc)
}

func others(ids []string, exclude string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != exclude {
			out = append(out, id)
		}
	}
	return out
}

func equalUint64(a, b []uint64) bool {
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

func equalString(a, b []string) bool {
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

// TestSingleNodeElectionAndPropose：单节点集群应立即成为领导者，Propose 立即提交并应用。
func TestSingleNodeElectionAndPropose(t *testing.T) {
	fsm := &testFSM{}
	n := newNodeIn(t, "n1", []string{"n1"}, t.TempDir(), raft.NewMemNetwork(), fsm, clusterConfig{})
	if err := n.Start(); err != nil {
		t.Fatalf("启动失败: %v", err)
	}
	t.Cleanup(n.Stop)

	waitFor(t, "单节点成为领导者", n.IsLeader)

	st := n.Status()
	if st.Role != raft.RoleLeader {
		t.Fatalf("角色期望 leader，实际 %s", st.Role)
	}
	if st.Term == 0 {
		t.Fatalf("任期不应为 0: %+v", st)
	}
	if !equalString(st.Peers, []string{"n1"}) {
		t.Fatalf("Peers 期望 [n1]，实际 %v", st.Peers)
	}
	if !n.HasQuorum() {
		t.Fatalf("单节点集群应恒有多数派")
	}
	if n.Leader() != "n1" {
		t.Fatalf("Leader() 期望 n1，实际 %q", n.Leader())
	}

	before := n.Status()
	res, err := n.Propose(context.Background(), []byte("alpha"))
	if err != nil {
		t.Fatalf("Propose 失败: %v", err)
	}
	if res != "alpha" {
		t.Fatalf("Propose 返回值期望 alpha，实际 %v", res)
	}
	waitFor(t, "alpha 已应用到状态机", func() bool { return fsm.hasValue("alpha") })

	after := n.Status()
	if after.LastLogIndex <= before.LastLogIndex {
		t.Fatalf("LastLogIndex 未递增: %d -> %d", before.LastLogIndex, after.LastLogIndex)
	}
	if after.CommitIndex <= before.CommitIndex {
		t.Fatalf("CommitIndex 未递增: %d -> %d", before.CommitIndex, after.CommitIndex)
	}
	if after.LastApplied <= before.LastApplied {
		t.Fatalf("LastApplied 未递增: %d -> %d", before.LastApplied, after.LastApplied)
	}
	if fsm.lastIndex() != after.LastApplied {
		t.Fatalf("状态机进度与 Status.LastApplied 不一致: %d vs %d", fsm.lastIndex(), after.LastApplied)
	}
	fsm.assertContiguous(t, "n1")
}

// TestThreeNodeElectionConverges：三节点最终恰好一个 leader，其余为 follower，且对 leader 的认知一致。
func TestThreeNodeElectionConverges(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")
	leader := c.waitLeader(t, c.ids)

	waitFor(t, "三节点对 Leader() 的认知一致", func() bool {
		for _, id := range c.ids {
			if c.nodes[id].Leader() != leader {
				return false
			}
		}
		return true
	})
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		if id == leader {
			if st.Role != raft.RoleLeader {
				t.Fatalf("节点 %s 期望 leader，实际 %s", id, st.Role)
			}
			continue
		}
		if st.Role != raft.RoleFollower {
			t.Fatalf("节点 %s 期望 follower，实际 %s", id, st.Role)
		}
		if !equalString(st.Peers, c.ids) {
			t.Fatalf("节点 %s 的 Peers 不合理: %v", id, st.Peers)
		}
	}
}

// TestLogReplicationOrderConsistent：leader 连续提案后所有节点状态一致且顺序一致。
func TestLogReplicationOrderConsistent(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")

	want := []string{"m0", "m1", "m2", "m3", "m4"}
	for _, v := range want {
		c.proposeOnLeader(t, c.ids, v)
	}
	waitFor(t, "所有节点都应用了全部载荷", func() bool {
		for _, id := range c.ids {
			if !c.fsms[id].hasValues(want) {
				return false
			}
		}
		return true
	})
	waitFor(t, "所有节点的应用进度一致", func() bool {
		target := c.nodes[c.ids[0]].Status().LastApplied
		for _, id := range c.ids {
			if c.nodes[id].Status().LastApplied != target {
				return false
			}
		}
		return true
	})

	base := c.fsms[c.ids[0]].state()
	for _, id := range c.ids {
		got := c.fsms[id].state()
		if !equalUint64(got.Applied, base.Applied) {
			t.Fatalf("节点 %s 的应用索引序列与基准节点不一致:\n%s=%v\n基准=%v", id, id, got.Applied, base.Applied)
		}
		if !equalString(got.Values, base.Values) {
			t.Fatalf("节点 %s 的应用载荷与基准节点不一致:\n%s=%v\n基准=%v", id, id, got.Values, base.Values)
		}
	}

	// 载荷的相对顺序必须与提案顺序一致（过滤掉无操作条目）。
	pos := 0
	for _, v := range base.Values {
		if pos < len(want) && v == want[pos] {
			pos++
		}
	}
	if pos != len(want) {
		t.Fatalf("载荷顺序与提案顺序不符，匹配 %d/%d: %v", pos, len(want), base.Values)
	}
}

// TestMajorityStillCommitsWithOneFollowerDown：3 节点掉 1 个仍能提交，且存活两节点状态一致。
func TestMajorityStillCommitsWithOneFollowerDown(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")
	leader := c.stableLeader(t, c.ids)
	rest := others(c.ids, leader)
	victim, survivor := rest[0], rest[1]

	c.net.Down(victim)

	leader = c.proposeOnLeader(t, c.ids, "majority")
	waitFor(t, "leader 与存活 follower 都已应用", func() bool {
		return c.fsms[leader].hasValue("majority") && c.fsms[survivor].hasValue("majority")
	})

	got := c.fsms[leader].state()
	other := c.fsms[survivor].state()
	if !equalUint64(got.Applied, other.Applied) || !equalString(got.Values, other.Values) {
		t.Fatalf("多数派两节点状态不一致:\n%s=%v %v\n%s=%v %v", leader, got.Applied, got.Values, survivor, other.Applied, other.Values)
	}
	if st := c.nodes[leader].Status(); st.CommitIndex != st.LastLogIndex {
		t.Fatalf("最新条目应已提交: commit=%d last=%d", st.CommitIndex, st.LastLogIndex)
	}
}

// TestMinorityCannotCommit：只剩 leader（1/3）时提案不得成功，也不得被应用。
func TestMinorityCannotCommit(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")
	leader := c.stableLeader(t, c.ids)

	for _, id := range others(c.ids, leader) {
		c.net.Down(id)
	}
	if !c.nodes[leader].IsLeader() {
		t.Fatalf("少数派被下线不应让 leader 失去角色（它只是失去了提交能力）")
	}

	before := c.nodes[leader].Status()
	beforeFSM := c.fsms[leader].state()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.nodes[leader].Propose(ctx, []byte("minority")); err == nil {
		t.Fatalf("仅剩 1/3 成员时 Propose 不应成功")
	} else if !errors.Is(err, raft.ErrNoQuorum) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 ErrNoQuorum 或 ctx 超时，实际 %v", err)
	}

	if c.fsms[leader].hasValue("minority") {
		t.Fatalf("少数派下的提案不得被应用")
	}
	if after := c.fsms[leader].state(); !equalUint64(after.Applied, beforeFSM.Applied) || !equalString(after.Values, beforeFSM.Values) {
		t.Fatalf("少数派下的提案不得改变状态机:\n前 %v\n后 %v", beforeFSM.Applied, after.Applied)
	}
	if after := c.nodes[leader].Status(); after.CommitIndex != before.CommitIndex {
		t.Fatalf("少数派下 CommitIndex 不应推进: %d -> %d", before.CommitIndex, after.CommitIndex)
	}
}

// TestLeaderFailover：leader 不可达后剩余 2 节点选出新 leader，并能提交新提案。
func TestLeaderFailover(t *testing.T) {
	c := startCluster(t, "n1", "n2", "n3")
	old := c.stableLeader(t, c.ids)

	c.net.Down(old)
	alive := others(c.ids, old)
	// 剩余 2 个节点仍是多数派，必须能选出新 leader 并完成提交。
	newLeader := c.proposeOnLeader(t, alive, "after-failover")
	if newLeader == old {
		t.Fatalf("新 leader 不应是已下线的节点")
	}
	if c.nodes[old].HasQuorum() {
		t.Fatalf("被隔离的旧 leader 不应仍认为拥有多数派")
	}

	waitFor(t, "新 leader 应用了提案", func() bool { return c.fsms[newLeader].hasValue("after-failover") })
	survivor := others(alive, newLeader)[0]
	waitFor(t, "存活 follower 追平", func() bool { return c.fsms[survivor].hasValue("after-failover") })

	if c.nodes[newLeader].Status().Term <= c.nodes[old].Status().Term {
		t.Fatalf("新 leader 任期应高于旧 leader: new=%d old=%d",
			c.nodes[newLeader].Status().Term, c.nodes[old].Status().Term)
	}
}

// TestRestartRecoversTermAndLog：同一 Dir 重启后任期不回退、日志仍在、状态被完整重放。
func TestRestartRecoversTermAndLog(t *testing.T) {
	dir := t.TempDir()
	fsm1 := &testFSM{}
	n1 := newNodeIn(t, "n1", []string{"n1"}, dir, raft.NewMemNetwork(), fsm1, clusterConfig{})
	if err := n1.Start(); err != nil {
		t.Fatalf("启动失败: %v", err)
	}
	t.Cleanup(n1.Stop)

	waitFor(t, "n1 成为 leader", n1.IsLeader)
	vals := []string{"v1", "v2", "v3"}
	for _, v := range vals {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		_, err := n1.Propose(ctx, []byte(v))
		cancel()
		if err != nil {
			t.Fatalf("Propose(%s) 失败: %v", v, err)
		}
	}
	waitFor(t, "全部条目已应用", func() bool { return fsm1.hasValues(vals) })

	termBefore := n1.Status().Term
	logBefore := n1.Status().LastLogIndex
	appliedBefore := fsm1.state()
	n1.Stop()

	// 重新 New + Start：同一个 Dir，全新网络与全新状态机（等价于进程重启）。
	fsm2 := &testFSM{}
	n2 := newNodeIn(t, "n1", []string{"n1"}, dir, raft.NewMemNetwork(), fsm2, clusterConfig{})
	if err := n2.Start(); err != nil {
		t.Fatalf("重启节点失败: %v", err)
	}
	t.Cleanup(n2.Stop)

	waitFor(t, "重启后重新成为 leader", n2.IsLeader)
	st := n2.Status()
	if st.Term < termBefore {
		t.Fatalf("任期回退: 重启前 %d，重启后 %d", termBefore, st.Term)
	}
	if st.LastLogIndex < logBefore {
		t.Fatalf("日志条目丢失: 重启前 %d，重启后 %d", logBefore, st.LastLogIndex)
	}
	// 新 leader 提交本任期的无操作条目后，旧条目被隐式提交并重放。
	waitFor(t, "重启后状态机追平旧日志", func() bool { return fsm2.lastIndex() >= logBefore })
	fsm2.assertContiguous(t, "n1(重启后)")
	for _, v := range vals {
		if !fsm2.hasValue(v) {
			t.Fatalf("重启后状态机缺少 %s: %v", v, fsm2.state().Values)
		}
	}
	after := fsm2.state()
	if len(after.Values) < len(appliedBefore.Values) {
		t.Fatalf("重启后重放不完整: 前 %d 条，后 %d 条", len(appliedBefore.Values), len(after.Values))
	}
	if !equalString(after.Values[:len(appliedBefore.Values)], appliedBefore.Values) {
		t.Fatalf("重启后重放顺序与重启前不一致:\n前 %v\n后 %v", appliedBefore.Values, after.Values)
	}
}

// TestLaggingFollowerCatchesUp：长时间下线的 follower 重新上线后追平（覆盖快照安装路径）。
func TestLaggingFollowerCatchesUp(t *testing.T) {
	// 缩小快照阈值，确保 leader 在 follower 离线期间完成日志压缩，
	// 从而走 InstallSnapshot 这条路径而不是逐条回退复制。
	c := startClusterCfg(t, clusterConfig{snapshotThreshold: 8}, "n1", "n2", "n3")
	leader := c.stableLeader(t, c.ids)
	rest := others(c.ids, leader)
	victim, survivor := rest[0], rest[1]

	c.net.Down(victim)

	want := make([]string, 0, 24)
	for i := 0; i < 24; i++ {
		v := fmt.Sprintf("lag%02d", i)
		want = append(want, v)
		leader = c.proposeOnLeader(t, c.ids, v)
	}
	waitFor(t, "leader 已压缩日志（覆盖快照路径）", func() bool {
		return c.nodes[leader].Status().SnapshotIndex > 0
	})
	waitFor(t, "在线节点追平", func() bool { return c.fsms[survivor].hasValues(want) })

	c.net.Up(victim)
	waitFor(t, "落后节点追平", func() bool { return c.fsms[victim].hasValues(want) })
	waitFor(t, "落后节点进度与 leader 一致", func() bool {
		return c.nodes[victim].Status().LastApplied == c.nodes[leader].Status().LastApplied
	})

	// 该节点离线期间收不到任何日志，因此本地不可能自行压缩；
	// SnapshotIndex > 0 只可能来自 leader 的整体快照安装 —— 这正是本用例要覆盖的路径。
	if st := c.nodes[victim].Status(); st.SnapshotIndex == 0 {
		t.Fatalf("落后节点应通过 InstallSnapshot 追赶（SnapshotIndex 仍为 0）")
	}

	got := c.fsms[victim].state()
	base := c.fsms[leader].state()
	if !equalUint64(got.Applied, base.Applied) || !equalString(got.Values, base.Values) {
		t.Fatalf("追赶后状态与 leader 不一致:\n落后节点 %v\nleader %v", got.Values, base.Values)
	}
}

// TestHalfWrittenLogTailIsDiscarded：raft.log 尾部半写记录必须被截断，且已提交条目不丢。
func TestHalfWrittenLogTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	fsm1 := &testFSM{}
	n1 := newNodeIn(t, "n1", []string{"n1"}, dir, raft.NewMemNetwork(), fsm1, clusterConfig{})
	if err := n1.Start(); err != nil {
		t.Fatalf("启动失败: %v", err)
	}
	t.Cleanup(n1.Stop)

	waitFor(t, "n1 成为 leader", n1.IsLeader)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	_, err := n1.Propose(ctx, []byte("committed"))
	cancel()
	if err != nil {
		t.Fatalf("Propose 失败: %v", err)
	}
	waitFor(t, "已应用", func() bool { return fsm1.hasValue("committed") })

	termBefore := n1.Status().Term
	logBefore := n1.Status().LastLogIndex
	n1.Stop()

	// 模拟"写一半被杀"：帧头声明 128 字节载荷，实际只落 10 字节。
	logPath := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("打开 raft.log 失败: %v", err)
	}
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[0:4], 128)
	binary.BigEndian.PutUint32(hdr[4:8], 0xdeadbeef)
	if _, err := f.Write(hdr); err != nil {
		t.Fatalf("写入半条记录头失败: %v", err)
	}
	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatalf("写入半条记录载荷失败: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("关闭 raft.log 失败: %v", err)
	}

	fsm2 := &testFSM{}
	n2 := newNodeIn(t, "n1", []string{"n1"}, dir, raft.NewMemNetwork(), fsm2, clusterConfig{})
	if err := n2.Start(); err != nil {
		t.Fatalf("半写记录导致恢复失败: %v", err)
	}
	t.Cleanup(n2.Stop)

	waitFor(t, "恢复后重新成为 leader", n2.IsLeader)
	st := n2.Status()
	if st.Term < termBefore {
		t.Fatalf("任期回退: 之前 %d，恢复后 %d", termBefore, st.Term)
	}
	// 半写记录必须被丢弃：日志长度只应增加新任期的那条无操作条目。
	if st.LastLogIndex != logBefore+1 {
		t.Fatalf("半写记录未被正确截断: 期望 lastLogIndex=%d，实际 %d", logBefore+1, st.LastLogIndex)
	}
	waitFor(t, "已提交条目被完整重放", func() bool { return fsm2.hasValue("committed") })
	fsm2.assertContiguous(t, "n1(半写恢复)")
}

// TestMemNetworkPartitionAndHeal：内存网络的分区/恢复/下线语义本身必须可用。
func TestMemNetworkPartitionAndHeal(t *testing.T) {
	const echoMethod = "test.echo"
	net := raft.NewMemNetwork()
	n1 := net.Transport("n1")
	n2 := net.Transport("n2")
	n3 := net.Transport("n3")
	if net.Transport("n1") != n1 {
		t.Fatalf("同一 id 的 Transport 应返回同一实例")
	}

	echo := func(_ context.Context, from string, payload []byte) ([]byte, error) {
		return []byte(from + "|" + string(payload)), nil
	}
	for _, tr := range []raft.Transport{n1, n2, n3} {
		if err := tr.Serve(echoMethod, echo); err != nil {
			t.Fatalf("注册 RPC 处理器失败: %v", err)
		}
	}
	if err := n1.Serve(echoMethod, echo); !errors.Is(err, raft.ErrDuplicateMethod) {
		t.Fatalf("重复注册应返回 ErrDuplicateMethod，实际 %v", err)
	}
	t.Cleanup(func() {
		_ = n1.Close()
		_ = n2.Close()
		_ = n3.Close()
	})

	call := func(tr raft.Transport, to, method, msg string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := tr.Call(ctx, to, method, []byte(msg))
		if err != nil {
			return "", err
		}
		return string(resp), nil
	}

	// 默认全连通。
	if got, err := call(n1, "n2", echoMethod, "hi"); err != nil || got != "n1|hi" {
		t.Fatalf("默认拓扑应全连通，得到 %q, %v", got, err)
	}
	if _, err := call(n1, "n2", "test.unknown", "hi"); err == nil {
		t.Fatalf("未知方法应返回错误")
	}

	// 分成 {n1} 与 {n2,n3} 两组。
	net.Partition([]string{"n1"}, []string{"n2", "n3"})
	if _, err := call(n1, "n2", echoMethod, "x"); err == nil {
		t.Fatalf("分区后跨组调用 n1→n2 应失败")
	}
	if _, err := call(n1, "n3", echoMethod, "x"); err == nil {
		t.Fatalf("分区后跨组调用 n1→n3 应失败")
	}
	if got, err := call(n2, "n3", echoMethod, "x"); err != nil || got != "n2|x" {
		t.Fatalf("分区后组内应仍互通，得到 %q, %v", got, err)
	}

	// Heal 恢复全连通。
	net.Heal()
	if got, err := call(n1, "n2", echoMethod, "x"); err != nil || got != "n1|x" {
		t.Fatalf("Heal 后应恢复连通，得到 %q, %v", got, err)
	}

	// Down/Up：双向不可达。
	net.Down("n2")
	if _, err := call(n1, "n2", echoMethod, "x"); err == nil {
		t.Fatalf("Down 后对 n2 的调用应失败")
	}
	if _, err := call(n2, "n1", echoMethod, "x"); err == nil {
		t.Fatalf("Down 后 n2 也不应能发出请求")
	}
	net.Up("n2")
	if got, err := call(n1, "n2", echoMethod, "x"); err != nil || got != "n1|x" {
		t.Fatalf("Up 后应恢复连通，得到 %q, %v", got, err)
	}
}

// TestTCPTransportRoundTrip 覆盖真实 TCP 传输的编解码。
//
// 这一条是"只有真端口才能守住"的用例：此前请求载荷的长度按 u16 读、按 u32 写，
// 真实集群里每次 RPC 的载荷都变成空串（选举因此永远无法完成），
// 而进程内内存网络绕过了编解码，测试全绿也发现不了。
func TestTCPTransportRoundTrip(t *testing.T) {
	logger := raft.NewLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	aAddr := freeTCPAddr(t)
	bAddr := freeTCPAddr(t)
	for bAddr == aAddr {
		// 端口在"关闭临时监听"后可能被再次分配：确保两个节点拿到不同地址。
		bAddr = freeTCPAddr(t)
	}
	peers := map[string]string{"a": aAddr, "b": bAddr}

	ta, err := raft.NewTCPTransport(aAddr, "a", peers, logger)
	if err != nil {
		t.Fatalf("创建传输 a 失败: %v", err)
	}
	t.Cleanup(func() { _ = ta.Close() })
	tb, err := raft.NewTCPTransport(bAddr, "b", peers, logger)
	if err != nil {
		t.Fatalf("创建传输 b 失败: %v", err)
	}
	t.Cleanup(func() { _ = tb.Close() })

	const echoMethod = "test.tcp_echo"
	// 载荷刻意超过 255 字节：长度字段一旦按错的口径读，立刻就能看出来。
	want := []byte(`{"op":"queue.put","payload":{"vhost":"/","name":"` +
		string(bytes.Repeat([]byte("q"), 300)) + `"}}`)
	if err := tb.Serve(echoMethod, func(_ context.Context, from string, payload []byte) ([]byte, error) {
		if from != "a" {
			return nil, fmt.Errorf("来源标识错误: %q", from)
		}
		return payload, nil
	}); err != nil {
		t.Fatalf("注册 RPC 处理器失败: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := ta.Call(ctx, "b", echoMethod, want)
	if err != nil {
		t.Fatalf("TCP RPC 失败: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("回显不一致: got %d 字节 want %d 字节", len(got), len(want))
	}
}

// freeTCPAddr 预留一个空闲的本地 TCP 地址（绑定后立即关闭）。
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("分配空闲端口失败: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("释放临时监听失败: %v", err)
	}
	return addr
}
