package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Node 必须满足对外契约 Consensus。
var _ Consensus = (*Node)(nil)

// maxAppendEntries 是单次 AppendEntries 携带的最大条目数。
//
// 限制批大小的理由：一条超大 RPC 会长时间占用发送方协程与接收方主锁，
// 反而拖慢心跳与选举；分批发还能让 follower 的 nextIndex 更快地稳步前进。
const maxAppendEntries = 64

// 三种 RPC 的请求/响应体。字段名即线格式，改动等同于破坏兼容（仅集群内自用，风险可控）。
type requestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type requestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type appendEntriesArgs struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type appendEntriesReply struct {
	Term uint64 `json:"term"`
	// Success 表示一致性检查通过且条目已落盘。
	Success bool `json:"success"`
	// LastLogIndex 是拒绝时的本地最后索引，供 leader 快速回退 nextIndex
	// （避免"每次只退一格"在长差距下退化成 O(n) 轮往返）。
	LastLogIndex uint64 `json:"last_log_index"`
}

type installSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
	// Members 是快照时刻的成员表：快照压缩会丢掉配置变更条目，
	// 若不随快照下发，接收方会退回配置文件里的初始成员表，算出错误的多数派。
	Members []Member `json:"members,omitempty"`
}

type installSnapshotReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// timeoutNowArgs / timeoutNowReply 是 MethodTimeoutNow 的请求与应答体。
type timeoutNowArgs struct {
	// Term 是发出方（leader）的任期：接收方据此先推进任期，避免用旧任期竞选。
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
}

type timeoutNowReply struct {
	Term uint64 `json:"term"`
	// OK 表示接收方确实发起了选举（它是投票成员且未停止）。
	OK bool `json:"ok"`
}

// applyResult 是一条日志提交并应用后的结果。
type applyResult struct {
	value any
	err   error
}

// Node 是一个自研最小 Raft 节点。
//
// 并发模型（对应 AGENTS.md §6）：
//   - mu 是唯一主锁，保护全部共识状态（角色/任期/投票/日志/复制进度/待提交提案）；
//   - fsmMu 串行化对上层状态机的调用（Apply / Snapshot / Restore），并保证"逐条应用"
//     与"整体安装快照"不会交错；锁序恒为 fsmMu → mu，反向路径不存在；
//   - 网络 RPC 一律在锁外发出（先持锁取快照，解锁后再调用），
//     否则"持锁等对端、对端又来抢同一把锁"会直接死锁；
//   - 后台协程只有两个：runLoop（选举/心跳驱动）与 applyLoop（状态机应用），
//     都监听 done，Stop 之后必然退出。
type Node struct {
	id       string
	members  []Member // 按 ID 排序；含自己（除非自己已被移除）
	selfAddr string
	tr       Transport
	fsm      FSM
	log      Logger
	dir      string

	onMembershipChange func(Membership)

	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	snapshotThreshold uint64

	mu    sync.Mutex
	fsmMu sync.Mutex

	role     Role
	term     uint64
	votedFor string
	leaderID string

	commitIndex uint64
	lastApplied uint64

	storage *raftStorage
	rlog    *raftLog
	// snapData 是最近一次快照的内容缓存，避免发给落后节点时在锁内读文件。
	snapData []byte

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// pendingConfIndex 是本节点作为 leader 时最后一条成员变更条目的索引。
	// 它大于 commitIndex 表示"变更尚未提交"，此时拒绝新的变更（无 joint consensus）。
	pendingConfIndex uint64

	// votes 是本轮竞选中已获得的票（含自己）。
	votes map[string]struct{}
	// lastAck 是 leader 视角下各 follower 最近一次成功应答的时间，用于判定多数派联系。
	lastAck map[string]time.Time
	// lastLeaderContact 是 follower 视角下最近一次听到 leader 的时间。
	lastLeaderContact time.Time

	appliedSinceSnap uint64

	proposals map[uint64]chan applyResult

	done     chan struct{}
	applyCh  chan struct{}
	resetCh  chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once

	started bool
	closed  bool
}

// New 构造一个 Raft 节点。此时不启动任何协程，也不会发起选举（需显式 Start）。
//
// 若目录中已有快照，会先调用一次 FSM.Restore 把状态机恢复到快照位置，
// 这样启动后的重放从 lastApplied+1 继续，不会重复应用已压缩的前缀。
//
// 成员表按"状态文件 → 快照 → Options（首次引导）"的优先级解析，并立即把其中
// 带地址的成员注册进传输层 —— 这样运行期加入的节点在重启后依然能被联系上。
func New(opt Options) (*Node, error) {
	if err := validateOptions(opt); err != nil {
		return nil, err
	}

	electionTimeout := opt.ElectionTimeout
	if electionTimeout <= 0 {
		electionTimeout = DefaultElectionTimeout
	}
	heartbeat := opt.HeartbeatInterval
	if heartbeat <= 0 {
		heartbeat = DefaultHeartbeatInterval
	}
	threshold := opt.SnapshotThreshold
	if threshold == 0 {
		threshold = DefaultSnapshotThreshold
	}
	logger := opt.Logger
	if logger == nil {
		logger = nopLogger{}
	}

	st, err := openStorage(opt.Dir)
	if err != nil {
		return nil, err
	}
	snap, hasSnap, err := st.loadSnapshot()
	if err != nil {
		_ = st.close()
		return nil, err
	}
	var snapIndex, snapTerm uint64
	var snapData []byte
	if hasSnap {
		snapIndex, snapTerm, snapData = snap.LastIncludedIndex, snap.LastIncludedTerm, snap.Data
	}
	rl, err := newRaftLog(st, snapIndex, snapTerm)
	if err != nil {
		_ = st.close()
		return nil, err
	}
	ps, err := st.loadState()
	if err != nil {
		_ = st.close()
		return nil, err
	}

	members := resolveInitialMembers(opt, ps, snap, hasSnap)
	n := &Node{
		id: opt.ID, members: members, selfAddr: memberAddr(members, opt.ID),
		tr: opt.Transport, fsm: opt.FSM, log: logger, dir: opt.Dir,
		onMembershipChange: opt.OnMembershipChange,
		electionTimeout:    electionTimeout, heartbeatInterval: heartbeat, snapshotThreshold: threshold,
		role: RoleFollower, term: ps.Term, votedFor: ps.VotedFor,
		commitIndex: snapIndex, lastApplied: snapIndex,
		storage: st, rlog: rl, snapData: snapData,
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
		votes:      make(map[string]struct{}),
		lastAck:    make(map[string]time.Time),
		proposals:  make(map[uint64]chan applyResult),
		done:       make(chan struct{}),
		applyCh:    make(chan struct{}, 1),
		resetCh:    make(chan struct{}, 1),
	}
	n.registerMemberAddrs(members)
	if hasSnap {
		if err := opt.FSM.Restore(snapData); err != nil {
			_ = st.close()
			return nil, fmt.Errorf("恢复状态机快照失败: %w", err)
		}
	}
	return n, nil
}

// resolveInitialMembers 解析启动时的成员表。
//
// 优先级：状态文件（最近一次已应用的成员表）→ 快照（含成员表）→ Options（首次引导）。
// 前两者存在就说明本节点曾经加入过集群，必须沿用，否则重启会把运行期的成员变更抹掉。
func resolveInitialMembers(opt Options, ps persistedState, snap persistedSnapshot, hasSnap bool) []Member {
	if len(ps.Members) > 0 {
		return normalizeMembers(ps.Members)
	}
	if hasSnap && len(snap.Members) > 0 {
		return normalizeMembers(snap.Members)
	}
	out := make([]Member, 0, len(opt.Peers)+len(opt.Learners))
	for _, id := range opt.Peers {
		out = append(out, Member{ID: id})
	}
	for _, id := range opt.Learners {
		out = append(out, Member{ID: id, Learner: true})
	}
	return normalizeMembers(out)
}

// normalizeMembers 去重、丢弃空 ID 并按 ID 排序（成员表参与多数派计算，顺序必须稳定）。
func normalizeMembers(in []Member) []Member {
	seen := make(map[string]struct{}, len(in))
	out := make([]Member, 0, len(in))
	for _, m := range in {
		if m.ID == "" {
			continue
		}
		if _, dup := seen[m.ID]; dup {
			continue
		}
		seen[m.ID] = struct{}{}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// memberAddr 返回指定成员的地址（不存在或未知名时为空串）。
func memberAddr(members []Member, id string) string {
	for _, m := range members {
		if m.ID == id {
			return m.Addr
		}
	}
	return ""
}

// registerMemberAddrs 把带地址的成员注册进传输层（传输层不支持注册时静默跳过）。
func (n *Node) registerMemberAddrs(members []Member) {
	reg, ok := n.tr.(PeerRegistrar)
	if !ok {
		return
	}
	for _, m := range members {
		if m.Addr != "" {
			reg.RegisterPeer(m.ID, m.Addr)
		}
	}
}

// validateOptions 校验必填项与成员表的一致性。
func validateOptions(opt Options) error {
	if opt.ID == "" {
		return errors.New("raft: Options.ID 不能为空")
	}
	if opt.Dir == "" {
		return errors.New("raft: Options.Dir 不能为空")
	}
	if opt.FSM == nil {
		return errors.New("raft: Options.FSM 不能为空")
	}
	if opt.Transport == nil {
		return errors.New("raft: Options.Transport 不能为空")
	}
	if len(opt.Peers) == 0 && len(opt.Learners) == 0 {
		return errors.New("raft: Options.Peers 与 Options.Learners 不能同时为空")
	}
	seen := make(map[string]struct{}, len(opt.Peers)+len(opt.Learners))
	found := false
	for _, p := range append(append([]string(nil), opt.Peers...), opt.Learners...) {
		if p == "" {
			return errors.New("raft: 成员 ID 不能为空")
		}
		if _, dup := seen[p]; dup {
			return fmt.Errorf("raft: 成员 %q 重复（Peers 与 Learners 不得重叠）", p)
		}
		seen[p] = struct{}{}
		if p == opt.ID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("raft: 成员表必须包含本节点 ID %q（作为投票成员或 learner）", opt.ID)
	}
	if len(opt.Peers) == 0 {
		return errors.New("raft: Options.Peers 不能为空（至少要有一个投票成员，否则无法选出领导者）")
	}
	return nil
}

// 注册三种 Raft RPC 处理器。
//
// 处理器里只做"持锁改状态 + 返回响应"，绝不发 RPC；对上层状态机的回调一律交给 applyLoop。
func (n *Node) serve() error {
	if err := n.tr.Serve(MethodRequestVote, n.handleRequestVote); err != nil {
		return err
	}
	if err := n.tr.Serve(MethodAppendEntries, n.handleAppendEntries); err != nil {
		return err
	}
	if err := n.tr.Serve(MethodInstallSnapshot, n.handleInstallSnapshot); err != nil {
		return err
	}
	if err := n.tr.Serve(MethodTimeoutNow, n.handleTimeoutNow); err != nil {
		return err
	}
	return nil
}

// Start 启动后台协程（选举/心跳/应用）。重复调用无副作用。
func (n *Node) Start() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.started {
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()

	if err := n.serve(); err != nil {
		return err
	}

	n.mu.Lock()
	n.started = true
	n.mu.Unlock()

	n.wg.Add(2)
	go n.runLoop()
	go n.applyLoop()
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
	return nil
}

// Stop 停止节点并释放持久化文件句柄。幂等。
//
// 关停顺序：先置 RoleShutdown 让所有处理器拒绝新请求并唤醒等待中的提案，
// 再关 done 让后台协程退出，最后等待协程结束并关闭文件。
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.role = RoleShutdown
		n.closed = true
		n.failProposalsLocked(ErrStopped)
		n.mu.Unlock()

		close(n.done)
		n.wg.Wait()

		n.mu.Lock()
		if err := n.storage.close(); err != nil {
			n.log.Warn("关闭 raft 持久化文件失败", "err", err)
		}
		n.mu.Unlock()
	})
}

// Propose 追加一条日志并等待其提交与应用。
//
// 语义：非 leader 返回 ErrNotLeader（本实现不做转发，转发属上层元数据层的职责）；
// 等待期间若失去多数派返回 ErrNoQuorum，若本节点不再是 leader 返回 ErrNotLeader，
// 若节点被停止返回 ErrStopped，若 ctx 超时返回 ctx 的错误。
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	if n.closed || n.role == RoleShutdown {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != RoleLeader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	if !n.hasQuorumLocked() {
		n.mu.Unlock()
		return nil, ErrNoQuorum
	}
	index, err := n.appendLocked(data)
	if err != nil {
		n.mu.Unlock()
		return nil, fmt.Errorf("追加日志失败: %w", err)
	}
	ch := make(chan applyResult, 1)
	n.proposals[index] = ch
	n.maybeAdvanceCommitLocked()
	n.mu.Unlock()

	select {
	case res := <-ch:
		return res.value, res.err
	case <-ctx.Done():
		n.dropProposal(index)
		return nil, ctx.Err()
	case <-n.done:
		n.dropProposal(index)
		return nil, ErrStopped
	}
}

// IsLeader 判断本节点当前是否为领导者。
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// Leader 返回已知领导者 ID（未知为空串）。
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Status 返回只读状态快照。
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	var progress map[string]uint64
	if n.role == RoleLeader {
		progress = make(map[string]uint64, len(n.matchIndex))
		for id, idx := range n.matchIndex {
			progress[id] = idx
		}
	}
	return Status{
		ID:            n.id,
		Role:          n.role,
		Term:          n.term,
		Leader:        n.leaderID,
		CommitIndex:   n.commitIndex,
		LastLogIndex:  n.rlog.lastIndex(),
		LastApplied:   n.lastApplied,
		SnapshotIndex: n.rlog.snapIndex(),
		Peers:         n.voterIDsLocked(),
		Learners:      n.learnerIDsLocked(),
		Progress:      progress,
	}
}

// Membership 返回当前成员划分。
func (n *Node) Membership() Membership {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.membershipLocked()
}

func (n *Node) membershipLocked() Membership {
	return Membership{Voters: n.voterIDsLocked(), Learners: n.learnerIDsLocked()}
}

// voterIDsLocked 返回排序后的投票成员 ID（要求持有主锁）。
func (n *Node) voterIDsLocked() []string {
	out := make([]string, 0, len(n.members))
	for _, m := range n.members {
		if !m.Learner {
			out = append(out, m.ID)
		}
	}
	return out
}

// learnerIDsLocked 返回排序后的非投票成员 ID（要求持有主锁）。
func (n *Node) learnerIDsLocked() []string {
	out := make([]string, 0, len(n.members))
	for _, m := range n.members {
		if m.Learner {
			out = append(out, m.ID)
		}
	}
	return out
}

// memberIDsLocked 返回全部成员 ID（投票 + learner，要求持有主锁）。
func (n *Node) memberIDsLocked() []string {
	out := make([]string, 0, len(n.members))
	for _, m := range n.members {
		out = append(out, m.ID)
	}
	return out
}

// memberLocked 查找成员（要求持有主锁）。
func (n *Node) memberLocked(id string) (Member, bool) {
	for _, m := range n.members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// isVoterLocked 判断 id 是否为投票成员（要求持有主锁）。
func (n *Node) isVoterLocked(id string) bool {
	m, ok := n.memberLocked(id)
	return ok && !m.Learner
}

// selfIsVoterLocked 判断本节点当前是否为投票成员（要求持有主锁）。
func (n *Node) selfIsVoterLocked() bool { return n.isVoterLocked(n.id) }

// HasQuorum 判断本节点是否仍与多数派保持联系。
//
// 判定口径（对外是同一个方法，内部按角色分两种依据）：
//   - leader：最近一个"应答窗口"内成功应答过的成员数（含自己）是否达到多数派；
//   - follower：是否在选举超时内听到过 leader。
//
// 这是启发式判断：被网络分区到少数派的一侧会在一个窗口/选举超时后转为 false。
func (n *Node) HasQuorum() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.hasQuorumLocked()
}

// voterCountLocked 是投票成员数（要求持有主锁；不分配切片，供热路径使用）。
func (n *Node) voterCountLocked() int {
	c := 0
	for _, m := range n.members {
		if !m.Learner {
			c++
		}
	}
	return c
}

// quorum 是多数派所需票数。**只统计投票成员**：learner 不参与表决。
func (n *Node) quorum() int { return n.voterCountLocked()/2 + 1 }

// leaderAckWindow 是 leader 判定"仍与多数派联系"的时间窗。
//
// 取 4 倍心跳间隔（不超过选举超时）：太短会把偶发丢包误判为分区，
// 太长则失去响应变慢 —— 4 个心跳周期既容忍个别丢包，又能在数百毫秒内发现分区。
func (n *Node) leaderAckWindow() time.Duration {
	w := 4 * n.heartbeatInterval
	if w > n.electionTimeout {
		w = n.electionTimeout
	}
	return w
}

// rpcTimeout 是单次 RPC 的超时：取一个选举超时，保证不会因对端卡死而无限等待。
func (n *Node) rpcTimeout() time.Duration { return n.electionTimeout }

func (n *Node) hasQuorumLocked() bool {
	switch n.role {
	case RoleLeader:
		count := 1 // 自己
		now := time.Now()
		window := n.leaderAckWindow()
		for _, m := range n.members {
			if m.Learner || m.ID == n.id {
				continue
			}
			if t, ok := n.lastAck[m.ID]; ok && now.Sub(t) < window {
				count++
			}
		}
		return count >= n.quorum()
	default:
		// follower / candidate，以及"自己是 learner 或已被移除"的情形：
		// 依据是否在选举超时内听到 leader —— learner 与已移除节点不参与表决，
		// 但只要还在跟着某个 leader，就不该被 pause_minority 判成"失去多数派"。
		if n.lastLeaderContact.IsZero() {
			return false
		}
		return time.Since(n.lastLeaderContact) < n.electionTimeout
	}
}

// randomElectionTimeout 返回 [ElectionTimeout, 2*ElectionTimeout) 内的随机时长。
//
// 随机化的目的是打破"同时超时→同时竞选→都没拿到多数派"的活锁。
func (n *Node) randomElectionTimeout() time.Duration {
	return n.electionTimeout + time.Duration(rand.Int63n(int64(n.electionTimeout)))
}

// resetElectionLocked 唤醒 runLoop 重新计时（收到 leader 心跳或投出一票时调用）。
func (n *Node) resetElectionLocked() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

// runLoop 是选举与心跳的驱动：非 leader 按随机选举超时发起竞选，
// leader 按心跳间隔复制日志并评估多数派联系。
func (n *Node) runLoop() {
	defer n.wg.Done()
	wasLeader := false
	for {
		n.mu.Lock()
		role := n.role
		selfVoter := n.selfIsVoterLocked()
		single := selfVoter && n.voterCountLocked() == 1
		n.mu.Unlock()
		if role == RoleShutdown {
			return
		}

		// 单节点集群无需等待选举超时：自己就是全部成员，立即当选，
		// 否则每次重启都要白等一个（随机化的）选举周期，上层观测会看到明显抖动。
		if single && role != RoleLeader {
			n.startElection()
			continue
		}

		// 刚当选就立刻广播一轮心跳，把"其他节点还没听说新 leader"的窗口压到最小。
		// 若等到一个心跳间隔后才首次广播，某些选举超时更早的 peer 会先发起新一轮竞选，
		// 造成没必要的换届（对外表现为 Propose 偶发 ErrNotLeader）。
		if role == RoleLeader && !wasLeader {
			wasLeader = true
			n.tickLeader()
			continue
		}
		wasLeader = role == RoleLeader

		wait := n.heartbeatInterval
		if role != RoleLeader {
			wait = n.randomElectionTimeout()
		}
		timer := time.NewTimer(wait)
		select {
		case <-n.done:
			timer.Stop()
			return
		case <-n.resetCh:
			timer.Stop()
			continue
		case <-timer.C:
		}

		n.mu.Lock()
		role = n.role
		selfVoter = n.selfIsVoterLocked()
		n.mu.Unlock()
		if role == RoleShutdown {
			return
		}
		switch {
		case role == RoleLeader:
			n.tickLeader()
		case selfVoter:
			n.startElection()
		default:
			// learner / 已被移除：绝不发起竞选，只等 leader 的心跳把自己带上去。
		}
	}
}

// startElection 发起一轮竞选：任期 +1、投自己一票（先落盘）后并行拉票。
func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == RoleLeader || n.role == RoleShutdown || !n.selfIsVoterLocked() {
		n.mu.Unlock()
		return
	}
	n.role = RoleCandidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	// 选举安全性：投票（与任期一起）必须先 fsync 再对外拉票，
	// 否则崩溃重启后可能在同任期投出第二票。
	if err := n.storage.saveState(n.term, n.votedFor, n.members); err != nil {
		n.log.Error("持久化任期与投票失败", "term", n.term, "err", err)
	}
	term := n.term
	lastIndex, lastTerm := n.rlog.lastIndex(), n.rlog.lastTerm()
	n.votes = map[string]struct{}{n.id: {}}
	if len(n.votes) >= n.quorum() {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		return
	}
	others := make([]string, 0, len(n.members)-1)
	for _, m := range n.members {
		// 只向**投票成员**拉票：learner 的表决不计入多数派。
		if m.ID != n.id && !m.Learner {
			others = append(others, m.ID)
		}
	}
	n.mu.Unlock()

	n.log.Info("发起选举", "id", n.id, "term", term)
	args := requestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	for _, p := range others {
		p := p
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			n.sendRequestVote(p, args)
		}()
	}
}

// becomeLeaderLocked 完成角色切换：初始化复制进度、追加"无操作"条目。
//
// 为什么要追加无操作条目：Raft 只允许提交"当前任期"的条目（Figure 8 安全性），
// 因此新 leader 无法直接判定旧任期条目是否已提交。追加一条本任期的空条目后，
// 它一旦被多数派复制即可提交，进而"隐含提交"它之前的全部条目 ——
// 这也是重启后 commitIndex 能自动恢复、状态机不必等到下一次客户端写入的唯一途径。
// 副作用：状态机会收到一条 Data 为空的条目，上层必须容忍（见 types.go 的 FSM 约定）。
func (n *Node) becomeLeaderLocked() {
	if n.role == RoleLeader {
		return
	}
	if !n.selfIsVoterLocked() {
		// learner 不可能当选；此外"自己已被移除"的节点即使拿到旧票也不得执政。
		n.log.Warn("非投票成员试图成为领导者，已拒绝", "id", n.id, "term", n.term)
		n.role = RoleFollower
		return
	}
	now := time.Now()
	n.role = RoleLeader
	n.leaderID = n.id
	last := n.rlog.lastIndex()
	// 新任期从"没有未提交变更"开始：上一个任期里未提交的配置变更已被覆盖，
	// 若还留着 pendingConfIndex 就会永久拒绝新的成员变更。
	n.pendingConfIndex = 0
	n.nextIndex = make(map[string]uint64, len(n.members))
	n.matchIndex = make(map[string]uint64, len(n.members))
	n.lastAck = make(map[string]time.Time, len(n.members))
	for _, m := range n.members {
		if m.ID == n.id {
			continue
		}
		n.nextIndex[m.ID] = last + 1
		n.matchIndex[m.ID] = 0
		// 乐观初始应答时间：首轮心跳返回前不至于误判"无多数派"，窗口过后若无应答即判失去联系。
		n.lastAck[m.ID] = now
	}
	n.matchIndex[n.id] = last
	n.log.Info("当选领导者", "id", n.id, "term", n.term, "last_log_index", last)

	if _, err := n.appendLocked(nil); err != nil {
		n.log.Error("追加无操作条目失败", "err", err)
	} else {
		n.maybeAdvanceCommitLocked()
	}
	// 立即重置 runLoop 的计时：否则它还在按"候选人选举超时"等待，
	// 期间 follower 可能先超时发起新一轮竞选，造成无谓的选举抖动。
	n.resetElectionLocked()
}

// appendLocked 追加一条日志并落盘（fsync），返回新条目索引。
func (n *Node) appendLocked(data []byte) (uint64, error) {
	index := n.rlog.lastIndex() + 1
	e := Entry{Index: index, Term: n.term, Data: data}
	if err := n.rlog.append(e); err != nil {
		return 0, err
	}
	// 日志落盘后才允许对外宣称"已复制"：这是 AppendEntries 成功语义的前提。
	if err := n.rlog.sync(); err != nil {
		return 0, err
	}
	if n.role == RoleLeader {
		n.matchIndex[n.id] = index
	}
	return index, nil
}

// tickLeader 广播一轮 AppendEntries / InstallSnapshot，并评估多数派联系。
func (n *Node) tickLeader() {
	type task struct {
		peer string
		args appendEntriesArgs
		snap *installSnapshotArgs
	}
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	term := n.term
	commit := n.commitIndex
	// 复制对象是**全部成员**（含 learner）：learner 不投票，但必须跟上日志，
	// 否则"追平后再提升为 voter"就无从谈起。
	members := append([]Member(nil), n.members...)
	tasks := make([]task, 0, len(members))
	for _, m := range members {
		if m.ID == n.id {
			continue
		}
		next := n.nextIndex[m.ID]
		if next == 0 {
			next = 1
		}
		// 需要的前一条已被压缩掉：只能整体安装快照（增量追赶不再可能）。
		if next <= n.rlog.snapIndex() {
			if snap, ok := n.snapshotToSendLocked(); ok {
				tasks = append(tasks, task{peer: m.ID, snap: &snap})
				continue
			}
		}
		args := appendEntriesArgs{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: next - 1,
			LeaderCommit: commit,
		}
		if t, ok := n.rlog.termAt(args.PrevLogIndex); ok {
			args.PrevLogTerm = t
		} else if snap, ok := n.snapshotToSendLocked(); ok {
			tasks = append(tasks, task{peer: m.ID, snap: &snap})
			continue
		}
		args.Entries = n.rlog.entriesFrom(next, maxAppendEntries)
		tasks = append(tasks, task{peer: m.ID, args: args})
	}
	n.mu.Unlock()

	for _, tk := range tasks {
		tk := tk
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			if tk.snap != nil {
				n.sendInstallSnapshot(tk.peer, *tk.snap)
				return
			}
			n.sendAppendEntries(tk.peer, tk.args)
		}()
	}

	// 失去多数派时立刻让等待中的 Propose 失败，而不是让调用方干等到 ctx 超时。
	n.mu.Lock()
	if n.role == RoleLeader && !n.hasQuorumLocked() {
		n.failProposalsLocked(ErrNoQuorum)
	}
	n.mu.Unlock()
}

// snapshotToSendLocked 返回可发送的快照内容（无快照或未缓存时 ok=false）。
func (n *Node) snapshotToSendLocked() (installSnapshotArgs, bool) {
	if n.rlog.snapIndex() == 0 || n.snapData == nil {
		return installSnapshotArgs{}, false
	}
	return installSnapshotArgs{
		Term:              n.term,
		LeaderID:          n.id,
		LastIncludedIndex: n.rlog.snapIndex(),
		LastIncludedTerm:  n.rlog.snapTerm(),
		Data:              n.snapData,
		Members:           append([]Member(nil), n.members...),
	}, true
}

// sendAppendEntries 向单个 follower 发送一次日志复制（锁外调用）。
func (n *Node) sendAppendEntries(peer string, args appendEntriesArgs) {
	payload, err := json.Marshal(args)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	defer cancel()
	resp, err := n.tr.Call(ctx, peer, MethodAppendEntries, payload)
	if err != nil {
		n.log.Debug("AppendEntries 调用失败", "peer", peer, "err", err)
		return
	}
	var reply appendEntriesReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		n.log.Warn("解析 AppendEntries 响应失败", "peer", peer, "err", err)
		return
	}
	n.handleAppendReply(peer, args, reply)
}

// sendInstallSnapshot 向落后过多的 follower 整体发送快照（锁外调用）。
func (n *Node) sendInstallSnapshot(peer string, args installSnapshotArgs) {
	payload, err := json.Marshal(args)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	defer cancel()
	resp, err := n.tr.Call(ctx, peer, MethodInstallSnapshot, payload)
	if err != nil {
		n.log.Debug("InstallSnapshot 调用失败", "peer", peer, "err", err)
		return
	}
	var reply installSnapshotReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		n.log.Warn("解析 InstallSnapshot 响应失败", "peer", peer, "err", err)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader || n.term != args.Term {
		return
	}
	if reply.Term > n.term {
		n.stepDownLocked(reply.Term, "")
		return
	}
	if !reply.Success {
		return
	}
	n.lastAck[peer] = time.Now()
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1
	n.maybeAdvanceCommitLocked()
}

// handleAppendReply 处理 AppendEntries 应答：推进 progress 或回退重试。
func (n *Node) handleAppendReply(peer string, args appendEntriesArgs, reply appendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader || n.term != args.Term {
		return // 陈旧应答（已换届或已不是 leader）
	}
	if reply.Term > n.term {
		n.stepDownLocked(reply.Term, "")
		return
	}
	// 无论成功与否，RPC 往返本身证明链路可用 —— 这正是多数派联系的依据。
	n.lastAck[peer] = time.Now()
	if reply.Success {
		if len(args.Entries) > 0 {
			if last := args.Entries[len(args.Entries)-1].Index; last > n.matchIndex[peer] {
				n.matchIndex[peer] = last
			}
		} else if args.PrevLogIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = args.PrevLogIndex
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeAdvanceCommitLocked()
		return
	}
	// 失败：回退 nextIndex。只允许后退，避免应答乱序时把进度推过头。
	next := reply.LastLogIndex + 1
	if next < 1 {
		next = 1
	}
	if next > n.nextIndex[peer] {
		next = n.nextIndex[peer]
	}
	if args.PrevLogIndex > 0 && next > args.PrevLogIndex {
		next = args.PrevLogIndex
	}
	n.nextIndex[peer] = next
}

// maybeAdvanceCommitLocked 依据"多数派已复制"推进 commitIndex。
//
// 关键安全规则（Figure 8）：只有当索引 N 处的条目属于**当前任期**时才允许提交它。
// 否则一个旧任期的条目可能"看起来被多数派复制"，却在随后被新 leader 覆盖。
// 提交 N 隐含提交 N-1 及之前（由 applyLoop 顺序应用保证）。
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != RoleLeader {
		return
	}
	candidates := make([]uint64, 0, n.voterCountLocked())
	candidates = append(candidates, n.rlog.lastIndex())
	for _, m := range n.members {
		if m.Learner || m.ID == n.id {
			// learner 的复制进度不参与提交判定（它不是多数派的一部分）。
			continue
		}
		candidates = append(candidates, n.matchIndex[m.ID])
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] > candidates[j] })
	q := n.quorum()
	if q > len(candidates) {
		return
	}
	replicated := candidates[q-1]
	if replicated <= n.commitIndex {
		return
	}
	term, ok := n.rlog.termAt(replicated)
	if !ok || term != n.term {
		return
	}
	n.commitIndex = replicated
	n.signalApplyLocked()
}

// signalApplyLocked 唤醒 applyLoop（非阻塞；丢失信号无妨，applyLoop 会一直应用到底）。
func (n *Node) signalApplyLocked() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

// failProposalsLocked 让所有等待中的提案立即失败。
//
// 注意：已经追加到日志的条目仍可能在后续被提交并应用（若多数派恢复），
// 但调用方已经拿到错误、不会再等待，这是"宁可让调用方重试，也不让它挂死"的取舍。
func (n *Node) failProposalsLocked(err error) {
	if len(n.proposals) == 0 {
		return
	}
	pending := n.proposals
	n.proposals = make(map[uint64]chan applyResult)
	for _, ch := range pending {
		ch <- applyResult{err: err}
	}
}

// failProposalsExceptLocked 让除 keep 之外的全部等待中提案失败。
//
// 用于"应用一条成员变更导致本节点卸任"的场景：那条成员变更本身必须成功返回
// （它已经提交并应用了），其余提案则应立即失败而不是干等超时。
func (n *Node) failProposalsExceptLocked(keep uint64, err error) {
	if len(n.proposals) == 0 {
		return
	}
	for index, ch := range n.proposals {
		if index == keep {
			continue
		}
		delete(n.proposals, index)
		ch <- applyResult{err: err}
	}
}

// dropProposal 在调用方放弃等待（ctx 超时 / 节点停止）时移除提案。
func (n *Node) dropProposal(index uint64) {
	n.mu.Lock()
	delete(n.proposals, index)
	n.mu.Unlock()
}

// deliverResult 把状态机的应用结果交给提案方（若仍在等待）。
func (n *Node) deliverResult(index uint64, value any, err error) {
	n.mu.Lock()
	ch := n.proposals[index]
	delete(n.proposals, index)
	n.mu.Unlock()
	if ch != nil {
		ch <- applyResult{value: value, err: err}
	}
}

// stepDownLocked 因观察到更高任期而退回 follower。
func (n *Node) stepDownLocked(term uint64, leaderID string) {
	if n.term == term {
		if n.role != RoleFollower {
			n.role = RoleFollower
			n.failProposalsLocked(ErrNotLeader)
		}
		n.leaderID = leaderID
		return
	}
	n.term = term
	n.votedFor = ""
	n.role = RoleFollower
	n.leaderID = leaderID
	// 即使落盘失败也不回滚内存中的任期：内存里保持更高任期最多让本节点拒绝旧 leader，
	// 而用旧任期对外应答则可能导致同任期重复投票（安全性破坏），两害相权取其轻。
	if err := n.storage.saveState(n.term, n.votedFor, n.members); err != nil {
		n.log.Error("持久化任期失败", "term", term, "err", err)
	}
	n.failProposalsLocked(ErrNotLeader)
}

// candidateLogUpToDateLocked 判断候选人日志是否"至少与本地一样新"（先比任期，再比索引）。
func (n *Node) candidateLogUpToDateLocked(lastIndex, lastTerm uint64) bool {
	myTerm := n.rlog.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= n.rlog.lastIndex()
}

// handleRequestVote 处理拉票请求。
func (n *Node) handleRequestVote(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var args requestVoteArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, fmt.Errorf("解析 RequestVote 请求失败: %w", err)
	}
	n.mu.Lock()
	if n.role == RoleShutdown {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(requestVoteReply{Term: term})
	}
	if args.Term > n.term {
		n.stepDownLocked(args.Term, "")
	}
	reply := requestVoteReply{Term: n.term}
	// 只给**已知的投票成员**投票：否则一个"自认为已被加入、但本节点还没应用该配置变更"
	// 的节点，就可能凑出一个与旧多数派不相交的多数派，同时选出两个 leader。
	grantable := args.Term == n.term &&
		n.isVoterLocked(args.CandidateID) &&
		(n.votedFor == "" || n.votedFor == args.CandidateID) &&
		n.candidateLogUpToDateLocked(args.LastLogIndex, args.LastLogTerm)
	if grantable {
		if n.votedFor != args.CandidateID {
			n.votedFor = args.CandidateID
			// 投票先落盘再应答：否则崩溃重启后可能在同一任期投出第二票。
			if err := n.storage.saveState(n.term, n.votedFor, n.members); err != nil {
				n.mu.Unlock()
				return nil, fmt.Errorf("持久化投票失败: %w", err)
			}
		}
		reply.VoteGranted = true
		n.resetElectionLocked()
	}
	n.mu.Unlock()
	n.log.Debug("处理拉票", "candidate", args.CandidateID, "term", args.Term, "granted", reply.VoteGranted)
	return json.Marshal(reply)
}

// sendRequestVote 向单个 peer 拉票（锁外调用）。
func (n *Node) sendRequestVote(peer string, args requestVoteArgs) {
	payload, err := json.Marshal(args)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	defer cancel()
	resp, err := n.tr.Call(ctx, peer, MethodRequestVote, payload)
	if err != nil {
		n.log.Debug("RequestVote 调用失败", "peer", peer, "err", err)
		return
	}
	var reply requestVoteReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		n.log.Warn("解析 RequestVote 响应失败", "peer", peer, "err", err)
		return
	}
	n.handleRequestVoteReply(peer, args.Term, reply)
}

// handleRequestVoteReply 统计选票。
func (n *Node) handleRequestVoteReply(peer string, term uint64, reply requestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleCandidate || n.term != term {
		return
	}
	if reply.Term > n.term {
		n.stepDownLocked(reply.Term, "")
		return
	}
	if !reply.VoteGranted {
		return
	}
	// 只统计投票成员的票：learner 的票不算数（否则多数派会被算小）。
	if !n.isVoterLocked(peer) {
		return
	}
	n.votes[peer] = struct{}{}
	if len(n.votes) >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

// handleAppendEntries 处理日志复制请求。
func (n *Node) handleAppendEntries(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var args appendEntriesArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, fmt.Errorf("解析 AppendEntries 请求失败: %w", err)
	}
	n.mu.Lock()
	if n.role == RoleShutdown {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(appendEntriesReply{Term: term})
	}
	if args.Term < n.term {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(appendEntriesReply{Term: term})
	}
	if args.Term > n.term {
		n.stepDownLocked(args.Term, args.LeaderID)
	}
	n.role = RoleFollower
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionLocked()

	reply := appendEntriesReply{Term: n.term}
	if args.PrevLogIndex > n.rlog.lastIndex() {
		reply.LastLogIndex = n.rlog.lastIndex()
		n.mu.Unlock()
		return json.Marshal(reply)
	}
	if args.PrevLogIndex >= n.rlog.snapIndex() {
		prevTerm, ok := n.rlog.termAt(args.PrevLogIndex)
		if !ok || prevTerm != args.PrevLogTerm {
			// 一致性检查失败：冲突处（含）之后全部截断，等 leader 重发。
			if err := n.rlog.truncateFrom(args.PrevLogIndex); err != nil {
				n.mu.Unlock()
				return nil, fmt.Errorf("截断冲突日志失败: %w", err)
			}
			reply.LastLogIndex = n.rlog.lastIndex()
			n.mu.Unlock()
			return json.Marshal(reply)
		}
	}
	// PrevLogIndex 落在快照覆盖范围内：前缀已固化，视为匹配，只需追加其后的条目。

	if len(args.Entries) > 0 {
		if err := n.appendEntriesLocked(args.Entries); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("追加日志失败: %w", err)
		}
		// 答复成功之前必须 fsync：否则 leader 会以为条目已被持久化。
		if err := n.rlog.sync(); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("日志落盘失败: %w", err)
		}
	}
	if args.LeaderCommit > n.commitIndex {
		commit := args.LeaderCommit
		if last := n.rlog.lastIndex(); commit > last {
			commit = last
		}
		n.commitIndex = commit
		n.signalApplyLocked()
	}
	reply.Success = true
	n.mu.Unlock()
	return json.Marshal(reply)
}

// appendEntriesLocked 追加 leader 发来的条目，遇到与本地不一致的位置先截断。
func (n *Node) appendEntriesLocked(entries []Entry) error {
	for _, e := range entries {
		if e.Index <= n.rlog.snapIndex() {
			continue // 已被快照覆盖，无需重复写入
		}
		if e.Index <= n.rlog.lastIndex() {
			if t, ok := n.rlog.termAt(e.Index); ok && t == e.Term {
				continue // 本地已有相同条目
			}
			if err := n.rlog.truncateFrom(e.Index); err != nil {
				return err
			}
		}
		if err := n.rlog.append(e); err != nil {
			return err
		}
	}
	return nil
}

// handleInstallSnapshot 处理整体快照安装。
//
// FSM.Restore 与快照文件写入都在 fsmMu 保护下、且不持有主锁进行：
// fsmMu 保证它不会与 applyLoop 的逐条应用交错（AGENTS.md §6：不在锁内调用外部回调）。
func (n *Node) handleInstallSnapshot(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var args installSnapshotArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, fmt.Errorf("解析 InstallSnapshot 请求失败: %w", err)
	}
	n.mu.Lock()
	if n.role == RoleShutdown {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(installSnapshotReply{Term: term})
	}
	if args.Term < n.term {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(installSnapshotReply{Term: term})
	}
	if args.Term > n.term {
		n.stepDownLocked(args.Term, args.LeaderID)
	}
	n.role = RoleFollower
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionLocked()
	behind := args.LastIncludedIndex > n.rlog.snapIndex()
	n.mu.Unlock()

	if !behind {
		return json.Marshal(installSnapshotReply{Term: n.term, Success: true})
	}

	n.fsmMu.Lock()
	defer n.fsmMu.Unlock()

	// 等待 fsmMu 期间可能已被更新的快照覆盖，二次校验避免把状态机倒退回旧快照。
	n.mu.Lock()
	if args.LastIncludedIndex <= n.rlog.snapIndex() {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(installSnapshotReply{Term: term, Success: true})
	}
	n.mu.Unlock()

	snap := persistedSnapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
		Members:           args.Members,
	}
	if err := n.storage.saveSnapshot(snap); err != nil {
		return nil, fmt.Errorf("写入快照文件失败: %w", err)
	}
	if err := n.fsm.Restore(args.Data); err != nil {
		return nil, fmt.Errorf("恢复状态机快照失败: %w", err)
	}

	n.mu.Lock()
	if args.LastIncludedIndex <= n.rlog.snapIndex() {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(installSnapshotReply{Term: term, Success: true})
	}
	// 采纳快照里的成员表：这是"落后节点/新节点"唯一能拿到运行期成员变更的途径。
	membersChanged := n.adoptMembersLocked(args.Members)
	// 本地在快照位置处有相同任期的条目时保留其后缀，否则整体丢弃重建。
	var err error
	if t, ok := n.rlog.termAt(args.LastIncludedIndex); ok && t == args.LastIncludedTerm {
		err = n.rlog.compact(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		err = n.rlog.reset(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	if err != nil {
		n.mu.Unlock()
		return nil, fmt.Errorf("安装快照后整理日志失败: %w", err)
	}
	if args.LastIncludedIndex > n.commitIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > n.lastApplied {
		n.lastApplied = args.LastIncludedIndex
	}
	n.snapData = args.Data
	n.appliedSinceSnap = 0
	term := n.term
	members := n.membershipLocked()
	n.mu.Unlock()

	if membersChanged {
		n.notifyMembership(members)
	}
	n.log.Info("已安装快照", "index", args.LastIncludedIndex, "term", args.LastIncludedTerm)
	return json.Marshal(installSnapshotReply{Term: term, Success: true})
}

// adoptMembersLocked 用快照带来的成员表替换本地成员表，返回是否发生了变化。
//
// 快照里的成员表是"**该位置的事实**"：即便本地成员表更新（例如本地已应用了
// 快照点之后的变更），也不应回退 —— 因此只有本地日志中没有比快照更新的配置变更时才采纳。
// 实务上"本地成员表 != 快照成员表"只发生在本地落后于快照的情形，此时采纳是正确的。
func (n *Node) adoptMembersLocked(in []Member) bool {
	if len(in) == 0 {
		return false
	}
	members := normalizeMembers(in)
	if membersEqual(n.members, members) {
		return false
	}
	n.members = members
	n.selfAddr = memberAddr(members, n.id)
	n.registerMemberAddrs(members)
	if !n.selfIsVoterLocked() && n.role == RoleLeader {
		// 新成员表里没有自己：立即让位，并唤醒 runLoop 重新评估。
		n.stepDownLocked(n.term, n.leaderID)
	}
	if err := n.storage.saveState(n.term, n.votedFor, n.members); err != nil {
		n.log.Warn("持久化成员表失败", "err", err)
	}
	return true
}

// membersEqual 判断两份成员表是否一致（均假定已排序）。
func membersEqual(a, b []Member) bool {
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

// notifyMembership 触发成员变更回调（必须在锁外调用）。
func (n *Node) notifyMembership(m Membership) {
	if n.onMembershipChange == nil {
		return
	}
	defer func() {
		// 回调是上层代码，panic 不应该把 Raft 的协程带走。
		if r := recover(); r != nil {
			n.log.Error("成员变更回调 panic", "err", r)
		}
	}()
	n.onMembershipChange(m)
}

// applyLoop 是唯一调用 FSM.Apply / FSM.Snapshot 的协程。
//
// 顺序、不跳号、不重复三条不变式都落在这里：游标 lastApplied 只在此处递增。
func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case <-n.applyCh:
		}
		n.applyCommitted()
	}
}

// applyCommitted 把 commitIndex 之内的条目按索引顺序应用到状态机。
func (n *Node) applyCommitted() {
	for {
		// 锁序固定为 fsmMu → mu：保证"逐条应用"与"安装快照"互斥。
		n.fsmMu.Lock()
		n.mu.Lock()
		if n.role == RoleShutdown || n.lastApplied >= n.commitIndex {
			n.mu.Unlock()
			n.fsmMu.Unlock()
			return
		}
		index := n.lastApplied + 1
		entry, ok := n.rlog.entryAt(index)
		if !ok {
			n.mu.Unlock()
			n.fsmMu.Unlock()
			// 已提交却缺条目属实现缺陷，宁可停下并报错，也不要跳号继续（会造成静默不一致）。
			n.log.Error("已提交索引缺少日志条目", "index", index, "commit", n.commitIndex)
			return
		}
		// 刻意**不**在 Apply 之前推进 lastApplied：Apply 失败时把它留在原地（后续提交信号会重试），
		// 否则该条目会被当成"已应用"而永不重放 —— 状态机已变、内核未变，两边静默分叉。
		needSnapshot := n.appliedSinceSnap+1 >= n.snapshotThreshold
		n.mu.Unlock()

		if entry.Type == EntryConfChange {
			// 成员变更条目由 Raft 自己消化，不进上层状态机。
			values, err := n.applyConfChangeEntry(index, entry.Data)
			if err != nil {
				n.fsmMu.Unlock()
				n.deliverResult(index, nil, err)
				n.log.Error("应用成员变更条目失败，已停止推进（等待重试）", "index", index, "err", err)
				return
			}
			n.markApplied(index)
			if needSnapshot {
				n.takeSnapshot(index, entry.Term)
			}
			n.fsmMu.Unlock()
			n.deliverResult(index, nil, nil)
			if values.changed {
				n.notifyMembership(values.membership)
			}
			continue
		}

		value, err := n.fsm.Apply(index, entry.Data)
		if err != nil {
			n.fsmMu.Unlock()
			n.deliverResult(index, value, err)
			n.log.Error("应用状态机条目失败，已停止推进（等待重试）", "index", index, "err", err)
			return
		}
		n.markApplied(index)
		if needSnapshot {
			n.takeSnapshot(index, entry.Term)
		}
		n.fsmMu.Unlock()
		n.deliverResult(index, value, nil)
	}
}

// markApplied 推进应用游标 lastApplied 与应用计数。
//
// 只在状态机**成功应用**之后调用：它是"这条已应用"的唯一凭据。
func (n *Node) markApplied(index uint64) {
	n.mu.Lock()
	n.lastApplied = index
	n.appliedSinceSnap++
	n.mu.Unlock()
}

// confApplyOutcome 是一次成员变更的应用结果。
type confApplyOutcome struct {
	changed    bool
	membership Membership
}

// applyConfChangeEntry 应用一条已提交的成员变更条目。
//
// 与提案路径的区别：这里对"重复/无意义"的操作是**幂等宽容**的（重放会再次走到这里），
// 只有解析失败才算错误。严格的合法性校验放在 ChangeMembership 的提案路径上。
func (n *Node) applyConfChangeEntry(index uint64, data []byte) (confApplyOutcome, error) {
	var cc ConfChange
	if err := json.Unmarshal(data, &cc); err != nil {
		return confApplyOutcome{}, fmt.Errorf("解析成员变更条目失败: %w", err)
	}
	n.mu.Lock()
	changed := n.applyConfChangeLocked(cc)
	// 本次变更导致本节点不再是领导者（例如移除的是自己）：其余在途提案必然无法完成，
	// 立即让它们失败，而不是让调用方干等 ctx 超时。**本次**这条变更要正常返回。
	if n.role != RoleLeader {
		n.failProposalsExceptLocked(index, ErrNotLeader)
	}
	if index >= n.pendingConfIndex {
		n.pendingConfIndex = 0
	}
	out := confApplyOutcome{changed: changed, membership: n.membershipLocked()}
	n.mu.Unlock()

	if changed {
		n.log.Info("成员变更已生效", "op", cc.Op, "id", cc.ID,
			"voters", out.membership.Voters, "learners", out.membership.Learners)
	}
	return out, nil
}

// applyConfChangeLocked 把一条成员变更作用到本地成员表，返回是否发生了变化。
func (n *Node) applyConfChangeLocked(cc ConfChange) bool {
	switch cc.Op {
	case ConfAddLearner:
		m, ok := n.memberLocked(cc.ID)
		if ok {
			// 幂等：已经是成员。地址有更新时同步一次（例如新节点换了监听地址）。
			if cc.Addr != "" && m.Addr != cc.Addr {
				for i := range n.members {
					if n.members[i].ID == cc.ID {
						n.members[i].Addr = cc.Addr
					}
				}
				n.registerMembersLocked()
				n.persistMembersLocked()
				return true
			}
			return false
		}
		n.members = normalizeMembers(append(n.members, Member{ID: cc.ID, Addr: cc.Addr, Learner: true}))
	case ConfPromote:
		found := false
		for i := range n.members {
			if n.members[i].ID == cc.ID {
				found = true
				if !n.members[i].Learner {
					return false // 幂等：已经是投票成员
				}
				n.members[i].Learner = false
			}
		}
		if !found {
			return false
		}
	case ConfRemove:
		next := make([]Member, 0, len(n.members))
		removed := false
		for _, m := range n.members {
			if m.ID == cc.ID {
				removed = true
				continue
			}
			next = append(next, m)
		}
		if !removed {
			return false
		}
		n.members = next
		if reg, ok := n.tr.(PeerRegistrar); ok {
			reg.UnregisterPeer(cc.ID)
		}
	default:
		n.log.Warn("未知的成员变更操作，已忽略", "op", string(cc.Op), "id", cc.ID)
		return false
	}

	n.members = normalizeMembers(n.members)
	n.selfAddr = memberAddr(n.members, n.id)
	n.registerMembersLocked()
	n.persistMembersLocked()
	if !n.selfIsVoterLocked() && n.role == RoleLeader {
		// 领导者把自己移出（或降为 learner）：立即卸任。
		//
		// 这里**不能**走 stepDownLocked：它会 fail 掉全部在途提案，包括"正在被应用的
		// 这条配置变更"，而这条变更必须把成功结果交回提案方。其余提案由调用方在
		// applyConfChangeEntry 里显式失败（见 failProposalsExceptLocked）。
		n.role = RoleFollower
		n.leaderID = ""
		n.resetElectionLocked()
	}
	return true
}

// registerMembersLocked 把成员表里的地址注册进传输层（要求持有主锁）。
func (n *Node) registerMembersLocked() {
	reg, ok := n.tr.(PeerRegistrar)
	if !ok {
		return
	}
	for _, m := range n.members {
		if m.Addr != "" {
			reg.RegisterPeer(m.ID, m.Addr)
		}
	}
}

// persistMembersLocked 把成员表与当前任期/投票一起落盘（要求持有主锁）。
func (n *Node) persistMembersLocked() {
	if err := n.storage.saveState(n.term, n.votedFor, n.members); err != nil {
		n.log.Error("持久化成员表失败", "err", err)
	}
}

// ChangeMembership 提交一次成员变更（仅领导者可用）。
//
// 用法（新增一个节点）：
//  1. 新节点以 Options.Learners=[自己] 启动（它因此不会竞选、也不计入多数派）；
//  2. 在 leader 上 ChangeMembership{Op: ConfAddLearner, ID: 新节点, Addr: 地址}；
//  3. AwaitCatchUp 等它追平；
//  4. ChangeMembership{Op: ConfPromote, ID: 新节点} 把它提升为投票成员。
//
// 本实现**一次只允许一个未提交的变更**（没有 joint consensus）：否则新旧配置的
// 多数派可能不相交，同一任期选出两个 leader。
func (n *Node) ChangeMembership(ctx context.Context, cc ConfChange) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	if n.closed || n.role == RoleShutdown {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.role != RoleLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if !n.hasQuorumLocked() {
		n.mu.Unlock()
		return ErrNoQuorum
	}
	if n.pendingConfIndex > n.commitIndex {
		n.mu.Unlock()
		return ErrConfigInFlight
	}
	if err := n.validateConfChangeLocked(cc); err != nil {
		n.mu.Unlock()
		return err
	}
	data, err := json.Marshal(cc)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("编码成员变更失败: %w", err)
	}
	index, err := n.appendConfLocked(data)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("追加成员变更日志失败: %w", err)
	}
	n.pendingConfIndex = index
	ch := make(chan applyResult, 1)
	n.proposals[index] = ch
	n.maybeAdvanceCommitLocked()
	n.mu.Unlock()

	select {
	case res := <-ch:
		return res.err
	case <-ctx.Done():
		n.dropProposal(index)
		return ctx.Err()
	case <-n.done:
		n.dropProposal(index)
		return ErrStopped
	}
}

// validateConfChangeLocked 在提案阶段做严格校验（要求持有主锁）。
func (n *Node) validateConfChangeLocked(cc ConfChange) error {
	if cc.ID == "" {
		return errors.New("raft: 成员变更缺少目标节点 ID")
	}
	switch cc.Op {
	case ConfAddLearner:
		if _, ok := n.memberLocked(cc.ID); ok {
			return fmt.Errorf("%w: %s", ErrMemberExists, cc.ID)
		}
	case ConfPromote:
		m, ok := n.memberLocked(cc.ID)
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownMember, cc.ID)
		}
		if !m.Learner {
			return fmt.Errorf("%w: %s", ErrNotLearner, cc.ID)
		}
	case ConfRemove:
		if _, ok := n.memberLocked(cc.ID); !ok {
			return fmt.Errorf("%w: %s", ErrUnknownMember, cc.ID)
		}
		if n.isVoterLocked(cc.ID) && n.voterCountLocked() <= 1 {
			return ErrLastVoter
		}
	default:
		return fmt.Errorf("raft: 未知的成员变更操作 %q", string(cc.Op))
	}
	return nil
}

// appendConfLocked 追加一条成员变更日志并落盘。
func (n *Node) appendConfLocked(data []byte) (uint64, error) {
	index := n.rlog.lastIndex() + 1
	e := Entry{Index: index, Term: n.term, Data: data, Type: EntryConfChange}
	if err := n.rlog.append(e); err != nil {
		return 0, err
	}
	if err := n.rlog.sync(); err != nil {
		return 0, err
	}
	n.matchIndex[n.id] = index
	return index, nil
}

// awaitCatchUpPoll 是 AwaitCatchUp 的轮询间隔。
const awaitCatchUpPoll = 20 * time.Millisecond

// AwaitCatchUp 等待成员 id 的复制进度追上本节点的提交点（仅领导者可用）。
func (n *Node) AwaitCatchUp(ctx context.Context, id string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	tick := time.NewTicker(awaitCatchUpPoll)
	defer tick.Stop()
	for {
		n.mu.Lock()
		role := n.role
		commit := n.commitIndex
		progress := n.matchIndex[id]
		known := false
		for _, m := range n.members {
			if m.ID == id {
				known = true
				break
			}
		}
		n.mu.Unlock()
		if role != RoleLeader {
			return ErrNotLeader
		}
		if !known {
			return fmt.Errorf("%w: %s", ErrUnknownMember, id)
		}
		if id == n.id || progress >= commit {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		case <-tick.C:
		}
	}
}

// TransferLeadership 把领导者身份主动让给目标节点（仅领导者可用）。
//
// 做法：先给目标发一条 TimeoutNow（让它立刻竞选），再把自己降为 follower。
// 顺序很关键 —— 先通知再卸任，目标才有机会用一个更高的任期拿到多数票；
// 若目标日志落后（未能当选），本节点已经卸任，集群会在一次随机选举超时内选出新 leader，
// 不会出现"两个 leader 都认为自己有效"的窗口。
//
// 它是 rebalance 的基础能力（把队列的 leader 从承载最多的节点迁走），不是共识安全性的一环。
func (n *Node) TransferLeadership(ctx context.Context, target string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	if n.closed || n.role == RoleShutdown {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.role != RoleLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if target == n.id {
		n.mu.Unlock()
		return nil
	}
	if !n.isVoterLocked(target) {
		n.mu.Unlock()
		return fmt.Errorf("%w: %s（只有投票成员才能接任 leader）", ErrUnknownMember, target)
	}
	term := n.term
	n.mu.Unlock()

	args := timeoutNowArgs{Term: term, LeaderID: n.id}
	payload, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("编码 TimeoutNow 请求失败: %w", err)
	}
	resp, err := n.tr.Call(ctx, target, MethodTimeoutNow, payload)
	if err != nil {
		return fmt.Errorf("通知 %s 竞选失败: %w", target, err)
	}
	var reply timeoutNowReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		return fmt.Errorf("解析 %s 的 TimeoutNow 应答失败: %w", target, err)
	}
	if !reply.OK {
		return fmt.Errorf("%s 未接受 leader 让位（可能已不是投票成员）", target)
	}

	n.mu.Lock()
	// 只在"还是那个任期里的 leader"时卸任：期间可能已经被更高任期接管。
	if n.role == RoleLeader && n.term == term {
		n.stepDownLocked(term, "")
	}
	n.mu.Unlock()
	n.log.Info("已把领导者让给", "target", target, "term", term)
	return nil
}

// handleTimeoutNow 处理"立即竞选"请求。
//
// 只做两件事：推进到请求方的任期（避免用旧任期竞选），然后立刻发起一轮选举。
// 是否真的能当选仍由标准 RequestVote 规则决定。
func (n *Node) handleTimeoutNow(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var args timeoutNowArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, fmt.Errorf("解析 TimeoutNow 请求失败: %w", err)
	}
	n.mu.Lock()
	if n.role == RoleShutdown {
		term := n.term
		n.mu.Unlock()
		return json.Marshal(timeoutNowReply{Term: term})
	}
	if args.Term > n.term {
		n.stepDownLocked(args.Term, args.LeaderID)
	}
	voter := n.selfIsVoterLocked()
	term := n.term
	n.mu.Unlock()
	if !voter {
		return json.Marshal(timeoutNowReply{Term: term})
	}
	n.log.Info("收到 TimeoutNow，立即发起选举", "id", n.id, "from", args.LeaderID)
	n.startElection()
	return json.Marshal(timeoutNowReply{Term: term, OK: true})
}

// takeSnapshot 调用状态机快照并压缩日志前缀。调用方必须已持有 fsmMu。
func (n *Node) takeSnapshot(throughIndex, throughTerm uint64) {
	data, err := n.fsm.Snapshot()
	if err != nil {
		n.log.Warn("状态机快照失败，跳过本次压缩", "index", throughIndex, "err", err)
		return
	}
	n.mu.Lock()
	if throughIndex <= n.rlog.snapIndex() {
		n.mu.Unlock()
		return
	}
	members := append([]Member(nil), n.members...)
	n.mu.Unlock()

	if err := n.storage.saveSnapshot(persistedSnapshot{
		LastIncludedIndex: throughIndex,
		LastIncludedTerm:  throughTerm,
		Data:              data,
		Members:           members,
	}); err != nil {
		n.log.Warn("写入快照文件失败", "index", throughIndex, "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if throughIndex <= n.rlog.snapIndex() {
		return
	}
	if err := n.rlog.compact(throughIndex, throughTerm); err != nil {
		n.log.Warn("压缩日志失败", "index", throughIndex, "err", err)
		return
	}
	n.snapData = data
	if n.lastApplied > throughIndex {
		n.appliedSinceSnap = n.lastApplied - throughIndex
	} else {
		n.appliedSinceSnap = 0
	}
	n.log.Debug("日志已压缩", "index", throughIndex, "term", throughTerm, "remain", n.rlog.count())
}

// slogAdapter 把 *slog.Logger 适配成 raft.Logger。
type slogAdapter struct {
	l *slog.Logger
}

func (s slogAdapter) Debug(msg string, args ...any) { s.l.Debug(msg, args...) }
func (s slogAdapter) Info(msg string, args ...any)  { s.l.Info(msg, args...) }
func (s slogAdapter) Warn(msg string, args ...any)  { s.l.Warn(msg, args...) }
func (s slogAdapter) Error(msg string, args ...any) { s.l.Error(msg, args...) }

// nopLogger 在未提供日志器时静默丢弃日志。
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// NewLogger 把 *slog.Logger 适配成 raft.Logger。
func NewLogger(l *slog.Logger) Logger {
	if l == nil {
		return nopLogger{}
	}
	return slogAdapter{l: l}
}
