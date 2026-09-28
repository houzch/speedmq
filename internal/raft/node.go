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
}

type installSnapshotReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
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
	id    string
	peers []string // 已排序，含自己
	tr    Transport
	fsm   FSM
	log   Logger
	dir   string

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
func New(opt Options) (*Node, error) {
	if err := validateOptions(opt); err != nil {
		return nil, err
	}
	peers := append([]string(nil), opt.Peers...)
	sort.Strings(peers)

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

	n := &Node{
		id: opt.ID, peers: peers, tr: opt.Transport, fsm: opt.FSM, log: logger, dir: opt.Dir,
		electionTimeout: electionTimeout, heartbeatInterval: heartbeat, snapshotThreshold: threshold,
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
	if hasSnap {
		if err := opt.FSM.Restore(snapData); err != nil {
			_ = st.close()
			return nil, fmt.Errorf("恢复状态机快照失败: %w", err)
		}
	}
	return n, nil
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
	if len(opt.Peers) == 0 {
		return errors.New("raft: Options.Peers 不能为空")
	}
	seen := make(map[string]struct{}, len(opt.Peers))
	found := false
	for _, p := range opt.Peers {
		if p == "" {
			return errors.New("raft: Options.Peers 含空成员 ID")
		}
		if _, dup := seen[p]; dup {
			return fmt.Errorf("raft: Options.Peers 成员 %q 重复", p)
		}
		seen[p] = struct{}{}
		if p == opt.ID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("raft: Options.Peers 必须包含本节点 ID %q", opt.ID)
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
	peers := make([]string, len(n.peers))
	copy(peers, n.peers)
	return Status{
		ID:            n.id,
		Role:          n.role,
		Term:          n.term,
		Leader:        n.leaderID,
		CommitIndex:   n.commitIndex,
		LastLogIndex:  n.rlog.lastIndex(),
		LastApplied:   n.lastApplied,
		SnapshotIndex: n.rlog.snapIndex(),
		Peers:         peers,
	}
}

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

// quorum 是多数派所需票数。
func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

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
		for _, p := range n.peers {
			if p == n.id {
				continue
			}
			if t, ok := n.lastAck[p]; ok && now.Sub(t) < window {
				count++
			}
		}
		return count >= n.quorum()
	case RoleFollower:
		if n.lastLeaderContact.IsZero() {
			return false
		}
		return time.Since(n.lastLeaderContact) < n.electionTimeout
	default:
		return false
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
		single := len(n.peers) == 1
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
		n.mu.Unlock()
		if role == RoleShutdown {
			return
		}
		if role == RoleLeader {
			n.tickLeader()
		} else {
			n.startElection()
		}
	}
}

// startElection 发起一轮竞选：任期 +1、投自己一票（先落盘）后并行拉票。
func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == RoleLeader || n.role == RoleShutdown {
		n.mu.Unlock()
		return
	}
	n.role = RoleCandidate
	n.term++
	n.votedFor = n.id
	n.leaderID = ""
	// 选举安全性：投票（与任期一起）必须先 fsync 再对外拉票，
	// 否则崩溃重启后可能在同任期投出第二票。
	if err := n.storage.saveState(n.term, n.votedFor); err != nil {
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
	others := make([]string, 0, len(n.peers)-1)
	for _, p := range n.peers {
		if p != n.id {
			others = append(others, p)
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
	now := time.Now()
	n.role = RoleLeader
	n.leaderID = n.id
	last := n.rlog.lastIndex()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	n.lastAck = make(map[string]time.Time, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
		// 乐观初始应答时间：首轮心跳返回前不至于误判"无多数派"，窗口过后若无应答即判失去联系。
		n.lastAck[p] = now
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
	tasks := make([]task, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		next := n.nextIndex[p]
		if next == 0 {
			next = 1
		}
		// 需要的前一条已被压缩掉：只能整体安装快照（增量追赶不再可能）。
		if next <= n.rlog.snapIndex() {
			if snap, ok := n.snapshotToSendLocked(); ok {
				tasks = append(tasks, task{peer: p, snap: &snap})
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
			tasks = append(tasks, task{peer: p, snap: &snap})
			continue
		}
		args.Entries = n.rlog.entriesFrom(next, maxAppendEntries)
		tasks = append(tasks, task{peer: p, args: args})
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
	candidates := make([]uint64, 0, len(n.peers))
	candidates = append(candidates, n.rlog.lastIndex())
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		candidates = append(candidates, n.matchIndex[p])
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
	if err := n.storage.saveState(n.term, n.votedFor); err != nil {
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
	grantable := args.Term == n.term &&
		(n.votedFor == "" || n.votedFor == args.CandidateID) &&
		n.candidateLogUpToDateLocked(args.LastLogIndex, args.LastLogTerm)
	if grantable {
		if n.votedFor != args.CandidateID {
			n.votedFor = args.CandidateID
			// 投票先落盘再应答：否则崩溃重启后可能在同一任期投出第二票。
			if err := n.storage.saveState(n.term, n.votedFor); err != nil {
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
	}
	if err := n.storage.saveSnapshot(snap); err != nil {
		return nil, fmt.Errorf("写入快照文件失败: %w", err)
	}
	if err := n.fsm.Restore(args.Data); err != nil {
		return nil, fmt.Errorf("恢复状态机快照失败: %w", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if args.LastIncludedIndex <= n.rlog.snapIndex() {
		return json.Marshal(installSnapshotReply{Term: n.term, Success: true})
	}
	// 本地在快照位置处有相同任期的条目时保留其后缀，否则整体丢弃重建。
	var err error
	if t, ok := n.rlog.termAt(args.LastIncludedIndex); ok && t == args.LastIncludedTerm {
		err = n.rlog.compact(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		err = n.rlog.reset(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	if err != nil {
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
	n.log.Info("已安装快照", "index", args.LastIncludedIndex, "term", args.LastIncludedTerm)
	return json.Marshal(installSnapshotReply{Term: n.term, Success: true})
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
		n.lastApplied = index
		n.appliedSinceSnap++
		needSnapshot := n.appliedSinceSnap >= n.snapshotThreshold
		n.mu.Unlock()

		value, err := n.fsm.Apply(index, entry.Data)

		if needSnapshot {
			n.takeSnapshot(index, entry.Term)
		}
		n.fsmMu.Unlock()

		n.deliverResult(index, value, err)
	}
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
	n.mu.Unlock()

	if err := n.storage.saveSnapshot(persistedSnapshot{
		LastIncludedIndex: throughIndex,
		LastIncludedTerm:  throughTerm,
		Data:              data,
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
