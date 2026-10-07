package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/houzch/swiftmq/internal/raft"
)

// methodMetaPropose 是 follower 把写请求转发给 leader 用的 RPC 方法名。
//
// 复用集群端口而不是另开监听：Raft 的传输层本来就是"一根连接、按方法名复用"，
// 元数据转发只是它的第二个使用者。名字刻意不带 raft. 前缀 —— 那是共识内部方法的保留前缀。
const methodMetaPropose = "meta.propose"

// 成员变更的转发方法名：与 meta.propose 同源，但走的是"leader 上的成员变更流程"
// （含 learner 追平等待），不是一个普通的状态机命令。
const (
	methodMetaMemberAdd    = "meta.member_add"
	methodMetaMemberRemove = "meta.member_remove"
)

// memberRequest 是成员变更转发请求体。
type memberRequest struct {
	ID   string `json:"id"`
	Addr string `json:"addr,omitempty"`
}

// 等待 leader 的两档参数：选举窗口内 Leader() 本就短暂为空，
// 短轮询能把这几百毫秒里的写请求接住，而不是立刻把 ErrNotLeader 甩给上层让它重试。
const (
	leaderWaitTimeout  = 2 * time.Second
	leaderPollInterval = 50 * time.Millisecond
)

// proposeResponse 是 meta.propose 的应答体：成功回 {"ok":true}，否则回 {"err":"原因"}。
//
// 用应答体而不是 RPC 错误来传递"leader 拒绝"：这样 follower 能带上具体原因（如角色已变化），
// 运维一眼能看出是"没找到 leader"还是"leader 换了"。
type proposeResponse struct {
	OK  bool   `json:"ok"`
	Err string `json:"err"`
}

// open 是 Open 的实现：校验参数，再按模式构造后端。
func open(_ context.Context, opt Options) (*Store, error) {
	if opt.Applier == nil {
		return nil, errors.New("元数据层需要 Applier（内核回调）")
	}
	if opt.Logger == nil {
		return nil, errors.New("元数据层需要 Logger")
	}
	if opt.Dir == "" {
		return nil, errors.New("元数据层需要 Dir（快照与日志的落盘目录）")
	}
	switch opt.Mode {
	case ModeLocal:
		impl, err := openLocal(opt)
		if err != nil {
			return nil, err
		}
		return &Store{impl: impl}, nil
	case ModeRaft:
		if opt.NodeID == "" {
			return nil, errors.New("集群模式需要 NodeID")
		}
		if opt.Listen == "" {
			return nil, errors.New("集群模式需要 Listen（集群 RPC 监听地址）")
		}
		if _, ok := opt.Peers[opt.NodeID]; !ok {
			return nil, fmt.Errorf("集群模式的 Peers 必须包含本节点 %q", opt.NodeID)
		}
		impl, err := openCluster(opt)
		if err != nil {
			return nil, err
		}
		return &Store{impl: impl}, nil
	default:
		return nil, fmt.Errorf("未知的元数据后端模式 %d", opt.Mode)
	}
}

// clusterStore 是集群后端：写入经 Raft 提交，非 leader 节点转发给 leader。
type clusterStore struct {
	opt       Options
	fsm       *fsm
	node      raft.Consensus
	transport raft.Transport
	logger    Logger

	// ownTransport 表示传输层是本层自己创建的（Options.Transport 为 nil 时的 TCP 传输）。
	// 注入进来的传输归调用方所有：它可能在别处被复用，由我们关掉会让调用方"突然失联"
	// （测试里换网络重启节点就是这种情形），所以关闭时只回收自己创建的那一份。
	ownTransport bool

	closeOnce sync.Once
	closeErr  error
}

// openCluster 启动 Raft 节点、注册转发处理器，并在 Start 之前把完整状态交给内核。
func openCluster(opt Options) (*clusterStore, error) {
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建元数据目录 %s 失败: %w", opt.Dir, err)
	}
	transport := opt.Transport
	ownTransport := false
	if transport == nil {
		t, err := raft.NewTCPTransport(opt.Listen, opt.NodeID, opt.Peers, opt.Logger)
		if err != nil {
			return nil, fmt.Errorf("创建集群传输失败 (listen=%s): %w", opt.Listen, err)
		}
		transport = t
		ownTransport = true
	}

	f := newFSM(opt.Applier, opt.Logger)
	voters := append([]string(nil), opt.Voters...)
	if len(voters) == 0 {
		voters = sortedPeerIDs(opt.Peers)
	} else {
		sort.Strings(voters)
	}
	c := &clusterStore{opt: opt, fsm: f, transport: transport, logger: opt.Logger, ownTransport: ownTransport}
	node, err := raft.New(raft.Options{
		ID:                 opt.NodeID,
		Peers:              voters,
		Learners:           append([]string(nil), opt.Learners...),
		Dir:                opt.Dir,
		Transport:          transport,
		FSM:                f,
		ElectionTimeout:    opt.ElectionTimeout,
		HeartbeatInterval:  opt.HeartbeatInterval,
		Logger:             opt.Logger,
		OnMembershipChange: opt.OnMembershipChange,
	})
	if err != nil {
		c.abort()
		return nil, fmt.Errorf("创建 Raft 节点失败 (node_id=%s): %w", opt.NodeID, err)
	}
	c.node = node

	if err := transport.Serve(methodMetaPropose, c.servePropose); err != nil {
		c.abort()
		return nil, fmt.Errorf("注册元数据转发处理器 %s 失败: %w", methodMetaPropose, err)
	}
	if err := transport.Serve(methodMetaMemberAdd, c.serveMemberAdd); err != nil {
		c.abort()
		return nil, fmt.Errorf("注册成员变更处理器 %s 失败: %w", methodMetaMemberAdd, err)
	}
	if err := transport.Serve(methodMetaMemberRemove, c.serveMemberRemove); err != nil {
		c.abort()
		return nil, fmt.Errorf("注册成员变更处理器 %s 失败: %w", methodMetaMemberRemove, err)
	}

	// 先把完整状态交给内核，再 Start：Start 之后的重放会逐条 ApplyMeta，
	// 顺序上"全量在前、增量在后"，内核就不会看到"先增量、后全量"而丢掉期间的变更。
	if err := opt.Applier.RestoreMeta(f.state()); err != nil {
		c.abort()
		return nil, fmt.Errorf("初始化内核元数据失败: %w", err)
	}
	if err := node.Start(); err != nil {
		c.abort()
		return nil, fmt.Errorf("启动 Raft 节点失败 (node_id=%s): %w", opt.NodeID, err)
	}

	q, e, b, u := counts(f.state())
	opt.Logger.Info("元数据层已打开", "mode", ModeRaft.String(), "node_id", opt.NodeID, "dir", opt.Dir,
		"voters", len(voters), "learners", len(opt.Learners),
		"queues", q, "exchanges", e, "bindings", b, "users", u)
	return c, nil
}

// abort 是打开失败路径上的清理。
//
// 只记日志不返回错误：此时首要错误已经产生（调用方拿到的也是它），
// 再抛一个清理错误只会盖掉真正的原因。
func (c *clusterStore) abort() {
	if err := c.shutdown(); err != nil {
		c.logger.Warn("清理集群资源失败", "err", err)
	}
}

// shutdown 按"先停 Raft 节点、再关自己创建的传输"的顺序回收资源。
func (c *clusterStore) shutdown() error {
	if c.node != nil {
		c.node.Stop()
	}
	if !c.ownTransport {
		// 注入的传输归调用方所有：它可能还要被复用（例如换网络重开节点），不由我们关闭。
		return nil
	}
	if err := c.transport.Close(); err != nil {
		return fmt.Errorf("关闭集群传输失败: %w", err)
	}
	return nil
}

// write 提交一次变更：本节点是 leader 就直接 Propose，否则转给 leader。
func (c *clusterStore) write(ctx context.Context, op Op, payload any) error {
	data, err := encodeCommand(op, payload)
	if err != nil {
		return err
	}
	if c.node.IsLeader() {
		if _, err := c.node.Propose(ctx, data); err != nil {
			return c.convertProposeErr(op, err)
		}
		return nil
	}
	return c.forward(ctx, op, data)
}

// forward 把已编码的命令转给当前 leader。
//
// 返回的失败一律收敛为 ErrNotLeader：从上层视角，"转发不出去"与"我不是 leader"是同一件事 ——
// 都应该去别的节点重试。具体原因（超时 / leader 换了 / 未选出）写进错误文本，供运维定位。
func (c *clusterStore) forward(ctx context.Context, op Op, data []byte) error {
	leader, err := c.waitLeader(ctx)
	if err != nil {
		return err
	}
	if leader == "" {
		return fmt.Errorf("%w: %s 内未获知 leader（op=%s）", ErrNotLeader, leaderWaitTimeout, op)
	}
	if leader == c.opt.NodeID {
		// 已知 leader 是自己但 IsLeader() 尚未翻转（角色更新存在瞬间窗口）：直接本地提交，
		// 免得为一次写请求绕一圈自环 RPC。
		if _, err := c.node.Propose(ctx, data); err != nil {
			return c.convertProposeErr(op, err)
		}
		return nil
	}

	resp, err := c.transport.Call(ctx, leader, methodMetaPropose, data)
	if err != nil {
		return fmt.Errorf("%w: 向 leader %s 转发 %s 失败: %v", ErrNotLeader, leader, op, err)
	}
	var r proposeResponse
	if err := json.Unmarshal(resp, &r); err != nil {
		return fmt.Errorf("%w: leader %s 对 %s 的应答无法解析: %v", ErrNotLeader, leader, op, err)
	}
	if r.Err != "" {
		return fmt.Errorf("%w: leader %s 拒绝 %s: %s", ErrNotLeader, leader, op, r.Err)
	}
	if !r.OK {
		return fmt.Errorf("%w: leader %s 对 %s 未返回成功标记", ErrNotLeader, leader, op)
	}
	return nil
}

// waitLeader 返回已知的 leader；未知时短轮询等待，仍为空返回空串。
func (c *clusterStore) waitLeader(ctx context.Context) (string, error) {
	if leader := c.node.Leader(); leader != "" {
		return leader, nil
	}
	timer := time.NewTimer(leaderWaitTimeout)
	defer timer.Stop()
	tick := time.NewTicker(leaderPollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timer.C:
			return "", nil
		case <-tick.C:
			if leader := c.node.Leader(); leader != "" {
				return leader, nil
			}
		}
	}
}

// servePropose 是 meta.propose 的处理器（在 leader 侧执行）：把 follower 转来的命令本地提交。
func (c *clusterStore) servePropose(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var cmd command
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return nil, fmt.Errorf("解析 %s 请求失败 (from=%s): %w", methodMetaPropose, from, err)
	}
	if !c.node.IsLeader() {
		return encodeProposeResponse(false, "本节点已不是 leader")
	}
	if _, err := c.node.Propose(ctx, payload); err != nil {
		return encodeProposeResponse(false, err.Error())
	}
	return encodeProposeResponse(true, "")
}

// convertProposeErr 把 Raft 的提交错误翻译成上层语义。
//
// 不再是 leader（并发选主时会发生）收敛为 ErrNotLeader，上层只需判断一种错误就能决定"换节点重试"；
// 失去多数派则原样保留 raft.ErrNoQuorum —— 运维据此区分"正在选主"与"网络分区"。
func (c *clusterStore) convertProposeErr(op Op, err error) error {
	if errors.Is(err, raft.ErrNotLeader) {
		return fmt.Errorf("%w: 提交 %s 时本节点已不是 leader: %v", ErrNotLeader, op, err)
	}
	return fmt.Errorf("提交元数据变更 %s 失败: %w", op, err)
}

// state 返回本节点当前的状态机快照。
func (c *clusterStore) state() State { return c.fsm.state() }

// membership 返回元数据组的成员划分。
func (c *clusterStore) membership() raft.Membership { return c.node.Membership() }

// status 返回集群侧共识进度 + 状态机规模。
func (c *clusterStore) status() Status {
	rs := c.node.Status()
	q, e, b, u := counts(c.fsm.state())
	return Status{
		Mode:           ModeRaft.String(),
		NodeID:         c.opt.NodeID,
		Role:           rs.Role.String(),
		Term:           rs.Term,
		Leader:         rs.Leader,
		CommitIndex:    rs.CommitIndex,
		LastApplied:    rs.LastApplied,
		ProposeEntries: rs.ProposeEntries,
		FsyncTotal:     rs.FsyncTotal,
		Peers:          rs.Peers,
		Learners:       rs.Learners,
		HasQuorum:      c.node.HasQuorum(),
		AppliedRecords: c.fsm.appliedRecords(),
		Queues:         q,
		Exchanges:      e,
		Bindings:       b,
		Users:          u,
	}
}

// close 停止 Raft 并回收本层创建的资源；可重复调用（第二次起直接返回首次的错误）。
func (c *clusterStore) close() error {
	c.closeOnce.Do(func() { c.closeErr = c.shutdown() })
	return c.closeErr
}

// ---------------------------------------------------------------------------
// 成员变更（M6d）
// ---------------------------------------------------------------------------

// catchUpTimeout 是"等新成员追平"的上限。
//
// 比普通写入宽松得多：新节点要从零拉日志/快照，日志多时耗时不可忽略；
// 调用方（管理 API）通常把 HTTP 超时设得更长。
const catchUpTimeout = 60 * time.Second

// addMember 把节点加入集群：learner → 等追平 → 提升为 voter。
func (c *clusterStore) addMember(ctx context.Context, id, addr string) error {
	if id == "" {
		return errors.New("meta: 成员 ID 不能为空")
	}
	if c.node.IsLeader() {
		return c.addMemberLocal(ctx, id, addr)
	}
	return c.forwardMember(ctx, methodMetaMemberAdd, memberRequest{ID: id, Addr: addr})
}

// removeMember 把节点从集群移除。
func (c *clusterStore) removeMember(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("meta: 成员 ID 不能为空")
	}
	if c.node.IsLeader() {
		return c.removeMemberLocal(ctx, id)
	}
	return c.forwardMember(ctx, methodMetaMemberRemove, memberRequest{ID: id})
}

// addMemberLocal 在 leader 上执行完整的加入流程（幂等：重复调用不会报错）。
func (c *clusterStore) addMemberLocal(ctx context.Context, id, addr string) error {
	err := c.node.ChangeMembership(ctx, raft.ConfChange{Op: raft.ConfAddLearner, ID: id, Addr: addr})
	if err != nil && !errors.Is(err, raft.ErrMemberExists) {
		return fmt.Errorf("把 %s 作为 learner 加入失败: %w", id, err)
	}

	// 等它把日志/快照拉过去再提升：否则短暂的"投票成员但日志落后"会降低集群可用性。
	catchUpCtx, cancel := context.WithTimeout(ctx, catchUpTimeout)
	defer cancel()
	if err := c.node.AwaitCatchUp(catchUpCtx, id); err != nil {
		return fmt.Errorf("等待新成员 %s 追平失败: %w", id, err)
	}

	err = c.node.ChangeMembership(ctx, raft.ConfChange{Op: raft.ConfPromote, ID: id})
	if err != nil && !errors.Is(err, raft.ErrNotLearner) {
		// ErrNotLearner = 它已经是投票成员（重复调用的幂等路径）。
		return fmt.Errorf("把 %s 提升为投票成员失败: %w", id, err)
	}
	c.logger.Info("集群成员已加入", "id", id, "addr", addr)
	return nil
}

// removeMemberLocal 在 leader 上移除成员（幂等：目标已不是成员时视为成功）。
func (c *clusterStore) removeMemberLocal(ctx context.Context, id string) error {
	err := c.node.ChangeMembership(ctx, raft.ConfChange{Op: raft.ConfRemove, ID: id})
	if err != nil {
		if errors.Is(err, raft.ErrUnknownMember) {
			return nil
		}
		return fmt.Errorf("移除成员 %s 失败: %w", id, err)
	}
	c.logger.Info("集群成员已移除", "id", id)
	return nil
}

// forwardMember 把成员变更请求转给当前 leader。
func (c *clusterStore) forwardMember(ctx context.Context, method string, req memberRequest) error {
	leader, err := c.waitLeader(ctx)
	if err != nil {
		return err
	}
	if leader == "" {
		return fmt.Errorf("%w: %s 内未获知 leader（%s）", ErrNotLeader, leaderWaitTimeout, method)
	}
	if leader == c.opt.NodeID {
		// 已知 leader 是自己但 IsLeader() 尚未翻转：直接本地执行。
		if method == methodMetaMemberAdd {
			return c.addMemberLocal(ctx, req.ID, req.Addr)
		}
		return c.removeMemberLocal(ctx, req.ID)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("编码 %s 请求失败: %w", method, err)
	}
	resp, err := c.transport.Call(ctx, leader, method, payload)
	if err != nil {
		return fmt.Errorf("%w: 向 leader %s 转发 %s 失败: %v", ErrNotLeader, leader, method, err)
	}
	var r proposeResponse
	if err := json.Unmarshal(resp, &r); err != nil {
		return fmt.Errorf("%w: leader %s 对 %s 的应答无法解析: %v", ErrNotLeader, leader, method, err)
	}
	if r.Err != "" {
		return fmt.Errorf("%w: leader %s 拒绝 %s: %s", ErrNotLeader, leader, method, r.Err)
	}
	if !r.OK {
		return fmt.Errorf("%w: leader %s 对 %s 未返回成功标记", ErrNotLeader, leader, method)
	}
	return nil
}

// serveMemberAdd 是 meta.member_add 的处理器（在 leader 侧执行）。
func (c *clusterStore) serveMemberAdd(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var req memberRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析 %s 请求失败 (from=%s): %w", methodMetaMemberAdd, from, err)
	}
	if !c.node.IsLeader() {
		return encodeProposeResponse(false, "本节点已不是 leader")
	}
	if err := c.addMemberLocal(ctx, req.ID, req.Addr); err != nil {
		return encodeProposeResponse(false, err.Error())
	}
	return encodeProposeResponse(true, "")
}

// serveMemberRemove 是 meta.member_remove 的处理器（在 leader 侧执行）。
func (c *clusterStore) serveMemberRemove(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var req memberRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析 %s 请求失败 (from=%s): %w", methodMetaMemberRemove, from, err)
	}
	if !c.node.IsLeader() {
		return encodeProposeResponse(false, "本节点已不是 leader")
	}
	if err := c.removeMemberLocal(ctx, req.ID); err != nil {
		return encodeProposeResponse(false, err.Error())
	}
	return encodeProposeResponse(true, "")
}

// encodeProposeResponse 编码 meta.propose 的应答体。
func encodeProposeResponse(ok bool, errMsg string) ([]byte, error) {
	data, err := json.Marshal(proposeResponse{OK: ok, Err: errMsg})
	if err != nil {
		return nil, fmt.Errorf("编码 %s 应答失败: %w", methodMetaPropose, err)
	}
	return data, nil
}

// sortedPeerIDs 返回排序后的成员 ID：Raft 的成员列表参与选举比较，
// 顺序必须由内容决定而不是由 map 迭代顺序决定。
func sortedPeerIDs(peers map[string]string) []string {
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
