package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/houzch/swiftmq/internal/raft"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件是仲裁队列（Quorum Queue）：**每条队列一个独立的 Raft 组**，
// 消息复制到多数派后才算被接受，因此单节点故障不会丢已确认的消息。
//
// 与设计的对应关系：
//   - 队列放置不再是 client-local，而是"组 leader 服务、全体成员复制"；
//   - 经典队列的 `store.QueueStore` 在这里被 **Raft 日志**取代（日志本身就是持久化）；
//   - 投递、未确认、prefetch 是 leader 本地的**软状态**，不参与复制。leader 变更后
//     新 leader 的 ready 里仍然有那些消息（它们从未被复制地"移出"），于是被重新投递 ——
//     这就是仲裁队列的 at-least-once 语义，也让"未确认消息不丢"不需要额外的协调。
//
// 有意不做的（与 RabbitMQ 的口径一致，声明时明确拒绝而不是静默忽略）：
// `x-expires`、`x-max-priority`、`x-overflow=reject-publish-dlx`，以及非 durable / exclusive /
// auto-delete 的仲裁队列。**未做**：跨节点成员的动态增删（成员与集群成员一致，见 M6c）。

const (
	// quorumElectionTimeout / quorumHeartbeat 刻意比元数据组宽松：
	// 一台机器上可能同时跑多条队列的组，心跳太密会让选举风暴互相干扰。
	quorumElectionTimeout = 500 * time.Millisecond
	quorumHeartbeat       = 100 * time.Millisecond
	// quorumSnapshotEvery 是日志压缩阈值（已应用条目数）。
	//
	// 仲裁队列的状态就是"尚未确认的消息"，因此快照大小与队列深度同阶 ——
	// 阈值给大一些以减少快照次数，代价是恢复时要重放更多条目。
	quorumSnapshotEvery = 4096
	// quorumProposeTimeout 是单次提案（发布 / 确认 / 清空）的上限。
	quorumProposeTimeout = 10 * time.Second
	// quorumLeaderWait 是"本节点还不知道组 leader"时的等待上限（见 vhost.queueOwner）。
	quorumLeaderWait = 3 * time.Second
	// quorumLeaderPoll 是等 leader 的轮询间隔。
	quorumLeaderPoll = 20 * time.Millisecond
)

// 仲裁队列的日志命令。
//
// 日志里只记"决定"，不记"过程"：长度限制怎么算、谁先谁后、是否过期，全部由 leader
// 判定后写进日志，副本只按序应用。这样状态机不依赖时间与本地状态，副本之间不会分叉。
const (
	quorumOpPublish = "publish"
	quorumOpAck     = "ack"
	quorumOpPurge   = "purge"
)

// quorumCommand 是一条日志命令。
type quorumCommand struct {
	Op string `json:"op"`
	// Seq 是消息在队列内的稳定序号（由 leader 分配）：发布时指定，确认时引用。
	Seq uint64 `json:"seq,omitempty"`
	// Body 是 MessageCodec 编码后的消息（仅 publish）。
	Body []byte `json:"body,omitempty"`
	// ExpireAt 是该消息的到期时刻（Unix 毫秒，0 表示不过期）。
	//
	// 到期时刻必须**由 leader 算好写进日志**：如果让每个副本各自用"应用时刻 + TTL" 计算，
	// 副本之间的过期边界就会不一致；重启后重放也会把 TTL 重新起算。
	ExpireAt int64 `json:"expire_at,omitempty"`
}

func encodeQuorumCommand(cmd quorumCommand) ([]byte, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("编码仲裁队列日志命令失败: %w", err)
	}
	return data, nil
}

// checkQuorumDeclare 校验仲裁队列的声明约束。
//
// 与 RabbitMQ 的口径一致：仲裁队列必须 durable、非独占、非自动删除、且有名字
// （名字要能被所有成员识别，服务端命名没有意义）。这些是**语义前提**，
// 违反时必须明确报 406，而不是悄悄建成一条不具备复制保证的队列。
func checkQuorumDeclare(req plugin.QueueDeclare) error {
	if req.Name == "" {
		return precondition("quorum queues must be declared with a name")
	}
	if !req.Durable {
		return precondition("quorum queues must be durable")
	}
	if req.Exclusive {
		return precondition("quorum queues cannot be exclusive")
	}
	if req.AutoDelete {
		return precondition("quorum queues cannot be auto-delete")
	}
	return nil
}

// groupTransport 把一条 Raft 组的 RPC 挂到共享集群端口上。
//
// 集群端口是"一根连接、按方法名分发"（见 raft.Transport 的约定），因此多开 Raft 组
// 只需给方法名加组前缀 —— 不必再开端口，也不必改共识层。
type groupTransport struct {
	prefix string
	// inner 为 nil 表示单机部署下的"单机组"：没有邻居，任何跨节点调用都不该发生。
	inner raft.Transport
}

func (t groupTransport) Call(ctx context.Context, to, method string, payload []byte) ([]byte, error) {
	if t.inner == nil {
		return nil, fmt.Errorf("仲裁队列的单机组不应发起跨节点调用（to=%s method=%s）", to, method)
	}
	return t.inner.Call(ctx, to, t.prefix+method, payload)
}

func (t groupTransport) Serve(method string, h func(context.Context, string, []byte) ([]byte, error)) error {
	if t.inner == nil {
		return nil
	}
	return t.inner.Serve(t.prefix+method, h)
}

// Close 是空实现：集群端口归内核所有（元数据组也在用），由内核统一关闭。
func (t groupTransport) Close() error { return nil }

// quorumGroup 是一条仲裁队列的 Raft 组。
type quorumGroup struct {
	b   *Broker
	id  string // vhost + \x00 + queue
	dir string
	q   *queue
	log *slog.Logger

	mu        sync.Mutex
	node      *raft.Node
	stopped   bool
	wasLeader bool
}

// startQuorumGroup 启动一条仲裁队列的 Raft 组（每个节点都会为同一条队列启动自己的一份）。
func (b *Broker) startQuorumGroup(v *vhost, q *queue) error {
	id := v.name + "\x00" + q.name
	dir := filepath.Join(b.cfg.DataDir, "quorum",
		store.SafeDirName(v.name), store.SafeDirName(q.name))
	g := &quorumGroup{b: b, id: id, dir: dir, q: q, log: b.log}

	node, err := raft.New(raft.Options{
		ID:    b.nodeID,
		Peers: b.quorumMembers(),
		Dir:   dir,
		// 方法名带组前缀：同一个集群端口上可以并存任意多条队列的组。
		Transport:         groupTransport{prefix: "quorum:" + id + ":", inner: b.cluster},
		FSM:               &quorumFSM{g: g},
		ElectionTimeout:   quorumElectionTimeout,
		HeartbeatInterval: quorumHeartbeat,
		SnapshotThreshold: quorumSnapshotEvery,
		Logger:            b.log,
	})
	if err != nil {
		return err
	}
	g.node = node
	if err := node.Start(); err != nil {
		node.Stop()
		return err
	}
	q.quorum = g
	b.log.Debug("仲裁队列的 Raft 组已启动", "vhost", v.name, "queue", q.name,
		"members", len(b.quorumMembers()), "dir", dir)
	return nil
}

// quorumMembers 返回仲裁队列 Raft 组的投票成员。
//
// 与元数据组保持一致：集群全部成员；单机部署时就是自己（单机组，日志仍然持久化）。
// 成员动态增删属 M6c —— 本期是静态成员，与 cluster.peers 同进同退。
func (b *Broker) quorumMembers() []string {
	if !b.clusterOn {
		return []string{b.nodeID}
	}
	ids := make([]string, 0, len(b.cfg.Cluster.Peers))
	for id := range b.cfg.Cluster.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// stop 停掉 Raft 组并删除它的日志与快照目录。可重复调用。
func (g *quorumGroup) stop() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	g.stopped = true
	node := g.node
	g.mu.Unlock()

	if node != nil {
		node.Stop()
	}
	// 队列被删除时数据一并回收：仲裁队列的数据就是这组日志与快照。
	if err := os.RemoveAll(g.dir); err != nil {
		g.log.Warn("删除仲裁队列的日志目录失败", "dir", g.dir, "err", err)
	}
}

// isLeader 表示本节点是否是这条队列 Raft 组的 leader（即"服务节点"）。
func (g *quorumGroup) isLeader() bool {
	g.mu.Lock()
	stopped, node := g.stopped, g.node
	g.mu.Unlock()
	return !stopped && node != nil && node.IsLeader()
}

// leaderNode 返回当前 leader 的节点 ID；未知（正在选主）时为空串。
func (g *quorumGroup) leaderNode() string {
	g.mu.Lock()
	stopped, node := g.stopped, g.node
	g.mu.Unlock()
	if stopped || node == nil {
		return ""
	}
	return node.Leader()
}

// propose 提交一条命令并等待它被提交与应用。
func (g *quorumGroup) propose(cmd quorumCommand) error {
	data, err := encodeQuorumCommand(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), quorumProposeTimeout)
	defer cancel()
	_, err = g.node.Propose(ctx, data)
	return err
}

// proposeAck 把"这条消息已确认"写进日志。
//
// 异步执行：客户端 ack 没有可见应答，没必要让 ack 路径白等一次 Raft 往返。
// 提案失败只记日志 —— 消息仍留在日志里，会被重新投递（at-least-once），不会丢。
func (g *quorumGroup) proposeAck(seq uint64) {
	data, err := encodeQuorumCommand(quorumCommand{Op: quorumOpAck, Seq: seq})
	if err != nil {
		g.log.Error("编码仲裁队列确认命令失败", "queue", g.q.name, "seq", seq, "err", err)
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), quorumProposeTimeout)
		defer cancel()
		if _, err := g.node.Propose(ctx, data); err != nil {
			g.log.Warn("仲裁队列的确认写入日志失败（消息会被重新投递）",
				"queue", g.q.name, "seq", seq, "err", err)
		}
	}()
}

// quorumFSM 是仲裁队列的 Raft 状态机。
//
// 状态 = "已被接受、尚未确认"的消息集合，直接体现在队列的 ready 列表上：
//
//	publish → 追加进 ready（所有副本都追加，因此任何副本都能随时接任 leader）
//	ack     → 从 ready 移除（leader 上的投递与未确认是本地软状态，不参与复制）
//	purge   → 清空
type quorumFSM struct {
	g *quorumGroup
}

// Apply 按索引顺序应用一条已提交的日志。
func (f *quorumFSM) Apply(index uint64, data []byte) (any, error) {
	// leader 当选时会追加一条空条目用于提交此前任期的日志：它不改变状态。
	if len(data) == 0 {
		return nil, nil
	}
	var cmd quorumCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("解析仲裁队列日志命令失败 (index=%d): %w", index, err)
	}
	q := f.g.q
	switch cmd.Op {
	case quorumOpPublish:
		msg, err := f.g.b.decodeMessage(cmd.Body)
		if err != nil {
			return nil, err
		}
		q.applyQuorumPublish(cmd.Seq, msg, cmd.ExpireAt)
	case quorumOpAck:
		q.applyQuorumAck(cmd.Seq)
	case quorumOpPurge:
		q.applyQuorumPurge()
	default:
		// 未知命令是确定性错误：所有副本会对同一条日志做同样的失败，
		// 因此不会破坏一致性，反而能让坏命令尽早暴露。
		return nil, fmt.Errorf("未知的仲裁队列日志命令 %q", cmd.Op)
	}
	return nil, nil
}

// quorumSnapshotEntry 是快照里的一条消息。
type quorumSnapshotEntry struct {
	Seq      uint64 `json:"seq"`
	Body     []byte `json:"body"`
	ExpireAt int64  `json:"expire_at,omitempty"`
}

type quorumSnapshot struct {
	Entries []quorumSnapshotEntry `json:"entries"`
}

// Snapshot 返回状态机快照，用于日志压缩与落后节点追赶。
//
// 必须把**未确认**的消息也算进来：它们已经从 ready 移走，但在语义上仍在队列里；
// 漏掉它们会让"快照之后重启"的节点永久丢掉这些消息。
func (f *quorumFSM) Snapshot() ([]byte, error) {
	q := f.g.q
	q.mu.Lock()
	items := make([]*queuedMsg, 0, len(q.ready)+len(q.unacked))
	items = append(items, q.ready...)
	for _, item := range q.unacked {
		items = append(items, item)
	}
	q.mu.Unlock()

	// 按序号排序：ready 与 unacked 合起来才是完整顺序。
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })

	snap := quorumSnapshot{Entries: make([]quorumSnapshotEntry, 0, len(items))}
	for _, item := range items {
		body, err := f.g.b.encodeMessage(item.msg)
		if err != nil {
			return nil, err
		}
		e := quorumSnapshotEntry{Seq: item.seq, Body: body}
		if !item.expireAt.IsZero() {
			e.ExpireAt = item.expireAt.UnixMilli()
		}
		snap.Entries = append(snap.Entries, e)
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("编码仲裁队列快照失败: %w", err)
	}
	return data, nil
}

// Restore 用快照整体替换状态机的内存状态。
func (f *quorumFSM) Restore(data []byte) error {
	var snap quorumSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("解析仲裁队列快照失败: %w", err)
	}
	items := make([]*queuedMsg, 0, len(snap.Entries))
	var maxSeq uint64
	for _, e := range snap.Entries {
		msg, err := f.g.b.decodeMessage(e.Body)
		if err != nil {
			return err
		}
		item := &queuedMsg{msg: msg, seq: e.Seq, priority: msg.Properties.Priority}
		if e.ExpireAt > 0 {
			item.expireAt = time.UnixMilli(e.ExpireAt)
		}
		items = append(items, item)
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })

	q := f.g.q
	q.mu.Lock()
	q.ready = items
	q.readyBytes = 0
	for _, item := range items {
		q.readyBytes += messageSize(item.msg)
	}
	q.unacked = map[uint64]*queuedMsg{}
	if maxSeq > q.nextSeq {
		q.nextSeq = maxSeq
	}
	needDispatch := len(q.consumers) > 0
	q.mu.Unlock()
	if needDispatch {
		go q.dispatch()
	}
	return nil
}

// ---------------------------------------------------------------------------
// 队列侧的仲裁语义
// ---------------------------------------------------------------------------

// publishQuorum 是仲裁队列的发布路径：把"这条消息入队"写进 Raft 日志。
//
// 返回的 wait 在"已复制到多数派并应用"后返回 —— 协议层等它再回 confirm，
// 因此 confirm 的含义从"已落盘"变成"已在多数派落盘"，这正是仲裁队列的承诺。
func (q *queue) publishQuorum(msg *plugin.Message) (accepted bool, wait func() error, err error) {
	g := q.quorum
	if !g.isLeader() {
		return false, nil, plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 仲裁队列 '%s' 的 leader 不在本节点（当前 leader=%q），请重试",
			q.name, g.leaderNode())
	}
	body, err := g.b.encodeMessage(msg)
	if err != nil {
		return false, nil, err
	}

	// 长度限制由 leader 判定：判定结果（拒绝 / 挤掉队首）随后进入日志，
	// 副本不需要重复判定，也就不会出现"各副本各算一遍"的分歧。
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return true, nil, nil
	}
	candidate := &queuedMsg{msg: msg}
	var drop []uint64
	if q.wouldExceedLocked(candidate) {
		if q.args.overflow == overflowRejectPublish {
			q.mu.Unlock()
			return false, nil, nil
		}
		for len(q.ready) > 0 && q.wouldExceedLocked(candidate) {
			head := q.ready[0]
			q.ready = q.ready[1:]
			q.readyBytes -= messageSize(head.msg)
			drop = append(drop, head.seq)
		}
	}
	q.nextSeq++
	seq := q.nextSeq
	var expireAt int64
	if ttl := q.args.effectiveTTL(parseExpiration(msg.Properties.Expiration)); ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}
	q.mu.Unlock()

	for _, s := range drop {
		if err := g.propose(quorumCommand{Op: quorumOpAck, Seq: s}); err != nil {
			q.log.Warn("仲裁队列挤出队首失败", "queue", q.name, "seq", s, "err", err)
			break
		}
	}

	data, err := encodeQuorumCommand(quorumCommand{
		Op: quorumOpPublish, Seq: seq, Body: body, ExpireAt: expireAt,
	})
	if err != nil {
		return false, nil, plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - %v", err)
	}
	// 提案必须**立刻启动**（而不是等协议层调用 wait）：未开 confirm 的客户端没有等待者，
	// 若把提案放在 wait 里，消息永远不会被复制。
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), quorumProposeTimeout)
		defer cancel()
		_, perr := g.node.Propose(ctx, data)
		done <- perr
	}()
	return true, func() error {
		if perr := <-done; perr != nil {
			return fmt.Errorf("仲裁队列复制失败（未达到多数派）: %w", perr)
		}
		return nil
	}, nil
}

// purgeQuorum 清空队列：把"清空"写进日志，副本据此清掉各自的 ready。
//
// 返回值是**清空前**的就绪条数（与 RabbitMQ 的 queue.purge 语义一致）。
// 本地不先清：只留"应用日志"这一条路径，避免本地与副本状态不一致。
func (q *queue) purgeQuorum() uint32 {
	g := q.quorum
	q.mu.Lock()
	n := uint32(len(q.ready))
	q.mu.Unlock()
	if !g.isLeader() {
		return n
	}
	if err := g.propose(quorumCommand{Op: quorumOpPurge}); err != nil {
		q.log.Warn("仲裁队列清空失败", "queue", q.name, "err", err)
	}
	return n
}

// sweepQuorum 由 leader 驱动的消息过期。
//
// 过期是"时间"决定的，因此不能让每个副本各算一遍 —— 由 leader 判定，并把结果
// （一条 ack 命令）写进日志，副本只负责应用。
func (q *queue) sweepQuorum(now time.Time) {
	g := q.quorum
	if !g.isLeader() {
		return
	}
	q.mu.Lock()
	var expired []*queuedMsg
	for len(q.ready) > 0 {
		head := q.ready[0]
		if head.expireAt.IsZero() || head.expireAt.After(now) {
			break
		}
		q.ready = q.ready[1:]
		q.readyBytes -= messageSize(head.msg)
		expired = append(expired, head)
	}
	q.mu.Unlock()

	for _, item := range expired {
		// 配了死信就按队列配置转出去（死信路由由派发器异步完成），随后把确认写进日志。
		if q.args.hasDeadLetter() {
			q.deadLetter(item.msg, deathReasonExpired)
		}
		g.proposeAck(item.seq)
	}
}

// applyQuorumPublish 把一条已提交的发布落进队列（所有副本都执行）。
func (q *queue) applyQuorumPublish(seq uint64, msg *plugin.Message, expireAtMs int64) {
	item := &queuedMsg{msg: msg, seq: seq, priority: msg.Properties.Priority}
	if expireAtMs > 0 {
		item.expireAt = time.UnixMilli(expireAtMs)
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	if seq > q.nextSeq {
		q.nextSeq = seq
	}
	q.insertLocked(item)
	q.published.Add(1)
	needDispatch := len(q.consumers) > 0
	q.mu.Unlock()
	if needDispatch {
		// 不能在这里同步投递：Apply 跑在该组的应用协程上，向慢消费者写 socket 会拖住整组的应用。
		// dispatch 自身用 dispatching 标志防重入，并发触发是安全的。
		go q.dispatch()
	}
}

// applyQuorumAck 把一条已提交的确认落进队列：从 ready 里移除（幂等）。
func (q *queue) applyQuorumAck(seq uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, item := range q.ready {
		if item.seq == seq {
			q.ready = append(q.ready[:i], q.ready[i+1:]...)
			q.readyBytes -= messageSize(item.msg)
			return
		}
	}
}

// applyQuorumPurge 清空就绪消息（幂等）。
func (q *queue) applyQuorumPurge() {
	q.mu.Lock()
	q.ready = nil
	q.readyBytes = 0
	q.mu.Unlock()
}

// cancelConsumersWithReason 取消全部本地消费者并逐个通知协议层。
//
// 服务端主动取消（例如仲裁队列换 leader）必须走到协议层的 Cancel 回调，
// 否则客户端会以为消费者还在，静默地收不到消息。
func (q *queue) cancelConsumersWithReason(reason string) {
	q.mu.Lock()
	subs := make([]plugin.Subscription, 0, len(q.consumers))
	for _, c := range q.consumers {
		subs = append(subs, c.sub)
		q.requeueByConsumerLocked(c.sub.Tag)
	}
	q.consumers = nil
	q.mu.Unlock()

	for _, sub := range subs {
		if sub.Cancel != nil {
			sub.Cancel(reason)
		}
	}
}

// checkQuorumLeadership 在后台周期检查各仲裁队列的 leader 变化并收拾本地状态。
func (b *Broker) checkQuorumLeadership() {
	for _, v := range b.vhosts {
		v.mu.RLock()
		queues := make([]*queue, 0, len(v.queues))
		for _, q := range v.queues {
			if q.quorum != nil {
				queues = append(queues, q)
			}
		}
		v.mu.RUnlock()

		for _, q := range queues {
			g := q.quorum
			isLeader := g.isLeader()
			g.mu.Lock()
			changed := isLeader != g.wasLeader
			g.wasLeader = isLeader
			g.mu.Unlock()
			if !changed {
				continue
			}
			if isLeader {
				b.log.Info("仲裁队列已成为 leader，开始服务", "queue", q.name)
				q.dispatch()
				continue
			}
			// 失去 leader 身份：旧 leader 上的消费者不会再收到投递，必须主动取消并通知客户端；
			// 未确认的消息留在日志里，由新 leader 重新投递。
			b.log.Warn("仲裁队列失去 leader 身份，取消本地消费者", "queue", q.name)
			q.cancelConsumersWithReason("CONSUMER_CANCELLED - quorum queue leader changed")
			b.dropProxiesForQueue(v.name, q.name, "仲裁队列 leader 变更")
		}
	}
}

// stopQuorumGroups 停掉全部仲裁队列的 Raft 组（不删除数据）。进程退出时调用。
func (b *Broker) stopQuorumGroups() {
	for _, v := range b.vhosts {
		v.mu.RLock()
		groups := make([]*quorumGroup, 0)
		for _, q := range v.queues {
			if q.quorum != nil {
				groups = append(groups, q.quorum)
			}
		}
		v.mu.RUnlock()
		for _, g := range groups {
			g.stop()
		}
	}
}
