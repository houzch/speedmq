package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/houzch/speedmq/internal/meta"
	"github.com/houzch/speedmq/internal/raft"
	"github.com/houzch/speedmq/internal/store"
	"github.com/houzch/speedmq/pkg/plugin"
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
	// 合批的时间窗口与批大小上限现由配置提供（config.Quorum；见 Broker 上的 quorumAckBatchWindow 等字段）。
	// 窗口 <=0 表示关闭合批；默认值与旧常量一致（ack 10ms/1024、publish 5ms/256）。
)

// 仲裁队列的日志命令。
//
// 日志里只记"决定"，不记"过程"：长度限制怎么算、谁先谁后、是否过期，全部由 leader
// 判定后写进日志，副本只按序应用。这样状态机不依赖时间与本地状态，副本之间不会分叉。
const (
	quorumOpPublish      = "publish"
	quorumOpPublishBatch = "publish_batch"
	quorumOpAck          = "ack"
	quorumOpAckBatch     = "ack_batch"
	quorumOpPurge        = "purge"
)

// quorumPublishItem 是 publish_batch 里的一条发布。
//
// Seq 与 ExpireAt 由 leader 判定后**写死在日志里**（副本不重算，保证确定性）；
// Dropped 是"应用本条之前"leader 判定要淘汰的队首序号（按判定顺序），副本按序把它们当确认应用。
// 这样长度限制/淘汰完全由 leader 的单点判定决定，副本只复现结果，不会各算一遍而分叉。
type quorumPublishItem struct {
	// Seq 是**序号提示**：0 表示由应用侧按日志顺序分配（当前实现恒为 0）。
	// 非 0 时若与既有序号冲突也会被应用侧纠正，避免"已确认却丢消息"（B5-follow-2）。
	Seq      uint64   `json:"seq"`
	Body     []byte   `json:"body,omitempty"`
	ExpireAt int64    `json:"expire_at,omitempty"`
	Dropped  []uint64 `json:"dropped,omitempty"`
}

// quorumCommand 是一条日志命令。
type quorumCommand struct {
	Op string `json:"op"`
	// Seq 是消息在队列内的稳定序号（由 leader 分配）：发布时指定，确认时引用。
	Seq uint64 `json:"seq,omitempty"`
	// Seqs 是合批确认（ack_batch）里的多个序号，按到达顺序排列。
	//
	// 与 Seq 分开而不是复用：既有 ack 命令的线格式保持不变（兼容旧日志与单条路径）。
	Seqs []uint64 `json:"seqs,omitempty"`
	// Items 是合批发布（publish_batch）里的多条消息，按 leader 分配序号的顺序排列。
	Items []quorumPublishItem `json:"items,omitempty"`
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
	// tr 是该组在共享集群端口上的传输（方法名带组前缀），组自身还要在它上面注册
	// 一个"运行期改组"的处理器（见 serveReconfig）。
	tr groupTransport

	mu        sync.Mutex
	node      *raft.Node
	stopped   bool
	wasLeader bool

	// ack 合批（M2）：窗口内累积的确认序号，由一个一次性定时器在窗口结束时合并成
	// 一条 ack_batch 日志条目（见 proposeAck / flushAcks）。ackMu 只保护这三个字段，
	// 不与组主锁 mu 嵌套（两者各自独立获取）。
	ackMu       sync.Mutex
	pendingAcks []uint64
	ackTimer    *time.Timer

	// publish 合批（M3）：窗口内累积的发布，满批或窗口到期后合并成一条 publish_batch 日志条目。
	// pubMu 只保护这两个字段；登记在持有队列锁 q.mu 时进行（见 publishQuorum），
	// 以保证"批内顺序 == 序号顺序"（非优先级队列的就绪表是追加式的，顺序敏感）。
	pubMu    sync.Mutex
	pubBatch *pubBatch
	pubTimer *time.Timer
}

// pubBatch 是一批待提交的发布。
//
// done 在提案完成（成功或失败）时关闭，err 在此之前写入；等待方先 <-done 再读 err。
// dropped 记录本批在 leader 本地已移除的队首（提案失败时按序回插，避免 ready 与副本日志分叉）。
type pubBatch struct {
	items   []quorumPublishItem
	dropped []*queuedMsg
	done    chan struct{}
	err     error
}

// finish 结束一批发布：写入结果并唤醒全部等待方。只应被调用一次。
func (b *pubBatch) finish(err error) {
	b.err = err
	close(b.done)
}

// startQuorumGroup 启动一条仲裁队列的 Raft 组（每个节点都会为同一条队列启动自己的一份）。
//
// 组的投票成员就是该队列在元数据里记录的**副本集**（rec.Replicas）；集群里其余节点
// 以 learner 身份加入同一个组 —— learner 照常复制日志（因此扩副本不需要搬数据），
// 但不计入多数派、不参与选举。运行期的 grow 就是把目标节点提升为 voter（见 growTo）。
func (b *Broker) startQuorumGroup(v *vhost, q *queue, rec meta.Queue) error {
	id := v.name + "\x00" + q.name
	dir := filepath.Join(b.cfg.DataDir, "quorum",
		store.SafeDirName(v.name), store.SafeDirName(q.name))
	voters, learners := b.quorumGroupMembers(rec)
	g := &quorumGroup{
		b: b, id: id, dir: dir, q: q, log: b.log,
		tr: groupTransport{prefix: "quorum:" + id + ":", inner: b.cluster},
	}

	node, err := raft.New(raft.Options{
		ID:       b.nodeID,
		Peers:    voters,
		Learners: learners,
		Dir:      dir,
		// 方法名带组前缀：同一个集群端口上可以并存任意多条队列的组。
		Transport:         g.tr,
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
	if err := g.serveReconfig(); err != nil {
		node.Stop()
		return err
	}
	if err := node.Start(); err != nil {
		node.Stop()
		return err
	}
	q.quorum = g
	b.log.Debug("仲裁队列的 Raft 组已启动", "vhost", v.name, "queue", q.name,
		"voters", voters, "learners", learners, "dir", dir)
	return nil
}

// quorumGroupMembers 解析一条仲裁队列的 Raft 组成员：投票成员取元数据里的副本集，
// 集群里其余节点作为 learner。
//
// 为什么让非副本节点以 learner 身份一起复制：这样"扩副本"只是提权，不需要在运行期
// 让一个从未跑过该组、也没有任何数据的节点冷启动接入 —— 少一类容易出错的中间态。
// 副本数（voter 数）仍严格等于元数据里的 Replicas，故障容错口径因此是清晰的。
//
// 防御：本节点既不在副本集里、又不在集群成员表里时（成员表与副本集短暂不一致），
// 至少把它自己算作 learner，否则 raft.New 会因为"成员表不含本节点"直接报错。
func (b *Broker) quorumGroupMembers(rec meta.Queue) (voters, learners []string) {
	members := b.quorumClusterMembers()
	if len(rec.Replicas) == 0 {
		return members, nil
	}
	voters = append([]string(nil), rec.Replicas...)
	sort.Strings(voters)
	inReplicas := make(map[string]struct{}, len(voters))
	for _, id := range voters {
		inReplicas[id] = struct{}{}
	}
	for _, id := range members {
		if _, ok := inReplicas[id]; !ok {
			learners = append(learners, id)
		}
	}
	if !containsID(voters, b.nodeID) && !containsID(learners, b.nodeID) {
		learners = append(learners, b.nodeID)
		sort.Strings(learners)
	}
	return voters, learners
}

// quorumClusterMembers 返回仲裁队列组可以放置副本的节点（已排序）。
//
// 取 cluster.peers 地址簿：它同时定义了"谁能被联系上"，因此跨节点复制才可能成立。
// 运行期经 add_member 加入、未写进各节点配置地址簿的成员不在其列 —— 这是本期的已知边界
// （与"仲裁队列成员跟随集群成员表"的现状一致）。
func (b *Broker) quorumClusterMembers() []string {
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

// quorumReplicaSet 从集群成员里挑出 size 个节点作为副本集（已排序、确定性）。
//
// 必须确定性：副本集要写进元数据、并成为每个节点建组时的初始投票成员，
// 各节点独立算出的结果必须完全一致（否则会分叉成两个多数派）。
// 同一个元数据状态 + 同一个 size ⇒ 同一份 Replicas。
func (b *Broker) quorumReplicaSet(size int) []string {
	members := b.quorumClusterMembers()
	if size <= 0 || size > len(members) {
		size = len(members)
	}
	return append([]string(nil), members[:size]...)
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

	// 停掉 ack 合批定时器并丢弃窗口内未提交的确认：这些消息仍在日志里，重启后会重投
	// （at-least-once 不变）；此刻不再向即将停止的 node 提案。
	g.ackMu.Lock()
	g.pendingAcks = nil
	if g.ackTimer != nil {
		g.ackTimer.Stop()
		g.ackTimer = nil
	}
	g.ackMu.Unlock()

	// 停掉发布合批定时器，并让窗口内未提交的发布**显式失败**：发布有等待方（confirm），
	// 只丢弃会让它们永久挂起；这些消息尚未进入日志，调用方会收到错误并重试。
	g.pubMu.Lock()
	pub := g.pubBatch
	g.pubBatch = nil
	if g.pubTimer != nil {
		g.pubTimer.Stop()
		g.pubTimer = nil
	}
	g.pubMu.Unlock()
	if pub != nil {
		pub.finish(errors.New("仲裁队列已停止，发布未完成"))
	}

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

// proposeAck 把"这条消息已确认"记入日志（M2：合批）。
//
// 合批：确认先累积在一个短窗口内，由一次性定时器把窗口内的确认合并成**一条** ack_batch
// 日志条目（见 flushAcks），从而把"每条确认一次 Raft 提案"降到"每批一次"。
//
// 为什么可以延迟：客户端 ack 没有可见应答；窗口内节点崩溃只会让该批确认未落盘，
// 消息被重新投递（at-least-once 语义不变）。异步执行，提案失败只记日志。
func (g *quorumGroup) proposeAck(seq uint64) {
	if g.b.quorumAckBatchWindow <= 0 {
		// 关闭合批：立即单条提交（保留可配为 0 关闭的能力）。
		g.proposeAckBatch([]uint64{seq})
		return
	}
	g.ackMu.Lock()
	g.pendingAcks = append(g.pendingAcks, seq)
	if len(g.pendingAcks) >= g.b.quorumAckBatchMax {
		// 积满上限：不等窗口，立即提交（避免单条日志过大、确认延迟无谓累积）。
		g.ackMu.Unlock()
		g.flushAcks()
		return
	}
	if g.ackTimer == nil {
		// 首个确认到达时启动一次性定时器；窗口内的后续确认只是追加，不再重复启动。
		g.ackTimer = time.AfterFunc(g.b.quorumAckBatchWindow, g.flushAcks)
	}
	g.ackMu.Unlock()
}

// flushAcks 把当前累积的确认作为一条 ack_batch 命令提交并清空（定时器/满批/停止时调用）。
func (g *quorumGroup) flushAcks() {
	g.ackMu.Lock()
	seqs := g.pendingAcks
	g.pendingAcks = nil
	if g.ackTimer != nil {
		// 由定时器自身触发时 Stop 返回 false，无副作用；由满批/停止路径触发时取消待触发的定时器。
		g.ackTimer.Stop()
		g.ackTimer = nil
	}
	g.ackMu.Unlock()
	if len(seqs) == 0 {
		return
	}
	g.proposeAckBatch(seqs)
}

// proposeAckBatch 把一批确认作为**一条** ack_batch 命令异步写入日志。
//
// 提案失败只记日志 —— 消息仍留在日志里，会被重新投递（at-least-once），不会丢。
func (g *quorumGroup) proposeAckBatch(seqs []uint64) {
	data, err := encodeQuorumCommand(quorumCommand{Op: quorumOpAckBatch, Seqs: seqs})
	if err != nil {
		g.log.Error("编码仲裁队列合批确认命令失败", "queue", g.q.name, "count", len(seqs), "err", err)
		return
	}
	// 观测：批次数与确认条数（二者之比即 ack 平均批大小，见 QuorumWriteStats）。
	g.b.ackBatches.Add(1)
	g.b.ackSeqs.Add(uint64(len(seqs)))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), quorumProposeTimeout)
		defer cancel()
		if _, err := g.node.Propose(ctx, data); err != nil {
			g.log.Warn("仲裁队列的合批确认写入日志失败（消息会被重新投递）",
				"queue", g.q.name, "count", len(seqs), "err", err)
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
	case quorumOpPublishBatch:
		// 按序应用批内每条：先按 leader 记录的顺序淘汰队首，再插入本条。
		// 淘汰与序号都写死在日志里，副本只复现结果，因此不会各算一遍而分叉。
		for _, it := range cmd.Items {
			for _, dead := range it.Dropped {
				q.applyQuorumAck(dead)
			}
			msg, err := f.g.b.decodeMessage(it.Body)
			if err != nil {
				return nil, err
			}
			q.applyQuorumPublish(it.Seq, msg, it.ExpireAt)
		}
	case quorumOpAck:
		q.applyQuorumAck(cmd.Seq)
	case quorumOpAckBatch:
		// 按序应用批内每条确认。确认之间互不影响（各自从 ready 里移除一个不同序号），
		// 因此即使顺序与到达顺序不同，结果也一致；仍按序应用以求与日志顺序严格对应。
		for _, seq := range cmd.Seqs {
			q.applyQuorumAck(seq)
		}
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
	a := q.arg()
	var dropped []*queuedMsg
	if q.wouldExceedLocked(candidate) {
		if a.overflow == overflowRejectPublish {
			q.mu.Unlock()
			return false, nil, nil
		}
		for len(q.ready) > 0 && q.wouldExceedLocked(candidate) {
			head := q.ready[0]
			q.ready = q.ready[1:]
			q.readyBytes -= messageSize(head.msg)
			dropped = append(dropped, head)
		}
	}
	var expireAt int64
	if ttl := a.effectiveTTL(parseExpiration(msg.Properties.Expiration)); ttl > 0 {
		expireAt = time.Now().Add(ttl).UnixMilli()
	}
	// **不在 leader 侧分配序号**（Seq 留 0，由应用侧统一分配）：leader 的应用状态可能滞后于日志
	// （选主后尤其明显），此时按本地 nextSeq 分配会与"已提交/已入日志但尚未应用"的条目撞号，
	// 之后的确认就会删错条目 → 已确认却丢消息（B5-follow-2 根因）。序号在 apply 时按同一日志顺序
	// 分配给所有副本，既确定又永不重复。
	item := quorumPublishItem{Body: body, ExpireAt: expireAt}
	if len(dropped) > 0 {
		item.Dropped = make([]uint64, len(dropped))
		for i, head := range dropped {
			item.Dropped[i] = head.seq
		}
	}
	// 登记进发布批：在持有 q.mu 时进行，保证"批内顺序 == 序号顺序"
	// （非优先级队列的就绪表是追加式的，应用顺序必须与序号一致）。
	batch := g.enqueuePublishLocked(item, dropped)
	q.mu.Unlock()
	g.b.quorumAcceptedPublish.Add(1)

	// wait 在"整批已复制到多数派并应用"后返回 —— 协议层等它再回 confirm。
	// 同批各项共享同一份结论：批次整体提交，不存在"批内部分落盘"。
	return true, func() error {
		<-batch.done
		if batch.err != nil {
			return fmt.Errorf("仲裁队列复制失败（未达到多数派）: %w", batch.err)
		}
		return nil
	}, nil
}

// enqueuePublishLocked 把一条发布登记进当前发布批，返回该批（其 done/err 供调用方等待）。
//
// **调用方必须持有 q.mu**：序号分配与登记在同一临界区完成，才能保证批内顺序与序号顺序一致。
// 满批（或窗口关闭）时把整批封口并提交；否则由一次性定时器在窗口到期时提交。
//
// 合批只改变"何时 / 以多大粒度写日志"，不改发布语义：批次一经提交即整体落盘并应用。
func (g *quorumGroup) enqueuePublishLocked(item quorumPublishItem, dropped []*queuedMsg) *pubBatch {
	g.pubMu.Lock()
	if g.pubBatch == nil {
		g.pubBatch = &pubBatch{done: make(chan struct{})}
	}
	b := g.pubBatch
	b.items = append(b.items, item)
	b.dropped = append(b.dropped, dropped...)

	flushNow := g.b.quorumPublishBatchWindow <= 0 || len(b.items) >= g.b.quorumPublishBatchMax
	if flushNow {
		// 封口：后续登记会新建批次，本批不再接受新条目（避免"已提交的批里又冒出条目"）。
		g.pubBatch = nil
		if g.pubTimer != nil {
			g.pubTimer.Stop()
			g.pubTimer = nil
		}
	} else if g.pubTimer == nil {
		g.pubTimer = time.AfterFunc(g.b.quorumPublishBatchWindow, g.flushPubs)
	}
	g.pubMu.Unlock()

	if flushNow {
		g.proposeBatch(b)
	}
	return b
}

// flushPubs 提交并清空当前发布批（定时器到期时调用）。
func (g *quorumGroup) flushPubs() {
	g.pubMu.Lock()
	b := g.pubBatch
	g.pubBatch = nil
	if g.pubTimer != nil {
		g.pubTimer.Stop()
		g.pubTimer = nil
	}
	g.pubMu.Unlock()
	g.proposeBatch(b)
}

// proposeBatch 把一批发布作为**一条** publish_batch 命令提交（异步，不阻塞调用方）。
//
// 编码 / 提案一律在独立 goroutine 内完成：调用方可能在持有 q.mu 时调用（满批路径），
// 而失败回滚需要再取 q.mu —— 若同步执行会自锁；异步执行也顺带避免把 Raft 往返放进队列锁。
func (g *quorumGroup) proposeBatch(b *pubBatch) {
	if b == nil || len(b.items) == 0 {
		return
	}
	go func() {
		data, err := encodeQuorumCommand(quorumCommand{Op: quorumOpPublishBatch, Items: b.items})
		if err != nil {
			g.rollbackDropped(b)
			g.log.Error("编码仲裁队列合批发布命令失败", "queue", g.q.name, "count", len(b.items), "err", err)
			b.finish(err)
			return
		}
		// 观测：批次数与发布条数（二者之比即发布平均批大小）。
		g.b.publishBatches.Add(1)
		g.b.publishItems.Add(uint64(len(b.items)))

		ctx, cancel := context.WithTimeout(context.Background(), quorumProposeTimeout)
		defer cancel()
		_, perr := g.node.Propose(ctx, data)
		if perr != nil {
			// 提案失败：本批在 leader 本地已移除的队首必须**回插**（副本仍认为它们在队列里），
			// 否则 ready 与副本日志分叉；本批的发布都未应用，等待方会收到错误并重试。
			g.rollbackDropped(b)
			g.log.Warn("仲裁队列的合批发布写入日志失败（消息未被接受）",
				"queue", g.q.name, "count", len(b.items), "err", perr)
		}
		b.finish(perr)
	}()
}

// rollbackDropped 把某批在 leader 本地已移除的队首按序回插（提案失败时调用）。
func (g *quorumGroup) rollbackDropped(b *pubBatch) {
	if len(b.dropped) == 0 {
		return
	}
	g.q.mu.Lock()
	g.q.insertFrontLocked(b.dropped)
	g.q.mu.Unlock()
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
		// 与经典队列保持一致：死信必须带上 x-death 头，且要复制后再改
		// （原消息可能仍被队列引用，就地改会污染它）。死信路由由派发器异步完成。
		if q.arg().hasDeadLetter() {
			clone := *item.msg
			clone.Properties.Headers = cloneHeaders(item.msg.Properties.Headers)
			addDeathHeader(&clone, deathReasonExpired, q.name, time.Now())
			q.deadLetter(&clone, deathReasonExpired)
		}
		g.proposeAck(item.seq)
	}
}

// applyQuorumPublish 把一条已提交的发布落进队列（所有副本都执行）。
//
// seq 是日志里的**序号提示**：为 0 表示由应用侧分配（当前实现恒为 0）。无论哪种情况，应用后的序号
// 都保证严格大于既有 nextSeq —— leader 分配序号时其应用状态可能滞后于日志，直接沿用就会与
// "已提交/已入日志但尚未应用"的条目撞号，随后 applyQuorumAck 会删错条目（表现为"已确认却丢消息"，
// B5-follow-2）。在应用侧统一分配/纠正即可根除，且所有副本按同一日志顺序应用，结果是确定的。
func (q *queue) applyQuorumPublish(seq uint64, msg *plugin.Message, expireAtMs int64) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	// 撞号（提示非 0 却不大于 nextSeq）会被重编；为 0 则按序分配。
	collided := seq != 0 && seq <= q.nextSeq
	if seq == 0 || collided {
		q.nextSeq++
		seq = q.nextSeq
	} else {
		q.nextSeq = seq
	}
	item := &queuedMsg{msg: msg, seq: seq, priority: msg.Properties.Priority}
	if expireAtMs > 0 {
		item.expireAt = time.UnixMilli(expireAtMs)
	}
	q.insertLocked(item)
	q.published.Add(1)
	needDispatch := len(q.consumers) > 0
	q.mu.Unlock()
	if g := q.quorum; g != nil {
		g.b.quorumAppliedPublish.Add(1)
		if collided {
			// 告警计数：出现即说明有 leader 分配了会撞号的序号（修复前应恒为 0）。
			g.b.quorumDupSeq.Add(1)
		}
	}
	if needDispatch {
		// 不能在这里同步投递：Apply 跑在该组的应用协程上，向慢消费者写 socket 会拖住整组的应用。
		// dispatch 自身用 dispatching 标志防重入，并发触发是安全的。
		go q.dispatch()
	}
}

// applyQuorumAck 把一条已提交的确认落进队列：从 ready 里移除（幂等）。
//
// 同时做**移除原因**判定（B5-follow-2 插桩）：本节点作为 leader 时，ready 里只应存在"未投递"消息
// （投递会把条目移出 ready 进 unacked）。因此"确认命中 ready"意味着**一条从未投递的消息被删掉了**
// —— 这是数据丢失的直接证据。follower 上命中 ready 属正常（它不投递，全部消息都在 ready），不计数。
func (q *queue) applyQuorumAck(seq uint64) {
	var removedMsg *plugin.Message
	q.mu.Lock()
	for i, item := range q.ready {
		if item.seq == seq {
			q.ready = append(q.ready[:i], q.ready[i+1:]...)
			q.readyBytes -= messageSize(item.msg)
			removedMsg = item.msg
			break
		}
	}
	q.mu.Unlock()
	// 判定放在队列锁之外：q.quorum.isLeader() 会取组锁，避免与 q.mu 形成嵌套锁序。
	if removedMsg != nil && q.quorum != nil && q.quorum.isLeader() {
		q.quorum.b.quorumAckRemovedUndelivered.Add(1)
		// 按 id 记录被错删的消息样本，供与客户端 confirmed/consumed id 对账。
		q.quorum.b.recordQuorumAckRemoved(seq, removedMsg)
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
	for _, v := range b.vhostList() {
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
				// 选主后重建消费者注册：本节点此前是 follower，客户端挂在本节点的消费者
				// 是"代理到旧 leader"的；旧 leader 已无法通知取消，必须把它们改挂成本地消费者，
				// 否则客户端会静默收不到消息（见 adoptLocalProxiesAsConsumers）。
				if n := b.adoptLocalProxiesAsConsumers(v, q); n > 0 {
					b.log.Info("选主后已重建本地消费者注册", "queue", q.name, "consumers", n)
				}
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
	for _, v := range b.vhostList() {
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

// ---------------------------------------------------------------------------
// 运行期扩副本（grow）与 leader 再平衡（rebalance）
//
// 语义边界（有意写清，避免被理解成 RabbitMQ 的全量能力）：
//   - grow 只扩大副本数，**不支持缩容**（要减少副本先删队列重建）；
//   - 副本只在 cluster.peers 地址簿内的节点之间选择，且不改变已选节点的相对顺序（确定性）；
//   - 因集群里其余节点本来就是 learner（一直在复制日志），grow 只做"提权"，
//     **不需要搬数据**，因此不涉及流量控制与数据迁移窗口；
//   - rebalance 只迁移 **leader**（服务节点），不改副本集、不搬数据。
// ---------------------------------------------------------------------------

// 组内的改组 / 让位 RPC 方法名（真实方法名由 groupTransport 补上组前缀）。
const (
	quorumReconfigMethod = "reconfig"
	quorumTransferMethod = "transfer"
)

const (
	// quorumReconfigTimeout 是单次改组（含等新成员追平）的上限。
	quorumReconfigTimeout = 60 * time.Second
	// quorumTransferTimeout 是 leader 让位的上限。
	quorumTransferTimeout = 10 * time.Second
	// quorumTransferSettleTimeout 是"确认让位真正生效"的上限：高负载下目标可能竞选失败、
	// 旧 leader 回弹，需要在这个窗口内轮询并重试让位。
	quorumTransferSettleTimeout = 20 * time.Second
	// quorumTransferRetryInterval 是确认让位生效时的轮询/重试间隔。
	quorumTransferRetryInterval = 500 * time.Millisecond
	// quorumVotersWait 是"等本地组看到新投票成员"的上限（尽力而为，超时只告警）。
	quorumVotersWait = 3 * time.Second
)

type quorumReconfigRequest struct {
	// Add 是要提升为投票成员的节点 ID。
	Add []string `json:"add"`
}

type quorumReconfigResponse struct {
	OK       bool     `json:"ok"`
	Err      string   `json:"err,omitempty"`
	Voters   []string `json:"voters,omitempty"`
	Learners []string `json:"learners,omitempty"`
}

type quorumTransferRequest struct {
	Target string `json:"target"`
}

type quorumTransferResponse struct {
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// serveReconfig 在组自己的传输前缀上注册改组与让位处理器。
//
// 单机模式下 groupTransport.inner 为 nil，Serve 是空实现（没有邻居需要服务），
// 因此这里不需要分支。
func (g *quorumGroup) serveReconfig() error {
	if err := g.tr.Serve(quorumReconfigMethod, g.handleReconfig); err != nil {
		return fmt.Errorf("注册仲裁队列改组处理器失败 (%s): %w", g.id, err)
	}
	if err := g.tr.Serve(quorumTransferMethod, g.handleTransfer); err != nil {
		return fmt.Errorf("注册仲裁队列让位处理器失败 (%s): %w", g.id, err)
	}
	return nil
}

// raftNode 返回底层 Raft 节点（已停止时为 nil）。
func (g *quorumGroup) raftNode() *raft.Node {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return nil
	}
	return g.node
}

// membership 返回组当前的成员划分。
func (g *quorumGroup) membership() raft.Membership {
	node := g.raftNode()
	if node == nil {
		return raft.Membership{}
	}
	return node.Membership()
}

// awaitLeaderNode 等本节点知道组 leader（最多 quorumLeaderWait）。
//
// learner 与 follower 都是从 leader 的心跳里"学到" leader 的，声明队列后立刻在本节点
// 发起改组/让位时，这个信息可能还没到 —— 短暂等一会儿比直接甩一个"正在选主"更好。
func (g *quorumGroup) awaitLeaderNode() string {
	deadline := time.Now().Add(quorumLeaderWait)
	for {
		if leader := g.leaderNode(); leader != "" {
			return leader
		}
		if !time.Now().Before(deadline) {
			return ""
		}
		time.Sleep(quorumLeaderPoll)
	}
}

// growTo 把给定的节点提升为该组的投票成员（幂等）。
//
// 若本节点就是组 leader 直接在本地做；否则把请求转给 leader —— 只有 leader 能提交配置变更。
func (g *quorumGroup) growTo(ctx context.Context, add []string) (raft.Membership, error) {
	node := g.raftNode()
	if node == nil {
		return raft.Membership{}, errors.New("仲裁队列的 Raft 组未运行")
	}
	if len(add) == 0 {
		return node.Membership(), nil
	}
	if node.IsLeader() {
		cctx, cancel := context.WithTimeout(ctx, quorumReconfigTimeout)
		defer cancel()
		if err := g.promote(cctx, add); err != nil {
			return raft.Membership{}, err
		}
		return node.Membership(), nil
	}
	leader := g.awaitLeaderNode()
	if leader == "" {
		return raft.Membership{}, errors.New("该仲裁队列的 Raft 组当前没有 leader（可能正在选主），请稍后重试")
	}
	payload, err := json.Marshal(quorumReconfigRequest{Add: add})
	if err != nil {
		return raft.Membership{}, fmt.Errorf("编码改组请求失败: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, quorumReconfigTimeout)
	defer cancel()
	raw, err := g.tr.Call(cctx, leader, quorumReconfigMethod, payload)
	if err != nil {
		return raft.Membership{}, fmt.Errorf("把改组请求转给 leader %s 失败: %w", leader, err)
	}
	var resp quorumReconfigResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return raft.Membership{}, fmt.Errorf("解析 leader %s 的改组应答失败: %w", leader, err)
	}
	if resp.Err != "" {
		return raft.Membership{}, fmt.Errorf("leader %s 拒绝改组: %s", leader, resp.Err)
	}
	return node.Membership(), nil
}

// promote 在 leader 上把成员提升为投票成员：learner（必要时先加入）→ 等追平 → 提升。
func (g *quorumGroup) promote(ctx context.Context, ids []string) error {
	node := g.raftNode()
	if node == nil {
		return errors.New("仲裁队列的 Raft 组未运行")
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		current := node.Membership()
		if containsID(current.Voters, id) {
			continue // 已是投票成员：幂等跳过
		}
		if !containsID(current.Learners, id) {
			err := node.ChangeMembership(ctx, raft.ConfChange{Op: raft.ConfAddLearner, ID: id})
			if err != nil && !errors.Is(err, raft.ErrMemberExists) {
				return fmt.Errorf("把 %s 加入该组失败: %w", id, err)
			}
		}
		if err := node.AwaitCatchUp(ctx, id); err != nil {
			return fmt.Errorf("等待 %s 追上该组日志失败: %w", id, err)
		}
		err := node.ChangeMembership(ctx, raft.ConfChange{Op: raft.ConfPromote, ID: id})
		if err != nil && !errors.Is(err, raft.ErrNotLearner) {
			return fmt.Errorf("把 %s 提升为投票成员失败: %w", id, err)
		}
		g.log.Info("仲裁队列副本已提升为投票成员", "queue", g.q.name, "node", id)
	}
	return nil
}

// handleReconfig 是 quorumReconfigMethod 的处理器（只在 leader 上真正生效）。
func (g *quorumGroup) handleReconfig(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var req quorumReconfigRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析改组请求失败 (from=%s): %w", from, err)
	}
	if !g.isLeader() {
		return encodeQuorumResponse(quorumReconfigResponse{Err: "本节点已不是该组的 leader"})
	}
	if err := g.promote(ctx, req.Add); err != nil {
		return encodeQuorumResponse(quorumReconfigResponse{Err: err.Error()})
	}
	m := g.membership()
	return encodeQuorumResponse(quorumReconfigResponse{
		OK: true, Voters: m.Voters, Learners: m.Learners,
	})
}

// transferTo 让该组的 leader 让位给 target。
func (g *quorumGroup) transferTo(ctx context.Context, target string) error {
	node := g.raftNode()
	if node == nil {
		return errors.New("仲裁队列的 Raft 组未运行")
	}
	if node.IsLeader() {
		cctx, cancel := context.WithTimeout(ctx, quorumTransferTimeout)
		defer cancel()
		return node.TransferLeadership(cctx, target)
	}
	leader := g.awaitLeaderNode()
	if leader == "" {
		return errors.New("该仲裁队列的 Raft 组当前没有 leader（可能正在选主），请稍后重试")
	}
	payload, err := json.Marshal(quorumTransferRequest{Target: target})
	if err != nil {
		return fmt.Errorf("编码让位请求失败: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, quorumTransferTimeout)
	defer cancel()
	raw, err := g.tr.Call(cctx, leader, quorumTransferMethod, payload)
	if err != nil {
		return fmt.Errorf("把让位请求转给 leader %s 失败: %w", leader, err)
	}
	var resp quorumTransferResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("解析 leader %s 的让位应答失败: %w", leader, err)
	}
	if resp.Err != "" {
		return fmt.Errorf("leader %s 拒绝让位: %s", leader, resp.Err)
	}
	return nil
}

// handleTransfer 是 quorumTransferMethod 的处理器（只在 leader 上真正生效）。
func (g *quorumGroup) handleTransfer(ctx context.Context, from string, payload []byte) ([]byte, error) {
	var req quorumTransferRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析让位请求失败 (from=%s): %w", from, err)
	}
	if !g.isLeader() {
		return encodeQuorumResponse(quorumTransferResponse{Err: "本节点已不是该组的 leader"})
	}
	cctx, cancel := context.WithTimeout(ctx, quorumTransferTimeout)
	defer cancel()
	node := g.raftNode()
	if node == nil {
		return encodeQuorumResponse(quorumTransferResponse{Err: "仲裁队列的 Raft 组未运行"})
	}
	if err := node.TransferLeadership(cctx, req.Target); err != nil {
		return encodeQuorumResponse(quorumTransferResponse{Err: err.Error()})
	}
	return encodeQuorumResponse(quorumTransferResponse{OK: true})
}

// encodeQuorumResponse 编码组内 RPC 的应答体。
func encodeQuorumResponse(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("编码仲裁队列组应答失败: %w", err)
	}
	return raw, nil
}

// ---------------------------------------------------------------------------
// 对内核外部的接口：副本集查询 / grow / rebalance
// ---------------------------------------------------------------------------

// QuorumQueueInfo 是仲裁队列副本集的只读视图（管理 API 与 CLI 展示用）。
type QuorumQueueInfo struct {
	VHost string
	Name  string
	// Leader 是组当前的服务节点（未知为空串）。
	Leader string
	// Replicas 是元数据里记录的副本集（声明时定死、grow 时扩大）。
	Replicas []string
	// Voters / Learners 是组**当前**的投票成员与非投票成员。
	Voters   []string
	Learners []string
}

// QuorumQueueInfo 返回一条仲裁队列的副本集视图；非仲裁队列返回 false。
func (b *Broker) QuorumQueueInfo(vhost, name string) (QuorumQueueInfo, bool) {
	v, ok := b.vhostOf(vhost)
	if !ok {
		return QuorumQueueInfo{}, false
	}
	q, ok := v.getQueue(name)
	if !ok || q.quorum == nil {
		return QuorumQueueInfo{}, false
	}
	info := QuorumQueueInfo{
		VHost:    vhost,
		Name:     name,
		Leader:   q.quorum.leaderNode(),
		Replicas: b.quorumReplicasOf(vhost, name, q),
	}
	m := q.quorum.membership()
	info.Voters = append([]string{}, m.Voters...)
	info.Learners = append([]string{}, m.Learners...)
	return info, true
}

// quorumReplicasOf 返回该队列的副本集：元数据里记录的（声明意图）与组当前投票成员
// （既成事实）的**并集**。
//
// 两者最终一致，取并集是为了稳妥：元数据在 follower 上落后于组的状态时（grow 刚发起、
// 或本节点不是元数据 leader），既不能把副本数看小（会误判成"缩容"），也不能漏掉已生效的成员。
func (b *Broker) quorumReplicasOf(vhost, name string, q *queue) []string {
	var metaReplicas []string
	if rec, ok := b.metaRecord(vhost, name); ok {
		metaReplicas = rec.Replicas
	}
	var voters []string
	if q != nil && q.quorum != nil {
		voters = q.quorum.membership().Voters
	}
	return unionSorted(metaReplicas, voters)
}

// unionSorted 合并两个 ID 列表：去重并排序（副本集的表示必须稳定、可比）。
func unionSorted(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		set[id] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// metaRecord 返回某队列在元数据里的记录。
func (b *Broker) metaRecord(vhost, name string) (meta.Queue, bool) {
	if b.meta == nil {
		return meta.Queue{}, false
	}
	rec, ok := b.meta.State().Queues[meta.Key(vhost, name)]
	return rec, ok
}

// GrowQuorumQueue 把一条仲裁队列的副本数扩到 size。
//
// 对齐 `rabbitmq-queues grow` 的语义：只扩大，不缩小；副本集落进元数据，
// 因此重启后仍是 size 副本，新加入集群的节点也按这份记录建组。
func (b *Broker) GrowQuorumQueue(ctx context.Context, vhost, name string, size int) (QuorumQueueInfo, error) {
	if !b.clusterOn || b.cluster == nil {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindNotImplemented,
			"NOT_IMPLEMENTED - 单机模式没有副本可扩（仲裁队列的组就在本节点）")
	}
	if size <= 0 {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 副本数必须大于 0")
	}
	v, ok := b.vhostOf(vhost)
	if !ok {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - vhost '%s' not found", vhost)
	}
	q, ok := v.getQueue(name)
	if !ok {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, vhost)
	}
	if q.quorum == nil {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 队列 '%s' 不是仲裁队列（x-queue-type=quorum），没有副本集可扩", name)
	}
	members := b.quorumClusterMembers()
	if size > len(members) {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 目标副本数 %d 超过集群成员数 %d", size, len(members))
	}
	current := b.quorumReplicasOf(vhost, name, q)
	if size < len(current) {
		return QuorumQueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 不支持缩容（当前 %d 副本，请求 %d）：本期只实现 grow", len(current), size)
	}

	// 目标副本集：保留已有副本，按成员表顺序补齐（确定性，所有节点算出的结果一致）。
	target := append([]string(nil), current...)
	for _, id := range members {
		if len(target) >= size {
			break
		}
		if !containsID(target, id) {
			target = append(target, id)
		}
	}
	sort.Strings(target)

	// 需要真正提升的节点 = 目标副本集 − 组当前投票成员（以**组的事实**为准：
	// 元数据可能领先于组，例如上一次 grow 只写完了元数据就中断了）。
	groupVoters := q.quorum.membership().Voters
	var add []string
	for _, id := range target {
		if !containsID(groupVoters, id) {
			add = append(add, id)
		}
	}

	// 先做真正的改组（Raft 配置变更会复制给组内所有成员并各自落盘），
	// 再把"目标是 N 副本"写进元数据。顺序上"先事实、后记录"：即使元数据写入失败，
	// 组本身已经被正确扩大（重启也保持），不会留下"元数据说 3 副本、实际只有 1 个投票成员"的假象。
	if _, err := q.quorum.growTo(ctx, add); err != nil {
		return QuorumQueueInfo{}, mapQuorumOpErr(err)
	}
	if rec, ok := b.metaRecord(vhost, name); ok {
		rec.Replicas = target
		if err := b.submitMeta(meta.OpPutQueue, rec); err != nil {
			return QuorumQueueInfo{}, err
		}
		// 等**本节点**的元数据状态反映这次变更：本节点可能不是元数据 leader，
		// 不同步等待会让紧随其后的查询（与下一次 grow 的副本数判断）看到旧值。
		if err := b.awaitMeta(func() bool {
			r, ok := b.metaRecord(vhost, name)
			return ok && sameIDSet(r.Replicas, target)
		}); err != nil {
			return QuorumQueueInfo{}, err
		}
	} else {
		b.log.Warn("队列不在元数据中，副本集只落在 Raft 组自身", "vhost", vhost, "queue", name)
	}

	b.awaitQuorumVoters(q, target)
	b.log.Info("仲裁队列副本已扩大", "vhost", vhost, "queue", name,
		"from", len(current), "to", len(target), "replicas", target)
	info, _ := b.QuorumQueueInfo(vhost, name)
	return info, nil
}

// awaitQuorumVoters 等本地组看到目标投票成员集合（尽力而为，超时只告警）。
//
// 与成员变更同一理由：本节点可能不是组 leader，要等一轮复制才看到新成员。
// 不等待的话，紧随其后的查询会返回一份**旧**的成员表，运维会以为没生效。
func (b *Broker) awaitQuorumVoters(q *queue, target []string) {
	g := q.quorum
	if g == nil {
		return
	}
	deadline := time.Now().Add(quorumVotersWait)
	for {
		if sameIDSet(g.membership().Voters, target) {
			return
		}
		if !time.Now().Before(deadline) {
			b.log.Warn("仲裁队列副本集已在组内生效，但本地成员表未在超时内更新",
				"queue", q.name, "target", target)
			return
		}
		time.Sleep(metaApplyPollInterval)
	}
}

// sameIDSet 判断两个已排序的 ID 列表是否相等。
func sameIDSet(a, b []string) bool {
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

// mapQuorumOpErr 把组操作错误收敛成 *plugin.Error（内核对外错误一律是 *Error）。
func mapQuorumOpErr(err error) error {
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return pe
	}
	return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 仲裁队列副本调整失败: %v", err)
}

// RebalanceResult 是一次 rebalance 的结果（管理 API 与 CLI 展示用）。
type RebalanceResult struct {
	VHost string `json:"vhost"`
	Name  string `json:"queue"`
	// Moved 表示是否真的迁移了 leader。
	Moved bool `json:"moved"`
	// From / To 是迁移前后的 leader 节点。
	From string `json:"from"`
	To   string `json:"to"`
	// Reason 说明没迁移的原因（例如"已经均衡"）。
	Reason string `json:"reason,omitempty"`
}

// RebalanceQuorumQueue 把一条仲裁队列的 leader 从"承担 leader 最多"的节点迁到同组中较空的节点。
//
// 边界（本期有意的最小实现）：
//   - 只迁移 **leader**，不改变副本集、不搬迁数据（非副本节点本来就是 learner，一直在复制）；
//   - 目标只在**该队列自己的投票成员**里选，safety 由 Raft 的选举规则保证；
//   - 当 leader 的负载不比最空的投票成员多时不动（避免无谓换届）；
//   - 不做全局最优调度、不做流量控制。
func (b *Broker) RebalanceQuorumQueue(ctx context.Context, vhost, name string) (RebalanceResult, error) {
	if !b.clusterOn || b.cluster == nil {
		return RebalanceResult{}, plugin.Errorf(plugin.KindNotImplemented,
			"NOT_IMPLEMENTED - 单机模式没有可再平衡的副本")
	}
	v, ok := b.vhostOf(vhost)
	if !ok {
		return RebalanceResult{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - vhost '%s' not found", vhost)
	}
	q, ok := v.getQueue(name)
	if !ok {
		return RebalanceResult{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, vhost)
	}
	if q.quorum == nil {
		return RebalanceResult{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 队列 '%s' 不是仲裁队列，没有 leader 可再平衡", name)
	}
	res := RebalanceResult{VHost: vhost, Name: name}

	m := q.quorum.membership()
	leader := q.quorum.leaderNode()
	if leader == "" {
		return RebalanceResult{}, plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 该仲裁队列当前没有 leader（可能正在选主），请稍后重试")
	}
	res.From = leader
	if len(m.Voters) < 2 {
		res.Reason = "副本集只有一个投票成员，无处可迁"
		return res, nil
	}

	counts := b.quorumLeaderCounts()
	target := ""
	best := 0
	for _, id := range m.Voters {
		if id == leader {
			continue
		}
		c := counts[id]
		if target == "" || c < best {
			target, best = id, c
		}
	}
	if target == "" || counts[leader] <= best {
		res.Reason = "各投票成员承担的 leader 数已经均衡"
		return res, nil
	}
	if err := q.quorum.transferTo(ctx, target); err != nil {
		return RebalanceResult{}, mapQuorumOpErr(err)
	}
	// 确认让位真正生效后再回报 Moved。让位是异步的选举：高负载下目标可能因日志落后
	// 拿不到多数票而竞选失败，旧 leader 又依据选举超时赢回（表现为 From → 无 leader → From）。
	// 因此这里带超时轮询，一旦回弹就重试让位；始终迁不走则如实报告未迁移（而不是谎报 Moved）。
	deadline := time.Now().Add(quorumTransferSettleTimeout)
	for {
		if cur := q.quorum.leaderNode(); cur != "" && cur != leader {
			res.Moved = true
			res.To = target
			b.log.Info("仲裁队列 leader 已再平衡", "vhost", vhost, "queue", name,
				"from", res.From, "to", target, "leader_counts", counts)
			return res, nil
		}
		if !time.Now().Before(deadline) {
			res.Reason = fmt.Sprintf("让位请求已发出，但 leader %s 在 %s 内未被让出（目标可能正在选主），请稍后重试",
				leader, quorumTransferSettleTimeout)
			return res, nil
		}
		select {
		case <-ctx.Done():
			return RebalanceResult{}, mapQuorumOpErr(ctx.Err())
		case <-time.After(quorumTransferRetryInterval):
		}
		if err := q.quorum.transferTo(ctx, target); err != nil {
			b.log.Warn("仲裁队列 leader 让位重试未成功（将继续等待）",
				"vhost", vhost, "queue", name, "target", target, "err", err)
		}
	}
}

// quorumLeaderCounts 统计各节点当前承担多少条仲裁队列的 leader（本节点视角）。
//
// 同一份组 leader 信息在各节点之间最终一致（leader 由 Raft 选出），因此这个计数
// 不依赖跨节点聚合，单节点即可算出。
func (b *Broker) quorumLeaderCounts() map[string]int {
	counts := map[string]int{}
	for _, v := range b.vhostList() {
		v.mu.RLock()
		queues := make([]*queue, 0, len(v.queues))
		for _, q := range v.queues {
			if q.quorum != nil {
				queues = append(queues, q)
			}
		}
		v.mu.RUnlock()
		for _, q := range queues {
			if leader := q.quorum.leaderNode(); leader != "" {
				counts[leader]++
			}
		}
	}
	return counts
}
