// Package broker 是内核：持有 vhost、拓扑与队列，并向协议插件暴露协议无关的操作面。
//
// 分层：Broker → vhost（拓扑）→ exchange / queue（路由与消息）。
// 协议插件通过 plugin.Core.Session(vhost) 拿到某个 vhost 的操作面。
package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/houzch/speedmq/internal/auth"
	"github.com/houzch/speedmq/internal/config"
	"github.com/houzch/speedmq/internal/meta"
	"github.com/houzch/speedmq/internal/raft"
	"github.com/houzch/speedmq/internal/store"
	"github.com/houzch/speedmq/pkg/plugin"
)

// Version 是内核版本，也是管理 UI、/api/overview 与 /metrics 上显示的版本号来源。
//
// 必须与发布 tag 一致：tag 去掉 `v` 前缀后应与本常量完全相同。
// 改版本号请用 `go run ./scripts/version <新版本号>`（它会连前端、compose、文档与镜像 tag 一起改齐）。
// release 工作流里有一步 verify-version 会做这个校验，不一致直接让发布失败 ——
// 曾经发生过 tag 打到 1.0.3、而这里仍是 1.0.0 导致镜像"标签写着 1.0.3、跑起来报 1.0.0"的事故。
const Version = "1.1.05"

const (
	// deadLetterBuffer 是死信派发队列的缓冲长度。
	deadLetterBuffer = 4096
	// backgroundInterval 是 TTL / 队列过期的扫描周期。
	backgroundInterval = 100 * time.Millisecond
	// watermarkInterval 是内存/磁盘水位的检查周期。
	//
	// 水位是"秒级"概念：检查过密只会白白增加系统调用，过疏则让流控反应迟钝。
	watermarkInterval = time.Second
)

// Broker 是内核单例。
type Broker struct {
	log  *slog.Logger
	cfg  *config.Config
	auth *auth.Store
	// vhosts 是 vhost 集合；vhostsMu 保护它 —— M8-7 起 vhost 可以在运行期增删，
	// 而读侧遍布会话打开、管理面与跨节点转发。
	vhostsMu sync.RWMutex
	vhosts   map[string]*vhost
	// sessions 用于生成会话标识（独占队列归属判定用）
	sessions atomic.Uint64

	// stores 是队列消息的持久化层（M4 起真正落盘）。
	stores *store.Manager
	// flow 是资源水位闸门：阻塞生产者而不是丢弃消息。
	flow *flowGate
	// memWatermark / diskLimit 是流控阈值，可运行期调整（M5 管理面会用到）。
	memWatermark atomic.Uint64 // float64 的位表示
	diskLimit    atomic.Uint64

	// subs 是连接级通知订阅：连接打开时登记，关闭时注销。
	subsMu sync.Mutex
	subs   map[int]chan plugin.Notification
	subSeq int

	// conns 是连接登记表（键为 RabbitMQ 风格的连接名）：管理面据此列出与强制关闭连接。
	connsMu sync.RWMutex
	conns   map[string]*connEntry

	// dlxCh 是死信派发入口：队列只做非阻塞入队，由单个后台协程实际路由。
	dlxCh  chan deadLetterEntry
	cancel context.CancelFunc
	done   chan struct{}
	// bg 收纳随内核生命周期的辅助协程（当前是首次引导的账号播种）。
	// Close 必须等它们退出：否则清理过程中它们还可能去写已经关掉的元数据层。
	bg sync.WaitGroup

	// ---- 集群（M6）----

	// nodeID 是本节点标识；单机模式即主机名（元数据里作为队列 Owner 的取值来源）。
	nodeID string
	// clusterOn 表示是否以集群模式运行（决定元数据后端与分区策略是否生效）。
	clusterOn bool
	// meta 是元数据层：durable 拓扑的权威来源。单机模式也用它（ModeLocal 落 state.json），
	// 因此内核里只有"一条"拓扑变更路径，不必到处判断是否集群。
	meta *meta.Store
	// clusterPaused 在 pause_minority 且与多数派失联时为 true：暂停服务。
	clusterPaused atomic.Bool
	// policies 是本节点已知的策略集合（由元数据层在打开/变更时重建，见 policy.go）。
	// 读侧（声明队列/交换机、重算生效参数）直接取这份快照，避免每次都去筛元数据。
	policies atomic.Pointer[policySet]

	// ---- 跨节点消息转发（M6b，见 forward.go）----

	// cluster 是集群端口：与 Raft、元数据转发**共用一根连接、按方法名分发**。
	// 由本节点创建并持有（元数据层只是借用），因此关闭顺序也由这里负责。
	cluster raft.Transport
	// 仲裁队列写路径合批参数（来自 config.Quorum；窗口 <=0 表示关闭合批）。
	quorumAckBatchWindow     time.Duration
	quorumAckBatchMax        int
	quorumPublishBatchWindow time.Duration
	quorumPublishBatchMax    int

	// codec 是消息编解码器，由进程入口注入（消息属性的类型体系属于协议，内核不解释它）。
	codecMu sync.RWMutex
	codec   plugin.MessageCodec

	// fwdMu 保护下面四张表；临界区内不做 RPC 与队列操作（避免与外层锁交叉）。
	fwdMu     sync.Mutex
	fwdLocal  map[string]*localProxy   // 本节点代客户端持有的远端消费者
	fwdRemote map[string]*remoteProxy  // 远端挂在本节点队列上的代理消费者
	fwdHeld   map[uint64]*heldDelivery // 本节点持有、等待远端结算的投递
	fwdGets   map[uint64]string        // 本节点尚未结算的跨节点拉取（编号 → Owner 节点）
	fwdNextID uint64                   // fwdHeld 的编号分配器
	// fwdLastLease / fwdLastReap 是后台维护的节流时间戳。
	fwdLastLease time.Time
	fwdLastReap  time.Time
	// fwdRepointUntil 是"每个代理标签下次允许改挂的最早时刻"：改挂失败后按退避窗口重试，
	// 同时保证同一消费者不会并发/反复改挂（见 repointForwards）。
	fwdRepointUntil map[string]time.Time
	// fwdQueueOwner 是"每个队列最近一次观察到的 owner"，键为 vhost + \x00 + queue。
	// 用于把稳态的归属校正降到 O(队列数)：只有 owner 真变了才去遍历代理（O(代理数)）。
	fwdQueueOwner map[string]string

	// 转发计数（只增不减，供管理面与日志观测）。
	fwdOut        atomic.Uint64
	fwdIn         atomic.Uint64
	fwdDeliveries atomic.Uint64
	// fwdBatches 是"发布转发 RPC 次数"。与 fwdOut（消息条数）相除即平均批大小（M0.5 的观测口径）。
	fwdBatches atomic.Uint64
	// ackBatches / ackSeqs 是 ack 合批（M2）的观测计数：写入日志的 ack_batch 条目数与其中的
	// 确认条数。二者之比即 ack 的平均批大小（见 QuorumWriteStats / metrics）。
	ackBatches atomic.Uint64
	ackSeqs    atomic.Uint64
	// publishBatches / publishItems 是 publish 合批（M3）的观测计数：写入日志的 publish_batch
	// 条目数与其中的消息条数。二者之比即发布的平均批大小。
	publishBatches atomic.Uint64
	publishItems   atomic.Uint64

	// handleForwardPublish 各分支的计数（Owner 侧），用于把"转发成功的发布"与"被拒/被丢的发布"
	// 分开观测（B5-follow-2 排查：确认是否存在"静默丢弃却回成功"的路径）。
	fwdQueueMissing   atomic.Uint64 // vhost / queue 在 Owner 本地缺失（**静默丢弃候选**）
	fwdRemoteMismatch atomic.Uint64 // 本节点并非该队列的服务节点
	fwdPublishErr     atomic.Uint64 // 入队失败
	fwdDurableErr     atomic.Uint64 // 落盘/多数派等待失败
	fwdRejected       atomic.Uint64 // 长度限制拒绝
	fwdNoWait         atomic.Uint64 // 仲裁队列发布未给出落盘等待（=队列已关闭，未入队）
	// fwdPhantomOK 统计代理侧收到的"未路由且未拒绝且无错误"的转发应答 —— 这正是"静默丢弃"
	// （既没入队、又被当成功上报确认）的签名。应为 0；非 0 即还有静默成功路径（B5-follow-2 排查）。
	fwdPhantomOK atomic.Uint64

	// 仲裁队列发布的"接受 vs 应用"计数与重复序号探测（B5-follow-2 排查）：
	// 正常应满足 applied == accepted 且 dupSeq == 0。若 applied < accepted，说明有发布被接受却没落进队列；
	// 若 dupSeq > 0，说明 leader 在"已提交但尚未应用"的窗口里分配了与既有条目冲突的序号
	// （选主后 apply 滞后），随后的确认可能删错条目 —— 表现为"已确认却丢消息"。
	quorumAcceptedPublish atomic.Uint64
	quorumAppliedPublish  atomic.Uint64
	quorumDupSeq          atomic.Uint64
	// quorumAckRemovedUndelivered 统计"确认命中 ready"的次数（仅在本节点作为 leader 时计入）。
	// leader 的 ready 只含未投递消息，因此该值应恒为 0；非 0 即"从未投递的消息被确认删掉"= 丢消息。
	quorumAckRemovedUndelivered atomic.Uint64
	// 按 id 追踪被移除原因（B5-follow-2 插桩）：记录被"确认命中 ready"删掉的消息样本。id 取消息体
	// 前 8 字节的大端整数（测试客户端的 check-loss 约定），seq 为队列内序号。用于与客户端的
	// confirmed/consumed id 集合直接对账 —— 命中即"被错删"，未命中即"根本不是这条路径丢的"。
	quorumAckRemovedMu   sync.Mutex
	quorumAckRemovedIDs  []uint64
	quorumAckRemovedSeqs []uint64
	// 转发边界按 id 追踪（B5-follow-2 插桩）：把"代理侧判定转发成功/失败"与"Owner 侧实际接受/出错"
	// 的消息 id 各留一份样本，用于判定"每转发批丢 1 条且拿到成功应答"发生在边界哪一侧。id 取消息体
	// 前 8 字节大端整数（check-loss 约定）。
	//
	// 四组集合**一律用固定大小的尾窗**（只留最近若干条，容量恒定）；写入一旦发生覆盖即由
	// `*_truncated` 标记显式暴露，避免"样本不全"被当成"完整样本"做对账。
	//
	// 为什么失败集合也要设上限（R1 实测）：持续失败下坏集合会**快速无界增长**——测得的极端速率约
	// 4.3 万条/s（≈0.34 MB/s ⇒ 1.2 GB/h），足以在数小时内把节点内存吃光。诊断样本不能反过来成为
	// 压垮 broker 的原因，因此改为有界尾窗。
	fwdTraceMu     sync.Mutex
	fwdSentOKRing  fwdIDRing
	fwdSentBadRing fwdIDRing
	fwdInRing      fwdIDRing
	fwdInBadRing   fwdIDRing
	// fwdPipe 是发布转发流水线的待处理队列（见 forward.go 的 forwardPublishAsync / runForwardPipeline）。
	fwdPipe chan *fwdPending
}

// fwdTraceOKWindow / fwdTraceBadWindow 分别是"成功 / 失败"id 尾窗的容量。
// 成功集合按本轮转发量取 65536；失败集合取更大的 262144（≈2 MiB/集合）以尽量容纳一次故障窗口的失败批次。
const (
	fwdTraceOKWindow  = 65536
	fwdTraceBadWindow = 262144
)

// fwdIDRing 是固定容量的 id 尾窗环形缓冲：容量满后覆盖最旧一条，用 truncated 标记是否发生过覆盖。
// 之所以不是简单切片 + cap：切片达到上限后 "静默丢弃后续" 会让对账把"样本不全"当成"完整样本"；
// 尾窗 + 显式截断标记能在恒定内存下保留**最近**（最相关）的样本，并把不确定性暴露出来。
type fwdIDRing struct {
	buf  []uint64
	next int
	tot  uint64
}

// newFwdIDRing 构造容量为 n 的尾窗。
func newFwdIDRing(n int) fwdIDRing {
	if n < 0 {
		n = 0
	}
	return fwdIDRing{buf: make([]uint64, n)}
}

// add 写入一条 id；容量满后覆盖最旧一条。
func (r *fwdIDRing) add(id uint64) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.next] = id
	r.next++
	if r.next == len(r.buf) {
		r.next = 0
	}
	r.tot++
}

// snapshot 返回尾窗内的 id 副本（顺序与对账无关，是按集合使用的）。
func (r *fwdIDRing) snapshot() []uint64 {
	if len(r.buf) == 0 || r.tot == 0 {
		return nil
	}
	if r.tot < uint64(len(r.buf)) {
		return append([]uint64(nil), r.buf[:r.next]...)
	}
	out := make([]uint64, 0, len(r.buf))
	out = append(out, r.buf[r.next:]...)
	out = append(out, r.buf[:r.next]...)
	return out
}

// truncated 表示尾窗是否发生过覆盖（即样本已不代表全量）。
func (r *fwdIDRing) truncated() bool { return r.tot > uint64(len(r.buf)) }

// messageID 从消息体前 8 字节解出测试客户端的 check-loss 消息 id（非该约定的消息返回 0）。
func messageID(body []byte) uint64 {
	if len(body) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(body[:8])
}

// traceFwdSent 记录代理侧一条转发的判定结果（成功/失败）。
func (b *Broker) traceFwdSent(id uint64, ok bool) {
	if id == 0 {
		return
	}
	b.fwdTraceMu.Lock()
	if ok {
		b.fwdSentOKRing.add(id)
	} else {
		b.fwdSentBadRing.add(id)
	}
	b.fwdTraceMu.Unlock()
}

// traceFwdIn 记录 Owner 侧一条转发的入队结果（接受/失败）。
func (b *Broker) traceFwdIn(id uint64, ok bool) {
	if id == 0 {
		return
	}
	b.fwdTraceMu.Lock()
	if ok {
		b.fwdInRing.add(id)
	} else {
		b.fwdInBadRing.add(id)
	}
	b.fwdTraceMu.Unlock()
}

// ForwardSentIDs 返回代理侧"转发成功/失败"的 id 样本副本与两组集合是否被截断。
func (b *Broker) ForwardSentIDs() (ok, bad []uint64, okTruncated, badTruncated bool) {
	b.fwdTraceMu.Lock()
	defer b.fwdTraceMu.Unlock()
	ok = b.fwdSentOKRing.snapshot()
	okTruncated = b.fwdSentOKRing.truncated()
	bad = b.fwdSentBadRing.snapshot()
	badTruncated = b.fwdSentBadRing.truncated()
	return ok, bad, okTruncated, badTruncated
}

// ForwardInIDs 返回 Owner 侧"接受入队/失败"的 id 样本副本与两组集合是否被截断。
func (b *Broker) ForwardInIDs() (ok, bad []uint64, okTruncated, badTruncated bool) {
	b.fwdTraceMu.Lock()
	defer b.fwdTraceMu.Unlock()
	ok = b.fwdInRing.snapshot()
	okTruncated = b.fwdInRing.truncated()
	bad = b.fwdInBadRing.snapshot()
	badTruncated = b.fwdInBadRing.truncated()
	return ok, bad, okTruncated, badTruncated
}

// quorumAckRemovedSampleMax 是"按 id 追踪移除原因"样本的上限，避免异常路径下无界增长。
const quorumAckRemovedSampleMax = 128

// recordQuorumAckRemoved 记录一条"确认命中 ready（从未投递即被删）"的消息样本：id + 序号。
func (b *Broker) recordQuorumAckRemoved(seq uint64, msg *plugin.Message) {
	var id uint64
	if msg != nil && len(msg.Body) >= 8 {
		id = binary.BigEndian.Uint64(msg.Body[:8])
	}
	b.quorumAckRemovedMu.Lock()
	if len(b.quorumAckRemovedIDs) < quorumAckRemovedSampleMax {
		b.quorumAckRemovedIDs = append(b.quorumAckRemovedIDs, id)
		b.quorumAckRemovedSeqs = append(b.quorumAckRemovedSeqs, seq)
	}
	b.quorumAckRemovedMu.Unlock()
}

// QuorumAckRemovedIDs 返回被"确认命中 ready"删掉的消息样本（id 与序号的浅拷贝副本）。
func (b *Broker) QuorumAckRemovedIDs() ([]uint64, []uint64) {
	b.quorumAckRemovedMu.Lock()
	defer b.quorumAckRemovedMu.Unlock()
	ids := make([]uint64, len(b.quorumAckRemovedIDs))
	copy(ids, b.quorumAckRemovedIDs)
	seqs := make([]uint64, len(b.quorumAckRemovedSeqs))
	copy(seqs, b.quorumAckRemovedSeqs)
	return ids, seqs
}

// New 构造内核。返回 error 是因为集群模式下元数据层可能启动失败
// （端口占用、日志损坏、成员表配置错误），此时必须让进程明确启动失败而不是带病运行。
func New(log *slog.Logger, cfg *config.Config) (*Broker, error) {
	log = log.With("component", "broker")

	names := make([]string, 0, len(cfg.VHosts)+1)
	names = append(names, cfg.VHosts...)
	// 默认 vhost 一定存在，避免配置里漏写导致所有客户端连不上
	names = append(names, cfg.DefaultVHost)

	b := &Broker{
		log:                      log,
		cfg:                      cfg,
		auth:                     auth.NewStore(cfg.Users),
		vhosts:                   map[string]*vhost{},
		stores:                   store.NewManager(cfg.DataDir, storageOptions(cfg), log),
		flow:                     newFlowGate(),
		subs:                     map[int]chan plugin.Notification{},
		conns:                    map[string]*connEntry{},
		dlxCh:                    make(chan deadLetterEntry, deadLetterBuffer),
		fwdPipe:                  make(chan *fwdPending, fwdPipeDepth),
		fwdSentOKRing:            newFwdIDRing(fwdTraceOKWindow),
		fwdSentBadRing:           newFwdIDRing(fwdTraceBadWindow),
		fwdInRing:                newFwdIDRing(fwdTraceOKWindow),
		fwdInBadRing:             newFwdIDRing(fwdTraceBadWindow),
		done:                     make(chan struct{}),
		nodeID:                   cfg.Cluster.NodeID,
		clusterOn:                cfg.Cluster.Enabled,
		fwdLocal:                 map[string]*localProxy{},
		fwdRemote:                map[string]*remoteProxy{},
		fwdHeld:                  map[uint64]*heldDelivery{},
		fwdGets:                  map[uint64]string{},
		fwdRepointUntil:          map[string]time.Time{},
		fwdQueueOwner:            map[string]string{},
		quorumAckBatchWindow:     time.Duration(cfg.Quorum.AckBatchWindowMS) * time.Millisecond,
		quorumAckBatchMax:        cfg.Quorum.AckBatchMax,
		quorumPublishBatchWindow: time.Duration(cfg.Quorum.PublishBatchWindowMS) * time.Millisecond,
		quorumPublishBatchMax:    cfg.Quorum.PublishBatchMax,
	}
	b.memWatermark.Store(math.Float64bits(cfg.Storage.MemoryHighWatermark))
	b.diskLimit.Store(cfg.Storage.DiskFreeLimit)
	for _, name := range names {
		if _, dup := b.vhosts[name]; dup {
			continue
		}
		b.vhosts[name] = newVHost(b, name, log, b.stores, b.dlxCh)
	}

	// 元数据层必须在 vhost 建好之后打开：打开过程会把已有拓扑回调给内核，
	// 回调需要落在已经存在的 vhost 上。配置里的 vhost 只是**首次引导**的种子，
	// 打开元数据时会按元数据把集合对齐（见 RestoreMeta / syncVHostsFromMeta）。
	if err := b.openMeta(); err != nil {
		b.stores.CloseAll()
		return nil, fmt.Errorf("打开元数据层失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.bg.Add(1)
	go func() {
		defer b.bg.Done()
		// vhost 先于账号播种：权限记录引用的 vhost 得先存在。
		b.bootstrapVHosts(ctx)
		b.bootstrapUsers(ctx)
	}()
	go b.background(ctx)
	// 发布转发流水线（M0.5）：与内核同生命周期。放在这里（而非按需惰性启动）是为了
	// 避免与 Close 里的 bg.Wait() 竞争（WaitGroup 不允许 Wait 与 Add 并发）。
	b.bg.Add(1)
	go func() {
		defer b.bg.Done()
		b.runForwardPipeline(ctx)
	}()
	return b, nil
}

// vhostOf 按名字取 vhost（并发安全）。
func (b *Broker) vhostOf(name string) (*vhost, bool) {
	b.vhostsMu.RLock()
	defer b.vhostsMu.RUnlock()
	v, ok := b.vhosts[name]
	return v, ok
}

// vhostList 返回全部 vhost 的快照切片（调用方不需要持锁）。
func (b *Broker) vhostList() []*vhost {
	b.vhostsMu.RLock()
	defer b.vhostsMu.RUnlock()
	out := make([]*vhost, 0, len(b.vhosts))
	for _, v := range b.vhosts {
		out = append(out, v)
	}
	return out
}

// addVHost 创建并登记一个 vhost；已存在时返回已有的那个（幂等）。
func (b *Broker) addVHost(name string) (*vhost, bool) {
	b.vhostsMu.Lock()
	defer b.vhostsMu.Unlock()
	if v, ok := b.vhosts[name]; ok {
		return v, false
	}
	v := newVHost(b, name, b.log, b.stores, b.dlxCh)
	b.vhosts[name] = v
	return v, true
}

// dropVHost 从集合里摘掉一个 vhost（已关闭的对象由调用方负责回收）。
func (b *Broker) dropVHost(name string) *vhost {
	b.vhostsMu.Lock()
	defer b.vhostsMu.Unlock()
	v, ok := b.vhosts[name]
	if !ok {
		return nil
	}
	delete(b.vhosts, name)
	return v
}

// storageOptions 把配置翻译成存储层选项。
func storageOptions(cfg *config.Config) store.Options {
	level, err := store.ParseFsync(cfg.Storage.Fsync)
	if err != nil {
		// 配置加载阶段已校验过，这里只做兜底：宁可按最接近的 os 跑，也不要静默变 always
		level = store.FsyncOS
	}
	return store.Options{
		Fsync:         level,
		FlushInterval: time.Duration(cfg.Storage.FlushIntervalMS) * time.Millisecond,
	}
}

// Close 停止后台协程（TTL 扫描、死信派发与水位检查）并收尾刷盘。可重复调用。
func (b *Broker) Close() {
	if b.cancel == nil {
		return
	}
	b.cancel()
	b.cancel = nil
	<-b.done
	// 辅助协程（账号播种）可能在重试等待中：等它退出再关元数据层，否则它会写到一个已关闭的存储上。
	b.bg.Wait()
	// 先停元数据层（集群模式会停 Raft），再停各仲裁队列的 Raft 组，
	// 最后关集群端口与队列存储：端口是它们共用的，必须最后关。
	if b.meta != nil {
		if err := b.meta.Close(); err != nil {
			b.log.Warn("关闭元数据层失败", "err", err)
		}
	}
	b.stopQuorumGroups()
	// 集群端口归本层所有（元数据层只是借用），因此由这里关闭。
	if b.cluster != nil {
		if err := b.cluster.Close(); err != nil {
			b.log.Warn("关闭集群端口失败", "err", err)
		}
		b.cluster = nil
	}
	// 收尾刷盘：把内存缓冲中的消息与 ack 记录落盘，否则优雅退出也会丢消息
	b.stores.CloseAll()
}

// background 是内核唯一的维护协程：周期性扫描过期消息，并派发死信。
//
// 用"单协程集中扫描"而不是"每队列一个定时器"：TTL 与死信的时效性是秒级概念，
// 100ms 的粒度足够，同时避免了每队列一个定时器的资源开销。
// 若将来需要更高精度或队列规模极大，再替换为按到期时间排序的时间轮。
func (b *Broker) background(ctx context.Context) {
	defer close(b.done)
	ticker := time.NewTicker(backgroundInterval)
	defer ticker.Stop()
	watermarks := time.NewTicker(watermarkInterval)
	defer watermarks.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-b.dlxCh:
			b.dispatchDeadLetter(e)
		case <-watermarks.C:
			b.checkWatermarks()
			b.checkCluster()
			b.maintainForwards(time.Now())
			b.checkQuorumLeadership()
		case <-ticker.C:
			now := time.Now()
			for _, v := range b.vhostList() {
				v.sweep(now)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 资源水位与流控（内存 / 磁盘 → 阻塞生产者 + Connection.Blocked）
// ---------------------------------------------------------------------------

// flowGate 是资源水位闸门。
//
// 水位触发时"阻塞生产者"而不是"丢弃消息"：丢弃会让业务在不知情的情况下丢数据，
// 阻塞则把压力交还给客户端（读循环停读 → TCP 背压），与 RabbitMQ 的做法一致。
type flowGate struct {
	// blocked 用**原子变量**而不是放进互斥锁里：
	// 每条消息发布前都要问一次"现在能不能发"，把锁竞争强加到热路径上是不必要的开销。
	// 只有真正处于阻塞状态（罕见）时才需要拿锁去取等待通道。
	blocked atomic.Bool

	mu sync.Mutex
	// ch 在 blocked 为 true 时有效，解除阻塞时被关闭。
	ch chan struct{}
}

func newFlowGate() *flowGate { return &flowGate{ch: make(chan struct{})} }

func (g *flowGate) isBlocked() bool { return g.blocked.Load() }

// set 更新阻塞状态，返回状态是否发生了变化。
func (g *flowGate) set(blocked bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blocked.Load() == blocked {
		return false
	}
	if blocked {
		// 先备好新的等待通道再置位：这样任何"看到 blocked=true 的等待者"
		// 都能拿到与之配套的通道（它在锁内读 ch，必然读到已换好的这个）。
		g.ch = make(chan struct{})
		g.blocked.Store(true)
	} else {
		// 先清位再关闭：等待者要么在 select 上被唤醒后重新检查并返回，
		// 要么在检查时已看到未阻塞而直接返回，两种情况都不会漏唤醒。
		g.blocked.Store(false)
		close(g.ch)
	}
	return true
}

// wait 在阻塞期间等待，解除或内核停止时返回。未阻塞时是一次原子读，无锁。
func (g *flowGate) wait(stop <-chan struct{}) error {
	for {
		if !g.blocked.Load() {
			return nil
		}
		g.mu.Lock()
		ch := g.ch
		g.mu.Unlock()

		select {
		case <-ch:
		case <-stop:
			return fmt.Errorf("内核正在停止")
		}
	}
}

// UpdateLimits 更新流控阈值并立即重新评估（M5 管理面与测试使用）。
func (b *Broker) UpdateLimits(memoryHighWatermark float64, diskFreeLimit uint64) {
	b.memWatermark.Store(math.Float64bits(memoryHighWatermark))
	b.diskLimit.Store(diskFreeLimit)
	b.checkWatermarks()
}

// checkWatermarks 重新评估资源水位，必要时切换阻塞状态并广播通知。
func (b *Broker) checkWatermarks() {
	blocked, reason := b.watermarkState()
	if !b.flow.set(blocked) {
		return
	}
	if blocked {
		b.log.Warn("资源水位触及上限，已阻塞生产者", "reason", reason)
	} else {
		b.log.Info("资源水位已恢复，生产者阻塞解除")
	}
	b.broadcastFlow(blocked, reason)
}

// watermarkState 返回当前是否应阻塞生产者及原因。
//
// 内存水位对齐 RabbitMQ 的语义：比较的是**本进程占用**与"水位比例 × 物理内存"，
// 而不是整机内存占用率 —— 后者会让一台内存偏紧的机器上一启动就被阻塞。
func (b *Broker) watermarkState() (bool, string) {
	if limit := b.diskLimit.Load(); limit > 0 {
		free, err := store.DiskFree(b.cfg.DataDir)
		if err == nil && free < limit {
			return true, fmt.Sprintf("磁盘剩余空间不足：剩余 %d 字节，下限 %d 字节", free, limit)
		}
	}
	if wm := math.Float64frombits(b.memWatermark.Load()); wm > 0 {
		if total, ok := store.TotalMemory(); ok && total > 0 {
			used := processMemory()
			if float64(used) > wm*float64(total) {
				return true, fmt.Sprintf("内存水位超限：进程占用 %d 字节，阈值 %.0f%% × %d 字节",
					used, wm*100, total)
			}
		}
	}
	return false, ""
}

// processMemory 返回本进程**正在使用**的内存字节数。
//
// 刻意用 HeapInuse + StackInuse，而**不是** MemStats.Sys：Sys 是"向操作系统申请过的
// 地址空间"的高水位，Go 运行期几乎不会及时归还（现代 Linux 上归还用 MADV_FREE，
// 页面在真正受压前仍然计入常驻内存）。于是它一旦越过水位阈值就再也降不回来 ——
// 闸门再也解除不了，生产者被**永久**阻塞，broker 对新生产者不可用，只能重启。
//
// 这是 M8-10 的 Linux 压测实测到的"水位锁存"：进程占用 7.68 GB > 阈值 6.67 GB 之后，
// 即使消费者把消息全部消费完（实测队列已空）、堆早已空下来，`connection.blocked` 也不会解除。
//
// 改用"在用"内存后语义不变（仍然是"本进程占用 vs 水位比例 × 物理内存"），
// 但水位能随内存回收真正回落 —— 闸门因此是**可恢复**的。
// 注意：若业务确实长期持有超过上限的数据（队列里真的有那么多消息），闸门保持阻塞是正确行为。
func processMemory() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse + ms.StackInuse
}

// ---------------------------------------------------------------------------
// 连接级通知
// ---------------------------------------------------------------------------

// subscribeNotifications 登记一个连接的通知通道；阻塞中接入的连接会立刻收到当前状态。
func (b *Broker) subscribeNotifications() (int, chan plugin.Notification) {
	ch := make(chan plugin.Notification, 4)
	// 先登记订阅、再读当前状态：反过来会出现"读完状态(未阻塞) → 水位切到阻塞并广播 → 才登记"
	// 的窗口，该连接就永远漏掉 Connection.Blocked。
	b.subsMu.Lock()
	b.subSeq++
	id := b.subSeq
	b.subs[id] = ch
	b.subsMu.Unlock()

	if b.flow.isBlocked() {
		select {
		case ch <- plugin.Notification{Blocked: true, Reason: "资源水位超限"}:
		default:
		}
	}
	return id, ch
}

func (b *Broker) unsubscribeNotifications(id int) {
	b.subsMu.Lock()
	delete(b.subs, id)
	b.subsMu.Unlock()
}

// broadcastFlow 向所有连接广播阻塞状态变化。发送一律非阻塞：通知不能拖住内核。
func (b *Broker) broadcastFlow(blocked bool, reason string) {
	n := plugin.Notification{Blocked: blocked, Reason: reason}
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- n:
		default:
		}
	}
}

// dispatchDeadLetter 把一条死信交给所属 vhost 路由。
func (b *Broker) dispatchDeadLetter(e deadLetterEntry) {
	v, ok := b.vhostOf(e.vhost)
	if !ok {
		return
	}
	v.dispatchDeadLetter(e)
}

// NewSession 为一条新连接创建协议无关的操作面。
func (b *Broker) NewSession(remote, local net.Addr) plugin.Core {
	id := fmt.Sprintf("conn-%d", b.sessions.Add(1))
	subID, notify := b.subscribeNotifications()
	entry := b.registerConn(id, remote, local)
	return &session{
		broker:   b,
		id:       id,
		log:      b.log.With("remote", remote.String(), "session", id),
		entry:    entry,
		notifyID: subID,
		notify:   notify,
	}
}

// session 是 plugin.Core 的实现，绑定单条连接。
type session struct {
	broker *Broker
	id     string
	log    *slog.Logger
	// user 是认证通过的用户名（权限校验用）。
	// 它在握手阶段写入、之后只读，因此无需加锁。
	user string
	// vh 在打开 vhost 后创建；缓存以避免重复调用 Session 时丢失独占队列等会话状态
	vh *vhostSession
	// entry 是管理面的连接登记项（连接名、快照回调、强制断开回调）。
	entry *connEntry

	notifyID  int
	notify    chan plugin.Notification
	closeOnce sync.Once
}

var _ plugin.Core = (*session)(nil)

// Logger 返回连接级日志器。
func (s *session) Logger() *slog.Logger { return s.log }

// Notifications 返回连接级事件通道。
func (s *session) Notifications() <-chan plugin.Notification { return s.notify }

// Close 注销连接级通知订阅与连接登记。可重复调用。
func (s *session) Close() {
	s.closeOnce.Do(func() {
		s.broker.unsubscribeNotifications(s.notifyID)
		s.broker.unregisterConn(s.entry)
	})
}

// SetConnectionProbe 记录协议层注入的连接/通道快照回调（管理面按需拉取）。
func (s *session) SetConnectionProbe(probe func() plugin.ConnectionInfo) {
	s.broker.setConnHooks(s.entry, probe, nil)
}

// SetDisconnectFunc 记录协议层注入的强制断开回调。
func (s *session) SetDisconnectFunc(fn func(reason string)) {
	s.broker.setConnHooks(s.entry, nil, fn)
}

// ServerProperties 返回 Connection.Start 下发的 server-properties。
//
// capabilities 声明即承诺：客户端会依据它切换代码路径，
// 因此只有真正实现的能力才允许置 true —— 声明了却没实现，比不声明更糟。
func (s *session) ServerProperties() map[string]any {
	// 被特性开关管着的能力要**同步收敛**：判定点拦下了操作，这里就必须把它声明为 false，
	// 否则客户端看到 true 会继续走那条命令，然后收到一堆意料之外的错误。
	exchangeBindings := s.broker.featureEnabled(flagExchangeExchangeBindings)
	return map[string]any{
		"product":     "SpeedMQ",
		"version":     Version,
		"platform":    "Go",
		"information": "https://github.com/houzch/speedmq",
		"capabilities": map[string]any{
			// 认证失败时用 Connection.Close 明确告知原因，而不是直接断开连接
			"authentication_failure_close": true,

			// ---- M2 已实现并声明 ----
			// 交换机间绑定：Exchange.Bind/Unbind 参与真实路由
			"exchange_exchange_bindings": exchangeBindings,
			// basic.nack：批量拒绝并可重新入队
			"basic.nack": true,
			// 消费者取消通知：队列被删除时服务端主动下发 basic.cancel
			"consumer_cancel_notify": true,
			// 每个消费者独立的 prefetch 额度
			"per_consumer_qos": true,

			// ---- M3 已实现并声明 ----
			// 发布者确认：confirm.select 后每条发布都回 basic.ack / basic.nack
			"publisher_confirms": true,

			// ---- M4 已实现并声明 ----
			// 内存/磁盘水位触发时向连接下发 Connection.Blocked / Unblocked，并阻塞生产者
			"connection.blocked": true,

			// ---- M8-14 已实现并声明 ----
			// 消费者优先级：basic.consume 的 x-priority 决定"有额度时谁先拿消息"（同优先级仍轮询）
			"consumer_priorities": true,
			// direct reply-to：amq.rabbitmq.reply-to 伪队列（每 channel 一份，必须 no-ack 消费）
			"direct_reply_to": true,
		},
	}
}

// Mechanisms 返回支持的 SASL 机制。
func (s *session) Mechanisms() []string { return s.broker.auth.Mechanisms() }

// Authenticate 校验 SASL 响应。
func (s *session) Authenticate(_ context.Context, mechanism string, response []byte, remote net.Addr) (plugin.Identity, error) {
	user, err := s.broker.auth.Authenticate(mechanism, response, remote)
	if err != nil {
		return plugin.Identity{}, err
	}
	s.user = user
	s.broker.setConnUser(s.entry, user)
	return plugin.Identity{User: user}, nil
}

// VHostExists 判断 vhost 是否存在。
func (s *session) VHostExists(name string) bool {
	_, ok := s.broker.vhostOf(name)
	return ok
}

// DefaultVHost 返回默认 vhost 名。
func (s *session) DefaultVHost() string { return s.broker.cfg.DefaultVHost }

// Session 返回绑定到指定 vhost 的操作面。
func (s *session) Session(vhostName string) (plugin.Session, error) {
	vh, ok := s.broker.vhostOf(vhostName)
	if !ok {
		return nil, plugin.Errorf(plugin.KindInvalidPath,
			"NOT_ALLOWED - vhost %s not found", vhostName)
	}
	if s.vh == nil {
		// vhost 级连接上限：只在连接**首次**打开这个 vhost 时校验。
		// 重复调用 Session() 是同一个连接在复用会话，不该自己把自己顶掉。
		if err := s.broker.checkConnectionLimit(vhostName); err != nil {
			return nil, err
		}
		perm, err := compilePermission(s.broker.auth, s.user, vhostName)
		if err != nil {
			return nil, err
		}
		s.vh = newVHostSession(vh, s.id, s.user, perm, s.log, s.broker.waitPublishGate)
	}
	s.broker.setConnVHost(s.entry, vhostName)
	return s.vh, nil
}

// waitPublishGate 在资源水位阻塞期间挂起发布；内核停止时返回错误以结束等待。
func (b *Broker) waitPublishGate() error { return b.flow.wait(b.done) }

// compilePermission 取出用户在该 vhost 上的权限并预编译正则。
//
// 无权限记录即拒绝（与 RabbitMQ 一致）：vhost 的访问权与 vhost 内的操作权都由此表决定。
//
// 例外：administrator 标签的用户对所有 vhost 拥有完全权限，**不需要**权限记录 ——
// RabbitMQ 的 rabbit_access_control 在标签为 administrator 时直接放行（实测：给一个新
// vhost 走 management API 发布消息返回 200，且 /api/permissions/{vhost}/{user} 会报出
// 一条隐式的 ".*" 记录）。M8-7 引入动态 vhost 后这一点变得可观察：新建的 vhost 上不会有
// 任何权限记录，若这里不放行，管理员刚建好的 vhost 自己都连不上。
// 管理面早已按同一口径把 administrator 视为"可见全部 vhost"（management.authenticate），
// 数据面必须与它一致，否则两层会各说各话。
func compilePermission(store *auth.Store, user, vhost string) (*permissionSet, error) {
	if u, ok := store.User(user); ok && hasTag(u.Tags, adminTag) {
		return newPermissionSet(config.Permission{Configure: ".*", Write: ".*", Read: ".*"})
	}
	p, ok := store.Permissions(user, vhost)
	if !ok {
		return nil, plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - access to vhost '%s' refused for user '%s'", vhost, user)
	}
	set, err := newPermissionSet(p)
	if err != nil {
		// 配置里的正则写错属于部署错误，直接拒绝而不是放行
		return nil, plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - invalid permission pattern for user '%s' on vhost '%s': %v", user, vhost, err)
	}
	return set, nil
}
