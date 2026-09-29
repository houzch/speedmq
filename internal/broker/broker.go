// Package broker 是内核：持有 vhost、拓扑与队列，并向协议插件暴露协议无关的操作面。
//
// 分层：Broker → vhost（拓扑）→ exchange / queue（路由与消息）。
// 协议插件通过 plugin.Core.Session(vhost) 拿到某个 vhost 的操作面。
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/houzch/swiftmq/internal/auth"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/internal/raft"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// Version 是内核版本。
const Version = "0.9.0"

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
	log    *slog.Logger
	cfg    *config.Config
	auth   *auth.Store
	vhosts map[string]*vhost
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

	// ---- 跨节点消息转发（M6b，见 forward.go）----

	// cluster 是集群端口：与 Raft、元数据转发**共用一根连接、按方法名分发**。
	// 由本节点创建并持有（元数据层只是借用），因此关闭顺序也由这里负责。
	cluster raft.Transport

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

	// 转发计数（只增不减，供管理面与日志观测）。
	fwdOut        atomic.Uint64
	fwdIn         atomic.Uint64
	fwdDeliveries atomic.Uint64
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
		log:       log,
		cfg:       cfg,
		auth:      auth.NewStore(cfg.Users),
		vhosts:    map[string]*vhost{},
		stores:    store.NewManager(cfg.DataDir, storageOptions(cfg), log),
		flow:      newFlowGate(),
		subs:      map[int]chan plugin.Notification{},
		conns:     map[string]*connEntry{},
		dlxCh:     make(chan deadLetterEntry, deadLetterBuffer),
		done:      make(chan struct{}),
		nodeID:    cfg.Cluster.NodeID,
		clusterOn: cfg.Cluster.Enabled,
		fwdLocal:  map[string]*localProxy{},
		fwdRemote: map[string]*remoteProxy{},
		fwdHeld:   map[uint64]*heldDelivery{},
		fwdGets:   map[uint64]string{},
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
	// 回调需要落在已经存在的 vhost 上（vhost 集合由配置驱动，见 cluster.go）。
	if err := b.openMeta(); err != nil {
		b.stores.CloseAll()
		return nil, fmt.Errorf("打开元数据层失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.background(ctx)
	return b, nil
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
			for _, v := range b.vhosts {
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
	mu      sync.Mutex
	blocked bool
	// ch 在 blocked 为 true 时有效，解除阻塞时被关闭。
	ch chan struct{}
}

func newFlowGate() *flowGate { return &flowGate{ch: make(chan struct{})} }

func (g *flowGate) isBlocked() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.blocked
}

// set 更新阻塞状态，返回状态是否发生了变化。
func (g *flowGate) set(blocked bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blocked == blocked {
		return false
	}
	g.blocked = blocked
	if blocked {
		g.ch = make(chan struct{})
	} else {
		close(g.ch)
	}
	return true
}

// wait 在阻塞期间等待，解除或内核停止时返回。
func (g *flowGate) wait(stop <-chan struct{}) error {
	for {
		g.mu.Lock()
		if !g.blocked {
			g.mu.Unlock()
			return nil
		}
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

// processMemory 返回本进程向操作系统申请的内存总量。
func processMemory() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys
}

// ---------------------------------------------------------------------------
// 连接级通知
// ---------------------------------------------------------------------------

// subscribeNotifications 登记一个连接的通知通道；阻塞中接入的连接会立刻收到当前状态。
func (b *Broker) subscribeNotifications() (int, chan plugin.Notification) {
	ch := make(chan plugin.Notification, 4)
	// 先读状态再登记，避免"登记后被广播漏掉"与"登记前状态变化"两种竞态同时成立
	blocked := b.flow.isBlocked()

	b.subsMu.Lock()
	b.subSeq++
	id := b.subSeq
	b.subs[id] = ch
	b.subsMu.Unlock()

	if blocked {
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
	v, ok := b.vhosts[e.vhost]
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
	return map[string]any{
		"product":     "SwiftMQ",
		"version":     Version,
		"platform":    "Go",
		"information": "https://github.com/houzch/swiftmq",
		"capabilities": map[string]any{
			// 认证失败时用 Connection.Close 明确告知原因，而不是直接断开连接
			"authentication_failure_close": true,

			// ---- M2 已实现并声明 ----
			// 交换机间绑定：Exchange.Bind/Unbind 参与真实路由
			"exchange_exchange_bindings": true,
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

			// ---- 尚未实现，一律不声明（客户端会自行降级）----
			//   consumer_priorities     → 消费者优先级（x-priority）尚未实现
			//   direct_reply_to         → amq.rabbitmq.reply-to 伪队列尚未实现
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
	_, ok := s.broker.vhosts[name]
	return ok
}

// DefaultVHost 返回默认 vhost 名。
func (s *session) DefaultVHost() string { return s.broker.cfg.DefaultVHost }

// Session 返回绑定到指定 vhost 的操作面。
func (s *session) Session(vhostName string) (plugin.Session, error) {
	vh, ok := s.broker.vhosts[vhostName]
	if !ok {
		return nil, plugin.Errorf(plugin.KindInvalidPath,
			"NOT_ALLOWED - vhost %s not found", vhostName)
	}
	if s.vh == nil {
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
func compilePermission(store *auth.Store, user, vhost string) (*permissionSet, error) {
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
