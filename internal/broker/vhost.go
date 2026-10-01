package broker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 预声明交换机。
const (
	defaultExchange = ""
	exchangeDirect  = "amq.direct"
	exchangeFanout  = "amq.fanout"
	exchangeTopic   = "amq.topic"
	exchangeHeaders = "amq.headers"
	exchangeMatch   = "amq.match"
)

// reservedPrefix 是服务端保留的名字前缀，客户端不得自行声明。
const reservedPrefix = "amq."

// 绑定目标的类型（与 meta.Binding.DestinationType 的取值一致）。
const (
	destinationQueue    = "queue"
	destinationExchange = "exchange"
)

// vhost 是一个虚拟主机的拓扑与队列集合。
type vhost struct {
	name string
	log  *slog.Logger
	// broker 是内核引用：拓扑变更需要经它提交到元数据层（durable 且非 exclusive 的对象）。
	broker *Broker

	mu        sync.RWMutex
	exchanges map[string]*exchange
	queues    map[string]*queue
	// consumers 是消费者标签索引：标签 → 队列（basic.cancel 用）。
	consumers map[string]*queue
	// sweepSet 是需要定时扫描的队列（配了消息 TTL 或队列过期）。
	sweepSet map[string]*queue
	// stores 是消息持久化层的入口，durable 队列据此打开自己的存储。
	stores *store.Manager
	// dlxCh 是死信派发器入口；由内核的单个后台协程消费。
	dlxCh chan<- deadLetterEntry
}

// deadLetterEntry 是一条待派发的死信。
type deadLetterEntry struct {
	vhost      string
	msg        *plugin.Message
	exchange   string
	routingKey string
	reason     string
}

// newVHost 创建 vhost 并预声明内置交换机。
func newVHost(b *Broker, name string, log *slog.Logger, stores *store.Manager, dlxCh chan<- deadLetterEntry) *vhost {
	v := &vhost{
		name:      name,
		log:       log.With("vhost", name),
		broker:    b,
		exchanges: map[string]*exchange{},
		queues:    map[string]*queue{},
		consumers: map[string]*queue{},
		sweepSet:  map[string]*queue{},
		stores:    stores,
		dlxCh:     dlxCh,
	}
	// 默认交换机：按 routing key（即队列名）直接投递，不可声明、不可删除
	v.exchanges[defaultExchange] = newExchange(defaultExchange, plugin.ExchangeDirect, true, false, true, nil)
	v.exchanges[exchangeDirect] = newExchange(exchangeDirect, plugin.ExchangeDirect, true, false, false, nil)
	v.exchanges[exchangeFanout] = newExchange(exchangeFanout, plugin.ExchangeFanout, true, false, false, nil)
	v.exchanges[exchangeTopic] = newExchange(exchangeTopic, plugin.ExchangeTopic, true, false, false, nil)
	v.exchanges[exchangeHeaders] = newExchange(exchangeHeaders, plugin.ExchangeHeaders, true, false, false, nil)
	v.exchanges[exchangeMatch] = newExchange(exchangeMatch, plugin.ExchangeHeaders, true, false, false, nil)
	return v
}

func (v *vhost) getExchange(name string) (*exchange, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	ex, ok := v.exchanges[name]
	return ex, ok
}

func (v *vhost) getQueue(name string) (*queue, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	q, ok := v.queues[name]
	return q, ok
}

// queueRemote 表示该队列的数据/服务是否在别的节点上，也就是"本节点是否只做代理"。
//
// 经典队列看声明时的 Owner（创建后不变）；仲裁队列看它 Raft 组**当前**的 leader ——
// leader 会随选举变化，因此在创建时定死是不对的。
func (v *vhost) queueRemote(q *queue) bool {
	if q.quorum != nil {
		return !q.quorum.isLeader()
	}
	return q.remote
}

// queueOwner 返回应当服务该队列的节点。
//
// 仲裁队列的 leader 会随选举变化，且 follower 要等一次心跳才知道新 leader；
// 因此这里在"还不知道 leader"时短暂等一小会儿（上限 quorumLeaderWait），
// 避免"刚声明完就在另一个节点发布"这类合法用法偶发失败。
// 仍然拿不到时返回空串，调用方必须明确报错并让客户端重试，而不是随便挑一个节点发过去。
func (v *vhost) queueOwner(q *queue) string {
	if q.quorum == nil {
		return q.nodeOwner
	}
	deadline := time.Now().Add(quorumLeaderWait)
	for {
		if leader := q.quorum.leaderNode(); leader != "" {
			return leader
		}
		if !time.Now().Before(deadline) {
			return ""
		}
		time.Sleep(quorumLeaderPoll)
	}
}

// queueOwnerBestEffort 与 queueOwner 相同，但**不等待**：管理面只做展示，
// 不能被某个队列的选主拖住。
func (v *vhost) queueOwnerBestEffort(q *queue) string {
	if q.quorum == nil {
		return q.nodeOwner
	}
	return q.quorum.leaderNode()
}

// ---------------------------------------------------------------------------
// 会话：绑定到某 vhost 与某个连接的 plugin.Session 实现
// ---------------------------------------------------------------------------

// vhostSession 是 plugin.Session 的实现。
type vhostSession struct {
	vh   *vhost
	id   string // 会话标识，用于独占队列归属判定
	user string // 认证用户（权限错误信息与审计用）
	perm *permissionSet
	log  *slog.Logger
	// gate 是生产端水位闸门：内存/磁盘水位触发时挂起发布（而不是丢弃消息）。
	// 为 nil 表示不做流控（内核单测直接构造会话的场景）。
	gate func() error

	mu        sync.Mutex
	exclusive map[string]struct{} // 本会话创建的独占队列
	tags      map[string]string   // 本会话创建的消费者标签 → 队列名
	closed    bool
}

var _ plugin.Session = (*vhostSession)(nil)

func newVHostSession(vh *vhost, id, user string, perm *permissionSet, log *slog.Logger, gate func() error) *vhostSession {
	if perm != nil {
		perm.user = user
	}
	return &vhostSession{
		vh:        vh,
		id:        id,
		user:      user,
		perm:      perm,
		log:       log.With("vhost", vh.name, "user", user),
		gate:      gate,
		exclusive: map[string]struct{}{},
		tags:      map[string]string{},
	}
}

// newQueueIn 创建队列：接线死信派发，并为 durable 队列打开持久化存储。
//
// 死信走"异步入队 + 内核后台派发"，而不是在队列持锁时同步路由 ——
// 后者在"死信目标恰好是本队列"时会自锁死。
func (s *vhostSession) newQueueIn(name string, req plugin.QueueDeclare, args queueArgs) (*queue, error) {
	// 拒绝"瞬时（non-durable）非独占队列"，对齐 RabbitMQ 4.x：
	// 4.0 起 `transient_nonexcl_queues` 特性默认禁用，声明这类队列会以硬错误（541 INTERNAL_ERROR）
	// 关闭连接。本项目的兼容基线（见 README）本身就写着"不保留瞬时队列"，
	// 这里把那条声明落到实现上 —— 双跑对照正是靠这条用例发现了"实现与声明不符"。
	//
	// 判定条件就是"非持久且非独占"：**auto_delete 不豁免**（实测确认，见 M8-5b 的探测记录）。
	// 只拦"新建"这一条路径：队列若已存在，由等价性检查（406）负责，不能改判成硬错误。
	if !req.Durable && !req.Exclusive {
		return nil, plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - cannot declare transient non-exclusive queue '%s' in vhost '%s': "+
				"the deprecated 'transient_nonexcl_queues' feature is disabled (RabbitMQ 4.x semantics)",
			name, s.vh.name)
	}

	q := newQueue(name, req.Durable, req.Exclusive, req.AutoDelete,
		s.ownerOf(req.Exclusive), map[string]any(req.Arguments), args, s.vh.log)
	s.vh.wireDeadLetter(q, args)

	if s.vh.stores != nil {
		st, recovered, err := s.vh.stores.Open(s.vh.name, name, req.Durable)
		if err != nil {
			return nil, plugin.Errorf(plugin.KindInternal,
				"INTERNAL_ERROR - 打开队列 '%s' 的存储失败: %v", name, err)
		}
		q.store = st
		q.restore(recovered)
	}
	return q, nil
}

// wireDeadLetter 给队列接线死信派发器。
//
// 抽到 vhost 上是因为有两条创建路径：会话声明（transient / exclusive）与
// 元数据恢复（durable 非 exclusive），两者的死信语义必须完全一致。
func (v *vhost) wireDeadLetter(q *queue, args queueArgs) {
	ch, vhostName := v.dlxCh, v.name
	q.deadLetter = func(msg *plugin.Message, reason string) {
		if ch == nil {
			return
		}
		select {
		case ch <- deadLetterEntry{vhost: vhostName, msg: msg, exchange: args.deadLetterEx, routingKey: args.deadLetterKey, reason: reason}:
		default:
			// 派发器积压：明确记录并丢弃，而不是无限堆积拖垮内核
			v.log.Warn("死信派发队列已满，丢弃死信", "queue", q.name, "reason", reason)
		}
	}
}

// managedQueue 表示该队列声明是否属于"集群级元数据"（durable 且非 exclusive）。
//
// 其余（transient / exclusive / auto-delete）是会话本地的，不进元数据：
// 进元数据会让"重启后凭空出现一堆临时队列"，也让单机行为偏离 M1–M5。
func managedQueue(req plugin.QueueDeclare) bool { return req.Durable && !req.Exclusive }

// managedExchange 表示该交换机声明是否属于"集群级元数据"（durable）。
func managedExchange(req plugin.ExchangeDeclare) bool { return req.Durable }

// ---------- 交换机 ----------

func (s *vhostSession) DeclareExchange(req plugin.ExchangeDeclare) error {
	if err := s.vh.broker.checkServing(); err != nil {
		return err
	}
	if err := s.perm.allowConfigure(req.Name); err != nil {
		return err
	}
	if req.Name == defaultExchange {
		if req.Passive {
			// 被动声明默认交换机：确认存在即可
			return nil
		}
		// 对齐 RabbitMQ：默认交换机上的声明/删除类操作一律 403
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - operation not permitted on the default exchange")
	}
	if exists, ok := s.vh.getExchange(req.Name); ok {
		if err := checkExchangeEquivalence(exists, req); err != nil {
			return err
		}
		if req.Passive {
			return nil
		}
		return nil
	}
	if req.Passive {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", req.Name, s.vh.name)
	}
	if isReserved(req.Name) {
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - cannot declare exchange name '%s' (reserved)", req.Name)
	}
	typ, err := parseExchangeType(req.Type)
	if err != nil {
		return err
	}

	// durable 交换机是集群级元数据：经元数据层提交（单机落 state.json、集群经 Raft 复制），
	// 本地建立统一由 ApplyMeta 完成 —— 一条变更只有一条落地路径，才不会有"哪边先写"的分叉。
	if managedExchange(req) {
		rec := meta.Exchange{
			VHost: s.vh.name, Name: req.Name, Type: string(typ), Durable: req.Durable,
			AutoDelete: req.AutoDelete, Internal: req.Internal, Arguments: req.Arguments,
		}
		if err := s.vh.broker.submitMeta(meta.OpPutExchange, rec); err != nil {
			return err
		}
		if err := s.vh.broker.awaitMeta(func() bool {
			_, ok := s.vh.getExchange(req.Name)
			return ok
		}); err != nil {
			return err
		}
		s.log.Debug("交换机已声明", "exchange", req.Name, "type", req.Type)
		return nil
	}

	ex := newExchange(req.Name, typ, req.Durable, req.AutoDelete, req.Internal, req.Arguments)
	s.vh.mu.Lock()
	// 并发声明的竞争：谁先写入谁生效
	if _, dup := s.vh.exchanges[req.Name]; dup {
		s.vh.mu.Unlock()
		return nil
	}
	s.vh.exchanges[req.Name] = ex
	s.vh.mu.Unlock()

	s.log.Debug("交换机已声明", "exchange", req.Name, "type", req.Type)
	return nil
}

func (s *vhostSession) DeleteExchange(name string, ifUnused bool) error {
	if err := s.vh.broker.checkServing(); err != nil {
		return err
	}
	if err := s.perm.allowConfigure(name); err != nil {
		return err
	}
	if name == defaultExchange {
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - operation not permitted on the default exchange")
	}
	ex, ok := s.vh.getExchange(name)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", name, s.vh.name)
	}
	if isReserved(name) {
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - cannot delete exchange name '%s' (reserved)", name)
	}
	if ifUnused && ex.bindingCount() > 0 {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - exchange '%s' in vhost '%s' in use", name, s.vh.name)
	}

	// durable 交换机：删除走元数据层，并级联清掉元数据里与它相关的绑定记录 ——
	// 否则 state.json 会不断堆积指向已删交换机的悬空绑定。
	if ex.durable {
		related := s.vh.broker.bindingsForExchange(s.vh.name, name)
		if err := s.vh.broker.submitMeta(meta.OpDeleteExchange,
			meta.Exchange{VHost: s.vh.name, Name: name}); err != nil {
			return err
		}
		for _, rec := range related {
			// 级联失败只告警：交换机删除本身已经提交成功，不能反过来让客户端以为删除失败。
			if err := s.vh.broker.submitMeta(meta.OpDeleteBinding, rec); err != nil {
				s.log.Warn("清理交换机绑定的元数据失败", "exchange", name, "err", err)
			}
		}
		if err := s.vh.broker.awaitMeta(func() bool {
			_, ok := s.vh.getExchange(name)
			return !ok
		}); err != nil {
			return err
		}
		s.log.Debug("交换机已删除", "exchange", name)
		return nil
	}

	s.vh.mu.Lock()
	delete(s.vh.exchanges, name)
	// 清理其他交换机指向它的绑定，避免留下悬空绑定
	for _, other := range s.vh.exchanges {
		other.removeExchangeBindings(name)
	}
	s.vh.mu.Unlock()

	s.log.Debug("交换机已删除", "exchange", name)
	return nil
}

func (s *vhostSession) BindExchange(destination, source, routingKey string, arguments map[string]any) error {
	if err := s.vh.broker.checkServing(); err != nil {
		return err
	}
	if err := s.perm.allowWrite(source); err != nil {
		return err
	}
	src, ok := s.vh.getExchange(source)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", source, s.vh.name)
	}
	dst, ok := s.vh.getExchange(destination)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", destination, s.vh.name)
	}
	// 两端都是 durable 交换机时绑定属于集群级元数据；否则是本地绑定。
	if src.durable && dst.durable {
		rec := meta.Binding{
			VHost: s.vh.name, Source: source, Destination: destination,
			DestinationType: destinationExchange, RoutingKey: routingKey, Arguments: arguments,
		}
		if err := s.vh.broker.submitMeta(meta.OpPutBinding, rec); err != nil {
			return err
		}
		return s.vh.broker.awaitMeta(func() bool {
			return src.hasExchangeBinding(routingKey, destination)
		})
	}
	src.addExchangeBinding(routingKey, destination, arguments)
	return nil
}

func (s *vhostSession) UnbindExchange(destination, source, routingKey string, arguments map[string]any) error {
	if err := s.vh.broker.checkServing(); err != nil {
		return err
	}
	if err := s.perm.allowWrite(source); err != nil {
		return err
	}
	src, ok := s.vh.getExchange(source)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", source, s.vh.name)
	}
	if dst, ok := s.vh.getExchange(destination); ok && src.durable && dst.durable {
		if !src.hasExchangeBinding(routingKey, destination) {
			return plugin.Errorf(plugin.KindNotFound,
				"NOT_FOUND - no binding '%s' between exchange '%s' and exchange '%s'",
				routingKey, source, destination)
		}
		rec := meta.Binding{
			VHost: s.vh.name, Source: source, Destination: destination,
			DestinationType: destinationExchange, RoutingKey: routingKey, Arguments: arguments,
		}
		if err := s.vh.broker.submitMeta(meta.OpDeleteBinding, rec); err != nil {
			return err
		}
		return s.vh.broker.awaitMeta(func() bool {
			return !src.hasExchangeBinding(routingKey, destination)
		})
	}
	if !src.removeExchangeBinding(routingKey, destination) {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no binding '%s' between exchange '%s' and exchange '%s'",
			routingKey, source, destination)
	}
	return nil
}

// ---------- 队列 ----------

func (s *vhostSession) DeclareQueue(req plugin.QueueDeclare) (plugin.QueueInfo, error) {
	if err := s.vh.broker.checkServing(); err != nil {
		return plugin.QueueInfo{}, err
	}
	// 参数先解析：非法参数必须在创建任何东西之前就报 406
	args, err := parseQueueArgs(req.Arguments)
	if err != nil {
		return plugin.QueueInfo{}, err
	}
	if args.queueType == queueTypeQuorum && !req.Passive {
		// 被动声明只是"查一下有没有"，不该因为调用方没重复写 durable 等标志就被拒。
		if err := checkQuorumDeclare(req); err != nil {
			return plugin.QueueInfo{}, err
		}
	}

	if req.Name == "" {
		if req.Passive {
			return plugin.QueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
				"PRECONDITION_FAILED - cannot passively declare a server-named queue")
		}
		// 服务端生成队列名
		name := generatedQueueName()
		if err := s.perm.allowConfigure(name); err != nil {
			return plugin.QueueInfo{}, err
		}
		if managedQueue(req) {
			return s.declareManagedQueue(name, req, args)
		}
		q, err := s.newQueueIn(name, req, args)
		if err != nil {
			return plugin.QueueInfo{}, err
		}
		s.addQueue(q, args)
		s.log.Debug("队列已声明（服务端命名）", "queue", name,
			"ttl", args.messageTTL, "dlx", args.deadLetterEx, "max_length", args.maxLength)
		ready, consumers := q.stats()
		return plugin.QueueInfo{Name: name, MessageCount: ready, ConsumerCount: consumers}, nil
	}

	if q, ok := s.vh.getQueue(req.Name); ok {
		return s.existingQueue(q, req)
	}
	if req.Passive {
		// 队列可能刚在别的节点声明、本节点还没应用它：短暂等一下再判 404（见 lookupQueue）。
		if q, ok := s.lookupQueue(req.Name); ok {
			return s.existingQueue(q, req)
		}
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", req.Name, s.vh.name)
	}
	if isReserved(req.Name) {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - cannot declare queue name '%s' (reserved)", req.Name)
	}
	if err := s.perm.allowConfigure(req.Name); err != nil {
		return plugin.QueueInfo{}, err
	}

	if managedQueue(req) {
		return s.declareManagedQueue(req.Name, req, args)
	}

	q, err := s.newQueueIn(req.Name, req, args)
	if err != nil {
		return plugin.QueueInfo{}, err
	}
	if !s.addQueue(q, args) {
		// 并发声明竞争：另一个会话已抢先创建。
		// 这里只能关掉自己刚打开的存储，绝不能删磁盘数据 —— 那份数据属于已存在的队列。
		q.discardStore()
		if existing, ok := s.vh.getQueue(req.Name); ok {
			ready, consumers := existing.stats()
			return plugin.QueueInfo{Name: req.Name, MessageCount: ready, ConsumerCount: consumers}, nil
		}
		return plugin.QueueInfo{Name: req.Name}, nil
	}
	s.log.Debug("队列已声明", "queue", req.Name,
		"ttl", args.messageTTL, "dlx", args.deadLetterEx, "max_length", args.maxLength)
	// 恢复出来的持久消息也要体现在声明响应里：客户端常据此判断"队列里是否还有存量"
	ready, consumers := q.stats()
	return plugin.QueueInfo{Name: req.Name, MessageCount: ready, ConsumerCount: consumers}, nil
}

// existingQueue 处理"队列已存在"的声明：等价性校验、独占校验，并返回 Owner 侧的统计。
func (s *vhostSession) existingQueue(q *queue, req plugin.QueueDeclare) (plugin.QueueInfo, error) {
	// exclusive 标志不一致一律 405（**两个方向都是**）：RabbitMQ 把它归为"无法获得独占访问"。
	// 实测（M8-5b 探测）：exclusive→非 exclusive 是 405、非 exclusive→exclusive 也是 405。
	// 注意 exclusive **不参与**等价性比较（见 checkQueueEquivalence），否则这里会先被判成 406。
	if q.exclusive != req.Exclusive {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'",
			q.name, s.vh.name)
	}
	if err := checkQueueEquivalence(q, req); err != nil {
		return plugin.QueueInfo{}, err
	}
	if q.exclusive && q.owner != s.id {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'",
			req.Name, s.vh.name)
	}
	return s.queueInfo(q), nil
}

// declareManagedQueue 提交一次"集群级"队列声明（durable 且非 exclusive），
// 并等待本地拓扑可见后返回统计信息。
//
// 声明本身由元数据层决定归属与顺序；本地对象由 ApplyMeta 建立，
// 因此这里在提交成功后要等一下本地应用 —— follower 的应用滞后于 leader 的提交。
func (s *vhostSession) declareManagedQueue(name string, req plugin.QueueDeclare, args queueArgs) (plugin.QueueInfo, error) {
	owner := s.vh.broker.queueOwner()
	if args.queueType == queueTypeQuorum {
		// 仲裁队列没有"声明者即 Owner"这回事：服务节点是它 Raft 组**当前**的 leader。
		owner = ""
	}
	rec := meta.Queue{
		VHost: s.vh.name, Name: name, Durable: req.Durable, AutoDelete: req.AutoDelete,
		Exclusive: req.Exclusive, Arguments: req.Arguments,
		Owner: owner, CreatedAt: time.Now().UTC(),
	}
	if err := s.vh.broker.submitMeta(meta.OpPutQueue, rec); err != nil {
		return plugin.QueueInfo{}, err
	}
	if err := s.vh.broker.awaitMeta(func() bool {
		_, ok := s.vh.getQueue(name)
		return ok
	}); err != nil {
		return plugin.QueueInfo{}, err
	}
	s.log.Debug("队列已声明（集群元数据）", "queue", name, "owner", rec.Owner,
		"ttl", args.messageTTL, "dlx", args.deadLetterEx, "max_length", args.maxLength)
	q, _ := s.vh.getQueue(name)
	// 恢复出来的持久消息也要体现在声明响应里：客户端常据此判断"队列里是否还有存量"
	ready, consumers := q.stats()
	return plugin.QueueInfo{Name: name, MessageCount: ready, ConsumerCount: consumers}, nil
}

// addQueue 把队列加入 vhost，并按需要登记到定时扫描集合。返回 false 表示同名队列已存在。
func (s *vhostSession) addQueue(q *queue, args queueArgs) bool {
	if !s.vh.addQueueObject(q, args) {
		return false
	}
	if q.exclusive {
		s.trackExclusive(q.name)
	}
	return true
}

func (s *vhostSession) DeleteQueue(name string, ifUnused, ifEmpty bool) (plugin.QueueInfo, error) {
	if err := s.vh.broker.checkServing(); err != nil {
		return plugin.QueueInfo{}, err
	}
	if err := s.perm.allowConfigure(name); err != nil {
		return plugin.QueueInfo{}, err
	}
	q, ok := s.lookupQueue(name)
	if !ok {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, s.vh.name)
	}
	if q.exclusive && q.owner != s.id {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'", name, s.vh.name)
	}
	// 统计信息要来自数据所在的节点：远端队列在本节点没有消息，报 0 会让 if-unused / if-empty
	// 判断形同虚设，也会让删除响应里的计数失真。
	info := s.queueInfo(q)
	ready, consumers := info.MessageCount, info.ConsumerCount
	if ifUnused && consumers > 0 {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - queue '%s' in vhost '%s' in use", name, s.vh.name)
	}
	if ifEmpty && ready > 0 {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - queue '%s' in vhost '%s' not empty", name, s.vh.name)
	}

	// durable 非 exclusive 队列：删除必须经元数据层，本地移除由 ApplyMeta 完成。
	if q.clusterManaged() {
		if err := s.vh.broker.submitMeta(meta.OpDeleteQueue,
			meta.Queue{VHost: s.vh.name, Name: name}); err != nil {
			return plugin.QueueInfo{}, err
		}
		if err := s.vh.broker.awaitMeta(func() bool {
			_, ok := s.vh.getQueue(name)
			return !ok
		}); err != nil {
			return plugin.QueueInfo{}, err
		}
		s.forgetQueueLocal(name)
		s.log.Debug("队列已删除（集群元数据）", "queue", name)
		return plugin.QueueInfo{Name: name, MessageCount: ready, ConsumerCount: consumers}, nil
	}

	s.removeQueue(name, q)
	s.log.Debug("队列已删除", "queue", name)
	return plugin.QueueInfo{Name: name, MessageCount: ready, ConsumerCount: consumers}, nil
}

func (s *vhostSession) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]any) error {
	if err := s.vh.broker.checkServing(); err != nil {
		return err
	}
	if err := s.perm.allowWrite(exchangeName); err != nil {
		return err
	}
	ex, ok := s.vh.getExchange(exchangeName)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", exchangeName, s.vh.name)
	}
	q, ok := s.lookupQueue(queueName)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", queueName, s.vh.name)
	}
	// 绑定是拓扑，与消息数据在哪无关：指向远端队列的绑定同样成立，
	// 发布时由本节点把消息转发给 Owner（见 forward.go）。
	// durable 交换机 + durable 非 exclusive 队列 → 绑定属于集群级元数据。
	if ex.durable && q.clusterManaged() {
		rec := meta.Binding{
			VHost: s.vh.name, Source: exchangeName, Destination: queueName,
			DestinationType: destinationQueue, RoutingKey: routingKey, Arguments: arguments,
		}
		if err := s.vh.broker.submitMeta(meta.OpPutBinding, rec); err != nil {
			return err
		}
		return s.vh.broker.awaitMeta(func() bool {
			return ex.hasQueueBinding(routingKey, queueName)
		})
	}
	ex.addBinding(routingKey, queueName, arguments)
	return nil
}

func (s *vhostSession) UnbindQueue(queueName, exchangeName, routingKey string, arguments map[string]any) error {
	if err := s.vh.broker.checkServing(); err != nil {
		return err
	}
	if err := s.perm.allowWrite(exchangeName); err != nil {
		return err
	}
	ex, ok := s.vh.getExchange(exchangeName)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", exchangeName, s.vh.name)
	}
	if q, ok := s.lookupQueue(queueName); ok && ex.durable && q.clusterManaged() {
		if !ex.hasQueueBinding(routingKey, queueName) {
			return plugin.Errorf(plugin.KindNotFound,
				"NOT_FOUND - no binding '%s' between exchange '%s' and queue '%s'",
				routingKey, exchangeName, queueName)
		}
		rec := meta.Binding{
			VHost: s.vh.name, Source: exchangeName, Destination: queueName,
			DestinationType: destinationQueue, RoutingKey: routingKey, Arguments: arguments,
		}
		if err := s.vh.broker.submitMeta(meta.OpDeleteBinding, rec); err != nil {
			return err
		}
		return s.vh.broker.awaitMeta(func() bool {
			return !ex.hasQueueBinding(routingKey, queueName)
		})
	}
	if !ex.removeBinding(routingKey, queueName) {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no binding '%s' between exchange '%s' and queue '%s'",
			routingKey, exchangeName, queueName)
	}
	return nil
}

func (s *vhostSession) PurgeQueue(name string) (uint32, error) {
	if err := s.vh.broker.checkServing(); err != nil {
		return 0, err
	}
	if err := s.perm.allowConfigure(name); err != nil {
		return 0, err
	}
	q, ok := s.lookupQueue(name)
	if !ok {
		return 0, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, s.vh.name)
	}
	if s.vh.queueRemote(q) {
		return s.vh.broker.forwardPurge(s.vh.name, name, s.vh.queueOwner(q))
	}
	return q.purge(), nil
}

// ---------- 发布 ----------

func (s *vhostSession) Publish(msg *plugin.Message, exchangeName, routingKey string, mandatory bool) (plugin.PublishResult, error) {
	var res plugin.PublishResult

	if err := s.vh.broker.checkServing(); err != nil {
		return res, err
	}

	// 资源水位触发时在这里挂起：读循环停读 → TCP 背压，客户端自然被限速。
	// 阻塞生产者而不是丢弃消息，是水位流控的核心语义。
	if s.gate != nil {
		if err := s.gate(); err != nil {
			return res, plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - %v", err)
		}
	}

	ex, ok := s.vh.getExchange(exchangeName)
	if !ok {
		return res, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", exchangeName, s.vh.name)
	}
	// 写权限按交换机名判定（默认交换机的名字是空串，与 RabbitMQ 一致）
	if err := s.perm.allowWrite(exchangeName); err != nil {
		return res, err
	}
	if ex.internal && exchangeName != defaultExchange {
		// 默认交换机在元数据上也是 internal，但客户端按队列名发布到它是标准用法，
		// 因此这里只拦住"其他内部交换机"——与 RabbitMQ 的可观察行为一致。
		return res, plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - cannot publish to internal exchange '%s' in vhost '%s'",
			exchangeName, s.vh.name)
	}

	// 默认交换机没有显式绑定：每个队列都以"自己的名字"隐式绑定在它上面。
	// 这是客户端最常用的发布方式（不写 exchange，直接以队列名当 routing key）。
	if exchangeName == defaultExchange {
		q, ok := s.vh.getQueue(routingKey)
		if !ok {
			return res, nil
		}
		if s.vh.queueRemote(q) {
			routed, rejected, err := s.vh.broker.forwardPublish(s.vh.name, routingKey, s.vh.queueOwner(q), msg)
			if err != nil {
				return res, err
			}
			res.Routed, res.Rejected = routed, rejected
			// 落盘/复制的等待在服务节点完成，转发应答即代表"已按其档位持久化"。
			return res, nil
		}
		accepted, wait, err := q.publish(cloneForQueue(msg))
		if err != nil {
			return res, err
		}
		res.Routed = true
		res.Rejected = !accepted
		res.Durable = waitForDurable([]func() error{wait})
		return res, nil
	}

	targets := s.vh.resolveQueues(ex, routingKey, msg.Properties, map[string]struct{}{})
	s.log.Debug("发布消息", "exchange", exchangeName, "routing_key", routingKey, "targets", len(targets))

	var waits []func() error
	for _, name := range targets {
		q, ok := s.vh.getQueue(name)
		if !ok {
			continue
		}
		// 命中远端队列：把消息转发给服务节点（数据面在那边，本节点只做代理）。
		if s.vh.queueRemote(q) {
			routed, rejected, err := s.vh.broker.forwardPublish(s.vh.name, name, s.vh.queueOwner(q), msg)
			if err != nil {
				return res, err
			}
			if routed {
				res.Routed = true
			}
			if rejected {
				res.Rejected = true
			}
			continue
		}
		res.Routed = true
		accepted, wait, err := q.publish(cloneForQueue(msg))
		if err != nil {
			return res, err
		}
		if !accepted {
			res.Rejected = true
		}
		if wait != nil {
			waits = append(waits, wait)
		}
	}
	// 扇出到 N 个队列时必须等**全部**队列持久化：只等一个会让确认语义形同虚设
	res.Durable = waitForDurable(waits)
	return res, nil
}

// waitForDurable 把多个队列的"可确认"凭据合成一个等待函数；都没有时返回 nil。
//
// 凭据只对"确实需要等待"的路径非 nil：经典队列的持久消息是落盘凭据，
// 仲裁队列是"复制到多数派"凭据；非持久消息两者都没有。
func waitForDurable(waits []func() error) func() error {
	var real []func() error
	for _, w := range waits {
		if w != nil {
			real = append(real, w)
		}
	}
	if len(real) == 0 {
		return nil
	}
	return func() error {
		var firstErr error
		for _, w := range real {
			if err := w(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}

// cloneForQueue 为每个目标队列复制一份消息。
//
// Redelivered 等状态是队列级的，不能跨队列共享；消息体与属性是只读的，可以共享。
func cloneForQueue(msg *plugin.Message) *plugin.Message {
	clone := *msg
	clone.Redelivered = false
	return &clone
}

// resolveQueues 展开交换机路由，递归处理交换机到交换机的绑定。
func (v *vhost) resolveQueues(ex *exchange, routingKey string, props plugin.Properties, visited map[string]struct{}) []string {
	if _, seen := visited[ex.name]; seen {
		return nil // 防止交换机绑定成环导致无限递归
	}
	visited[ex.name] = struct{}{}

	queues, exchanges := ex.routeAll(routingKey, props)
	out := queues
	for _, name := range exchanges {
		next, ok := v.getExchange(name)
		if !ok {
			continue
		}
		out = append(out, v.resolveQueues(next, routingKey, props, visited)...)
	}
	// 去重：不同路径可能汇聚到同一队列
	if len(out) < 2 {
		return out
	}
	seen := make(map[string]struct{}, len(out))
	uniq := out[:0]
	for _, name := range out {
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		uniq = append(uniq, name)
	}
	return uniq
}

// ---------------------------------------------------------------------------
// vhost 级只读视图（管理面用）
// ---------------------------------------------------------------------------

// VHostSnapshot 是 vhost 的只读视图。
type VHostSnapshot struct {
	Name            string
	Messages        int
	MessagesReady   int
	MessagesUnacked int
	QueueCount      int
	ExchangeCount   int
	ConsumerCount   int
	Published       uint64
	Delivered       uint64
	Acked           uint64
}

// snapshot 汇总 vhost 内的消息与对象计数。
func (v *vhost) snapshot() VHostSnapshot {
	s := VHostSnapshot{Name: v.name}

	v.mu.RLock()
	queues := make([]*queue, 0, len(v.queues))
	for _, q := range v.queues {
		queues = append(queues, q)
	}
	s.ExchangeCount = len(v.exchanges)
	v.mu.RUnlock()

	s.QueueCount = len(queues)
	for _, q := range queues {
		qs := q.snapshot(v.name)
		s.MessagesReady += qs.Ready
		s.MessagesUnacked += qs.Unacked
		s.ConsumerCount += qs.ConsumerCount
		s.Published += qs.Published
		s.Delivered += qs.Delivered
		s.Acked += qs.Acked
	}
	s.Messages = s.MessagesReady + s.MessagesUnacked
	return s
}

// queueSnapshots 返回本 vhost 全部队列的快照（按名字排序，保证输出稳定）。
func (v *vhost) queueSnapshots() []QueueSnapshot {
	v.mu.RLock()
	queues := make([]*queue, 0, len(v.queues))
	for _, q := range v.queues {
		queues = append(queues, q)
	}
	v.mu.RUnlock()

	out := make([]QueueSnapshot, 0, len(queues))
	for _, q := range queues {
		out = append(out, v.queueSnapshot(q))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// queueSnapshot 返回单个队列的只读快照；非本地服务的队列向服务节点取计数。
//
// 为什么不能直接报 0：运维看到 `messages: 0` 会认为队列是空的，据此做扩容或排查都会得出错误结论。
// 取不到时保留 0 但带上 Remote/Owner 标记，让"这是别人的数据"这件事在数据里可见。
func (v *vhost) queueSnapshot(q *queue) QueueSnapshot {
	s := q.snapshot(v.name)
	if q.quorum != nil {
		// 仲裁队列的"服务节点"是它 Raft 组的 leader：leader 变更时运维必须能看出来。
		// 展示路径不等待（见 queueOwnerBestEffort）。
		s.Owner = v.queueOwnerBestEffort(q)
		s.Remote = v.queueRemote(q)
		if s.Remote {
			v.fillRemoteStats(&s, q)
		}
		return s
	}
	if !q.remote {
		return s
	}
	s.Remote = true
	s.Owner = q.nodeOwner
	v.fillRemoteStats(&s, q)
	return s
}

// fillRemoteStats 用服务节点的真实计数覆盖本地（恒为 0）的计数。
func (v *vhost) fillRemoteStats(s *QueueSnapshot, q *queue) {
	owner := v.queueOwnerBestEffort(q)
	if owner == "" {
		// 仲裁队列正在选主：没有可问的对象，计数暂时不可得（Remote 标记已说明问题）。
		return
	}
	if st, err := v.broker.remoteQueueStats(v.name, q.name, owner); err == nil {
		s.Ready = int(st.Ready)
		s.Unacked = int(st.Unacked)
		s.ConsumerCount = int(st.Consumers)
	} else {
		v.log.Debug("取远端队列统计失败", "queue", q.name, "owner", owner, "err", err)
	}
}

// exchangeSnapshots 返回本 vhost 全部交换机的快照（按名字排序）。
func (v *vhost) exchangeSnapshots() []ExchangeSnapshot {
	v.mu.RLock()
	exs := make([]*exchange, 0, len(v.exchanges))
	for _, e := range v.exchanges {
		exs = append(exs, e)
	}
	v.mu.RUnlock()

	out := make([]ExchangeSnapshot, 0, len(exs))
	for _, e := range exs {
		out = append(out, e.snapshot(v.name))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// bindingSnapshots 返回本 vhost 全部绑定。
//
// 包含"默认交换机的隐式绑定"：每个队列都以自己的名字绑定在默认交换机上。
// 不列出来的话，管理 UI 的交换机列表会漏掉使用最频繁的那个交换机。
func (v *vhost) bindingSnapshots() []BindingSnapshot {
	v.mu.RLock()
	exs := make([]*exchange, 0, len(v.exchanges))
	for _, e := range v.exchanges {
		exs = append(exs, e)
	}
	queues := make([]*queue, 0, len(v.queues))
	for _, q := range v.queues {
		queues = append(queues, q)
	}
	v.mu.RUnlock()

	var out []BindingSnapshot
	for _, e := range exs {
		out = append(out, e.bindingSnapshots(v.name)...)
	}
	for _, q := range queues {
		out = append(out, BindingSnapshot{
			VHost:           v.name,
			Source:          defaultExchange,
			Destination:     q.name,
			DestinationType: "queue",
			RoutingKey:      q.name,
			Arguments:       map[string]any{},
			PropertiesKey:   q.name,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].Destination != out[j].Destination {
			return out[i].Destination < out[j].Destination
		}
		return out[i].RoutingKey < out[j].RoutingKey
	})
	return out
}

// consumerSnapshots 返回本 vhost 全部消费者（按队列、标签排序）。
func (v *vhost) consumerSnapshots() []ConsumerSnapshot {
	v.mu.RLock()
	queues := make([]*queue, 0, len(v.queues))
	for _, q := range v.queues {
		queues = append(queues, q)
	}
	v.mu.RUnlock()

	var out []ConsumerSnapshot
	for _, q := range queues {
		out = append(out, q.consumerSnapshots(v.name)...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Queue != out[j].Queue {
			return out[i].Queue < out[j].Queue
		}
		return out[i].Tag < out[j].Tag
	})
	return out
}

// ---------------------------------------------------------------------------
// vhost 级维护：定时扫描与内部路由（死信）
// ---------------------------------------------------------------------------

// sweep 扫描需要计时的队列：过期消息转死信，空闲队列按 x-expires 删除。
func (v *vhost) sweep(now time.Time) {
	v.mu.RLock()
	queues := make([]*queue, 0, len(v.sweepSet))
	for _, q := range v.sweepSet {
		queues = append(queues, q)
	}
	v.mu.RUnlock()

	for _, q := range queues {
		if !q.sweep(now) {
			continue
		}
		// 集群托管队列（durable 非 exclusive）的删除必须经元数据层，否则拓扑会分叉；
		// 本地移除由 ApplyMeta 完成，这里不做。
		if q.clusterManaged() {
			if err := v.broker.deleteManagedQueue(v.name, q.name); err != nil {
				v.log.Warn("过期队列的元数据删除失败，保留队列", "queue", q.name, "err", err)
				continue
			}
			v.log.Debug("队列已过期，正在按元数据删除", "queue", q.name)
			continue
		}
		v.log.Debug("队列已过期，已删除", "queue", q.name)
		v.removeQueue(q)
	}
}

// removeQueue 从 vhost 中移除队列，清理绑定与消费者索引，并通知消费者被取消。
func (v *vhost) removeQueue(q *queue) {
	v.mu.Lock()
	delete(v.queues, q.name)
	delete(v.sweepSet, q.name)
	for _, ex := range v.exchanges {
		ex.removeQueueBindings(q.name)
	}
	for tag, cq := range v.consumers {
		if cq == q {
			delete(v.consumers, tag)
		}
	}
	v.mu.Unlock()

	// 对齐 RabbitMQ 的 consumer cancel notify：队列被删时服务端主动下发 basic.cancel
	for _, tag := range q.cancelAllConsumers() {
		v.log.Debug("队列被删除，消费者被取消", "queue", q.name, "consumer_tag", tag)
	}
	q.close()
}

// routeInternal 按 exchange / routingKey 投递消息，不做权限检查。
//
// 用于内核自身的内部路由（死信）：死信不是"某个用户在发布"，
// 因此不该受发布者写权限的约束 —— 否则一个受限用户拒绝消息就会导致死信路由失败。
func (v *vhost) routeInternal(msg *plugin.Message, exchangeName, routingKey string) (routed, rejected bool) {
	if exchangeName == defaultExchange {
		q, ok := v.getQueue(routingKey)
		if !ok {
			return false, false
		}
		if v.queueRemote(q) {
			// 死信的目标队列在别的节点：同样要转发过去，否则死信就静默消失了。
			routed, rejected, err := v.broker.forwardPublish(v.name, routingKey, v.queueOwner(q), msg)
			if err != nil {
				v.log.Warn("死信转发失败", "queue", routingKey, "owner", v.queueOwner(q), "err", err)
				return false, true
			}
			return routed, rejected
		}
		accepted, _, err := q.publish(cloneForQueue(msg))
		if err != nil {
			v.log.Warn("死信入队失败", "queue", routingKey, "err", err)
			return false, true
		}
		return true, !accepted
	}

	ex, ok := v.getExchange(exchangeName)
	if !ok {
		return false, false
	}
	for _, name := range v.resolveQueues(ex, routingKey, msg.Properties, map[string]struct{}{}) {
		q, ok := v.getQueue(name)
		if !ok {
			continue
		}
		if v.queueRemote(q) {
			remoteRouted, remoteRejected, err := v.broker.forwardPublish(v.name, name, v.queueOwner(q), msg)
			if err != nil {
				// 死信没有生产者在前台等确认：转发失败只记账并告警，不阻塞其余目标。
				v.log.Warn("死信转发失败", "queue", name, "owner", v.queueOwner(q), "err", err)
				rejected = true
				continue
			}
			if remoteRouted {
				routed = true
			}
			if remoteRejected {
				rejected = true
			}
			continue
		}
		routed = true
		// 死信是异步派发，没有生产者在前台等待：持久化凭据无人等待也无妨（落盘/复制照常发生）
		accepted, _, err := q.publish(cloneForQueue(msg))
		if err != nil {
			v.log.Warn("死信入队失败", "queue", name, "err", err)
			rejected = true
			continue
		}
		if !accepted {
			rejected = true
		}
	}
	return routed, rejected
}

// dispatchDeadLetter 把一条死信投递到配置的死信交换机。
func (v *vhost) dispatchDeadLetter(e deadLetterEntry) {
	key := e.routingKey
	if key == "" {
		// 未配置 x-dead-letter-routing-key 时沿用原 routing key（对齐 RabbitMQ）
		key = e.msg.RoutingKey
	}
	// 死信的报文头里改成"当前"的路由信息，原始信息已保留在 x-death 中
	e.msg.Exchange = e.exchange
	e.msg.RoutingKey = key

	routed, _ := v.routeInternal(e.msg, e.exchange, key)
	if !routed {
		v.log.Debug("死信未命中任何队列，已丢弃",
			"exchange", e.exchange, "routing_key", key, "reason", e.reason)
	}
}

// ---------- 消费 ----------

func (s *vhostSession) Consume(sub plugin.Subscription) (string, error) {
	if err := s.vh.broker.checkServing(); err != nil {
		return "", err
	}
	if err := s.perm.allowRead(sub.Queue); err != nil {
		return "", err
	}
	q, ok := s.lookupQueue(sub.Queue)
	if !ok {
		return "", plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", sub.Queue, s.vh.name)
	}
	if q.exclusive && q.owner != s.id {
		return "", plugin.Errorf(plugin.KindResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'",
			sub.Queue, s.vh.name)
	}
	if sub.Tag == "" {
		sub.Tag = generatedConsumerTag()
	}

	s.vh.mu.Lock()
	if _, dup := s.vh.consumers[sub.Tag]; dup {
		s.vh.mu.Unlock()
		return "", plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - consumer tag '%s' already in use", sub.Tag)
	}
	s.vh.consumers[sub.Tag] = q
	s.vh.mu.Unlock()

	if s.vh.queueRemote(q) {
		// 远端队列的消费者：在服务节点注册一个代理消费者，投递由它推回来。
		if err := s.vh.broker.remoteConsume(s, q, sub); err != nil {
			s.forgetConsumer(sub.Tag)
			return "", err
		}
	} else if err := q.subscribe(sub); err != nil {
		s.forgetConsumer(sub.Tag)
		return "", err
	}

	s.mu.Lock()
	s.tags[sub.Tag] = sub.Queue
	s.mu.Unlock()

	return sub.Tag, nil
}

func (s *vhostSession) Cancel(consumerTag string) error {
	s.vh.mu.RLock()
	q, ok := s.vh.consumers[consumerTag]
	s.vh.mu.RUnlock()
	if !ok {
		// 对齐 RabbitMQ：未知消费者标签返回 404
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no consumer with tag '%s' in vhost '%s'", consumerTag, s.vh.name)
	}
	if err := s.perm.allowRead(q.name); err != nil {
		return err
	}
	s.forgetConsumer(consumerTag)
	if s.vh.queueRemote(q) {
		// 远端队列：交给服务节点取消（是否触发自动删除也由它判断，它才是数据的持有者）。
		if err := s.vh.broker.remoteCancel(s.vh.name, q.name, s.vh.queueOwner(q), consumerTag); err != nil {
			s.log.Warn("取消远端消费者失败（服务节点会在租约过期后自行摘除）", "queue", q.name, "err", err)
		}
		return nil
	}
	if q.cancel(consumerTag) {
		s.deleteQueueIfAuto(q.name)
	}
	return nil
}

func (s *vhostSession) Get(queueName string, noAck bool) (*plugin.Delivery, bool, error) {
	if err := s.vh.broker.checkServing(); err != nil {
		return nil, false, err
	}
	if err := s.perm.allowRead(queueName); err != nil {
		return nil, false, err
	}
	q, ok := s.lookupQueue(queueName)
	if !ok {
		return nil, false, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", queueName, s.vh.name)
	}
	if s.vh.queueRemote(q) {
		return s.vh.broker.forwardGet(s.vh.name, queueName, s.vh.queueOwner(q), noAck)
	}
	if q.exclusive && q.owner != s.id {
		return nil, false, plugin.Errorf(plugin.KindResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'",
			queueName, s.vh.name)
	}
	d, ok := q.get(noAck)
	ready, consumers := q.stats()
	s.log.Debug("主动拉取", "queue", queueName, "hit", ok, "ready", ready, "consumers", consumers)
	return d, ok, nil
}

// Close 释放会话资源：取消它的消费者、删除它的独占队列。
func (s *vhostSession) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	tags := make([]string, 0, len(s.tags))
	for tag := range s.tags {
		tags = append(tags, tag)
	}
	queues := make([]string, 0, len(s.exclusive))
	for name := range s.exclusive {
		queues = append(queues, name)
	}
	s.tags = map[string]string{}
	s.exclusive = map[string]struct{}{}
	s.mu.Unlock()

	for _, tag := range tags {
		s.vh.mu.RLock()
		q, ok := s.vh.consumers[tag]
		s.vh.mu.RUnlock()
		if !ok {
			continue
		}
		s.forgetConsumer(tag)
		if s.vh.queueRemote(q) {
			// 远端队列：通知服务节点摘掉代理消费者（它未确认的消息由服务节点放回队头）。
			if err := s.vh.broker.remoteCancel(s.vh.name, q.name, s.vh.queueOwner(q), tag); err != nil {
				s.log.Warn("取消远端消费者失败（服务节点会在租约过期后自行摘除）", "queue", q.name, "err", err)
			}
			continue
		}
		if q.cancel(tag) {
			s.deleteQueueIfAuto(q.name)
		}
	}
	for _, name := range queues {
		if q, ok := s.vh.getQueue(name); ok {
			s.removeQueue(name, q)
		}
	}
}

// ---------- 内部工具 ----------

func (s *vhostSession) ownerOf(exclusive bool) string {
	if exclusive {
		return s.id
	}
	return ""
}

func (s *vhostSession) trackExclusive(name string) {
	s.mu.Lock()
	s.exclusive[name] = struct{}{}
	s.mu.Unlock()
}

func (s *vhostSession) forgetConsumer(tag string) {
	s.mu.Lock()
	delete(s.tags, tag)
	s.mu.Unlock()
	s.vh.mu.Lock()
	delete(s.vh.consumers, tag)
	s.vh.mu.Unlock()
}

// removeQueue 从 vhost 中移除队列并清理本会话的相关状态。
func (s *vhostSession) removeQueue(name string, q *queue) {
	s.vh.removeQueue(q)

	s.mu.Lock()
	delete(s.exclusive, name)
	for tag, queueName := range s.tags {
		if queueName == name {
			delete(s.tags, tag)
		}
	}
	s.mu.Unlock()
}

// forgetQueueLocal 清理本会话对某队列的本地记账（消费者标签、独占归属），
// 不触碰 vhost 中的队列对象 —— 后者的移除由元数据应用完成。
func (s *vhostSession) forgetQueueLocal(name string) {
	s.mu.Lock()
	delete(s.exclusive, name)
	for tag, queueName := range s.tags {
		if queueName == name {
			delete(s.tags, tag)
		}
	}
	s.mu.Unlock()
}

// deleteQueueIfAuto 在自动删除队列的最后一名消费者离开后删除队列。
func (s *vhostSession) deleteQueueIfAuto(name string) {
	q, ok := s.vh.getQueue(name)
	if !ok {
		return
	}
	// 集群托管队列：删除经元数据层提交。
	if q.clusterManaged() {
		if err := s.vh.broker.submitMeta(meta.OpDeleteQueue,
			meta.Queue{VHost: s.vh.name, Name: name}); err != nil {
			s.log.Warn("自动删除队列的元数据删除失败", "queue", name, "err", err)
			return
		}
		s.forgetQueueLocal(name)
		s.log.Debug("自动删除队列已删除（集群元数据）", "queue", name)
		return
	}
	s.removeQueue(name, q)
	s.log.Debug("自动删除队列已删除", "queue", name)
}

// ---------------------------------------------------------------------------
// 声明校验
// ---------------------------------------------------------------------------

func checkExchangeEquivalence(ex *exchange, req plugin.ExchangeDeclare) error {
	typ, err := parseExchangeType(req.Type)
	if err != nil {
		return err
	}
	if ex.typ != typ || ex.durable != req.Durable || ex.autoDelete != req.AutoDelete || ex.internal != req.Internal {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - inequivalent arg for exchange '%s': declared as %s(durable=%t auto_delete=%t internal=%t)",
			req.Name, ex.typ, ex.durable, ex.autoDelete, ex.internal)
	}
	if !argsEquivalent(ex.arguments, req.Arguments) {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - inequivalent arguments for exchange '%s'", req.Name)
	}
	return nil
}

// checkQueueEquivalence 比较"已存在队列"与本次声明是否等价。
//
// 两处与直觉不同、但都是 RabbitMQ 的实测行为（见设计文档 M8-5b 的探测记录）：
//   - **不比较 exclusive**：exclusive 不一致由 existingQueue 判成 405（而不是 406）；
//   - **exclusive 队列不比较 durable**：它的生命周期绑在声明连接上，durable 没有实际意义，
//     RabbitMQ 对此跳过检查（用不同 durable 重声明一个 exclusive 队列会被接受）。
func checkQueueEquivalence(q *queue, req plugin.QueueDeclare) error {
	if !q.exclusive && q.durable != req.Durable {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - inequivalent arg 'durable' for queue '%s': received %t, current is %t",
			req.Name, req.Durable, q.durable)
	}
	if q.autoDelete != req.AutoDelete {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - inequivalent arg 'auto_delete' for queue '%s': received %t, current is %t",
			req.Name, req.AutoDelete, q.autoDelete)
	}
	if !argsEquivalent(q.arguments, req.Arguments) {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - inequivalent arguments for queue '%s'", req.Name)
	}
	return nil
}

func parseExchangeType(t plugin.ExchangeType) (plugin.ExchangeType, error) {
	switch t {
	case plugin.ExchangeDirect:
		return plugin.ExchangeDirect, nil
	case plugin.ExchangeFanout:
		return plugin.ExchangeFanout, nil
	case plugin.ExchangeTopic:
		return plugin.ExchangeTopic, nil
	case plugin.ExchangeHeaders:
		return plugin.ExchangeHeaders, nil
	default:
		return "", plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - unknown exchange type '%s'", string(t))
	}
}

// argsEquivalent 比较声明参数是否等价。
//
// 参数表可能含任意类型，这里做保守比较：键集合一致且值的字符串形式一致。
func argsEquivalent(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || fmt.Sprint(av) != fmt.Sprint(bv) {
			return false
		}
	}
	return true
}

func isReserved(name string) bool {
	return strings.HasPrefix(name, reservedPrefix)
}

// generatedQueueName 生成服务端队列名，形如 amq.gen-xxxxxxxxxxxxxxxxxxxxxx。
func generatedQueueName() string {
	return reservedPrefix + "gen-" + randomHex(11)
}

// generatedConsumerTag 生成服务端消费者标签。
func generatedConsumerTag() string {
	return reservedPrefix + "ctag-" + randomHex(8)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// 随机源不可用时退化为时间戳，保证唯一性优先于随机性
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
