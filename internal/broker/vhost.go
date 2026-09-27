package broker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

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

// vhost 是一个虚拟主机的拓扑与队列集合。
type vhost struct {
	name string
	log  *slog.Logger

	mu        sync.RWMutex
	exchanges map[string]*exchange
	queues    map[string]*queue
	// consumers 是消费者标签索引：标签 → 队列（basic.cancel 用）。
	consumers map[string]*queue
	// sweepSet 是需要定时扫描的队列（配了消息 TTL 或队列过期）。
	sweepSet map[string]*queue
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
func newVHost(name string, log *slog.Logger, dlxCh chan<- deadLetterEntry) *vhost {
	v := &vhost{
		name:      name,
		log:       log.With("vhost", name),
		exchanges: map[string]*exchange{},
		queues:    map[string]*queue{},
		consumers: map[string]*queue{},
		sweepSet:  map[string]*queue{},
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

	mu        sync.Mutex
	exclusive map[string]struct{} // 本会话创建的独占队列
	tags      map[string]string   // 本会话创建的消费者标签 → 队列名
	closed    bool
}

var _ plugin.Session = (*vhostSession)(nil)

func newVHostSession(vh *vhost, id, user string, perm *permissionSet, log *slog.Logger) *vhostSession {
	if perm != nil {
		perm.user = user
	}
	return &vhostSession{
		vh:        vh,
		id:        id,
		user:      user,
		perm:      perm,
		log:       log.With("vhost", vh.name, "user", user),
		exclusive: map[string]struct{}{},
		tags:      map[string]string{},
	}
}

// newQueueIn 创建队列并接线死信派发。
//
// 死信走"异步入队 + 内核后台派发"，而不是在队列持锁时同步路由 ——
// 后者在"死信目标恰好是本队列"时会自锁死。
func (s *vhostSession) newQueueIn(name string, req plugin.QueueDeclare, args queueArgs) *queue {
	q := newQueue(name, req.Durable, req.Exclusive, req.AutoDelete,
		s.ownerOf(req.Exclusive), map[string]any(req.Arguments), args)

	dlx, dlxKey, vhostName := args.deadLetterEx, args.deadLetterKey, s.vh.name
	ch := s.vh.dlxCh
	q.deadLetter = func(msg *plugin.Message, reason string) {
		if ch == nil {
			return
		}
		select {
		case ch <- deadLetterEntry{vhost: vhostName, msg: msg, exchange: dlx, routingKey: dlxKey, reason: reason}:
		default:
			// 派发器积压：明确记录并丢弃，而不是无限堆积拖垮内核
			s.log.Warn("死信派发队列已满，丢弃死信", "queue", name, "reason", reason)
		}
	}
	return q
}

// ---------- 交换机 ----------

func (s *vhostSession) DeclareExchange(req plugin.ExchangeDeclare) error {
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
	if err := s.perm.allowWrite(source); err != nil {
		return err
	}
	src, ok := s.vh.getExchange(source)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", source, s.vh.name)
	}
	if _, ok := s.vh.getExchange(destination); !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", destination, s.vh.name)
	}
	src.addExchangeBinding(routingKey, destination, arguments)
	return nil
}

func (s *vhostSession) UnbindExchange(destination, source, routingKey string, arguments map[string]any) error {
	if err := s.perm.allowWrite(source); err != nil {
		return err
	}
	src, ok := s.vh.getExchange(source)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", source, s.vh.name)
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
	// 参数先解析：非法参数必须在创建任何东西之前就报 406
	args, err := parseQueueArgs(req.Arguments)
	if err != nil {
		return plugin.QueueInfo{}, err
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
		s.addQueue(s.newQueueIn(name, req, args), args)
		s.log.Debug("队列已声明（服务端命名）", "queue", name,
			"ttl", args.messageTTL, "dlx", args.deadLetterEx, "max_length", args.maxLength)
		return plugin.QueueInfo{Name: name}, nil
	}

	if q, ok := s.vh.getQueue(req.Name); ok {
		if err := checkQueueEquivalence(q, req); err != nil {
			return plugin.QueueInfo{}, err
		}
		if q.exclusive && q.owner != s.id {
			return plugin.QueueInfo{}, plugin.Errorf(plugin.KindResourceLocked,
				"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'",
				req.Name, s.vh.name)
		}
		ready, consumers := q.stats()
		return plugin.QueueInfo{Name: req.Name, MessageCount: ready, ConsumerCount: consumers}, nil
	}
	if req.Passive {
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

	if !s.addQueue(s.newQueueIn(req.Name, req, args), args) {
		// 并发声明竞争：另一个会话已抢先创建
		return plugin.QueueInfo{Name: req.Name}, nil
	}
	s.log.Debug("队列已声明", "queue", req.Name,
		"ttl", args.messageTTL, "dlx", args.deadLetterEx, "max_length", args.maxLength)
	return plugin.QueueInfo{Name: req.Name}, nil
}

// addQueue 把队列加入 vhost，并按需要登记到定时扫描集合。返回 false 表示同名队列已存在。
func (s *vhostSession) addQueue(q *queue, args queueArgs) bool {
	s.vh.mu.Lock()
	if _, dup := s.vh.queues[q.name]; dup {
		s.vh.mu.Unlock()
		return false
	}
	s.vh.queues[q.name] = q
	if args.messageTTL > 0 || args.expires > 0 {
		s.vh.sweepSet[q.name] = q
	}
	s.vh.mu.Unlock()

	if q.exclusive {
		s.trackExclusive(q.name)
	}
	return true
}

func (s *vhostSession) DeleteQueue(name string, ifUnused, ifEmpty bool) (plugin.QueueInfo, error) {
	if err := s.perm.allowConfigure(name); err != nil {
		return plugin.QueueInfo{}, err
	}
	q, ok := s.vh.getQueue(name)
	if !ok {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, s.vh.name)
	}
	if q.exclusive && q.owner != s.id {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '%s' in vhost '%s'", name, s.vh.name)
	}
	ready, consumers := q.stats()
	if ifUnused && consumers > 0 {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - queue '%s' in vhost '%s' in use", name, s.vh.name)
	}
	if ifEmpty && ready > 0 {
		return plugin.QueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - queue '%s' in vhost '%s' not empty", name, s.vh.name)
	}

	s.removeQueue(name, q)
	s.log.Debug("队列已删除", "queue", name)
	return plugin.QueueInfo{Name: name, MessageCount: ready, ConsumerCount: consumers}, nil
}

func (s *vhostSession) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]any) error {
	if err := s.perm.allowWrite(exchangeName); err != nil {
		return err
	}
	ex, ok := s.vh.getExchange(exchangeName)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", exchangeName, s.vh.name)
	}
	if _, ok := s.vh.getQueue(queueName); !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", queueName, s.vh.name)
	}
	ex.addBinding(routingKey, queueName, arguments)
	return nil
}

func (s *vhostSession) UnbindQueue(queueName, exchangeName, routingKey string, arguments map[string]any) error {
	if err := s.perm.allowWrite(exchangeName); err != nil {
		return err
	}
	ex, ok := s.vh.getExchange(exchangeName)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", exchangeName, s.vh.name)
	}
	if !ex.removeBinding(routingKey, queueName) {
		return plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no binding '%s' between exchange '%s' and queue '%s'",
			routingKey, exchangeName, queueName)
	}
	return nil
}

func (s *vhostSession) PurgeQueue(name string) (uint32, error) {
	if err := s.perm.allowConfigure(name); err != nil {
		return 0, err
	}
	q, ok := s.vh.getQueue(name)
	if !ok {
		return 0, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, s.vh.name)
	}
	return q.purge(), nil
}

// ---------- 发布 ----------

func (s *vhostSession) Publish(msg *plugin.Message, exchangeName, routingKey string, mandatory bool) (plugin.PublishResult, error) {
	var res plugin.PublishResult

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
		res.Routed = true
		res.Rejected = !q.publish(cloneForQueue(msg))
		return res, nil
	}

	targets := s.vh.resolveQueues(ex, routingKey, msg.Properties, map[string]struct{}{})
	s.log.Debug("发布消息", "exchange", exchangeName, "routing_key", routingKey, "targets", len(targets))

	for _, name := range targets {
		q, ok := s.vh.getQueue(name)
		if !ok {
			continue
		}
		res.Routed = true
		if !q.publish(cloneForQueue(msg)) {
			res.Rejected = true
		}
	}
	return res, nil
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
		if q.sweep(now) {
			v.log.Debug("队列已过期，已删除", "queue", q.name)
			v.removeQueue(q)
		}
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
		return true, !q.publish(cloneForQueue(msg))
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
		routed = true
		if !q.publish(cloneForQueue(msg)) {
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
	if err := s.perm.allowRead(sub.Queue); err != nil {
		return "", err
	}
	q, ok := s.vh.getQueue(sub.Queue)
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

	if err := q.subscribe(sub); err != nil {
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
	if q.cancel(consumerTag) {
		s.deleteQueueIfAuto(q.name)
	}
	return nil
}

func (s *vhostSession) Get(queueName string, noAck bool) (*plugin.Delivery, bool, error) {
	if err := s.perm.allowRead(queueName); err != nil {
		return nil, false, err
	}
	q, ok := s.vh.getQueue(queueName)
	if !ok {
		return nil, false, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", queueName, s.vh.name)
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

// deleteQueueIfAuto 在自动删除队列的最后一名消费者离开后删除队列。
func (s *vhostSession) deleteQueueIfAuto(name string) {
	q, ok := s.vh.getQueue(name)
	if !ok {
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

func checkQueueEquivalence(q *queue, req plugin.QueueDeclare) error {
	if q.durable != req.Durable || q.exclusive != req.Exclusive || q.autoDelete != req.AutoDelete {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - inequivalent arg for queue '%s': declared as durable=%t exclusive=%t auto_delete=%t",
			req.Name, q.durable, q.exclusive, q.autoDelete)
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
