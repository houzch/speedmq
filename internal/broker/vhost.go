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
}

// newVHost 创建 vhost 并预声明内置交换机。
func newVHost(name string, log *slog.Logger) *vhost {
	v := &vhost{
		name:      name,
		log:       log.With("vhost", name),
		exchanges: map[string]*exchange{},
		queues:    map[string]*queue{},
		consumers: map[string]*queue{},
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
	vh  *vhost
	id  string // 会话标识，用于独占队列归属判定
	log *slog.Logger

	mu        sync.Mutex
	exclusive map[string]struct{} // 本会话创建的独占队列
	tags      map[string]string   // 本会话创建的消费者标签 → 队列名
	closed    bool
}

var _ plugin.Session = (*vhostSession)(nil)

func newVHostSession(vh *vhost, id string, log *slog.Logger) *vhostSession {
	return &vhostSession{
		vh:        vh,
		id:        id,
		log:       log.With("vhost", vh.name),
		exclusive: map[string]struct{}{},
		tags:      map[string]string{},
	}
}

// ---------- 交换机 ----------

func (s *vhostSession) DeclareExchange(req plugin.ExchangeDeclare) error {
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
	if req.Name == "" {
		if req.Passive {
			return plugin.QueueInfo{}, plugin.Errorf(plugin.KindPreconditionFailed,
				"PRECONDITION_FAILED - cannot passively declare a server-named queue")
		}
		// 服务端生成队列名
		name := generatedQueueName()
		q := newQueue(name, req.Durable, req.Exclusive, req.AutoDelete, s.ownerOf(req.Exclusive), req.Arguments)
		s.vh.mu.Lock()
		s.vh.queues[name] = q
		s.vh.mu.Unlock()
		if req.Exclusive {
			s.trackExclusive(name)
		}
		s.log.Debug("队列已声明（服务端命名）", "queue", name)
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

	q := newQueue(req.Name, req.Durable, req.Exclusive, req.AutoDelete, s.ownerOf(req.Exclusive), req.Arguments)
	s.vh.mu.Lock()
	if _, dup := s.vh.queues[req.Name]; dup {
		s.vh.mu.Unlock()
		return plugin.QueueInfo{Name: req.Name}, nil
	}
	s.vh.queues[req.Name] = q
	s.vh.mu.Unlock()

	if req.Exclusive {
		s.trackExclusive(req.Name)
	}
	s.log.Debug("队列已声明", "queue", req.Name)
	return plugin.QueueInfo{Name: req.Name}, nil
}

func (s *vhostSession) DeleteQueue(name string, ifUnused, ifEmpty bool) (plugin.QueueInfo, error) {
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
	q, ok := s.vh.getQueue(name)
	if !ok {
		return 0, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no queue '%s' in vhost '%s'", name, s.vh.name)
	}
	return q.purge(), nil
}

// ---------- 发布 ----------

func (s *vhostSession) Publish(msg *plugin.Message, exchangeName, routingKey string, mandatory bool) (bool, error) {
	ex, ok := s.vh.getExchange(exchangeName)
	if !ok {
		return false, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - no exchange '%s' in vhost '%s'", exchangeName, s.vh.name)
	}
	if ex.internal && exchangeName != defaultExchange {
		// 默认交换机在元数据上也是 internal，但客户端按队列名发布到它是标准用法，
		// 因此这里只拦住"其他内部交换机"——与 RabbitMQ 的可观察行为一致。
		return false, plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - cannot publish to internal exchange '%s' in vhost '%s'",
			exchangeName, s.vh.name)
	}

	// 默认交换机没有显式绑定：每个队列都以"自己的名字"隐式绑定在它上面。
	// 这是客户端最常用的发布方式（不写 exchange，直接以队列名当 routing key）。
	if exchangeName == defaultExchange {
		q, ok := s.vh.getQueue(routingKey)
		if !ok {
			return false, nil
		}
		clone := *msg
		clone.Redelivered = false
		q.publish(&clone)
		return true, nil
	}

	targets := s.resolveQueues(ex, routingKey, msg.Properties, map[string]struct{}{})
	s.log.Debug("发布消息", "exchange", exchangeName, "routing_key", routingKey,
		"targets", len(targets))

	routed := false
	for _, name := range targets {
		q, ok := s.vh.getQueue(name)
		if !ok {
			continue
		}
		// 每个队列一份消息副本：Redelivered 等状态是队列级的，不能跨队列共享
		clone := *msg
		clone.Redelivered = false
		q.publish(&clone)
		routed = true
	}
	return routed, nil
}

// resolveQueues 展开交换机路由，递归处理交换机到交换机的绑定。
func (s *vhostSession) resolveQueues(ex *exchange, routingKey string, props plugin.Properties, visited map[string]struct{}) []string {
	if _, seen := visited[ex.name]; seen {
		return nil // 防止交换机绑定成环导致无限递归
	}
	visited[ex.name] = struct{}{}

	queues, exchanges := ex.routeAll(routingKey, props)
	out := queues
	for _, name := range exchanges {
		next, ok := s.vh.getExchange(name)
		if !ok {
			continue
		}
		out = append(out, s.resolveQueues(next, routingKey, props, visited)...)
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

// ---------- 消费 ----------

func (s *vhostSession) Consume(sub plugin.Subscription) (string, error) {
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
	s.forgetConsumer(consumerTag)
	if q.cancel(consumerTag) {
		s.deleteQueueIfAuto(q.name)
	}
	return nil
}

func (s *vhostSession) Get(queueName string, noAck bool) (*plugin.Delivery, bool, error) {
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

// removeQueue 从 vhost 中移除队列，并清理指向它的绑定与消费者索引。
func (s *vhostSession) removeQueue(name string, q *queue) {
	s.vh.mu.Lock()
	delete(s.vh.queues, name)
	for _, ex := range s.vh.exchanges {
		ex.removeQueueBindings(name)
	}
	for tag, cq := range s.vh.consumers {
		if cq == q {
			delete(s.vh.consumers, tag)
		}
	}
	s.vh.mu.Unlock()

	s.mu.Lock()
	delete(s.exclusive, name)
	s.mu.Unlock()

	// 通知消费者被取消了（对齐 RabbitMQ 的 consumer cancel：队列被删时服务端主动 basic.cancel）
	for _, tag := range q.cancelAllConsumers() {
		s.log.Debug("队列被删除，消费者被取消", "queue", name, "consumer_tag", tag)
	}
	q.close()
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
