package broker

import (
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 死信原因（写入 x-death 的 reason 字段，取值与 RabbitMQ 一致）。
const (
	deathReasonRejected = "rejected"
	deathReasonExpired  = "expired"
	deathReasonMaxLen   = "maxlen"
)

// messageOverhead 是每条消息在内存中的近似固定开销（结构体、切片头、属性表指针等），
// 用于管理面的"队列内存占用"估算。它是估算而非精确值，因此对齐 RabbitMQ 的做法：
// 只承诺"同量级、可比较"，不承诺与 RSS 逐字节吻合。
const messageOverhead = 64

// queuedMsg 是队列中的一条消息及其队列内状态。
type queuedMsg struct {
	msg *plugin.Message
	// seq 是队列内单调递增序号，作为未确认消息的标识（与协议层的 delivery tag 无关）。
	seq uint64
	// consumerTag 记录它当前被哪个消费者持有（未确认状态时有效）。
	consumerTag string
	// expireAt 是到期时刻；零值表示不过期。
	expireAt time.Time
	// priority 是消息优先级（仅优先级队列有意义）。
	priority uint8
	// storeSeq 是持久化日志中的序号；persisted 表示该消息已写入磁盘、需要按序号回收。
	storeSeq  uint64
	persisted bool
}

// consumer 是队列上的一个消费者。
type consumer struct {
	sub      plugin.Subscription
	inFlight int
}

// hasCapacity 判断该消费者是否还有投递额度。
//
// no-ack 与 prefetch=0 都表示不限制；其余情况以 in-flight 数为闸门。
func (c *consumer) hasCapacity() bool {
	if c.sub.NoAck || c.sub.Prefetch == 0 {
		return true
	}
	return c.inFlight < int(c.sub.Prefetch)
}

// queue 是内核的队列：FIFO、竞争消费、prefetch、TTL、死信与长度限制。
//
// 投递模型：状态变更（发布 / 订阅 / 确认 / 重入队）时同步触发 dispatch，
// 并用 dispatching 标志防止重入。每队列独立投递协程与信用流控属于流控里程碑的工作，
// 这里先用更简单且行为等价的模型；届时替换不影响对外语义。
type queue struct {
	name       string
	durable    bool
	exclusive  bool
	autoDelete bool
	owner      string // 独占队列归属的会话标识；空表示非独占
	arguments  map[string]any
	args       queueArgs
	log        *slog.Logger

	// deadLetter 把消息交给 vhost 投递到死信交换机；由 vhost 在创建队列时注入。
	deadLetter func(msg *plugin.Message, reason string)

	// store 是持久化存储；非 durable 队列或未开启落盘时为 nil。
	// storeSeq 是持久化序号分配器（只在 store 非 nil 时使用）。
	store    *store.QueueStore
	storeSeq uint64

	mu          sync.Mutex
	ready       []*queuedMsg
	readyBytes  int64
	unacked     map[uint64]*queuedMsg
	consumers   []*consumer
	rr          int
	nextSeq     uint64
	hadConsumer bool
	closed      bool
	dispatching bool
	lastUsed    time.Time

	// 累计计数器：管理面与 Prometheus 指标用（只增不减）。
	published atomic.Uint64
	delivered atomic.Uint64
	gotten    atomic.Uint64
	acked     atomic.Uint64
}

func newQueue(name string, durable, exclusive, autoDelete bool, owner string,
	arguments map[string]any, args queueArgs, log *slog.Logger) *queue {
	return &queue{
		name:       name,
		durable:    durable,
		exclusive:  exclusive,
		autoDelete: autoDelete,
		owner:      owner,
		arguments:  arguments,
		args:       args,
		log:        log.With("queue", name),
		unacked:    map[uint64]*queuedMsg{},
		lastUsed:   time.Now(),
	}
}

// needsTimer 表示该队列需要被扫描（有过期消息或队列自身会过期）。
func (q *queue) needsTimer() bool {
	if q == nil {
		return false
	}
	return q.args.messageTTL > 0 || q.args.expires > 0
}

// stats 返回就绪消息数与消费者数。
func (q *queue) stats() (ready, consumers uint32) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return uint32(len(q.ready)), uint32(len(q.consumers))
}

// QueueSnapshot 是队列的只读视图（管理面与指标用）。
//
// 字段刻意与 RabbitMQ Management API 的队列对象同名，让"复用现有运维工具"
// 这件事停留在字段映射，而不是语义翻译。
type QueueSnapshot struct {
	VHost                string
	Name                 string
	Durable              bool
	AutoDelete           bool
	Exclusive            bool
	Arguments            map[string]any
	Ready                int
	Unacked              int
	ConsumerCount        int
	ConsumerTags         []string
	ExclusiveConsumerTag string
	// MemoryBytes 是内存占用的估算值（就绪消息体 + 每消息固定开销）。
	MemoryBytes int64
	// IdleSince 是该队列最近一次被访问（发布/投递/声明）的时刻。
	IdleSince time.Time
	// 累计计数。
	Published uint64
	Delivered uint64
	Gotten    uint64
	Acked     uint64
}

// snapshot 返回队列的只读快照。vhostName 由调用方提供（队列自身不持有 vhost 名）。
func (q *queue) snapshot(vhostName string) QueueSnapshot {
	s := QueueSnapshot{
		VHost:      vhostName,
		Name:       q.name,
		Durable:    q.durable,
		AutoDelete: q.autoDelete,
		Exclusive:  q.exclusive,
		Arguments:  q.arguments,
		Published:  q.published.Load(),
		Delivered:  q.delivered.Load(),
		Gotten:     q.gotten.Load(),
		Acked:      q.acked.Load(),
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	s.Ready = len(q.ready)
	s.Unacked = len(q.unacked)
	s.ConsumerCount = len(q.consumers)
	s.MemoryBytes = q.readyBytes + int64(len(q.ready))*messageOverhead
	s.IdleSince = q.lastUsed
	for _, c := range q.consumers {
		s.ConsumerTags = append(s.ConsumerTags, c.sub.Tag)
		if c.sub.Exclusive {
			s.ExclusiveConsumerTag = c.sub.Tag
		}
	}
	return s
}

// ConsumerSnapshot 是消费者的只读视图。
type ConsumerSnapshot struct {
	VHost       string
	Queue       string
	Tag         string
	AckRequired bool
	Prefetch    uint16
	Exclusive   bool
	Arguments   map[string]any
}

// consumerSnapshots 返回本队列消费者快照。
func (q *queue) consumerSnapshots(vhostName string) []ConsumerSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]ConsumerSnapshot, 0, len(q.consumers))
	for _, c := range q.consumers {
		out = append(out, ConsumerSnapshot{
			VHost:       vhostName,
			Queue:       q.name,
			Tag:         c.sub.Tag,
			AckRequired: !c.sub.NoAck,
			Prefetch:    c.sub.Prefetch,
			Exclusive:   c.sub.Exclusive,
			// 消费者参数（如 x-priority）当前未在内核侧保留，统一给空表，
			// 避免管理面出现 null（客户端会当成"字段缺失"）。
			Arguments: map[string]any{},
		})
	}
	return out
}

// touchLocked 记录队列被访问的时刻（x-expires 用）。
func (q *queue) touchLocked() { q.lastUsed = time.Now() }

// ---------------------------------------------------------------------------
// 发布
// ---------------------------------------------------------------------------

// publish 把消息入队并触发一次投递。
//
// 返回值 accepted 为 false 表示被长度限制拒绝或持久化失败；
// commit 非 nil 表示该消息需要落盘，调用方（协议层）应等待它后再回 confirm。
func (q *queue) publish(msg *plugin.Message) (accepted bool, commit *store.Commit) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return true, nil // 队列已删：投递失败由上层忽略，不算"被拒绝"
	}
	q.touchLocked()

	item := &queuedMsg{msg: msg, priority: msg.Properties.Priority}
	if ttl := q.args.effectiveTTL(parseExpiration(msg.Properties.Expiration)); ttl > 0 {
		item.expireAt = time.Now().Add(ttl)
	}

	if q.wouldExceedLocked(item) {
		switch q.args.overflow {
		case overflowRejectPublish:
			q.mu.Unlock()
			return false, nil
		case overflowRejectPublishDLX:
			q.mu.Unlock()
			if q.args.hasDeadLetter() {
				// reject-publish-dlx：把被拒的消息直接送去死信
				q.deadLetterLocked(item, deathReasonMaxLen)
			}
			return false, nil
		default:
			// drop-head：先挤出队首腾出空间
			q.dropHeadForRoomLocked(item)
		}
	}

	// 持久化：只有 durable 队列 + 持久消息（delivery-mode=2）才写盘，与 RabbitMQ 一致。
	// 写入失败时拒绝发布而不是静默入内存 —— 否则客户端会收到 confirm 却拿不到持久性。
	if q.store != nil && msg.Properties.Persistent() {
		q.storeSeq++
		item.storeSeq = q.storeSeq
		c, err := q.store.Append(item.storeSeq, msg)
		if err != nil {
			q.mu.Unlock()
			q.log.Error("消息落盘失败，已拒绝本次发布", "queue", q.name, "err", err)
			return false, nil
		}
		item.persisted = true
		commit = c
	}

	q.insertLocked(item)
	q.published.Add(1)
	q.mu.Unlock()
	q.dispatch()
	return true, commit
}

// wouldExceedLocked 判断加入该消息是否会超过长度限制。
func (q *queue) wouldExceedLocked(item *queuedMsg) bool {
	if q.args.maxLength > 0 && int64(len(q.ready))+1 > q.args.maxLength {
		return true
	}
	if q.args.maxLengthBytes > 0 && q.readyBytes+messageSize(item.msg) > q.args.maxLengthBytes {
		return true
	}
	return false
}

// dropHeadForRoomLocked 按 drop-head 策略挤出队首，直到能容纳新消息。
func (q *queue) dropHeadForRoomLocked(item *queuedMsg) {
	for len(q.ready) > 0 && q.wouldExceedLocked(item) {
		head := q.ready[0]
		q.ready = q.ready[1:]
		q.readyBytes -= messageSize(head.msg)
		// 被挤出的消息若有死信配置则进死信，否则丢弃
		q.deadLetterLocked(head, deathReasonMaxLen)
	}
}

// insertLocked 把消息放入就绪列表。
//
// 普通队列追加到队尾（FIFO）；优先级队列按优先级降序插入，同优先级保持 FIFO。
func (q *queue) insertLocked(item *queuedMsg) {
	q.readyBytes += messageSize(item.msg)
	if q.args.maxPriority == 0 {
		q.ready = append(q.ready, item)
		return
	}
	idx := len(q.ready)
	for i, m := range q.ready {
		if m.priority < item.priority {
			idx = i
			break
		}
	}
	q.ready = append(q.ready, nil)
	copy(q.ready[idx+1:], q.ready[idx:])
	q.ready[idx] = item
}

// insertFrontLocked 把消息放回队首（重新入队且非优先级队列时使用，用于尽量还原原顺序）。
func (q *queue) insertFrontLocked(items []*queuedMsg) {
	if len(items) == 0 {
		return
	}
	if q.args.maxPriority > 0 {
		// 优先级队列：重入队同样按优先级归位，否则优先级契约会被打破
		for _, it := range items {
			q.insertLocked(it)
		}
		return
	}
	sortBySeq(items)
	for _, it := range items {
		q.readyBytes += messageSize(it.msg)
	}
	q.ready = append(items, q.ready...)
}

// ---------------------------------------------------------------------------
// 消费与确认
// ---------------------------------------------------------------------------

// subscribe 注册消费者；消费者独占性冲突时返回错误。
func (q *queue) subscribe(sub plugin.Subscription) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return plugin.Errorf(plugin.KindNotFound, "NOT_FOUND - queue '%s' is closed", q.name)
	}
	if err := q.checkConsumerLocked(sub.Exclusive); err != nil {
		q.mu.Unlock()
		return err
	}
	q.touchLocked()
	q.consumers = append(q.consumers, &consumer{sub: sub})
	q.hadConsumer = true
	q.mu.Unlock()
	q.dispatch()
	return nil
}

// checkConsumerLocked 检查消费者独占性：独占消费者与已有消费者不能共存。
func (q *queue) checkConsumerLocked(exclusive bool) error {
	if len(q.consumers) == 0 {
		return nil
	}
	if exclusive {
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - queue '%s' in exclusive use", q.name)
	}
	for _, c := range q.consumers {
		if c.sub.Exclusive {
			return plugin.Errorf(plugin.KindAccessRefused,
				"ACCESS_REFUSED - queue '%s' in exclusive use by another consumer", q.name)
		}
	}
	return nil
}

// cancel 移除消费者并把它未确认的消息重新入队。
// 返回值表示该队列是否应被删除（自动删除队列且已无消费者）。
func (q *queue) cancel(tag string) (shouldDelete bool) {
	q.mu.Lock()
	for i, c := range q.consumers {
		if c.sub.Tag == tag {
			q.consumers = append(q.consumers[:i], q.consumers[i+1:]...)
			break
		}
	}
	q.requeueByConsumerLocked(tag)
	q.touchLocked()
	shouldDelete = q.autoDelete && q.hadConsumer && len(q.consumers) == 0
	q.mu.Unlock()

	q.dispatch()
	return shouldDelete
}

// cancelAllConsumers 取消队列上所有消费者（会话关闭时调用），返回被取消的标签列表。
func (q *queue) cancelAllConsumers() []string {
	q.mu.Lock()
	tags := make([]string, 0, len(q.consumers))
	for _, c := range q.consumers {
		tags = append(tags, c.sub.Tag)
		q.requeueByConsumerLocked(c.sub.Tag)
	}
	q.consumers = nil
	q.mu.Unlock()
	return tags
}

// requeueByConsumerLocked 把某消费者未确认的消息放回队首，保持原有相对顺序。
func (q *queue) requeueByConsumerLocked(tag string) {
	var back []*queuedMsg
	for seq, item := range q.unacked {
		if item.consumerTag != tag {
			continue
		}
		delete(q.unacked, seq)
		item.msg.Redelivered = true
		item.consumerTag = ""
		back = append(back, item)
	}
	q.insertFrontLocked(back)
}

// purge 清空就绪消息，返回清除条数（不含未确认消息，与 AMQP 语义一致）。
func (q *queue) purge() uint32 {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := uint32(len(q.ready))
	for _, item := range q.ready {
		// 被清掉的消息同样要在索引里标记删除，否则重启后会全部复活
		q.storeAckLocked(item)
	}
	q.ready = nil
	q.readyBytes = 0
	return n
}

// close 关闭队列、丢弃内存中的消息并删除其磁盘数据。
func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.ready = nil
	q.readyBytes = 0
	q.unacked = map[uint64]*queuedMsg{}
	q.consumers = nil
	st := q.store
	q.store = nil
	q.mu.Unlock()

	if st != nil {
		if err := st.Remove(); err != nil {
			q.log.Warn("删除队列存储失败", "queue", q.name, "err", err)
		}
	}
}

// discardStore 关闭存储但保留磁盘数据。
//
// 只用于"并发声明同一 durable 队列时落败"的场景：刚打开的存储句柄要关掉，
// 但磁盘上的数据属于那个已存在的队列，不能删。
func (q *queue) discardStore() {
	if q.store != nil {
		_ = q.store.Close()
	}
}

// restore 把从磁盘恢复出来的消息放回队首。
//
// 恢复出来的消息统一标记 redelivered=true：软状态（谁投递过、是否已投递）不落盘，
// 无法区分"曾投递未确认"与"从未投递"，而漏标 redelivered 会让客户端把重复投递
// 当成首次投递，是更危险的错误方向（设计 5.3.8）。
func (q *queue) restore(recovered []store.Recovered) {
	if len(recovered) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, r := range recovered {
		msg := r.Message
		msg.Redelivered = true
		item := &queuedMsg{
			msg:       msg,
			priority:  msg.Properties.Priority,
			storeSeq:  r.Seq,
			persisted: true,
		}
		if ttl := q.args.effectiveTTL(parseExpiration(msg.Properties.Expiration)); ttl > 0 {
			item.expireAt = time.Now().Add(ttl)
		}
		q.insertLocked(item)
		if r.Seq > q.storeSeq {
			q.storeSeq = r.Seq
		}
	}
	q.log.Info("队列已恢复持久化消息", "queue", q.name, "messages", len(recovered))
}

// settle 结算一条投递。
//
// 关键区别：SettleAck 是"正常消费完成"，SettleReject 是"拒绝"。
// 两者都丢弃消息，但只有 reject 才应该进死信 —— 把 ack 也当 reject 会让
// 每条正常消费的消息都被复制进死信队列。
func (q *queue) settle(item *queuedMsg, action plugin.SettleAction) {
	q.mu.Lock()
	if _, ok := q.unacked[item.seq]; !ok {
		// 已结算过（客户端重复 ack 等）：忽略，不报错
		q.mu.Unlock()
		return
	}
	delete(q.unacked, item.seq)
	q.decInFlightLocked(item.consumerTag)

	switch action {
	case plugin.SettleRequeue:
		if !q.closed {
			item.msg.Redelivered = true
			item.consumerTag = ""
			q.insertFrontLocked([]*queuedMsg{item})
		}
	case plugin.SettleReject:
		q.deadLetterLocked(item, deathReasonRejected)
	default:
		// SettleAck：正常消费完成，直接丢弃（并在索引中标记，避免重启后复活）
		q.acked.Add(1)
		q.storeAckLocked(item)
	}
	q.mu.Unlock()
}

// get 主动拉取一条消息。noAck 为 true 时投递即结算，不进入未确认集合。
func (q *queue) get(noAck bool) (*plugin.Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.touchLocked()

	item := q.popLiveHeadLocked()
	if item == nil {
		return nil, false
	}
	q.gotten.Add(1)
	if noAck {
		// no-ack 投递即结算：消息已归消费者，索引要同步标记，否则重启后会重复投递
		q.storeAckLocked(item)
		return &plugin.Delivery{
			Message:     item.msg,
			Queue:       q.name,
			Redelivered: item.msg.Redelivered,
			Settle:      func(plugin.SettleAction) {}, // no-ack 无需结算
		}, true
	}

	q.nextSeq++
	item.seq = q.nextSeq
	q.unacked[item.seq] = item
	item.consumerTag = ""

	d := &plugin.Delivery{
		Message:     item.msg,
		Queue:       q.name,
		Redelivered: item.msg.Redelivered,
	}
	d.Settle = func(action plugin.SettleAction) {
		q.settle(item, action)
		q.dispatch()
	}
	return d, true
}

// popLiveHeadLocked 取出队首的有效消息，途中把已过期的消息转入死信。
//
// 只在队首检查过期：这与 RabbitMQ 的队列级 TTL 行为一致（队首即最老），
// 可以保证"绝不把已过期消息投给消费者"，又不必扫描整个队列。
func (q *queue) popLiveHeadLocked() *queuedMsg {
	now := time.Now()
	for len(q.ready) > 0 {
		head := q.ready[0]
		if head.expireAt.IsZero() || head.expireAt.After(now) {
			q.ready = q.ready[1:]
			q.readyBytes -= messageSize(head.msg)
			return head
		}
		q.ready = q.ready[1:]
		q.readyBytes -= messageSize(head.msg)
		q.deadLetterLocked(head, deathReasonExpired)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 定时扫描（TTL 与队列过期）
// ---------------------------------------------------------------------------

// sweep 清理过期消息；返回队列自身是否已过期（需要被删除）。
func (q *queue) sweep(now time.Time) (expired bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	// 过期消息只能从队首开始清理（与 RabbitMQ 的队首过期语义一致）
	for len(q.ready) > 0 {
		head := q.ready[0]
		if head.expireAt.IsZero() || head.expireAt.After(now) {
			break
		}
		q.ready = q.ready[1:]
		q.readyBytes -= messageSize(head.msg)
		q.deadLetterLocked(head, deathReasonExpired)
	}
	if q.args.expires > 0 && len(q.consumers) == 0 && now.Sub(q.lastUsed) >= q.args.expires {
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// 投递
// ---------------------------------------------------------------------------

// dispatch 尽可能把就绪消息投递给有额度的消费者。
func (q *queue) dispatch() {
	q.mu.Lock()
	if q.closed || q.dispatching {
		q.mu.Unlock()
		return
	}
	q.dispatching = true

	for {
		batch := q.selectBatchLocked()
		if len(batch) == 0 {
			q.dispatching = false
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
		for _, item := range batch {
			err := item.sub.Deliver(item.delivery)
			if err != nil {
				// 投递失败（多半是连接已断）：消息重新入队，避免丢失
				item.delivery.Settle(plugin.SettleRequeue)
			}
		}
		q.mu.Lock()
	}
}

// batchItem 是一次待投递。
type batchItem struct {
	sub      plugin.Subscription
	delivery *plugin.Delivery
}

// selectBatchLocked 在锁内选出本批要投递的消息，并立即把它们置为"未确认"。
//
// 必须在锁内完成状态迁移，投递动作在锁外执行 —— 否则向慢消费者写 socket 会阻塞整个队列。
func (q *queue) selectBatchLocked() []batchItem {
	if q.closed || len(q.consumers) == 0 {
		return nil
	}
	var batch []batchItem
	for {
		c := q.nextConsumerLocked()
		if c == nil {
			break
		}
		item := q.popLiveHeadLocked()
		if item == nil {
			break
		}
		q.delivered.Add(1)

		if c.sub.NoAck {
			// no-ack 投递即结算：索引同步标记，避免重启后重复投递
			q.storeAckLocked(item)
			d := &plugin.Delivery{
				Message:     item.msg,
				Queue:       q.name,
				ConsumerTag: c.sub.Tag,
				Redelivered: item.msg.Redelivered,
				Settle:      func(plugin.SettleAction) {},
			}
			batch = append(batch, batchItem{sub: c.sub, delivery: d})
			continue
		}

		q.nextSeq++
		item.seq = q.nextSeq
		item.consumerTag = c.sub.Tag
		q.unacked[item.seq] = item
		c.inFlight++

		d := &plugin.Delivery{
			Message:     item.msg,
			Queue:       q.name,
			ConsumerTag: c.sub.Tag,
			Redelivered: item.msg.Redelivered,
		}
		d.Settle = func(action plugin.SettleAction) {
			q.settle(item, action)
			q.dispatch()
		}
		batch = append(batch, batchItem{sub: c.sub, delivery: d})
	}
	return batch
}

// nextConsumerLocked 按轮询顺序找一个还有额度的消费者。
func (q *queue) nextConsumerLocked() *consumer {
	n := len(q.consumers)
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		idx := (q.rr + i) % n
		c := q.consumers[idx]
		if c.hasCapacity() {
			q.rr = (idx + 1) % n
			return c
		}
	}
	return nil
}

func (q *queue) decInFlightLocked(tag string) {
	if tag == "" {
		return
	}
	for _, c := range q.consumers {
		if c.sub.Tag == tag {
			if c.inFlight > 0 {
				c.inFlight--
			}
			return
		}
	}
}

// ---------------------------------------------------------------------------
// 死信
// ---------------------------------------------------------------------------

// deadLetterLocked 把消息转成死信并交给内核的死信派发器。调用方需持有 q.mu。
//
// 这里可以在持锁状态下调用回调：内核的 deadLetter 实现只做"入队到派发器"，
// 不会回调本队列，因此不存在自锁（死信目标恰好是本队列）的风险。
func (q *queue) deadLetterLocked(item *queuedMsg, reason string) {
	// 消息离开本队列（无论是否真的进了 DLQ）：索引里要记一笔，否则重启后它会"复活"
	q.storeAckLocked(item)
	if q.deadLetter == nil || q.args.deadLetterEx == "" {
		return
	}
	// 复制一份：原消息可能仍被未确认集合引用，不能就地改它的头
	clone := *item.msg
	addDeathHeader(&clone, reason, q.name, time.Now())
	q.deadLetter(&clone, reason)
}

// storeAckLocked 在队列索引中把该消息标记为已离开队列。调用方需持有 q.mu。
//
// 用 persisted 标志保证只写一次：同一消息可能经过 requeue → 再投递 → 再结算，
// 重复写 ack 记录只会让索引无谓膨胀。
func (q *queue) storeAckLocked(item *queuedMsg) {
	if q.store == nil || !item.persisted {
		return
	}
	item.persisted = false
	if err := q.store.Ack(item.storeSeq); err != nil {
		q.log.Error("写入索引删除标记失败", "queue", q.name, "seq", item.storeSeq, "err", err)
	}
}

// addDeathHeader 在消息头里追加 x-death 记录（简化版：reason / queue / time / count）。
func addDeathHeader(msg *plugin.Message, reason, queue string, now time.Time) {
	if msg.Properties.Headers == nil {
		msg.Properties.Headers = map[string]any{}
	}
	entry := map[string]any{
		"reason": reason,
		"queue":  queue,
		"time":   now.Unix(),
		"count":  int64(1),
		// 保留原始路由信息：处理死信的代码常靠这两项判断"它当初从哪来"
		"exchange":     msg.Exchange,
		"routing-keys": []any{msg.RoutingKey},
	}
	prev, _ := msg.Properties.Headers["x-death"].([]any)
	if len(prev) > 0 {
		if first, ok := prev[0].(map[string]any); ok && first["reason"] == reason && first["queue"] == queue {
			if c, ok := first["count"].(int64); ok {
				entry["count"] = c + 1
			}
			msg.Properties.Headers["x-death"] = append([]any{entry}, prev[1:]...)
			return
		}
		msg.Properties.Headers["x-death"] = append([]any{entry}, prev...)
		return
	}
	msg.Properties.Headers["x-death"] = []any{entry}
}

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

// parseExpiration 解析消息级 TTL（AMQP 用毫秒字符串承载）。
//
// 值非法时按"不过期"处理：RabbitMQ 也对无法解析的 expiration 宽容对待，
// 这里选择同样的行为，避免一条畸形属性让整条消息投不出去。
func parseExpiration(s string) time.Duration {
	if s == "" {
		return 0
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// messageSize 估算一条消息占用的字节数。
//
// 长度限制按消息体大小计（不含属性与协议的固定开销），这是与 RabbitMQ 的
// x-max-length-bytes 存在的已知差异：我们的限额只近似它对内存的约束。
func messageSize(msg *plugin.Message) int64 {
	return int64(len(msg.Body))
}

func sortBySeq(items []*queuedMsg) {
	// 队列长度通常很小，插入排序足够且避免额外的排序开销
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].seq > items[j].seq; j-- {
			items[j-1], items[j] = items[j], items[j-1]
		}
	}
}
