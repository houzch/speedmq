package broker

import (
	"sync"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// queuedMsg 是队列中的一条消息及其队列内序号。
type queuedMsg struct {
	msg *plugin.Message
	// seq 是队列内单调递增序号，作为未确认消息的标识（与协议层的 delivery tag 无关）。
	seq uint64
	// consumerTag 记录它当前被哪个消费者持有（未确认状态时有效）。
	consumerTag string
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

// queue 是内核的队列：FIFO、竞争消费、prefetch 与未确认消息跟踪。
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

	mu          sync.Mutex
	ready       []*queuedMsg
	unacked     map[uint64]*queuedMsg
	consumers   []*consumer
	rr          int
	nextSeq     uint64
	hadConsumer bool
	closed      bool
	dispatching bool
}

func newQueue(name string, durable, exclusive, autoDelete bool, owner string, args map[string]any) *queue {
	return &queue{
		name:       name,
		durable:    durable,
		exclusive:  exclusive,
		autoDelete: autoDelete,
		owner:      owner,
		arguments:  args,
		unacked:    map[uint64]*queuedMsg{},
	}
}

// stats 返回就绪消息数与消费者数。
func (q *queue) stats() (ready, consumers uint32) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return uint32(len(q.ready)), uint32(len(q.consumers))
}

// publish 把消息入队并触发一次投递。
func (q *queue) publish(msg *plugin.Message) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.ready = append(q.ready, &queuedMsg{msg: msg})
	q.mu.Unlock()
	q.dispatch()
}

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
	if len(back) == 0 {
		return
	}
	// 按 seq 排序后置于队首，尽量还原原始顺序
	sortBySeq(back)
	q.ready = append(back, q.ready...)
}

// purge 清空就绪消息，返回清除条数（不含未确认消息，与 AMQP 语义一致）。
func (q *queue) purge() uint32 {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := uint32(len(q.ready))
	q.ready = nil
	return n
}

// close 关闭队列并丢弃消息。
func (q *queue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.ready = nil
	q.unacked = map[uint64]*queuedMsg{}
	q.consumers = nil
}

// settle 结算一条投递：requeue=false 为确认丢弃，true 为重新入队。
func (q *queue) settle(item *queuedMsg, requeue bool) {
	q.mu.Lock()
	if _, ok := q.unacked[item.seq]; !ok {
		// 已结算过（客户端重复 ack 等）：忽略，不报错
		q.mu.Unlock()
		return
	}
	delete(q.unacked, item.seq)
	q.decInFlightLocked(item.consumerTag)
	if requeue && !q.closed {
		item.msg.Redelivered = true
		item.consumerTag = ""
		q.ready = append([]*queuedMsg{item}, q.ready...)
	}
	q.mu.Unlock()
}

// get 主动拉取一条消息。noAck 为 true 时投递即结算，不进入未确认集合。
func (q *queue) get(noAck bool) (*plugin.Delivery, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.ready) == 0 {
		return nil, false
	}
	item := q.ready[0]
	q.ready = q.ready[1:]

	if noAck {
		return &plugin.Delivery{
			Message:     item.msg,
			Queue:       q.name,
			Redelivered: item.msg.Redelivered,
			Settle:      func(bool) {}, // no-ack 无需结算
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
	d.Settle = func(requeue bool) {
		q.settle(item, requeue)
		q.dispatch()
	}
	return d, true
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
				q.settleMessage(item.msg, true)
				continue
			}
			if item.sub.NoAck {
				// no-ack：投递即结算
				q.settleMessage(item.msg, false)
			}
		}
		q.mu.Lock()
	}
}

// batchItem 是一次待投递。
type batchItem struct {
	sub      plugin.Subscription
	delivery *plugin.Delivery
	msg      *plugin.Message
}

// selectBatchLocked 在锁内选出本批要投递的消息，并立即把它们置为"未确认"。
//
// 必须在锁内完成状态迁移，投递动作在锁外执行 —— 否则向慢消费者写 socket 会阻塞整个队列。
func (q *queue) selectBatchLocked() []batchItem {
	if q.closed || len(q.ready) == 0 || len(q.consumers) == 0 {
		return nil
	}
	var batch []batchItem
	for len(q.ready) > 0 {
		c := q.nextConsumerLocked()
		if c == nil {
			break
		}
		item := q.ready[0]
		q.ready = q.ready[1:]

		if c.sub.NoAck {
			d := &plugin.Delivery{
				Message:     item.msg,
				Queue:       q.name,
				ConsumerTag: c.sub.Tag,
				Redelivered: item.msg.Redelivered,
				Settle:      func(bool) {},
			}
			batch = append(batch, batchItem{sub: c.sub, delivery: d, msg: item.msg})
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
		d.Settle = func(requeue bool) {
			q.settle(item, requeue)
			q.dispatch()
		}
		batch = append(batch, batchItem{sub: c.sub, delivery: d, msg: item.msg})
	}
	return batch
}

// nextConsumerLocked 按轮询顺序找一个还有额度的消费者。
func (q *queue) nextConsumerLocked() *consumer {
	n := len(q.consumers)
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

// settleMessage 按消息指针结算（投递回调失败或 no-ack 时使用）。
func (q *queue) settleMessage(msg *plugin.Message, requeue bool) {
	q.mu.Lock()
	for seq, item := range q.unacked {
		if item.msg != msg {
			continue
		}
		delete(q.unacked, seq)
		q.decInFlightLocked(item.consumerTag)
		if requeue && !q.closed {
			item.msg.Redelivered = true
			item.consumerTag = ""
			q.ready = append([]*queuedMsg{item}, q.ready...)
		}
		q.mu.Unlock()
		return
	}
	q.mu.Unlock()
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

func sortBySeq(items []*queuedMsg) {
	// 队列长度通常很小，插入排序足够且避免引入 sort 依赖的开销
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].seq > items[j].seq; j-- {
			items[j-1], items[j] = items[j], items[j-1]
		}
	}
}
