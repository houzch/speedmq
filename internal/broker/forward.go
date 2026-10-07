package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/internal/raft"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件是 M6b 的跨节点消息转发：让客户端**连到任意节点**都能发布与消费任意队列，
// 语义对齐 RabbitMQ 经典队列（队列数据在 Owner 节点，非 Owner 节点做代理）。
//
// 数据面（队列数据、落盘、TTL/死信/长度限制）始终由 Owner 执行，代理节点只做三件事：
//  1. 发布：本节点路由后，把消息按"目标队列"转发给 Owner（见 forwardPublish）；
//  2. 消费：本节点注册一个"代理消费者"，Owner 把投递推回来（见 remoteConsume / handleDeliver）；
//  3. 拉取：basic.get 也转发给 Owner，未确认消息在 Owner 侧持有，结算再回传（见 forwardGet）。
//
// 复用集群端口（与 Raft、meta.propose 同一根连接、按方法名分发，见 raft.Transport 的约定），
// 因此不需要额外的监听端口与配置。
//
// 明确的取舍（不做的部分写清楚，而不是含糊其辞）：
//   - **投递是同步 RPC**：Owner 的队列投递循环会等代理节点把消息写进客户端 socket。
//     换来的是"投递失败即重新入队"，代价是慢客户端会拖慢该队列的投递节奏（批量/流水线转发留给 M7）。
//   - **至少一次**：RPC 应答丢失时消息会被重新入队并重投（客户端看到 redelivered=true），
//     与 AMQP 自身的确认语义同级，不承诺"恰好一次"。
//   - **代理消费者有租约**：代理节点每 3 秒续租，Owner 侧超过 15 秒未见续租即摘除该消费者
//     并把它未确认的消息重新入队 —— 代理节点进程崩溃时不会留下永久悬挂的消费者。

// 转发 RPC 方法名。前缀 cluster. 由应用层使用（raft. 前缀为共识层保留）。
const (
	fwdMethodPublish   = "cluster.queue.publish"
	fwdMethodGet       = "cluster.queue.get"
	fwdMethodConsume   = "cluster.queue.consume"
	fwdMethodCancel    = "cluster.queue.cancel"
	fwdMethodSettle    = "cluster.queue.settle"
	fwdMethodStats     = "cluster.queue.stats"
	fwdMethodPurge     = "cluster.queue.purge"
	fwdMethodExists    = "cluster.queue.exists"
	fwdMethodDeliver   = "cluster.queue.deliver"
	fwdMethodCanceled  = "cluster.queue.canceled"
	fwdMethodKeepalive = "cluster.queue.keepalive"
)

// 转发相关的时间参数。
const (
	// fwdPublishTimeout 是转发发布的上限：Owner 可能正在做 fsync=always 的落盘。
	fwdPublishTimeout = 15 * time.Second
	// fwdDeliverTimeout 是"投递推回代理节点"的上限：包含代理节点写客户端 socket 的时间。
	fwdDeliverTimeout = 30 * time.Second
	// fwdControlTimeout 是控制类 RPC（注册/取消/结算/续租）的上限。
	fwdControlTimeout = 5 * time.Second
	// fwdStatsTimeout 是取远端队列统计的上限，刻意很短：管理面不能被慢节点拖住。
	fwdStatsTimeout = 1 * time.Second
	// fwdLeaseInterval 是代理节点的续租间隔。
	fwdLeaseInterval = 3 * time.Second
	// fwdLeaseTTL 是 Owner 侧判定"代理节点已失联"的阈值（约 5 个续租周期）。
	fwdLeaseTTL = 15 * time.Second
	// fwdReapInterval 是 Owner 侧回收失效代理的检查间隔。
	fwdReapInterval = 5 * time.Second
	// fwdCatchUpTimeout 是"等本地元数据追平"的上限。
	//
	// 场景：客户端在节点 A 声明队列、立刻在节点 B 消费。声明本身在 A 返回时已提交，
	// 但 B 要等下一轮 Raft 复制与应用。这类"跨节点紧接着用"是合法用法，
	// 不能让它偶发 404，因此这里给一个很短的上限等它追平。
	fwdCatchUpTimeout = 3 * time.Second
	// fwdCatchUpPoll 是等待本地追平的轮询间隔。
	fwdCatchUpPoll = 10 * time.Millisecond
	// fwdRepointBackoff 是代理消费者改挂失败后的退避窗口。
	//
	// 没有退避时，一次选主会让一批代理反复改挂，而每次 RPC 最长阻塞 fwdControlTimeout（5s）。
	// 窗口取 10s（> 最坏完成时间：queueOwner 3s + RPC 5s），既避免反复 stall，
	// 也保证同一消费者不会因为上一轮还没跑完而被并发改挂两次。
	fwdRepointBackoff = 10 * time.Second
	// fwdBatchMax 是一次转发 RPC 最多携带的发布条数（也是流水线的成批上限）。
	fwdBatchMax = 128
	// fwdPipeDepth 是发布转发流水线的待处理队列深度（背压上界：
	// 队列满时登记方阻塞，等价于给生产者限流，避免内存被客户端支配）。
	fwdPipeDepth = 4096
)

// ---------------------------------------------------------------------------
// 线协议
// ---------------------------------------------------------------------------

// fwdEnvelope 是所有转发应答共用的信封。
//
// 带上 Kind 是为了**保留错误语义**：独占占用(405)、参数不一致(406)、队列不存在(404)
// 分属软/硬错误、作用域也不同，代理节点必须原样还原给客户端，不能一律降级成内部错误。
type fwdEnvelope struct {
	OK   bool   `json:"ok"`
	Kind int    `json:"kind,omitempty"`
	Err  string `json:"err,omitempty"`
}

type fwdPublishReq struct {
	VHost string `json:"vhost"`
	Queue string `json:"queue"`
	Msg   []byte `json:"msg"`
}

// fwdBatchPublishItem 是批量转发里的单条发布。
type fwdBatchPublishItem struct {
	VHost string `json:"vhost"`
	Queue string `json:"queue"`
	Msg   []byte `json:"msg"`
}

// fwdBatchPublishReq 是"一次 RPC 携带多条发布"的请求。
//
// 存在的理由：代理节点此前每条消息一次 RPC 且同步等待，单连接吞吐被钉在 ~1/RTT（~200 msg/s）。
// 批量化后一次往返携带整批，Owner 侧再借组提交统一落盘。
type fwdBatchPublishReq struct {
	Items []fwdBatchPublishItem `json:"items"`
}

// fwdBatchPublishItemResp 是单条发布的结果。
//
// Error 非空表示该条未成功（落盘失败/队列不可用等），调用方据此对该条否定确认；
// 它独立于信封的 OK（信封只表示"这次 RPC 本身"成功与否）。
type fwdBatchPublishItemResp struct {
	Routed   bool   `json:"routed"`
	Rejected bool   `json:"rejected"`
	Error    string `json:"err,omitempty"`
}

// fwdBatchPublishResp 是批量转发的应答，Results 与请求 Items 一一对应。
type fwdBatchPublishResp struct {
	fwdEnvelope
	Results []fwdBatchPublishItemResp `json:"results"`
}

type fwdGetReq struct {
	VHost string `json:"vhost"`
	Queue string `json:"queue"`
	NoAck bool   `json:"no_ack"`
}

type fwdGetResp struct {
	fwdEnvelope
	Empty       bool   `json:"empty"`
	DeliveryID  uint64 `json:"delivery_id"`
	ConsumerTag string `json:"consumer_tag"`
	Redelivered bool   `json:"redelivered"`
	Msg         []byte `json:"msg"`
}

type fwdConsumeReq struct {
	VHost     string `json:"vhost"`
	Queue     string `json:"queue"`
	WireTag   string `json:"wire_tag"`
	NoAck     bool   `json:"no_ack"`
	Prefetch  uint16 `json:"prefetch"`
	Exclusive bool   `json:"exclusive"`
}

type fwdDeliverReq struct {
	WireTag     string `json:"wire_tag"`
	DeliveryID  uint64 `json:"delivery_id"`
	Queue       string `json:"queue"`
	Redelivered bool   `json:"redelivered"`
	Msg         []byte `json:"msg"`
}

type fwdSettleReq struct {
	DeliveryID uint64 `json:"delivery_id"`
	Action     int    `json:"action"`
}

type fwdCancelReq struct {
	VHost   string `json:"vhost"`
	Queue   string `json:"queue"`
	WireTag string `json:"wire_tag"`
}

type fwdCanceledReq struct {
	WireTag string `json:"wire_tag"`
	Reason  string `json:"reason"`
}

type fwdStatsReq struct {
	VHost string `json:"vhost"`
	Queue string `json:"queue"`
}

type fwdStatsResp struct {
	fwdEnvelope
	Ready     uint32 `json:"ready"`
	Unacked   uint32 `json:"unacked"`
	Consumers uint32 `json:"consumers"`
}

type fwdPurgeReq struct {
	VHost string `json:"vhost"`
	Queue string `json:"queue"`
}

type fwdPurgeResp struct {
	fwdEnvelope
	Purged uint32 `json:"purged"`
}

type fwdEnvelopeOnly struct {
	fwdEnvelope
}

type fwdExistsReq struct {
	VHost string `json:"vhost"`
	Queue string `json:"queue"`
}

type fwdExistsResp struct {
	fwdEnvelope
	Exists bool `json:"exists"`
}

type fwdKeepaliveReq struct {
	Node   string   `json:"node"`
	Tags   []string `json:"tags,omitempty"`
	GetIDs []uint64 `json:"get_ids,omitempty"`
}

// fwdKeepaliveResp 是 Owner 对续租的应答。
//
// 除了"收到"（fwdEnvelope.OK），它还回报**本 Owner 不认识的标签/拉取**，代理节点据此重新注册。
// 这修掉一个静默饿死的缺口（U14）：代理节点与 Owner 之间分区超过租约期（fwdLeaseTTL）后，
// Owner 会摘除该代理并尝试通知，可若分区把通知也挡住，代理节点既不会因 repointForwards
// （owner 未变）重注册，也无从得知自己的注册已失效，消费者就永远收不到消息。
// 有了这份"未知清单"，代理节点下一次续租即可自愈。
type fwdKeepaliveResp struct {
	fwdEnvelope
	UnknownTags   []string `json:"unknown_tags,omitempty"`
	UnknownGetIDs []uint64 `json:"unknown_get_ids,omitempty"`
}

// ---------------------------------------------------------------------------
// 本节点持有的状态
// ---------------------------------------------------------------------------

// localProxy 是本节点"代客户端"持有的远端队列消费者（本节点是代理节点）。
type localProxy struct {
	wireTag string
	tag     string // 客户端侧的消费者标签
	vhost   string
	queue   string
	owner   string // 队列数据所在节点
	session *vhostSession
	sub     plugin.Subscription
}

// remoteProxy 是远端节点挂在本节点队列上的代理消费者（本节点是 Owner）。
type remoteProxy struct {
	wireTag   string
	proxyNode string
	vhost     string
	queue     string
	noAck     bool
	lastSeen  time.Time
}

// heldDelivery 是本节点持有、等待**远端**结算的投递（代理消费者投递或跨节点 basic.get）。
type heldDelivery struct {
	d         *plugin.Delivery
	proxyNode string
	wireTag   string // 空表示跨节点 basic.get
	lastSeen  time.Time
}

// SetMessageCodec 注入消息编解码器（跨节点转发必需）。
//
// 由进程入口在装配阶段调用：消息的属性值体系属于协议，内核不解释它，
// 因此编解码实现由协议侧提供（见 plugin.MessageCodec）。
func (b *Broker) SetMessageCodec(c plugin.MessageCodec) {
	b.codecMu.Lock()
	b.codec = c
	b.codecMu.Unlock()
}

// wireTagOf 生成集群内唯一的代理消费者标签。
//
// 客户端标签只在"本节点 + 本 vhost"内唯一，加上节点标识与 vhost 后才全局唯一，
// 同时保证标签是"可重算"的 —— 取消消费者时不必再查表。
func (b *Broker) wireTagOf(vhost, tag string) string {
	return b.nodeID + "\x00" + vhost + "\x00" + tag
}

// serveForwardMethods 把转发 RPC 处理器注册到集群传输上。
//
// 必须在 Raft 启动**之前**注册：静态成员表意味着邻居随时可能来找我们。
func (b *Broker) serveForwardMethods(t raft.Transport) error {
	handlers := []struct {
		method string
		h      func(context.Context, string, []byte) ([]byte, error)
	}{
		{fwdMethodPublish, b.handleForwardPublish},
		{fwdMethodGet, b.handleForwardGet},
		{fwdMethodConsume, b.handleForwardConsume},
		{fwdMethodCancel, b.handleForwardCancel},
		{fwdMethodSettle, b.handleForwardSettle},
		{fwdMethodStats, b.handleForwardStats},
		{fwdMethodPurge, b.handleForwardPurge},
		{fwdMethodExists, b.handleForwardExists},
		{fwdMethodDeliver, b.handleForwardDeliver},
		{fwdMethodCanceled, b.handleForwardCanceled},
		{fwdMethodKeepalive, b.handleForwardKeepalive},
	}
	for _, x := range handlers {
		if err := t.Serve(x.method, x.h); err != nil {
			return fmt.Errorf("注册转发处理器 %s 失败: %w", x.method, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 代理节点侧（客户端连着我，队列数据在别的节点）
// ---------------------------------------------------------------------------

// forwardPublish 把一条消息**同步**转发到 Owner 节点上的指定队列。
//
// 返回的 routed/rejected 与本地发布同义；持久消息的落盘等待在 Owner 侧完成后才应答，
// 因此调用方不需要再等 durability —— 拿到应答就意味着已按 Owner 的 fsync 档位落盘。
//
// 只用于"没有前台生产者在等"的内部路由（死信），它需要同步的 routed/rejected 来决定是否重新入队；
// 客户端发布路径请用 forwardPublishAsync（流水线化，见 M0.5）。两者共用同一套批量线协议。
func (b *Broker) forwardPublish(vhost, queue, owner string, msg *plugin.Message) (routed, rejected bool, err error) {
	raw, err := b.encodeMessage(msg)
	if err != nil {
		return false, false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), fwdPublishTimeout)
	defer cancel()
	var resp fwdBatchPublishResp
	req := fwdBatchPublishReq{Items: []fwdBatchPublishItem{{VHost: vhost, Queue: queue, Msg: raw}}}
	if err := b.fwdCall(ctx, owner, fwdMethodPublish, req, &resp); err != nil {
		return false, false, err
	}
	if len(resp.Results) != 1 {
		return false, false, fmt.Errorf("转发应答与请求条数不一致: %d", len(resp.Results))
	}
	r := resp.Results[0]
	if r.Error != "" {
		return false, false, errors.New(r.Error)
	}
	b.fwdOut.Add(1)
	b.fwdBatches.Add(1)
	return r.Routed, r.Rejected, nil
}

// ---------------------------------------------------------------------------
// 发布转发流水线（M0.5）
// ---------------------------------------------------------------------------

// fwdPending 是一条"已登记、待成批转发"的发布。
type fwdPending struct {
	owner string
	req   fwdPublishReq
	// id 是消息体前 8 字节解出的 check-loss 消息 id（非该约定为 0），供转发边界按 id 追踪使用。
	id   uint64
	done chan fwdBatchPublishItemResp
}

// errFwdRejected 表示 Owner 侧因队列长度限制拒绝了该条发布。
//
// 借"落盘等待返回错误"这条既有通路把"拒绝"传到确认层（confirm 模式下回 basic.nack），
// 因而无需改动 plugin.PublishResult 的字段。
var errFwdRejected = errors.New("队列因长度限制拒绝了发布")

// forwardPublishAsync 登记一条待转发的发布并**立即返回**。
//
// 与 forwardPublish 的区别：不在调用方（协议层连接读循环）里同步等 RPC，而是投入转发流水线，
// 由流水线按 Owner 成批发往 Owner。返回的 wait 在整条链路完成后返回：
//   - nil：已入队并按 Owner 的 fsync 档位落盘；
//   - errFwdRejected：队列因长度限制拒绝（调用方应否定确认）；
//   - 其它 error：转发失败（队列不可用 / 网络失败等）。
//
// 关于 Routed：目标队列来自本节点路由表，登记时即视为"已路由"，因此 `mandatory` 的
// Basic.Return 判定不受影响；唯一偏差是"登记后、Owner 处理前队列被删"这一竞态窗口内不再回 Return
// （原同步路径能回）——这是把"路由判定"与"投递"解耦所必需付出的、可接受的代价。
func (b *Broker) forwardPublishAsync(vhost, queue, owner string, msg *plugin.Message) (func() error, error) {
	raw, err := b.encodeMessage(msg)
	if err != nil {
		return nil, err
	}
	p := &fwdPending{
		owner: owner,
		req:   fwdPublishReq{VHost: vhost, Queue: queue, Msg: raw},
		id:    messageID(msg.Body),
		done:  make(chan fwdBatchPublishItemResp, 1),
	}
	select {
	case b.fwdPipe <- p:
	case <-b.done:
		return nil, errors.New("broker 正在关闭，发布未转发")
	}
	return func() error {
		select {
		case r := <-p.done:
			if r.Error != "" {
				return errors.New(r.Error)
			}
			if r.Rejected {
				return errFwdRejected
			}
			return nil
		case <-b.done:
			return errors.New("broker 正在关闭，转发结果未知")
		}
	}, nil
}

// runForwardPipeline 是发布转发的流水线：把登记进来的发布按 Owner 分组，一组一次 RPC。
//
// 天然成批：登记方（连接读循环）不再被 RPC 阻塞，队列自然积压，一次收满 fwdBatchMax 条即可。
// **顺序**：同一 Owner 内严格按登记顺序发送，因此单发布者看到的队列 FIFO 不会被流水线打乱。
func (b *Broker) runForwardPipeline(ctx context.Context) {
	for {
		var first *fwdPending
		select {
		case <-ctx.Done():
			b.failPendingForwards()
			return
		case first = <-b.fwdPipe:
		}
		batch := []*fwdPending{first}
	collect:
		for len(batch) < fwdBatchMax {
			select {
			case p := <-b.fwdPipe:
				batch = append(batch, p)
			default:
				break collect
			}
		}
		b.dispatchForwardBatch(batch)
	}
}

// failPendingForwards 在流水线退出时把仍在队列里的登记项全部置为失败，避免等待方永久挂起。
func (b *Broker) failPendingForwards() {
	for {
		select {
		case p := <-b.fwdPipe:
			p.done <- fwdBatchPublishItemResp{Error: "broker 正在关闭，转发未完成"}
		default:
			return
		}
	}
}

// dispatchForwardBatch 按 Owner 分组（组内保序），逐组发一次 RPC 并回填结果。
func (b *Broker) dispatchForwardBatch(batch []*fwdPending) {
	groups := make(map[string][]*fwdPending, 1)
	owners := make([]string, 0, 1)
	for _, p := range batch {
		if _, ok := groups[p.owner]; !ok {
			owners = append(owners, p.owner)
		}
		groups[p.owner] = append(groups[p.owner], p)
	}
	for _, owner := range owners {
		b.sendForwardBatch(owner, groups[owner])
	}
}

func (b *Broker) sendForwardBatch(owner string, items []*fwdPending) {
	req := fwdBatchPublishReq{Items: make([]fwdBatchPublishItem, 0, len(items))}
	for _, p := range items {
		req.Items = append(req.Items, fwdBatchPublishItem{VHost: p.req.VHost, Queue: p.req.Queue, Msg: p.req.Msg})
	}
	ctx, cancel := context.WithTimeout(context.Background(), fwdPublishTimeout)
	defer cancel()
	var resp fwdBatchPublishResp
	if err := b.fwdCall(ctx, owner, fwdMethodPublish, req, &resp); err != nil {
		for _, p := range items {
			b.traceFwdSent(p.id, false)
			p.done <- fwdBatchPublishItemResp{Error: err.Error()}
		}
		return
	}
	if len(resp.Results) != len(items) {
		for _, p := range items {
			b.traceFwdSent(p.id, false)
			p.done <- fwdBatchPublishItemResp{Error: "转发应答与请求条数不一致"}
		}
		return
	}
	b.fwdOut.Add(uint64(len(items)))
	b.fwdBatches.Add(1)
	for i, p := range items {
		r := resp.Results[i]
		// 诊断：应答"未路由且未拒绝且无错误"意味着既没入队、又会被当成功上报确认 —— 静默丢弃签名。
		if !r.Routed && !r.Rejected && r.Error == "" {
			b.fwdPhantomOK.Add(1)
		}
		// 按 id 追踪：代理侧把"成功"记为未拒且无错（与确认层口径一致）。
		b.traceFwdSent(p.id, r.Error == "" && !r.Rejected)
		p.done <- r
	}
}

// forwardGet 从 Owner 节点上的队列主动拉取一条消息。
func (b *Broker) forwardGet(vhost, queue, owner string, noAck bool) (*plugin.Delivery, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
	defer cancel()
	var resp fwdGetResp
	if err := b.fwdCall(ctx, owner, fwdMethodGet, fwdGetReq{VHost: vhost, Queue: queue, NoAck: noAck}, &resp); err != nil {
		return nil, false, err
	}
	if resp.Empty {
		return nil, false, nil
	}
	msg, err := b.decodeMessage(resp.Msg)
	if err != nil {
		return nil, false, err
	}
	msg.Redelivered = resp.Redelivered
	d := &plugin.Delivery{
		Message:     msg,
		Queue:       queue,
		ConsumerTag: resp.ConsumerTag,
		Redelivered: resp.Redelivered,
	}
	if noAck {
		// Owner 侧已投递即结算，这里无需再回报。
		d.Settle = func(plugin.SettleAction) {}
		return d, true, nil
	}
	id := resp.DeliveryID
	b.trackRemoteGet(id, owner)
	d.Settle = func(action plugin.SettleAction) { b.forwardSettle(owner, id, action) }
	return d, true, nil
}

// lookupQueue 取本地队列；本地还没有、但集群里已存在时，短暂等元数据追平后再取。
//
// 为什么需要它：客户端"在节点 A 声明队列、立刻在节点 B 消费/被动声明"是合法用法，
// 而 B 要等下一轮 Raft 复制才认识这个队列。没有这层等待，就会偶发 404，
// 让集群看起来"不可靠"。代价是"确实不存在的队列"要多一次邻居查询（见 peerKnowsQueue），
// 所以这里先确认存在性再等，而不是无脑轮询。
func (s *vhostSession) lookupQueue(name string) (*queue, bool) {
	if q, ok := s.vh.getQueue(name); ok {
		return q, true
	}
	if !s.vh.broker.waitForQueueCatchUp(s.vh.name, name) {
		return nil, false
	}
	return s.vh.getQueue(name)
}

// waitForQueueCatchUp 在"集群里确实有该队列、只是本地还没应用"时等它追平。
func (b *Broker) waitForQueueCatchUp(vhost, name string) bool {
	if !b.clusterOn {
		return false
	}
	if !b.peerKnowsQueue(vhost, name) {
		return false
	}
	deadline := time.Now().Add(fwdCatchUpTimeout)
	for time.Now().Before(deadline) {
		if v, ok := b.vhostOf(vhost); ok {
			if _, ok := v.getQueue(name); ok {
				return true
			}
		}
		time.Sleep(fwdCatchUpPoll)
	}
	b.log.Warn("等待本地元数据追平超时", "vhost", vhost, "queue", name)
	return false
}

// peerKnowsQueue 并行询问邻居"这个队列存在吗"，任一回答"存在"即为真。
func (b *Broker) peerKnowsQueue(vhost, name string) bool {
	req := fwdExistsReq{VHost: vhost, Queue: name}
	// 通道容量按成员数预置：提前返回时未读到的应答不会让协程永久阻塞。
	answers := make(chan bool, len(b.cfg.Cluster.Peers))
	asked := 0
	for id := range b.cfg.Cluster.Peers {
		if id == b.nodeID {
			continue
		}
		asked++
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), fwdStatsTimeout)
			defer cancel()
			var resp fwdExistsResp
			if err := b.fwdCall(ctx, peer, fwdMethodExists, req, &resp); err != nil {
				answers <- false
				return
			}
			answers <- resp.Exists
		}(id)
	}
	for i := 0; i < asked; i++ {
		if <-answers {
			return true
		}
	}
	return false
}

// queueInfo 返回队列声明类操作要用的统计信息。
//
// 远端队列的计数必须向服务节点取：报 0 会让客户端以为"队列里没有存量消息"，
// 据此决定是否要继续消费就是错的。
func (s *vhostSession) queueInfo(q *queue) plugin.QueueInfo {
	info := plugin.QueueInfo{Name: q.name}
	if s.vh.queueRemote(q) {
		if owner := s.vh.queueOwner(q); owner != "" {
			if st, err := s.vh.broker.remoteQueueStats(s.vh.name, q.name, owner); err == nil {
				info.MessageCount, info.ConsumerCount = st.Ready, st.Consumers
			} else {
				s.log.Debug("取远端队列统计失败", "queue", q.name, "owner", owner, "err", err)
			}
		}
		return info
	}
	info.MessageCount, info.ConsumerCount = q.stats()
	return info
}

// remoteConsume 在 Owner 节点上注册一个代理消费者。
//
// 调用方（vhostSession）已把消费者登记进本地 consumers 索引，这里只负责远端注册与本地代理记账。
func (b *Broker) remoteConsume(s *vhostSession, q *queue, sub plugin.Subscription) error {
	owner := s.vh.queueOwner(q)
	wireTag := b.wireTagOf(s.vh.name, sub.Tag)

	// **先登记再发起注册**：服务节点在 subscribe 时就会立刻尝试投递（队列里本就有存量消息），
	// 那条投递可能在本调用的应答之前就到达本节点；如果那时本地还没登记这个标签，
	// 投递会被判为"未知的代理消费者"，服务节点随之摘掉刚注册的消费者。
	proxy := &localProxy{
		wireTag: wireTag, tag: sub.Tag, vhost: s.vh.name, queue: q.name,
		owner: owner, session: s, sub: sub,
	}
	b.fwdMu.Lock()
	b.fwdLocal[wireTag] = proxy
	b.fwdMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
	defer cancel()
	var resp fwdEnvelopeOnly
	if err := b.fwdCall(ctx, owner, fwdMethodConsume, fwdConsumeReq{
		VHost: s.vh.name, Queue: q.name, WireTag: wireTag,
		NoAck: sub.NoAck, Prefetch: sub.Prefetch, Exclusive: sub.Exclusive,
	}, &resp); err != nil {
		b.fwdMu.Lock()
		delete(b.fwdLocal, wireTag)
		b.fwdMu.Unlock()
		return err
	}
	return nil
}

// remoteCancel 取消 Owner 节点上的代理消费者。
func (b *Broker) remoteCancel(vhost, queue, owner, tag string) error {
	wireTag := b.wireTagOf(vhost, tag)
	b.fwdMu.Lock()
	delete(b.fwdLocal, wireTag)
	delete(b.fwdRepointUntil, wireTag)
	b.fwdMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
	defer cancel()
	var resp fwdEnvelopeOnly
	// 取消失败不向客户端报错：客户端只想"别再投了"，本地登记已经清掉，
	// Owner 侧最迟会在租约过期后自行摘除。
	return b.fwdCall(ctx, owner, fwdMethodCancel, fwdCancelReq{VHost: vhost, Queue: queue, WireTag: wireTag}, &resp)
}

// forwardPurge 清空 Owner 节点上的队列。
func (b *Broker) forwardPurge(vhost, queue, owner string) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
	defer cancel()
	var resp fwdPurgeResp
	if err := b.fwdCall(ctx, owner, fwdMethodPurge, fwdPurgeReq{VHost: vhost, Queue: queue}, &resp); err != nil {
		return 0, err
	}
	return resp.Purged, nil
}

// remoteQueueStats 取 Owner 节点上队列的统计（管理面与删除前检查用）。
func (b *Broker) remoteQueueStats(vhost, queue, owner string) (fwdStatsResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fwdStatsTimeout)
	defer cancel()
	var resp fwdStatsResp
	err := b.fwdCall(ctx, owner, fwdMethodStats, fwdStatsReq{VHost: vhost, Queue: queue}, &resp)
	return resp, err
}

// forwardSettle 把一次结算回报给 Owner。
//
// 失败只记日志：消息仍留在 Owner 的未确认集合里，最终会在代理消费者失效时重新入队，
// 结果是"可能重投"而不是"丢消息"。
func (b *Broker) forwardSettle(owner string, deliveryID uint64, action plugin.SettleAction) {
	b.untrackRemoteGet(deliveryID)
	ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
	defer cancel()
	var resp fwdEnvelopeOnly
	if err := b.fwdCall(ctx, owner, fwdMethodSettle, fwdSettleReq{DeliveryID: deliveryID, Action: int(action)}, &resp); err != nil {
		b.log.Warn("跨节点结算失败（消息将在代理消费者失效时重新入队）",
			"owner", owner, "delivery_id", deliveryID, "err", err)
	}
}

// ---------------------------------------------------------------------------
// Owner 节点侧（客户端连着别人，队列数据在我这）
// ---------------------------------------------------------------------------

func (b *Broker) handleForwardPublish(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdBatchPublishReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析发布转发请求失败: %w", err)
	}
	results := make([]fwdBatchPublishItemResp, len(req.Items))
	// ids 按 id 追踪用：记录每条请求解出的 check-loss id（解码失败留 0，不参与追踪）。
	ids := make([]uint64, len(req.Items))
	// 先把整批逐条入队并收集"落盘/多数派等待"，最后统一等待：
	// 同一批共享一次组提交（M1）与多数派往返，而不是每条各等一轮。
	type durableWait struct {
		idx  int
		wait func() error
	}
	waits := make([]durableWait, 0, len(req.Items))
	for i, it := range req.Items {
		msg, err := b.decodeMessage(it.Msg)
		if err != nil {
			// 编解码失败是协议级错误：报给该条，让它按"发布失败"处理，绝不能静默丢消息。
			results[i].Error = err.Error()
			continue
		}
		ids[i] = messageID(msg.Body)
		v, ok := b.vhostOf(it.VHost)
		if !ok {
			// **不能静默成功**：Owner 本地没有该 vhost，消息并未入队。若回"成功"，
			// 代理节点会把它当已落盘而上报确认 → 已确认消息丢失。按失败回，让发布方重试。
			b.fwdQueueMissing.Add(1)
			results[i].Error = fmt.Sprintf("目标 vhost '%s' 在本节点不存在（未能入队）", it.VHost)
			continue
		}
		q, ok := v.getQueue(it.Queue)
		if !ok {
			// 同理：队列在 Owner 本地缺失（多为节点重启后元数据尚未回放完的瞬态），
			// 必须按失败回，绝不能让"静默丢弃"伪装成"已确认"。
			b.fwdQueueMissing.Add(1)
			results[i].Error = fmt.Sprintf("目标队列 '%s' 在本节点不存在（未能入队）", it.Queue)
			continue
		}
		if v.queueRemote(q) {
			b.fwdRemoteMismatch.Add(1)
			results[i].Error = fmt.Sprintf("队列 '%s' 的服务节点不一致（本节点并不持有它的数据）", it.Queue)
			continue
		}
		accepted, wait, err := q.publish(cloneForQueue(msg))
		if err != nil {
			b.fwdPublishErr.Add(1)
			results[i].Error = err.Error()
			continue
		}
		results[i].Routed = true
		results[i].Rejected = !accepted
		if !accepted {
			b.fwdRejected.Add(1)
		}
		if wait != nil {
			waits = append(waits, durableWait{idx: i, wait: wait})
		} else if q.isQuorum() {
			// 仲裁队列发布成功时**必然**给出"落盘/多数派"等待；wait==nil 只可能是队列已关闭，
			// 此时消息并未入队 —— 绝不能当成功上报（否则又是"已确认却从未入队"）。
			b.fwdNoWait.Add(1)
			results[i].Routed = false
			results[i].Rejected = false
			results[i].Error = fmt.Sprintf("队列 '%s' 当前不可用（未能入队）", it.Queue)
		}
	}
	for _, w := range waits {
		if err := w.wait(); err != nil {
			// 落盘/多数派失败：该条按失败回，代理节点会对它否定确认。
			b.fwdDurableErr.Add(1)
			results[w.idx].Routed = false
			results[w.idx].Rejected = false
			results[w.idx].Error = fmt.Sprintf("等待队列 %s 持久化失败: %v", req.Items[w.idx].Queue, err)
		}
	}
	b.fwdIn.Add(uint64(len(req.Items)))
	// 按 id 追踪：Owner 侧记录每条转发的最终入队结果（接受/失败），供与代理侧样本对账。
	for i := range results {
		r := results[i]
		b.traceFwdIn(ids[i], r.Error == "" && !r.Rejected)
	}
	return fwdJSON(fwdBatchPublishResp{fwdEnvelope: fwdOK, Results: results})
}

func (b *Broker) handleForwardGet(_ context.Context, from string, payload []byte) ([]byte, error) {
	var req fwdGetReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析主动拉取转发请求失败: %w", err)
	}
	v, ok := b.vhostOf(req.VHost)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - vhost '%s' 不存在", req.VHost)
	}
	q, ok := v.getQueue(req.Queue)
	if !ok {
		// **不能回"空"**：目标节点本地没有这条队列时回 Empty，会让调用方（尤其红线的"拉空证空"）
		// 把"这一侧根本没有该队列"误读成"队列已空"，从而漏判真实丢失。按 NOT_FOUND 回，让调用方
		// 判定为"未能证空"（null）而不是"已确认无丢失"。
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - no queue '%s' in vhost '%s'", req.Queue, req.VHost)
	}
	d, ok := q.get(req.NoAck)
	if !ok {
		return fwdJSON(fwdGetResp{fwdEnvelope: fwdOK, Empty: true})
	}
	raw, err := b.encodeMessage(d.Message)
	if err != nil {
		// 编码失败：把消息放回队列头，不能让它在"已取出但送不出去"的状态里消失。
		d.Settle(plugin.SettleRequeue)
		return nil, err
	}
	resp := fwdGetResp{
		fwdEnvelope: fwdOK,
		Empty:       false,
		ConsumerTag: d.ConsumerTag,
		Redelivered: d.Redelivered,
		Msg:         raw,
	}
	if !req.NoAck {
		resp.DeliveryID = b.holdDelivery(d, from, "")
	}
	return fwdJSON(resp)
}

func (b *Broker) handleForwardConsume(_ context.Context, from string, payload []byte) ([]byte, error) {
	var req fwdConsumeReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析消费者注册转发请求失败: %w", err)
	}
	v, ok := b.vhostOf(req.VHost)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - vhost '%s' 不存在", req.VHost)
	}
	q, ok := v.getQueue(req.Queue)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - no queue '%s' in vhost '%s'", req.Queue, req.VHost)
	}

	proxy := &remoteProxy{
		wireTag: req.WireTag, proxyNode: from, vhost: req.VHost, queue: req.Queue,
		noAck: req.NoAck, lastSeen: time.Now(),
	}
	b.fwdMu.Lock()
	if _, dup := b.fwdRemote[req.WireTag]; dup {
		b.fwdMu.Unlock()
		return fwdErr(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - consumer '%s' already registered", req.WireTag)
	}
	b.fwdRemote[req.WireTag] = proxy
	b.fwdMu.Unlock()

	sub := plugin.Subscription{
		Tag: req.WireTag, Queue: req.Queue, NoAck: req.NoAck,
		Exclusive: req.Exclusive, Prefetch: req.Prefetch,
		Deliver: func(d *plugin.Delivery) error { return b.deliverToProxy(proxy, d) },
		Cancel:  func(reason string) { b.notifyProxyCanceled(proxy, reason) },
	}
	if err := q.subscribe(sub); err != nil {
		b.fwdMu.Lock()
		delete(b.fwdRemote, req.WireTag)
		b.fwdMu.Unlock()
		return fwdErrFrom(err)
	}
	b.log.Debug("已为远端节点注册代理消费者",
		"proxy_node", from, "queue", req.Queue, "wire_tag", req.WireTag)
	return fwdAck()
}

func (b *Broker) handleForwardCancel(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdCancelReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析消费者取消失败: %w", err)
	}
	b.fwdMu.Lock()
	proxy := b.fwdRemote[req.WireTag]
	delete(b.fwdRemote, req.WireTag)
	b.fwdMu.Unlock()
	if proxy != nil {
		b.dropRemoteProxyLocal(proxy, "客户端取消了消费者")
	}
	return fwdAck()
}

func (b *Broker) handleForwardSettle(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdSettleReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析结算请求失败: %w", err)
	}
	b.fwdMu.Lock()
	hd := b.fwdHeld[req.DeliveryID]
	delete(b.fwdHeld, req.DeliveryID)
	b.fwdMu.Unlock()
	if hd == nil {
		// 已结算过（重复 ack / 租约已回收）：幂等忽略，与本地 settle 的口径一致。
		return fwdJSON(fwdEnvelopeOnly{})
	}
	hd.d.Settle(plugin.SettleAction(req.Action))
	return fwdAck()
}

func (b *Broker) handleForwardStats(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdStatsReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析统计请求失败: %w", err)
	}
	v, ok := b.vhostOf(req.VHost)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - vhost '%s' 不存在", req.VHost)
	}
	q, ok := v.getQueue(req.Queue)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - no queue '%s' in vhost '%s'", req.Queue, req.VHost)
	}
	ready, consumers := q.stats()
	q.mu.Lock()
	unacked := uint32(len(q.unacked))
	q.mu.Unlock()
	return fwdJSON(fwdStatsResp{fwdEnvelope: fwdOK, Ready: ready, Unacked: unacked, Consumers: consumers})
}

func (b *Broker) handleForwardPurge(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdPurgeReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析清空请求失败: %w", err)
	}
	v, ok := b.vhostOf(req.VHost)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - vhost '%s' 不存在", req.VHost)
	}
	q, ok := v.getQueue(req.Queue)
	if !ok {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - no queue '%s' in vhost '%s'", req.Queue, req.VHost)
	}
	return fwdJSON(fwdPurgeResp{fwdEnvelope: fwdOK, Purged: q.purge()})
}

// handleForwardExists 回答"这个队列在集群里存在吗"。
//
// 只读本地拓扑（不转发给 Owner）：本节点要么已经应用了同一条元数据日志，
// 要么还没应用 —— 前者回答"存在"，后者由提问方稍等重试即可。
func (b *Broker) handleForwardExists(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdExistsReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析存在性查询失败: %w", err)
	}
	exists := false
	if v, ok := b.vhostOf(req.VHost); ok {
		_, exists = v.getQueue(req.Queue)
	}
	return fwdJSON(fwdExistsResp{fwdEnvelope: fwdOK, Exists: exists})
}

// handleForwardDeliver 由 Owner 调用：把一条投递推回代理节点。
//
// 它跑在代理节点的集群端口上；投递失败意味着"代理节点没接住"，
// 因此返回错误让 Owner 把消息重新入队（而不是丢在代理节点的内存里）。
func (b *Broker) handleForwardDeliver(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdDeliverReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析投递转发请求失败: %w", err)
	}
	b.fwdMu.Lock()
	lp := b.fwdLocal[req.WireTag]
	b.fwdMu.Unlock()
	if lp == nil {
		return fwdErr(plugin.KindNotFound, "NOT_FOUND - 未知的代理消费者 '%s'", req.WireTag)
	}
	msg, err := b.decodeMessage(req.Msg)
	if err != nil {
		return nil, err
	}
	msg.Redelivered = req.Redelivered
	d := &plugin.Delivery{
		Message:     msg,
		Queue:       req.Queue,
		ConsumerTag: lp.tag,
		Redelivered: req.Redelivered,
		Settle:      func(plugin.SettleAction) {},
	}
	if !lp.sub.NoAck {
		owner, id := lp.owner, req.DeliveryID
		d.Settle = func(action plugin.SettleAction) { b.forwardSettle(owner, id, action) }
	}
	if err := lp.sub.Deliver(d); err != nil {
		return fwdErr(plugin.KindInternal, "INTERNAL_ERROR - 投递给客户端失败: %v", err)
	}
	b.fwdDeliveries.Add(1)
	return fwdAck()
}

// handleForwardCanceled 由 Owner 调用：通知代理节点"你的消费者已被取消"，
// 代理节点据此向客户端下发 basic.cancel（consumer_cancel_notify）。
func (b *Broker) handleForwardCanceled(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var req fwdCanceledReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析取消通知失败: %w", err)
	}
	b.fwdMu.Lock()
	lp := b.fwdLocal[req.WireTag]
	delete(b.fwdLocal, req.WireTag)
	delete(b.fwdRepointUntil, req.WireTag)
	b.fwdMu.Unlock()
	if lp != nil {
		lp.session.forgetConsumer(lp.tag)
		if lp.sub.Cancel != nil {
			lp.sub.Cancel(req.Reason)
		}
		b.log.Debug("远端消费者已被取消", "queue", lp.queue, "consumer_tag", lp.tag, "reason", req.Reason)
	}
	return fwdAck()
}

// handleForwardKeepalive 由代理节点周期调用：续租它的代理消费者与未结算的跨节点拉取，
// 并把**本 Owner 不认识的**标签/拉取回带（供代理节点重新注册，见 fwdKeepaliveResp）。
func (b *Broker) handleForwardKeepalive(_ context.Context, from string, payload []byte) ([]byte, error) {
	var req fwdKeepaliveReq
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("解析续租请求失败: %w", err)
	}
	now := time.Now()
	var unknownTags []string
	var unknownGets []uint64
	b.fwdMu.Lock()
	for _, tag := range req.Tags {
		if p := b.fwdRemote[tag]; p != nil && p.proxyNode == from {
			p.lastSeen = now
			continue
		}
		// 本 Owner 不认这条注册（典型：分区超过租约期后被回收，且摘除通知没能送达）。
		// 回带给代理节点让它重新注册，而不是让它静默饿死（U14）。
		unknownTags = append(unknownTags, tag)
	}
	for _, id := range req.GetIDs {
		if hd := b.fwdHeld[id]; hd != nil && hd.proxyNode == from {
			hd.lastSeen = now
			continue
		}
		unknownGets = append(unknownGets, id)
	}
	b.fwdMu.Unlock()
	return fwdJSON(fwdKeepaliveResp{
		fwdEnvelope:   fwdOK,
		UnknownTags:   unknownTags,
		UnknownGetIDs: unknownGets,
	})
}

// ---------------------------------------------------------------------------
// 投递、持有与回收
// ---------------------------------------------------------------------------

// deliverToProxy 把一条投递推给代理节点（Owner 侧调用，运行在队列的投递循环里）。
func (b *Broker) deliverToProxy(p *remoteProxy, d *plugin.Delivery) error {
	raw, err := b.encodeMessage(d.Message)
	if err != nil {
		return err
	}
	req := fwdDeliverReq{
		WireTag: p.wireTag, Queue: p.queue, Redelivered: d.Redelivered, Msg: raw,
	}
	ctx, cancel := context.WithTimeout(context.Background(), fwdDeliverTimeout)
	defer cancel()

	var held uint64
	if !p.noAck {
		// no-ack 投递在 Owner 侧已结算，无需持有。
		held = b.holdDelivery(d, p.proxyNode, p.wireTag)
		req.DeliveryID = held
	}
	var resp fwdEnvelopeOnly
	err = b.fwdCall(ctx, p.proxyNode, fwdMethodDeliver, req, &resp)
	if err != nil {
		if held != 0 {
			b.releaseHeld(held)
		}
		// 代理节点接不住（进程没了 / 网络断了）：摘掉这个消费者，让它未确认的消息回到队列。
		// 用异步摘除以避免在队列的投递循环里再进队列锁。
		go b.dropRemoteProxy(p, "投递失败: "+err.Error())
		return err
	}
	b.touchRemoteProxy(p)
	return nil
}

// holdDelivery 记录一条等待远端结算的投递，返回持有编号。
func (b *Broker) holdDelivery(d *plugin.Delivery, proxyNode, wireTag string) uint64 {
	b.fwdMu.Lock()
	defer b.fwdMu.Unlock()
	b.fwdNextID++
	id := b.fwdNextID
	b.fwdHeld[id] = &heldDelivery{d: d, proxyNode: proxyNode, wireTag: wireTag, lastSeen: time.Now()}
	return id
}

// releaseHeld 丢弃一条持有记录（不做结算）。
func (b *Broker) releaseHeld(id uint64) {
	b.fwdMu.Lock()
	delete(b.fwdHeld, id)
	b.fwdMu.Unlock()
}

// releaseHeldByTag 丢弃某个代理消费者名下的全部持有记录。
//
// 只删记录不结算：这些消息由队列自身的"取消消费者 → 未确认消息回队头"负责重新入队。
func (b *Broker) releaseHeldByTag(wireTag string) {
	b.fwdMu.Lock()
	for id, hd := range b.fwdHeld {
		if hd.wireTag == wireTag {
			delete(b.fwdHeld, id)
		}
	}
	b.fwdMu.Unlock()
}

// touchRemoteProxy 刷新代理消费者的"最近活跃"时间（投递成功本身就是一次续租）。
func (b *Broker) touchRemoteProxy(p *remoteProxy) {
	now := time.Now()
	b.fwdMu.Lock()
	if cur := b.fwdRemote[p.wireTag]; cur != nil {
		cur.lastSeen = now
	}
	b.fwdMu.Unlock()
}

// dropProxiesForQueue 摘除挂在某队列上的全部代理消费者。
//
// 用于"队列换服务节点"这类情形（例如仲裁队列的 leader 变更）：代理消费者注册在旧节点上，
// 新节点不会给它们投递，必须主动摘除并通知原节点，否则客户端会静默地收不到消息。
func (b *Broker) dropProxiesForQueue(vhost, queue, reason string) {
	b.fwdMu.Lock()
	var victims []*remoteProxy
	for tag, p := range b.fwdRemote {
		if p.vhost == vhost && p.queue == queue {
			delete(b.fwdRemote, tag)
			victims = append(victims, p)
		}
	}
	b.fwdMu.Unlock()
	for _, p := range victims {
		b.dropRemoteProxyLocal(p, reason)
	}
}

// adoptLocalProxiesAsConsumers 把"代理到旧 leader"的本地消费者改挂为本节点队列上的本地消费者。
//
// 场景：客户端连在 follower 上、消费者被代理到 leader；leader 宕机后**本节点当选新 leader**。
// 旧 leader 已经无法发出 basic.cancel，本节点 q.consumers 里也没有这些消费者 —— 若不改挂，
// 客户端会静默收不到消息、队列持续堆积（实测：选主后 consumers 归零、ready 一路涨）。
//
// 改挂后 Deliver 直接走会话投给客户端（不再经跨节点回推），Cancel 仍走协议层。
// 返回成功改挂的数量。
func (b *Broker) adoptLocalProxiesAsConsumers(v *vhost, q *queue) int {
	b.fwdMu.Lock()
	adopted := make([]*localProxy, 0, 4)
	for tag, p := range b.fwdLocal {
		if p.vhost == v.name && p.queue == q.name {
			delete(b.fwdLocal, tag)
			delete(b.fwdRepointUntil, tag)
			adopted = append(adopted, p)
		}
	}
	b.fwdMu.Unlock()

	n := 0
	for _, p := range adopted {
		sub := p.sub
		// 队列层标签用**客户端标签**，不能用 wireTag：adopt 之后这条消费者就是本节点队列上的
		// 本地消费者，而客户端后续的 basic.cancel（[vhostSession.Cancel]）与会话关闭清理都是按
		// **客户端标签**调用 q.cancel 的。若这里用 wireTag 注册，q.cancel(客户端标签) 就找不到条目
		// —— 消费者永久泄漏：管理面 `consumers` 不归零、队列持续向已断开的会话投递。
		// 投递与结算本来就走会话/队列的本地路径（与标签取值无关），因此改用客户端标签是安全的。
		sub.Tag = p.tag
		sub.Queue = q.name
		if err := q.subscribe(sub); err != nil {
			b.log.Warn("改挂代理消费者为本地消费者失败",
				"queue", q.name, "wire_tag", p.wireTag, "err", err)
			// 挂不上就显式取消并通知客户端，避免再次静默。
			if p.sub.Cancel != nil {
				p.sub.Cancel("CONSUMER_CANCELLED - quorum queue leader changed")
			}
			continue
		}
		b.log.Info("已把代理消费者改挂为本地消费者",
			"queue", q.name, "wire_tag", p.wireTag, "tag", p.tag)
		n++
	}
	return n
}

// queueOwnerKey 生成"队列归属"缓存与集合的键（vhost + 队列名唯一确定一条队列）。
func queueOwnerKey(vhost, queue string) string { return vhost + "\x00" + queue }

// refreshChangedQueues 遍历各 vhost 的仲裁队列，与 owner 缓存比较，返回 owner 发生变化的队列集合。
//
// 这是稳态快路径：只有仲裁队列的 owner 会随选举变化（经典队列的 owner 创建即定、永不变），
// 因此只遍历仲裁队列（O(仲裁队列数)），owner 不变时返回空集，调用方据此**跳过** O(代理数) 的扫描。
func (b *Broker) refreshChangedQueues() map[string]struct{} {
	changed := make(map[string]struct{})
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
			owner := v.queueOwnerBestEffort(q)
			key := queueOwnerKey(v.name, q.name)
			b.fwdMu.Lock()
			prev, ok := b.fwdQueueOwner[key]
			if !ok || prev != owner {
				b.fwdQueueOwner[key] = owner
				changed[key] = struct{}{}
			}
			b.fwdMu.Unlock()
		}
	}
	return changed
}

// repointForwards 周期性校正本地代理消费者的归属：目标节点已不是队列 owner 时改挂过去。
//
// 为什么需要它（实测缺陷）：仲裁队列换 leader（甚至旧 leader 直接宕机）时，
// 代理消费者原本挂在旧 leader 上；旧 leader 要么宕机、要么只能清理自己的注册，
// 而**代理节点自己不知道目标变了**，于是客户端静默收不到消息、队列持续堆积。
// checkQuorumLeadership 只在本节点 leader 身份变化时动作，覆盖不到"一直是 follower"的代理节点，
// 所以这里按周期做校正：
//   - 新 owner 就是本节点 → 就地转成本地消费者（本地操作，直接做）；
//   - 新 owner 是别的节点 → 到新 owner 重新注册（含 RPC，交给独立协程，见 repointProxy）。
//
// 开销控制（O(队列数)）：稳态下先只比较各仲裁队列的 owner（见 refreshChangedQueues），
// **没有任何队列换主、也没有待重试的改挂时直接返回**，不触碰代理表 —— 每 tick 耗时与代理数量解耦。
// 只有在"确实换主"或"还有改挂在退避重试"这两条罕见路径上，才遍历代理（O(代理数)）。
//
// 本函数跑在共享后台协程上（与水位/集群/选主检查同一条），因此**自身绝不发起网络调用**：
// 否则一次选主里的批量改挂会把水位流控与选主感知一起卡住（见评估报告 §3）。
func (b *Broker) repointForwards() {
	if !b.clusterOn {
		return
	}
	changed := b.refreshChangedQueues()
	b.fwdMu.Lock()
	pending := len(b.fwdRepointUntil) > 0
	b.fwdMu.Unlock()
	if len(changed) == 0 && !pending {
		return // 稳态：无换主、无待重试 → 直接返回，不做 O(代理数) 扫描
	}

	now := time.Now()
	b.fwdMu.Lock()
	proxies := make([]*localProxy, 0, len(b.fwdLocal))
	for _, p := range b.fwdLocal {
		proxies = append(proxies, p)
	}
	b.fwdMu.Unlock()

	for _, p := range proxies {
		// 因换主进入时，只处理换主队列的代理（避免为一次换主遍历全部代理）。
		// 纯重试（无换主）时不做此过滤：待重试的代理可能属于 owner 未再次变化的队列。
		if !pending && len(changed) > 0 {
			if _, ok := changed[queueOwnerKey(p.vhost, p.queue)]; !ok {
				continue
			}
		}
		v, ok := b.vhostOf(p.vhost)
		if !ok {
			continue
		}
		q, ok := v.getQueue(p.queue)
		if !ok {
			continue
		}
		// 用 best-effort 版本：queueOwner 在"还不知道 leader"时会阻塞轮询最多 quorumLeaderWait（3s），
		// 绝不能在共享后台协程里等。拿不到 owner 就跳过，下一轮（1s 后）再试。
		owner := v.queueOwnerBestEffort(q)
		if owner == "" || owner == p.owner {
			continue
		}
		if owner == b.nodeID {
			b.adoptLocalProxiesAsConsumers(v, q)
			continue
		}
		// 退避：同一消费者在窗口内最多发起一次改挂，避免失败时每轮都发一遍 RPC。
		if !b.beginRepoint(p.wireTag, now) {
			continue
		}
		go b.repointProxy(p, q, owner)
	}
}

// beginRepoint 判断该代理标签现在是否可以发起一次改挂尝试（带退避与在途去重）。
func (b *Broker) beginRepoint(wireTag string, now time.Time) bool {
	b.fwdMu.Lock()
	defer b.fwdMu.Unlock()
	if until, ok := b.fwdRepointUntil[wireTag]; ok && now.Before(until) {
		return false
	}
	b.fwdRepointUntil[wireTag] = now.Add(fwdRepointBackoff)
	return true
}

// repointProxy 在独立协程里把一条代理消费者改挂到新的 owner 节点。
//
// 失败时**恢复旧登记**（remoteConsume 失败会把它从表里删掉）并保留退避记录，下一轮再试 ——
// 绝不静默丢消费者；成功则清理节流记录（此后 owner 已一致，不再进入本路径）。
func (b *Broker) repointProxy(p *localProxy, q *queue, newOwner string) {
	err := b.remoteConsume(p.session, q, p.sub)
	if err != nil {
		b.restoreLocalProxy(p)
		b.log.Info("改挂代理消费者到新 owner 失败，将退避后重试",
			"queue", p.queue, "wire_tag", p.wireTag, "new_owner", newOwner, "err", err)
		return
	}
	b.log.Info("代理消费者的队列归属已变化，已改挂到新 owner",
		"queue", p.queue, "old_owner", p.owner, "new_owner", newOwner)
	b.fwdMu.Lock()
	delete(b.fwdRepointUntil, p.wireTag)
	b.fwdMu.Unlock()
}

// restoreLocalProxy 在改挂失败后把旧登记放回（仅当该标签当前不在表里）。
//
// remoteConsume 的语义是"先登记再发起注册、失败即删除"，对首次订阅（失败要报给客户端）是对的；
// 但改挂复用它会连旧登记一起删掉，使该消费者永远不再重试。这里补上恢复，保证可重试。
func (b *Broker) restoreLocalProxy(p *localProxy) {
	b.fwdMu.Lock()
	if _, ok := b.fwdLocal[p.wireTag]; !ok {
		b.fwdLocal[p.wireTag] = p
	} else {
		// 已被并发改挂成功/取消：本轮的旧指针无意义，连同节流记录一起清掉。
		delete(b.fwdRepointUntil, p.wireTag)
	}
	b.fwdMu.Unlock()
}

// dropRemoteProxy 摘除一个代理消费者（先从登记表移除，再真正取消）。
func (b *Broker) dropRemoteProxy(p *remoteProxy, reason string) {
	b.fwdMu.Lock()
	_, present := b.fwdRemote[p.wireTag]
	delete(b.fwdRemote, p.wireTag)
	b.fwdMu.Unlock()
	if !present {
		return
	}
	b.dropRemoteProxyLocal(p, reason)
}

// dropRemoteProxyLocal 执行摘除动作：清持有、取消队列上的消费者、通知代理节点。
func (b *Broker) dropRemoteProxyLocal(p *remoteProxy, reason string) {
	v, ok := b.vhostOf(p.vhost)
	if !ok {
		return
	}
	q, ok := v.getQueue(p.queue)
	if !ok {
		return
	}
	b.releaseHeldByTag(p.wireTag)
	// 队列的 cancel 会把该消费者未确认的消息放回队头，因此上面只需删掉持有记录。
	if q.cancel(p.wireTag) {
		b.deleteAutoQueue(v, q)
	}
	b.notifyProxyCanceled(p, reason)
	b.log.Info("已摘除远端代理消费者", "proxy_node", p.proxyNode, "queue", p.queue, "reason", reason)
}

// deleteAutoQueue 处理自动删除队列：集群托管队列的删除必须经元数据层（与协议侧同语义）。
func (b *Broker) deleteAutoQueue(v *vhost, q *queue) {
	if q.clusterManaged() {
		if err := b.submitMeta(meta.OpDeleteQueue, meta.Queue{VHost: v.name, Name: q.name}); err != nil {
			b.log.Warn("自动删除队列的元数据删除失败", "queue", q.name, "err", err)
		}
		return
	}
	v.removeQueue(q)
}

// notifyProxyCanceled 尽力通知代理节点"消费者已被取消"，失败只记日志（对方可能已经不在）。
func (b *Broker) notifyProxyCanceled(p *remoteProxy, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
	defer cancel()
	var resp fwdEnvelopeOnly
	if err := b.fwdCall(ctx, p.proxyNode, fwdMethodCanceled,
		fwdCanceledReq{WireTag: p.wireTag, Reason: reason}, &resp); err != nil {
		b.log.Debug("通知代理节点消费者已取消失败（对端可能已下线）",
			"proxy_node", p.proxyNode, "wire_tag", p.wireTag, "err", err)
	}
}

// trackRemoteGet 记录一条尚未结算的跨节点拉取，供续租上报使用。
func (b *Broker) trackRemoteGet(id uint64, owner string) {
	b.fwdMu.Lock()
	b.fwdGets[id] = owner
	b.fwdMu.Unlock()
}

// untrackRemoteGet 移除已结算的跨节点拉取记录。
func (b *Broker) untrackRemoteGet(id uint64) {
	b.fwdMu.Lock()
	delete(b.fwdGets, id)
	b.fwdMu.Unlock()
}

// maintainForwards 是内核后台协程的一部分：代理侧续租、Owner 侧回收失效代理。
func (b *Broker) maintainForwards(now time.Time) {
	if !b.clusterOn {
		return
	}
	b.sendForwardKeepalive(now)
	b.reapForwards(now)
	// 校正代理消费者归属：队列换 leader（含旧 leader 宕机）后必须改挂，否则客户端静默收不到消息。
	b.repointForwards()
}

// sendForwardKeepalive 按周期向各 Owner 节点续租本节点持有的代理消费者与未结算拉取。
func (b *Broker) sendForwardKeepalive(now time.Time) {
	b.fwdMu.Lock()
	if now.Sub(b.fwdLastLease) < fwdLeaseInterval {
		b.fwdMu.Unlock()
		return
	}
	b.fwdLastLease = now
	byNode := map[string]*fwdKeepaliveReq{}
	for _, p := range b.fwdLocal {
		req := byNode[p.owner]
		if req == nil {
			req = &fwdKeepaliveReq{Node: b.nodeID}
			byNode[p.owner] = req
		}
		req.Tags = append(req.Tags, p.wireTag)
	}
	for id, owner := range b.fwdGets {
		req := byNode[owner]
		if req == nil {
			req = &fwdKeepaliveReq{Node: b.nodeID}
			byNode[owner] = req
		}
		req.GetIDs = append(req.GetIDs, id)
	}
	b.fwdMu.Unlock()

	for owner, req := range byNode {
		go func(owner string, req *fwdKeepaliveReq) {
			ctx, cancel := context.WithTimeout(context.Background(), fwdControlTimeout)
			defer cancel()
			var resp fwdKeepaliveResp
			if err := b.fwdCall(ctx, owner, fwdMethodKeepalive, req, &resp); err != nil {
				b.log.Debug("向 Owner 续租失败", "owner", owner, "err", err)
				return
			}
			b.healUnknownForwardLeases(owner, resp)
		}(owner, req)
	}
}

// healUnknownForwardLeases 处理续租应答里的"未知标签 / 未知拉取"（见 fwdKeepaliveResp）。
//
// 未知标签 = 本节点仍持有的代理消费者在 Owner 上已不存在（多为分区超过租约期后 Owner 摘除了它、
// 而摘除通知也被分区挡住）。这里对它**重新注册**（本节点已成为服务节点时就地转本地消费者），
// 而不是让它静默饿死。未知拉取（跨节点 basic.get）直接丢弃本地登记，后续拉取会重新发起。
func (b *Broker) healUnknownForwardLeases(owner string, resp fwdKeepaliveResp) {
	if len(resp.UnknownGetIDs) > 0 {
		b.fwdMu.Lock()
		for _, id := range resp.UnknownGetIDs {
			delete(b.fwdGets, id)
		}
		b.fwdMu.Unlock()
	}
	for _, tag := range resp.UnknownTags {
		b.fwdMu.Lock()
		p := b.fwdLocal[tag]
		b.fwdMu.Unlock()
		if p == nil {
			continue // 已取消或已改挂
		}
		v, ok := b.vhostOf(p.vhost)
		if !ok {
			continue
		}
		q, ok := v.getQueue(p.queue)
		if !ok {
			continue
		}
		if v.queueOwnerBestEffort(q) == b.nodeID {
			// 本节点已是服务节点：把本地代理就地转成本地消费者。
			b.adoptLocalProxiesAsConsumers(v, q)
			continue
		}
		// 注册已被 Owner 权威地判为失效，因此这里**无论 owner 是否变化都要重新注册**；
		// 复用改挂路径（remoteConsume 会按当前 owner 重新解析并注册），并带退避防抖。
		if !b.beginRepoint(p.wireTag, time.Now()) {
			continue // 退避窗口内：下一轮续租再试
		}
		b.log.Info("Owner 续租应答回报未知消费者标签，正在重新注册",
			"queue", p.queue, "wire_tag", p.wireTag, "owner", owner)
		go b.repointProxy(p, q, owner)
	}
}

// reapForwards 在 Owner 侧回收租约过期的代理消费者与未结算拉取。
func (b *Broker) reapForwards(now time.Time) {
	b.fwdMu.Lock()
	if now.Sub(b.fwdLastReap) < fwdReapInterval {
		b.fwdMu.Unlock()
		return
	}
	b.fwdLastReap = now
	var staleProxies []*remoteProxy
	var staleHeld []*heldDelivery
	for tag, p := range b.fwdRemote {
		if now.Sub(p.lastSeen) > fwdLeaseTTL {
			delete(b.fwdRemote, tag)
			staleProxies = append(staleProxies, p)
		}
	}
	for id, hd := range b.fwdHeld {
		if now.Sub(hd.lastSeen) > fwdLeaseTTL {
			delete(b.fwdHeld, id)
			staleHeld = append(staleHeld, hd)
		}
	}
	b.fwdMu.Unlock()

	for _, hd := range staleHeld {
		// 代理节点已经不再续租：把它持有但未结算的消息送回队列头（可能被重投，但不丢）。
		b.log.Warn("跨节点投递的持有已过期，消息重新入队",
			"proxy_node", hd.proxyNode, "queue_tag", hd.wireTag)
		hd.d.Settle(plugin.SettleRequeue)
	}
	for _, p := range staleProxies {
		b.dropRemoteProxyLocal(p, "代理节点租约过期（未续租）")
	}
}

// ForwardStatus 返回转发层的可观测状态（管理面用）。
type ForwardStatus struct {
	// ProxyConsumers 是本节点代客户端持有的远端消费者数。
	ProxyConsumers int
	// RemoteConsumers 是远端挂在本节点队列上的代理消费者数。
	RemoteConsumers int
	// HeldDeliveries 是本节点持有、等待远端结算的投递数。
	HeldDeliveries int
	// ForwardedOut / ForwardedIn 是转发出去与接收进来的消息条数。
	ForwardedOut uint64
	ForwardedIn  uint64
	// ForwardedBatches 是转发发布所用 RPC 次数：ForwardedOut/ForwardedBatches 即平均批大小。
	ForwardedBatches uint64
	// Deliveries 是推回代理节点的投递条数。
	Deliveries uint64
	// Owner 侧 handleForwardPublish 的分支计数（B5-follow-2 排查用）。
	QueueMissing   uint64 // vhost/queue 在 Owner 本地缺失（静默丢弃候选）
	RemoteMismatch uint64 // 本节点并非该队列的服务节点
	PublishErr     uint64 // 入队失败
	DurableErr     uint64 // 落盘/多数派等待失败
	Rejected       uint64 // 长度限制拒绝
	NoWait         uint64 // 仲裁队列发布未给出落盘等待（=队列已关闭，未入队）
	// PhantomOK 是代理侧收到的"未路由且未拒绝且无错误"的应答数（静默丢弃签名，应为 0）。
	PhantomOK uint64
	// 转发边界按 id 追踪样本（B5-follow-2 排查）：代理侧转发成功/失败的 id、
	// Owner 侧接受入队/失败的 id。用于判定"每转发批丢 1 条且拿到成功应答"发生在边界哪一侧。
	// 四组集合均为**尾窗**（容量恒定，成功 65536 / 失败 262144）；对应的 `*Truncated` 为 true
	// 表示该组发生过覆盖、已是"最近窗口"而非全量样本。
	SentOKIDs        []uint64
	SentBadIDs       []uint64
	SentOKTruncated  bool
	SentBadTruncated bool
	InOKIDs          []uint64
	InBadIDs         []uint64
	InOKTruncated    bool
	InBadTruncated   bool
}

// ForwardStatus 汇总转发层状态。
func (b *Broker) ForwardStatus() ForwardStatus {
	b.fwdMu.Lock()
	st := ForwardStatus{
		ProxyConsumers:  len(b.fwdLocal),
		RemoteConsumers: len(b.fwdRemote),
		HeldDeliveries:  len(b.fwdHeld),
	}
	b.fwdMu.Unlock()
	st.ForwardedOut = b.fwdOut.Load()
	st.ForwardedIn = b.fwdIn.Load()
	st.ForwardedBatches = b.fwdBatches.Load()
	st.Deliveries = b.fwdDeliveries.Load()
	st.QueueMissing = b.fwdQueueMissing.Load()
	st.RemoteMismatch = b.fwdRemoteMismatch.Load()
	st.PublishErr = b.fwdPublishErr.Load()
	st.DurableErr = b.fwdDurableErr.Load()
	st.Rejected = b.fwdRejected.Load()
	st.NoWait = b.fwdNoWait.Load()
	st.PhantomOK = b.fwdPhantomOK.Load()
	st.SentOKIDs, st.SentBadIDs, st.SentOKTruncated, st.SentBadTruncated = b.ForwardSentIDs()
	st.InOKIDs, st.InBadIDs, st.InOKTruncated, st.InBadTruncated = b.ForwardInIDs()
	return st
}

// ---------------------------------------------------------------------------
// 编码与调用辅助
// ---------------------------------------------------------------------------

func (b *Broker) encodeMessage(m *plugin.Message) ([]byte, error) {
	b.codecMu.RLock()
	c := b.codec
	b.codecMu.RUnlock()
	if c == nil {
		return nil, plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 未配置消息编解码器，无法跨节点转发消息（进程入口需调用 SetMessageCodec）")
	}
	raw, err := c.EncodeMessage(m)
	if err != nil {
		return nil, plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 编码待转发消息失败: %v", err)
	}
	return raw, nil
}

func (b *Broker) decodeMessage(raw []byte) (*plugin.Message, error) {
	b.codecMu.RLock()
	c := b.codec
	b.codecMu.RUnlock()
	if c == nil {
		return nil, plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 未配置消息编解码器，无法接收跨节点转发的消息")
	}
	msg, err := c.DecodeMessage(raw)
	if err != nil {
		return nil, plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 解码转发消息失败: %v", err)
	}
	return msg, nil
}

// fwdCall 发起一次转发 RPC 并把应答解到 out（out 需内嵌 fwdEnvelope）。
//
// 一切失败都收敛为 *plugin.Error：协议层依赖 Kind 决定错误码与作用域，
// "内核返回错误一律是 *Error"这条契约不因跨节点而破例。
func (b *Broker) fwdCall(ctx context.Context, node, method string, req, out any) error {
	if node == "" {
		// 仲裁队列在选主窗口里会短暂没有服务节点：明确报"请重试"，而不是发往一个空地址。
		return plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 队列的服务节点暂不可知（%s），请重试", method)
	}
	// 只读一次 b.cluster：Close() 会在运行期把它置为 nil，若在这里分两次读取字段，
	// 第一次判空通过、第二次已变 nil，就会变成"在 nil 接口上调用方法"而 panic。
	c := b.cluster
	if c == nil {
		return plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 本节点未启用集群，无法与节点 %s 通信（%s）", node, method)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 编码转发请求失败（%s）: %v", method, err)
	}
	resp, err := c.Call(ctx, node, method, raw)
	if err != nil {
		return plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 与节点 %s 通信失败（%s）: %v", node, method, err)
	}
	var env fwdEnvelope
	if err := json.Unmarshal(resp, &env); err != nil {
		return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 解析节点 %s 的应答失败（%s）: %v", node, method, err)
	}
	if !env.OK {
		return plugin.Errorf(plugin.ErrorKind(env.Kind), "%s", env.Err)
	}
	if out != nil {
		if err := json.Unmarshal(resp, out); err != nil {
			return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 解析节点 %s 的应答失败（%s）: %v", node, method, err)
		}
	}
	return nil
}

// fwdOK 是所有成功应答共用的信封常量。
//
// 显式写出来而不是让零值当成功：`OK` 的零值是 false，
// 一旦哪个处理器忘了设，客户端就会收到"失败但没有原因"的应答 —— 这种坑不值得踩第二次。
var fwdOK = fwdEnvelope{OK: true}

// fwdJSON 编码一个成功应答。
func fwdJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("编码转发应答失败: %w", err)
	}
	return raw, nil
}

// fwdAck 返回一个"成功且无附加数据"的应答。
func fwdAck() ([]byte, error) { return fwdJSON(fwdEnvelopeOnly{fwdEnvelope: fwdOK}) }

// fwdErr 编码一个"带语义分类的失败"应答。
func fwdErr(kind plugin.ErrorKind, format string, args ...any) ([]byte, error) {
	return fwdJSON(fwdEnvelope{Kind: int(kind), Err: fmt.Sprintf(format, args...)})
}

// fwdErrFrom 把内核返回的 *plugin.Error 原样搬到应答里，保留 Kind。
func fwdErrFrom(err error) ([]byte, error) {
	if pe, ok := err.(*plugin.Error); ok {
		return fwdErr(pe.Kind, "%s", pe.Text)
	}
	return fwdErr(plugin.KindInternal, "INTERNAL_ERROR - %v", err)
}
