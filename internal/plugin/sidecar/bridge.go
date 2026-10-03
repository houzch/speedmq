package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

// 本文件是"把 plugin.Session 桥成 RPC"的内核侧实现：插件进程经反向调用（插件 → 内核）
// 触达内核语义。它只做**映射**，不另写一套语义 —— 每个方法都直接落到 plugin.Session 的
// 对应方法上，因此 vhost / 权限 / 路由 / 队列语义 / 确认在所有协议上仍然一致。
//
// 生命周期与并发：
//   - 每条客户端连接对应一个"流"（sidecar stream），流上可打开一个 plugin.Session；
//   - 会话里的消费者由内核按需把投递**回推**给插件（正向调用 session.deliver），
//     插件再经 session.settle 结算；未结算的投递在流关闭 / 插件断开时一律按"回队"处理
//     （与 internal/protocol/amqp091 的 drainPending 同一口径，避免消息滞留）。

// streamAttachment 是随流绑定到内核侧的不透明句柄内容（见 pkg/sidecar.Open.Attachment）。
//
// 除内核操作面外还带上**客户端地址**：core.authenticate 会把它交给内核的认证存储，
// 而用户表里的 remote_access 要按真实来源判定（默认账号只允许回环登录）。
// 若伪造一个回环地址，就等于静默放宽了这条安全约束。
type streamAttachment struct {
	core   sdk.Core
	remote net.Addr
}

// bridgeStream 是一条流上的桥状态。
type bridgeStream struct {
	// core 是这条连接的内核操作面（由 protocolHost.Serve 随流绑定）。
	core sdk.Core
	// remote 是客户端地址（认证时判定 remote_access 用）。
	remote net.Addr
	// sess 是在该流上打开的会话；为 nil 表示插件还没调 session.open。
	sess sdk.Session
	// consumers 是本会话注册的消费者（标签 → 状态）。
	consumers map[string]*bridgeConsumer
	// closeOnce 保证收尾动作只做一次（流结束与插件断开两条路径可能同时触发）。
	closeOnce sync.Once
}

// bridgeConsumer 是一个已注册消费者的投递泵状态。
type bridgeConsumer struct {
	noAck bool
	// ch 承接内核推来的投递，交给投递泵回推给插件。
	ch chan *sdk.Delivery
	// done 关闭即通知投递泵退出（消费者被取消 / 会话结束 / 连接断开）。
	done chan struct{}
	// once 保证 done 只关一次。
	once sync.Once
	// cancel 取消投递泵的上下文，从而让在途的 session.deliver 调用立即返回。
	// 由投递泵在启动时写入，signal 时读取；零值安全。
	mu     sync.Mutex
	cancel context.CancelFunc
}

// signal 通知投递泵退出（幂等）。**不等待**泵退出 —— 它可能被"插件发起的一次
// session.cancel"间接触发，而泵正等待同一次交互里的 session.deliver 应答，等下去会自锁。
func (c *bridgeConsumer) signal() {
	c.once.Do(func() { close(c.done) })
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// setCancel 由投递泵登记其上下文取消函数；若在登记前就已被要求停止，立即取消。
func (c *bridgeConsumer) setCancel(cancel context.CancelFunc) {
	c.mu.Lock()
	c.cancel = cancel
	select {
	case <-c.done:
		c.mu.Unlock()
		cancel()
	default:
		c.mu.Unlock()
	}
}

// pendingDelivery 是一条已回推给插件、等待结算的投递。
type pendingDelivery struct {
	id     uint64
	stream uint32
	deliv  *sdk.Delivery
}

// ---------------------------------------------------------------------------
// 流 / 核心操作面的绑定
// ---------------------------------------------------------------------------

// onStreamOpen 由 pkg/sidecar.Client 在建流时（kindOpen 帧写出之前）回调，
// 把流与这条连接的内核操作面绑定。之所以要早于帧写出：插件一旦知道流号就可能立刻
// 发起 session.open，绑定若晚一步就会命中"未知流"。
func (p *Plugin) onStreamOpen(stream uint32, meta sidecar.Open) {
	att, ok := meta.Attachment.(streamAttachment)
	if !ok || att.core == nil {
		return
	}
	p.sessMu.Lock()
	p.streams[stream] = &bridgeStream{
		core:      att.core,
		remote:    att.remote,
		consumers: map[string]*bridgeConsumer{},
	}
	p.sessMu.Unlock()
}

// releaseStream 释放某条流上的桥状态：停消费者、关会话，并把未结算投递重新入队。
func (p *Plugin) releaseStream(stream uint32) {
	p.sessMu.Lock()
	bs := p.streams[stream]
	delete(p.streams, stream)
	pending := p.takePendingLocked(stream)
	p.sessMu.Unlock()

	for _, pd := range pending {
		pd.deliv.Settle(sdk.SettleRequeue)
	}
	if bs != nil {
		p.closeStream(bs)
	}
}

// releaseCore 兜底回收：Open 失败时调用方拿不到流号，用 core 身份反查并释放。
func (p *Plugin) releaseCore(core sdk.Core) {
	if core == nil {
		return
	}
	p.sessMu.Lock()
	var id uint32
	found := false
	for sid, bs := range p.streams {
		if bs.core == core {
			id, found = sid, true
			break
		}
	}
	p.sessMu.Unlock()
	if found {
		p.releaseStream(id)
	}
}

// resetStreams 在插件连接断开时清空全部桥状态（新连接会重新分配流号，不能串台）。
func (p *Plugin) resetStreams() {
	p.sessMu.Lock()
	streams := p.streams
	p.streams = map[uint32]*bridgeStream{}
	deliveries := p.deliveries
	p.deliveries = map[uint64]*pendingDelivery{}
	p.sessMu.Unlock()

	for _, pd := range deliveries {
		pd.deliv.Settle(sdk.SettleRequeue)
	}
	for _, bs := range streams {
		p.closeStream(bs)
	}
}

// closeStream 收尾一条流的桥状态（幂等）。
func (p *Plugin) closeStream(bs *bridgeStream) {
	bs.closeOnce.Do(func() {
		p.sessMu.Lock()
		consumers := bs.consumers
		bs.consumers = map[string]*bridgeConsumer{}
		p.sessMu.Unlock()
		for _, cs := range consumers {
			cs.signal()
		}
		// 不等投递泵退出：见 signal 的说明；泵会在 done 关闭或在途调用被取消后自行结束。
		if bs.sess != nil {
			bs.sess.Close()
		}
	})
}

// takePendingLocked 取走某条流上所有未结算投递（调用方持 sessMu）。
func (p *Plugin) takePendingLocked(stream uint32) []*pendingDelivery {
	var out []*pendingDelivery
	for id, pd := range p.deliveries {
		if pd.stream == stream {
			out = append(out, pd)
			delete(p.deliveries, id)
		}
	}
	return out
}

func (p *Plugin) sessionFor(stream uint32) (sdk.Session, error) {
	p.sessMu.Lock()
	bs := p.streams[stream]
	var sess sdk.Session
	if bs != nil {
		sess = bs.sess
	}
	p.sessMu.Unlock()
	if sess == nil {
		return nil, &sidecar.RPCError{Kind: int(sdk.KindPreconditionFailed),
			Text: fmt.Sprintf("sidecar: 流 %d 尚未打开会话（请先调用 %s）", stream, sidecar.MethodSessionOpen)}
	}
	return sess, nil
}

// ---------------------------------------------------------------------------
// 反向调用分发
// ---------------------------------------------------------------------------

// onCall 是 pkg/sidecar.ClientOptions.OnCall 的实现：插件发来的反向调用在这里落到内核语义。
func (p *Plugin) onCall(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodCoreAuthenticate:
		var req sidecar.CoreAuthenticateParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		return p.authenticate(ctx, req)
	case sidecar.MethodSessionOpen:
		var req sidecar.SessionOpenParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		return nil, p.openSession(req)
	case sidecar.MethodSessionClose:
		var req sidecar.SessionCloseParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		p.releaseStream(req.Stream)
		return nil, nil
	default:
		return p.dispatchSession(method, params)
	}
}

// authenticate 让插件按自己的协议完成认证（core.authenticate）。
//
// 认证成功后这条连接的内核操作面才有身份，后续 session.open 才能通过权限检查 ——
// 这正是外部进程插件与进程内协议插件在认证上的等价点（后者直接调 core.Authenticate）。
// 刻意不持 sessMu 调用：认证会落到用户表（另有自己的锁），没必要占着桥的锁。
func (p *Plugin) authenticate(ctx context.Context, req sidecar.CoreAuthenticateParams) (any, error) {
	p.sessMu.Lock()
	bs := p.streams[req.Stream]
	p.sessMu.Unlock()
	if bs == nil {
		return nil, &sidecar.RPCError{Kind: int(sdk.KindNotFound),
			Text: fmt.Sprintf("sidecar: 未知的流 %d（内核未把该流绑定到连接）", req.Stream)}
	}
	ident, err := bs.core.Authenticate(ctx, req.Mechanism, req.Response, bs.remote)
	if err != nil {
		return nil, errToRPC(err)
	}
	return sidecar.AuthIdentityDTO{User: ident.User, VHost: ident.VHost}, nil
}

func (p *Plugin) openSession(req sidecar.SessionOpenParams) error {
	// 整段持锁：core.Session 是纯内存操作；若在两次加锁之间释放锁，可能被 releaseStream
	// 抢先回收该流，导致会话被挂到一个已失效的桥状态上（永不被关闭）。
	p.sessMu.Lock()
	defer p.sessMu.Unlock()
	bs := p.streams[req.Stream]
	if bs == nil {
		return &sidecar.RPCError{Kind: int(sdk.KindNotFound),
			Text: fmt.Sprintf("sidecar: 未知的流 %d（内核未把该流绑定到连接）", req.Stream)}
	}
	if bs.sess != nil {
		return &sidecar.RPCError{Kind: int(sdk.KindPreconditionFailed),
			Text: fmt.Sprintf("sidecar: 流 %d 上的会话已打开", req.Stream)}
	}
	sess, err := bs.core.Session(req.VHost)
	if err != nil {
		return errToRPC(err)
	}
	bs.sess = sess
	return nil
}

// dispatchSession 处理需要"某条流上的会话"的方法，一对一映射到 plugin.Session。
func (p *Plugin) dispatchSession(method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeclareExchange:
		var req sidecar.ExchangeDeclareParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		return nil, errToRPC(sess.DeclareExchange(sdk.ExchangeDeclare{
			Name: req.Name, Type: sdk.ExchangeType(req.Type), Passive: req.Passive,
			Durable: req.Durable, AutoDelete: req.AutoDelete, Internal: req.Internal, Arguments: req.Arguments,
		}))

	case sidecar.MethodSessionDeleteExchange:
		var req sidecar.DeleteExchangeParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		return nil, errToRPC(sess.DeleteExchange(req.Name, req.IfUnused))

	case sidecar.MethodSessionBindExchange, sidecar.MethodSessionUnbindExchange:
		var req sidecar.ExchangeBindParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		if method == sidecar.MethodSessionBindExchange {
			return nil, errToRPC(sess.BindExchange(req.Destination, req.Source, req.RoutingKey, req.Arguments))
		}
		return nil, errToRPC(sess.UnbindExchange(req.Destination, req.Source, req.RoutingKey, req.Arguments))

	case sidecar.MethodSessionDeclareQueue:
		var req sidecar.QueueDeclareParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		info, err := sess.DeclareQueue(sdk.QueueDeclare{
			Name: req.Name, Passive: req.Passive, Durable: req.Durable,
			Exclusive: req.Exclusive, AutoDelete: req.AutoDelete, Arguments: req.Arguments,
		})
		if err != nil {
			return nil, errToRPC(err)
		}
		return queueInfoDTO(info), nil

	case sidecar.MethodSessionDeleteQueue:
		var req sidecar.DeleteQueueParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		info, err := sess.DeleteQueue(req.Name, req.IfUnused, req.IfEmpty)
		if err != nil {
			return nil, errToRPC(err)
		}
		return queueInfoDTO(info), nil

	case sidecar.MethodSessionBindQueue, sidecar.MethodSessionUnbindQueue:
		var req sidecar.QueueBindParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		if method == sidecar.MethodSessionBindQueue {
			return nil, errToRPC(sess.BindQueue(req.Queue, req.Exchange, req.RoutingKey, req.Arguments))
		}
		return nil, errToRPC(sess.UnbindQueue(req.Queue, req.Exchange, req.RoutingKey, req.Arguments))

	case sidecar.MethodSessionPurgeQueue:
		var req sidecar.PurgeQueueParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		n, err := sess.PurgeQueue(req.Name)
		if err != nil {
			return nil, errToRPC(err)
		}
		return sidecar.PurgeResult{Count: n}, nil

	case sidecar.MethodSessionPublish:
		var req sidecar.PublishParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		res, err := sess.Publish(messageFromDTO(req.Message), req.Exchange, req.RoutingKey, req.Mandatory)
		if err != nil {
			return nil, errToRPC(err)
		}
		// 持久化路径必须在**返回之前**完成：调用返回即等于"已按 fsync 档位落盘"，
		// 与协议插件内"收到 confirm 即已落盘"的语义一致。
		if res.Durable != nil {
			if err := res.Durable(); err != nil {
				return nil, errToRPC(err)
			}
		}
		return sidecar.PublishResultDTO{Routed: res.Routed, Rejected: res.Rejected}, nil

	case sidecar.MethodSessionGet:
		var req sidecar.GetParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		d, ok, err := sess.Get(req.Queue, req.NoAck)
		if err != nil {
			return nil, errToRPC(err)
		}
		out := sidecar.GetResult{Found: ok}
		if ok {
			var id uint64
			if !req.NoAck {
				id = p.registerPending(req.Stream, d)
			}
			out.Delivery = deliveryDTO(d, id)
		}
		return out, nil

	case sidecar.MethodSessionConsume:
		var req sidecar.ConsumeParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		return p.consume(req)

	case sidecar.MethodSessionCancel:
		var req sidecar.CancelParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		sess, err := p.sessionFor(req.Stream)
		if err != nil {
			return nil, err
		}
		err = sess.Cancel(req.Tag)
		p.removeConsumer(req.Stream, req.Tag)
		return nil, errToRPC(err)

	case sidecar.MethodSessionSettle:
		var req sidecar.SettleParams
		if err := unmarshal(params, &req); err != nil {
			return nil, err
		}
		return nil, p.settle(req)

	default:
		return nil, fmt.Errorf("sidecar: 未知的内核方法 %q", method)
	}
}

// consume 注册消费者并把投递回推接好：投递经 session.deliver 推给插件，插件再经
// session.settle 结算；未结算的投递在流关闭时由 releaseStream 重新入队。
func (p *Plugin) consume(req sidecar.ConsumeParams) (any, error) {
	sess, err := p.sessionFor(req.Stream)
	if err != nil {
		return nil, err
	}
	cs := &bridgeConsumer{
		noAck: req.NoAck,
		ch:    make(chan *sdk.Delivery, 64),
		done:  make(chan struct{}),
	}
	sub := sdk.Subscription{
		Tag:       req.Tag,
		Queue:     req.Queue,
		NoAck:     req.NoAck,
		Exclusive: req.Exclusive,
		Prefetch:  req.Prefetch,
		// Deliver 在通道满时**阻塞**而不是直接报错：报错会让内核把消息回队并立刻重投，
		// 在持续满的情况下变成空转；阻塞则把背压交还给该队列的派发协程（与慢消费者同效）。
		Deliver: func(d *sdk.Delivery) error {
			select {
			case cs.ch <- d:
				return nil
			case <-cs.done:
				return errors.New("sidecar: 消费者已结束")
			}
		},
		// Cancel 只做信号，不等待投递泵退出：它可能被内核在持锁路径上调用，等退出会自锁。
		Cancel: func(string) { cs.signal() },
	}
	tag, err := sess.Consume(sub)
	if err != nil {
		return nil, errToRPC(err)
	}

	p.sessMu.Lock()
	if bs := p.streams[req.Stream]; bs != nil {
		bs.consumers[tag] = cs
	}
	p.sessMu.Unlock()

	go p.pump(req.Stream, cs)
	return sidecar.ConsumeResult{Tag: tag}, nil
}

// pump 是投递泵：把消费者收到的投递逐条回推给插件（逐条串行，保证同一消费者的投递顺序）。
//
// 它绑定一个可取消的 ctx：消费者停止（内核主动取消 / 会话关闭 / 连接断开）时，
// 在途的 session.deliver 调用会被立刻取消，泵随即退出。
func (p *Plugin) pump(stream uint32, cs *bridgeConsumer) {
	ctx, cancel := context.WithCancel(context.Background())
	cs.setCancel(cancel)
	defer cancel()

	for {
		select {
		case <-cs.done:
			return
		case d := <-cs.ch:
			p.pushDelivery(ctx, stream, cs, d)
		}
	}
}

// pushDelivery 回推一条投递给插件（正向调用 session.deliver），失败则重新入队。
func (p *Plugin) pushDelivery(ctx context.Context, stream uint32, cs *bridgeConsumer, d *sdk.Delivery) {
	var id uint64
	if !cs.noAck {
		id = p.registerPending(stream, d)
	}
	params := sidecar.DeliverParams{
		Stream:      stream,
		DeliveryID:  id,
		Queue:       d.Queue,
		ConsumerTag: d.ConsumerTag,
		Redelivered: d.Redelivered,
		Message:     messageToDTO(d.Message),
	}
	c := p.Client()
	if c == nil {
		p.dropPending(id)
		d.Settle(sdk.SettleRequeue)
		return
	}
	// 连接断开（c.Done）或消费者停止（ctx 取消）都会让调用返回；失败即把消息重新入队。
	if err := c.Call(ctx, sidecar.MethodSessionDeliver, params, nil); err != nil {
		p.dropPending(id)
		d.Settle(sdk.SettleRequeue)
	}
}

// settle 结算一条投递。重复结算会被内核幂等忽略，因此这里找不到记录时也不报错。
func (p *Plugin) settle(req sidecar.SettleParams) error {
	action, err := parseSettleAction(req.Action)
	if err != nil {
		return err
	}
	p.sessMu.Lock()
	pd := p.deliveries[req.DeliveryID]
	delete(p.deliveries, req.DeliveryID)
	p.sessMu.Unlock()
	if pd == nil {
		return nil
	}
	pd.deliv.Settle(action)
	return nil
}

func (p *Plugin) removeConsumer(stream uint32, tag string) {
	p.sessMu.Lock()
	var cs *bridgeConsumer
	if bs := p.streams[stream]; bs != nil {
		cs = bs.consumers[tag]
		delete(bs.consumers, tag)
	}
	// 该消费者不再回推，清掉它名下的待结算登记；未确认消息本身已由内核随取消回队，
	// 之后插件即便补一次结算也是幂等的空操作。
	for id, pd := range p.deliveries {
		if pd.stream == stream && pd.deliv.ConsumerTag == tag {
			delete(p.deliveries, id)
		}
	}
	p.sessMu.Unlock()
	if cs != nil {
		cs.signal()
	}
}

// registerPending 为一条投递分配编号并登记（等待插件结算），返回编号。
func (p *Plugin) registerPending(stream uint32, d *sdk.Delivery) uint64 {
	p.sessMu.Lock()
	p.nextDelivery++
	id := p.nextDelivery
	p.deliveries[id] = &pendingDelivery{id: id, stream: stream, deliv: d}
	p.sessMu.Unlock()
	return id
}

func (p *Plugin) dropPending(id uint64) {
	if id == 0 {
		return
	}
	p.sessMu.Lock()
	delete(p.deliveries, id)
	p.sessMu.Unlock()
}

// ---------------------------------------------------------------------------
// DTO 转换与错误语义
// ---------------------------------------------------------------------------

// errToRPC 把内核错误翻成可跨 RPC 还原的带分类错误；非 *plugin.Error 原样返回。
func errToRPC(err error) error {
	if err == nil {
		return nil
	}
	var perr *sdk.Error
	if errors.As(err, &perr) {
		return &sidecar.RPCError{Kind: int(perr.Kind), Text: perr.Text}
	}
	return err
}

func unmarshal(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("sidecar: 反向调用参数无法解析: %w", err)
	}
	return nil
}

func parseSettleAction(action string) (sdk.SettleAction, error) {
	switch action {
	case sidecar.SettleActionAck:
		return sdk.SettleAck, nil
	case sidecar.SettleActionRequeue:
		return sdk.SettleRequeue, nil
	case sidecar.SettleActionReject:
		return sdk.SettleReject, nil
	default:
		return 0, fmt.Errorf("sidecar: 未知的结算动作 %q", action)
	}
}

func queueInfoDTO(info sdk.QueueInfo) sidecar.QueueInfoResult {
	return sidecar.QueueInfoResult{
		Name:          info.Name,
		MessageCount:  info.MessageCount,
		ConsumerCount: info.ConsumerCount,
	}
}

func deliveryDTO(d *sdk.Delivery, id uint64) *sidecar.DeliveryDTO {
	return &sidecar.DeliveryDTO{
		Message:     messageToDTO(d.Message),
		Queue:       d.Queue,
		ConsumerTag: d.ConsumerTag,
		Redelivered: d.Redelivered,
		DeliveryID:  id,
	}
}

func messageToDTO(m *sdk.Message) sidecar.MessageDTO {
	if m == nil {
		return sidecar.MessageDTO{}
	}
	return sidecar.MessageDTO{
		Exchange:    m.Exchange,
		RoutingKey:  m.RoutingKey,
		Properties:  propertiesToDTO(m.Properties),
		Body:        m.Body,
		Redelivered: m.Redelivered,
	}
}

func messageFromDTO(m sidecar.MessageDTO) *sdk.Message {
	return &sdk.Message{
		Exchange:    m.Exchange,
		RoutingKey:  m.RoutingKey,
		Properties:  propertiesFromDTO(m.Properties),
		Body:        m.Body,
		Redelivered: m.Redelivered,
	}
}

func propertiesToDTO(p sdk.Properties) sidecar.PropertiesDTO {
	return sidecar.PropertiesDTO{
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		Headers:         p.Headers,
		DeliveryMode:    p.DeliveryMode,
		Priority:        p.Priority,
		CorrelationID:   p.CorrelationID,
		ReplyTo:         p.ReplyTo,
		Expiration:      p.Expiration,
		MessageID:       p.MessageID,
		Timestamp:       p.Timestamp,
		Type:            p.Type,
		UserID:          p.UserID,
		AppID:           p.AppID,
	}
}

func propertiesFromDTO(p sidecar.PropertiesDTO) sdk.Properties {
	return sdk.Properties{
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		Headers:         p.Headers,
		DeliveryMode:    p.DeliveryMode,
		Priority:        p.Priority,
		CorrelationID:   p.CorrelationID,
		ReplyTo:         p.ReplyTo,
		Expiration:      p.Expiration,
		MessageID:       p.MessageID,
		Timestamp:       p.Timestamp,
		Type:            p.Type,
		UserID:          p.UserID,
		AppID:           p.AppID,
	}
}
