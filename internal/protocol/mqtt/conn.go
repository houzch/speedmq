package mqtt

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件是 MQTT 3.1.1 的连接状态机：握手、订阅、收发、QoS 结算、遗嘱与保留消息。
//
// 与内核的边界（设计 §10.5 的硬约束）：本文件**不认识**队列、绑定、死信、TTL 的实现，
// 它只把 MQTT 的语义翻译成 plugin.Session 上的调用。于是：
//   - MQTT 消息与 AMQP 消息进的是同一个队列，吃同一套死信/长度限制/持久化规则；
//   - 权限校验用的是同一个用户表（CONNECT 的 username/password → SASL PLAIN）；
//   - 管理面看到的连接、消费者、队列统计是同一份数据。
//
// 并发模型：读循环单协程；写路径经 writeMu 串行化（内核的投递回调在队列协程里执行，
// 与读循环并发）。订阅表由 mu 保护。

// MQTT 3.1.1 的协议标识：报文里必须是 "MQTT" + 级别 4。
const (
	protocolName  = "MQTT"
	protocolLevel = 4
)

// topicHeader 是随消息携带的"原始 MQTT 主题"头。
//
// 为什么需要它：MQTT 主题到路由键的映射会把 "/" 换成 "."，反过来换回来在
// "主题里本来含点号"时会有歧义。带上原始主题就让 MQTT→MQTT 的链路完全无损；
// 对一个不是由 MQTT 发布的消息（例如 AMQP 客户端直接发到 amq.topic），
// 则退回按路由键反推主题 —— 这正是跨协议互通期望的行为。
const topicHeader = "x-mqtt-topic"

// errClientDisconnect 表示客户端发来 DISCONNECT（正常断开，不该发遗嘱）。
var errClientDisconnect = errors.New("mqtt: 客户端主动断开")

// autoClientSeq 给"空 Client ID"的连接生成互不相同的临时标识。
var autoClientSeq atomic.Uint64

// maxQoS 是本插件支持的最大 QoS。
//
// MQTT 允许 0/1/2；这里支持到 1，订阅请求 QoS=2 时**按规范降级授予 1**
// （SUBACK 里如实回 1，客户端不会误以为拿到了恰好一次语义）。
// 入站 QoS=2 的发布报文仍会完整走完 PUBREC/PUBREL/PUBCOMP 四步握手，
// 但语义按"至少一次"处理（不做去重）—— 这一取舍写在文档里。
const maxQoS byte = 1

// options 是插件的配置段（plugins.mqtt）。
type options struct {
	// Exchange 是 MQTT 主题落地的交换机，默认 amq.topic。
	Exchange string `json:"exchange"`
	// MaxPacketSize 是单报文上限（字节），默认 8 MiB；<=0 表示用默认值。
	MaxPacketSize int `json:"max_packet_size"`
	// Prefetch 是每个订阅队列的 in-flight 上限（QoS≥1 生效），默认 32。
	Prefetch int `json:"prefetch"`
}

const (
	defaultExchange      = "amq.topic"
	defaultMaxPacketSize = 8 << 20
	defaultPrefetch      = 32
)

// withDefaults 补齐缺省值。
func (o options) withDefaults() options {
	if o.Exchange == "" {
		o.Exchange = defaultExchange
	}
	if o.MaxPacketSize <= 0 {
		o.MaxPacketSize = defaultMaxPacketSize
	}
	if o.Prefetch <= 0 {
		o.Prefetch = defaultPrefetch
	}
	return o
}

// qosQueue 是"某个 QoS 一个队列 + 一个消费者"的订阅单元。
//
// 为什么按 QoS 而不是按主题过滤器建队列：见 topic.go 的 subscriptionQueue 说明。
type qosQueue struct {
	qos     byte
	name    string
	tag     string
	filters map[string]struct{}
}

// conn 是一条 MQTT 连接。
type conn struct {
	log      *slog.Logger
	nc       net.Conn
	core     plugin.Core
	br       *bufio.Reader
	opts     options
	retained *retainedStore

	// writeMu 串行化整帧写出。
	writeMu sync.Mutex

	// 握手后填充，此后只读。
	clientID     string
	cleanSession bool
	keepAliveSec uint16
	will         *willMessage
	user         string
	vhost        string
	sess         plugin.Session

	mu           sync.Mutex
	queues       map[byte]*qosQueue           // qos → 订阅单元
	byTag        map[string]*qosQueue         // consumer tag → 订阅单元（投递时定 QoS）
	filterQoS    map[string]map[byte]struct{} // filter → 已绑定的 QoS 集合
	inflight     map[uint16]*plugin.Delivery
	nextPacket   uint16
	inboundQoS2  map[uint16]struct{}
	closed       bool
	disconnected bool

	// 观测计数（管理面快照用）。
	sentPackets uint64
	recvPackets uint64
}

func newConn(log *slog.Logger, nc net.Conn, core plugin.Core, opts options, retained *retainedStore) *conn {
	return &conn{
		log:         log,
		nc:          nc,
		core:        core,
		br:          bufio.NewReaderSize(nc, 4096),
		opts:        opts,
		retained:    retained,
		queues:      map[byte]*qosQueue{},
		byTag:       map[string]*qosQueue{},
		filterQoS:   map[string]map[byte]struct{}{},
		inflight:    map[uint16]*plugin.Delivery{},
		inboundQoS2: map[uint16]struct{}{},
	}
}

// run 是连接的生命周期：握手 → 读循环 → 收尾（遗嘱、取消订阅、清连接级资源）。
func (c *conn) run(ctx context.Context) error {
	err := c.serve(ctx)
	// 无论因为什么结束（含客户端主动 DISCONNECT），都必须走一遍收尾。
	c.shutdown()
	if errors.Is(err, errClientDisconnect) {
		return nil
	}
	return err
}

func (c *conn) serve(ctx context.Context) error {
	if err := c.handshake(ctx); err != nil {
		return err
	}
	for {
		if c.keepAliveSec > 0 {
			// 规范：服务端在 1.5 倍 Keep Alive 内没收到任何报文即可判定连接失效。
			deadline := time.Now().Add(time.Duration(c.keepAliveSec) * time.Second * 3 / 2)
			if err := c.nc.SetReadDeadline(deadline); err != nil {
				return err
			}
		}
		typ, flags, remaining, err := readFixedHeader(c.br)
		if err != nil {
			return err
		}
		body, err := readBody(c.br, remaining, c.opts.MaxPacketSize)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.recvPackets++
		c.mu.Unlock()
		if err := c.handlePacket(typ, flags, body); err != nil {
			return err
		}
	}
}

// ---------------------------------------------------------------------------
// 握手
// ---------------------------------------------------------------------------

func (c *conn) handshake(ctx context.Context) error {
	typ, flags, remaining, err := readFixedHeader(c.br)
	if err != nil {
		return err
	}
	if typ != pktConnect {
		return fmt.Errorf("%w: 首个报文必须是 CONNECT，实际类型 %d", errMalformed, typ)
	}
	if flags != 0 {
		return fmt.Errorf("%w: CONNECT 标志位应为 0，实际 %d", errMalformed, flags)
	}
	body, err := readBody(c.br, remaining, c.opts.MaxPacketSize)
	if err != nil {
		return err
	}
	p, err := parseConnect(body)
	if err != nil {
		return err
	}
	if p.ProtocolName != protocolName || p.ProtocolLevel != protocolLevel {
		_ = c.write(encodeConnack(false, connBadProtocol))
		return fmt.Errorf("mqtt: 不支持的协议版本 %q/%d（本插件只实现 MQTT 3.1.1）",
			p.ProtocolName, p.ProtocolLevel)
	}
	if err := validClientID(p.ClientID, p.CleanSession); err != nil {
		_ = c.write(encodeConnack(false, connIDRejected))
		return err
	}
	if p.Username == "" {
		_ = c.write(encodeConnack(false, connBadCredentials))
		return errors.New("mqtt: CONNECT 未携带用户名（本插件要求认证）")
	}

	// MQTT 的用户名/口令映射到 SASL PLAIN：内核的认证与权限表因此对两个协议是同一份。
	plain := make([]byte, 0, len(p.Username)+len(p.Password)+2)
	plain = append(plain, 0)
	plain = append(plain, p.Username...)
	plain = append(plain, 0)
	plain = append(plain, p.Password...)
	ident, err := c.core.Authenticate(ctx, "PLAIN", plain, c.nc.RemoteAddr())
	if err != nil {
		_ = c.write(encodeConnack(false, connackCodeForAuth(err)))
		return fmt.Errorf("mqtt: 认证失败: %w", err)
	}

	vhost := c.core.DefaultVHost()
	if !c.core.VHostExists(vhost) {
		// MQTT 没有 vhost 概念：一律用内核的默认 vhost（见文档）。
		_ = c.write(encodeConnack(false, connServerUnavailable))
		return fmt.Errorf("mqtt: 默认 vhost %q 不存在", vhost)
	}
	sess, err := c.core.Session(vhost)
	if err != nil {
		_ = c.write(encodeConnack(false, connNotAuthorized))
		return fmt.Errorf("mqtt: 打开 vhost %q 失败: %w", vhost, err)
	}

	c.clientID = p.ClientID
	if c.clientID == "" {
		// 规范允许 Clean Session=1 时用空 Client ID，由服务端分配一个。
		c.clientID = fmt.Sprintf("swiftmq-auto-%d-%d", time.Now().UnixNano(), autoClientSeq.Add(1))
	}
	c.cleanSession = p.CleanSession
	c.keepAliveSec = p.KeepAlive
	c.will = p.Will
	c.user = ident.User
	c.vhost = vhost
	c.sess = sess

	// Session Present：只在"持久会话 + 该客户端的订阅队列已存在"时为真。
	sessionPresent := false
	if !p.CleanSession {
		sessionPresent = c.detectExistingSession()
	}

	if err := c.write(encodeConnack(sessionPresent, connAccepted)); err != nil {
		return err
	}
	c.log.Info("MQTT 客户端已连接",
		"client_id", c.clientID, "user", ident.User, "clean_session", p.CleanSession,
		"keepalive", p.KeepAlive, "session_present", sessionPresent)
	// 让管理面能看见这条连接（协议无关：管理面只读取快照，不认识 MQTT）。
	c.core.SetConnectionProbe(c.snapshot)
	c.core.SetDisconnectFunc(func(reason string) {
		// MQTT 3.1.1 没有服务端主动断开的下行报文，只能关闭连接。
		c.log.Info("管理面强制关闭 MQTT 连接", "client_id", c.clientID, "reason", reason)
		_ = c.nc.Close()
	})
	return nil
}

// detectExistingSession 判断该客户端是否已有持久会话（决定 CONNACK 的 Session Present）。
//
// 判定方式就是"按队列是否存在"：持久会话的订阅落在 durable 队列上，队列在即会话在。
// 被动声明也必须带上"当初声明时用的标志"：内核会做等价性校验（与 RabbitMQ 一致），
// 只写名字会被判成"参数不一致"，于是这里永远探不到已有会话。
func (c *conn) detectExistingSession() bool {
	for _, qos := range []byte{0, 1} {
		if _, err := c.sess.DeclareQueue(plugin.QueueDeclare{
			Name:    subscriptionQueue(c.clientID, qos),
			Passive: true,
			Durable: true, // Clean Session=0 时我们就是用这套标志声明的
		}); err == nil {
			return true
		}
	}
	return false
}

// connackCodeForAuth 把内核的认证失败分类映射成 CONNACK 返回码。
func connackCodeForAuth(err error) byte {
	var ae *plugin.AuthError
	if errors.As(err, &ae) && ae.Kind == plugin.AuthFailureMechanismUnsupported {
		return connNotAuthorized
	}
	return connBadCredentials
}

// ---------------------------------------------------------------------------
// 报文分发
// ---------------------------------------------------------------------------

func (c *conn) handlePacket(typ, flags byte, body []byte) error {
	switch typ {
	case pktPublish:
		return c.handlePublish(flags, body)
	case pktPuback:
		return c.handlePuback(body)
	case pktPubrel:
		return c.handlePubrel(flags, body)
	case pktSubscribe:
		return c.handleSubscribe(flags, body)
	case pktUnsubscribe:
		return c.handleUnsubscribe(flags, body)
	case pktPingreq:
		if err := expectEmptyBody(typ, body); err != nil {
			return err
		}
		return c.write(encodePingresp())
	case pktDisconnect:
		if err := expectEmptyBody(typ, body); err != nil {
			return err
		}
		c.mu.Lock()
		c.disconnected = true
		c.mu.Unlock()
		return errClientDisconnect
	case pktConnect:
		// 规范：一个网络连接只允许一次 CONNECT。
		return fmt.Errorf("%w: 重复的 CONNECT", errMalformed)
	default:
		// PUBREC / PUBCOMP / CONNACK / SUBACK 等是服务端发出的报文，客户端不应发送。
		return fmt.Errorf("%w: 客户端不应发送报文类型 %d", errMalformed, typ)
	}
}

// ---------------------------------------------------------------------------
// 发布（入站）
// ---------------------------------------------------------------------------

func (c *conn) handlePublish(flags byte, body []byte) error {
	p, err := parsePublish(flags, body)
	if err != nil {
		return err
	}
	if err := validTopic(p.Topic, false); err != nil {
		return err
	}
	if err := c.publish(p.Topic, p.Payload, p.QoS, p.Retain); err != nil {
		return err
	}
	switch p.QoS {
	case 0:
		return nil
	case 1:
		return c.write(encodeAck(pktPuback, p.PacketID))
	default:
		// QoS2：完成四步握手，但语义按至少一次（不去重，见 maxQoS 的说明）。
		c.mu.Lock()
		c.inboundQoS2[p.PacketID] = struct{}{}
		c.mu.Unlock()
		return c.write(encodeAck(pktPubrec, p.PacketID))
	}
}

// handlePubrel 处理 QoS2 握手的第三步。
func (c *conn) handlePubrel(flags byte, body []byte) error {
	if flags != 0x02 {
		return fmt.Errorf("%w: PUBREL 标志位应为 2，实际 %d", errMalformed, flags)
	}
	id, err := parsePacketID(body)
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.inboundQoS2, id)
	c.mu.Unlock()
	return c.write(encodeAck(pktPubcomp, id))
}

// publish 把一条 MQTT 消息投给内核。
//
// QoS≥1 映射为持久消息（delivery-mode=2），并在**落盘完成之后**才回 PUBACK/PUBREC ——
// 这样 MQTT 的"至少一次"承诺与 M4 的 confirm 语义对齐：
// 客户端收到 PUBACK 就意味着消息已按 fsync 档位落盘，而不是"进了内存"。
func (c *conn) publish(topic string, payload []byte, qos byte, retain bool) error {
	msg := &plugin.Message{
		Exchange:   c.opts.Exchange,
		RoutingKey: routingKey(topic),
		Properties: plugin.Properties{
			DeliveryMode: 1,
			Headers:      map[string]any{topicHeader: topic},
		},
		Body: payload,
	}
	if qos > 0 {
		msg.Properties.DeliveryMode = 2
	}
	res, err := c.sess.Publish(msg, c.opts.Exchange, msg.RoutingKey, false)
	if err != nil {
		return err
	}
	if res.Durable != nil {
		if err := res.Durable(); err != nil {
			return err
		}
	}
	if retain {
		// retain=1 且载荷为空 = 清除该主题的保留消息（规范规定），set 内部已处理。
		c.retained.set(c.vhost, topic, payload, qos)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 订阅
// ---------------------------------------------------------------------------

func (c *conn) handleSubscribe(flags byte, body []byte) error {
	p, err := parseSubscribe(flags, body)
	if err != nil {
		return err
	}
	type accepted struct {
		filter  string
		granted byte
	}
	codes := make([]byte, 0, len(p.Topics))
	ok := make([]accepted, 0, len(p.Topics))
	// 阶段一：只建队列与绑定，**不挂消费者**。
	//
	// 顺序很关键：若先挂消费者再回 SUBACK，队列里的就绪消息（持久会话重连时必然有）
	// 会立刻经投递路径写出 PUBLISH，客户端就会先收到数据、后收到 SUBACK ——
	// 协议没禁止，但会打乱客户端的"订阅已生效"判断。绑定只是拓扑，不产生投递。
	for _, t := range p.Topics {
		granted := t.QoS
		if granted > maxQoS {
			granted = maxQoS
		}
		// 单个过滤器非法：按规范回失败码，不影响同报文里的其他订阅。
		if err := validTopic(t.Filter, true); err != nil {
			codes = append(codes, subackFailure)
			continue
		}
		if err := c.bindFilter(t.Filter, granted); err != nil {
			c.log.Warn("订阅失败", "client_id", c.clientID, "filter", t.Filter, "err", err)
			codes = append(codes, subackFailure)
			continue
		}
		codes = append(codes, granted)
		ok = append(ok, accepted{filter: t.Filter, granted: granted})
	}
	if err := c.write(encodeSuback(p.PacketID, codes)); err != nil {
		return err
	}
	// 阶段二：SUBACK 之后再开始投递。
	for _, a := range ok {
		if err := c.startConsuming(a.granted); err != nil {
			return err
		}
	}
	// 阶段三：保留消息最后投（它同样不该抢在 SUBACK 前面）。
	for _, a := range ok {
		if err := c.deliverRetained(a.filter, a.granted); err != nil {
			return err
		}
	}
	return nil
}

// bindFilter 建立（或复用）该 QoS 的订阅队列，并把过滤器绑上去。
func (c *conn) bindFilter(filter string, qos byte) error {
	qq, err := c.ensureQueue(qos)
	if err != nil {
		return err
	}
	c.mu.Lock()
	_, bound := qq.filters[filter]
	c.mu.Unlock()
	if bound {
		return nil
	}
	if err := c.sess.BindQueue(qq.name, c.opts.Exchange, routingKey(filter), nil); err != nil {
		return err
	}
	c.mu.Lock()
	qq.filters[filter] = struct{}{}
	set := c.filterQoS[filter]
	if set == nil {
		set = map[byte]struct{}{}
		c.filterQoS[filter] = set
	}
	set[qos] = struct{}{}
	c.mu.Unlock()
	return nil
}

// ensureQueue 声明该 QoS 的订阅队列并登记（幂等；此阶段不挂消费者）。
//
// Clean Session 决定队列的持久性，这是 MQTT"会话"与内核"队列"之间最自然的一处映射：
//   - Clean Session=1 → 非持久 + autoDelete：最后一个消费者离开即删除，
//     语义上等价于"会话随连接结束而丢弃"；
//   - Clean Session=0 → durable：队列留在磁盘上，重连后继续投递积压消息（持久会话）。
func (c *conn) ensureQueue(qos byte) (*qosQueue, error) {
	c.mu.Lock()
	if qq, ok := c.queues[qos]; ok {
		c.mu.Unlock()
		return qq, nil
	}
	c.mu.Unlock()

	name := subscriptionQueue(c.clientID, qos)
	if _, err := c.sess.DeclareQueue(plugin.QueueDeclare{
		Name:       name,
		Durable:    !c.cleanSession,
		AutoDelete: c.cleanSession,
		// Clean Session=1 的队列必须独占：RabbitMQ 4.x 禁止"瞬时（non-durable）非独占队列"（541），
		// 而独占恰好是它的真实语义 —— 队列只服务这一个 MQTT 连接，随连接关闭一起回收。
		Exclusive: c.cleanSession,
	}); err != nil {
		return nil, fmt.Errorf("声明订阅队列 %s 失败: %w", name, err)
	}
	qq := &qosQueue{qos: qos, name: name, filters: map[string]struct{}{}}
	c.mu.Lock()
	if existing, dup := c.queues[qos]; dup {
		c.mu.Unlock()
		return existing, nil
	}
	c.queues[qos] = qq
	c.mu.Unlock()
	return qq, nil
}

// consumerSeq 给消费者标签加序号，保证跨连接唯一（同一 Client ID 重连也不会撞标签）。
var consumerSeq atomic.Uint64

// startConsuming 为该 QoS 的订阅队列挂上消费者（幂等）。
//
// **先登记、再注册**：队列里可能已经有就绪消息，内核会在 Consume 内部就发起投递 ——
// 那时若 byTag 还没有这条记录，投递会被当成"未知消费者"退回队列（消息不丢，
// 但要等下一次发布才会重投）。这是 M6b 跨节点转发层踩过的同一类竞态，处理方式一致。
func (c *conn) startConsuming(qos byte) error {
	c.mu.Lock()
	qq, known := c.queues[qos]
	if !known || qq.tag != "" {
		c.mu.Unlock()
		return nil
	}
	qq.tag = newConsumerTag(c.clientID, qos)
	c.byTag[qq.tag] = qq
	c.mu.Unlock()

	if _, err := c.sess.Consume(plugin.Subscription{
		Tag:      qq.tag,
		Queue:    qq.name,
		NoAck:    qos == 0,
		Prefetch: c.prefetchFor(qos),
		Deliver:  c.deliver,
		Cancel:   c.onConsumerCancelled,
	}); err != nil {
		c.mu.Lock()
		delete(c.byTag, qq.tag)
		qq.tag = ""
		c.mu.Unlock()
		return fmt.Errorf("在队列 %s 上注册消费者失败: %w", qq.name, err)
	}
	return nil
}

// newConsumerTag 生成消费者标签。
//
// 标签由协议侧自己给（而不是让内核生成），是为了能"先登记再注册"：
// 内核生成标签意味着调用返回前我们不知道它是什么，也就无法提前登记。
func newConsumerTag(clientID string, qos byte) string {
	return fmt.Sprintf("mqtt-q%d-%s-%d", qos, clientID, consumerSeq.Add(1))
}

// prefetchFor 返回该 QoS 的 in-flight 上限：QoS0 无确认、不需要窗口。
func (c *conn) prefetchFor(qos byte) uint16 {
	if qos == 0 {
		return 0
	}
	return uint16(c.opts.Prefetch)
}

func (c *conn) handleUnsubscribe(flags byte, body []byte) error {
	p, err := parseUnsubscribe(flags, body)
	if err != nil {
		return err
	}
	for _, filter := range p.Filters {
		c.unsubscribe(filter)
	}
	return c.write(encodeAck(pktUnsuback, p.PacketID))
}

// unsubscribe 解除一个过滤器的全部绑定；订阅队列空了就连同消费者一起回收。
func (c *conn) unsubscribe(filter string) {
	c.mu.Lock()
	qoses := c.filterQoS[filter]
	delete(c.filterQoS, filter)
	var empty []*qosQueue
	for qos := range qoses {
		qq, ok := c.queues[qos]
		if !ok {
			continue
		}
		delete(qq.filters, filter)
		if len(qq.filters) == 0 {
			empty = append(empty, qq)
		}
	}
	c.mu.Unlock()

	for _, qq := range empty {
		_ = c.sess.UnbindQueue(qq.name, c.opts.Exchange, routingKey(filter), nil)
		// 队列里已无任何绑定：留着也收不到消息，直接回收（QoS1 的未确认消息会随之重入队）。
		c.mu.Lock()
		delete(c.queues, qq.qos)
		tag := qq.tag
		if tag != "" {
			delete(c.byTag, tag)
			qq.tag = ""
		}
		c.mu.Unlock()
		if tag == "" {
			continue // 还没挂消费者（订阅建立失败在 SUBACK 之前退订）
		}
		if err := c.sess.Cancel(tag); err != nil {
			c.log.Debug("取消消费者失败", "queue", qq.name, "err", err)
		}
		if _, err := c.sess.DeleteQueue(qq.name, false, false); err != nil {
			c.log.Debug("回收订阅队列失败", "queue", qq.name, "err", err)
		}
	}
}

// deliverRetained 把命中过滤器的保留消息投给刚建立订阅的客户端。
func (c *conn) deliverRetained(filter string, grantedQoS byte) error {
	for _, m := range c.retained.lookup(c.vhost, filter) {
		qos := m.QoS
		if qos > grantedQoS {
			qos = grantedQoS
		}
		if err := c.sendPublish(m.Topic, m.Payload, qos, true, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 投递（出站）
// ---------------------------------------------------------------------------

// deliver 由内核的队列投递协程调用。
//
// 返回错误表示"这条没送出去"，内核据此把消息重新入队（至少一次）。
func (c *conn) deliver(d *plugin.Delivery) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("mqtt: 连接已关闭")
	}
	qq := c.byTag[d.ConsumerTag]
	c.mu.Unlock()
	if qq == nil {
		return fmt.Errorf("mqtt: 未知的消费者 %q", d.ConsumerTag)
	}
	topic := topicOf(d.Message)
	if topic == "" {
		// 无法确定主题：MQTT 的 PUBLISH 必须带主题，投出去就是协议违规报文。
		// 这不是"暂时投不出去"，而是"这条消息不属于任何 MQTT 主题"，因此按**拒绝**结算
		// （配了死信的队列会把它转入死信），而不是返回错误 —— 后者会让内核不断重投，
		// 变成"投不出去又退不掉"的活锁。
		c.log.Warn("投递缺少路由键，无法确定 MQTT 主题，已按拒绝处理", "queue", d.Queue)
		if d.Settle != nil {
			d.Settle(plugin.SettleReject)
		}
		return nil
	}

	if qq.qos == 0 {
		// NoAck 订阅由内核结算，这里不需要 Settle。
		return c.sendPublish(topic, d.Message.Body, 0, false, d.Redelivered, nil)
	}

	id, err := c.allocPacketID()
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.inflight[id] = d
	c.mu.Unlock()
	if err := c.sendPublish(topic, d.Message.Body, 1, false, d.Redelivered, &id); err != nil {
		c.mu.Lock()
		delete(c.inflight, id)
		c.mu.Unlock()
		return err
	}
	return nil
}

// topicOf 还原一条内核消息对应的 MQTT 主题。
//
// 优先用发布时带上的原始主题头（MQTT→MQTT 无损）；没有该头说明消息来自别的协议
// （例如 AMQP 客户端直接发到 amq.topic），此时按路由键反推主题，这就是跨协议互通。
func topicOf(msg *plugin.Message) string {
	if msg.Properties.Headers != nil {
		if v, ok := msg.Properties.Headers[topicHeader].(string); ok && v != "" {
			return v
		}
	}
	return strings.ReplaceAll(msg.RoutingKey, ".", "/")
}

// handlePuback 处理客户端对出站 QoS1 的确认。
func (c *conn) handlePuback(body []byte) error {
	id, err := parsePacketID(body)
	if err != nil {
		return err
	}
	c.mu.Lock()
	d := c.inflight[id]
	delete(c.inflight, id)
	c.mu.Unlock()
	if d == nil {
		// 重复的 PUBACK 或未知 ID：容忍（重发是允许的），但绝不结算两次。
		c.log.Debug("收到未知 Packet ID 的 PUBACK", "packet_id", id)
		return nil
	}
	if d.Settle != nil {
		d.Settle(plugin.SettleAck)
	}
	return nil
}

// onConsumerCancelled 是内核主动取消消费者（如队列被删除/管理员操作）时的回调。
func (c *conn) onConsumerCancelled(reason string) {
	c.log.Warn("服务端取消了 MQTT 订阅", "client_id", c.clientID, "reason", reason)
}

// sendPublish 编码并写出一条 PUBLISH。
//
// packetID 为 nil 表示 QoS0（报文里不带 Packet ID）。
func (c *conn) sendPublish(topic string, payload []byte, qos byte, retain, dup bool, packetID *uint16) error {
	var id uint16
	if packetID != nil {
		id = *packetID
	}
	return c.write(encodePublish(dup, qos, retain, topic, id, payload))
}

// allocPacketID 分配一个未被占用的出站 Packet ID（1..65535）。
func (c *conn) allocPacketID() (uint16, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < 65535; i++ {
		c.nextPacket++
		if c.nextPacket == 0 {
			c.nextPacket = 1
		}
		if _, used := c.inflight[c.nextPacket]; !used {
			return c.nextPacket, nil
		}
	}
	return 0, errors.New("mqtt: 出站 Packet ID 已耗尽（在途 QoS1 消息过多）")
}

// ---------------------------------------------------------------------------
// 写出与收尾
// ---------------------------------------------------------------------------

// write 原子地写出一整帧（多协程并发调用是安全的）。
func (c *conn) write(f []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.nc.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if _, err := c.nc.Write(f); err != nil {
		return err
	}
	c.mu.Lock()
	c.sentPackets++
	c.mu.Unlock()
	return nil
}

// shutdown 收尾：发布遗嘱（仅非正常断开）→ 关闭会话 → 释放连接级资源。
func (c *conn) shutdown() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	will := c.will
	disconnected := c.disconnected
	sess := c.sess
	clientID := c.clientID
	c.mu.Unlock()

	// 遗嘱：客户端"异常断开"（没发 DISCONNECT）时才发布，符合 MQTT 3.1.1 §3.1.2.5。
	if will != nil && !disconnected && sess != nil {
		if err := c.publish(will.Topic, will.Payload, will.QoS, will.Retain); err != nil {
			c.log.Warn("发布遗嘱消息失败", "client_id", clientID, "topic", will.Topic, "err", err)
		}
	}
	if sess != nil {
		// 取消消费者、把未确认消息交还队列；Clean Session 的队列由 autoDelete 回收。
		sess.Close()
	}
	c.core.Close()
	if clientID != "" {
		c.log.Info("MQTT 连接已结束", "client_id", clientID,
			"recv_packets", c.recvPackets, "sent_packets", c.sentPackets)
	}
}

// snapshot 向管理面提供连接快照（与 AMQP 连接共用同一套展示）。
//
// MQTT 没有"通道"概念，这里把**每个订阅队列**映射成一条虚拟通道，
// 目的是让运维在统一的管理界面里能看到 MQTT 订阅者与未确认数，
// 而不是看到一条"零通道"的连接。这是有意的展示层适配，不影响协议语义。
func (c *conn) snapshot() plugin.ConnectionInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	channels := make([]plugin.ChannelInfo, 0, len(c.queues))
	for qos := range c.queues {
		unacked := 0
		if qos > 0 {
			unacked = len(c.inflight)
		}
		channels = append(channels, plugin.ChannelInfo{
			Number:        uint16(qos) + 1,
			ConsumerCount: 1,
			PrefetchCount: c.prefetchFor(qos),
			Unacked:       unacked,
		})
	}
	return plugin.ConnectionInfo{
		Protocol:         "MQTT 3.1.1",
		AuthMechanism:    "PLAIN",
		HeartbeatSeconds: c.keepAliveSec,
		Channels:         channels,
	}
}
