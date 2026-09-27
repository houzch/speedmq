// Package broker 是内核：持有 vhost、拓扑与队列，并向协议插件暴露协议无关的操作面。
//
// 分层：Broker → vhost（拓扑）→ exchange / queue（路由与消息）。
// 协议插件通过 plugin.Core.Session(vhost) 拿到某个 vhost 的操作面。
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/houzch/swiftmq/internal/auth"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// Version 是内核版本。
const Version = "0.3.0"

const (
	// deadLetterBuffer 是死信派发队列的缓冲长度。
	deadLetterBuffer = 4096
	// backgroundInterval 是 TTL / 队列过期的扫描周期。
	backgroundInterval = 100 * time.Millisecond
)

// Broker 是内核单例。
type Broker struct {
	log    *slog.Logger
	cfg    *config.Config
	auth   *auth.Store
	vhosts map[string]*vhost
	// sessions 用于生成会话标识（独占队列归属判定用）
	sessions atomic.Uint64

	// dlxCh 是死信派发入口：队列只做非阻塞入队，由单个后台协程实际路由。
	dlxCh  chan deadLetterEntry
	cancel context.CancelFunc
	done   chan struct{}
}

// New 构造内核。
func New(log *slog.Logger, cfg *config.Config) *Broker {
	log = log.With("component", "broker")

	names := make([]string, 0, len(cfg.VHosts)+1)
	names = append(names, cfg.VHosts...)
	// 默认 vhost 一定存在，避免配置里漏写导致所有客户端连不上
	names = append(names, cfg.DefaultVHost)

	b := &Broker{
		log:    log,
		cfg:    cfg,
		auth:   auth.NewStore(cfg.Users),
		vhosts: map[string]*vhost{},
		dlxCh:  make(chan deadLetterEntry, deadLetterBuffer),
		done:   make(chan struct{}),
	}
	for _, name := range names {
		if _, dup := b.vhosts[name]; dup {
			continue
		}
		b.vhosts[name] = newVHost(name, log, b.dlxCh)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.background(ctx)
	return b
}

// Close 停止后台协程（TTL 扫描与死信派发）。可重复调用。
func (b *Broker) Close() {
	if b.cancel == nil {
		return
	}
	b.cancel()
	b.cancel = nil
	<-b.done
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

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-b.dlxCh:
			b.dispatchDeadLetter(e)
		case <-ticker.C:
			now := time.Now()
			for _, v := range b.vhosts {
				v.sweep(now)
			}
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
func (b *Broker) NewSession(remote net.Addr) plugin.Core {
	id := fmt.Sprintf("conn-%d", b.sessions.Add(1))
	return &session{
		broker: b,
		id:     id,
		log:    b.log.With("remote", remote.String(), "session", id),
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
}

var _ plugin.Core = (*session)(nil)

// Logger 返回连接级日志器。
func (s *session) Logger() *slog.Logger { return s.log }

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

			// ---- 尚未实现，一律不声明（客户端会自行降级）----
			//   consumer_priorities     → 消费者优先级（x-priority）尚未实现
			//   direct_reply_to         → amq.rabbitmq.reply-to 伪队列尚未实现
			//   connection.blocked      → M4
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
		s.vh = newVHostSession(vh, s.id, s.user, perm, s.log)
	}
	return s.vh, nil
}

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
