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

	"github.com/houzch/swiftmq/internal/auth"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// Version 是内核版本。
const Version = "0.2.0"

// Broker 是内核单例。
type Broker struct {
	log    *slog.Logger
	cfg    *config.Config
	auth   *auth.Store
	vhosts map[string]*vhost
	// sessions 用于生成会话标识（独占队列归属判定用）
	sessions atomic.Uint64
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
	}
	for _, name := range names {
		if _, dup := b.vhosts[name]; dup {
			continue
		}
		b.vhosts[name] = newVHost(name, log)
	}
	return b
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

			// ---- 尚未实现，一律不声明（客户端会自行降级）----
			//   publisher_confirms      → M3
			//   consumer_priorities     → M3
			//   direct_reply_to         → M3
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
		s.vh = newVHostSession(vh, s.id, s.log)
	}
	return s.vh, nil
}
