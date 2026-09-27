// Package broker 是内核：持有 vhost、用户，并向协议插件暴露协议无关的操作面 plugin.Core。
//
// M2 起在这里接入 exchange / queue / binding 与路由（设计文档第 5 章）。
package broker

import (
	"context"
	"log/slog"
	"net"

	"github.com/swiftmq/swiftmq/internal/auth"
	"github.com/swiftmq/swiftmq/internal/config"
	"github.com/swiftmq/swiftmq/pkg/plugin"
)

// Version 是内核版本。
const Version = "0.1.0"

// Broker 是内核单例。
type Broker struct {
	log    *slog.Logger
	cfg    *config.Config
	auth   *auth.Store
	vhosts map[string]struct{}
}

// New 构造内核。
func New(log *slog.Logger, cfg *config.Config) *Broker {
	vhosts := make(map[string]struct{}, len(cfg.VHosts)+1)
	for _, v := range cfg.VHosts {
		vhosts[v] = struct{}{}
	}
	// 默认 vhost 一定存在，避免配置里漏写导致所有客户端连不上
	vhosts[cfg.DefaultVHost] = struct{}{}

	return &Broker{
		log:    log,
		cfg:    cfg,
		auth:   auth.NewStore(cfg.Users),
		vhosts: vhosts,
	}
}

// NewSession 为一条新连接创建协议无关的操作面。
func (b *Broker) NewSession(remote net.Addr) plugin.Core {
	return &session{broker: b, log: b.log.With("remote", remote.String())}
}

// session 是 plugin.Core 的实现，绑定单条连接。
type session struct {
	broker *Broker
	log    *slog.Logger
}

var _ plugin.Core = (*session)(nil)

// Logger 返回连接级日志器。
func (s *session) Logger() *slog.Logger { return s.log }

// ServerProperties 返回 Connection.Start 下发的 server-properties。
//
// capabilities 声明即承诺（设计文档 4.2）：客户端会依据它切换代码路径，
// 因此只有真正实现的能力才允许置 true —— 声明了却没实现，比不声明更糟。
// 每完成一个里程碑，在这里打开对应 capability。
func (s *session) ServerProperties() map[string]any {
	return map[string]any{
		"product":     "SwiftMQ",
		"version":     Version,
		"platform":    "Go",
		"information": "https://github.com/swiftmq/swiftmq",
		"capabilities": map[string]any{
			// M1：认证失败时用 Connection.Close 明确告知原因，而不是直接断开连接
			"authentication_failure_close": true,

			// 以下能力尚未实现，一律不声明（客户端会自行降级或关闭该特性）：
			//   exchange_exchange_bindings  → M2
			//   publisher_confirms          → M3
			//   basic.nack                  → M3
			//   consumer_cancel_notify      → M3
			//   per_consumer_qos            → M3
			//   consumer_priorities         → M3
			//   direct_reply_to             → M3
			//   connection.blocked          → M4
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
