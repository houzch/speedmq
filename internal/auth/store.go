// Package auth 提供 v1 的内置用户表与 SASL 机制实现（PLAIN / AMQPLAIN）。
//
// v1 只做 PLAIN / AMQPLAIN / EXTERNAL；EXTERNAL 依赖 TLS 客户端证书，
// 随 TLS 支持一起补齐。LDAP / OAuth2 走认证授权插件，不进入内核。
package auth

import (
	"fmt"
	"net"
	"strings"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// Store 是内置用户表。
type Store struct {
	users map[string]config.User
}

// NewStore 构造用户表。
func NewStore(users map[string]config.User) *Store { return &Store{users: users} }

// Mechanisms 返回支持的 SASL 机制名（顺序即 Connection.Start 中下发的顺序）。
func (s *Store) Mechanisms() []string { return []string{"PLAIN", "AMQPLAIN"} }

// Permissions 返回用户在指定 vhost 上的权限；第二个返回值为 false 表示无权访问该 vhost。
func (s *Store) Permissions(user, vhost string) (config.Permission, bool) {
	rec, ok := s.users[user]
	if !ok {
		return config.Permission{}, false
	}
	p, ok := rec.Permissions[vhost]
	return p, ok
}

// Authenticate 校验 SASL 响应并返回用户名。
//
// 失败时返回 *plugin.AuthError，由协议插件映射为对应的协议错误码（不在此处耦合协议细节）。
func (s *Store) Authenticate(mechanism string, response []byte, remote net.Addr) (string, error) {
	var user, pass string
	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		// 格式：[authzid] \0 authcid \0 passwd
		parts := strings.Split(string(response), "\x00")
		switch len(parts) {
		case 3:
			user, pass = parts[1], parts[2]
		case 2:
			// 少数客户端省略 authzid 分隔符；容忍之，避免无谓的不兼容。
			user, pass = parts[0], parts[1]
		default:
			return "", &plugin.AuthError{
				Kind: plugin.AuthFailureAccessRefused,
				Text: "ACCESS_REFUSED - PLAIN 响应格式非法",
			}
		}
	case "AMQPLAIN":
		// 响应就是一个标准 field-table（含 4 字节长度前缀），取值 LOGIN / PASSWORD。
		t, err := codec.NewDecoder(response).Table()
		if err != nil {
			return "", &plugin.AuthError{
				Kind: plugin.AuthFailureAccessRefused,
				Text: fmt.Sprintf("ACCESS_REFUSED - AMQPLAIN 响应解析失败: %v", err),
			}
		}
		user = stringField(t, "LOGIN")
		pass = stringField(t, "PASSWORD")
	default:
		return "", &plugin.AuthError{
			Kind: plugin.AuthFailureMechanismUnsupported,
			Text: fmt.Sprintf("NOT_ALLOWED - 不支持的认证机制 %s", mechanism),
		}
	}

	rec, ok := s.users[user]
	// 用户不存在与口令错误返回同一个错误，避免泄露用户是否存在。
	if !ok || rec.Password != pass {
		return "", &plugin.AuthError{
			Kind: plugin.AuthFailureAccessRefused,
			Text: fmt.Sprintf("ACCESS_REFUSED - Login was refused using authentication mechanism %s. "+
				"For details see the broker logfile.", mechanism),
		}
	}
	if !rec.RemoteAccess && !isLoopback(remote) {
		// 对齐 RabbitMQ：内置 guest 默认只允许从本机登录。
		return "", &plugin.AuthError{
			Kind: plugin.AuthFailureAccessRefused,
			Text: fmt.Sprintf("ACCESS_REFUSED - user %q can only connect via localhost", user),
		}
	}
	return user, nil
}

func stringField(t codec.Table, key string) string {
	if v, ok := t[key].(string); ok {
		return v
	}
	return ""
}

func isLoopback(addr net.Addr) bool {
	if addr == nil {
		return false
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
