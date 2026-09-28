// Package auth 提供 v1 的内置用户表与 SASL 机制实现（PLAIN / AMQPLAIN）。
//
// v1 只做 PLAIN / AMQPLAIN / EXTERNAL；EXTERNAL 依赖 TLS 客户端证书，
// 随 TLS 支持一起补齐。LDAP / OAuth2 走认证授权插件，不进入内核。
package auth

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// Store 是内置用户表。
//
// M5 起支持运行期增删改（管理 API / swiftmqctl），因此所有访问都要过锁：
// len(M5) 前后只有一个从配置加载的只读表，那时加锁是多余的；现在不是了。
type Store struct {
	mu    sync.RWMutex
	users map[string]config.User
}

// NewStore 构造用户表。
func NewStore(users map[string]config.User) *Store {
	cp := make(map[string]config.User, len(users))
	for k, v := range users {
		cp[k] = v
	}
	return &Store{users: cp}
}

// Mechanisms 返回支持的 SASL 机制名（顺序即 Connection.Start 中下发的顺序）。
func (s *Store) Mechanisms() []string { return []string{"PLAIN", "AMQPLAIN"} }

// Permissions 返回用户在指定 vhost 上的权限；第二个返回值为 false 表示无权访问该 vhost。
func (s *Store) Permissions(user, vhost string) (config.Permission, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.users[user]
	if !ok {
		return config.Permission{}, false
	}
	p, ok := rec.Permissions[vhost]
	return p, ok
}

// User 返回用户记录快照。
func (s *Store) User(name string) (config.User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[name]
	return u, ok
}

// UserNames 返回全部用户名（已排序，保证管理 API 输出稳定）。
func (s *Store) UserNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.users))
	for name := range s.users {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// UpsertUser 新建或更新用户。
//
// 口令为明文：与配置文件的表示保持一致（v1 的内置用户表就是"节点管理员"，
// 哈希存储与外部认证后端留给认证插件，见设计 10.2）。
// 通过管理 API 创建的用户一律允许远端登录 —— 它是运维显式创建的账号，
// 默认只允许本机会让它形同虚设（配置文件里的 guest 仍按配置的 remote_access 生效）。
func (s *Store) UpsertUser(name, password string, tags []string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("用户名不能为空")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.users[name]
	rec.Password = password
	rec.Tags = tags
	rec.RemoteAccess = true
	if rec.Permissions == nil {
		rec.Permissions = map[string]config.Permission{}
	}
	s.users[name] = rec
	return nil
}

// DeleteUser 删除用户（连同其权限记录）。不存在时返回 false。
func (s *Store) DeleteUser(name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; !ok {
		return false, nil
	}
	delete(s.users, name)
	return true, nil
}

// UserPermission 是"某用户在某 vhost 上的权限"的扁平快照。
type UserPermission struct {
	User      string
	VHost     string
	Configure string
	Write     string
	Read      string
}

// AllPermissions 返回全部权限记录（按用户、vhost 排序）。
func (s *Store) AllPermissions() []UserPermission {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []UserPermission
	for name, rec := range s.users {
		for vhost, p := range rec.Permissions {
			out = append(out, UserPermission{
				User: name, VHost: vhost,
				Configure: p.Configure, Write: p.Write, Read: p.Read,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		return out[i].VHost < out[j].VHost
	})
	return out
}

// SetPermission 设置用户在某个 vhost 上的权限。
func (s *Store) SetPermission(user, vhost string, p config.Permission) error {
	if strings.TrimSpace(vhost) == "" {
		return fmt.Errorf("vhost 不能为空")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.users[user]
	if !ok {
		return fmt.Errorf("用户 %s 不存在", user)
	}
	if rec.Permissions == nil {
		rec.Permissions = map[string]config.Permission{}
	}
	rec.Permissions[vhost] = p
	s.users[user] = rec
	return nil
}

// DeletePermission 删除用户在某个 vhost 上的权限。
func (s *Store) DeletePermission(user, vhost string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.users[user]
	if !ok {
		return false, fmt.Errorf("用户 %s 不存在", user)
	}
	if _, ok := rec.Permissions[vhost]; !ok {
		return false, nil
	}
	delete(rec.Permissions, vhost)
	s.users[user] = rec
	return true, nil
}

// Verify 校验用户名口令，并按用户配置决定是否允许该来源地址访问。
//
// 管理 HTTP API 的 Basic Auth 与 AMQP 的 SASL 走**同一份用户表**，
// 否则"AMQP 能登录、管理面登不上"会成为长期的运维困惑。
// guest 的本机限制同样生效（配置 remote_access=false 时）。
func (s *Store) Verify(user, password string, remote net.Addr) (config.User, error) {
	s.mu.RLock()
	rec, ok := s.users[user]
	s.mu.RUnlock()
	if !ok || rec.Password != password {
		return config.User{}, fmt.Errorf("用户名或密码错误")
	}
	if !rec.RemoteAccess && !isLoopback(remote) {
		return config.User{}, fmt.Errorf("用户 %q 仅允许从本机访问（如需开放请设置 remote_access: true）", user)
	}
	return rec, nil
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
