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
//
// 用户与它的权限表都要**深拷贝**：只拷外层会让 auth 直接改写 config 里的 map
// （`ApplyPermission` 是原地写入）—— 于是运行期新增的权限会"渗回"配置，
// 再被首次引导的播种逻辑当成配置内容重新写进元数据，表现为"删掉的权限自己复活"。
func NewStore(users map[string]config.User) *Store {
	cp := make(map[string]config.User, len(users))
	for k, v := range users {
		cp[k] = copyUser(v)
	}
	return &Store{users: cp}
}

// copyUser 复制一条用户记录（含权限表与功能组），避免与调用方共享可变 map/slice。
func copyUser(u config.User) config.User {
	u.Tags = append([]string(nil), u.Tags...)
	u.APIGroups = append([]string(nil), u.APIGroups...)
	if u.Permissions != nil {
		perms := make(map[string]config.Permission, len(u.Permissions))
		for vhost, p := range u.Permissions {
			perms[vhost] = p
		}
		u.Permissions = perms
	}
	return u
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
	// Root / Disabled / MustChangePassword 不在这里改：它们由"总账号归属"与"首次改密"
	// 两条独立语义决定，顺手清零会让"改个口令把总账号保护也改没了"。
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

// ---------------------------------------------------------------------------
// 元数据应用路径（M8-4）
//
// 下面这组方法与 UpsertUser / SetPermission 的区别是**语义**而不是实现：
// 写路径（管理 API）表达的是"运维想做什么"，要校验、要报错、要返回是否命中；
// 应用路径（元数据/Raft 日志）表达的是"已经决定要变成什么样"，必须**幂等且不失败** ——
// 返回错误会让 Raft 反复重放同一条日志，把整个节点卡住。
// ---------------------------------------------------------------------------

// ApplyUser 覆盖式写入一条来自元数据的用户记录。
//
// 与 UpsertUser 的差别在 RemoteAccess：这里按记录给的值走，因此配置文件里的 guest
// （remote_access=false）在元数据里往返一趟之后仍然只允许本机登录；
// UpsertUser 是"管理 API 创建账号"的语义，一律允许远端登录。
func (s *Store) ApplyUser(name string, u config.User) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("元数据中的用户缺少用户名")
	}
	rec := u
	s.mu.Lock()
	defer s.mu.Unlock()
	// 权限在本包里挂在用户记录下，而元数据里用户与权限是两组独立记录：
	// 覆盖用户字段时保留已应用的权限，否则"先写权限、后写用户"的顺序会把权限抹掉。
	if old, ok := s.users[name]; ok {
		rec.Permissions = old.Permissions
	}
	if rec.Permissions == nil {
		rec.Permissions = map[string]config.Permission{}
	}
	s.users[name] = rec
	return nil
}

// ApplyDeleteUser 删除用户；不存在时静默成功（日志重放必须幂等）。
func (s *Store) ApplyDeleteUser(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, name)
}

// ApplyPermission 覆盖式写入一条权限记录；用户不存在时返回 false 且不做任何改动。
//
// 不返回 error 是刻意的：调用方（内核）只据此记一条告警。"先给权限、后建用户"
// 以及快照中权限记录先于用户记录应用都是合法顺序，为此让 Raft 停机会因小失大。
func (s *Store) ApplyPermission(user, vhost string, p config.Permission) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.users[user]
	if !ok {
		return false
	}
	if rec.Permissions == nil {
		rec.Permissions = map[string]config.Permission{}
	}
	rec.Permissions[vhost] = p
	s.users[user] = rec
	return true
}

// ApplyDeletePermission 删除一条权限记录；用户或权限不存在时静默成功。
func (s *Store) ApplyDeletePermission(user, vhost string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.users[user]
	if !ok {
		return
	}
	delete(rec.Permissions, vhost)
	s.users[user] = rec
}

// ReplaceUsers 用一组用户记录整体替换用户表（元数据快照恢复用）。
//
// 传入的 map 会被拷贝，调用方随后修改它不会影响本表。
func (s *Store) ReplaceUsers(users map[string]config.User) {
	cp := make(map[string]config.User, len(users))
	for k, v := range users {
		cp[k] = v
	}
	s.mu.Lock()
	s.users = cp
	s.mu.Unlock()
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
	// 被禁用的账号与口令错误返回同一句话：禁用的目的就是让对方连不上，
	// 而"该账号已被禁用"会把账号是否存在泄露给未认证方。
	if rec.Disabled {
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
	// 用户不存在、口令错误、账号被禁用一律返回同一个错误，避免泄露账号是否存在。
	if !ok || rec.Password != pass || rec.Disabled {
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
