// Package config 负责内核配置的加载与默认值。
//
// M1 只支持"默认值 + JSON 配置文件覆盖"；YAML 与 SWIFTMQ_* 环境变量在 M5 随管理面一起补齐。
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
)

// Permission 是用户在某个 vhost 上的操作权限。
//
// 三分类与语义对齐 RabbitMQ：configure 管拓扑声明与删除，write 管发布与绑定，
// read 管消费与拉取。值为正则字符串，匹配对应的资源名（交换机名 / 队列名）。
type Permission struct {
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

// User 是内置用户表的一条记录（v1 仅内存用户表；LDAP / OAuth2 由认证插件提供）。
type User struct {
	// Password 明文口令。v1 仅用于内网开发环境，后续由认证插件提供哈希/外部后端。
	Password string `json:"password"`
	// Tags 用户标签（administrator / management / monitoring），M5 管理面起生效。
	Tags []string `json:"tags,omitempty"`
	// RemoteAccess 为 true 时允许从非本机地址登录。
	// 对齐 RabbitMQ：内置 guest 用户默认仅允许本机登录。
	RemoteAccess bool `json:"remote_access,omitempty"`
	// Permissions 按 vhost 名索引的权限。未列出的 vhost 一律拒绝。
	// 权限检查在 M3 生效：越权操作返回 403 ACCESS_REFUSED。
	Permissions map[string]Permission `json:"permissions,omitempty"`
}

// Listener 是监听配置。
type Listener struct {
	// Addr 形如 ":5672"。
	Addr string `json:"addr"`
}

// Storage 是存储与资源流控配置（M4 起生效）。
type Storage struct {
	// Fsync 是落盘档位：none / os / batch / always。
	//
	// 它同时决定 publisher confirm 的时机（见设计 5.3.5），默认 os ——
	// 对齐 RabbitMQ 经典队列"confirm 前不 fsync"的行为与性能特征。
	Fsync string `json:"fsync"`
	// FlushIntervalMS 是兜底刷盘间隔（毫秒），即"消息在内存里最多待多久"的上界，默认 200。
	FlushIntervalMS int `json:"flush_interval_ms"`
	// MemoryHighWatermark 是内存水位：进程内存超过"该比例 × 物理内存"即阻塞生产者，默认 0.4。
	// 0 表示关闭内存水位检查。
	MemoryHighWatermark float64 `json:"memory_high_watermark"`
	// DiskFreeLimit 是数据目录剩余空间下限（字节），低于它即阻塞生产者，默认 50 MiB。
	// 0 表示关闭磁盘水位检查。
	DiskFreeLimit uint64 `json:"disk_free_limit"`
}

// Management 是管理面（Management HTTP API + 内嵌管理 UI）配置。
type Management struct {
	// Enabled 为 false 时完全不启用管理面（不监听端口、不注册路由）。
	Enabled bool `json:"enabled"`
	// Addr 是管理面的监听地址，默认 ":15672"（对齐 RabbitMQ 管理插件端口）。
	Addr string `json:"addr"`
}

// Config 是内核配置。
type Config struct {
	// DataDir 节点数据目录：消息、日志与元数据都放在其下
	// （对齐 RabbitMQ 的 RABBITMQ_MNESIA_DIR 定位）。
	DataDir string `json:"data_dir"`
	// DefaultVHost 默认 vhost 名。
	DefaultVHost string `json:"default_vhost"`
	// VHosts v1 的 vhost 清单；M2 起改为元数据存储中的动态资源。
	VHosts []string `json:"vhosts"`
	// Listeners 按插件名覆盖监听地址，如 {"amqp091": ":5672"}。
	Listeners map[string][]Listener `json:"listeners,omitempty"`
	// Plugins 插件的配置段，按插件名索引；插件经 Host.Config 读取。
	//
	// 内核自身只关心其中的治理开关（enabled / required / builtin，见 PluginFlags），
	// 其余字段原样交给插件。
	Plugins map[string]json.RawMessage `json:"plugins,omitempty"`
	// Users 内置用户表。
	Users map[string]User `json:"users"`
	// Storage 存储与流控配置段。
	Storage Storage `json:"storage"`
	// Management 管理面配置段（M5 起生效）。
	Management Management `json:"management"`
}

// Default 返回默认配置。
func Default() *Config {
	return &Config{
		DataDir:      "data",
		DefaultVHost: "/",
		VHosts:       []string{"/"},
		Plugins:      map[string]json.RawMessage{},
		Users: map[string]User{
			"guest": {
				Password:     "guest",
				Tags:         []string{"administrator"},
				RemoteAccess: false,
			},
		},
		Storage: DefaultStorage(),
		Management: Management{
			Enabled: true,
			Addr:    ":15672",
		},
	}
}

// 默认存储与流控参数。
const (
	// DefaultFlushIntervalMS 是兜底刷盘间隔（毫秒）。
	DefaultFlushIntervalMS = 200
	// DefaultMemoryHighWatermark 是内存水位（占物理内存比例）。
	DefaultMemoryHighWatermark = 0.4
	// DefaultDiskFreeLimit 是数据目录剩余空间下限（50 MiB）。
	DefaultDiskFreeLimit = 50 << 20
)

// DefaultStorage 返回默认存储与流控配置。
func DefaultStorage() Storage {
	return Storage{
		Fsync:               "os",
		FlushIntervalMS:     DefaultFlushIntervalMS,
		MemoryHighWatermark: DefaultMemoryHighWatermark,
		DiskFreeLimit:       DefaultDiskFreeLimit,
	}
}

// fullPermission 表示不受限的权限。
var fullPermission = Permission{Configure: ".*", Write: ".*", Read: ".*"}

// Load 读取配置文件；path 为空时直接返回默认配置。
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
		if err := json.Unmarshal(raw, cfg); err != nil {
			return nil, fmt.Errorf("解析配置文件失败: %w", err)
		}
	}
	if cfg.DefaultVHost == "" {
		cfg.DefaultVHost = "/"
	}
	if cfg.Plugins == nil {
		cfg.Plugins = map[string]json.RawMessage{}
	}
	// 环境变量优先于文件：容器化部署用 SWIFTMQ_* 覆盖镜像内置配置，
	// 这样同一份配置可以为不同环境所用（对齐设计第 11 章的"配置"选型）。
	if err := cfg.ApplyEnv(); err != nil {
		return nil, err
	}
	if err := normalizeStorage(&cfg.Storage); err != nil {
		return nil, err
	}
	if err := normalizeManagement(&cfg.Management); err != nil {
		return nil, err
	}
	backfillPermissions(cfg)
	return cfg, nil
}

// ApplyEnv 用 SWIFTMQ_* 环境变量覆盖配置。未设置的变量不改变任何字段。
func (c *Config) ApplyEnv() error {
	str := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			*dst = v
		}
	}
	str("SWIFTMQ_DATA_DIR", &c.DataDir)
	str("SWIFTMQ_DEFAULT_VHOST", &c.DefaultVHost)
	str("SWIFTMQ_FSYNC", &c.Storage.Fsync)
	str("SWIFTMQ_MANAGEMENT_ADDR", &c.Management.Addr)

	if v, ok := os.LookupEnv("SWIFTMQ_FLUSH_INTERVAL_MS"); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("SWIFTMQ_FLUSH_INTERVAL_MS 取值非法: %q", v)
		}
		c.Storage.FlushIntervalMS = n
	}
	if v, ok := os.LookupEnv("SWIFTMQ_MEMORY_HIGH_WATERMARK"); ok && v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("SWIFTMQ_MEMORY_HIGH_WATERMARK 取值非法: %q", v)
		}
		c.Storage.MemoryHighWatermark = f
	}
	if v, ok := os.LookupEnv("SWIFTMQ_DISK_FREE_LIMIT"); ok && v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("SWIFTMQ_DISK_FREE_LIMIT 取值非法: %q", v)
		}
		c.Storage.DiskFreeLimit = n
	}
	if v, ok := os.LookupEnv("SWIFTMQ_MANAGEMENT_ENABLED"); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("SWIFTMQ_MANAGEMENT_ENABLED 取值非法: %q（应为 true/false）", v)
		}
		c.Management.Enabled = b
	}
	if v, ok := os.LookupEnv("SWIFTMQ_AMQP_ADDR"); ok && v != "" {
		if c.Listeners == nil {
			c.Listeners = map[string][]Listener{}
		}
		c.Listeners["amqp091"] = []Listener{{Addr: v}}
	}
	return nil
}

// PluginFlags 读取某插件配置段里的治理开关（enabled / required / builtin）。
//
// 默认值：enabled=true、required=false、builtin=true —— M5 的插件都是随内核编译进来的；
// 外部进程插件形态落地后，由部署方在配置里显式声明 builtin: false。
func (c *Config) PluginFlags(name string) (enabled, required, builtin bool) {
	enabled, required, builtin = true, false, true
	raw, ok := c.Plugins[name]
	if !ok {
		return
	}
	var flags struct {
		Enabled  *bool `json:"enabled"`
		Required *bool `json:"required"`
		Builtin  *bool `json:"builtin"`
	}
	// 配置段非法时按默认值继续：段内容的正确性由插件自己在 Init 时报错，
	// 内核不因为读不懂某个插件的私有字段而拒绝启动。
	if err := json.Unmarshal(raw, &flags); err != nil {
		return
	}
	if flags.Enabled != nil {
		enabled = *flags.Enabled
	}
	if flags.Required != nil {
		required = *flags.Required
	}
	if flags.Builtin != nil {
		builtin = *flags.Builtin
	}
	return
}

// normalizeManagement 校验并补齐管理面配置。
func normalizeManagement(m *Management) error {
	if m.Addr == "" {
		m.Addr = ":15672"
	}
	if !m.Enabled {
		return nil
	}
	if _, _, err := net.SplitHostPort(m.Addr); err != nil {
		return fmt.Errorf("management.addr 取值非法: %q（应形如 :15672 或 127.0.0.1:15672）", m.Addr)
	}
	return nil
}

// normalizeStorage 校验并补齐存储配置：值非法时直接失败，
// 避免"配错了却静默按默认档位跑"导致可靠性预期落空。
func normalizeStorage(s *Storage) error {
	switch s.Fsync {
	case "":
		s.Fsync = "os"
	case "none", "os", "batch", "always":
	default:
		return fmt.Errorf("storage.fsync 取值非法: %q（可选 none / os / batch / always）", s.Fsync)
	}
	if s.FlushIntervalMS <= 0 {
		s.FlushIntervalMS = DefaultFlushIntervalMS
	}
	if s.MemoryHighWatermark < 0 {
		return fmt.Errorf("storage.memory_high_watermark 不能为负数: %v", s.MemoryHighWatermark)
	}
	return nil
}

// backfillPermissions 为未显式声明权限的内置用户补上全部 vhost 的完全权限。
//
// 理由：v1 的内置用户表就是"节点管理员"（与 administrator 标签一致），
// 让配置文件里没写权限的既有部署保持可用。想限制某个用户时显式声明 permissions 即可 ——
// 一旦声明，未列出的 vhost 一律拒绝（与 RabbitMQ 语义一致）。
// M5 管理面引入动态权限表后，这里会改为不再兜底。
func backfillPermissions(cfg *Config) {
	vhosts := append([]string{}, cfg.VHosts...)
	if !containsString(vhosts, cfg.DefaultVHost) {
		vhosts = append(vhosts, cfg.DefaultVHost)
	}
	for name, u := range cfg.Users {
		if len(u.Permissions) > 0 {
			continue
		}
		perms := make(map[string]Permission, len(vhosts))
		for _, vh := range vhosts {
			perms[vh] = fullPermission
		}
		u.Permissions = perms
		cfg.Users[name] = u
	}
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// PluginConfig 返回某个插件的配置段；不存在时返回空对象。
func (c *Config) PluginConfig(name string) json.RawMessage {
	if raw, ok := c.Plugins[name]; ok {
		return raw
	}
	return json.RawMessage("{}")
}
