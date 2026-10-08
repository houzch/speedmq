// Package config 负责内核配置的加载与默认值。
//
// M1 只支持"默认值 + JSON 配置文件覆盖"；YAML 与 SPEEDMQ_* 环境变量在 M5 随管理面一起补齐。
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
	// Root 标记内置的**总管理员账号**（语义对齐操作系统的 root）。
	//
	// 它不可删除、不可禁用、不可降级（必须保留 administrator 标签），改口令/改名只允许本人操作。
	// 只有在"首次引导播种"与"启动兜底"两处会被置位，管理 API 不能把别的账号提升为 root ——
	// 否则"总管理员"就退化成一个普通标签，保护规则随之失效。
	Root bool `json:"root,omitempty"`
	// Disabled 为 true 时该账号不能登录（AMQP 与管理面同一份用户表，两处都拒）。
	Disabled bool `json:"disabled,omitempty"`
	// MustChangePassword 为 true 时，管理 UI 登录后强制先改账号名/口令，改完才放行。
	//
	// 只在"首次引导播种"时对新装的初始账号置位：它针对的是"安装后仍是出厂口令"这一状态，
	// 而不是把所有该字段为空的账号都当成待改密（那会误伤既有部署）。
	MustChangePassword bool `json:"must_change_password,omitempty"`
	// APIGroups 是该账号被允许访问的**管理接口功能组**（overview / topology /
	// connections / accounts / policies / vhosts / cluster / plugins）。
	//
	// **为空表示不限制**：用标签允许的全部接口 —— 这样既有账号、rabbitmqadmin 与脚本
	// 的行为完全不变；新账号可以在此基础上按功能组收窄（只读监控账号只勾 overview 等）。
	// 它是标签（administrator / management / monitoring）之上的**收窄**，不是替代。
	APIGroups []string `json:"api_groups,omitempty"`
	// Permissions 按 vhost 名索引的权限。未列出的 vhost 一律拒绝。
	// 权限检查在 M3 生效：越权操作返回 403 ACCESS_REFUSED。
	Permissions map[string]Permission `json:"permissions,omitempty"`
}

// Listener 是监听配置。
type Listener struct {
	// Addr 形如 ":5672"。
	Addr string `json:"addr"`
	// TLS 提供证书时，该监听走 TLS（证书在启动时读取并校验）。
	TLS *TLS `json:"tls,omitempty"`
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

// Quorum 是仲裁队列（quorum queue）写路径的合批参数（M2 ack 合批 / M3 publish 合批）。
//
// 两个"窗口"是合批的等待上界：窗口内到达的确认/发布合并为一条日志条目；置 0（或负数）即关闭合批
// （每条立即单条提交）。默认值与此前的实现常量一致（ack 10ms / publish 5ms），故不配置即与旧行为相同。
type Quorum struct {
	// AckBatchWindowMS 是 ack 合批的时间窗口（毫秒），默认 10；<=0 关闭合批。
	AckBatchWindowMS int `json:"ack_batch_window_ms"`
	// AckBatchMax 是单条 ack_batch 携带的确认条数上限（积满即提交，不等窗口），默认 1024。
	AckBatchMax int `json:"ack_batch_max"`
	// PublishBatchWindowMS 是 publish 合批的时间窗口（毫秒），默认 5；<=0 关闭合批。
	PublishBatchWindowMS int `json:"publish_batch_window_ms"`
	// PublishBatchMax 是单条 publish_batch 携带的发布条数上限（积满即提交），默认 256。
	PublishBatchMax int `json:"publish_batch_max"`
}

// Management 是管理面（Management HTTP API + 内嵌管理 UI）配置。
type Management struct {
	// Enabled 为 false 时完全不启用管理面（不监听端口、不注册路由）。
	Enabled bool `json:"enabled"`
	// Addr 是管理面的监听地址，默认 ":15672"（对齐 RabbitMQ 管理插件端口）。
	Addr string `json:"addr"`
	// Language 是管理 UI 的**默认界面语言**（如 zh-CN / en）。
	//
	// 留空时在启动阶段按本机系统时区自动推断（见 LanguageFromTimezone）——
	// 这就是"安装时按部署地时区决定默认语言"的落点。取值非法直接启动失败，
	// 避免拼错的配置静默退回默认语言。用户在管理 UI 里选择的语言存在浏览器本地，
	// 优先于这个默认值。
	Language string `json:"language"`
	// TLS 提供证书时管理面走 HTTPS（字段与协议监听完全一致）。
	TLS *TLS `json:"tls,omitempty"`
}

// Cluster 是集群配置（二期 M6）。
//
// 只有 enabled=true 时才走 Raft：元数据（durable 拓扑与账号权限）在成员间复制并落盘；
// 未启用时仍是单机语义（元数据落本地快照，不监听集群端口）—— 默认关闭，
// 让"单机部署"保持与 M1–M5 完全一致的行为。
type Cluster struct {
	// Enabled 为 true 时启用集群。
	Enabled bool `json:"enabled"`
	// NodeID 是本节点标识，集群内唯一；留空时取主机名。
	NodeID string `json:"node_id"`
	// Listen 是集群内 RPC 监听地址，默认 ":25672"（对齐 RabbitMQ 的节点间端口）。
	Listen string `json:"listen"`
	// Peers 是集群地址簿：node_id → 该节点可达的 RPC 地址（**含自己**）。
	//
	// 它既是传输层寻址的依据，也是**首次引导**时的投票成员集合；
	// 一旦本地已持久化过成员表（发生过运行期成员变更），就以持久化的为准 ——
	// 运行期成员变更无需再改配置文件（见 `join` 与 `speedmqctl add_member`）。
	Peers map[string]string `json:"peers,omitempty"`
	// Join 表示本节点以 **learner** 身份加入既有集群：启动时只复制日志、不参与投票、
	// 不发起竞选，等待集群 leader 通过 `speedmqctl add_member` 把它提升为投票成员。
	//
	// 与 Peers 一样只在**首次引导**时生效。新节点用它启动，可以避免"自认为已是成员、
	// 却在别人的成员表之外"造成的选举抖动。
	Join bool `json:"join,omitempty"`
	// PartitionPolicy 是分区策略：
	//   - pause_minority（默认）：与多数派失联时暂停服务，避免脑裂产生分叉数据；
	//   - ignore：不暂停（仅记录状态），由部署方自行承担风险。
	PartitionPolicy string `json:"partition_policy"`
}

// 分区策略取值。
const (
	// PartitionPauseMinority 是默认策略：少数派停止服务。
	PartitionPauseMinority = "pause_minority"
	// PartitionIgnore 表示不做分区保护。
	PartitionIgnore = "ignore"
)

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
	// Quorum 仲裁队列写路径合批参数段（M2/M3）。
	Quorum Quorum `json:"quorum"`
	// Management 管理面配置段（M5 起生效）。
	Management Management `json:"management"`
	// Cluster 集群配置段（M6 起生效）。
	Cluster Cluster `json:"cluster"`
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
				// 出厂账号即**总管理员**：安装后第一次登录会被强制改掉账号名与口令，
				// 因此这里不预置 must_change_password —— 那个标记由首次引导播种统一置位
				// （见 broker.seedConfigUsers），这样"配置文件里没写 root"的部署也能拿到总账号。
				Root: true,
			},
		},
		Storage: DefaultStorage(),
		Quorum:  DefaultQuorum(),
		Management: Management{
			Enabled: true,
			Addr:    ":15672",
		},
		Cluster: DefaultCluster(),
	}
}

// 默认集群参数。
const (
	// DefaultClusterListen 是节点间 RPC 的默认监听地址（对齐 RabbitMQ 的节点间端口）。
	DefaultClusterListen = ":25672"
)

// DefaultCluster 返回默认集群配置：默认关闭，单机部署行为与 M1–M5 完全一致。
func DefaultCluster() Cluster {
	return Cluster{
		Enabled:         false,
		Listen:          DefaultClusterListen,
		PartitionPolicy: PartitionPauseMinority,
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

// 默认仲裁队列合批参数（与实现里此前的常量一致，保证"不配置 = 旧行为"）。
const (
	// DefaultQuorumAckBatchWindowMS 是 ack 合批的默认时间窗口（毫秒）。
	DefaultQuorumAckBatchWindowMS = 10
	// DefaultQuorumAckBatchMax 是单条 ack_batch 的默认确认条数上限。
	DefaultQuorumAckBatchMax = 1024
	// DefaultQuorumPublishBatchWindowMS 是 publish 合批的默认时间窗口（毫秒）。
	DefaultQuorumPublishBatchWindowMS = 5
	// DefaultQuorumPublishBatchMax 是单条 publish_batch 的默认发布条数上限。
	DefaultQuorumPublishBatchMax = 256
)

// DefaultQuorum 返回默认仲裁队列合批参数。
func DefaultQuorum() Quorum {
	return Quorum{
		AckBatchWindowMS:     DefaultQuorumAckBatchWindowMS,
		AckBatchMax:          DefaultQuorumAckBatchMax,
		PublishBatchWindowMS: DefaultQuorumPublishBatchWindowMS,
		PublishBatchMax:      DefaultQuorumPublishBatchMax,
	}
}

// fullPermission 表示不受限的权限。
var fullPermission = Permission{Configure: ".*", Write: ".*", Read: ".*"}

// stripUTF8BOM 去掉开头的 UTF-8 字节顺序标记（BOM）；没有则原样返回。
//
// 为什么要做：Windows 上不少编辑器与脚本（例如 PowerShell 的 `Set-Content -Encoding UTF8`、
// 部分版本的记事本）会写出**带 BOM** 的文件，而 encoding/json 不接受 BOM，只会报一句
// `invalid character 'ï' looking for beginning of value` —— 这个报错对使用者毫无指向性，
// 排查成本很高（该字节肉眼不可见）。BOM 只可能出现在文件开头，剥离它对正常文件无影响。
func stripUTF8BOM(raw []byte) []byte {
	const bomLen = 3
	if len(raw) >= bomLen && raw[0] == 0xEF && raw[1] == 0xBB && raw[2] == 0xBF {
		return raw[bomLen:]
	}
	return raw
}

// Load 读取配置文件；path 为空时直接返回默认配置。
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取配置文件失败: %w", err)
		}
		if err := json.Unmarshal(stripUTF8BOM(raw), cfg); err != nil {
			return nil, fmt.Errorf("解析配置文件失败: %w", err)
		}
	}
	if cfg.DefaultVHost == "" {
		cfg.DefaultVHost = "/"
	}
	if cfg.Plugins == nil {
		cfg.Plugins = map[string]json.RawMessage{}
	}
	// 环境变量优先于文件：容器化部署用 SPEEDMQ_* 覆盖镜像内置配置，
	// 这样同一份配置可以为不同环境所用（对齐设计第 11 章的"配置"选型）。
	if err := cfg.ApplyEnv(); err != nil {
		return nil, err
	}
	if err := normalizeStorage(&cfg.Storage); err != nil {
		return nil, err
	}
	normalizeQuorum(&cfg.Quorum)
	if err := normalizeManagement(&cfg.Management); err != nil {
		return nil, err
	}
	if err := normalizeListeners(cfg.Listeners); err != nil {
		return nil, err
	}
	if err := normalizeCluster(&cfg.Cluster); err != nil {
		return nil, err
	}
	backfillPermissions(cfg)
	return cfg, nil
}

// ApplyEnv 用 SPEEDMQ_* 环境变量覆盖配置。未设置的变量不改变任何字段。
func (c *Config) ApplyEnv() error {
	str := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			*dst = v
		}
	}
	str("SPEEDMQ_DATA_DIR", &c.DataDir)
	str("SPEEDMQ_DEFAULT_VHOST", &c.DefaultVHost)
	str("SPEEDMQ_FSYNC", &c.Storage.Fsync)
	str("SPEEDMQ_MANAGEMENT_ADDR", &c.Management.Addr)
	str("SPEEDMQ_MANAGEMENT_LANGUAGE", &c.Management.Language)
	str("SPEEDMQ_CLUSTER_NODE_ID", &c.Cluster.NodeID)
	str("SPEEDMQ_CLUSTER_LISTEN", &c.Cluster.Listen)
	str("SPEEDMQ_CLUSTER_PARTITION_POLICY", &c.Cluster.PartitionPolicy)

	if v, ok := os.LookupEnv("SPEEDMQ_FLUSH_INTERVAL_MS"); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("SPEEDMQ_FLUSH_INTERVAL_MS 取值非法: %q", v)
		}
		c.Storage.FlushIntervalMS = n
	}
	if v, ok := os.LookupEnv("SPEEDMQ_MEMORY_HIGH_WATERMARK"); ok && v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("SPEEDMQ_MEMORY_HIGH_WATERMARK 取值非法: %q", v)
		}
		c.Storage.MemoryHighWatermark = f
	}
	if v, ok := os.LookupEnv("SPEEDMQ_DISK_FREE_LIMIT"); ok && v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("SPEEDMQ_DISK_FREE_LIMIT 取值非法: %q", v)
		}
		c.Storage.DiskFreeLimit = n
	}
	if v, ok := os.LookupEnv("SPEEDMQ_MANAGEMENT_ENABLED"); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("SPEEDMQ_MANAGEMENT_ENABLED 取值非法: %q（应为 true/false）", v)
		}
		c.Management.Enabled = b
	}
	if v, ok := os.LookupEnv("SPEEDMQ_CLUSTER_ENABLED"); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("SPEEDMQ_CLUSTER_ENABLED 取值非法: %q（应为 true/false）", v)
		}
		c.Cluster.Enabled = b
	}
	if v, ok := os.LookupEnv("SPEEDMQ_CLUSTER_JOIN"); ok && v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("SPEEDMQ_CLUSTER_JOIN 取值非法: %q（应为 true/false）", v)
		}
		c.Cluster.Join = b
	}
	if v, ok := os.LookupEnv("SPEEDMQ_AMQP_ADDR"); ok && v != "" {
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
	// 默认语言与"是否启用管理面"无关：即便本次停用，配置里也应留下一个确定的值，
	// 便于日志与测试对照。留空 = 按系统时区推断（安装时的默认语言）。
	if m.Language == "" {
		m.Language = LanguageFromTimezone(SystemTimezone())
	}
	if !IsSupportedLanguage(m.Language) {
		return fmt.Errorf("management.language 取值非法: %q（可选 %s）", m.Language, languageHint())
	}
	if !m.Enabled {
		return nil
	}
	if _, _, err := net.SplitHostPort(m.Addr); err != nil {
		return fmt.Errorf("management.addr 取值非法: %q（应形如 :15672 或 127.0.0.1:15672）", m.Addr)
	}
	return m.TLS.normalize("management")
}

// normalizeListeners 校验各协议监听的 TLS 配置。
//
// 逐个监听校验（而不是只看插件级），是因为一个插件可能同时开明文与 TLS 两个端口 ——
// 这正是迁移期的常态，错误信息也必须能定位到具体那个监听。
func normalizeListeners(listeners map[string][]Listener) error {
	for pluginName, list := range listeners {
		for i := range list {
			if err := list[i].TLS.normalize(fmt.Sprintf("listeners.%s[%d]", pluginName, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// normalizeCluster 校验并补齐集群配置。
//
// 默认关闭集群：未启用时不校验监听地址，也不触碰成员表，让单机部署与 M1–M5 完全一致。
func normalizeCluster(c *Cluster) error {
	if c.Listen == "" {
		c.Listen = DefaultClusterListen
	}
	if c.NodeID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			return fmt.Errorf("cluster.node_id 为空且无法获取主机名，请显式配置")
		}
		// 与 speedmqd 的节点名（speedmq@<host>）保持同一口径：同一台机器上，
		// /api/cluster 的 node_id 与 /api/nodes 的 name 应当能对上。
		c.NodeID = "speedmq@" + host
	}
	switch c.PartitionPolicy {
	case "":
		c.PartitionPolicy = PartitionPauseMinority
	case PartitionPauseMinority, PartitionIgnore:
	default:
		return fmt.Errorf("cluster.partition_policy 取值非法: %q（可选 %s / %s）",
			c.PartitionPolicy, PartitionPauseMinority, PartitionIgnore)
	}
	if !c.Enabled {
		return nil
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("cluster.listen 取值非法: %q（应形如 :25672 或 127.0.0.1:25672）", c.Listen)
	}
	// 成员表必须包含自己：缺省时按本节点监听地址补上，让"单节点集群"零配置可跑。
	if len(c.Peers) == 0 {
		c.Peers = map[string]string{c.NodeID: c.Listen}
	}
	for id, addr := range c.Peers {
		if id == "" {
			return fmt.Errorf("cluster.peers 含空节点标识")
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("cluster.peers[%s] 地址非法: %q", id, addr)
		}
	}
	if _, ok := c.Peers[c.NodeID]; !ok {
		return fmt.Errorf("cluster.peers 缺少本节点 %q（成员表须含自己）", c.NodeID)
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

// normalizeQuorum 补齐仲裁队列的批大小上限：非法（<1）时退回默认。
//
// 两个"时间窗口"不在此处理 —— 窗口 <=0 是合法的"关闭合批"语义，不能被当成未配置而改回默认；
// 未配置时靠 Default() 提供默认值（Load 从 Default 起再反序列化，缺省字段保留默认）。
func normalizeQuorum(q *Quorum) {
	if q.AckBatchMax < 1 {
		q.AckBatchMax = DefaultQuorumAckBatchMax
	}
	if q.PublishBatchMax < 1 {
		q.PublishBatchMax = DefaultQuorumPublishBatchMax
	}
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
