// Package meta 是集群元数据层：vhost / exchange / queue / binding / user / permission。
//
// 它填的正是 M5 留下的坑 —— "拓扑与账号只在内存里，重启即丢、多节点各说各话"。
//
// 两种后端，对上层是同一套 API（内核代码不需要分支）：
//   - ModeRaft（cluster.enabled=true）：写入经 Raft 提交 —— leader 直接 Propose，
//     follower 通过 Transport 转发给 leader，提交后在**所有节点**按同一顺序应用；
//   - ModeLocal（单机）：写入直接应用到内存状态，并原子落盘 <data_dir>/meta/state.json。
//
// 约定（实现必须遵守）：
//  1. 元数据只承载"集群级拓扑"：**durable 且非 exclusive 的队列**、交换机、绑定、用户与权限。
//     transient / exclusive / auto-delete 的队列是会话本地的，不属于集群元数据。
//  2. ApplyMeta 必须**幂等**：Raft 重放与快照恢复会重复应用同一批 Op，
//     实现里不得出现"第二次应用产生不同结果"的逻辑（例如自增计数、追加式副作用）。
//  3. 状态机内**不得**调用 Store.Write（会自锁）；写入一律由外部调用方发起。
package meta

import (
	"context"
	"errors"
	"time"

	"github.com/houzch/speedmq/internal/raft"
)

// Mode 是元数据存储的后端模式。
type Mode uint8

const (
	// ModeLocal 是单机模式：内存状态 + 本地快照文件。
	ModeLocal Mode = iota
	// ModeRaft 是集群模式：Raft 复制日志 + 快照。
	ModeRaft
)

// String 返回模式名。
func (m Mode) String() string {
	if m == ModeRaft {
		return "raft"
	}
	return "local"
}

// Op 是元数据变更的操作类型。取值即 Raft 日志里的命令名（保持稳定，便于排查）。
type Op string

// 全部元数据操作。
const (
	OpPutVHost         Op = "vhost.put"
	OpDeleteVHost      Op = "vhost.delete"
	OpPutExchange      Op = "exchange.put"
	OpDeleteExchange   Op = "exchange.delete"
	OpPutQueue         Op = "queue.put"
	OpDeleteQueue      Op = "queue.delete"
	OpPutBinding       Op = "binding.put"
	OpDeleteBinding    Op = "binding.delete"
	OpPutUser          Op = "user.put"
	OpDeleteUser       Op = "user.delete"
	OpPutPermission    Op = "permission.put"
	OpDeletePermission Op = "permission.delete"
	OpPutPolicy        Op = "policy.put"
	OpDeletePolicy     Op = "policy.delete"
	OpPutVHostLimit    Op = "vhost.limit.put"
	OpDeleteVHostLimit Op = "vhost.limit.delete"
	OpPutFeatureFlag   Op = "feature.flag.put"
)

// VHost 是 vhost 的元数据记录。
type VHost struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// Exchange 是交换机的元数据记录。
type Exchange struct {
	VHost      string         `json:"vhost"`
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments,omitempty"`
}

// Queue 是队列的元数据记录。
type Queue struct {
	VHost      string         `json:"vhost"`
	Name       string         `json:"name"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Exclusive  bool           `json:"exclusive"`
	Arguments  map[string]any `json:"arguments,omitempty"`
	// Owner 是持有该队列消息数据的节点 ID（队列放置结果）。本期取"声明该队列的节点"。
	Owner     string    `json:"owner"`
	CreatedAt time.Time `json:"created_at"`
	// Replicas 是仲裁队列的**副本集**（Raft 组的投票成员，已排序）。
	//
	// 只有仲裁队列（x-queue-type=quorum）会写它；经典队列为空。
	// 它由声明时的 x-quorum-initial-group-size 决定，之后可由 grow 在运行期扩大；
	// 落进元数据是为了让"扩到 N 副本"这个决定随日志复制到全体节点、并在重启/新节点加入后仍然生效
	// （Raft 组自身的成员表也持久化在各自的数据目录里，两者是同一决定的两处记录）。
	Replicas []string `json:"replicas,omitempty"`
}

// Binding 是绑定的元数据记录。
type Binding struct {
	VHost           string         `json:"vhost"`
	Source          string         `json:"source"`
	Destination     string         `json:"destination"`
	DestinationType string         `json:"destination_type"` // queue / exchange
	RoutingKey      string         `json:"routing_key"`
	Arguments       map[string]any `json:"arguments,omitempty"`
}

// User 是用户的元数据记录（口令为明文，与配置文件口径一致）。
type User struct {
	Name         string   `json:"name"`
	Password     string   `json:"password"`
	Tags         []string `json:"tags,omitempty"`
	RemoteAccess bool     `json:"remote_access"`
	// Root 标记总管理员账号（不可删除/禁用/降级），见 config.User.Root。
	Root bool `json:"root,omitempty"`
	// Disabled 为 true 时该账号不能登录。
	Disabled bool `json:"disabled,omitempty"`
	// MustChangePassword 为 true 时首次登录须先改账号名/口令。
	MustChangePassword bool `json:"must_change_password,omitempty"`
	// APIGroups 是允许访问的管理接口功能组；为空表示不限制（见 config.User.APIGroups）。
	APIGroups []string `json:"api_groups,omitempty"`
}

// Permission 是权限的元数据记录。
type Permission struct {
	User      string `json:"user"`
	VHost     string `json:"vhost"`
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

// Policy 是策略的元数据记录（对齐 RabbitMQ 的 policy 对象）。
//
// Definition 里的键是**去 x- 前缀**的形式（`message-ttl` / `max-length` / `dead-letter-exchange` …），
// 与 RabbitMQ 的管理 API 一致；落到队列参数时才补回 x- 前缀。
type Policy struct {
	VHost   string `json:"vhost"`
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	// ApplyTo 是作用对象：queues / classic_queues / quorum_queues / exchanges / all。
	ApplyTo string `json:"apply_to"`
	// Definition 是策略内容。
	Definition map[string]any `json:"definition,omitempty"`
	// Priority 是优先级：数字越大越优先，多个策略命中同一个对象时只有最高的生效。
	Priority int `json:"priority"`
}

// VHostLimit 是 vhost 级限制的元数据记录（对齐 RabbitMQ 的 vhost-limits）。
//
// Name 取 max-connections / max-queues；Value 恒为正数（0 或负数在写路径就被拒，
// 见 broker.SetVHostLimit）—— "值为 0 / 负数"该怎么解释没有公认口径，
// 与其自创一种，不如明确报错。
type VHostLimit struct {
	VHost string `json:"vhost"`
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// FeatureFlag 是特性开关的元数据记录。
//
// 只持久化"被显式改过的开关"：没有记录 = 用注册表里的默认状态，
// 于是升级时新增的开关不必回填，也不会因为快照里缺字段而被当成关闭。
type FeatureFlag struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

// State 是元数据的完整快照。
//
// 这是**只读视图**：调用方不得修改其中的记录值。State() 返回时容器已被复制，
// 因此调用方可以安全地在锁外遍历；但记录内部的 map/slice 仍与状态机共享，
// 只读访问是安全的（状态机对记录只做整体替换，不原地改写）。
type State struct {
	VHosts      map[string]VHost
	Exchanges   map[string]Exchange
	Queues      map[string]Queue
	Bindings    []Binding
	Users       map[string]User
	Permissions map[string]Permission
	Policies    map[string]Policy
	// Limits 是 vhost 级限制，键为 LimitKey(vhost, name)。
	Limits map[string]VHostLimit
	// FeatureFlags 是**被显式改过**的特性开关，键为开关名。
	FeatureFlags map[string]FeatureFlag
}

// Key 生成 (vhost, name) 的复合键，用于 Exchanges / Queues 的 map。
func Key(vhost, name string) string { return vhost + "\x00" + name }

// PermissionKey 生成 (vhost, user) 的复合键。
func PermissionKey(vhost, user string) string { return vhost + "\x00" + user }

// PolicyKey 生成 (vhost, policy) 的复合键。
func PolicyKey(vhost, name string) string { return vhost + "\x00" + name }

// LimitKey 生成 (vhost, 限制名) 的复合键。
func LimitKey(vhost, name string) string { return vhost + "\x00" + name }

// Applier 由内核实现：元数据变更提交后，用它更新本节点的内存拓扑。
//
// 之所以用回调而不是"Store 直接持有内核"：状态机的应用顺序由 Raft 决定，
// 内核只负责把结果落到自己的数据结构上，两层职责清晰、也便于测试替身。
type Applier interface {
	// ApplyMeta 应用一次已提交的元数据变更；payload 是该 Op 对应记录的 JSON。
	// 必须是幂等的（Raft 重放/快照恢复会重复应用）。
	ApplyMeta(op Op, payload []byte) error
	// RestoreMeta 用完整快照重建内存拓扑（启动重放前、以及收到 InstallSnapshot 时调用）。
	RestoreMeta(state State) error
}

// Logger 是元数据层需要的日志能力。
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Options 是打开元数据存储的参数。
type Options struct {
	// Mode 决定后端。
	Mode Mode
	// NodeID 是本节点 ID（集群模式必填，也是队列 Owner 的取值）。
	NodeID string
	// Dir 是元数据目录（单机模式放 state.json；集群模式放 Raft 日志与快照）。
	Dir string
	// Listen 是集群 RPC 监听地址（仅 ModeRaft；形如 ":25672"）。
	Listen string
	// Peers 是集群**投票**成员 node_id → RPC 地址（仅 ModeRaft；含本节点）。
	//
	// 只在首次引导时生效：一旦本地已持久化过成员表，就以持久化的为准
	// （否则运行期的成员变更会在重启后被打回原形）。
	Peers map[string]string
	// Learners 是初始的非投票成员 ID（仅 ModeRaft）：新节点以 learner 身份加入时使用。
	Learners []string
	// Voters 是初始的**投票**成员 ID（仅 ModeRaft）；留空时取 Peers 的全部键。
	//
	// 单独给一个字段是为了支持"peers（地址簿）与投票成员不一致"的场景：
	// 新节点加入时，它的地址簿里有自己（否则连不上任何邻居），但它还不是投票成员。
	Voters []string
	// Transport 可注入自定义传输（测试用内存网络）；为 nil 时由本包创建 TCP 传输。
	Transport raft.Transport
	// Applier 由内核提供，必填。
	Applier Applier
	// Logger 日志器，必填。
	Logger Logger
	// ElectionTimeout / HeartbeatInterval 透传给 Raft；零值用 raft 的默认值。
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	// OnMembershipChange 在成员划分变化后回调（仅 ModeRaft；在 Raft 应用协程内）。
	// 实现必须快速返回；需要重活的调用方应把成员表转交给自己的后台协程。
	OnMembershipChange func(raft.Membership)
}

// Status 是元数据层的只读状态（管理面与 CLI 展示用）。
type Status struct {
	// Mode 是后端模式名（local / raft）。
	Mode string
	// NodeID 是本节点 ID。
	NodeID string
	// Role 是当前角色：单机模式恒为 "single"，集群模式为 leader / follower / candidate。
	Role string
	// Term 是 Raft 任期（单机模式为 0）。
	Term uint64
	// Leader 是已知领导者 ID（单机模式即自己）。
	Leader string
	// CommitIndex / LastApplied 是共识进度。
	CommitIndex uint64
	LastApplied uint64
	// ProposeEntries / FsyncTotal 是元数据 Raft 组的写路径计数（M4 观测；单机模式恒为 0）。
	ProposeEntries uint64
	FsyncTotal     uint64
	// Peers 是投票成员 ID（含自己，已排序）。
	Peers []string
	// Learners 是非投票成员 ID（已排序；没有成员变更时为 nil）。
	Learners []string
	// HasQuorum 表示是否仍与多数派保持联系（单机模式恒为 true）。
	HasQuorum bool
	// AppliedRecords 是状态机累计应用过的变更条数（观测用）。
	AppliedRecords uint64
	// Queues / Exchanges / Bindings / Users 是元数据规模（便于运维一眼看出状态是否一致）。
	Queues    int
	Exchanges int
	Bindings  int
	Users     int
}

// ErrNotLeader 表示写入被拒绝且无法转发（例如 leader 未知、本节点与多数派失联）。
var ErrNotLeader = errors.New("meta: 当前节点无法提交元数据变更（非 leader 且无法转发）")

// ErrLocalMode 表示在单机模式下请求了集群专属操作（如成员变更）。
var ErrLocalMode = errors.New("meta: 单机模式不支持集群成员变更")

// Store 是元数据存储。
type Store struct {
	// 字段由实现文件定义（store.go / fsm.go）。
	impl storeImpl
}

// Open 打开元数据存储：ModeLocal 会加载（或初始化）本地快照，ModeRaft 会启动 Raft 节点并重放日志。
//
// 无论哪种模式，打开完成后都会先调用一次 Applier.RestoreMeta 把完整状态交给内核，
// 之后每条提交的变更再经 Applier.ApplyMeta 增量下发 —— 内核因此不需要自己判断"首次加载"。
func Open(ctx context.Context, opt Options) (*Store, error) { return open(ctx, opt) }

// Write 提交一次元数据变更。payload 会被 JSON 编码后作为日志命令。
//
// 语义：ModeLocal 直接应用；ModeRaft 下 leader 本地提交、follower 转发给 leader。
// 返回 nil 表示该变更已在**本节点**应用完成（集群模式下即已提交并应用）。
func (s *Store) Write(ctx context.Context, op Op, payload any) error {
	return s.impl.write(ctx, op, payload)
}

// State 返回当前元数据快照。
//
// 返回的是**与状态机隔离的副本**：容器（map/slice）都已复制，调用方可以安全地在锁外
// 遍历/索引，不会与状态机的写入竞争。记录值本身与状态机共享（记录进入状态后不再被原地改写）。
func (s *Store) State() State { return s.impl.state() }

// Status 返回元数据层状态。
func (s *Store) Status() Status { return s.impl.status() }

// Membership 返回当前成员划分（单机模式为空）。
func (s *Store) Membership() raft.Membership { return s.impl.membership() }

// AddMember 把一个节点加入集群：先以 learner 身份加入、等它追平后提升为投票成员。
//
// 分两步（而非直接作为 voter 加入）是 Raft 的常规做法：新节点通常没有日志，
// 直接参与表决会在它追平前拖慢（甚至短暂阻塞）集群；先 learner 后提升则不影响可用性。
// 仅集群模式可用；非 leader 会自动转发给 leader。
func (s *Store) AddMember(ctx context.Context, id, addr string) error {
	return s.impl.addMember(ctx, id, addr)
}

// RemoveMember 把一个节点从集群移除（仅集群模式；非 leader 自动转发给 leader）。
func (s *Store) RemoveMember(ctx context.Context, id string) error {
	return s.impl.removeMember(ctx, id)
}

// Close 关闭存储（集群模式会停止 Raft 并落盘）。可重复调用。
func (s *Store) Close() error { return s.impl.close() }

// storeImpl 由 ModeLocal / ModeRaft 两个实现提供；这样 Store 只暴露一层稳定 API。
type storeImpl interface {
	write(ctx context.Context, op Op, payload any) error
	state() State
	status() Status
	membership() raft.Membership
	addMember(ctx context.Context, id, addr string) error
	removeMember(ctx context.Context, id string) error
	close() error
}
