// Package raft 是一个自研的最小 Raft 实现：领导者选举、日志复制、持久化与快照。
//
// 为什么自研而不是引库（见 AGENTS.md §1 的硬约束）：内核承诺"零第三方依赖、Go 侧可离线构建"，
// 而 Raft 是协议而非魔法 —— 自研可控、能针对本项目裁剪（与内核共用一套记录帧格式与日志风格），
// 代价是必须自己把正确性测出来（见 test/unit/raft 的选举/复制/重启/少数派用例）。
// 与设计 §11「内嵌 hashicorp/raft 或 etcd/raft」的偏离已在 M6 交付说明中记录。
//
// 成员变更（M6d）：支持**运行期**增删成员，但有意采用"每次只改一个成员"的简化，
// 不做 joint consensus —— 单成员变更时新旧多数派必然重叠，安全性因此成立。
// 成员分两种：voter（投票、计入多数派、可当选）与 learner（只复制、不投票、不竞选）。
// 新增节点先以 learner 加入、追平后再提升为 voter，避免"新节点拖慢集群"。
//
// 范围（有意不做，避免一次吞下太多）：
//   - **无 joint consensus**：一次只允许一个未提交的配置变更，且不允许一次变更多个成员；
//   - 只实现 Raft 的核心安全规则，不含 PreVote / ReadIndex / 流水线复制等优化；
//   - 快照只做"日志压缩 + 落后节点整体安装"，不做增量快照。
//
// 正确性不变量（实现必须遵守，测试必须覆盖）：
//  1. 任期与投票：同一任期最多投一次；currentTerm / votedFor 先落盘（fsync）再对外应答。
//  2. 选举限制：候选人的日志必须"至少一样新"才可能当选（先比最后一条的任期，再比索引）。
//  3. 日志匹配：AppendEntries 带 prevLogIndex/prevLogTerm，不匹配就拒绝；冲突处截断后追加。
//  4. 提交规则：只有当前任期的条目被多数派复制后才算提交（Raft 论文 Figure 8 的安全性要求）；
//     提交某条意味着它之前的条目一并提交。
//  5. 状态机应用：单协程按索引顺序应用已提交条目，绝不跳号、绝不重复。
package raft

import (
	"context"
	"errors"
	"time"
)

// Role 是节点在任一时刻的角色。
type Role uint8

const (
	// RoleFollower 跟随者（初始角色）。
	RoleFollower Role = iota
	// RoleCandidate 候选人（选举中）。
	RoleCandidate
	// RoleLeader 领导者（唯一可接受写请求的角色）。
	RoleLeader
	// RoleShutdown 已停止。
	RoleShutdown
)

// String 返回角色名（日志与观测输出用）。
func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	case RoleShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

// EntryType 区分日志条目的种类。
//
// 为什么需要它：Raft 必须在把条目交给上层状态机之前，先把**成员变更**条目消化掉
// （更新成员表、注册地址），这类条目不属于上层状态机。零值即普通条目，
// 因此旧日志（没有该字段）解析后仍是普通条目，格式向后兼容。
type EntryType uint8

const (
	// EntryNormal 是交给上层状态机的普通条目。
	EntryNormal EntryType = 0
	// EntryConfChange 是成员变更条目，Data 为 ConfChange 的 JSON。
	EntryConfChange EntryType = 1
)

// Entry 是一条日志条目。Data 的语义由上层状态机决定（本层只保证顺序与不丢）。
type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Data  []byte    `json:"data,omitempty"`
	Type  EntryType `json:"type,omitempty"`
}

// ConfChangeOp 是成员变更的操作类型。
type ConfChangeOp string

const (
	// ConfAddLearner 把节点加入为**非投票**成员（learner）。
	ConfAddLearner ConfChangeOp = "add_learner"
	// ConfPromote 把已存在的 learner 提升为投票成员。
	ConfPromote ConfChangeOp = "promote"
	// ConfRemove 把节点（voter 或 learner）从集群中移除。
	ConfRemove ConfChangeOp = "remove"
)

// ConfChange 是一次成员变更。Addr 是该成员的集群 RPC 地址：
// 它随配置条目一起复制到全体成员，接收方据此注册地址，这样**新加入的节点
// 不需要改任何节点的配置文件**就能被联系上。
type ConfChange struct {
	Op   ConfChangeOp `json:"op"`
	ID   string       `json:"id"`
	Addr string       `json:"addr,omitempty"`
}

// Member 是一个成员及其角色。
type Member struct {
	ID      string `json:"id"`
	Addr    string `json:"addr,omitempty"`
	Learner bool   `json:"learner,omitempty"`
}

// Membership 是当前成员划分（只读视图）。
type Membership struct {
	// Voters 是投票成员 ID（已排序，含本节点时也在其中）。
	Voters []string
	// Learners 是非投票成员 ID（已排序）。
	Learners []string
}

// Status 是 Raft 节点的只读状态快照（管理面与日志用）。
type Status struct {
	// ID 是本节点标识。
	ID string
	// Role 是当前角色。
	Role Role
	// Term 是当前任期。
	Term uint64
	// Leader 是已知的领导者 ID；未知时为空串。
	Leader string
	// CommitIndex 是已提交的最大日志索引。
	CommitIndex uint64
	// LastLogIndex 是本地日志的最后一条索引（含未提交）。
	LastLogIndex uint64
	// LastApplied 是已应用到状态机的最大索引。
	LastApplied uint64
	// SnapshotIndex 是快照覆盖到的最大索引（未被压缩时为 0）。
	SnapshotIndex uint64
	// Peers 是**投票成员**（含自己，按 ID 排序）。多数派由它计算。
	Peers []string
	// Learners 是非投票成员（按 ID 排序）。
	Learners []string
	// Progress 是 leader 视角下各成员已复制的最大索引（非 leader 为空）。
	Progress map[string]uint64
}

// FSM 是 Raft 之上的状态机。
//
// 约定：Apply 会被**单协程按索引顺序**调用；实现必须是确定性的（同一份日志在任何节点得到同一状态），
// 且不得在其中调用 Raft（否则自锁）。Snapshot/Restore 用于日志压缩与节点追赶。
type FSM interface {
	// Apply 应用一条已提交的日志，返回给提案方结果（nil 表示无返回值）。
	Apply(index uint64, data []byte) (any, error)
	// Snapshot 返回状态机的完整快照，用于压缩日志与让落后节点追赶。
	Snapshot() ([]byte, error)
	// Restore 用快照重建状态机（在启动重放前或收到 InstallSnapshot 时调用）。
	Restore(data []byte) error
}

// Transport 是节点间 RPC 的抽象：**一根连接、按方法名复用**。
//
// 只暴露字节流（payload 由调用方自行编码，内核统一用 JSON），这样 Raft 自身不关心
// 序列化格式，元数据转发等上层 RPC 也能复用同一个集群端口，不必再开一个监听。
type Transport interface {
	// Call 向节点 to 发起一次 RPC，返回响应字节。方法未注册、节点不可达等都要返回错误。
	Call(ctx context.Context, to string, method string, payload []byte) ([]byte, error)
	// Serve 注册一个 RPC 处理器。同一个方法重复注册返回错误。
	Serve(method string, handler func(ctx context.Context, from string, payload []byte) ([]byte, error)) error
	// Close 关闭传输层（停止监听、断开连接）。
	Close() error
}

// PeerRegistrar 是 Transport 的**可选**扩展：允许运行期注册/注销成员的网络地址。
//
// 成员变更是运行期发生的，而"节点 ID → 地址"的映射通常来自静态配置；
// 没有这个扩展，新节点就只能靠"改所有节点的配置文件并重启"才能被联系上 ——
// 那正是本里程碑要消除的东西。TCP 传输实现了它；进程内内存网络按 ID 直接寻址，
// 因此不需要（不实现该接口即可，Raft 侧会做类型断言）。
type PeerRegistrar interface {
	// RegisterPeer 注册或更新 id 的地址（空地址忽略）。
	RegisterPeer(id, addr string)
	// UnregisterPeer 注销 id 的地址（成员被移除时调用）。
	UnregisterPeer(id string)
}

// RPC 方法名（Raft 内部使用；上层可注册自己方法名，不得占用 "raft." 前缀）。
const (
	MethodRequestVote     = "raft.request_vote"
	MethodAppendEntries   = "raft.append_entries"
	MethodInstallSnapshot = "raft.install_snapshot"
)

// Options 是构造 Raft 节点的参数。
type Options struct {
	// ID 是本节点标识（集群内唯一）。
	ID string
	// Peers 是初始的**投票**成员 ID（含自己）。长度 1 表示单节点集群（立即成为领导者）。
	//
	// 它只在"本地没有任何已持久化的成员表"时生效（首次引导）。此后成员表由
	// 配置变更条目决定并随快照/状态落盘 —— 否则一次运行期变更会在重启后被打回原形。
	Peers []string
	// Learners 是初始的非投票成员 ID（用于"新节点先以 learner 加入"）。
	// 与 Peers 一样只在首次引导时生效，不得与 Peers 重叠。
	Learners []string
	// Dir 是持久化目录（日志 + 快照 + 任期/投票），必须可写。
	Dir string
	// Transport 是节点间 RPC 通道；为 nil 时按 Fatal 处理（调用方必须提供）。
	Transport Transport
	// FSM 是上层状态机。
	FSM FSM
	// ElectionTimeout 是选举超时基准，实际取值随机化到 [1x, 2x]（避免同时竞选）。
	// 零值用 DefaultElectionTimeout。
	ElectionTimeout time.Duration
	// HeartbeatInterval 是领导者心跳间隔，零值用 DefaultHeartbeatInterval。
	HeartbeatInterval time.Duration
	// SnapshotThreshold 是触发日志压缩的已应用条目数阈值，零值用 DefaultSnapshotThreshold。
	SnapshotThreshold uint64
	// Logger 用于输出选举、复制失败等事件。
	Logger Logger
	// OnMembershipChange 在成员划分**发生实际变化**后回调（在 Raft 的应用协程内）。
	//
	// 实现必须快速返回、不得阻塞、不得回调 Raft（否则自锁）：它只适合"投递通知"，
	// 需要做重活的调用方应把成员表转交给自己的后台协程。集群模式下仲裁队列组
	// 需要据此重建/调整自己的 Raft 组，就是靠这个回调。
	OnMembershipChange func(Membership)
}

// Logger 是 Raft 需要的日志能力（避免直接依赖 *slog.Logger 造成测试替身负担）。
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// 默认参数。
const (
	// DefaultElectionTimeout 是选举超时基准。
	DefaultElectionTimeout = 300 * time.Millisecond
	// DefaultHeartbeatInterval 是心跳间隔（须显著小于选举超时）。
	DefaultHeartbeatInterval = 50 * time.Millisecond
	// DefaultSnapshotThreshold 是日志压缩阈值（已应用条目数）。
	DefaultSnapshotThreshold = 4096
)

// 错误。
var (
	// ErrNotLeader 表示当前节点不是领导者（Propose 只在领导者上有效）。
	ErrNotLeader = errors.New("raft: 本节点不是领导者")
	// ErrStopped 表示节点已停止。
	ErrStopped = errors.New("raft: 节点已停止")
	// ErrNoQuorum 表示当前节点与多数派失去联系（多数派不可达 / 分区中）。
	ErrNoQuorum = errors.New("raft: 与多数派失去联系")
	// ErrDuplicateMethod 表示 RPC 方法重复注册。
	ErrDuplicateMethod = errors.New("raft: RPC 方法重复注册")
	// ErrConfigInFlight 表示上一个成员变更尚未提交（本实现一次只允许一个，无 joint consensus）。
	ErrConfigInFlight = errors.New("raft: 上一个成员变更尚未提交")
	// ErrUnknownMember 表示操作的目标不是当前成员。
	ErrUnknownMember = errors.New("raft: 目标节点不是当前成员")
	// ErrMemberExists 表示目标已经是成员。
	ErrMemberExists = errors.New("raft: 目标节点已是成员")
	// ErrNotLearner 表示目标不是 learner（只有 learner 能被提升为投票成员）。
	ErrNotLearner = errors.New("raft: 目标节点不是 learner")
	// ErrLastVoter 表示该变更会让集群失去全部投票成员（被拒绝，避免永久失去可用性）。
	ErrLastVoter = errors.New("raft: 不能移除最后的投票成员")
)

// Consensus 是 Raft 对外提供的能力集合。
//
// 上层（元数据存储）依赖这个接口而不是 *Node：这样单节点模式可以用内存实现替身，
// 集群模式才注入真实的 *Node，两套路径共用同一段上层代码。
type Consensus interface {
	// Propose 提交一条日志。仅领导者可用，非领导者返回 ErrNotLeader。
	// 返回值为状态机 Apply 的结果；提交失败（失去多数派、节点停止）时返回错误。
	Propose(ctx context.Context, data []byte) (any, error)
	// ChangeMembership 提交一次成员变更。仅领导者可用，非领导者返回 ErrNotLeader。
	// 一次只允许一个未提交的变更（否则返回 ErrConfigInFlight）。
	ChangeMembership(ctx context.Context, cc ConfChange) error
	// Membership 返回当前成员划分。
	Membership() Membership
	// AwaitCatchUp 等待成员 id 的复制进度追上本节点当前提交点（仅领导者可用）。
	// 用于"新节点先以 learner 加入，追平后再提升为 voter"。
	AwaitCatchUp(ctx context.Context, id string) error
	// IsLeader 判断本节点当前是否为领导者。
	IsLeader() bool
	// Leader 返回已知领导者 ID（未知为空串）。
	Leader() string
	// Status 返回只读状态快照。
	Status() Status
	// HasQuorum 判断本节点是否仍与多数派保持联系。
	// 领导者依据最近一轮心跳的应答数；跟随者依据是否在选举超时内听到领导者。
	HasQuorum() bool
	// Start 启动后台协程（选举、复制、应用）。
	Start() error
	// Stop 停止节点并释放资源（幂等）。
	Stop()
}

// MemNetwork 是进程内的"集群网络"，用于测试与单进程多节点验证。
//
// 它支持按节点对切断链路（模拟分区/宕机），这是 pause_minority 等语义能被测试的前提。
type MemNetwork interface {
	// Transport 返回 id 对应的传输实现（同一个 id 重复调用返回同一个实例）。
	Transport(id string) Transport
	// Partition 切断给定的节点分组之间的联系（组内互通，组间完全隔离）；再次调用会重置拓扑。
	Partition(groups ...[]string)
	// Heal 恢复全部链路。
	Heal()
	// Down 让节点 id 彻底不可达（模拟进程被杀）；Up 恢复。
	Down(id string)
	// Up 恢复节点 id 的连通性。
	Up(id string)
}
