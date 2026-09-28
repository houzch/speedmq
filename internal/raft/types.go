// Package raft 是一个自研的最小 Raft 实现：领导者选举、日志复制、持久化与快照。
//
// 为什么自研而不是引库（见 AGENTS.md §1 的硬约束）：内核承诺"零第三方依赖、Go 侧可离线构建"，
// 而 Raft 是协议而非魔法 —— 自研可控、能针对本项目裁剪（与内核共用一套记录帧格式与日志风格），
// 代价是必须自己把正确性测出来（见 test/unit/raft 的选举/复制/重启/少数派用例）。
// 与设计 §11「内嵌 hashicorp/raft 或 etcd/raft」的偏离已在 M6 交付说明中记录。
//
// 本期范围（有意不做，避免一次吞下太多）：
//   - **静态成员**：投票成员来自 Options.Peers，不支持运行期成员变更（无 joint consensus）；
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

// Entry 是一条日志条目。Data 的语义由上层状态机决定（本层只保证顺序与不丢）。
type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
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
	// Peers 是全部投票成员（含自己，按 ID 排序）。
	Peers []string
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
	// Peers 是全部投票成员 ID（含自己）。长度 1 表示单节点集群（立即成为领导者）。
	Peers []string
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
)

// Consensus 是 Raft 对外提供的能力集合。
//
// 上层（元数据存储）依赖这个接口而不是 *Node：这样单节点模式可以用内存实现替身，
// 集群模式才注入真实的 *Node，两套路径共用同一段上层代码。
type Consensus interface {
	// Propose 提交一条日志。仅领导者可用，非领导者返回 ErrNotLeader。
	// 返回值为状态机 Apply 的结果；提交失败（失去多数派、节点停止）时返回错误。
	Propose(ctx context.Context, data []byte) (any, error)
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
