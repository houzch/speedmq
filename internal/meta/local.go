package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/houzch/speedmq/internal/raft"
)

// stateFileName 是单机模式快照的文件名（<Dir>/state.json）。
const stateFileName = "state.json"

// localStore 是单机后端：内存状态 + 每次写入后把完整状态原子落盘。
//
// 为什么整份状态覆盖落盘、而不是追加增量日志：元数据规模小（拓扑与账号，通常几 KB），
// 整份覆盖 + rename 换来一个很强的恢复不变量 —— 磁盘上永远是一份完整可解析的快照，
// 重启路径没有"回放日志、处理半写记录"这些分支（与设计 §5.3.2 的 replace-by-rename 约定一致）。
type localStore struct {
	dir     string
	id      string
	applier Applier
	logger  Logger

	// writeMu 串行化一整次写入（应用 → 落盘 → 回调）。
	// 并发写入若不串行，"后应用的状态"可能先落盘、再被"先应用的状态"覆盖，磁盘就回退了；
	// 它同时让单机路径与 Raft 的"单协程顺序应用"语义保持一致：两次回调的顺序 = 两次变更的顺序。
	writeMu sync.Mutex
	// mu 只保护内存字段，临界区里不做 IO（AGENTS.md §6：不在锁内做慢操作）。
	mu      sync.RWMutex
	st      State
	applied uint64
	closed  bool
}

// openLocal 打开单机后端：有快照就加载，没有就空初始化；两种情况都恰好向内核交付一次完整状态。
func openLocal(opt Options) (*localStore, error) {
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建元数据目录 %s 失败: %w", opt.Dir, err)
	}
	s := &localStore{dir: opt.Dir, id: opt.NodeID, applier: opt.Applier, logger: opt.Logger, st: newState()}

	path := s.statePath()
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		st, derr := decodeState(data)
		if derr != nil {
			// 快照存在但无法解析：宁可启动失败，也不静默按空状态继续 ——
			// 后者会让运维看到"拓扑全没了"却不知道原因，还会被这次启动覆盖掉原文件。
			return nil, fmt.Errorf("解析元数据快照 %s 失败: %w", path, derr)
		}
		s.st = st
	case errors.Is(err, fs.ErrNotExist):
		// 首次启动：保持空状态；下面的 RestoreMeta 会把"现在是空的"明确告诉内核。
	default:
		return nil, fmt.Errorf("读取元数据快照 %s 失败: %w", path, err)
	}

	if err := opt.Applier.RestoreMeta(s.st); err != nil {
		return nil, fmt.Errorf("初始化内核元数据失败: %w", err)
	}
	q, e, b, u := counts(s.st)
	opt.Logger.Info("元数据层已打开", "mode", ModeLocal.String(), "node_id", opt.NodeID, "dir", opt.Dir,
		"queues", q, "exchanges", e, "bindings", b, "users", u)
	return s, nil
}

// statePath 返回单机快照文件路径。
func (s *localStore) statePath() string { return filepath.Join(s.dir, stateFileName) }

// write 应用一次变更：先改内存状态、再原子落盘、最后增量下发给内核。
//
// 顺序上"先状态、后落盘"：落盘失败时磁盘会落后于内存（返回错误、调用方按失败处理），
// 但下一次成功写入会把整份状态重新覆盖，不会长期不一致；反过来"先落盘再改内存"则要在
// 落盘和变更之间再备份一份状态，代价（每次写入全量拷贝）高于它带来的收益。
func (s *localStore) write(_ context.Context, op Op, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("编码元数据记录失败 (op=%s): %w", op, err)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("写入 %s 失败: 元数据存储已关闭", op)
	}
	if err := applyToState(&s.st, op, raw); err != nil {
		s.mu.Unlock()
		return err
	}
	s.applied++
	snapshot, err := json.Marshal(s.st)
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("编码元数据快照失败: %w", err)
	}

	if err := writeFileAtomic(s.statePath(), snapshot); err != nil {
		return fmt.Errorf("落盘元数据快照 %s 失败: %w", s.statePath(), err)
	}
	if err := s.applier.ApplyMeta(op, raw); err != nil {
		s.logger.Error("元数据变更下发内核失败", "op", string(op), "err", err)
		return fmt.Errorf("元数据变更 %s 下发内核失败: %w", op, err)
	}
	return nil
}

// state 返回当前快照的一个**隔离副本**（容器新复制，调用方可安全地在锁外遍历）。
func (s *localStore) state() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneState(s.st)
}

// status 返回单机模式的状态：没有共识，所以角色恒为 single、恒有"多数派"。
func (s *localStore) status() Status {
	s.mu.RLock()
	// counts 读的是 map 长度，必须与状态写入同处读锁内：解锁后再统计会与 Apply 并发。
	q, e, b, u := counts(s.st)
	applied := s.applied
	s.mu.RUnlock()

	// 单机没有集群成员，Peers 只在 NodeID 已知时给出一项，避免对外展示一个空 ID。
	var peers []string
	if s.id != "" {
		peers = []string{s.id}
	}
	return Status{
		Mode:           ModeLocal.String(),
		NodeID:         s.id,
		Role:           "single",
		Leader:         s.id,
		Peers:          peers,
		HasQuorum:      true,
		AppliedRecords: applied,
		Queues:         q,
		Exchanges:      e,
		Bindings:       b,
		Users:          u,
	}
}

// membership 返回空：单机模式没有共识成员。
func (s *localStore) membership() raft.Membership { return raft.Membership{} }

// addMember / removeMember 在单机模式下明确报错：没有 Raft 组可改。
func (s *localStore) addMember(_ context.Context, _, _ string) error { return ErrLocalMode }

func (s *localStore) removeMember(_ context.Context, _ string) error { return ErrLocalMode }

// close 标记关闭并等待在途写入结束；可重复调用。
func (s *localStore) close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// writeFileAtomic 把 data 原子地写到 path：临时文件 → fsync → rename。
//
// 只 fsync 文件本身、不 fsync 目录项：目录句柄在 Windows 上无法 FlushFileBuffers，
// 而 rename 已经保证了"读到旧快照或新快照、不会读到半截"这个对本进程可见的原子性。
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, stateFileName+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		// 失败路径必须自己清理临时文件：否则崩溃目录里会堆满半写快照。
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
