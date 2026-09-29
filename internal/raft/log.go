package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// 记录帧格式与内核既有约定保持一致（见 AGENTS.md §8.1 与 internal/store/logfile.go）：
//
//	<payload-len u32 BE><crc32 u32 BE><payload>
//
// 复用同一套格式的理由：崩溃恢复逻辑同源（顺序扫描，遇到首个半写/损坏记录即截断尾部），
// 排障时同一套工具也能读懂两种文件。载荷用 JSON，Entry.Data 会被 base64 编码 —— 多出来的
// 编码开销换来的是"日志文件可直接人读/可直接比对"，对元数据这种低吞吐场景是划算的。
const (
	recordHeaderSize = 8
	// maxRecordSize 是单条记录的合理性上限。损坏文件的长度前缀可能是天文数字，
	// 直接按它分配内存会被一次读取打爆；超限一律判定为无效尾部并截断。
	maxRecordSize = 64 << 20
)

// 持久化文件名。三者职责分离：任期/投票是小而高频的安全状态，
// 日志是主体，快照用于压缩与落后节点追赶。
const (
	stateFileName    = "raft.state"
	logFileName      = "raft.log"
	snapshotFileName = "snapshot.json"
)

// recordFile 是帧格式的只追加文件。
//
// 写入用 WriteAt 而不是缓冲写：Raft 对"应答前必须落盘"有要求（选举安全性），
// 缓冲只会把 flush 时机藏起来；这里把缓冲交给调用方（是否需要 fsync 由语义决定）。
type recordFile struct {
	path  string
	f     *os.File
	valid int64 // 有效字节数（尾部截断后）
}

// openRecordFile 打开（必要时创建）记录文件，并截断尾部半写/损坏记录。
func openRecordFile(path string) (*recordFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	valid, err := truncateRecordTail(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &recordFile{path: path, f: f, valid: valid}, nil
}

// truncateRecordTail 顺序扫描文件、截断尾部无效记录，返回有效字节数。
//
// 这是崩溃恢复的关键：进程在写帧头/载荷中途被杀，文件尾部会留下半条记录；
// 若不截断，后续 append 会接在半条记录之后，导致整个文件从此不可解析。
func truncateRecordTail(f *os.File) (int64, error) {
	var off int64
	hdr := make([]byte, recordHeaderSize)
	for {
		if _, err := f.ReadAt(hdr, off); err != nil {
			break // EOF 或半写的记录头
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		want := binary.BigEndian.Uint32(hdr[4:8])
		if n > maxRecordSize {
			break
		}
		payload := make([]byte, n)
		if _, err := f.ReadAt(payload, off+recordHeaderSize); err != nil {
			break // 半写的载荷
		}
		if crc32.ChecksumIEEE(payload) != want {
			break // CRC 不符：视为尾部损坏
		}
		off += recordHeaderSize + int64(n)
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != off {
		if err := f.Truncate(off); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// append 在文件尾追加一条记录，返回该记录的偏移。
func (r *recordFile) append(payload []byte) (int64, error) {
	off := r.valid
	frame := encodeFrame(payload)
	if _, err := r.f.WriteAt(frame, off); err != nil {
		return 0, err
	}
	r.valid += int64(len(frame))
	return off, nil
}

// sync 把已写入的数据 fsync 到磁盘。
func (r *recordFile) sync() error { return r.f.Sync() }

// truncate 把文件截断到 off（丢弃其后的冲突/半写记录）并落盘。
func (r *recordFile) truncate(off int64) error {
	if off >= r.valid {
		return nil
	}
	if err := r.f.Truncate(off); err != nil {
		return err
	}
	r.valid = off
	return r.f.Sync()
}

// forEach 顺序遍历有效记录（含其文件偏移）。仅在加载阶段调用。
func (r *recordFile) forEach(fn func(offset int64, payload []byte) error) error {
	var off int64
	hdr := make([]byte, recordHeaderSize)
	for off < r.valid {
		if _, err := r.f.ReadAt(hdr, off); err != nil {
			return fmt.Errorf("读取记录头失败(偏移 %d): %w", off, err)
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		want := binary.BigEndian.Uint32(hdr[4:8])
		if n > maxRecordSize {
			return fmt.Errorf("偏移 %d 处记录长度 %d 超出上限", off, n)
		}
		payload := make([]byte, n)
		if _, err := r.f.ReadAt(payload, off+recordHeaderSize); err != nil {
			return fmt.Errorf("读取记录载荷失败(偏移 %d): %w", off, err)
		}
		if crc32.ChecksumIEEE(payload) != want {
			return fmt.Errorf("偏移 %d 处记录 CRC 校验失败", off)
		}
		if err := fn(off, payload); err != nil {
			return err
		}
		off += recordHeaderSize + int64(n)
	}
	return nil
}

// close 关闭文件。因写入未使用用户态缓冲，直接关闭即可。
func (r *recordFile) close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// encodeFrame 把载荷打成一条记录帧。
func encodeFrame(payload []byte) []byte {
	frame := make([]byte, recordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[recordHeaderSize:], payload)
	return frame
}

// writeRecordFile 用给定载荷重建整个记录文件，返回各记录的偏移。
//
// 先写临时文件再 rename：同目录内 rename 是原子的，崩溃后只会看到"旧文件"或"新文件"，
// 不会看到半写状态。这也是日志压缩（丢弃快照覆盖的前缀）唯一安全的做法 ——
// 原地删除文件头部无法实现，只能重建。
func writeRecordFile(path string, payloads [][]byte) ([]int64, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	offs := make([]int64, len(payloads))
	var off int64
	for i, p := range payloads {
		offs[i] = off
		frame := encodeFrame(p)
		if _, err := f.WriteAt(frame, off); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return nil, err
		}
		off += int64(len(frame))
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	syncDirBestEffort(filepath.Dir(path))
	return offs, nil
}

// syncDirBestEffort 尽力把目录项刷盘（rename 之后需要）。
//
// 目录 fsync 在部分平台（如 Windows）会直接报错，这是平台已知限制：失败不阻断流程，
// 代价仅限"极端掉电时 rename 可能未持久化"—— 那时读到的是旧文件，仍是完整文件，不会错乱。
func syncDirBestEffort(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}

// persistedState 是 raft.state 中的一条记录。
//
// 该文件是**只追加**的：每次任期/投票变化追加一条，加载时取最后一条有效记录。
// 好处是无需 rename 就能做到崩溃安全，且变更频率低（每个任期至多几次），文件不会膨胀。
//
// Members 也放在这里：成员表同样"取最后一条有效记录"即可，而它与任期/投票一样
// 属于"必须在重启后原样恢复"的状态（重启后若退回配置文件的初始成员表，
// 一次运行期变更就白做了）。
type persistedState struct {
	Term     uint64   `json:"term"`
	VotedFor string   `json:"voted_for"`
	Members  []Member `json:"members,omitempty"`
}

// persistedSnapshot 是 snapshot.json 的内容。
//
// Members 随快照一起走：快照压缩会丢掉日志前缀，因此日志里的配置变更条目可能
// 不再存在 —— 落后节点安装快照时必须同时拿到成员表，否则它会用配置文件的
// 初始成员表参与共识，算出错误的多数派。
type persistedSnapshot struct {
	LastIncludedIndex uint64   `json:"last_included_index"`
	LastIncludedTerm  uint64   `json:"last_included_term"`
	Data              []byte   `json:"data"`
	Members           []Member `json:"members,omitempty"`
}

// raftStorage 是 Raft 的持久化层，聚合任期/投票、日志条目与状态机快照。
//
// 所有方法都必须在 Node 主锁保护下调用（Raft 要求"状态变更与落盘"是可串行化的）。
type raftStorage struct {
	dir   string
	state *recordFile
	log   *recordFile
}

// openStorage 打开（必要时创建）Raft 目录与其中的文件。
func openStorage(dir string) (*raftStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("创建 raft 目录失败: %w", err)
	}
	st, err := openRecordFile(filepath.Join(dir, stateFileName))
	if err != nil {
		return nil, fmt.Errorf("打开 %s 失败: %w", stateFileName, err)
	}
	lg, err := openRecordFile(filepath.Join(dir, logFileName))
	if err != nil {
		_ = st.close()
		return nil, fmt.Errorf("打开 %s 失败: %w", logFileName, err)
	}
	return &raftStorage{dir: dir, state: st, log: lg}, nil
}

// loadState 返回最后一条有效的任期/投票记录；文件为空时返回零值。
func (s *raftStorage) loadState() (persistedState, error) {
	var out persistedState
	err := s.state.forEach(func(_ int64, payload []byte) error {
		var ps persistedState
		if err := json.Unmarshal(payload, &ps); err != nil {
			// 单条记录解析失败不阻断启动：取上一条有效记录即可（任期回退到更早的值是安全的，
			// 因为回退只会导致本节点用更小的任期参与，最终会被更高任期纠正）。
			return nil
		}
		out = ps
		return nil
	})
	if err != nil {
		return persistedState{}, err
	}
	return out, nil
}

// saveState 追加并 fsync 任期/投票/成员表。
//
// 必须是"先 fsync 再对外应答"：否则崩溃后本节点可能在同一任期投出第二票，
// 破坏"同任期只投一次"这一选举安全性的根基。
func (s *raftStorage) saveState(term uint64, votedFor string, members []Member) error {
	b, err := json.Marshal(persistedState{Term: term, VotedFor: votedFor, Members: members})
	if err != nil {
		return err
	}
	if _, err := s.state.append(b); err != nil {
		return err
	}
	return s.state.sync()
}

// loadEntries 顺序加载全部日志条目及其文件偏移。
func (s *raftStorage) loadEntries() ([]Entry, []int64, error) {
	var ents []Entry
	var offs []int64
	err := s.log.forEach(func(off int64, payload []byte) error {
		var e Entry
		if err := json.Unmarshal(payload, &e); err != nil {
			return fmt.Errorf("解析日志条目失败(偏移 %d): %w", off, err)
		}
		ents = append(ents, e)
		offs = append(offs, off)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return ents, offs, nil
}

// rewriteLog 用给定载荷重建 raft.log（日志压缩用），返回新文件中各记录的偏移。
//
// 必须先关闭旧句柄：Windows 不允许 rename 覆盖一个仍被打开的文件。
// 若重建失败，尽力恢复旧句柄，避免节点此后完全不可用。
func (s *raftStorage) rewriteLog(payloads [][]byte) ([]int64, error) {
	if err := s.log.close(); err != nil {
		return nil, err
	}
	offs, err := writeRecordFile(filepath.Join(s.dir, logFileName), payloads)
	if err != nil {
		if rf, rerr := openRecordFile(filepath.Join(s.dir, logFileName)); rerr == nil {
			s.log = rf
		}
		return nil, err
	}
	rf, err := openRecordFile(filepath.Join(s.dir, logFileName))
	if err != nil {
		return nil, err
	}
	s.log = rf
	return offs, nil
}

// saveSnapshot 原子写入快照文件。
func (s *raftStorage) saveSnapshot(snap persistedSnapshot) error {
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, snapshotFileName)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
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
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDirBestEffort(s.dir)
	return nil
}

// loadSnapshot 读取快照；不存在时返回 has=false。
func (s *raftStorage) loadSnapshot() (persistedSnapshot, bool, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, snapshotFileName))
	if errors.Is(err, os.ErrNotExist) {
		return persistedSnapshot{}, false, nil
	}
	if err != nil {
		return persistedSnapshot{}, false, err
	}
	var snap persistedSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return persistedSnapshot{}, false, fmt.Errorf("解析快照文件失败: %w", err)
	}
	return snap, true, nil
}

// close 关闭持久化文件（幂等）。
func (s *raftStorage) close() error {
	var first error
	if s.state != nil {
		if err := s.state.close(); err != nil && first == nil {
			first = err
		}
	}
	if s.log != nil {
		if err := s.log.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// raftLog 是日志的内存镜像 + 落盘封装。
//
// ents[i] 恒对应索引 ents[0].Index+i；ents[0] 是"快照哨兵"，记录快照覆盖到的
// (index, term)，本身不落盘。offs[i] 是 ents[i] 在 raft.log 中的偏移（哨兵为 -1），
// 用于 O(1) 截断冲突后缀。
type raftLog struct {
	st   *raftStorage
	ents []Entry
	offs []int64
}

// newRaftLog 由文件内容构造日志，并把哨兵置于快照位置。
func newRaftLog(st *raftStorage, snapIndex, snapTerm uint64) (*raftLog, error) {
	loaded, offs, err := st.loadEntries()
	if err != nil {
		return nil, err
	}
	// 丢弃已被快照覆盖的前缀：快照落盘与日志压缩不是一次原子操作，
	// 崩溃后可能留下"快照已前进、日志还没压缩"的中间态，这里统一收敛。
	start := 0
	for start < len(loaded) && loaded[start].Index <= snapIndex {
		start++
	}
	ents := make([]Entry, 0, len(loaded)-start+1)
	ents = append(ents, Entry{Index: snapIndex, Term: snapTerm})
	ents = append(ents, loaded[start:]...)
	loffs := make([]int64, 0, len(ents))
	loffs = append(loffs, -1)
	loffs = append(loffs, offs[start:]...)
	return &raftLog{st: st, ents: ents, offs: loffs}, nil
}

// snapIndex 是快照覆盖到的最大索引（无快照时为 0）。
func (l *raftLog) snapIndex() uint64 { return l.ents[0].Index }

// snapTerm 是快照最后一条的任期。
func (l *raftLog) snapTerm() uint64 { return l.ents[0].Term }

// lastIndex 是本地日志最后一条索引（含未提交）。
func (l *raftLog) lastIndex() uint64 { return l.ents[len(l.ents)-1].Index }

// lastTerm 是本地日志最后一条的任期。
func (l *raftLog) lastTerm() uint64 { return l.ents[len(l.ents)-1].Term }

// termAt 返回索引处的任期；索引已被快照覆盖或超前时返回 ok=false。
func (l *raftLog) termAt(index uint64) (uint64, bool) {
	if index < l.ents[0].Index || index > l.lastIndex() {
		return 0, false
	}
	return l.ents[index-l.ents[0].Index].Term, true
}

// entryAt 返回索引处的条目。
func (l *raftLog) entryAt(index uint64) (Entry, bool) {
	if index < l.ents[0].Index || index > l.lastIndex() {
		return Entry{}, false
	}
	return l.ents[index-l.ents[0].Index], true
}

// entriesFrom 返回从 index 起、至多 max 条日志（max<=0 表示不限）。
func (l *raftLog) entriesFrom(index uint64, max int) []Entry {
	base := l.ents[0].Index
	if index <= base {
		index = base + 1
	}
	pos := int(index - base)
	if pos >= len(l.ents) {
		return nil
	}
	end := len(l.ents)
	if max > 0 && pos+max < end {
		end = pos + max
	}
	out := make([]Entry, end-pos)
	copy(out, l.ents[pos:end])
	return out
}

// count 返回自快照以来（不含哨兵）的条目数。
func (l *raftLog) count() int { return len(l.ents) - 1 }

// append 追加一条日志（仅写入，不 fsync；是否需要落盘由调用方决定）。
func (l *raftLog) append(e Entry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	off, err := l.st.log.append(b)
	if err != nil {
		return err
	}
	l.ents = append(l.ents, e)
	l.offs = append(l.offs, off)
	return nil
}

// sync 把日志 fsync 到磁盘。
func (l *raftLog) sync() error { return l.st.log.sync() }

// truncateFrom 丢弃 index 及其之后的全部条目（要求 index <= lastIndex+1）。
//
// 用于 AppendEntries 的一致性检查：与 leader 冲突的后缀必须整段丢弃后重写。
// 快照覆盖范围内的前缀不可截断（那部分已经"固化成状态机状态"了）。
func (l *raftLog) truncateFrom(index uint64) error {
	base := l.ents[0].Index
	if index <= base {
		return nil
	}
	pos := int(index - base)
	if pos >= len(l.ents) {
		return nil
	}
	if err := l.st.log.truncate(l.offs[pos]); err != nil {
		return err
	}
	l.ents = l.ents[:pos]
	l.offs = l.offs[:pos]
	return nil
}

// compact 丢弃快照覆盖的前缀，并把哨兵推进到 (throughIndex, throughTerm)。
func (l *raftLog) compact(throughIndex, throughTerm uint64) error {
	base := l.ents[0].Index
	if throughIndex <= base {
		return nil
	}
	last := l.lastIndex()
	if throughIndex > last {
		// 防御：快照不可能超过本地最后一条（快照只对已应用的条目做）。
		throughIndex, throughTerm = last, l.lastTerm()
	}
	pos := int(throughIndex - base)
	kept := l.ents[pos:] // kept[0].Index == throughIndex
	payloads := make([][]byte, 0, len(kept)-1)
	for _, e := range kept[1:] {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		payloads = append(payloads, b)
	}
	offs, err := l.st.rewriteLog(payloads)
	if err != nil {
		return err
	}
	l.ents = append([]Entry{{Index: throughIndex, Term: throughTerm}}, kept[1:]...)
	l.offs = append([]int64{-1}, offs...)
	return nil
}

// reset 丢弃全部日志并把哨兵置到 (index, term)（安装快照且本地无匹配后缀时使用）。
func (l *raftLog) reset(index, term uint64) error {
	if _, err := l.st.rewriteLog(nil); err != nil {
		return err
	}
	l.ents = []Entry{{Index: index, Term: term}}
	l.offs = []int64{-1}
	return nil
}
