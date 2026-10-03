package store

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// ErrClosed 表示存储已关闭。
var ErrClosed = errors.New("队列存储已关闭")

// indexEntrySize 是索引记录的长度：seq(8) + seg(4) + seg-offset(8) + seg-len(4) + kind(1)。
//
// M8-1 加了 seg 字段：段轮转之后只靠偏移无法定位消息（偏移是**段内**偏移）。
const indexEntrySize = 25

// legacyIndexEntrySize 是引入段号之前的索引记录长度（seq + offset + len + kind）。
// 旧实现只有一个段，因此读到旧格式时等价于 seg=1，升级不需要迁移脚本。
const legacyIndexEntrySize = 21

// segmentMaxBytes 是单个段文件的大小上限，超过就封口并切到新段。
//
// 8 MiB 是"段不过于零碎"与"回收粒度不过粗"之间的折中：段越小回收越及时，
// 但文件数、打开开销与索引里的段号离散度都更高。
const segmentMaxBytes = 8 << 20

// 索引记录状态。
const (
	indexPublish uint8 = 1
	indexAck     uint8 = 2
)

// Recovered 是从磁盘恢复出来的一条消息。
type Recovered struct {
	// Seq 是该消息在持久化日志中的序号（单调递增，队列内唯一）。
	Seq uint64
	// Message 是消息本体；恢复出来的消息在上层应标记为 redelivered。
	Message *plugin.Message
}

// Options 是存储层选项。
type Options struct {
	// Fsync 是落盘档位；FsyncNone 时不会创建任何存储。
	Fsync FsyncLevel
	// FlushInterval 是兜底刷盘间隔，限制"消息在内存里最多待多久"。
	FlushInterval time.Duration
	// SegmentMaxBytes 是单个段文件的大小上限，0 表示用默认值（segmentMaxBytes）。
	//
	// 做成可配置主要是为了让测试能把段压到很小、从而验证轮转与回收；
	// 生产上一般不需要调整。
	SegmentMaxBytes int64
}

// segmentLimit 返回生效的段大小上限。
func (s *QueueStore) segmentLimit() int64 {
	if s.opts.SegmentMaxBytes > 0 {
		return s.opts.SegmentMaxBytes
	}
	return segmentMaxBytes
}

// Commit 是一次持久化写入的完成凭据。
//
// 语义：Wait 返回时，这条消息已按当前 fsync 档位完成落盘
// （os = 已 write 到操作系统；batch / always = 已 fsync）。
type Commit struct {
	once sync.Once
	done chan struct{}
	err  error
}

func newCommit() *Commit { return &Commit{done: make(chan struct{})} }

func (c *Commit) complete(err error) {
	if c == nil {
		return
	}
	c.once.Do(func() {
		c.err = err
		close(c.done)
	})
}

// Wait 阻塞直到该写入完成；c 为 nil 时直接返回 nil。
func (c *Commit) Wait() error {
	if c == nil {
		return nil
	}
	<-c.done
	return c.err
}

// pendingRecord 是一条待刷盘的记录。
type pendingRecord struct {
	// ack 为 true 表示这是一条"消息已离开队列"的索引标记，不写段文件。
	ack     bool
	seq     uint64
	payload []byte
	commit  *Commit
}

// QueueStore 是单个队列的持久化存储：一个段文件（消息体）+ 一个索引文件（队列索引）。
//
// 写入路径：Append / Ack 只把记录放进内存缓冲并唤醒刷盘协程，返回的 Commit 供上层
// 等待落盘完成 —— 这样协议层可以在"回 basic.ack 之前"精确地等到持久化。
type QueueStore struct {
	dir  string
	opts Options
	log  *slog.Logger

	idx *logFile

	// segs / activeSeg 是段管理状态：activeSeg 是当前**可写**的段，
	// segs 里的其余段都已封口（只有封口段才可能被回收）。由 flushMu 保护。
	segs      map[uint32]*segmentInfo
	activeSeg uint32
	// liveSeg 追踪"未 ack 的 seq 落在哪个段"，Ack 时据此递减该段的存活计数。
	liveSeg map[uint64]uint32

	// flushMu 串行化刷盘，避免刷盘协程与 Close 的收尾刷盘并发写同一个文件。
	flushMu sync.Mutex

	mu      sync.Mutex
	pending []*pendingRecord
	closed  bool

	notify chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newQueueStore(dir string, opts Options, log *slog.Logger) *QueueStore {
	return &QueueStore{
		dir:    dir,
		opts:   opts,
		log:    log,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Dir 返回存储目录。
func (s *QueueStore) Dir() string { return s.dir }

// openFiles 只打开索引文件；段文件在 recover 里按需打开（详见 segment.go）。
func (s *QueueStore) openFiles() error {
	idx, err := openLogFile(indexPath(s.dir, "000001.idx"))
	if err != nil {
		return fmt.Errorf("打开队列索引失败: %w", err)
	}
	s.idx = idx
	s.segs = map[uint32]*segmentInfo{}
	s.liveSeg = map[uint64]uint32{}
	return nil
}

// recover 重放索引与段文件，返回仍未结算（未被 ack）的消息。
//
// 依据设计 5.3.8：索引中仍处于 Publish 状态的记录说明该消息未被确认，
// 重启后回到队头，并在下次投递时标记 redelivered。
func (s *QueueStore) recover() ([]Recovered, error) {
	var entries []indexEntry
	if err := s.idx.forEach(func(_ int64, payload []byte) error {
		e, err := decodeIndexEntry(payload)
		if err != nil {
			return err
		}
		entries = append(entries, e)
		return nil
	}); err != nil {
		return nil, err
	}

	// 仍存活的消息 = 最后一次 publish 之后没有对应 ack 的那些。
	alive := make(map[uint64]indexEntry, len(entries))
	var order []uint64
	for _, e := range entries {
		if e.ack {
			delete(alive, e.seq)
			continue
		}
		if _, ok := alive[e.seq]; !ok {
			order = append(order, e.seq)
		}
		alive[e.seq] = e
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	// 只打开"还有存活消息"的段；没有存活消息的段文件在下面统一清理。
	out := make([]Recovered, 0, len(order))
	// unreadable 记录"打不开"的段：它们绝不能进入删除集合 —— 打开失败可能是瞬时的
	// （fd 耗尽、权限/IO 抖动），若被当成"陈旧段"删掉，段内仍存活的消息就永久丢失了。
	unreadable := make(map[uint32]struct{})
	for _, seq := range order {
		e := alive[seq]
		seg, ok := s.segs[e.seg]
		if !ok {
			opened, err := openSegment(s.dir, e.seg)
			if err != nil {
				s.log.Warn("打开段失败，保留该段待下次重试（本次跳过其消息）",
					"dir", s.dir, "segment", e.seg, "err", err)
				s.segs[e.seg] = nil // 占位：避免对同一段反复尝试
				unreadable[e.seg] = struct{}{}
				continue
			}
			seg = opened
			s.segs[e.seg] = seg
		}
		if seg == nil {
			continue
		}
		payload, err := seg.file.read(e.off)
		if err != nil {
			s.log.Warn("恢复消息失败，已跳过", "dir", s.dir, "seq", seq, "err", err)
			continue
		}
		if uint32(len(payload)) != e.size {
			s.log.Warn("恢复消息长度与索引不一致，已跳过", "dir", s.dir, "seq", seq)
			continue
		}
		msg, err := decodeMessage(payload)
		if err != nil {
			s.log.Warn("恢复消息解码失败，已跳过", "dir", s.dir, "seq", seq, "err", err)
			continue
		}
		seg.live++
		s.liveSeg[seq] = e.seg
		out = append(out, Recovered{Seq: seq, Message: msg})
	}

	// 打不开的段从运行期集合里丢掉，但**保留其文件**（进 keep）——它们不是"陈旧段"，
	// 只是这次读不了；同时记下"必须保留"的段（有存活消息的）。
	keep := make(map[uint32]struct{}, len(s.segs))
	maxSeg := uint32(0)
	for id, seg := range s.segs {
		if seg == nil {
			delete(s.segs, id)
			if _, bad := unreadable[id]; bad {
				keep[id] = struct{}{}
			}
			continue
		}
		keep[id] = struct{}{}
		if id > maxSeg {
			maxSeg = id
		}
	}
	if maxSeg == 0 {
		maxSeg = 1
	}
	s.activeSeg = maxSeg
	// 除最大段外一律封口；最大段保持可写，重启后继续往它追加，避免每次重启都产生新段。
	for id, seg := range s.segs {
		seg.sealed = id != maxSeg
	}
	// 清掉没有存活消息的段文件（上次消费空但还没来得及回收的、或被打断留下的）。
	s.removeStaleSegments(keep)

	if len(out) > 0 {
		s.log.Info("已从磁盘恢复队列消息", "dir", s.dir, "messages", len(out), "segments", len(s.segs))
	}
	return out, nil
}

// startFlusher 启动刷盘协程。
func (s *QueueStore) startFlusher() {
	interval := s.opts.FlushInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-s.notify:
			case <-ticker.C:
			}
			s.flush()
		}
	}()
}

// Append 追加一条消息，返回等待落盘的凭据。
func (s *QueueStore) Append(seq uint64, msg *plugin.Message) (*Commit, error) {
	payload, err := encodeMessage(msg)
	if err != nil {
		return nil, err
	}
	c := newCommit()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.pending = append(s.pending, &pendingRecord{seq: seq, payload: payload, commit: c})
	s.mu.Unlock()

	s.signal()
	return c, nil
}

// Ack 记录"该序号的消息已离开队列"。
//
// 用追加一条标记记录来表达删除（设计 5.3.7）：不做原地删除与压缩，
// 段文件的回收交给后台任务，避免随机写。
func (s *QueueStore) Ack(seq uint64) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.pending = append(s.pending, &pendingRecord{ack: true, seq: seq})
	s.mu.Unlock()

	s.signal()
	return nil
}

func (s *QueueStore) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// flush 把内存缓冲写入文件，并按档位 fsync，最后完成所有等待者。
func (s *QueueStore) flush() {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()
	if len(pending) == 0 {
		return
	}

	err := s.writeRecords(pending)
	for _, p := range pending {
		p.commit.complete(err)
	}
	if err != nil {
		s.log.Error("队列存储刷盘失败", "dir", s.dir, "records", len(pending), "err", err)
	}
}

func (s *QueueStore) writeRecords(pending []*pendingRecord) error {
	for _, p := range pending {
		if p.ack {
			if err := s.applyAck(p.seq); err != nil {
				return err
			}
			continue
		}
		if err := s.applyPublish(p); err != nil {
			return err
		}
	}

	// flush **所有**打开的段：一批 pending 里可能触发段轮转，
	// 只 flush 活跃段会漏掉刚刚封口的那个，它的数据就迟迟不落到操作系统（崩溃即丢）。
	if err := s.idx.flush(); err != nil {
		return err
	}
	if err := s.flushSegments(func(seg *segmentInfo) error { return seg.file.flush() }); err != nil {
		return err
	}
	// 索引压缩与 fsync 档位**无关**：它是"磁盘占用有界"的必要条件。
	// 放在档位分支之前，否则 os 档位（多数生产配置）下索引会一直只增不减。
	// 顺带一提，压缩可能会替换 s.idx，因此必须在下面的 s.idx.sync() 之前完成。
	if err := s.maybeCompactIndex(); err != nil {
		return err
	}
	if s.opts.Fsync < FsyncBatch {
		// none 不会走到这里（不创建存储）；os 只写到操作系统，不 fsync
		return nil
	}
	// 顺序很关键：先让段数据持久化，再 sync 索引。索引是"消息存在"的判据，
	// 反过来（索引先落盘）会在两者之间崩溃时留下"索引可见、数据未落盘"的窗口。
	if err := s.flushSegments(func(seg *segmentInfo) error { return seg.file.sync() }); err != nil {
		return err
	}
	return s.idx.sync()
}

// flushSegments 对全部打开的段执行同一个动作（flush 或 sync）。
func (s *QueueStore) flushSegments(fn func(*segmentInfo) error) error {
	for _, seg := range s.segs {
		if seg == nil {
			continue
		}
		if err := fn(seg); err != nil {
			return err
		}
	}
	return nil
}

// applyPublish 落一条消息：必要时先轮转段，再写段与索引。
func (s *QueueStore) applyPublish(p *pendingRecord) error {
	if err := s.ensureActiveSegment(); err != nil {
		return err
	}
	cur := s.segs[s.activeSeg]
	// 到达上限就封口换新段。只在段非空时轮转，否则"单条消息大于上限"会让每次写入都换段。
	if cur.size > 0 && cur.size+int64(len(p.payload)) > s.segmentLimit() {
		if err := s.rollSegment(); err != nil {
			return err
		}
		cur = s.segs[s.activeSeg]
	}
	offset, err := cur.file.append(p.payload)
	if err != nil {
		return err
	}
	cur.size = cur.file.size()
	if _, err := s.idx.append(encodeIndexEntry(indexEntry{
		seq: p.seq, seg: s.activeSeg, off: offset, size: uint32(len(p.payload)),
	})); err != nil {
		return err
	}
	cur.live++
	s.liveSeg[p.seq] = s.activeSeg
	return nil
}

// applyAck 记录"该序号的消息已离开队列"，并在段被清空时回收它。
func (s *QueueStore) applyAck(seq uint64) error {
	if _, err := s.idx.append(encodeIndexEntry(indexEntry{seq: seq, ack: true})); err != nil {
		return err
	}
	segID, ok := s.liveSeg[seq]
	if !ok {
		return nil // 该消息不在磁盘上（例如非持久消息），无需递减
	}
	delete(s.liveSeg, seq)
	seg, ok := s.segs[segID]
	if !ok || seg == nil {
		return nil
	}
	seg.live--
	// 只有"已封口且已清空"的段才回收：活跃段还要继续写，删掉它会丢消息。
	if seg.live <= 0 && seg.sealed {
		s.collectSegment(segID)
	}
	return nil
}

// Close 停止刷盘、完成最后一次落盘并关闭全部文件。可重复调用。
func (s *QueueStore) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
		s.flush() // 收尾刷盘（与刷盘协程通过 flushMu 串行）
	})
	if s.idx == nil {
		return nil
	}
	// 与刷盘互斥：避免一边关闭段文件、一边还有写入落在它上面。
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	errs := make([]error, 0, len(s.segs)+1)
	for _, seg := range s.segs {
		if seg != nil {
			errs = append(errs, seg.file.close())
		}
	}
	s.segs = map[uint32]*segmentInfo{}
	errs = append(errs, s.idx.close())
	return errors.Join(errs...)
}

// Remove 关闭存储并删除其目录（队列被删除时调用）。
func (s *QueueStore) Remove() error {
	_ = s.Close()
	return removeDir(s.dir)
}

// 索引记录的编解码见 segment.go（M8-1 加了段号，编解码也随之移到那里）。
