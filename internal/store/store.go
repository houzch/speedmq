package store

import (
	"encoding/binary"
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

// indexEntrySize 是索引记录的长度：seq(8) + seg-offset(8) + seg-len(4) + kind(1)。
const indexEntrySize = 21

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

	seg *logFile
	idx *logFile

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

// openFiles 打开段文件与索引文件（内部已做尾部截断）。
func (s *QueueStore) openFiles() error {
	seg, err := openLogFile(indexlessPath(s.dir, "000001.seg"))
	if err != nil {
		return fmt.Errorf("打开消息段失败: %w", err)
	}
	idx, err := openLogFile(indexPath(s.dir, "000001.idx"))
	if err != nil {
		_ = seg.close()
		return fmt.Errorf("打开队列索引失败: %w", err)
	}
	s.seg, s.idx = seg, idx
	return nil
}

// recover 重放索引与段文件，返回仍未结算（未被 ack）的消息。
//
// 依据设计 5.3.8：索引中仍处于 Publish 状态的记录说明该消息未被确认，
// 重启后回到队头，并在下次投递时标记 redelivered。
func (s *QueueStore) recover() ([]Recovered, error) {
	type entry struct {
		seq  uint64
		off  int64
		size uint32
		ack  bool
	}
	var entries []entry
	if err := s.idx.forEach(func(_ int64, payload []byte) error {
		if len(payload) != indexEntrySize {
			return fmt.Errorf("索引记录长度非法: %d", len(payload))
		}
		entries = append(entries, entry{
			seq:  binary.BigEndian.Uint64(payload[0:8]),
			off:  int64(binary.BigEndian.Uint64(payload[8:16])),
			size: binary.BigEndian.Uint32(payload[16:20]),
			ack:  payload[20] == indexAck,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	alive := make(map[uint64]entry, len(entries))
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

	out := make([]Recovered, 0, len(order))
	for _, seq := range order {
		e := alive[seq]
		payload, err := s.seg.read(e.off)
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
		out = append(out, Recovered{Seq: seq, Message: msg})
	}
	if len(out) > 0 {
		s.log.Info("已从磁盘恢复队列消息", "dir", s.dir, "messages", len(out))
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
			if _, err := s.idx.append(encodeIndexEntry(p.seq, 0, 0, indexAck)); err != nil {
				return err
			}
			continue
		}
		offset, err := s.seg.append(p.payload)
		if err != nil {
			return err
		}
		if _, err := s.idx.append(encodeIndexEntry(p.seq, offset, uint32(len(p.payload)), indexPublish)); err != nil {
			return err
		}
	}

	if err := s.seg.flush(); err != nil {
		return err
	}
	if err := s.idx.flush(); err != nil {
		return err
	}
	if s.opts.Fsync < FsyncBatch {
		// none 不会走到这里（不会创建存储）；os 只写到操作系统，不 fsync
		return nil
	}
	if err := s.seg.sync(); err != nil {
		return err
	}
	return s.idx.sync()
}

// Close 停止刷盘、完成最后一次落盘并关闭文件。可重复调用。
func (s *QueueStore) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
		s.flush() // 收尾刷盘（与刷盘协程通过 flushMu 串行）
	})
	if s.seg == nil {
		return nil
	}
	return errors.Join(s.seg.close(), s.idx.close())
}

// Remove 关闭存储并删除其目录（队列被删除时调用）。
func (s *QueueStore) Remove() error {
	_ = s.Close()
	return removeDir(s.dir)
}

func encodeIndexEntry(seq uint64, offset int64, size uint32, kind uint8) []byte {
	b := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint64(b[0:8], seq)
	binary.BigEndian.PutUint64(b[8:16], uint64(offset))
	binary.BigEndian.PutUint32(b[16:20], size)
	b[20] = kind
	return b
}
