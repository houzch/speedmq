package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// 本文件是 M8-1「段轮转与空间回收」的实现。
//
// 早期实现只有一个段文件、Ack 只追加一条索引标记，消息体永远不会被回收 ——
// 长期运行的队列会把磁盘吃满。这里补上三件事：**段按大小轮转**、
// **段内消息全部确认后整段删除**、**索引随之压缩重写**（否则索引自己也会只增不减）。
//
// 回收的安全性依赖一条不变式：
//
//	只有"已封口（不再写入）且段内所有消息都已 ack"的段才会被删除。
//
// 已删除的段在索引里可能仍留有历史记录，但那些记录对应的消息都已经离开队列，
// 恢复时会自然跳过它们；索引压缩时也会把它们一并丢掉。

// segmentInfo 是一个段文件的运行期状态。
type segmentInfo struct {
	file   *logFile
	size   int64 // 已写入字节数（含记录头）
	live   int   // 该段里尚未 ack 的消息数
	sealed bool  // 已封口：不再写入；只有封口的段才可能被回收
}

// segmentName 返回段文件名，如 000007.seg。
func segmentName(id uint32) string { return fmt.Sprintf("%06d.seg", id) }

// segmentPath 返回段文件的路径。
func segmentPath(dir string, id uint32) string { return indexlessPath(dir, segmentName(id)) }

// indexEntry 是一条解码后的索引记录。
type indexEntry struct {
	seq  uint64
	seg  uint32
	off  int64
	size uint32
	ack  bool
}

// decodeIndexEntry 解析一条索引记录。
//
// 兼容 21 字节的旧格式（没有段号）：旧实现只有一个段，等价于 seg=1，
// 因此升级后不需要迁移脚本，旧数据仍能原样读出来。
func decodeIndexEntry(b []byte) (indexEntry, error) {
	switch len(b) {
	case indexEntrySize:
		return indexEntry{
			seq:  binary.BigEndian.Uint64(b[0:8]),
			seg:  binary.BigEndian.Uint32(b[8:12]),
			off:  int64(binary.BigEndian.Uint64(b[12:20])),
			size: binary.BigEndian.Uint32(b[20:24]),
			ack:  b[24] == indexAck,
		}, nil
	case legacyIndexEntrySize:
		return indexEntry{
			seq:  binary.BigEndian.Uint64(b[0:8]),
			seg:  1,
			off:  int64(binary.BigEndian.Uint64(b[8:16])),
			size: binary.BigEndian.Uint32(b[16:20]),
			ack:  b[20] == indexAck,
		}, nil
	default:
		return indexEntry{}, fmt.Errorf("索引记录长度非法: %d", len(b))
	}
}

// encodeIndexEntry 编码一条索引记录（总是写新格式）。
func encodeIndexEntry(e indexEntry) []byte {
	b := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint64(b[0:8], e.seq)
	binary.BigEndian.PutUint32(b[8:12], e.seg)
	binary.BigEndian.PutUint64(b[12:20], uint64(e.off))
	binary.BigEndian.PutUint32(b[20:24], e.size)
	if e.ack {
		b[24] = indexAck
	} else {
		b[24] = indexPublish
	}
	return b
}

// openSegment 打开（必要时创建）一个段文件。
func openSegment(dir string, id uint32) (*segmentInfo, error) {
	f, err := openLogFile(segmentPath(dir, id))
	if err != nil {
		return nil, err
	}
	return &segmentInfo{file: f, size: f.size()}, nil
}

// ensureActiveSegment 保证存在可写的活跃段，必要时创建。调用方需持有 flushMu。
func (s *QueueStore) ensureActiveSegment() error {
	if cur, ok := s.segs[s.activeSeg]; ok && !cur.sealed {
		return nil
	}
	id := s.activeSeg
	if id == 0 {
		id = 1
	}
	seg, err := openSegment(s.dir, id)
	if err != nil {
		return err
	}
	s.segs[id] = seg
	s.activeSeg = id
	return nil
}

// rollSegment 封口当前活跃段并切到新段。调用方需持有 flushMu。
func (s *QueueStore) rollSegment() error {
	prev := s.activeSeg
	if cur, ok := s.segs[prev]; ok {
		cur.sealed = true
	}
	next := prev
	if next == 0 {
		next = 1
	} else {
		next++
	}
	seg, err := openSegment(s.dir, next)
	if err != nil {
		return err
	}
	s.segs[next] = seg
	s.activeSeg = next

	// 封口时旧段可能**已经是空的**（消息先被确认、段随后才写满，即"边写边确认"的负载）——
	// 这种情况下不会再有任何 Ack 事件指向它，回收检查必须在这里补一次。
	// 这是实测踩到的坑：漏掉这一步的话，这类队列的段一个都不会被回收。
	if old, ok := s.segs[prev]; ok && old.live <= 0 {
		s.collectSegment(prev)
	}
	return nil
}

// collectSegment 回收一个段：关闭并删除文件。
//
// 调用方必须已经确认"该段已封口且无存活消息"—— 这是本文件顶部那条不变式的落点。
func (s *QueueStore) collectSegment(id uint32) {
	seg, ok := s.segs[id]
	if !ok {
		return
	}
	delete(s.segs, id)
	if err := seg.file.close(); err != nil {
		s.log.Warn("关闭待回收的段失败", "dir", s.dir, "segment", id, "err", err)
	}
	if err := os.Remove(segmentPath(s.dir, id)); err != nil && !os.IsNotExist(err) {
		s.log.Warn("删除段文件失败", "dir", s.dir, "segment", id, "err", err)
		return
	}
	s.log.Debug("已回收段文件", "dir", s.dir, "segment", id, "freed_bytes", seg.size)
}

// maybeCompactIndex 在"索引里的失效记录远多于有效记录"时重写索引。
//
// 段被回收后，索引里指向它的记录就成了纯粹负担（索引同样只增不减）。
// 判据 = 存活记录的理想大小 + 一个小下限，再留一倍余量：
//   - 下限（16 KiB）是为了避免小索引被频繁重写；
//   - 与存活量挂钩的部分会随队列规模自适应 —— 大队列的索引本来就该更大。
//
// 这个判据保证索引大小**有上界**（约 2×理想大小 + 一批增量），不会随历史写入线性增长。
func (s *QueueStore) maybeCompactIndex() error {
	want := int64(len(s.liveSeg))*int64(indexEntrySize) + 16<<10
	if s.idx.size() <= want*2 {
		return nil
	}
	return s.compactIndex()
}

// compactIndex 重写索引：只保留仍存活（未被 ack）的消息记录，并原子替换旧文件。
//
// 新旧格式在这里统一成新格式 —— 旧格式的索引只要经历过一次压缩，就自动完成升级。
func (s *QueueStore) compactIndex() error {
	idxPath := indexPath(s.dir, "000001.idx")
	tmpPath := indexPath(s.dir, "index.compact.tmp")

	tmp, err := openLogFile(tmpPath)
	if err != nil {
		return err
	}
	// forEach 是按 valid 指针顺序读文件的，因此必须先把缓冲刷下去 ——
	// 否则会读到"已计数、尚未落文件"的字节，表现为 EOF。
	if err := s.idx.flush(); err != nil {
		_ = tmp.close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("压缩前刷索引失败: %w", err)
	}
	live := s.liveSeg
	werr := s.idx.forEach(func(_ int64, payload []byte) error {
		e, derr := decodeIndexEntry(payload)
		if derr != nil {
			return derr
		}
		if e.ack {
			return nil // 已确认的记录不再需要
		}
		if segID, ok := live[e.seq]; !ok || segID != e.seg {
			return nil // 消息已离开队列（或所在段已被回收）
		}
		_, aerr := tmp.append(encodeIndexEntry(e))
		return aerr
	})
	if werr != nil {
		// 带上"内存计数 vs 文件实际大小"：这两个数字不一致，说明索引文件被别处改动过。
		werr = fmt.Errorf("遍历索引失败（valid=%d, 文件大小=%d）: %w",
			s.idx.size(), s.idx.fileSize(), werr)
	}
	if werr == nil {
		// 只有 batch / always 档位才需要把新索引真正刷到盘上；
		// os 档位下"写 tmp + 原子替换"已经足够保证索引不无限增长。
		if s.opts.Fsync >= FsyncBatch {
			werr = tmp.sync()
		} else {
			werr = tmp.flush()
		}
	}
	if cerr := tmp.close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("重写索引失败: %w", werr)
	}

	if err := s.idx.close(); err != nil {
		return fmt.Errorf("关闭旧索引失败: %w", err)
	}
	// 同一卷内的 Rename 是原子的（Windows 上等价于 MoveFileEx + REPLACE_EXISTING），
	// 因此"替换索引"这一步不存在"旧文件已删、新文件未就位"的中间态。
	if err := os.Rename(tmpPath, idxPath); err != nil {
		return fmt.Errorf("替换索引文件失败: %w", err)
	}
	reopened, err := openLogFile(idxPath)
	if err != nil {
		return fmt.Errorf("重新打开索引失败: %w", err)
	}
	s.idx = reopened
	s.log.Info("索引已压缩", "dir", s.dir, "live_messages", len(live), "bytes", reopened.size())
	return nil
}

// removeStaleSegments 在恢复阶段清理"没有任何存活消息"的段文件。
//
// 它们要么是上次运行结束时刚好被消费空的段，要么是压缩/回收中途被打断留下的。
func (s *QueueStore) removeStaleSegments(keep map[uint32]struct{}) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || filepath.Ext(name) != ".seg" {
			continue
		}
		var id uint32
		if _, err := fmt.Sscanf(name, "%06d.seg", &id); err != nil {
			continue
		}
		if _, ok := keep[id]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
			s.log.Warn("清理空段失败", "dir", s.dir, "segment", id, "err", err)
			continue
		}
		s.log.Debug("恢复时清理了无存活消息的段", "dir", s.dir, "segment", id)
	}
}
