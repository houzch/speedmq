package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// recordHeaderSize 是记录头长度：载荷长度(4) + CRC32(4)。
const recordHeaderSize = 8

// maxRecordSize 是单条记录载荷的合理性上限。
//
// 损坏文件的长度前缀可能是个天文数字，直接按它分配内存会被一次读取打爆；
// 超过上限一律判定为无效尾部并截断。
const maxRecordSize = 512 << 20

// logFile 是一个只追加写的记录日志文件。
//
// 记录格式：<payload-len u32 BE><crc32 u32 BE><payload>。
// 打开时顺序扫描并校验 CRC，遇到半写或损坏的尾部记录就截断到最后一个完整记录之后 ——
// 这正是崩溃恢复需要的语义：宁可丢掉尾部未写完的记录，也不能让后续解析全部错位。
type logFile struct {
	f     *os.File
	w     *bufio.Writer
	valid int64
}

// openLogFile 打开（必要时创建）日志文件，并完成尾部截断。
func openLogFile(path string) (*logFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	valid, err := truncateToValid(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	// 追加写的起点必须显式定位到有效数据的末尾：新打开的 fd 偏移是 0，
	// 而 truncateToValid 的定位读（ReadAt）不会改变它。少了这一步，
	// 重新打开一个**非空**文件之后的写入会从文件头覆盖已有记录 ——
	// 索引与段都会在"重启后继续写入"时损坏（这是 M8-7 排障时抓到的真实缺陷）。
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &logFile{f: f, w: bufio.NewWriterSize(f, 256<<10), valid: valid}, nil
}

// truncateToValid 扫描文件、截断尾部无效记录，返回有效字节数。
func truncateToValid(f *os.File) (int64, error) {
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
			break
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

// append 追加一条记录，返回该记录载荷在文件中的偏移。
func (l *logFile) append(payload []byte) (int64, error) {
	offset := l.valid
	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))
	if _, err := l.w.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := l.w.Write(payload); err != nil {
		return 0, err
	}
	l.valid += recordHeaderSize + int64(len(payload))
	return offset, nil
}

// read 读取指定偏移处的记录载荷，并校验 CRC。
func (l *logFile) read(offset int64) ([]byte, error) {
	var hdr [recordHeaderSize]byte
	if _, err := l.f.ReadAt(hdr[:], offset); err != nil {
		return nil, fmt.Errorf("读取记录头失败: %w", err)
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	want := binary.BigEndian.Uint32(hdr[4:8])
	if n > maxRecordSize {
		return nil, fmt.Errorf("记录长度 %d 超出上限", n)
	}
	payload := make([]byte, n)
	if _, err := l.f.ReadAt(payload, offset+recordHeaderSize); err != nil {
		return nil, fmt.Errorf("读取记录载荷失败: %w", err)
	}
	if crc32.ChecksumIEEE(payload) != want {
		return nil, fmt.Errorf("偏移 %d 处记录 CRC 校验失败", offset)
	}
	return payload, nil
}

// forEach 从文件头顺序遍历所有有效记录。
//
// 仅在恢复阶段调用（此时还没有任何待刷盘的写入）。
func (l *logFile) forEach(fn func(offset int64, payload []byte) error) error {
	var off int64
	for off < l.valid {
		payload, err := l.read(off)
		if err != nil {
			return err
		}
		if err := fn(off, payload); err != nil {
			return err
		}
		off += recordHeaderSize + int64(len(payload))
	}
	return nil
}

// size 返回有效字节数。
func (l *logFile) size() int64 { return l.valid }

// fileSize 返回文件的真实大小；读不到时返回 -1。
//
// 用来在出错时把"内存里的有效字节数"与"文件实际大小"一起报出来 ——
// 两者不一致说明文件被外部改动过，这是定位这类问题最关键的两个数字。
func (l *logFile) fileSize() int64 {
	info, err := l.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

// flush 把用户态缓冲写入操作系统（不保证落盘）。
func (l *logFile) flush() error { return l.w.Flush() }

// sync 写入并 fsync 到磁盘。
func (l *logFile) sync() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// close 刷盘后关闭文件。可重复调用（第二次起为安全空操作），以满足 Close 幂等约定。
func (l *logFile) close() error {
	f := l.f
	if f == nil {
		return nil
	}
	l.f = nil
	if err := l.w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
