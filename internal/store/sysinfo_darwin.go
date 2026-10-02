//go:build darwin

package store

import (
	"encoding/binary"
	"syscall"
)

// diskFreeOS 返回路径所在文件系统的可用字节数（路径必须已存在）。
//
// macOS 与 Linux 一样走 POSIX 的 statfs，只是结构体字段类型不同
// （darwin：Bavail 是 uint64、Bsize 是 uint32；linux：分别是 uint64 / int64），
// 统一转成 uint64 再相乘即可。
func diskFreeOS(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}

// totalMemoryOS 返回物理内存总量；取不到时返回 false。
//
// macOS 没有 /proc，改用 sysctl("hw.memsize")。标准库的 syscall 只有 Sysctl
// （返回**原始字节**，且会把结尾的 NUL 去掉），没有 SysctlUint64，所以这里自己按小端解析。
//
// 注意不能要求"长度必须等于 8"：hw.memsize 是 uint64，而常见容量（如 16 GiB = 0x0000000400000000）
// 最高字节为 0，会被 Sysctl 当作结尾 NUL 剥掉，长度只剩 7 字节。因此这里固定用 8 字节缓冲，
// 不足 8 字节时在高位补 0。
func totalMemoryOS() (uint64, bool) {
	raw, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0, false
	}
	b := []byte(raw)
	if len(b) == 0 || len(b) > 8 {
		return 0, false
	}
	var buf [8]byte
	copy(buf[:], b)
	total := binary.LittleEndian.Uint64(buf[:])
	if total == 0 {
		return 0, false
	}
	return total, true
}
