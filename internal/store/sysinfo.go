package store

import (
	"errors"
	"os"
	"strings"
)

// ErrSysInfoUnsupported 表示当前平台未实现资源探测（磁盘可用空间 / 物理内存总量）。
//
// 生产与验收只承诺 Linux（见设计 1.4），其他平台取不到指标是设计内的行为：
// 流控在取不到指标时按"未超限"处理，绝不会因为探测失败就误阻塞业务。
// 定义在这里（无构建标签）是为了让所有平台都能引用它做判断。
var ErrSysInfoUnsupported = errors.New("当前平台未实现资源探测")

// DiskFree 返回路径所在文件系统的可用字节数。
//
// 关键行为：路径不存在时**向上找到最近的存在目录**再探测。
// 数据目录在第一条 durable 队列落盘之前可能并不存在，若直接探测就会失败，
// 而失败会被流控当作"取不到指标"，于是磁盘水位静默失效 —— 这正是要避免的
// "看起来配了、实际没生效"。向上查找让探测只依赖"卷存在"这一事实。
func DiskFree(path string) (uint64, error) {
	target, ok := nearestExistingDir(path)
	if !ok {
		return 0, os.ErrNotExist
	}
	return diskFreeOS(target)
}

// TotalMemory 返回物理内存总量；取不到时返回 false。
func TotalMemory() (uint64, bool) { return totalMemoryOS() }

// nearestExistingDir 从 path 起向上找到第一个存在的目录。
func nearestExistingDir(path string) (string, bool) {
	if path == "" {
		return "", false
	}
	cur := path
	for {
		if info, err := os.Stat(cur); err == nil && info.IsDir() {
			return cur, true
		}
		parent := parentDir(cur)
		if parent == "" || parent == cur {
			return "", false
		}
		cur = parent
	}
}

// parentDir 返回上一级目录；已到根（".", "/", "C:\"）时返回空串。
func parentDir(p string) string {
	trimmed := strings.TrimRight(p, `/\`)
	if trimmed == "" {
		return ""
	}
	// 保留 Windows 的卷前缀（"C:" 的父目录仍是它自己）
	if len(trimmed) == 2 && trimmed[1] == ':' {
		return ""
	}
	idx := strings.LastIndexAny(trimmed, `/\`)
	if idx < 0 {
		return ""
	}
	// 形如 "/foo" 的父目录是 "/"
	if idx == 0 {
		return string(trimmed[0])
	}
	// "C:/foo" 的父目录是 "C:/"（保留卷根，别退化成一个裸盘符）
	if idx == 2 && trimmed[1] == ':' {
		return trimmed[:3]
	}
	return trimmed[:idx]
}
