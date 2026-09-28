package store

import (
	"path/filepath"
	"testing"
)

// 本文件覆盖平台相关的资源探测：水位流控与 /api/nodes 都依赖它，
// 取不到指标会让磁盘水位失效（"看起来没超限"），因此必须有断言而不是只看返回值。
//
// 例外是明确未实现的平台（sysinfo_other.go）：那里返回错误是设计行为。

func TestDiskFree(t *testing.T) {
	dir := t.TempDir()
	free, err := DiskFree(dir)
	if err != nil {
		if err == ErrSysInfoUnsupported {
			t.Skipf("当前平台未实现磁盘探测: %v", err)
		}
		t.Fatalf("探测 %s 的可用空间失败: %v", dir, err)
	}
	if free == 0 {
		t.Fatalf("可用空间不应为 0（探测实现可能没取到值）")
	}
}

// TestDiskFreeMissingPath 覆盖"数据目录尚不存在"的场景：
// 必须向上找到最近的存在目录再探测，否则磁盘水位会静默失效
// （流控把探测失败当成"未超限"）。
func TestDiskFreeMissingPath(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "not", "created", "yet")

	got, err := DiskFree(missing)
	if err != nil {
		if err == ErrSysInfoUnsupported {
			t.Skipf("当前平台未实现磁盘探测: %v", err)
		}
		t.Fatalf("路径不存在时也应能探测到所在卷: %v", err)
	}
	want, err := DiskFree(base)
	if err != nil {
		t.Fatalf("探测已存在路径失败: %v", err)
	}
	if got != want {
		t.Fatalf("同一卷上的可用空间应一致: got %d want %d", got, want)
	}
}

func TestTotalMemory(t *testing.T) {
	total, ok := TotalMemory()
	if !ok {
		t.Skip("当前平台未实现内存总量探测")
	}
	// 小于 64 MiB 的"物理内存"必然是取错了字段
	if total < 64<<20 {
		t.Fatalf("物理内存总量可疑: %d 字节", total)
	}
}
