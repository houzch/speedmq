//go:build !linux && !windows

package store

// diskFreeOS 在未支持的平台上返回 ErrSysInfoUnsupported。
func diskFreeOS(string) (uint64, error) { return 0, ErrSysInfoUnsupported }

// totalMemoryOS 在未支持的平台上返回 false。
func totalMemoryOS() (uint64, bool) { return 0, false }
