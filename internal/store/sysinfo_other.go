//go:build !linux && !windows && !darwin

package store

// diskFreeOS 在尚未实现探测的平台上返回 ErrSysInfoUnsupported。
func diskFreeOS(string) (uint64, error) { return 0, ErrSysInfoUnsupported }

// totalMemoryOS 在尚未实现探测的平台上返回 false。
func totalMemoryOS() (uint64, bool) { return 0, false }
