//go:build !linux

package config

// platformTimezone 在非 Linux 平台不做系统时区探测（验收与生产只承诺 Linux，见 AGENTS.md §1），
// 只依赖 TZ 环境变量；未设置时回退默认语言。未实现的能力显式返回空串，而不是假装成功。
func platformTimezone() string {
	return ""
}
