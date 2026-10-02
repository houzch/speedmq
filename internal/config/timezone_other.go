//go:build !unix && !windows

package config

// platformTimezone 在既不是 unix 也不是 Windows 的平台（如 js/wasm）不做系统探测：
// 只依赖 TZ 环境变量（见 timezone.go），未设置时回退默认语言。
// 未实现的能力显式返回空串，而不是假装成功。
func platformTimezone() string {
	return ""
}
