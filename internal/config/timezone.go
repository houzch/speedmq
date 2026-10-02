package config

import (
	"os"
	"time"
)

// SystemTimezone 返回本机的 IANA 时区名（如 Asia/Shanghai）；无法确定时返回空串。
//
// 优先使用 TZ 环境变量（容器里显式设置最可靠），其次交给平台实现读取系统时区文件。
// 结果只用于推断管理 UI 的默认语言，不参与任何业务逻辑，因此探测失败即回退默认语言，
// 不做模糊猜测。
func SystemTimezone() string {
	if tz := os.Getenv("TZ"); tz != "" {
		if _, err := time.LoadLocation(tz); err == nil {
			return tz
		}
	}
	return platformTimezone()
}
