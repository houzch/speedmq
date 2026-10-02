//go:build unix

package config

import (
	"os"
	"strings"
)

// platformTimezone 在 unix 平台（Linux / macOS 等）从 /etc/timezone 或 /etc/localtime 符号链接
// 解析 IANA 时区名。
//
// 两个来源都取不到时返回空串：容器镜像常把时区做成符号链接（读 /etc/timezone 拿不到），
// 因此先试 /etc/timezone，再退回解析符号链接目标里的 zoneinfo/ 路径。
// macOS 上 /etc/timezone 通常不存在，但 /etc/localtime 是指向
// /var/db/timezone/zoneinfo/<Region>/<City> 的符号链接，同样能通过 zoneinfo/ 解析出来。
func platformTimezone() string {
	if raw, err := os.ReadFile("/etc/timezone"); err == nil {
		if tz := strings.TrimSpace(string(raw)); tz != "" {
			return tz
		}
	}
	if target, err := os.Readlink("/etc/localtime"); err == nil {
		if idx := strings.Index(target, "zoneinfo/"); idx >= 0 {
			return target[idx+len("zoneinfo/"):]
		}
	}
	return ""
}
