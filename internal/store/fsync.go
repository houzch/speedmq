// Package store 实现队列消息的持久化：分段追加日志 + 队列索引 + 组提交刷盘与崩溃恢复。
//
// 设计对齐 docs/SwiftMQ-design.md 的 5.3 节：
//   - 消息体写"每队列 segment"，队列索引只记 seq-id → 位置 / 长度 / 状态；
//   - 每条记录带长度前缀与 CRC32，恢复时丢弃尾部半写记录；
//   - fsync 分档（none / os / batch / always），且**确认时机与档位强绑定**。
//
// 本包只依赖标准库与内核内部的协议编解码（复用内容属性的编解码，保证属性类型不失真）。
package store

import "fmt"

// FsyncLevel 是落盘档位（对齐设计 5.3.5）。
//
// 档位直接决定 publisher confirm 的时机，二者不可拆分配置 ——
// "收到 confirm = 不会丢" 这个业务假设只有在 batch / always 下才成立。
type FsyncLevel int

const (
	// FsyncNone 不落盘：消息只存在于内存（尽力而为，对齐瞬时消息语义）。
	FsyncNone FsyncLevel = iota
	// FsyncOS 写入文件但不 fsync：进程崩溃不丢，主机断电可能丢。
	// 对齐 RabbitMQ 经典队列"confirm 前不 fsync"的行为，因此也是默认档位。
	FsyncOS
	// FsyncBatch 组提交：一批记录写完后才 fsync，fsync 完成才回 confirm。
	FsyncBatch
	// FsyncAlways 每条记录落盘即 fsync，fsync 完成才回 confirm。
	FsyncAlways
)

// String 返回档位名（与配置项取值一致）。
func (l FsyncLevel) String() string {
	switch l {
	case FsyncNone:
		return "none"
	case FsyncOS:
		return "os"
	case FsyncBatch:
		return "batch"
	case FsyncAlways:
		return "always"
	default:
		return fmt.Sprintf("unknown(%d)", int(l))
	}
}

// ParseFsync 解析档位名；空串按 os 处理。
func ParseFsync(s string) (FsyncLevel, error) {
	switch s {
	case "":
		return FsyncOS, nil
	case "none":
		return FsyncNone, nil
	case "os":
		return FsyncOS, nil
	case "batch":
		return FsyncBatch, nil
	case "always":
		return FsyncAlways, nil
	default:
		return FsyncOS, fmt.Errorf("未知的 fsync 档位 %q（可选 none / os / batch / always）", s)
	}
}
