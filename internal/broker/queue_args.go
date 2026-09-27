package broker

import (
	"fmt"
	"strings"
	"time"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// 队列参数键名（与 RabbitMQ 一致）。
const (
	argMessageTTL     = "x-message-ttl"
	argMaxLength      = "x-max-length"
	argMaxLengthBytes = "x-max-length-bytes"
	argOverflow       = "x-overflow"
	argDeadLetterEx   = "x-dead-letter-exchange"
	argDeadLetterKey  = "x-dead-letter-routing-key"
	argMaxPriority    = "x-max-priority"
	argQueueExpires   = "x-expires"
	argQueueType      = "x-queue-type"
)

// 溢出策略。
const (
	overflowDropHead         = "drop-head"
	overflowRejectPublish    = "reject-publish"
	overflowRejectPublishDLX = "reject-publish-dlx"
)

// queueArgs 是队列声明参数中本实现支持的子集。
type queueArgs struct {
	// messageTTL 是 x-message-ttl（队列级消息存活时间）；0 表示不限。
	messageTTL time.Duration
	// expires 是 x-expires（队列在无消费者且无访问后的存活时间）；0 表示不限。
	expires time.Duration
	// maxLength / maxLengthBytes 是长度限制；0 表示不限。
	maxLength      int64
	maxLengthBytes int64
	// overflow 是长度超限时的策略。
	overflow string
	// deadLetterEx / deadLetterKey 是死信投递目标；deadLetterKey 为空时沿用原 routing key。
	deadLetterEx  string
	deadLetterKey string
	// maxPriority 是 x-max-priority；0 表示普通队列。
	maxPriority uint8
}

// parseQueueArgs 解析并校验队列参数。
//
// 校验从严：值非法时返回 PRECONDITION_FAILED（对齐 RabbitMQ）。
// 明确不支持的能力返回 NOT_IMPLEMENTED，而不是静默忽略 —— 静默忽略会让业务
// 以为自己拿到了 TTL/优先级/仲裁队列语义，实际没有。
func parseQueueArgs(args map[string]any) (queueArgs, error) {
	a := queueArgs{overflow: overflowDropHead}
	if len(args) == 0 {
		return a, nil
	}

	if v, ok, err := intArg(args, argMessageTTL); err != nil {
		return a, err
	} else if ok {
		if v < 0 {
			return a, precondition("%s 不能为负数", argMessageTTL)
		}
		a.messageTTL = time.Duration(v) * time.Millisecond
	}

	if v, ok, err := intArg(args, argQueueExpires); err != nil {
		return a, err
	} else if ok {
		if v <= 0 {
			return a, precondition("%s 必须大于 0", argQueueExpires)
		}
		a.expires = time.Duration(v) * time.Millisecond
	}

	if v, ok, err := intArg(args, argMaxLength); err != nil {
		return a, err
	} else if ok {
		if v < 0 {
			return a, precondition("%s 不能为负数", argMaxLength)
		}
		a.maxLength = v
	}

	if v, ok, err := intArg(args, argMaxLengthBytes); err != nil {
		return a, err
	} else if ok {
		if v < 0 {
			return a, precondition("%s 不能为负数", argMaxLengthBytes)
		}
		a.maxLengthBytes = v
	}

	if v, ok := stringArg(args, argOverflow); ok {
		switch v {
		case overflowDropHead, overflowRejectPublish, overflowRejectPublishDLX:
			a.overflow = v
		default:
			return a, precondition("未知的 %s 取值 %q", argOverflow, v)
		}
	}

	if v, ok := stringArg(args, argDeadLetterEx); ok {
		a.deadLetterEx = v
	}
	if v, ok := stringArg(args, argDeadLetterKey); ok {
		a.deadLetterKey = v
	}

	if v, ok, err := intArg(args, argMaxPriority); err != nil {
		return a, err
	} else if ok {
		if v < 0 || v > 255 {
			return a, precondition("%s 必须在 0..255 之间", argMaxPriority)
		}
		a.maxPriority = uint8(v)
	}

	// 队列类型：v1 只有 classic。要求 quorum / stream 时明确报错，
	// 否则业务会以为自己得到了仲裁队列的持久性保证。
	if v, ok := stringArg(args, argQueueType); ok && v != "" && v != "classic" {
		return a, plugin.Errorf(plugin.KindNotImplemented,
			"NOT_IMPLEMENTED - queue type %q is not supported yet (only classic)", v)
	}

	return a, nil
}

// hasTTL 表示该队列需要过期扫描。
func (a queueArgs) hasTTL() bool { return a.messageTTL > 0 || a.hasDeadLetter() }

func (a queueArgs) hasDeadLetter() bool { return a.deadLetterEx != "" }

// effectiveTTL 返回一条消息的存活时间：队列级与消息级取较小值（对齐 RabbitMQ）。
func (a queueArgs) effectiveTTL(perMessage time.Duration) time.Duration {
	switch {
	case a.messageTTL <= 0:
		return perMessage
	case perMessage <= 0:
		return a.messageTTL
	case perMessage < a.messageTTL:
		return perMessage
	default:
		return a.messageTTL
	}
}

// ---------------------------------------------------------------------------
// 参数取值：客户端可能用不同数值类型表达同一个值，这里统一收敛
// ---------------------------------------------------------------------------

func intArg(args map[string]any, key string) (int64, bool, error) {
	v, ok := args[key]
	if !ok || v == nil {
		return 0, false, nil
	}
	switch n := v.(type) {
	case int:
		return int64(n), true, nil
	case int8:
		return int64(n), true, nil
	case int16:
		return int64(n), true, nil
	case int32:
		return int64(n), true, nil
	case int64:
		return n, true, nil
	case uint:
		return int64(n), true, nil
	case uint8:
		return int64(n), true, nil
	case uint16:
		return int64(n), true, nil
	case uint32:
		return int64(n), true, nil
	case uint64:
		return int64(n), true, nil
	case float32:
		return int64(n), true, nil
	case float64:
		return int64(n), true, nil
	case string:
		var out int64
		if _, err := fmt.Sscanf(strings.TrimSpace(n), "%d", &out); err != nil {
			return 0, true, precondition("%s 的值 %q 不是整数", key, n)
		}
		return out, true, nil
	default:
		return 0, true, precondition("%s 的类型 %T 不受支持", key, v)
	}
}

func stringArg(args map[string]any, key string) (string, bool) {
	v, ok := args[key]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}

func precondition(format string, a ...any) error {
	return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - "+format, a...)
}
