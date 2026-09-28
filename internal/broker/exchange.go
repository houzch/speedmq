package broker

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// binding 是一条"交换机 → 目标队列"的绑定。
type binding struct {
	routingKey string
	queue      string
	arguments  map[string]any
}

// exchange 是交换机实例。
//
// route 在 M2 采用线性扫描：正确优先。direct 的 routing-key 索引与 topic 的 Trie
// 属于性能优化，留到性能里程碑替换，对外行为不变。
type exchange struct {
	name       string
	typ        plugin.ExchangeType
	durable    bool
	autoDelete bool
	internal   bool
	arguments  map[string]any

	mu       sync.RWMutex
	bindings []binding
	// exchBindings 是"交换机 → 交换机"的绑定，binding.queue 字段存放目标交换机名。
	// 声明 exchange_exchange_bindings 能力就必须真的按它路由，否则是静默的语义缺失。
	exchBindings []binding
}

func newExchange(name string, typ plugin.ExchangeType, durable, autoDelete, internal bool, args map[string]any) *exchange {
	return &exchange{
		name:       name,
		typ:        typ,
		durable:    durable,
		autoDelete: autoDelete,
		internal:   internal,
		arguments:  args,
	}
}

// addBinding 添加绑定；同 (routing key, 队列) 已存在时幂等忽略。
func (e *exchange) addBinding(routingKey, queue string, args map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, b := range e.bindings {
		if b.queue == queue && b.routingKey == routingKey {
			return
		}
	}
	e.bindings = append(e.bindings, binding{routingKey: routingKey, queue: queue, arguments: args})
}

// removeBinding 移除指定绑定，返回是否真的移除了。
func (e *exchange) removeBinding(routingKey, queue string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, b := range e.bindings {
		if b.queue == queue && b.routingKey == routingKey {
			e.bindings = append(e.bindings[:i], e.bindings[i+1:]...)
			return true
		}
	}
	return false
}

// removeQueueBindings 移除指向某队列的全部绑定，返回移除数量。
func (e *exchange) removeQueueBindings(queue string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	kept := e.bindings[:0]
	removed := 0
	for _, b := range e.bindings {
		if b.queue == queue {
			removed++
			continue
		}
		kept = append(kept, b)
	}
	e.bindings = kept
	return removed
}

// bindingCount 返回当前绑定数（if-unused 判断用）。
func (e *exchange) bindingCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.bindings) + len(e.exchBindings)
}

// hasQueueBinding 判断 (routing key, 队列) 绑定是否存在。
//
// 集群模式下用于"提交后等待本地应用"：确认绑定真的落到本节点上了才向客户端返回成功。
func (e *exchange) hasQueueBinding(routingKey, queue string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, b := range e.bindings {
		if b.queue == queue && b.routingKey == routingKey {
			return true
		}
	}
	return false
}

// hasExchangeBinding 判断 (routing key, 目标交换机) 的交换机间绑定是否存在。
func (e *exchange) hasExchangeBinding(routingKey, dest string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, b := range e.exchBindings {
		if b.queue == dest && b.routingKey == routingKey {
			return true
		}
	}
	return false
}

// ExchangeSnapshot 是交换机的只读视图（管理面用）。
type ExchangeSnapshot struct {
	VHost            string
	Name             string
	Type             plugin.ExchangeType
	Durable          bool
	AutoDelete       bool
	Internal         bool
	Arguments        map[string]any
	QueueBindings    int
	ExchangeBindings int
}

// snapshot 返回交换机的只读快照。
func (e *exchange) snapshot(vhostName string) ExchangeSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return ExchangeSnapshot{
		VHost:            vhostName,
		Name:             e.name,
		Type:             e.typ,
		Durable:          e.durable,
		AutoDelete:       e.autoDelete,
		Internal:         e.internal,
		Arguments:        e.arguments,
		QueueBindings:    len(e.bindings),
		ExchangeBindings: len(e.exchBindings),
	}
}

// BindingSnapshot 是绑定的只读视图（管理面用）。
type BindingSnapshot struct {
	VHost           string
	Source          string
	Destination     string
	DestinationType string // "queue" / "exchange"
	RoutingKey      string
	Arguments       map[string]any
	// PropertiesKey 是 RabbitMQ Management API 里绑定的稳定标识：
	// 无参数时就是 routing key，有参数时附加参数哈希（避免同名绑定互相覆盖）。
	PropertiesKey string
}

// bindingSnapshots 返回本交换机作为 source 的全部绑定。
func (e *exchange) bindingSnapshots(vhostName string) []BindingSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]BindingSnapshot, 0, len(e.bindings)+len(e.exchBindings))
	for _, b := range e.bindings {
		out = append(out, BindingSnapshot{
			VHost:           vhostName,
			Source:          e.name,
			Destination:     b.queue,
			DestinationType: "queue",
			RoutingKey:      b.routingKey,
			Arguments:       b.arguments,
			PropertiesKey:   propertiesKey(b.routingKey, b.arguments),
		})
	}
	for _, b := range e.exchBindings {
		out = append(out, BindingSnapshot{
			VHost:           vhostName,
			Source:          e.name,
			Destination:     b.queue,
			DestinationType: "exchange",
			RoutingKey:      b.routingKey,
			Arguments:       b.arguments,
			PropertiesKey:   propertiesKey(b.routingKey, b.arguments),
		})
	}
	return out
}

// propertiesKey 生成绑定的稳定标识（对齐 RabbitMQ 的 properties_key 语义）。
func propertiesKey(routingKey string, args map[string]any) string {
	if len(args) == 0 {
		return routingKey
	}
	// 键排序后拼接，保证同一组参数总是得到同一个 key
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(routingKey)
	b.WriteByte('-')
	for _, k := range keys {
		fmt.Fprintf(&b, "%s:%v;", k, args[k])
	}
	return b.String()
}

// addExchangeBinding 添加一条交换机到交换机的绑定；已存在则幂等忽略。
func (e *exchange) addExchangeBinding(routingKey, destination string, args map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, b := range e.exchBindings {
		if b.queue == destination && b.routingKey == routingKey {
			return
		}
	}
	e.exchBindings = append(e.exchBindings, binding{routingKey: routingKey, queue: destination, arguments: args})
}

// removeExchangeBinding 移除一条交换机到交换机的绑定，返回是否真的移除了。
func (e *exchange) removeExchangeBinding(routingKey, destination string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, b := range e.exchBindings {
		if b.queue == destination && b.routingKey == routingKey {
			e.exchBindings = append(e.exchBindings[:i], e.exchBindings[i+1:]...)
			return true
		}
	}
	return false
}

// removeExchangeBindings 移除指向某交换机的全部绑定，返回移除数量（交换机删除时用）。
func (e *exchange) removeExchangeBindings(destination string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	kept := e.exchBindings[:0]
	removed := 0
	for _, b := range e.exchBindings {
		if b.queue == destination {
			removed++
			continue
		}
		kept = append(kept, b)
	}
	e.exchBindings = kept
	return removed
}

// routeAll 返回命中的目标队列与目标交换机（均已去重，保持绑定顺序）。
//
// 交换机间绑定由 vhost 递归展开，本方法只负责单跳匹配。
func (e *exchange) routeAll(routingKey string, props plugin.Properties) (queues, exchanges []string) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	seenQ := make(map[string]struct{}, len(e.bindings))
	seenX := make(map[string]struct{}, len(e.exchBindings))

	matches := func(b binding) bool {
		switch e.typ {
		case plugin.ExchangeDirect:
			return b.routingKey == routingKey
		case plugin.ExchangeFanout:
			return true
		case plugin.ExchangeTopic:
			return topicMatch(b.routingKey, routingKey)
		case plugin.ExchangeHeaders:
			return headersMatch(b.arguments, props.Headers)
		}
		return false
	}

	for _, b := range e.bindings {
		if !matches(b) {
			continue
		}
		if _, dup := seenQ[b.queue]; dup {
			continue
		}
		seenQ[b.queue] = struct{}{}
		queues = append(queues, b.queue)
	}
	for _, b := range e.exchBindings {
		if !matches(b) {
			continue
		}
		if _, dup := seenX[b.queue]; dup {
			continue
		}
		seenX[b.queue] = struct{}{}
		exchanges = append(exchanges, b.queue)
	}
	return queues, exchanges
}

// ---------------------------------------------------------------------------
// 路由匹配
// ---------------------------------------------------------------------------

// topicMatch 判断 topic 交换机下 routing key 是否命中绑定模式。
//
// 规则：以 "." 分段；"*" 匹配恰好一段；"#" 匹配零到多段。
func topicMatch(pattern, key string) bool {
	return topicMatchSegments(strings.Split(pattern, "."), strings.Split(key, "."))
}

func topicMatchSegments(pattern, key []string) bool {
	for len(pattern) > 0 {
		if pattern[0] == "#" {
			// "#" 匹配零到多段：逐个后缀尝试
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(key); i++ {
				if topicMatchSegments(pattern[1:], key[i:]) {
					return true
				}
			}
			return false
		}
		if len(key) == 0 {
			return false
		}
		if pattern[0] != "*" && pattern[0] != key[0] {
			return false
		}
		pattern, key = pattern[1:], key[1:]
	}
	return len(key) == 0
}

// headersMatch 判断消息头是否命中 headers 交换机的绑定参数。
//
// x-match=all（默认）要求绑定参数里的每个非 x- 键都与消息头相等；
// x-match=any 只要有一个相等即命中。
func headersMatch(require, headers map[string]any) bool {
	if len(require) == 0 {
		// 没有匹配条件时，all 语义下视为全部满足
		return true
	}
	matchAll := true
	if v, ok := require["x-match"].(string); ok && strings.EqualFold(v, "any") {
		matchAll = false
	}

	matchedAny := false
	for k, want := range require {
		if strings.HasPrefix(k, "x-") {
			continue
		}
		got, ok := headers[k]
		if ok && valueEqual(got, want) {
			matchedAny = true
			continue
		}
		if matchAll {
			return false
		}
	}
	if matchAll {
		return true
	}
	return matchedAny
}

// valueEqual 比较 field-table 值。由于客户端可能用不同数值类型表达同一个数，
// 这里先做类型精确比较，再退回到字符串比较，避免"1 与 int32(1) 不相等"这类假阴性。
func valueEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			return as == bs
		}
	}
	if fmt.Sprint(a) == fmt.Sprint(b) {
		return true
	}
	return false
}
