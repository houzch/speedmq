package mqtt

import (
	"sort"
	"sync"
)

// retainedMessage 是一条保留消息（MQTT 3.1.1 §3.3.1.3）。
type retainedMessage struct {
	VHost   string
	Topic   string
	Payload []byte
	QoS     byte
}

// retainedStore 保存各 vhost 的保留消息：订阅建立时按过滤器取出来先投给订阅者。
//
// 设计取舍（**已知限制**）：保留消息存在**插件内存**里，重启即丢。
// 之所以不像普通消息那样落内核队列：保留消息的语义是"按主题查询最新一条"，
// 而内核的队列是"先进先出"，用它承载需要再造一套回收策略（每个主题一条记录、
// 主题数无上限）。放进内核则要先把"KV 式读取"暴露成插件能力 —— 那是内核契约的扩张，
// 不适合塞进"验证插件化"这一轮。文档里如实记录，后续可用内部队列 + 索引补齐。
type retainedStore struct {
	mu   sync.Mutex
	msgs map[string]retainedMessage // key = vhost \x00 topic
}

func newRetainedStore() *retainedStore {
	return &retainedStore{msgs: map[string]retainedMessage{}}
}

func retainedKey(vhost, topic string) string { return vhost + "\x00" + topic }

// set 写入/覆盖一条保留消息；payload 为空表示**清除**该主题的保留消息（规范规定）。
func (s *retainedStore) set(vhost, topic string, payload []byte, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(payload) == 0 {
		delete(s.msgs, retainedKey(vhost, topic))
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	s.msgs[retainedKey(vhost, topic)] = retainedMessage{VHost: vhost, Topic: topic, Payload: cp, QoS: qos}
}

// lookup 返回命中过滤器的全部保留消息（按主题排序，保证投递顺序确定）。
func (s *retainedStore) lookup(vhost, filter string) []retainedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []retainedMessage
	for _, m := range s.msgs {
		if m.VHost != vhost || !matchesFilter(filter, m.Topic) {
			continue
		}
		m.Payload = append([]byte(nil), m.Payload...)
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}
