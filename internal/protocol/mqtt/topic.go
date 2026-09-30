package mqtt

import (
	"fmt"
	"strings"
)

// 本文件是 MQTT 与内核之间的**主题映射**：它决定了 MQTT 的"主题"如何落成
// 内核的"交换机 + 队列 + 绑定"。这是整个插件里最需要解释的一处设计，因此单独成文件。
//
// 核心决定：**MQTT 主题复用内核的 topic 交换机（amq.topic）**，理由有三：
//  1. 通配语义几乎完全对齐 —— MQTT 的 "+"（单层）就是 AMQP 的 "*"，
//     MQTT 的 "#"（多层，含父级）就是 AMQP 的 "#"；
//  2. 于是路由、死信、TTL、长度限制、确认语义**全部免费继承**，
//     不会出现"MQTT 的队列没有死信"这类语义分裂（这正是设计 §10.5 的硬约束 1）；
//  3. 与 RabbitMQ 的 MQTT 插件选择一致（同样用 amq.topic），使用者心智一致。
//
// 层级分隔符的差异是本映射唯一的"信息损失"：MQTT 用 "/"、AMQP 用 "."，
// 因此这里把 "/" 逐层替换成 "."。副作用是：MQTT 主题里如果**本身含点号**，
// 会与"层级分隔符"混淆（`a.b/c` 与 `a/b.c` 映射后相同）。这是 RabbitMQ 的
// MQTT 插件同样存在的已知取舍，此处保持一致并在文档里写明。

// maxClientIDLength 是接受的 Client ID 上限。
//
// MQTT 3.1.1 允许 1–23 个字符的严格实现，也允许服务端接受更长；这里取 128，
// 既容纳常见客户端（UUID 形态 36 字符）又能保证派生出的队列名在 AMQP shortstr 上限内。
const maxClientIDLength = 128

// subscriptionQueuePrefix 是订阅队列名的前缀（对齐 RabbitMQ MQTT 插件的命名习惯）。
const subscriptionQueuePrefix = "mqtt-subscription-"

// routingKey 把 MQTT 主题（或主题过滤器）转换成 AMQP topic 路由键。
//
// 逐层处理而不是整体替换：只有"整层为 +"才是通配符，主题里出现的字面量 +
// 不该被当成通配符（MQTT 规范也不允许这种过滤器，这里先转换、由校验拒绝）。
func routingKey(topic string) string {
	if topic == "" {
		return ""
	}
	levels := strings.Split(topic, "/")
	for i, l := range levels {
		if l == "+" {
			levels[i] = "*"
		}
	}
	return strings.Join(levels, ".")
}

// validTopic 校验 MQTT 主题或主题过滤器。
//
// allowWildcards 为 false 时用于 PUBLISH 的主题（规范禁止发布到通配主题）。
func validTopic(topic string, allowWildcards bool) error {
	if topic == "" {
		return fmt.Errorf("%w: 主题不能为空", errMalformed)
	}
	if len(topic) > 65535 {
		return fmt.Errorf("%w: 主题过长（%d 字节）", errMalformed, len(topic))
	}
	levels := strings.Split(topic, "/")
	for i, l := range levels {
		hasPlus := strings.Contains(l, "+")
		hasHash := strings.Contains(l, "#")
		if !allowWildcards {
			if hasPlus || hasHash {
				return fmt.Errorf("%w: 发布主题不得包含通配符 %q", errMalformed, topic)
			}
			continue
		}
		if hasPlus && l != "+" {
			return fmt.Errorf("%w: 通配符 + 必须独占一层（%q）", errMalformed, topic)
		}
		if hasHash {
			if l != "#" {
				return fmt.Errorf("%w: 通配符 # 必须独占一层（%q）", errMalformed, topic)
			}
			if i != len(levels)-1 {
				return fmt.Errorf("%w: 通配符 # 只能出现在最后一层（%q）", errMalformed, topic)
			}
		}
	}
	return nil
}

// validClientID 校验 Client ID。
func validClientID(id string, cleanSession bool) error {
	if id == "" {
		// MQTT 3.1.1 允许空 Client ID，但仅在 Clean Session 为真时有意义
		// （服务端会分配一个），否则无法找回会话 —— 报 0x02 identifier rejected。
		if !cleanSession {
			return fmt.Errorf("%w: Clean Session=0 时 Client ID 不能为空", errMalformed)
		}
		return nil
	}
	if len(id) > maxClientIDLength {
		return fmt.Errorf("%w: Client ID 过长（%d 字节，上限 %d）", errMalformed, len(id), maxClientIDLength)
	}
	return nil
}

// matchesFilter 判断一个具体主题是否命中主题过滤器（用于保留消息的投递）。
//
// 这里按 MQTT 自己的层级语义实现，而不是先转成路由键再复用内核的匹配器：
// 转换会引入 "层级分隔符与字面点号混淆" 的问题，而保留消息的匹配做不到"让内核兜底"。
func matchesFilter(filter, topic string) bool {
	if filter == topic {
		return true
	}
	if !strings.ContainsAny(filter, "+#") {
		return false
	}
	return matchMQTTLevels(strings.Split(filter, "/"), strings.Split(topic, "/"))
}

func matchMQTTLevels(pattern, levels []string) bool {
	for len(pattern) > 0 {
		if pattern[0] == "#" {
			// "#" 匹配剩余全部层级（含零层，因此 "sport/#" 也命中 "sport"）。
			return true
		}
		if len(levels) == 0 {
			return false
		}
		if pattern[0] != "+" && pattern[0] != levels[0] {
			return false
		}
		pattern, levels = pattern[1:], levels[1:]
	}
	return len(levels) == 0
}

// subscriptionQueue 返回某个 (Client ID, QoS) 组合对应的队列名。
//
// 同一 Client ID 的同一 QoS 只用一个队列，多个主题过滤器都绑到它上面：
// 于是"一条消息命中同一订阅者的多个过滤器"只会投递一次（内核在同一队列上去重），
// 与 MQTT 期望的行为一致，也避免了"每个过滤器一个队列"带来的队列爆炸。
func subscriptionQueue(clientID string, qos byte) string {
	return fmt.Sprintf("%sq%d-%s", subscriptionQueuePrefix, qos, clientID)
}
