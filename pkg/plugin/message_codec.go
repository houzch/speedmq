package plugin

// MessageCodec 把一条消息编成不透明字节，并从字节还原。
//
// 为什么需要它：跨节点转发必须把整条消息搬到"队列数据所在的节点"，
// 而属性值的类型体系是由协议决定的 —— AMQP 0-9-1 用 field-table 承载消息头，
// 其中 int32 与 double、longstr 与 bytes 在客户端看来是**不同的类型**，
// 用 JSON 之类的通用格式中转会把它们悄悄退化（数字全变 float64、bytes 变字符串），
// 客户端于是收到与发送端不一致的消息头。因此编解码由协议侧提供，内核只搬运字节。
//
// 实现约束：
//  1. **保真**：Encode 后再 Decode 得到的消息，除 `Redelivered` 之外必须与原始消息等价
//     （`Redelivered` 是队列内软状态，由接收端的队列自己决定）；
//  2. 不得依赖网络与磁盘，必须是纯函数；
//  3. 对非法输入返回错误而不是 panic 或静默截断。
type MessageCodec interface {
	// EncodeMessage 编码一条消息。
	EncodeMessage(m *Message) ([]byte, error)
	// DecodeMessage 还原一条消息；数据非法时返回错误（调用方按"投递失败"处理）。
	DecodeMessage(data []byte) (*Message, error)
}
