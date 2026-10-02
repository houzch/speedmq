// 本文件是"内核语义桥"在插件侧的**薄封装 + 演示**：它把 pkg/sidecar 的 JSON DTO 包装成
// 接近 pkg/plugin.Session 的好用方法，并把跨 RPC 回来的错误分类还原成 plugin.Error。
//
// 为什么封装放在这里而不是 pkg/sidecar：pkg/sidecar 是对外线契约、只依赖标准库，
// 不能 import pkg/plugin；而这个示例是独立 module，两边都能用（见 AGENTS.md §2）。
//
// 用法（在 Handler.Open 里）：
//
//	br, _ := sidecar.BridgeFromContext(ctx)
//	s := newSessionClient(stream.ID(), br)
//	if err := s.Open(ctx, "/"); err != nil { ... }
//	q, _ := s.DeclareQueue(ctx, plugin.QueueDeclare{Exclusive: true, AutoDelete: true})
//	_, _ = s.Publish(ctx, &plugin.Message{Body: []byte("hi")}, "", q.Name, false)
//	tag, _ := s.Consume(ctx, q.Name, "", false)
//	// 投递经 Handler.Call 的 session.deliver 到达；用 s.Settle(ctx, id, plugin.SettleAck) 结算。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

// runSessionDemo 是把上面这套封装串起来的一个演示：在给定流上打开会话，
// 声明一个临时队列，发布一条消息，注册消费者并等待内核把投递回推回来。
//
// 结算在 handleDeliver 里完成 —— 那一侧才是投递真正到达的地方（内核正向调用 session.deliver）。
func (h *handler) runSessionDemo(ctx context.Context, stream *sidecar.Stream) {
	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		h.log.Printf("session 演示：ctx 中没有内核桥，跳过")
		return
	}
	s := newSessionClient(stream.ID(), br)
	if err := s.Open(ctx, h.vhost); err != nil {
		h.log.Printf("session 演示：打开 vhost %q 的会话失败: %v", h.vhost, err)
		return
	}
	// 独占 + 自动删除：演示用临时队列，流结束时会随会话一起被内核清理。
	q, err := s.DeclareQueue(ctx, plugin.QueueDeclare{Exclusive: true, AutoDelete: true})
	if err != nil {
		h.log.Printf("session 演示：声明队列失败: %v", err)
		return
	}
	res, err := s.Publish(ctx, &plugin.Message{Body: []byte("hello from echosidecar")}, "", q.Name, false)
	if err != nil {
		h.log.Printf("session 演示：发布失败: %v", err)
		return
	}
	tag, err := s.Consume(ctx, q.Name, "", false)
	if err != nil {
		h.log.Printf("session 演示：注册消费者失败: %v", err)
		return
	}
	h.log.Printf("session 演示：queue=%s routed=%v tag=%s（等 session.deliver 回推后结算）",
		q.Name, res.Routed, tag)
}

// handleDeliver 处理内核回推的投递（正向调用 session.deliver）：打印消息体并 ack 结算。
func (h *handler) handleDeliver(ctx context.Context, params json.RawMessage) (any, error) {
	var p sidecar.DeliverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("session.deliver 参数解析失败: %w", err)
	}
	msg := messageFromDTO(p.Message)
	h.log.Printf("session 演示：收到投递 id=%d queue=%s consumer=%s body=%q",
		p.DeliveryID, p.Queue, p.ConsumerTag, msg.Body)

	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("session 演示：ctx 中没有内核桥，无法结算")
	}
	// 结算不需要流号：投递编号在整条连接上全局唯一。
	s := newSessionClient(0, br)
	if err := s.Settle(ctx, p.DeliveryID, plugin.SettleAck); err != nil {
		return nil, fmt.Errorf("session 演示：结算失败: %w", err)
	}
	return nil, nil
}

// sessionClient 绑定"某条流上的一个内核会话"，把反向调用包成方法。
type sessionClient struct {
	br     *sidecar.Bridge
	stream uint32
}

func newSessionClient(stream uint32, br *sidecar.Bridge) *sessionClient {
	return &sessionClient{br: br, stream: stream}
}

// Open 在该流上打开指定 vhost 的会话（内核会做与协议插件相同的权限校验）。
func (s *sessionClient) Open(ctx context.Context, vhost string) error {
	return s.call(ctx, sidecar.MethodSessionOpen,
		sidecar.SessionOpenParams{Stream: s.stream, VHost: vhost}, nil)
}

// Close 释放该流上的会话（取消其消费者、删除其独占队列）。
func (s *sessionClient) Close(ctx context.Context) error {
	return s.call(ctx, sidecar.MethodSessionClose, sidecar.SessionCloseParams{Stream: s.stream}, nil)
}

// DeclareQueue 声明队列。
func (s *sessionClient) DeclareQueue(ctx context.Context, req plugin.QueueDeclare) (plugin.QueueInfo, error) {
	var out sidecar.QueueInfoResult
	err := s.call(ctx, sidecar.MethodSessionDeclareQueue, sidecar.QueueDeclareParams{
		Stream:     s.stream,
		Name:       req.Name,
		Passive:    req.Passive,
		Durable:    req.Durable,
		Exclusive:  req.Exclusive,
		AutoDelete: req.AutoDelete,
		Arguments:  req.Arguments,
	}, &out)
	if err != nil {
		return plugin.QueueInfo{}, err
	}
	return plugin.QueueInfo{Name: out.Name, MessageCount: out.MessageCount, ConsumerCount: out.ConsumerCount}, nil
}

// DeleteQueue 删除队列并返回删除前的统计。
func (s *sessionClient) DeleteQueue(ctx context.Context, name string, ifUnused, ifEmpty bool) (plugin.QueueInfo, error) {
	var out sidecar.QueueInfoResult
	err := s.call(ctx, sidecar.MethodSessionDeleteQueue, sidecar.DeleteQueueParams{
		Stream: s.stream, Name: name, IfUnused: ifUnused, IfEmpty: ifEmpty,
	}, &out)
	if err != nil {
		return plugin.QueueInfo{}, err
	}
	return plugin.QueueInfo{Name: out.Name, MessageCount: out.MessageCount, ConsumerCount: out.ConsumerCount}, nil
}

// Publish 发布一条消息。
//
// 返回的 PublishResult.Durable 恒为 nil：持久化等待在内核侧应答之前就已完成
// （调用返回即等于"已按 fsync 档位落盘"），因此插件不必再等一次。
func (s *sessionClient) Publish(ctx context.Context, msg *plugin.Message, exchange, routingKey string, mandatory bool) (plugin.PublishResult, error) {
	var out sidecar.PublishResultDTO
	err := s.call(ctx, sidecar.MethodSessionPublish, sidecar.PublishParams{
		Stream:     s.stream,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Mandatory:  mandatory,
		Message:    messageToDTO(msg),
	}, &out)
	if err != nil {
		return plugin.PublishResult{}, err
	}
	return plugin.PublishResult{Routed: out.Routed, Rejected: out.Rejected}, nil
}

// Consume 注册消费者并返回内核最终使用的标签。
//
// 之后的投递经 Handler.Call 的 session.deliver 到达；用 Settle 结算。
func (s *sessionClient) Consume(ctx context.Context, queue, tag string, noAck bool) (string, error) {
	var out sidecar.ConsumeResult
	err := s.call(ctx, sidecar.MethodSessionConsume, sidecar.ConsumeParams{
		Stream: s.stream, Queue: queue, Tag: tag, NoAck: noAck,
	}, &out)
	if err != nil {
		return "", err
	}
	return out.Tag, nil
}

// Cancel 取消一个消费者。
func (s *sessionClient) Cancel(ctx context.Context, tag string) error {
	return s.call(ctx, sidecar.MethodSessionCancel, sidecar.CancelParams{Stream: s.stream, Tag: tag}, nil)
}

// Settle 结算一条投递（编号来自 DeliverParams.DeliveryID）。与流无关，因此 stream 不参与。
func (s *sessionClient) Settle(ctx context.Context, deliveryID uint64, action plugin.SettleAction) error {
	name, err := settleActionName(action)
	if err != nil {
		return err
	}
	return s.call(ctx, sidecar.MethodSessionSettle,
		sidecar.SettleParams{DeliveryID: deliveryID, Action: name}, nil)
}

// call 统一执行反向调用并把带分类的错误还原成 plugin.Error。
func (s *sessionClient) call(ctx context.Context, method string, params, out any) error {
	return bridgeError(s.br.Call(ctx, method, params, out))
}

// bridgeError 把 *sidecar.RPCError 还原为 plugin.Errorf(kind, ...)，其余错误原样返回。
func bridgeError(err error) error {
	if err == nil {
		return nil
	}
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}

func settleActionName(action plugin.SettleAction) (string, error) {
	switch action {
	case plugin.SettleAck:
		return sidecar.SettleActionAck, nil
	case plugin.SettleRequeue:
		return sidecar.SettleActionRequeue, nil
	case plugin.SettleReject:
		return sidecar.SettleActionReject, nil
	default:
		return "", fmt.Errorf("未知的结算动作 %d", int(action))
	}
}

func messageToDTO(m *plugin.Message) sidecar.MessageDTO {
	if m == nil {
		return sidecar.MessageDTO{}
	}
	return sidecar.MessageDTO{
		Exchange:    m.Exchange,
		RoutingKey:  m.RoutingKey,
		Properties:  propertiesToDTO(m.Properties),
		Body:        m.Body,
		Redelivered: m.Redelivered,
	}
}

// messageFromDTO 把回推回来的消息 DTO 还原成 plugin.Message（供消费者使用）。
func messageFromDTO(m sidecar.MessageDTO) *plugin.Message {
	return &plugin.Message{
		Exchange:    m.Exchange,
		RoutingKey:  m.RoutingKey,
		Properties:  propertiesFromDTO(m.Properties),
		Body:        m.Body,
		Redelivered: m.Redelivered,
	}
}

func propertiesToDTO(p plugin.Properties) sidecar.PropertiesDTO {
	return sidecar.PropertiesDTO{
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		Headers:         p.Headers,
		DeliveryMode:    p.DeliveryMode,
		Priority:        p.Priority,
		CorrelationID:   p.CorrelationID,
		ReplyTo:         p.ReplyTo,
		Expiration:      p.Expiration,
		MessageID:       p.MessageID,
		Timestamp:       p.Timestamp,
		Type:            p.Type,
		UserID:          p.UserID,
		AppID:           p.AppID,
	}
}

func propertiesFromDTO(p sidecar.PropertiesDTO) plugin.Properties {
	return plugin.Properties{
		ContentType:     p.ContentType,
		ContentEncoding: p.ContentEncoding,
		Headers:         p.Headers,
		DeliveryMode:    p.DeliveryMode,
		Priority:        p.Priority,
		CorrelationID:   p.CorrelationID,
		ReplyTo:         p.ReplyTo,
		Expiration:      p.Expiration,
		MessageID:       p.MessageID,
		Timestamp:       p.Timestamp,
		Type:            p.Type,
		UserID:          p.UserID,
		AppID:           p.AppID,
	}
}
