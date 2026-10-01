package main

import (
	"flag"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// clusterPeer 是本节点在对端的地址（队列数据所在节点）。
//
// 它把探针切成"跨节点转发验证"模式：客户端连到**非 Owner** 节点，
// 发布与消费都必须正常 —— 发布转发给 Owner，投递由 Owner 推回本节点。
var clusterPeer = flag.String("cluster-peer", "",
	"跨节点转发验证：指定队列数据所在节点（Owner）的地址；本进程的 -addr 则作为非 Owner 节点")

// runClusterCheck 验证 M6b 的跨节点转发。
//
// 单向走通一次就同时覆盖了两个方向：
//
//	非 Owner 节点发布 --转发--> Owner 队列 --代理投递--> 非 Owner 节点上的消费者
func runClusterCheck() error {
	const queue = "m6b.probe.q"

	// 1. 在 Owner 节点上声明 durable 队列（消息数据落在它上面）。
	ownerConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", *user, *pass, *clusterPeer))
	if err != nil {
		return fmt.Errorf("连接 Owner 节点 %s 失败: %w", *clusterPeer, err)
	}
	defer ownerConn.Close()
	ownerCh, err := ownerConn.Channel()
	if err != nil {
		return fmt.Errorf("Owner 节点打开 channel 失败: %w", err)
	}
	defer ownerCh.Close()
	if _, err := ownerCh.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("在 Owner 节点声明 durable 队列失败: %w", err)
	}
	if _, err := ownerCh.QueuePurge(queue, false); err != nil {
		return fmt.Errorf("清空队列失败: %w", err)
	}

	// 2. 客户端连到非 Owner 节点，并在那里消费（内核会注册代理消费者）。
	proxyConn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("连接非 Owner 节点 %s 失败: %w", *addr, err)
	}
	proxyCh, err := proxyConn.Channel()
	if err != nil {
		proxyConn.Close()
		return fmt.Errorf("非 Owner 节点打开 channel 失败: %w", err)
	}
	deliveries, err := proxyCh.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		proxyConn.Close()
		return fmt.Errorf("在非 Owner 节点上消费失败（远端队列应可消费）: %w", err)
	}

	// 3. 从非 Owner 节点发布 3 条持久消息：必须被转发到 Owner 的队列。
	//    消息头刻意用 int64/int32：跨节点的编解码若退化成 JSON，类型就会变（这是 M6b 的关键保真点）。
	for i := 0; i < 3; i++ {
		if err := proxyCh.Publish("", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      amqp.Table{"x-probe": int64(7), "x-seq": int32(i)},
			Body:         []byte(fmt.Sprintf("m6b-%d", i)),
		}); err != nil {
			proxyConn.Close()
			return fmt.Errorf("从非 Owner 节点发布第 %d 条失败: %w", i, err)
		}
	}

	// 4. 投递应当由 Owner 推回本节点。
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case d, ok := <-deliveries:
			if !ok {
				proxyConn.Close()
				return fmt.Errorf("投递通道被关闭（已收到 %v）", seen)
			}
			seen[string(d.Body)] = true
			if v, _ := d.Headers["x-probe"].(int64); v != 7 {
				proxyConn.Close()
				return fmt.Errorf("消息头未保真: x-probe=%#v (%T)", d.Headers["x-probe"], d.Headers["x-probe"])
			}
			if _, ok := d.Headers["x-seq"].(int32); !ok {
				proxyConn.Close()
				return fmt.Errorf("消息头类型退化: x-seq=%#v (%T)", d.Headers["x-seq"], d.Headers["x-seq"])
			}
			if err := d.Ack(false); err != nil {
				proxyConn.Close()
				return fmt.Errorf("确认第 %d 条失败: %w", i, err)
			}
		case <-time.After(15 * time.Second):
			proxyConn.Close()
			return fmt.Errorf("等待跨节点投递超时（已收到 %v）", seen)
		}
	}

	// 5. 先断开这条连接：代理消费者随之被摘除（未确认的消息回到 Owner 队头）。
	//    否则下面 basic.get 想取的消息会被这个还在的消费者"截胡"，测出来的空队列是假象。
	if err := proxyConn.Close(); err != nil {
		return fmt.Errorf("关闭代理连接失败: %w", err)
	}

	// 6. 跨节点 basic.get：消息由 Owner 发布，在非 Owner 节点主动拉取。
	if err := ownerCh.Publish("", queue, false, false, amqp.Publishing{Body: []byte("get-me")}); err != nil {
		return fmt.Errorf("在 Owner 节点发布失败: %w", err)
	}
	getConn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("重新连接非 Owner 节点 %s 失败: %w", *addr, err)
	}
	defer getConn.Close()
	getCh, err := getConn.Channel()
	if err != nil {
		return fmt.Errorf("非 Owner 节点打开 channel 失败: %w", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		d, ok, err := getCh.Get(queue, false)
		if err != nil {
			return fmt.Errorf("跨节点拉取失败: %w", err)
		}
		if ok {
			if string(d.Body) != "get-me" {
				return fmt.Errorf("跨节点拉取内容错误: %q", d.Body)
			}
			if err := d.Ack(false); err != nil {
				return fmt.Errorf("确认拉取的消息失败: %w", err)
			}
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("跨节点 basic.get 超时")
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("      Owner=%s 非 Owner=%s：发布转发 + 代理消费 + 跨节点拉取均通过（消息头 int64/int32 保真）\n",
		*clusterPeer, *addr)
	return nil
}
