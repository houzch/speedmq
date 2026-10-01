package main

import (
	"flag"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 仲裁队列（Quorum Queue）的真进程验证分两步，中间由外部脚本杀掉组 leader：
//
//	produce：声明仲裁队列并发布 3 条持久消息，等齐全部 confirm（= 已复制到多数派）
//	verify ：在幸存节点上把消息取回来，验证"已确认的消息没有随 leader 一起消失"
var (
	quorumProduce = flag.String("quorum-produce", "",
		"仲裁队列验证第一步：声明该名字的仲裁队列、发布 3 条已确认消息后退出")
	quorumVerify = flag.String("quorum-verify", "",
		"仲裁队列验证第二步：消费该名字的仲裁队列并核对 3 条消息")
)

const quorumProbeCount = 3

// runQuorumProduce 声明仲裁队列并发布若干条已确认消息。
//
// confirm 在仲裁队列上的含义是"已复制到多数派"，因此全部 confirm 到齐就等价于
// "这些消息已经在多数派落盘" —— 这正是后面杀 leader 验证的前提。
func runQuorumProduce() error {
	conn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("连接 %s 失败: %w", *addr, err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel 失败: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(*quorumProduce, true, false, false, false,
		amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return fmt.Errorf("在 %s 上声明仲裁队列 %s 失败: %w", *addr, *quorumProduce, err)
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("开启 confirm 失败: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, quorumProbeCount))
	for i := 0; i < quorumProbeCount; i++ {
		if err := ch.Publish("", *quorumProduce, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(fmt.Sprintf("quorum-%d", i)),
		}); err != nil {
			return fmt.Errorf("发布第 %d 条失败: %w", i, err)
		}
	}
	for i := 0; i < quorumProbeCount; i++ {
		select {
		case c := <-confirms:
			if !c.Ack {
				return fmt.Errorf("第 %d 条收到否定确认", i+1)
			}
		case <-time.After(20 * time.Second):
			return fmt.Errorf("等待第 %d 条确认超时（仲裁队列的 confirm 依赖复制到多数派）", i+1)
		}
	}
	fmt.Printf("      %s：已声明仲裁队列 %s，%d 条消息全部确认（= 已复制到多数派）\n",
		*addr, *quorumProduce, quorumProbeCount)
	return nil
}

// runQuorumVerify 在（幸存）节点上把消息取回来并核对条数。
func runQuorumVerify() error {
	conn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("连接 %s 失败: %w", *addr, err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel 失败: %w", err)
	}
	defer ch.Close()

	got := map[string]bool{}
	deadline := time.Now().Add(25 * time.Second)
	for len(got) < quorumProbeCount && time.Now().Before(deadline) {
		d, ok, err := ch.Get(*quorumVerify, false)
		if err != nil {
			return fmt.Errorf("从 %s 取消息失败: %w", *addr, err)
		}
		if !ok {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		got[string(d.Body)] = true
		if err := d.Ack(false); err != nil {
			return fmt.Errorf("确认失败: %w", err)
		}
	}
	if len(got) != quorumProbeCount {
		return fmt.Errorf("只取回 %d/%d 条消息（%v）", len(got), quorumProbeCount, got)
	}
	fmt.Printf("      %s：从仲裁队列 %s 取回全部 %d 条消息\n", *addr, *quorumVerify, quorumProbeCount)
	return nil
}
