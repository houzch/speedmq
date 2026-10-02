package main

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// m8Cases 返回 M8 的用例：AMQP 事务（tx.select / tx.commit / tx.rollback）与 frame-max 协商。
//
// 事务的核心语义不在报文里而在时序上，所以这三条用例断言的都是"客户端观察到什么"：
// 提交前不可见、回滚即丢弃、与发布确认互斥。
func m8Cases() []testCase {
	return []testCase{
		{"M8 事务：commit 前的发布对外不可见，commit 后可消费", testTxCommitVisibility},
		{"M8 事务：rollback 丢弃已缓冲的发布", testTxRollbackDiscards},
		{"M8 事务：与发布确认互斥（406，只关 channel）", testTxConfirmMutuallyExclusive},
		{"M8-5 frame-max 协商：低于下限被拒（8192），下限值可用且大消息可分片", testFrameMaxNegotiation},
	}
}

// testFrameMaxNegotiation 覆盖 frame-max 的协商下限与大消息分片。
//
// 下限来自 RabbitMQ 4.x：协商值 < 8192 时服务端在 Tune 之后就关闭连接
// （实测日志："negotiated frame_max = 4096 is lower than the minimum allowed value (8192)"）。
// 这条差异是双跑对照（M8-5）抓到的，之前本实现会默默接受 4096。
func testFrameMaxNegotiation() error {
	_, tlsConf, err := dialConfig()
	if err != nil {
		return err
	}
	// 低于下限：必须连不上
	if conn, err := amqp.DialConfig(probeURL(), amqp.Config{FrameSize: 4096, TLSClientConfig: tlsConf}); err == nil {
		_ = conn.Close()
		return fmt.Errorf("frame_max=4096 低于协商下限（8192），连接竟然成功了")
	}

	// 下限值本身必须可用，且 300 KiB 的消息（远超 frame_max）要能完整往返 —— 即分片正确。
	conn, err := amqp.DialConfig(probeURL(), amqp.Config{FrameSize: 8192, TLSClientConfig: tlsConf})
	if err != nil {
		return fmt.Errorf("frame_max=8192 应当被接受: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel 失败: %w", err)
	}
	defer func() { _ = ch.Close() }()

	q, err := declareTempQueue(ch)
	if err != nil {
		return err
	}
	small := []byte("frame-min")
	big := make([]byte, 300*1024)
	for i := range big {
		big[i] = byte(i)
	}
	for _, body := range [][]byte{small, big} {
		if err := ch.Publish("", q, false, false, amqp.Publishing{Body: body}); err != nil {
			return fmt.Errorf("发布 %d 字节失败: %w", len(body), err)
		}
		d, ok, err := ch.Get(q, true)
		if err != nil {
			return fmt.Errorf("取回 %d 字节的消息失败: %w", len(body), err)
		}
		if !ok {
			return fmt.Errorf("未取回 %d 字节的消息", len(body))
		}
		if len(d.Body) != len(body) {
			return fmt.Errorf("消息长度不符: 收到 %d，发出 %d", len(d.Body), len(body))
		}
		for i := range body {
			if d.Body[i] != body[i] {
				return fmt.Errorf("%d 字节的消息在第 %d 个字节起内容不一致", len(body), i)
			}
		}
	}
	return nil
}

func testTxCommitVisibility() error {
	return withChannel(func(ch *amqp.Channel) error {
		q, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		if err := ch.Tx(); err != nil {
			return fmt.Errorf("开启事务失败: %w", err)
		}
		if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte("tx-1")}); err != nil {
			return fmt.Errorf("事务内发布失败: %w", err)
		}

		// 关键断言：commit 之前队列里必须看不到这条消息。
		if d, ok, err := ch.Get(q, true); err != nil {
			return fmt.Errorf("commit 前 get 失败: %w", err)
		} else if ok {
			return fmt.Errorf("commit 前消息已可见（body=%q）", d.Body)
		}

		if err := ch.TxCommit(); err != nil {
			return fmt.Errorf("提交事务失败: %w", err)
		}
		d, ok, err := ch.Get(q, true)
		if err != nil {
			return fmt.Errorf("commit 后 get 失败: %w", err)
		}
		if !ok || string(d.Body) != "tx-1" {
			return fmt.Errorf("commit 后未取到消息: ok=%v body=%q", ok, d.Body)
		}
		return nil
	})
}

func testTxRollbackDiscards() error {
	return withChannel(func(ch *amqp.Channel) error {
		q, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		if err := ch.Tx(); err != nil {
			return fmt.Errorf("开启事务失败: %w", err)
		}
		if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte("tx-dropped")}); err != nil {
			return fmt.Errorf("事务内发布失败: %w", err)
		}
		if err := ch.TxRollback(); err != nil {
			return fmt.Errorf("回滚事务失败: %w", err)
		}

		// 回滚之后这条消息必须彻底消失，且**不能**在后续 commit 里冒出来。
		if d, ok, err := ch.Get(q, true); err != nil {
			return fmt.Errorf("回滚后 get 失败: %w", err)
		} else if ok {
			return fmt.Errorf("回滚后消息仍可见（body=%q）", d.Body)
		}
		if err := ch.TxCommit(); err != nil {
			return fmt.Errorf("回滚后再提交失败: %w", err)
		}
		if _, ok, err := ch.Get(q, true); err != nil {
			return fmt.Errorf("再次 get 失败: %w", err)
		} else if ok {
			return fmt.Errorf("回滚的消息在后续 commit 后出现了")
		}
		return nil
	})
}

func testTxConfirmMutuallyExclusive() error {
	conn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("拨号失败: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel 失败: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("开启 confirm 失败: %w", err)
	}
	// 已 confirm 的通道上再 tx.select 必须被拒：406 PRECONDITION_FAILED，且只关 channel。
	err = ch.Tx()
	if err == nil {
		return fmt.Errorf("confirm 与事务应互斥，但 tx.select 成功了")
	}
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) {
		return fmt.Errorf("返回的不是 AMQP 协议错误: %v", err)
	}
	if amqpErr.Code != amqp.PreconditionFailed {
		return fmt.Errorf("期望 406 PRECONDITION_FAILED，实际 code=%d reason=%q", amqpErr.Code, amqpErr.Reason)
	}
	return nil
}
