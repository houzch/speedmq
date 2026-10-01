package main

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// m8Cases 返回 M8 的用例：AMQP 事务（tx.select / tx.commit / tx.rollback）。
//
// 事务的核心语义不在报文里而在时序上，所以这三条用例断言的都是"客户端观察到什么"：
// 提交前不可见、回滚即丢弃、与发布确认互斥。
func m8Cases() []testCase {
	return []testCase{
		{"M8 事务：commit 前的发布对外不可见，commit 后可消费", testTxCommitVisibility},
		{"M8 事务：rollback 丢弃已缓冲的发布", testTxRollbackDiscards},
		{"M8 事务：与发布确认互斥（406，只关 channel）", testTxConfirmMutuallyExclusive},
	}
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
