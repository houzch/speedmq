package amqp091_test

import "testing"

// 本文件覆盖 Queue.Delete 的**幂等**语义：删除一个不存在的队列必须回 `Queue.Delete-Ok(0)`，
// 而不是 404。规则由实测 RabbitMQ 4.3 得到 —— 客户端常把"清理残留队列"写成无条件的
// `queue_delete`，若这里回 404（软错误）会顺手关掉 Channel，把后续操作全部带崩。
//
// 与之相对的另一条契约：管理面 `DELETE /api/queues/{vhost}/{name}` 对不存在的对象回 404，
// 那条由 management 层单独保持（见 test/unit/management），两者刻意不一致，不要混为一谈。

func TestQueueDeleteMissingQueueIsIdempotent(t *testing.T) {
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)

	// 删一个从未声明过的队列：必须是 Delete-Ok(0)，且 Channel 不得被关闭。
	// （若服务端回 404，下面的 queueDelete 会读到 Channel.Close 并直接判定失败。）
	if n := c.queueDelete(1, "smoke.no.such.queue"); n != 0 {
		t.Fatalf("删除不存在的队列应回 Delete-Ok(0)，实际 message_count=%d", n)
	}
	// 再删一次仍是 Ok(0)：幂等，不因"上次已不存在"而变化
	if n := c.queueDelete(1, "smoke.no.such.queue"); n != 0 {
		t.Fatalf("重复删除不存在的队列应仍回 Delete-Ok(0)，实际 message_count=%d", n)
	}

	// 显式证明软错误没有被误触发：同一个 Channel 仍能正常声明并删除队列
	c.queueDeclare(1, "smoke.delete.alive")
	if n := c.queueDelete(1, "smoke.delete.alive"); n != 0 {
		t.Fatalf("删除刚声明的空队列应回 Delete-Ok(0)，实际 message_count=%d", n)
	}
}
