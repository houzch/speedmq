package spec

import (
	"github.com/houzch/swiftmq/internal/protocol/codec"
)

// 本文件覆盖 M8 所需的方法：AMQP 事务（class 90）。
//
// Tx.Select 的字段与 Confirm.Select 完全一致（一个 no-wait 位）；
// Tx.Commit / Tx.Rollback 没有参数，三个应答（*-Ok）也都没有字段 ——
// 事务的语义全在时序上（提交前不可见、回滚即丢弃），不在报文里。

// DecodeTxSelect 解析 Tx.Select 的参数区，返回是否 no-wait。
//
// 参数区**可能为空**：AMQP 的 bit 字段全部打包进同一个 octet，一个 bit 都不发等价于全为 false。
// 真实的 Go 客户端（amqp091-go 的 Channel.Tx()）就是这么发的（参数区为 nil），
// 因此这里必须容忍空参数区，否则所有主流 Go 客户端都会在 tx.select 上被拒。
func DecodeTxSelect(args []byte) (noWait bool, err error) {
	if len(args) == 0 {
		return false, nil
	}
	return codec.NewBitReader(codec.NewDecoder(args)).Bit()
}

// EncodeTxSelectOk 构造 Tx.Select-Ok 的参数区（无字段）。
func EncodeTxSelectOk() []byte { return nil }

// EncodeTxCommitOk 构造 Tx.Commit-Ok 的参数区（无字段）。
//
// Commit-Ok 只有在缓冲里的操作全部应用成功后才发出；因此客户端收到它
// 就可以认为这一批发布/确认已经生效（持久消息也已按 fsync 档位落盘）。
func EncodeTxCommitOk() []byte { return nil }

// EncodeTxRollbackOk 构造 Tx.Rollback-Ok 的参数区（无字段）。
func EncodeTxRollbackOk() []byte { return nil }
