// Package sidecar 定义**外部进程插件（B 形态）**与内核之间的通信协议，并给出两端实现：
//   - 内核侧用 Client 连接并驱动插件进程；
//   - 插件进程侧用 Server 接收内核连接、处理它的请求。
//
// 为什么自研协议而不是引 gRPC：本项目的构建承诺是"零第三方依赖、可离线复现"，
// 而这里需要的只是"一条本机连接上的多路复用 + 握手 + 心跳"，用标准库 100 行就能表达清楚。
// 协议刻意做成**长度前缀 + 单字节类型 + 载荷**，载荷要么是 JSON（控制面）要么是原始字节
// （数据面），因此搬消息体时没有 base64 的 33% 开销。
//
// 边界（与 A 形态的差别）：
//   - 协议插件的 `Sniff` 留在内核侧：它决定"这条入站连接该交给谁"，属于接入层职责；
//   - 其余一切都可远程：握手、心跳、方法调用，以及**客户端字节流的双向转发**。
//     没有文件描述符传递（Windows 上没有 SCM_RIGHTS），所以数据面走代理 —— 这也是
//     "跨平台可用"与"零依赖"之间的取舍，代价是本机多一次内存拷贝。
package sidecar

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ProtocolVersion 是线协议版本。内核与插件进程必须一致，否则握手被拒绝。
//
// 它和 pkg/plugin.APIVersion 是两件事：前者约束"字节流怎么讲"，
// 后者约束"插件能调用内核的哪些能力"。版本不匹配时都要**明确拒绝**，
// 而不是"尽量兼容"——静默降级会让插件作者以为自己的代码生效了。
const ProtocolVersion = "v1"

const (
	// maxFrame 是单帧上限。16 MiB 与内核的 frame-max 同量级：
	// 既容得下一块数据，又不至于让一个畸形长度声明把内存打爆。
	maxFrame = 16 << 20
	// MaxDataChunk 是数据帧单块上限。分块的意义在于"边读边转"：
	// 内存占用与消息大小无关，大消息不会被整体读进内存。
	MaxDataChunk = 64 << 10
)

// kind 是帧类型（单字节）。
type kind uint8

const (
	// kindHello：内核 → 插件，握手请求。
	kindHello kind = 1
	// kindHelloAck：插件 → 内核，握手应答（含插件元数据，或拒绝原因）。
	kindHelloAck kind = 2
	// kindPing：内核 → 插件，心跳。
	kindPing kind = 3
	// kindPong：插件 → 内核，心跳应答。
	kindPong kind = 4
	// kindCall：内核 → 插件，方法调用。
	kindCall kind = 5
	// kindReply：插件 → 内核，方法调用结果。
	kindReply kind = 6
	// kindOpen：内核 → 插件，为一条客户端连接打开逻辑流。
	kindOpen kind = 7
	// kindOpenAck：插件 → 内核，逻辑流打开结果。
	kindOpenAck kind = 8
	// kindData：双向，逻辑流上的原始字节。
	kindData kind = 9
	// kindClose：双向，关闭逻辑流。
	kindClose kind = 10
)

// ErrProtocol 表示对端发来的字节不符合本协议（长度越界、类型未知、载荷截断）。
var ErrProtocol = errors.New("sidecar: 协议错误")

// frameWriter 串行化整帧写出。
//
// 必须串行：心跳、控制面应答与数据面块都可能来自不同协程，
// 交错写会让对端把半个帧当成帧头，直接污染整条连接。
type frameWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (fw *frameWriter) write(k kind, payload []byte) error {
	if len(payload)+1 > maxFrame {
		return fmt.Errorf("%w: 帧过大（%d 字节）", ErrProtocol, len(payload)+1)
	}
	buf := make([]byte, 4+1+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(1+len(payload)))
	buf[4] = byte(k)
	copy(buf[5:], payload)
	fw.mu.Lock()
	defer fw.mu.Unlock()
	_, err := fw.w.Write(buf)
	return err
}

// writeData 发一块数据帧：载荷 = 4 字节流号 + 原始字节。
//
// 流号放在载荷里而不是帧头，是为了让"数据帧"与"控制帧"共用同一套帧格式，
// 读侧只需按类型分发，不必为数据面定义第二套头部。
func (fw *frameWriter) writeData(stream uint32, data []byte) error {
	for len(data) > 0 {
		n := len(data)
		if n > MaxDataChunk {
			n = MaxDataChunk
		}
		payload := make([]byte, 4+n)
		binary.BigEndian.PutUint32(payload, stream)
		copy(payload[4:], data[:n])
		if err := fw.write(kindData, payload); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// readFrame 读一帧，返回类型与载荷。
func readFrame(r *bufio.Reader) (kind, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[:4])
	if n == 0 {
		// 长度字段至少要含类型字节：0 说明对端在说另一种语言。
		return 0, nil, fmt.Errorf("%w: 长度字段为 0", ErrProtocol)
	}
	if n > maxFrame {
		return 0, nil, fmt.Errorf("%w: 帧过大（%d 字节）", ErrProtocol, n)
	}
	k := kind(head[4])
	payload := make([]byte, n-1)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return k, payload, nil
}

// splitData 从数据帧载荷里取出流号与数据。
func splitData(payload []byte) (uint32, []byte, error) {
	if len(payload) < 4 {
		return 0, nil, fmt.Errorf("%w: 数据帧缺少流号", ErrProtocol)
	}
	return binary.BigEndian.Uint32(payload[:4]), payload[4:], nil
}
