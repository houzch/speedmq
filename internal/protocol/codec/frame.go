package codec

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Frame 是一条 AMQP 帧。
//
//	0      1         3         7                              size+7  size+8
//	+------+---------+---------+ - - - - - - - - - - - - - - +-------+
//	| type | channel |  size   |          payload            |  0xCE |
//	+------+---------+---------+ - - - - - - - - - - - - - - +-------+
type Frame struct {
	// Type 见 FrameMethod / FrameHeader / FrameBody / FrameHeartbeat。
	Type uint8
	// Channel 为 0 时表示 Connection 级方法。
	Channel uint16
	// Payload 在 FrameReader 的下一次 Read 之后失效，需要保留时必须复制。
	Payload []byte
}

var (
	// ErrFrameEnd 表示帧结束字节不是 0xCE（对应 501 FRAME_ERROR）。
	ErrFrameEnd = errors.New("codec: 帧结束字节不是 0xCE")
	// ErrFrameTooLarge 表示帧超过当前 frame-max（对应 501 FRAME_ERROR）。
	ErrFrameTooLarge = errors.New("codec: 帧超过 frame-max")
)

// FrameReader 从缓冲读取器顺序读取帧，并复用 payload 缓冲区以减少分配。
type FrameReader struct {
	r   *bufio.Reader
	hdr [7]byte
	max uint32
	buf []byte
}

// NewFrameReader 构造帧读取器；max 为允许的最大帧长度。
func NewFrameReader(r *bufio.Reader, max uint32) *FrameReader {
	return &FrameReader{r: r, max: max}
}

// SetMax 更新最大帧长度（在 frame-max 协商完成后调用）。
func (fr *FrameReader) SetMax(max uint32) { fr.max = max }

// Read 读取一条帧。心跳帧的 Payload 为空。
func (fr *FrameReader) Read() (Frame, error) {
	if _, err := io.ReadFull(fr.r, fr.hdr[:]); err != nil {
		return Frame{}, err
	}
	typ := fr.hdr[0]
	ch := binary.BigEndian.Uint16(fr.hdr[1:3])
	size := binary.BigEndian.Uint32(fr.hdr[3:7])

	if size > fr.max {
		return Frame{}, fmt.Errorf("%w: size=%d max=%d", ErrFrameTooLarge, size, fr.max)
	}
	if int(size) > cap(fr.buf) {
		fr.buf = make([]byte, size)
	}
	payload := fr.buf[:size]
	if size > 0 {
		if _, err := io.ReadFull(fr.r, payload); err != nil {
			return Frame{}, err
		}
	}
	end, err := fr.r.ReadByte()
	if err != nil {
		return Frame{}, err
	}
	if end != FrameEnd {
		return Frame{}, fmt.Errorf("%w: 收到 0x%02X", ErrFrameEnd, end)
	}
	return Frame{Type: typ, Channel: ch, Payload: payload}, nil
}

// FrameWriter 顺序写出帧。调用方负责串行化（同一连接同一时刻只应有一个写者）。
type FrameWriter struct {
	w   *bufio.Writer
	hdr [7]byte
}

// NewFrameWriter 构造帧写入器。
func NewFrameWriter(w *bufio.Writer) *FrameWriter { return &FrameWriter{w: w} }

// Write 写出一条帧（写入的是缓冲，需 Flush 才落到连接上）。
func (fw *FrameWriter) Write(f Frame) error {
	fw.hdr[0] = f.Type
	binary.BigEndian.PutUint16(fw.hdr[1:3], f.Channel)
	binary.BigEndian.PutUint32(fw.hdr[3:7], uint32(len(f.Payload)))
	if _, err := fw.w.Write(fw.hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := fw.w.Write(f.Payload); err != nil {
			return err
		}
	}
	return fw.w.WriteByte(FrameEnd)
}

// WriteHeartbeat 写出一个心跳帧。
func (fw *FrameWriter) WriteHeartbeat() error {
	return fw.Write(Frame{Type: FrameHeartbeat, Channel: 0})
}

// Flush 刷新底层缓冲。
func (fw *FrameWriter) Flush() error { return fw.w.Flush() }
