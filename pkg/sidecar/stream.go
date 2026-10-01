package sidecar

import (
	"encoding/json"
	"io"
	"sync"
)

// Stream 是一条逻辑流：内核侧对应"一条客户端连接"，插件侧由插件自己读写。
//
// 它实现 io.ReadWriteCloser，两端都可以直接当成"一条连接"使用；底层的多路复用、
// 分块与帧编解码都被藏在这里。
//
// 背压（**已知边界**）：收到的数据块进一个有缓冲的通道，缓冲满时**阻塞整条连接的
// 分发协程**（即所有流一起等），而不是无限堆积内存。这是"可预测的内存占用"与
// "单流阻塞影响其他流"之间的取舍；按流限速/暂停读取属于后续优化。
type Stream struct {
	fw      *frameWriter
	id      uint32
	chunks  chan []byte
	done    chan struct{}
	onClose func(*Stream)

	mu     sync.Mutex
	cur    []byte
	err    error
	closed bool
}

func newStream(fw *frameWriter, id uint32, onClose func(*Stream)) *Stream {
	return &Stream{
		fw:      fw,
		id:      id,
		chunks:  make(chan []byte, 8),
		done:    make(chan struct{}),
		onClose: onClose,
	}
}

// ID 返回流号（排查日志用）。
func (s *Stream) ID() uint32 { return s.id }

// Read 实现 io.Reader。对端关闭后返回 io.EOF 或导致关闭的错误。
//
// 结束信号走独立的 done 通道而**不关闭 chunks**：分发协程随时可能在 push，
// 一旦 close(chunks) 与之竞争就会 send on closed channel panic。
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if len(s.cur) > 0 {
			n := copy(p, s.cur)
			s.cur = s.cur[n:]
			s.mu.Unlock()
			return n, nil
		}
		err := s.err
		s.mu.Unlock()
		if err != nil {
			return 0, err
		}
		select {
		case chunk := <-s.chunks:
			s.mu.Lock()
			s.cur = chunk
			s.mu.Unlock()
		case <-s.done:
			// 先把通道里已到达的块读完（select 可能命中任一分支），再返回结束原因。
			s.mu.Lock()
			if len(s.cur) > 0 {
				n := copy(p, s.cur)
				s.cur = s.cur[n:]
				s.mu.Unlock()
				return n, nil
			}
			err := s.err
			s.mu.Unlock()
			if err == nil {
				err = io.EOF
			}
			return 0, err
		}
	}
}

// Write 实现 io.Writer：按 MaxDataChunk 分块发出去。
func (s *Stream) Write(p []byte) (int, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if err := s.fw.writeData(s.id, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close 实现 io.Closer：通知对端关闭该流。可重复调用。
func (s *Stream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	payload, err := json.Marshal(Close{Stream: s.id, Reason: "closed by peer"})
	if err != nil {
		return err
	}
	werr := s.fw.write(kindClose, payload)
	s.fail(io.EOF)
	return werr
}

// push 由分发协程调用：把收到的数据块交给读取方。流已结束时静默丢弃。
func (s *Stream) push(data []byte) {
	// 必须拷贝：分发用的读缓冲会被下一次读取复用。
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case <-s.done:
		return
	default:
	}
	select {
	case s.chunks <- cp:
	case <-s.done:
	}
}

// fail 标记该流已结束并唤醒读取方（幂等）。
func (s *Stream) fail(err error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	s.mu.Unlock()
	close(s.done)
	if s.onClose != nil {
		s.onClose(s)
	}
}
