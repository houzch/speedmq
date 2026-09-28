package raft

import (
	"context"
	"fmt"
	"sync"
)

// handler 是 RPC 处理器的统一签名（内存网络与 TCP 传输共用）。
//
// 用类型别名而不是自定义类型：Transport 接口里的处理器是匿名函数类型，
// 只有别名才与它"类型相同"，自定义类型会因方法签名不匹配而无法满足接口。
type handler = func(ctx context.Context, from string, payload []byte) ([]byte, error)

// memNetwork 是进程内"集群网络"：默认全连通，可整体切分区、可单独让某节点下线。
//
// 为什么需要它：pause_minority、故障切换、落后节点追赶这些语义只有在"能精确控制
// 任意两点之间可达性"的前提下才可测；真实 TCP 做不到（也不能在单测里依赖端口）。
type memNetwork struct {
	mu     sync.RWMutex
	nodes  map[string]*memTransport
	groups map[string]int  // 节点 → 分组号；为空表示默认全连通
	down   map[string]bool // 已下线的节点（双向不可达）
}

// NewMemNetwork 创建一个进程内集群网络。
func NewMemNetwork() MemNetwork { return newMemNetwork() }

func newMemNetwork() *memNetwork {
	return &memNetwork{
		nodes:  make(map[string]*memTransport),
		groups: make(map[string]int),
		down:   make(map[string]bool),
	}
}

// Transport 返回 id 对应的传输实现；同一 id 重复调用返回同一实例。
func (net *memNetwork) Transport(id string) Transport {
	net.mu.Lock()
	defer net.mu.Unlock()
	if t, ok := net.nodes[id]; ok {
		return t
	}
	t := &memTransport{net: net, id: id, handlers: make(map[string]handler)}
	net.nodes[id] = t
	return t
}

// Partition 重置拓扑为给定分组：组内互通、组间完全隔离。
//
// 未被任何分组提及的节点视为自成分组（与所有组隔离）—— 这样"忘记写某个节点"
// 的后果是明确的隔离，而不是意外地让它仍与所有人通话。
func (net *memNetwork) Partition(groups ...[]string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.groups = make(map[string]int)
	for i, g := range groups {
		for _, id := range g {
			net.groups[id] = i
		}
	}
}

// Heal 恢复默认全连通拓扑（清空分区与 Down 状态）。
func (net *memNetwork) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.groups = make(map[string]int)
	net.down = make(map[string]bool)
}

// Down 让节点 id 彻底不可达（模拟进程被杀）：收不到请求、也发不出请求。
func (net *memNetwork) Down(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.down[id] = true
}

// Up 恢复节点 id 的连通性。
func (net *memNetwork) Up(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.down, id)
}

// canReach 判断 a → b 的单向链路是否可用。
func (net *memNetwork) canReach(a, b string) bool {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return net.canReachLocked(a, b)
}

func (net *memNetwork) canReachLocked(a, b string) bool {
	if net.down[a] || net.down[b] {
		return false
	}
	if len(net.groups) == 0 {
		return true
	}
	ga, oka := net.groups[a]
	gb, okb := net.groups[b]
	if !oka || !okb {
		return false
	}
	return ga == gb
}

func (net *memNetwork) peer(id string) *memTransport {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return net.nodes[id]
}

// memTransport 是 memNetwork 中的单节点端点。
type memTransport struct {
	net *memNetwork
	id  string

	mu       sync.RWMutex
	handlers map[string]handler
	closed   bool
}

// Call 同步投递一次 RPC。处理器在独立协程中执行、结果经缓冲 channel 回传，
// 这样 ctx 取消时调用方能立刻返回，而处理器自然跑完退出（不会泄漏）。
func (t *memTransport) Call(ctx context.Context, to, method string, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("raft: 节点 %s 的传输已关闭", t.id)
	}
	if !t.net.canReach(t.id, to) {
		return nil, fmt.Errorf("raft: 节点 %s 到 %s 的链路不可用", t.id, to)
	}
	peer := t.net.peer(to)
	if peer == nil {
		return nil, fmt.Errorf("raft: 未知节点 %s", to)
	}
	h := peer.handlerFor(method)
	if h == nil {
		return nil, fmt.Errorf("raft: 节点 %s 未注册方法 %s", to, method)
	}

	type result struct {
		payload []byte
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := h(ctx, t.id, payload)
		ch <- result{payload: resp, err: err}
	}()
	select {
	case r := <-ch:
		return r.payload, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Serve 注册方法处理器；重复注册返回 ErrDuplicateMethod。
func (t *memTransport) Serve(method string, h handler) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("raft: 节点 %s 的传输已关闭", t.id)
	}
	if _, dup := t.handlers[method]; dup {
		return ErrDuplicateMethod
	}
	t.handlers[method] = h
	return nil
}

// Close 关闭端点（幂等）。
func (t *memTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	t.handlers = make(map[string]handler)
	return nil
}

func (t *memTransport) handlerFor(method string) handler {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return nil
	}
	return t.handlers[method]
}
