package meta

import (
	"encoding/json"
	"fmt"
	"sync"
)

// command 是一条元数据日志命令的线格式：{"op":"queue.put","payload":{…}}。
//
// payload 用 RawMessage 原样保留，是为了把它**不经二次解码**地透传给 Applier.ApplyMeta ——
// 内核拿到的就是调用方写入的那份记录 JSON，字段语义与日志里落盘的内容完全一致。
type command struct {
	Op      Op              `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

// encodeCommand 把一次变更编码为日志命令；同一份字节既进 Raft 日志，也用于 follower 转发。
func encodeCommand(op Op, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("编码元数据记录失败 (op=%s): %w", op, err)
	}
	data, err := json.Marshal(command{Op: op, Payload: raw})
	if err != nil {
		return nil, fmt.Errorf("编码元数据命令失败 (op=%s): %w", op, err)
	}
	return data, nil
}

// fsm 是 Raft 之上的元数据状态机，同时承载单机后端复用的那套应用逻辑。
//
// 把"状态 + 应用逻辑"独立成类型，是为了让 Raft 的 Apply/Restore 与单机写入走**同一段**状态变换：
// 两套实现的话，幂等性、绑定去重这类语义迟早会分叉（types.go 的约定 1/2）。
type fsm struct {
	// mu 保护 st/applied。Raft 是单协程调用 Apply，但 State()/Status() 会被管理面并发读取。
	mu      sync.RWMutex
	st      State
	applied uint64
	applier Applier
	logger  Logger
}

// newFSM 构造状态机；applier/logger 由 Open 保证非空。
func newFSM(applier Applier, logger Logger) *fsm {
	return &fsm{st: newState(), applier: applier, logger: logger}
}

// Apply 实现 raft.FSM：按索引顺序应用一条已提交的日志。
//
// 先在锁内改状态、解锁后再回调 Applier：Applier 可能回头读 State()，
// 持锁回调会自锁（AGENTS.md §6 "禁止在持锁状态下调用外部回调"）。
func (f *fsm) Apply(index uint64, data []byte) (any, error) {
	// leader 当选时会追加一条"无操作"条目（Data 为空），用于提交此前任期的日志。
	// 它没有记录体：既不改变状态，也不该给内核下发任何变更 —— 直接当成功返回。
	if len(data) == 0 {
		return nil, nil
	}
	var cmd command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("解析元数据日志命令失败 (index=%d): %w", index, err)
	}
	f.mu.Lock()
	err := applyToState(&f.st, cmd.Op, cmd.Payload)
	if err == nil {
		f.applied++
	}
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := f.applier.ApplyMeta(cmd.Op, cmd.Payload); err != nil {
		// 不静默吞掉：向上返回后 Raft 会重试或停机，让问题浮出来；
		// 吞掉的话内核拓扑与已提交日志会悄悄分叉，而日志是最难对账的东西。
		f.logger.Error("元数据变更下发内核失败", "op", string(cmd.Op), "index", index, "err", err)
		return nil, fmt.Errorf("元数据变更 %s 下发内核失败 (index=%d): %w", cmd.Op, index, err)
	}
	return nil, nil
}

// Snapshot 实现 raft.FSM：返回完整状态的 JSON（就是 State 本身的线格式，不含任何额外包装）。
func (f *fsm) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, err := json.Marshal(f.st)
	if err != nil {
		return nil, fmt.Errorf("编码元数据快照失败: %w", err)
	}
	return data, nil
}

// Restore 实现 raft.FSM：用快照整体替换状态，并且**只**回调一次 RestoreMeta。
//
// 这里不逐条重放 ApplyMeta：快照代表"截至某一索引的完整状态"，
// 逐条重放既无必要，也会在内核侧产生与真实日志次序不符的中间态。
func (f *fsm) Restore(data []byte) error {
	st, err := decodeState(data)
	if err != nil {
		return fmt.Errorf("解析元数据快照失败: %w", err)
	}
	f.mu.Lock()
	f.st = st
	// 快照已经把之前的条目整体压缩掉了，重放计数从恢复点重新累计，
	// 否则这个观测值会把已压缩的条目重复计入，失去"本节点应用了多少条"的含义。
	f.applied = 0
	f.mu.Unlock()
	if err := f.applier.RestoreMeta(st); err != nil {
		return fmt.Errorf("用元数据快照重建内核拓扑失败: %w", err)
	}
	return nil
}

// state 返回当前状态（只读视图，调用方不得修改其中的 map/slice）。
func (f *fsm) state() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.st
}

// appliedRecords 返回累计应用过的条目数。
func (f *fsm) appliedRecords() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied
}

// newState 返回各集合都已初始化的空状态。
//
// 必须在构造时就初始化 map：nil map 上的写入会 panic，而本仓库禁止任何运行期 panic 路径。
func newState() State {
	return State{
		VHosts:      map[string]VHost{},
		Exchanges:   map[string]Exchange{},
		Queues:      map[string]Queue{},
		Bindings:    []Binding{},
		Users:       map[string]User{},
		Permissions: map[string]Permission{},
	}
}

// decodeState 解析 State 的 JSON 表示，并补齐 JSON 的 null / 缺字段造成的 nil 集合。
func decodeState(data []byte) (State, error) {
	st := newState()
	if err := json.Unmarshal(data, &st); err != nil {
		return State{}, err
	}
	normalizeState(&st)
	return st, nil
}

// normalizeState 让六个集合都可写，避免反序列化结果在后续 put 时踩到 nil map。
func normalizeState(st *State) {
	if st.VHosts == nil {
		st.VHosts = map[string]VHost{}
	}
	if st.Exchanges == nil {
		st.Exchanges = map[string]Exchange{}
	}
	if st.Queues == nil {
		st.Queues = map[string]Queue{}
	}
	if st.Bindings == nil {
		st.Bindings = []Binding{}
	}
	if st.Users == nil {
		st.Users = map[string]User{}
	}
	if st.Permissions == nil {
		st.Permissions = map[string]Permission{}
	}
}

// counts 返回各集合的规模（Status 展示用）。
//
// 注意这里刻意**不**统计 per-vhost 的次级索引：状态里只保存权威记录，
// 派生视图由内核自己维护，避免同一份数据两处存储、两处可能不一致。
func counts(st State) (queues, exchanges, bindings, users int) {
	return len(st.Queues), len(st.Exchanges), len(st.Bindings), len(st.Users)
}

// decodeRecord 把一条 Op 的 payload 解析成对应记录类型。
func decodeRecord[T any](op Op, payload []byte) (T, error) {
	var rec T
	if len(payload) == 0 {
		return rec, fmt.Errorf("元数据操作 %s 缺少记录体", op)
	}
	if err := json.Unmarshal(payload, &rec); err != nil {
		return rec, fmt.Errorf("解析 %s 的记录失败: %w", op, err)
	}
	return rec, nil
}

// applyToState 把一条变更作用到状态上；这是单机与 Raft 两条路径共用的唯一状态变换。
//
// 幂等性的来源是"集合语义"：put 是覆盖式写入、delete 是删键（键不存在也算成功）、
// 绑定按身份去重后追加 —— 同一批 Op 重放任意次都得到同一状态。
//
// delete 刻意**不**级联（删 vhost 不连带删它的交换机/队列）：级联会把多条记录的删除
// 藏进一条 Op，内核收到的 ApplyMeta 就看不到被删掉的每个对象；由调用方显式发出多条 Op，
// 增量下发的粒度才与最终状态一一对应。
func applyToState(st *State, op Op, payload []byte) error {
	switch op {
	case OpPutVHost:
		rec, err := decodeRecord[VHost](op, payload)
		if err != nil {
			return err
		}
		st.VHosts[rec.Name] = rec
	case OpDeleteVHost:
		rec, err := decodeRecord[VHost](op, payload)
		if err != nil {
			return err
		}
		delete(st.VHosts, rec.Name)

	case OpPutExchange:
		rec, err := decodeRecord[Exchange](op, payload)
		if err != nil {
			return err
		}
		st.Exchanges[Key(rec.VHost, rec.Name)] = rec
	case OpDeleteExchange:
		rec, err := decodeRecord[Exchange](op, payload)
		if err != nil {
			return err
		}
		delete(st.Exchanges, Key(rec.VHost, rec.Name))

	case OpPutQueue:
		rec, err := decodeRecord[Queue](op, payload)
		if err != nil {
			return err
		}
		st.Queues[Key(rec.VHost, rec.Name)] = rec
	case OpDeleteQueue:
		rec, err := decodeRecord[Queue](op, payload)
		if err != nil {
			return err
		}
		delete(st.Queues, Key(rec.VHost, rec.Name))

	case OpPutBinding:
		rec, err := decodeRecord[Binding](op, payload)
		if err != nil {
			return err
		}
		key, err := bindingKey(rec)
		if err != nil {
			return err
		}
		idx, err := indexBinding(st.Bindings, key)
		if err != nil {
			return err
		}
		if idx >= 0 {
			// 覆盖式写入并保留原位置：重放同一批 Op 时 slice 的内容与顺序都不变。
			st.Bindings[idx] = rec
			return nil
		}
		st.Bindings = append(st.Bindings, rec)
	case OpDeleteBinding:
		rec, err := decodeRecord[Binding](op, payload)
		if err != nil {
			return err
		}
		key, err := bindingKey(rec)
		if err != nil {
			return err
		}
		idx, err := indexBinding(st.Bindings, key)
		if err != nil {
			return err
		}
		if idx >= 0 {
			st.Bindings = append(st.Bindings[:idx], st.Bindings[idx+1:]...)
		}

	case OpPutUser:
		rec, err := decodeRecord[User](op, payload)
		if err != nil {
			return err
		}
		st.Users[rec.Name] = rec
	case OpDeleteUser:
		rec, err := decodeRecord[User](op, payload)
		if err != nil {
			return err
		}
		delete(st.Users, rec.Name)

	case OpPutPermission:
		rec, err := decodeRecord[Permission](op, payload)
		if err != nil {
			return err
		}
		st.Permissions[PermissionKey(rec.VHost, rec.User)] = rec
	case OpDeletePermission:
		rec, err := decodeRecord[Permission](op, payload)
		if err != nil {
			return err
		}
		delete(st.Permissions, PermissionKey(rec.VHost, rec.User))

	default:
		// 未知 Op 是确定性错误：所有节点会对同一条日志做同样的失败，
		// 因此它不会破坏状态一致性，反而能让坏命令尽早暴露。
		return fmt.Errorf("未知的元数据操作 %q", op)
	}
	return nil
}

// bindingKey 生成绑定的身份键：vhost + source + destination + destination_type + routing_key + args。
//
// 参数表参与身份，是因为 RabbitMQ 允许同一对端点之间并存多条"路由键相同、参数不同"的绑定
// （headers 交换机尤其常见），只用三元组去重会把它们误判成同一条。
// json.Marshal 对 map 键排序，所以同一份参数表得到的字符串是确定的。
func bindingKey(b Binding) (string, error) {
	args, err := json.Marshal(b.Arguments)
	if err != nil {
		return "", fmt.Errorf("编码绑定参数失败 (vhost=%s source=%s): %w", b.VHost, b.Source, err)
	}
	return Key(b.VHost, b.Source) + "\x00" + b.Destination + "\x00" + b.DestinationType +
		"\x00" + b.RoutingKey + "\x00" + string(args), nil
}

// indexBinding 返回身份键命中的绑定下标，未命中返回 -1。
func indexBinding(list []Binding, key string) (int, error) {
	for i := range list {
		k, err := bindingKey(list[i])
		if err != nil {
			return -1, err
		}
		if k == key {
			return i, nil
		}
	}
	return -1, nil
}
