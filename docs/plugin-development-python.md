# SpeedMQ 外部进程插件开发指南 —— Python

> 🌐 本文档提供多语言版本：[文档多语言索引](../i18n/README.md)

> **面向**：用 Python 给 SpeedMQ 写外部进程插件（sidecar）的开发者。
> **先读**：[外部进程插件（sidecar）开发指南](plugin-development.md) —— 那里讲了心智模型、配置字段与线协议总表；
> 本文只讲 **Python 怎么落地**，以及本机实测过的步骤与结果。
> **示例工程**：工作区 `speedmq-plugin/python/sidecar_plugin.py`（仅标准库，零第三方依赖）。

---

## 1. 它跑起来是什么样

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三条要点（容易搞错，先记牢）：

1. **你的进程是服务端**：监听一个本机地址，等内核来连（`plugins.<名>.sidecar.address`）。
2. **对外业务端口由内核开**：客户端连的是内核的端口，字节被代理给你（`protocols[].listeners`）。
3. **`prefix` 必须非空**：内核按前缀嗅探决定"这条连接交给谁"。`prefix` 为空表示**不参与嗅探**，
   连接在它自己的监听上也不会被交给你（实测：连接会被立刻断开）。前缀长度 ≤ 8 字节，ASCII。

---

## 2. 三步跑起来

### 第一步：在配置里声明插件

`speedmqd.json`（**实际配置是标准 JSON，不能带注释**）：

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/speedmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` 是**内核去连你的地址**（内核是客户端，插件是服务端）。
- `spawn` 留空 = 内核只连不拉，进程由你自己管（systemd / supervisor / compose）。
- `prefix` **必须非空**：客户端发来的首字节须以此开头（内核按前缀嗅探决定连接交给谁）。
- `listeners` 是对外端口，由内核打开（客户端连的是内核，不是你）。
- 跨容器部署时 `address` 用**服务名**（如 `tcp://py-sidecar:19001`），且插件要监听 `0.0.0.0`。

### 第二步：启动

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### 第三步：验证

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. 实现要点

### 3.1 分帧（唯一必须自己写对的字节层）

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

数据帧的载荷 = `4 字节大端流号 + 原始字节`；控制面载荷是 JSON。

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 握手与心跳

内核连上后**先发 Hello**，你必须回一帧 `HelloAck`；内核会校验
`name == 配置里的插件名` 且 `api_version == 内核的 APIVersion`（当前 `v1`）。
之后内核每 2s 发一次 `Ping`，你回 `Pong` 即可（不回会被判死）。

### 3.3 逻辑流

`kindOpen` 到达 → **先回 `OpenAck`**，再开始服务；`kindData` 到达 → 原样（或按你的协议解析后）写回
`kindData`；处理结束 → 发 `kindClose`。一条流 = 一条客户端连接。

### 3.4 反向调用与内核语义桥

插件 → 内核的调用走 `kindCall` 且 `"reverse": true`，内核在同一条连接上回 `kindReply`。
**顺序很重要**：

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **不能省**：连接的内核操作面在认证前没有身份，直接 `session.open` 会被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。参数就是你协议里解析出的 SASL 响应：

```python
plain = b"\x00" + user.encode() + b"\x00" + password.encode()
ident = call("core.authenticate", {
    "stream": stream_id, "mechanism": "PLAIN",
    "response": base64.b64encode(plain).decode(),   # 字节在 JSON 里是 base64
})
# 拿到的会话与进程内协议插件完全同一套语义
call("session.open", {"stream": stream_id, "vhost": "/"})
q = call("session.declare_queue", {"stream": stream_id, "exclusive": True, "auto_delete": True})
call("session.publish", {"stream": stream_id, "routing_key": q["name"],
                         "message": {"body": base64.b64encode(b"hi").decode()}})
call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
```

消费的投递由内核**正向回推**（`kindCall`，`method = "session.deliver"`），处理完用
`session.settle` 结算（`ack` / `requeue` / `reject`，投递编号全局唯一，不需要流号）。

### 3.5 并发模型（Python 版）

| 角色 | 线程 |
| --- | --- |
| 帧读循环 | 每条内核连接一个 |
| 流处理 | 每条流一个（因此多条客户端连接可并发） |
| 正向调用处理 | 每个调用一个 |

**必须注意**：读循环里**不能**同步等待反向调用应答（会死锁）——正向调用（`session.deliver`）
要丢到独立线程处理，因为它在处理中可能又要发起 `session.settle`。写帧必须加锁串行化。

---

## 4. 代码走读（示例工程）

`speedmq-plugin/python/sidecar_plugin.py` 约 320 行，重点函数：

| 位置 | 作用 |
| --- | --- |
| `read_frame` / `Conn.send` | 分帧读写（长度前缀 + kind） |
| `Conn.call` | 反向调用：编号 → 发送 → 等应答（按 `reverse=true` 与 `id` 匹配） |
| `Conn.serve` | 帧读循环与分发 |
| `Conn._handle_hello` | 校验插件名/API 版本并回 HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | 流生命周期与回显 |
| `Conn._dispatch` | 处理正向调用：`session.deliver`（含 settle）、`stats` |
| `Conn._session_demo` | 认证 + 声明队列 + 发布 + 消费 |

---

## 5. 实测（本机复现）

环境：Windows + Python 3.12；内核跑在 Docker（`speedmq:1.1.01`），插件跑在宿主机，
内核用 `tcp://host.docker.internal:19001` 连它。

```
plugin=py-sidecar state=enabled           # /api/plugins
echo=[PYhello]                            # 客户端连内核端口 19002 发 "PYhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
内核已接入 plugin=py-sidecar peer=('127.0.0.1', 52864)
握手完成 plugin=py-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-9e5d0d695639bad12ce919
收到投递（session.deliver） queue=amq.gen-9e5d0d695639bad12ce919 delivery_id=1 body=hello from python sidecar
流已打开 plugin=py-sidecar stream=1 remote=172.17.0.1:43856 local=172.17.0.2:19002
```

覆盖到的链路：**握手 → 认证 → 语义桥（声明/发布/消费）→ 投递回推 → 结算 → 字节流回显**。

---

## 6. Python 特有的注意点

- **不要用 `time.sleep` 等心跳**：读循环是阻塞式的，靠内核的 Ping 维持连接即可；
  若给 socket 设了读超时，记得把超时当成"连接结束"处理（内核 `kill -9` 时 socket 可能不及时关闭）。
- **`json.dumps` 默认会加空格**：示例用 `separators=(",", ":")` 只是为了日志好看，协议本身不要求。
- **字节就是 base64**：`message.body`、`core.authenticate.response` 在 JSON 里都是 base64 字符串，
  别忘了 `base64.b64encode/decode`。
- **写帧要加锁**：心跳、应答、数据块来自不同线程，交错写会污染整条连接（示例用 `threading.Lock`）。
- 用 `asyncio` 也可以，但要保证"写串行 + 读循环不阻塞"，思路与线程版一致。

---

## 7. 进阶

- 想给插件配自己的管理界面：在配置里加 `console_url`，管理后台的「插件管理」页会出现直达入口
  （见主文档 §5.8）。
- 插件把自己做成独立服务、由 systemd / K8s 管理：`spawn: []` + `restart: "never"`，由外部拉起。
- 需要多协议共存：在同一条监听上让多个插件用不同 `prefix`，或各开专属端口。
