# SpeedMQ 外部进程插件开发指南 —— Node.js

> 🌐 本文档提供多语言版本：[文档多语言索引](../i18n/README.md)

> **面向**：用 Node.js 给 SpeedMQ 写外部进程插件（sidecar）的开发者。
> **先读**：[外部进程插件（sidecar）开发指南](plugin-development.md)（心智模型 / 配置字段 / 线协议总表）。
> **示例工程**：工作区 `speedmq-plugin/nodejs/index.js`（仅 Node 标准库，**无需 npm 依赖**）。

---

## 1. 它跑起来是什么样

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三条要点：**你的进程是服务端**（等内核来连）；**对外端口由内核开**（配置 `protocols[].listeners`）；
**`prefix` 必须非空**（空前缀 = 不参与嗅探，连接不会被交给你；实测会被立刻断开，≤8 字节 ASCII）。

---

## 2. 三步跑起来

### 第一步：配置

`speedmqd.json`（**实际配置是标准 JSON，不能带注释**）：

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/speedmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### 第二步：启动

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### 第三步：验证

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. 实现要点

### 3.1 分帧

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Node 里用 `Buffer`：累积收到的字节，够一帧就切出来处理。

```js
drain() {
  while (this.buf.length >= 5) {
    const len = this.buf.readUInt32BE(0)
    if (this.buf.length < 4 + len) return      // 帧还没收全
    const kind = this.buf.readUInt8(4)
    const payload = this.buf.subarray(5, 4 + len)
    this.buf = this.buf.subarray(4 + len)
    this.dispatch(kind, payload)
  }
}
```

一条流 = 一条客户端连接；数据帧载荷是 `4 字节大端流号 + 原始字节`。

### 3.2 握手与心跳

内核**先发 Hello**，你回 `HelloAck`；内核校验 `name` 与 `api_version`（当前 `v1`）后接入。
之后每 2s 一个 `Ping`，回 `Pong` 即可（由读循环顺手处理，不需要定时器）。

### 3.3 异步模型（Node 版）

单线程事件循环，天然避免了"写交错"问题——但要注意**不要让读循环 await 反向调用**：

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` 是 `async`：它可能再 `await call('session.settle', …)`，因此绝不能写成同步等待。

### 3.4 语义桥（必须先认证）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` 不能省：连接的内核操作面在认证前没有身份，直接 `session.open` 会被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

```js
const ident = await call('core.authenticate', {
  stream, mechanism: 'PLAIN',
  response: Buffer.concat([Buffer.from(`\x00${user}\x00`), Buffer.from(password)]).toString('base64'),
})
await call('session.open', { stream, vhost: '/' })
const q = await call('session.declare_queue', { stream, exclusive: true, auto_delete: true })
await call('session.publish', { stream, routing_key: q.name,
  message: { body: Buffer.from('hi').toString('base64') } })
await call('session.consume', { stream, queue: q.name, prefetch: 32 })
```

投递由内核**正向回推**（`method = "session.deliver"`），处理后 `session.settle`
（`ack` / `requeue` / `reject`；投递编号全局唯一，不带流号）。

---

## 4. 代码走读（示例工程）

`speedmq-plugin/nodejs/index.js` 约 330 行：

| 位置 | 作用 |
| --- | --- |
| `u32()` / `Conn.send()` | 分帧读写 |
| `Conn.drain()` / `dispatch()` | 按帧解析与分发 |
| `Conn.call()` | 反向调用（`Promise` + `pending` 表，按 `reverse=true` 与 `id` 匹配） |
| `Stream` | 流读侧：`push/end/read` 组成一个异步队列 |
| `handleHello` | 校验并回 HelloAck |
| `handleForwardCall` / `handleMethod` | 正向调用（`session.deliver` + settle、`stats`） |
| `sessionDemo` | 认证 + 声明 + 发布 + 消费 |

---

## 5. 实测（本机复现）

Windows + Node v24；内核在 Docker（`speedmq:1.1.01`），插件在宿主机（`tcp://host.docker.internal:19011`）。

```
plugin=node-sidecar state=enabled         # /api/plugins
echo=[NDhello]                            # 客户端连内核端口 19012 发 "NDhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=node-sidecar addr=0.0.0.0:19011 version=0.1.0
内核已接入 plugin=node-sidecar peer=127.0.0.1:50987
握手完成 plugin=node-sidecar kernel=1.1.01
认证通过 user=guest
收到投递（session.deliver） queue=amq.gen-893688b01c62b2ad5763a7 delivery_id=1 body=hello from node sidecar
session 演示完成 queue=amq.gen-893688b01c62b2ad5763a7
流已打开 plugin=node-sidecar stream=1 remote=172.17.0.1:40450 local=172.17.0.2:19012
```

覆盖：**握手 → 认证 → 语义桥 → 投递回推 → 结算 → 字节流回显**。

---

## 6. Node.js 特有的注意点

- **`socket.write` 一次写一帧**：示例把整帧拼成一个 `Buffer` 再写，因此不需要额外加锁；
  若你把一帧拆成多次 `write`，就要自己保证顺序。
- **`stream.on('data')` 的 chunk 边界与帧无关**：必须自己累积缓冲（见 `drain()`）。
- **base64**：`message.body`、`core.authenticate.response` 在 JSON 里是 base64 字符串
  （`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`）。
- **不要在 `drain()` 里 `await`**：它是同步的帧切分函数；把异步处理交给 `handleForwardCall`。
- **ESM vs CJS**：示例用 CommonJS（`require`）以便 `node index.js` 直接跑；改 ESM 只需换 `import`。

---

## 7. 进阶

- 插件自带管理界面：配置里加 `console_url`（主文档 §5.8），管理后台「插件管理」页会出现直达入口。
- 独立部署（K8s / systemd）：`spawn: []` + `address: "tcp://<服务名>:19011"`，容器内监听 `0.0.0.0`。
- 端口复用：多个协议各给不同 `prefix`，内核按前缀把连接分发到各自插件。
