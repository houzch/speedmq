# SpeedMQ 外部进程插件开发指南 —— PHP

> 🌐 本文档提供多语言版本：[文档多语言索引](../i18n/README.md)

> **面向**：用 PHP 给 SpeedMQ 写外部进程插件（sidecar）的开发者。
> **先读**：[外部进程插件（sidecar）开发指南](plugin-development.md)（心智模型 / 配置字段 / 线协议总表）。
> **示例工程**：工作区 `speedmq-plugin/php/sidecar_plugin.php`（仅标准库，**无需 composer 依赖**）。

---

## 1. 它跑起来是什么样

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

三条要点：**你的进程是服务端**（等内核来连）；**对外端口由内核开**（`protocols[].listeners`）；
**`prefix` 必须非空**（空前缀 = 不参与嗅探，连接不会被交给你；实测会被立刻断开，≤8 字节 ASCII）。

---

## 2. 三步跑起来

### 第一步：配置

`speedmqd.json`（**实际配置是标准 JSON，不能带注释**）：

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/speedmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### 第二步：启动

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### 第三步：验证

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP 用 `pack`/`unpack`：

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

数据帧载荷 = `pack('N', $streamId) . 原始字节`。

### 3.2 握手与心跳

内核**先发 Hello**，你回 `HelloAck`；内核校验 `name` 与 `api_version`（当前 `v1`）。
之后每 2s 一个 `Ping`，回 `Pong`。

### 3.3 并发模型：可重入的帧泵（PHP 没有线程）

PHP CLI 是单线程阻塞式的，因此这里不用"每条流一个线程"，而是：

- **读循环**（`serve()`）负责握手、心跳、开流、回显数据、处理正向调用；
- **回显**不需要额外状态机：收到 `kindData` 就立刻原样写回 `kindData`；
- **反向调用**用 `callAndWait()`：发送 `kindCall` 后，一边读帧一边分发，
  直到读到**自己的**应答（`reverse=true` 且 `id` 匹配）才返回。

```php
public function callAndWait(string $method, $params = null)
{
    $id = ++$this->nextId;
    $this->sendJson(KIND_CALL, ['id' => $id, 'method' => $method, 'reverse' => true, 'params' => $params]);
    for (;;) {
        [$kind, $payload] = $this->readFrame();
        if ($kind === KIND_REPLY) {
            $reply = json_decode($payload, true);
            if (!empty($reply['reverse']) && $reply['id'] === $id) {
                if (empty($reply['ok'])) throw new RuntimeException($reply['error'] ?? '调用失败');
                return $reply['data'] ?? null;
            }
            continue;
        }
        $this->dispatchOther($kind, $payload);   // 心跳/数据/正向调用照常处理
    }
}
```

这意味着 `dispatchOther()` **必须可重入**：它可能在一次 `callAndWait` 内部被再次调用
（例如处理 `session.deliver` 时又要 `session.settle`）。示例即是这么做的。

### 3.4 语义桥（必须先认证）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` 不能省：连接的内核操作面在认证前没有身份，直接 `session.open` 会被拒
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

```php
$ident = $this->callAndWait('core.authenticate', [
    'stream'    => $streamId,
    'mechanism' => 'PLAIN',
    // SASL PLAIN 响应：\x00<user>\x00<password>，字节在 JSON 里走 base64
    'response'  => base64_encode("\x00{$user}\x00{$password}"),
]);
$this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
$q = $this->callAndWait('session.declare_queue', ['stream' => $streamId, 'exclusive' => true, 'auto_delete' => true]);
$this->callAndWait('session.consume', ['stream' => $streamId, 'queue' => $q['name'], 'prefetch' => 32]);
```

投递由内核**正向回推**（`method = "session.deliver"`），处理后 `session.settle`
（`ack` / `requeue` / `reject`；投递编号全局唯一，不带流号）。

---

## 4. 代码走读（示例工程）

`speedmq-plugin/php/sidecar_plugin.php` 约 320 行：

| 位置 | 作用 |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | 分帧读写 |
| `Conn::serve()` | 主读循环 |
| `Conn::dispatchOther()` | 分发非握手帧（可重入） |
| `Conn::callAndWait()` | 反向调用（可重入帧泵） |
| `Conn::handleHello()` | 校验并回 HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | 正向调用（`session.deliver` + settle、`stats`） |
| `Conn::sessionDemo()` | 认证 + 声明 + 发布 + 消费 |

---

## 5. 实测（本机复现）

Windows + PHP 7.4；内核在 Docker（`speedmq:1.1.01`），插件在宿主机（`tcp://host.docker.internal:19021`）。

```
php -l sidecar_plugin.php  → No syntax errors detected
plugin=php-sidecar state=enabled          # /api/plugins
echo=[PHhello]                            # 客户端连内核端口 19022 发 "PHhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=php-sidecar addr=0.0.0.0:19021 version=0.1.0
内核已接入 plugin=php-sidecar peer=127.0.0.1:51006
握手完成 plugin=php-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-955afebb58d6b5307ba36e
流已打开 plugin=php-sidecar stream=1 remote=172.17.0.1:59664 local=172.17.0.2:19022
收到投递（session.deliver） queue=amq.gen-955afebb58d6b5307ba36e delivery_id=1 body=hello from php sidecar
```

覆盖：**握手 → 认证 → 语义桥 → 投递回推 → 结算 → 字节流回显**。

---

## 6. PHP 特有的注意点

- **PHP 7.4 没有 `mixed` 返回类型**（PHP 8.0 才有）：示例里反向调用返回"任意类型"，
  因此**不写返回类型声明**（用 `@return mixed` 注释）。在 7.4 上写 `: mixed` 会直接语法错误。
- **JSON 的数字类型**：`json_decode($s, true)` 默认把整数解成 `int`，大整数可能变 `float`；
  投递编号在本示例规模内没问题，若你的编号很大，考虑 `JSON_BIGINT_AS_STRING`。
- **base64 是必须的**：`message.body` 与 `core.authenticate.response` 在 JSON 里是 base64 字符串
  （`base64_encode` / `base64_decode($s, true)`）。
- **不要用 `pcntl_fork` 做并发**：Windows 上没有 pcntl，而且 fork 后会破坏"单连接单写者"的假设；
  单线程 + 可重入帧泵已经够用（除非你要在流上做很重的计算，那更适合放到外部服务）。
- **`stream_socket_accept` 是阻塞的**：进程生命周期由内核（`spawn`）或 supervisor 管理；
  记得处理 `fread` 返回 `''`（EOF）→ 结束该连接并回到 accept。
- **输出缓冲**：日志用 `fwrite(STDOUT, …)` 并跟一个换行，便于被内核按行转发到内核日志。

---

## 7. 进阶

- 插件自带管理界面：配置里加 `console_url`（主文档 §5.8），管理后台「插件管理」页会出现直达入口。
- 独立部署：`spawn: []` + `address: "tcp://<服务名>:19021"`，容器内监听 `0.0.0.0`。
- 需要更高并发时，可把插件改成常驻的 Swoole / RoadRunner 之类，但**线协议不变**，只需保证：
  写帧串行、读循环不阻塞、反向调用按 `id`+`reverse` 匹配。
