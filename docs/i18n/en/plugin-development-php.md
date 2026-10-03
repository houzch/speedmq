# SwiftMQ External-Process Plugin Development Guide — PHP

> **Audience**: developers writing external-process plugins (sidecars) for SwiftMQ in PHP.
> **Read first**: [External-Process Plugin (sidecar) Development Guide](plugin-development.md) (mental model / configuration fields / full wire-protocol table).
> **Example project**: workspace `swiftmq-plugin/php/sidecar_plugin.php` (standard library only, **no composer dependencies**).

---

## 1. What It Looks Like When Running

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Three key points: **your process is the server** (it waits for the kernel to connect); **the external port is opened by the kernel** (`protocols[].listeners`);
**`prefix` must be non-empty** (an empty prefix = does not participate in sniffing, and the connection will not be handed to you; empirically it is dropped immediately, ≤8 bytes ASCII).

---

## 2. Running It in Three Steps

### Step 1: Configuration

`swiftmqd.json` (**the actual configuration is standard JSON and cannot contain comments**):

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/swiftmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### Step 2: Start It

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Step 3: Verify

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
```

---

## 3. Implementation Highlights

### 3.1 Framing

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

PHP uses `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Data frame payload = `pack('N', $streamId) . raw bytes`.

### 3.2 Handshake and Heartbeat

The kernel **sends Hello first**, and you reply `HelloAck`; the kernel validates `name` and `api_version` (currently `v1`).
After that there is a `Ping` every 2s, and you reply `Pong`.

### 3.3 Concurrency Model: A Reentrant Frame Pump (PHP Has No Threads)

PHP CLI is single-threaded and blocking, so instead of "one thread per stream", this uses:

- The **read loop** (`serve()`) handles the handshake, heartbeat, opening streams, echoing data, and processing forward calls;
- **Echo** needs no extra state machine: on receiving `kindData`, immediately write it back as `kindData`;
- **Reverse calls** use `callAndWait()`: after sending `kindCall`, it reads and dispatches frames
  until it reads **its own** reply (`reverse=true` and matching `id`), then returns.

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

This means `dispatchOther()` **must be reentrant**: it may be called again inside a `callAndWait`
(for example, handling `session.deliver` in turn requires `session.settle`). That is exactly what the example does.

### 3.4 Semantic Bridge (Must Authenticate First)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` cannot be skipped: the connection's kernel operation surface has no identity before authentication, and a direct `session.open` is rejected
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

Deliveries are **pushed forward** by the kernel (`method = "session.deliver"`); after processing, `session.settle`
(`ack` / `requeue` / `reject`; the delivery number is globally unique and carries no stream number).

---

## 4. Code Walkthrough (Example Project)

`swiftmq-plugin/php/sidecar_plugin.php` is about 320 lines:

| Location | Purpose |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Frame read/write |
| `Conn::serve()` | Main read loop |
| `Conn::dispatchOther()` | Dispatch non-handshake frames (reentrant) |
| `Conn::callAndWait()` | Reverse call (reentrant frame pump) |
| `Conn::handleHello()` | Validate and reply HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Forward calls (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Auth + declare + publish + consume |

---

## 5. Empirical Test (Reproduced Locally)

Windows + PHP 7.4; the kernel runs in Docker (`swiftmq:1.1.01`), the plugin runs on the host (`tcp://host.docker.internal:19021`).

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

Covered: **handshake → auth → semantic bridge → delivery pushback → settle → byte-stream echo**.

---

## 6. PHP-Specific Notes

- **PHP 7.4 has no `mixed` return type** (that arrived in PHP 8.0): in the example, reverse calls return "any type",
  so **no return type declaration is written** (a `@return mixed` comment is used instead). Writing `: mixed` on 7.4 is a straight-up syntax error.
- **JSON number types**: `json_decode($s, true)` decodes integers as `int` by default, and large integers may become `float`;
  delivery numbers are fine at this example's scale, but if your numbers are very large, consider `JSON_BIGINT_AS_STRING`.
- **base64 is mandatory**: `message.body` and `core.authenticate.response` are base64 strings in JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **Do not use `pcntl_fork` for concurrency**: there is no pcntl on Windows, and forking would break the "one writer per connection" assumption;
  single-threaded + a reentrant frame pump is already sufficient (unless you need very heavy computation on a stream, which is better placed in an external service).
- **`stream_socket_accept` blocks**: the process lifecycle is managed by the kernel (`spawn`) or a supervisor;
  remember to handle `fread` returning `''` (EOF) → end that connection and return to accept.
- **Output buffering**: log with `fwrite(STDOUT, …)` followed by a newline, so the kernel can forward it line by line into the kernel log.

---

## 7. Advanced Topics

- Plugin with its own admin UI: add `console_url` in the configuration (main document §5.8), and a direct entry will appear on the admin console's "Plugin Management" page.
- Standalone deployment: `spawn: []` + `address: "tcp://<service-name>:19021"`, listening on `0.0.0.0` inside the container.
- For higher concurrency, you can turn the plugin into a long-running Swoole / RoadRunner, etc., but the **wire protocol stays the same**; just ensure:
  serialized frame writes, a non-blocking read loop, and reverse calls matched by `id` + `reverse`.
