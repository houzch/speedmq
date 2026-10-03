# Hướng dẫn phát triển plugin tiến trình ngoài cho SwiftMQ —— PHP

> **Đối tượng**: các nhà phát triển dùng PHP để viết plugin tiến trình ngoài (sidecar) cho SwiftMQ.
> **Đọc trước**: [Hướng dẫn phát triển plugin tiến trình ngoài (sidecar)](plugin-development.md) (mô hình tư duy / trường cấu hình / bảng tổng hợp giao thức đường dây).
> **Dự án ví dụ**: workspace `swiftmq-plugin/php/sidecar_plugin.php` (chỉ thư viện chuẩn, **không cần phụ thuộc composer**).

---

## 1. Khi nó chạy thì trông như thế nào

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Ba điểm chính: **tiến trình của bạn là server** (chờ kernel kết nối tới); **cổng hướng ngoại do kernel mở** (`protocols[].listeners`);
**`prefix` phải khác rỗng** (tiền tố rỗng = không tham gia sniffing, kết nối sẽ không được giao cho bạn; thực tế sẽ bị ngắt ngay lập tức, ≤8 byte ASCII).

---

## 2. Ba bước để chạy

### Bước một: cấu hình

`swiftmqd.json` (**cấu hình thực tế là JSON chuẩn, không được có chú thích**):

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

### Bước hai: khởi động

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Bước ba: kiểm chứng

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
```

---

## 3. Những điểm cốt lõi khi hiện thực

### 3.1 Chia frame

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

PHP dùng `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Payload của frame dữ liệu = `pack('N', $streamId) . byte thô`.

### 3.2 Bắt tay và nhịp tim

Kernel **gửi Hello trước**, bạn trả `HelloAck`; kernel kiểm tra `name` và `api_version` (hiện tại `v1`).
Sau đó mỗi 2s một `Ping`, trả `Pong`.

### 3.3 Mô hình đồng thời: bơm frame tái nhập (PHP không có thread)

PHP CLI là đơn luồng kiểu chặn, nên ở đây không dùng "mỗi luồng một thread", mà là:

- **Vòng lặp đọc** (`serve()`) phụ trách bắt tay, nhịp tim, mở luồng, phản hồi dữ liệu, xử lý lời gọi xuôi;
- **Phản hồi (echo)** không cần máy trạng thái thêm: nhận `kindData` là ghi lại nguyên trạng vào `kindData` ngay;
- **Lời gọi ngược** dùng `callAndWait()`: sau khi gửi `kindCall`, vừa đọc frame vừa phân phối,
  tới khi đọc được phản hồi **của chính nó** (`reverse=true` và `id` khớp) mới trả về.

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

Điều này nghĩa là `dispatchOther()` **phải tái nhập được**: nó có thể bị gọi lại bên trong một lần `callAndWait`
(ví dụ khi xử lý `session.deliver` lại phải `session.settle`). Ví dụ làm đúng như vậy.

### 3.4 Cầu ngữ nghĩa (phải xác thực trước)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` không được bỏ qua: mặt thao tác kernel của kết nối chưa có danh tính trước khi xác thực, gọi thẳng `session.open` sẽ bị từ chối
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

Delivery được kernel **đẩy xuôi về** (`method = "session.deliver"`), xử lý xong thì `session.settle`
(`ack` / `requeue` / `reject`; mã delivery là duy nhất toàn cục, không mang số luồng).

---

## 4. Đọc hiểu mã (dự án ví dụ)

`swiftmq-plugin/php/sidecar_plugin.php` khoảng 320 dòng:

| Vị trí | Chức năng |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Đọc/ghi chia frame |
| `Conn::serve()` | Vòng lặp đọc chính |
| `Conn::dispatchOther()` | Phân phối frame không phải bắt tay (tái nhập được) |
| `Conn::callAndWait()` | Lời gọi ngược (bơm frame tái nhập) |
| `Conn::handleHello()` | Kiểm tra và trả HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Lời gọi xuôi (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Xác thực + khai báo + publish + consume |

---

## 5. Kiểm nghiệm thực tế (tái hiện trên máy cục bộ)

Windows + PHP 7.4; kernel trong Docker (`swiftmq:1.1.01`), plugin trên máy host (`tcp://host.docker.internal:19021`).

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

Bao phủ: **bắt tay → xác thực → cầu ngữ nghĩa → đẩy delivery về → quyết toán → phản hồi luồng byte**.

---

## 6. Những lưu ý đặc thù của PHP

- **PHP 7.4 không có kiểu trả về `mixed`** (PHP 8.0 mới có): trong ví dụ, lời gọi ngược trả về "kiểu bất kỳ",
  do đó **không viết khai báo kiểu trả về** (dùng chú thích `@return mixed`). Viết `: mixed` trên 7.4 sẽ lỗi cú pháp ngay.
- **Kiểu số của JSON**: `json_decode($s, true)` mặc định giải số nguyên thành `int`, số nguyên lớn có thể thành `float`;
  mã delivery trong quy mô ví dụ này không có vấn đề, nếu mã của bạn rất lớn, cân nhắc `JSON_BIGINT_AS_STRING`.
- **base64 là bắt buộc**: `message.body` và `core.authenticate.response` trong JSON là chuỗi base64
  (`base64_encode` / `base64_decode($s, true)`).
- **Đừng dùng `pcntl_fork` để làm đồng thời**: Windows không có pcntl, và sau fork sẽ phá vỡ giả định "một kết nối một bên ghi";
  đơn luồng + bơm frame tái nhập đã đủ dùng (trừ khi bạn muốn làm tính toán rất nặng trên luồng, việc đó phù hợp hơn với một dịch vụ bên ngoài).
- **`stream_socket_accept` là kiểu chặn**: vòng đời tiến trình do kernel (`spawn`) hoặc supervisor quản lý;
  nhớ xử lý việc `fread` trả về `''` (EOF) → kết thúc kết nối đó và quay lại accept.
- **Đệm đầu ra**: log dùng `fwrite(STDOUT, …)` kèm một ký tự xuống dòng, thuận tiện để kernel chuyển tiếp theo dòng vào log kernel.

---

## 7. Nâng cao

- Plugin có sẵn giao diện quản trị: thêm `console_url` vào cấu hình (tài liệu chính §5.8), trang「Quản lý plugin」của UI quản trị sẽ xuất hiện lối vào trực tiếp.
- Triển khai độc lập: `spawn: []` + `address: "tcp://<tên dịch vụ>:19021"`, lắng nghe `0.0.0.0` trong container.
- Khi cần đồng thời cao hơn, có thể đổi plugin thành dạng thường trú như Swoole / RoadRunner, nhưng **giao thức đường dây không đổi**, chỉ cần đảm bảo:
  ghi frame tuần tự, vòng lặp đọc không chặn, lời gọi ngược khớp theo `id`+`reverse`.
