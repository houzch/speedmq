# Hướng dẫn phát triển plugin tiến trình ngoài cho SwiftMQ —— Node.js

> **Đối tượng**: các nhà phát triển dùng Node.js để viết plugin tiến trình ngoài (sidecar) cho SwiftMQ.
> **Đọc trước**: [Hướng dẫn phát triển plugin tiến trình ngoài (sidecar)](plugin-development.md) (mô hình tư duy / trường cấu hình / bảng tổng hợp giao thức đường dây).
> **Dự án ví dụ**: workspace `swiftmq-plugin/nodejs/index.js` (chỉ thư viện chuẩn của Node, **không cần phụ thuộc npm**).

---

## 1. Khi nó chạy thì trông như thế nào

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Ba điểm chính: **tiến trình của bạn là server** (chờ kernel kết nối tới); **cổng hướng ngoại do kernel mở** (cấu hình `protocols[].listeners`);
**`prefix` phải khác rỗng** (tiền tố rỗng = không tham gia sniffing, kết nối sẽ không được giao cho bạn; thực tế sẽ bị ngắt ngay lập tức, ≤8 byte ASCII).

---

## 2. Ba bước để chạy

### Bước một: cấu hình

`swiftmqd.json` (**cấu hình thực tế là JSON chuẩn, không được có chú thích**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/swiftmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Bước hai: khởi động

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Bước ba: kiểm chứng

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

Trong Node dùng `Buffer`: tích lũy byte nhận được, đủ một frame thì cắt ra xử lý.

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

Một luồng = một kết nối client; payload của frame dữ liệu là `4 byte số luồng big-endian + byte thô`.

### 3.2 Bắt tay và nhịp tim

Kernel **gửi Hello trước**, bạn trả `HelloAck`; kernel kiểm tra `name` và `api_version` (hiện tại `v1`) rồi mới kết nối.
Sau đó mỗi 2s một `Ping`, chỉ cần trả `Pong` (do vòng lặp đọc tiện thể xử lý, không cần timer).

### 3.3 Mô hình bất đồng bộ (bản Node)

Vòng lặp sự kiện đơn luồng, tự nhiên tránh được vấn đề "ghi xen kẽ" —— nhưng phải lưu ý **đừng để vòng lặp đọc await lời gọi ngược**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` là `async`: nó có thể lại `await call('session.settle', …)`, do đó tuyệt đối không được viết thành chờ đồng bộ.

### 3.4 Cầu ngữ nghĩa (phải xác thực trước)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` không được bỏ qua: mặt thao tác kernel của kết nối chưa có danh tính trước khi xác thực, gọi thẳng `session.open` sẽ bị từ chối
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

Delivery được kernel **đẩy xuôi về** (`method = "session.deliver"`), xử lý xong thì `session.settle`
(`ack` / `requeue` / `reject`; mã delivery là duy nhất toàn cục, không mang số luồng).

---

## 4. Đọc hiểu mã (dự án ví dụ)

`swiftmq-plugin/nodejs/index.js` khoảng 330 dòng:

| Vị trí | Chức năng |
| --- | --- |
| `u32()` / `Conn.send()` | Đọc/ghi chia frame |
| `Conn.drain()` / `dispatch()` | Phân tích và phân phối theo frame |
| `Conn.call()` | Lời gọi ngược (`Promise` + bảng `pending`, khớp theo `reverse=true` và `id`) |
| `Stream` | Phía đọc luồng: `push/end/read` tạo thành một hàng đợi bất đồng bộ |
| `handleHello` | Kiểm tra và trả HelloAck |
| `handleForwardCall` / `handleMethod` | Lời gọi xuôi (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Xác thực + khai báo + publish + consume |

---

## 5. Kiểm nghiệm thực tế (tái hiện trên máy cục bộ)

Windows + Node v24; kernel trong Docker (`swiftmq:1.1.01`), plugin trên máy host (`tcp://host.docker.internal:19011`).

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

Bao phủ: **bắt tay → xác thực → cầu ngữ nghĩa → đẩy delivery về → quyết toán → phản hồi luồng byte**.

---

## 6. Những lưu ý đặc thù của Node.js

- **`socket.write` ghi một frame một lần**: ví dụ ghép cả frame thành một `Buffer` rồi mới ghi, nên không cần khóa thêm;
  nếu bạn tách một frame thành nhiều lần `write`, thì phải tự đảm bảo thứ tự.
- **Ranh giới chunk của `stream.on('data')` không liên quan tới frame**: phải tự tích lũy đệm (xem `drain()`).
- **base64**: `message.body`, `core.authenticate.response` trong JSON là chuỗi base64
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Đừng `await` trong `drain()`**: nó là hàm cắt frame đồng bộ; hãy giao việc xử lý bất đồng bộ cho `handleForwardCall`.
- **ESM vs CJS**: ví dụ dùng CommonJS (`require`) để `node index.js` chạy trực tiếp; đổi sang ESM chỉ cần thay `import`.

---

## 7. Nâng cao

- Plugin có sẵn giao diện quản trị: thêm `console_url` vào cấu hình (tài liệu chính §5.8), trang「Quản lý plugin」của UI quản trị sẽ xuất hiện lối vào trực tiếp.
- Triển khai độc lập (K8s / systemd): `spawn: []` + `address: "tcp://<tên dịch vụ>:19011"`, lắng nghe `0.0.0.0` trong container.
- Chia sẻ cổng: nhiều giao thức mỗi cái cho một `prefix` khác nhau, kernel phân phối kết nối tới từng plugin theo tiền tố.
