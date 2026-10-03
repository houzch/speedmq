# Hướng dẫn phát triển plugin tiến trình ngoài cho SwiftMQ —— Java

> **Đối tượng**: các nhà phát triển dùng Java để viết plugin tiến trình ngoài (sidecar) cho SwiftMQ.
> **Đọc trước**: [Hướng dẫn phát triển plugin tiến trình ngoài (sidecar)](plugin-development.md) (mô hình tư duy / trường cấu hình / bảng tổng hợp giao thức đường dây).
> **Dự án ví dụ**: workspace `swiftmq-plugin/java/SidecarPlugin.java` (một tệp, chỉ thư viện chuẩn JDK, không cần Maven/Gradle).

---

## 1. Khi nó chạy thì trông như thế nào

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/swiftmq/classes", "SidecarPlugin",
                  "--addr", "0.0.0.0:19031", "--name", "java-sidecar"],
        "protocols": [
          { "name": "javaecho", "prefix": "JV",
            "listeners": [{ "name": "javaecho", "addr": ":19032" }] }
        ]
      }
    }
  }
}
```

### Bước hai: biên dịch và khởi động

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Bước ba: kiểm chứng

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

Java dùng `DataInputStream`/`DataOutputStream` là nhàn nhất —— `readInt`/`writeInt` của chúng chính là **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Phía ghi phải **tuần tự** (nhịp tim, phản hồi, khối dữ liệu đến từ các luồng khác nhau):

```java
void send(int kind, byte[] payload) throws IOException {
    synchronized (writeLock) {
        out.writeInt(1 + payload.length);
        out.write(kind);
        out.write(payload);
        out.flush();
    }
}
```

Payload của frame dữ liệu = `4 byte số luồng big-endian + byte thô`.

### 3.2 Bắt tay và nhịp tim

Kernel **gửi Hello trước**, bạn trả `HelloAck`; kernel kiểm tra `name` và `api_version` (hiện tại `v1`) rồi mới kết nối.
Sau đó mỗi 2s một `Ping`, trả `Pong`.

### 3.3 Mô hình đồng thời (bản Java)

| Vai trò | Luồng (thread) |
| --- | --- |
| Vòng lặp đọc frame | Một cho mỗi kết nối kernel |
| Xử lý luồng | Một cho mỗi luồng (nhiều kết nối client có thể chạy đồng thời) |
| Xử lý lời gọi xuôi | Một cho mỗi lời gọi |

**Trong vòng lặp đọc không được chờ đồng bộ phản hồi của lời gọi ngược** (sẽ deadlock): việc xử lý `session.deliver` phải ném sang luồng riêng,
vì bên trong nó còn phải `session.settle` (lại là một lời gọi ngược). Ví dụ làm đúng như vậy.

### 3.4 Cầu ngữ nghĩa (phải xác thực trước)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` không được bỏ qua: mặt thao tác kernel của kết nối chưa có danh tính trước khi xác thực, gọi thẳng `session.open` sẽ bị từ chối
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```java
Map<String, Object> auth = new LinkedHashMap<>();
auth.put("stream", streamId);
auth.put("mechanism", "PLAIN");
// SASL PLAIN 响应：NUL + user + NUL + password，字节在 JSON 里走 base64
auth.put("response", Base64.getEncoder().encodeToString(plainResponse(user, password)));
Object ident = call("core.authenticate", auth, 10_000);

call("session.open", Map.of("stream", streamId, "vhost", "/"), 10_000);
Map<String, Object> q = asMap(call("session.declare_queue",
        Map.of("stream", streamId, "exclusive", true, "auto_delete", true), 10_000));
call("session.consume", Map.of("stream", streamId, "queue", q.get("name"), "prefetch", 32), 10_000);
```

Delivery được kernel **đẩy xuôi về** (`method = "session.deliver"`), xử lý xong thì `session.settle`
(`ack` / `requeue` / `reject`; mã delivery là duy nhất toàn cục, không mang số luồng).

---

## 4. Đọc hiểu mã (dự án ví dụ)

`swiftmq-plugin/java/SidecarPlugin.java` khoảng 470 dòng (gồm JSON tối giản):

| Vị trí | Chức năng |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Đọc/ghi chia frame (`DataInputStream` + khóa ghi) |
| `Conn.serve()` | Vòng lặp đọc frame và phân phối |
| `Conn.call()` | Lời gọi ngược (bảng `pending` + blocking queue, có bảo vệ timeout) |
| `Conn.handleHello()` | Kiểm tra và trả HelloAck |
| `StreamState` | Phía đọc luồng (`BlockingQueue`, `STREAM_END` biểu thị kết thúc) |
| `Conn.handleForwardCall()` / `handleMethod()` | Lời gọi xuôi (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Xác thực + khai báo + publish + consume |
| `Json` (cuối tệp) | Đọc/ghi JSON tối giản, chỉ để ví dụ không phụ thuộc gì |

> **Khuyến nghị cho môi trường production**: thay `Json` bằng thư viện bạn quen dùng (Jackson / Gson), hoặc dùng một stack sẵn có khác ngoài `java.net.http` ——
> không liên quan tới điều mà ví dụ này muốn trình bày (giao thức đường dây).

---

## 5. Kiểm nghiệm thực tế (tái hiện trên máy cục bộ)

Windows + JDK 25; kernel trong Docker (`swiftmq:1.1.01`), plugin trên máy host (`tcp://host.docker.internal:19031`).

```
javac -encoding UTF-8 -d classes SidecarPlugin.java   → 退出码 0
plugin=java-sidecar state=enabled          # /api/plugins
echo=[JVhello]                             # 客户端连内核端口 19032 发 "JVhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=java-sidecar addr=0.0.0.0:19031 version=0.1.0
内核已接入 plugin=java-sidecar peer=/127.0.0.1:51039
握手完成 plugin=java-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-4ce088d93638eafe830191
流已打开 plugin=java-sidecar stream=1 remote=172.17.0.1:34112 local=172.17.0.2:19032
收到投递（session.deliver） queue=amq.gen-4ce088d93638eafe830191 delivery_id=1 body=hello from java sidecar
```

Bao phủ: **bắt tay → xác thực → cầu ngữ nghĩa → đẩy delivery về → quyết toán → phản hồi luồng byte**.

---

## 6. Những lưu ý đặc thù của Java

- **`\uXXXX` trong mã nguồn bị trình biên dịch xử lý ở mọi vị trí** (kể cả chú thích!). Chú thích trong ví dụ cố ý viết thành
  "NUL + tên người dùng + NUL + mật khẩu" thay vì viết thẳng `\u0000`, nếu không javac sẽ báo ký tự không hợp lệ.
- **Mã nguồn tiếng Trung phải `javac -encoding UTF-8`**, nếu không trên mặc định Windows (GBK) sẽ báo "ký tự không ánh xạ được của mã hóa GBK".
  Khi chạy nếu muốn in tiếng Trung cho đúng, thêm `-Dfile.encoding=UTF-8`.
- **Biến cục bộ được lambda bắt giữ phải effectively final**: trong ví dụ, `name` bị gán lại trong lúc phân tích tham số,
  nên trong lambda dùng `opts.name` (trường chỉ được gán một lần).
- **`DataInputStream` là kiểu chặn**: khi kết nối đứt sẽ ném `EOFException`/`IOException`, dựa vào đó để kết thúc.
- **base64**: `message.body`, `core.authenticate.response` trong JSON là chuỗi base64
  (`Base64.getEncoder()/getDecoder()`).
- **Thư viện chuẩn JDK không có JSON**: ví dụ tự mang theo bản hiện thực tối giản; `Json.parse` giải số nguyên thành `Long`, số thực thành `Double`,
  khi lấy `id` thì dùng `((Number) m.get("id")).longValue()`.

---

## 7. Nâng cao

- Đóng gói thành jar chạy được (`Main-Class: SidecarPlugin`) hoặc dùng `jlink` để tinh gọn runtime,
  rồi đổi `spawn` thành `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`.
- Plugin có sẵn giao diện quản trị: thêm `console_url` vào cấu hình (tài liệu chính §5.8), trang「Quản lý plugin」của UI quản trị sẽ xuất hiện lối vào trực tiếp.
- Triển khai độc lập: `spawn: []` + `address: "tcp://<tên dịch vụ>:19031"`, lắng nghe `0.0.0.0` trong container.
