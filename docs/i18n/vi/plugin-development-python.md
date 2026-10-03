# Hướng dẫn phát triển plugin tiến trình ngoài cho SwiftMQ —— Python

> **Đối tượng**: các nhà phát triển dùng Python để viết plugin tiến trình ngoài (sidecar) cho SwiftMQ.
> **Đọc trước**: [Hướng dẫn phát triển plugin tiến trình ngoài (sidecar)](plugin-development.md) —— ở đó trình bày mô hình tư duy, trường cấu hình và bảng tổng hợp giao thức đường dây;
> tài liệu này chỉ bàn **cách triển khai cụ thể bằng Python**, cùng các bước và kết quả đã kiểm nghiệm thực tế trên máy cục bộ.
> **Dự án ví dụ**: workspace `swiftmq-plugin/python/sidecar_plugin.py` (chỉ thư viện chuẩn, không phụ thuộc bên thứ ba).

---

## 1. Khi nó chạy thì trông như thế nào

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Ba điểm chính (dễ hiểu nhầm, hãy nhớ kỹ trước):

1. **Tiến trình của bạn là server**: lắng nghe một địa chỉ cục bộ, chờ kernel kết nối tới (`plugins.<tên>.sidecar.address`).
2. **Cổng nghiệp vụ hướng ngoại do kernel mở**: client kết nối tới cổng của kernel, byte được proxy tới bạn (`protocols[].listeners`).
3. **`prefix` phải khác rỗng**: kernel sniffing theo tiền tố để quyết định "kết nối này giao cho ai". `prefix` rỗng nghĩa là **không tham gia sniffing**,
   kết nối trên cổng lắng nghe của chính nó cũng sẽ không được giao cho bạn (thực tế: kết nối bị ngắt ngay lập tức). Độ dài tiền tố ≤ 8 byte, ASCII.

---

## 2. Ba bước để chạy

### Bước một: khai báo plugin trong cấu hình

`swiftmqd.json` (**cấu hình thực tế là JSON chuẩn, không được có chú thích**):

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` là **địa chỉ mà kernel kết nối tới bạn** (kernel là client, plugin là server).
- `spawn` để trống = kernel chỉ kết nối không kéo lên, tiến trình do bạn tự quản lý (systemd / supervisor / compose).
- `prefix` **phải khác rỗng**: byte đầu tiên client gửi tới phải bắt đầu bằng nó (kernel sniffing theo tiền tố để quyết định giao kết nối cho ai).
- `listeners` là cổng hướng ngoại, do kernel mở (client kết nối tới kernel, không phải bạn).
- Khi triển khai khác container, `address` dùng **tên dịch vụ** (như `tcp://py-sidecar:19001`), và plugin phải lắng nghe `0.0.0.0`.

### Bước hai: khởi động

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Bước ba: kiểm chứng

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Những điểm cốt lõi khi hiện thực

### 3.1 Chia frame (tầng byte duy nhất bạn buộc phải tự viết cho đúng)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

Payload của frame dữ liệu = `4 byte số luồng big-endian + byte thô`; payload của mặt điều khiển là JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Bắt tay và nhịp tim

Sau khi kernel kết nối, **nó gửi Hello trước**, bạn phải trả về một frame `HelloAck`; kernel sẽ kiểm tra
`name == tên plugin trong cấu hình` và `api_version == APIVersion của kernel` (hiện tại `v1`).
Sau đó kernel gửi `Ping` mỗi 2s, bạn chỉ cần trả `Pong` (không trả sẽ bị coi là chết).

### 3.3 Luồng logic

`kindOpen` đến → **trả `OpenAck` trước**, rồi mới bắt đầu phục vụ; `kindData` đến → ghi lại nguyên trạng (hoặc sau khi phân tích theo giao thức của bạn) vào
`kindData`; xử lý xong → gửi `kindClose`. Một luồng = một kết nối client.

### 3.4 Lời gọi ngược và cầu ngữ nghĩa của kernel

Lời gọi từ plugin → kernel đi qua `kindCall` với `"reverse": true`, kernel trả `kindReply` trên cùng một kết nối.
**Thứ tự rất quan trọng**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **không được bỏ qua**: mặt thao tác kernel của kết nối chưa có danh tính trước khi xác thực, gọi thẳng `session.open` sẽ bị từ chối
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Tham số chính là phản hồi SASL mà bạn phân tích ra từ giao thức:

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

Delivery của việc tiêu thụ được kernel **đẩy xuôi về** (`kindCall`, `method = "session.deliver"`), xử lý xong thì
quyết toán bằng `session.settle` (`ack` / `requeue` / `reject`; mã delivery là duy nhất toàn cục, không cần số luồng).

### 3.5 Mô hình đồng thời (bản Python)

| Vai trò | Luồng (thread) |
| --- | --- |
| Vòng lặp đọc frame | Một cho mỗi kết nối kernel |
| Xử lý luồng | Một cho mỗi luồng (do đó nhiều kết nối client có thể chạy đồng thời) |
| Xử lý lời gọi xuôi | Một cho mỗi lời gọi |

**Phải lưu ý**: trong vòng lặp đọc **không được** chờ đồng bộ phản hồi của lời gọi ngược (sẽ deadlock) —— lời gọi xuôi (`session.deliver`)
phải ném sang luồng riêng để xử lý, vì trong lúc xử lý nó có thể lại phát sinh `session.settle`. Việc ghi frame phải được khóa để tuần tự hóa.

---

## 4. Đọc hiểu mã (dự án ví dụ)

`swiftmq-plugin/python/sidecar_plugin.py` khoảng 320 dòng, các hàm trọng tâm:

| Vị trí | Chức năng |
| --- | --- |
| `read_frame` / `Conn.send` | Đọc/ghi chia frame (tiền tố độ dài + kind) |
| `Conn.call` | Lời gọi ngược: đánh số → gửi → chờ phản hồi (khớp theo `reverse=true` và `id`) |
| `Conn.serve` | Vòng lặp đọc frame và phân phối |
| `Conn._handle_hello` | Kiểm tra tên plugin/phiên bản API và trả HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Vòng đời luồng và phản hồi lại (echo) |
| `Conn._dispatch` | Xử lý lời gọi xuôi: `session.deliver` (gồm settle), `stats` |
| `Conn._session_demo` | Xác thực + khai báo hàng đợi + publish + consume |

---

## 5. Kiểm nghiệm thực tế (tái hiện trên máy cục bộ)

Môi trường: Windows + Python 3.12; kernel chạy trong Docker (`swiftmq:1.1.01`), plugin chạy trên máy host,
kernel dùng `tcp://host.docker.internal:19001` để kết nối tới nó.

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

Chuỗi được bao phủ: **bắt tay → xác thực → cầu ngữ nghĩa (khai báo/publish/consume) → đẩy delivery về → quyết toán → phản hồi luồng byte**.

---

## 6. Những lưu ý đặc thù của Python

- **Đừng dùng `time.sleep` để chờ nhịp tim**: vòng lặp đọc là dạng chặn (blocking), chỉ cần dựa vào Ping của kernel để duy trì kết nối;
  nếu đặt read timeout cho socket, nhớ coi timeout như "kết nối kết thúc" (khi kernel bị `kill -9`, socket có thể không đóng kịp).
- **`json.dumps` mặc định thêm dấu cách**: ví dụ dùng `separators=(",", ":")` chỉ để log trông đẹp hơn, bản thân giao thức không yêu cầu.
- **Byte chính là base64**: `message.body`, `core.authenticate.response` trong JSON đều là chuỗi base64,
  đừng quên `base64.b64encode/decode`.
- **Ghi frame phải khóa**: nhịp tim, phản hồi, khối dữ liệu đến từ các luồng khác nhau, ghi xen kẽ sẽ làm hỏng cả kết nối (ví dụ dùng `threading.Lock`).
- Dùng `asyncio` cũng được, nhưng phải đảm bảo "ghi tuần tự + vòng lặp đọc không chặn", tư duy giống bản dùng thread.

---

## 7. Nâng cao

- Muốn cấu hình giao diện quản trị riêng cho plugin: thêm `console_url` vào cấu hình, trang「Quản lý plugin」của UI quản trị sẽ xuất hiện lối vào trực tiếp
  (xem tài liệu chính §5.8).
- Plugin tự biến mình thành dịch vụ độc lập, do systemd / K8s quản lý: `spawn: []` + `restart: "never"`, do bên ngoài kéo lên.
- Cần nhiều giao thức cùng tồn tại: cho nhiều plugin dùng `prefix` khác nhau trên cùng một listener, hoặc mỗi cái mở cổng riêng.
