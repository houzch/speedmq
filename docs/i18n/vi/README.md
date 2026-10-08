<!-- i18n-switcher -->
[简体中文](../../../README-cn.md) | [繁體中文](../zh-TW/README.md) | [English](../../../README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | **Tiếng Việt** | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SpeedMQ

Middleware nhắn tin **tương thích RabbitMQ** được viết bằng Go. Các client RabbitMQ hiện có **không cần sửa mã, không cần đổi SDK**, chỉ cần đổi địa chỉ kết nối là có thể tích hợp.

## Giới thiệu

- **Tương thích giao thức**: AMQP 0-9-1 (bao gồm phần mở rộng của RabbitMQ) và MQTT 3.1.1; đường cơ sở tương thích là **ngữ nghĩa RabbitMQ 4.3**.
- **Triển khai đơn giản**: một binary / một container, UI quản trị đã được nhúng sẵn, không cần Nginx, cơ sở dữ liệu hay runtime Node bổ sung.
- **Vận hành đủ dùng**: UI quản trị (queue / exchange / kết nối / quyền tài khoản / virtual host / policy / giới hạn / cluster), Prometheus `/metrics`, dòng lệnh `speedmqctl`.
- **Cổng mặc định**: `5672` (AMQP), `1883` (MQTT), `15672` (UI quản trị / HTTP API / chỉ số).

Các khả năng hiện có: lưu trữ bền vững (nhật ký phân đoạn + mức fsync + phục hồi sau sự cố), xác nhận publish, TTL / dead-letter / giới hạn độ dài, ưu tiên consumer, Direct Reply-To, cluster (siêu dữ liệu Raft + queue trọng tài + chuyển tiếp giữa các node), bật/tắt nóng plugin.

***

## Bắt đầu nhanh

### Cách 1: Docker (khuyến nghị)

**Không cần clone kho mã: chỉ cần kéo image về và chạy.**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.05
```

Image được phát hành ở hai nơi với nội dung giống nhau (chọn nơi nhanh hơn): Docker Hub `houzch/speedmq` và GitHub GHCR `ghcr.io/houzch/speedmq`; cả hai đều có `linux/amd64` và `linux/arm64`.

- Dữ liệu nằm trong volume có tên `speedmq-data`, không mất khi tạo lại container.
- Dừng / xoá: `docker stop speedmq`, `docker rm speedmq` (volume dữ liệu vẫn giữ).

**Muốn sửa cấu hình hoặc dùng compose thì clone kho mã:**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # Dùng image đã phát hành; đổi thành up -d --build nếu muốn build tại máy

docker compose ps        # Trạng thái phải là Up (healthy)
docker compose logs -f   # Theo dõi log
```

- Cấu hình được mount chỉ đọc từ `configs/speedmqd.json`; sửa xong `docker compose restart` là có hiệu lực.
- Dừng: `docker compose down` (giữ dữ liệu); `docker compose down -v` (xoá cả dữ liệu).

### Cách 2: Binary cục bộ (cần Go 1.24+)

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> Sản phẩm build của UI quản trị không được đưa vào kho. Nếu muốn dùng UI, trước tiên chạy `npm ci && npm run build` trong `web/`;
> không build vẫn có thể khởi động và gửi/nhận message bình thường, chỉ là khi truy cập `/` sẽ báo「UI quản trị chưa được build」.

### Đăng nhập lần đầu (nhớ đổi tài khoản mặc định trước)

| Lối vào | Địa chỉ / Thông tin xác thực |
| --- | --- |
| UI quản trị | <http://localhost:15672/> (tên người dùng `guest`, mật khẩu `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (tài khoản như trên) |

Tài khoản tổng của instance mới cài mang dấu hiệu「bắt buộc đổi mật khẩu khi đăng nhập lần đầu」: sau khi đăng nhập UI quản trị sẽ **bắt buộc đổi đồng thời tên tài khoản và mật khẩu**, đổi xong mới vào được trang quản trị.

Cũng có thể gọi API trực tiếp để hoàn tất (phù hợp với tự động hóa):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ Mặc định `guest/guest` nhất quán với hành vi của RabbitMQ: **chỉ cho phép đăng nhập từ máy cục bộ**. Kết nối từ ngoài container / từ xa cần bật `remote_access` cho người dùng đó trong cấu hình (cấu hình mẫu đã bật cho tình huống container).
> **Khi dịch vụ đã có thể truy cập từ bên ngoài, hãy đổi thông tin xác thực ngay lập tức.**

### Tích hợp vào ứng dụng của bạn (chỉ cần đổi địa chỉ kết nối)

```python
# Python (pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go (amqp091-go)
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (client mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

HTTP API quản trị tương thích với `rabbitmqadmin`; 「thêm queue / exchange」trong UI quản trị chính là endpoint khai báo chuẩn, script cũng làm được:

```bash
# khai báo queue (queue trọng tài biểu diễn bằng arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Vận hành hằng ngày

| Hạng mục | Lối vào |
| --- | --- |
| UI quản trị | <http://localhost:15672/>: queue / exchange / kết nối / quyền tài khoản / virtual host / policy / giới hạn / feature flag / cluster, góc trên bên phải có thể đặt tự động làm mới và **ngôn ngữ giao diện** |
| Chỉ số giám sát | <http://localhost:15672/metrics> (định dạng text Prometheus, cần xác thực); bảng điều khiển và cảnh báo xem tại [docs/ops/monitoring](ops/monitoring/README.md) |
| Dòng lệnh | `./bin/speedmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (tắt nóng, cổng đóng ngay lập tức) |
| Kiểm tra sức khỏe | `nc -z 127.0.0.1 15672` (compose đã tích hợp sẵn healthcheck) |
| Sao lưu và phục hồi | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Nâng cấp | [docs/ops/upgrade.md](ops/upgrade.md) |
| Đường cơ sở bảo mật | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Cấu hình thường dùng (ví dụ đầy đủ xem tại [configs/speedmqd.json](../../../configs/speedmqd.json), cũng có thể ghi đè bằng biến môi trường `SPEEDMQ_*`):

| Mục cấu hình | Mô tả | Mặc định |
| --- | --- | --- |
| `data_dir` | Thư mục dữ liệu (message + metadata), **nhớ phải lưu trữ bền vững** | `data` |
| `listeners` | Địa chỉ lắng nghe của từng giao thức, có thể cấu hình TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Địa chỉ lắng nghe của UI quản trị / API | `:15672` |
| `management.language` | Ngôn ngữ mặc định của UI quản trị; để trống sẽ tự động chọn theo múi giờ nơi triển khai | Tự động |
| `storage.fsync` | Mức ghi xuống đĩa `none / os / batch / always` (đồng thời quyết định thời điểm confirm) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Mức nước tài nguyên: khi kích hoạt sẽ chặn producer, **không mất message** | `0.4` / 50 MiB |
| `users` | Bảng người dùng tích hợp (mật khẩu + tag + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Cluster nhiều node (mặc định tắt), thay đổi thành viên dùng `speedmqctl add_member` | Tắt |

> Cổng có thể bị chiếm: dùng `listeners` / `management.addr` để đổi sang cổng khác.

***

## Cấu trúc dự án

```
speedmq/
├── cmd/
│   ├── speedmqd/        # điểm vào tiến trình broker (thứ cần chạy chính là nó)
│   └── speedmqctl/      # CLI vận hành (đi qua HTTP API quản trị, tách rời khỏi phiên bản nhân)
├── internal/            # phần hiện thực nhân
│   ├── protocol/        # plugin giao thức: amqp091, mqtt (mã hóa/giải mã / phương thức / phiên)
│   ├── broker/          # nhân: vhost, exchange, queue, dead-letter, điều khiển luồng, chế độ xem mặt quản trị
│   ├── store/           # lưu trữ bền vững: nhật ký phân đoạn, chỉ mục queue, phục hồi sau sự cố
│   ├── raft/ meta/      # cluster: Raft tự phát triển và sao chép siêu dữ liệu
│   ├── management/      # HTTP API quản trị + chỉ số Prometheus + dịch vụ tĩnh UI nhúng
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # hợp đồng ổn định hướng ngoài: API plugin (plugin) và giao thức đường dây plugin tiến trình ngoài (sidecar)
├── web/                 # dự án frontend UI quản trị (Vue 3 + Vite), sản phẩm khi build được nhúng vào binary qua go:embed
├── configs/             # cấu hình mẫu
├── docs/ops/            # tài liệu vận hành: sao lưu phục hồi / nâng cấp / đường cơ sở bảo mật / giám sát
├── Dockerfile、docker-compose.yml
└── speedmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## Đóng góp

Hoan nghênh gửi Issue và Pull Request. Nền tảng tồn tại của dự án này là **tương thích giao thức**, do đó:

- Khi sửa bug hãy mô tả hành vi RabbitMQ tương ứng (phiên bản, client, các bước tái hiện);
- Với thay đổi liên quan đến chi tiết giao thức, hãy kèm kết quả đối chiếu với RabbitMQ;
- Trước khi gửi, đảm bảo `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .` đều pass.

***

## Giấy phép

Dự án này sử dụng [Giấy phép Apache 2.0](../../../LICENSE).

Được phép sử dụng, sửa đổi, phân phối (bao gồm cả sử dụng thương mại), cần giữ lại thông báo bản quyền và giấy phép, và không cung cấp bất kỳ bảo đảm nào.

Copyright 2026 houzch (xem [NOTICE](../../../NOTICE))

***

## Lời cảm ơn

Đặc tả giao thức AMQP 0-9-1 và ngữ nghĩa hành vi của [RabbitMQ](https://www.rabbitmq.com/) là chuẩn đối chiếu cho công việc tương thích của dự án này. Dự án này là hiện thực độc lập, không có quan hệ trực thuộc với RabbitMQ chính thức, và không sử dụng mã của nó.

***

## Tham gia nhóm trao đổi

Quét mã để tham gia nhóm trao đổi SpeedMQ, có vấn đề gì có thể hỏi trực tiếp trong nhóm:

![Nhóm trao đổi SpeedMQ](../../../1280X1280.PNG)
