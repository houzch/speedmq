# Giám sát và cảnh báo SwiftMQ

Thư mục này cung cấp các mẫu giám sát dùng được ngay:

| Tệp | Tác dụng |
| --- | --- |
| `prometheus-alerts.yml` | Quy tắc cảnh báo Prometheus (`groups: - name: swiftmq`) |
| `grafana-dashboard.json` | Bảng điều khiển Grafana có thể import (các panel bao phủ các tín hiệu then chốt dưới đây) |
| `README.md` | Cách dùng, danh sách chỉ số, ý nghĩa và cách xử lý của từng cảnh báo, khoảng trống đã biết |

---

## 1. Cách dùng

### 1.1 Thu thập (Prometheus)

Mặt quản trị (mặc định `:15672`) phơi ra `/metrics` định dạng text Prometheus, **cần Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: swiftmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Khuyến nghị tạo riêng một tài khoản chỉ đọc cho giám sát (tag `monitoring` là đọc được chỉ số), đừng dùng lại mật khẩu quản trị viên.

Kiểm tra việc thu thập có bình thường không (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Quy tắc cảnh báo

Đặt `prometheus-alerts.yml` vào thư mục rules của Prometheus, tham chiếu trong `prometheus.yml` rồi reload:

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

Trong quy tắc dùng thống nhất `job="swiftmq"`; nếu tên job của bạn khác, hãy thay thế toàn văn.

### 1.3 Bảng điều khiển Grafana

`grafana-dashboard.json` được import qua **Dashboards → Import → tải lên JSON**, khi import chọn nguồn dữ liệu Prometheus của bạn
(trong bảng điều khiển tham chiếu bằng biến `${DS_PROMETHEUS}`). Biến mẫu `DS_PROMETHEUS` sẽ được gán trong ánh xạ import.

**【chưa kiểm chứng】** Máy cục bộ chưa dựng instance Grafana, chưa làm kiểm chứng import thật; JSON này chỉ được kiểm tra cú pháp JSON (14 panel, phân tích qua).

---

## 2. Đoạn thật của `/metrics` (bằng chứng)

Dưới đây là **đầu ra thật** của `/metrics` từ instance `1.0.0` trên máy cục bộ (đã tạo một queue durable `persist.q`,
nên các chỉ số per-queue có tag `vhost`/`queue` đã xuất hiện):

```
# HELP swiftmq_up node còn sống hay không
# TYPE swiftmq_up gauge
swiftmq_up 1
# HELP swiftmq_build_info thông tin build
# TYPE swiftmq_build_info gauge
swiftmq_build_info 1{version="1.0.0",node="swiftmq@DESKTOP-HBDCVPA"}
# HELP swiftmq_resource_blocked mức nước tài nguyên có chặn producer không (1=đang chặn)
# TYPE swiftmq_resource_blocked gauge
swiftmq_resource_blocked 0
# HELP swiftmq_connections số kết nối hiện tại
# TYPE swiftmq_connections gauge
swiftmq_connections 0
# HELP swiftmq_channels số channel hiện tại
# TYPE swiftmq_channels gauge
swiftmq_channels 0
# HELP swiftmq_queues số queue hiện tại
# TYPE swiftmq_queues gauge
swiftmq_queues 0
# HELP swiftmq_exchanges số exchange hiện tại
# TYPE swiftmq_exchanges gauge
swiftmq_exchanges 6
# HELP swiftmq_consumers số consumer hiện tại
# TYPE swiftmq_consumers gauge
swiftmq_consumers 0
# HELP swiftmq_queue_messages tổng số message sẵn sàng
# TYPE swiftmq_queue_messages gauge
swiftmq_queue_messages 0
# HELP swiftmq_queue_messages_unacknowledged tổng số message chưa xác nhận
# TYPE swiftmq_queue_messages_unacknowledged gauge
swiftmq_queue_messages_unacknowledged 0
# HELP swiftmq_process_memory_bytes số byte bộ nhớ mà tiến trình này xin hệ điều hành
# TYPE swiftmq_process_memory_bytes gauge
swiftmq_process_memory_bytes 1564672
# HELP swiftmq_memory_total_bytes tổng lượng bộ nhớ vật lý
# TYPE swiftmq_memory_total_bytes gauge
swiftmq_memory_total_bytes 34181279744
# HELP swiftmq_disk_free_bytes dung lượng trống của thư mục dữ liệu
# TYPE swiftmq_disk_free_bytes gauge
swiftmq_disk_free_bytes 240091688960
# HELP swiftmq_memory_high_watermark tỷ lệ mức nước bộ nhớ
# TYPE swiftmq_memory_high_watermark gauge
swiftmq_memory_high_watermark 0.4
# HELP swiftmq_disk_free_limit_bytes giới hạn dưới dung lượng trống đĩa
# TYPE swiftmq_disk_free_limit_bytes gauge
swiftmq_disk_free_limit_bytes 52428800
# HELP swiftmq_queue_messages_ready số message sẵn sàng trong queue
# TYPE swiftmq_queue_messages_ready gauge
# HELP swiftmq_queue_messages_unacknowledged số message chưa xác nhận trong queue
# TYPE swiftmq_queue_messages_unacknowledged gauge
# HELP swiftmq_queue_consumers số consumer trên queue
# TYPE swiftmq_queue_consumers gauge
# HELP swiftmq_queue_memory_bytes giá trị ước lượng mức chiếm dụng bộ nhớ của queue
# TYPE swiftmq_queue_memory_bytes gauge
# HELP swiftmq_queue_messages_published_total số message queue đã nhận lũy kế
# TYPE swiftmq_queue_messages_published_total counter
# HELP swiftmq_queue_messages_delivered_total số message queue đã gửi đi lũy kế
# TYPE swiftmq_queue_messages_delivered_total counter
# HELP swiftmq_queue_messages_acked_total số message queue đã xác nhận lũy kế
# TYPE swiftmq_queue_messages_acked_total counter
swiftmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
swiftmq_queue_consumers{vhost="/",queue="persist.q"} 0
swiftmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
swiftmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
swiftmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP swiftmq_plugin_info siêu dữ liệu plugin (value luôn là 1, trạng thái xem tag state)
# TYPE swiftmq_plugin_info gauge
# HELP swiftmq_plugin_up plugin có đang phục vụ không (1=enabled, 0=trạng thái khác: disabled/failed/down)
# TYPE swiftmq_plugin_up gauge
swiftmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="amqp091",state="enabled"} 1
swiftmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Danh sách chỉ số (tất cả đều tồn tại thật, nguồn `internal/management/metrics.go`)

| Chỉ số | Loại | Tag | Ngữ nghĩa |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | tiến trình tự báo còn sống (hiện tại luôn là 1) |
| `swiftmq_build_info` | gauge | `version`,`node` | thông tin build, value luôn 1 |
| `swiftmq_resource_blocked` | gauge | — | mức nước tài nguyên có chặn producer không (1=đang chặn) |
| `swiftmq_connections` | gauge | — | số kết nối hiện tại |
| `swiftmq_channels` | gauge | — | số channel hiện tại |
| `swiftmq_queues` | gauge | — | số queue hiện tại |
| `swiftmq_exchanges` | gauge | — | số exchange hiện tại |
| `swiftmq_consumers` | gauge | — | số consumer hiện tại |
| `swiftmq_queue_messages` | gauge | — | tổng số message sẵn sàng **toàn cục** |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | tổng số message chưa xác nhận **toàn cục** |
| `swiftmq_process_memory_bytes` | gauge | — | bộ nhớ **đang dùng** của tiến trình (`HeapInuse+StackInuse`) |
| `swiftmq_memory_total_bytes` | gauge | — | tổng lượng bộ nhớ vật lý |
| `swiftmq_disk_free_bytes` | gauge | — | dung lượng trống của thư mục dữ liệu |
| `swiftmq_memory_high_watermark` | gauge | — | tỷ lệ mức nước bộ nhớ |
| `swiftmq_disk_free_limit_bytes` | gauge | — | giới hạn dưới dung lượng trống đĩa |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | số message sẵn sàng của một queue |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | số message chưa xác nhận của một queue |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | số consumer của một queue |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | ước lượng mức chiếm dụng bộ nhớ của một queue |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | số message queue nhận lũy kế |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | số message queue gửi đi lũy kế |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | số message queue xác nhận lũy kế |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | siêu dữ liệu plugin, value luôn 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | plugin có đang phục vụ không (1=enabled, 0=khác) |

### 3.1 Lưu ý khi sử dụng (tránh viết sai)

- **Hai họ series cùng tên khác cardinality**: `swiftmq_queue_messages_unacknowledged` **vừa** có series toàn cục không tag,
  **vừa** có series per-queue có tag; còn phía "sẵn sàng" toàn cục gọi là `swiftmq_queue_messages`, per-queue gọi là
  `swiftmq_queue_messages_ready` (tên không đối xứng). Khi viết quy tắc hãy dùng `{queue=~".+"}` để rõ ràng chỉ lấy họ per-queue.
- **Cách hiểu `swiftmq_process_memory_bytes`**: hiện thực là `MemStats.HeapInuse + StackInuse` (**bộ nhớ đang dùng**),
  cùng cách hiểu với việc phán đoán mức nước bộ nhớ của nhân; nhưng nội dung `# HELP` của nó viết là "số byte bộ nhớ xin hệ điều hành", **nội dung không khớp với cách hiểu thực tế**,
  hãy lấy tài liệu này làm chuẩn.
- **Series per-queue chỉ xuất hiện khi queue tồn tại**: sau khi queue bị xóa series đó biến mất (phía Prometheus sẽ trở thành stale).
  Cảnh báo liên quan "queue phải tồn tại nhưng không có dữ liệu" có thể kết hợp `absent()` hoặc `or vector(0)` của Grafana.
- **counter về 0 sau khi tiến trình khởi động lại**: `*_total` là lũy kế trong tiến trình, khởi động lại là bắt đầu từ 0; hãy dùng `rate()`/`increase()`,
  đừng đặt ngưỡng trực tiếp trên giá trị tuyệt đối.
- **Tín hiệu cluster không có metrics**: xem §5.

---

## 4. Ý nghĩa cảnh báo và cách xử lý khuyến nghị (tương ứng `prometheus-alerts.yml`)

| Cảnh báo | Điều kiện kích hoạt | Ý nghĩa | Cách xử lý khuyến nghị |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` 1m | mục tiêu thu thập hoàn toàn không truy cập được | kiểm tra tiến trình/cổng/mạng/xác thực; khởi động lại và xem log khởi động |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` 1m | thu thập thành công nhưng tiến trình tự báo không còn sống | mục dự phòng, xem log thoát bất thường |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` 2m | plugin disabled/failed/down | `swiftmqctl plugins show <name>` xem `runtime_note`; plugin ngoài `restart=always` thường tự hồi phục |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` 5m | mức nước bộ nhớ/đĩa bị kích hoạt, producer bị chặn | kiểm tra mức nước bộ nhớ và dung lượng đĩa trống; xác nhận consumer có tiến triển không |
| `SwiftMQMemoryWatermarkHigh` | tỷ lệ bộ nhớ đang dùng > 0.9×mức nước 10m | tiến gần mức nước bộ nhớ | giảm tồn đọng/tăng tốc độ consume, phòng kích hoạt chặn |
| `SwiftMQDiskFreeLow` | trống < 1.5×giới hạn dưới đĩa 10m | thư mục dữ liệu sắp đầy | mở rộng/dọn dẹp; tới giới hạn dưới sẽ chặn producer |
| `SwiftMQQueueBacklogGrowing` | sẵn sàng >10000 và tăng đơn điệu trong 15m | queue tồn đọng liên tục | mở rộng consumer / kiểm tra phía consume; rà soát bất thường dead-letter/TTL |
| `SwiftMQQueueNoConsumers` | consumer=0 và có message sẵn sàng 15m | không ai consume | kiểm tra tiến trình phía consume; xác nhận consumer chưa mất kết nối |
| `SwiftMQUnackedPileUp` | chưa xác nhận >1000 15m | consumer bị kẹt/không ack | kiểm tra logic xử lý consumer và prefetch; khi cần thì đóng kết nối để gửi lại |
| `SwiftMQConnectionSpike` | kết nối >10000 10m | số kết nối bất thường | kiểm tra rò rỉ kết nối; client nên tái sử dụng kết nối |

> Các ngưỡng (10000 / 1000 v.v.) là **giá trị khởi điểm**, hãy điều chỉnh theo quy mô queue và đặc điểm nghiệp vụ của bạn.

---

## 5. Khoảng trống đã biết: cluster "mất đa số / không có leader" hiện chưa có chỉ số

- **Sự thật**: `/metrics` **không có** bất kỳ chỉ số dạng cluster nào (không có `swiftmq_cluster_*`). Trạng thái cluster chỉ nằm trong JSON của `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Do đó** `prometheus-alerts.yml` **cố ý không viết** cảnh báo dựa trên chỉ số cluster —— viết cũng **mãi mãi không kích hoạt**
  (Prometheus không báo lỗi vì tên chỉ số không tồn tại), đó thuộc loại bàn giao "trông đúng nhưng thực ra vô hiệu".
- **Phương án tự dựng** (chọn một trong hai, đều cần bạn dựng ngoài SwiftMQ, không thuộc phạm vi kho này):
  1. dùng JSON exporter phổ thông thu thập `/api/cluster`, ánh xạ thành chỉ số tùy chỉnh (như `swiftmq_cluster_has_quorum`), rồi cảnh báo trên chỉ số đó;
  2. dùng script probe định kỳ gọi `/api/cluster`, khi `has_quorum=false` hoặc `paused=true` thì bắn cảnh báo.
- Cách hiểu ngưỡng liên quan: `has_quorum=false` nghĩa là mất liên lạc với đa số; dưới `pause_minority` (mặc định) lúc này **dịch vụ sẽ tạm dừng và ngắt kết nối**.

---

## 6. Các mục **chưa kiểm chứng** khác

- Bảng điều khiển Grafana **chưa được kiểm chứng import trong Grafana thật** (chỉ kiểm tra cú pháp JSON qua).
- Quy tắc cảnh báo **chưa được kiểm chứng load trong Prometheus/Alertmanager thật** (máy cục bộ chưa dựng Prometheus).
  Nhưng **tên chỉ số trong quy tắc đã được đối chiếu từng dòng với đầu ra thật của `/metrics`** (xem §2/§3), không tồn tại vấn đề "viết sai tên dẫn đến mãi mãi không kích hoạt".
