# Giám sát và cảnh báo SpeedMQ

Thư mục này cung cấp các mẫu giám sát dùng được ngay:

| Tệp | Tác dụng |
| --- | --- |
| `prometheus-alerts.yml` | Quy tắc cảnh báo Prometheus (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Bảng điều khiển Grafana có thể import (các panel bao phủ các tín hiệu then chốt dưới đây) |
| `README.md` | Cách dùng, danh sách chỉ số, ý nghĩa và cách xử lý của từng cảnh báo, khoảng trống đã biết |

---

## 1. Cách dùng

### 1.1 Thu thập (Prometheus)

Mặt quản trị (mặc định `:15672`) phơi ra `/metrics` định dạng text Prometheus, **cần Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: speedmq
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
  - "rules/speedmq-alerts.yml"
```

Trong quy tắc dùng thống nhất `job="speedmq"`; nếu tên job của bạn khác, hãy thay thế toàn văn.

### 1.3 Bảng điều khiển Grafana

`grafana-dashboard.json` được import qua **Dashboards → Import → tải lên JSON**, khi import chọn nguồn dữ liệu Prometheus của bạn
(trong bảng điều khiển tham chiếu bằng biến `${DS_PROMETHEUS}`). Biến mẫu `DS_PROMETHEUS` sẽ được gán trong ánh xạ import.

**【chưa kiểm chứng】** Máy cục bộ chưa dựng instance Grafana, chưa làm kiểm chứng import thật; JSON này chỉ được kiểm tra cú pháp JSON (14 panel, phân tích qua).

---

## 2. Đoạn thật của `/metrics` (bằng chứng)

Dưới đây là **đầu ra thật** của `/metrics` từ instance `1.0.0` trên máy cục bộ (đã tạo một queue durable `persist.q`,
nên các chỉ số per-queue có tag `vhost`/`queue` đã xuất hiện):

```
# HELP speedmq_up node còn sống hay không
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info thông tin build
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked mức nước tài nguyên có chặn producer không (1=đang chặn)
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections số kết nối hiện tại
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels số channel hiện tại
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues số queue hiện tại
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges số exchange hiện tại
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers số consumer hiện tại
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages tổng số message sẵn sàng
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged tổng số message chưa xác nhận
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes số byte bộ nhớ mà tiến trình này xin hệ điều hành
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes tổng lượng bộ nhớ vật lý
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes dung lượng trống của thư mục dữ liệu
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark tỷ lệ mức nước bộ nhớ
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes giới hạn dưới dung lượng trống đĩa
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready số message sẵn sàng trong queue
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged số message chưa xác nhận trong queue
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers số consumer trên queue
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes giá trị ước lượng mức chiếm dụng bộ nhớ của queue
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total số message queue đã nhận lũy kế
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total số message queue đã gửi đi lũy kế
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total số message queue đã xác nhận lũy kế
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info siêu dữ liệu plugin (value luôn là 1, trạng thái xem tag state)
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up plugin có đang phục vụ không (1=enabled, 0=trạng thái khác: disabled/failed/down)
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Danh sách chỉ số (tất cả đều tồn tại thật, nguồn `internal/management/metrics.go`)

| Chỉ số | Loại | Tag | Ngữ nghĩa |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | tiến trình tự báo còn sống (hiện tại luôn là 1) |
| `speedmq_build_info` | gauge | `version`,`node` | thông tin build, value luôn 1 |
| `speedmq_resource_blocked` | gauge | — | mức nước tài nguyên có chặn producer không (1=đang chặn) |
| `speedmq_connections` | gauge | — | số kết nối hiện tại |
| `speedmq_channels` | gauge | — | số channel hiện tại |
| `speedmq_queues` | gauge | — | số queue hiện tại |
| `speedmq_exchanges` | gauge | — | số exchange hiện tại |
| `speedmq_consumers` | gauge | — | số consumer hiện tại |
| `speedmq_queue_messages` | gauge | — | tổng số message sẵn sàng **toàn cục** |
| `speedmq_queue_messages_unacknowledged` | gauge | — | tổng số message chưa xác nhận **toàn cục** |
| `speedmq_process_memory_bytes` | gauge | — | bộ nhớ **đang dùng** của tiến trình (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | tổng lượng bộ nhớ vật lý |
| `speedmq_disk_free_bytes` | gauge | — | dung lượng trống của thư mục dữ liệu |
| `speedmq_memory_high_watermark` | gauge | — | tỷ lệ mức nước bộ nhớ |
| `speedmq_disk_free_limit_bytes` | gauge | — | giới hạn dưới dung lượng trống đĩa |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | số message sẵn sàng của một queue |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | số message chưa xác nhận của một queue |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | số consumer của một queue |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | ước lượng mức chiếm dụng bộ nhớ của một queue |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | số message queue nhận lũy kế |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | số message queue gửi đi lũy kế |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | số message queue xác nhận lũy kế |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | siêu dữ liệu plugin, value luôn 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | plugin có đang phục vụ không (1=enabled, 0=khác) |

### 3.1 Lưu ý khi sử dụng (tránh viết sai)

- **Hai họ series cùng tên khác cardinality**: `speedmq_queue_messages_unacknowledged` **vừa** có series toàn cục không tag,
  **vừa** có series per-queue có tag; còn phía "sẵn sàng" toàn cục gọi là `speedmq_queue_messages`, per-queue gọi là
  `speedmq_queue_messages_ready` (tên không đối xứng). Khi viết quy tắc hãy dùng `{queue=~".+"}` để rõ ràng chỉ lấy họ per-queue.
- **Cách hiểu `speedmq_process_memory_bytes`**: hiện thực là `MemStats.HeapInuse + StackInuse` (**bộ nhớ đang dùng**),
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
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | mục tiêu thu thập hoàn toàn không truy cập được | kiểm tra tiến trình/cổng/mạng/xác thực; khởi động lại và xem log khởi động |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | thu thập thành công nhưng tiến trình tự báo không còn sống | mục dự phòng, xem log thoát bất thường |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | plugin disabled/failed/down | `speedmqctl plugins show <name>` xem `runtime_note`; plugin ngoài `restart=always` thường tự hồi phục |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | mức nước bộ nhớ/đĩa bị kích hoạt, producer bị chặn | kiểm tra mức nước bộ nhớ và dung lượng đĩa trống; xác nhận consumer có tiến triển không |
| `SpeedMQMemoryWatermarkHigh` | tỷ lệ bộ nhớ đang dùng > 0.9×mức nước 10m | tiến gần mức nước bộ nhớ | giảm tồn đọng/tăng tốc độ consume, phòng kích hoạt chặn |
| `SpeedMQDiskFreeLow` | trống < 1.5×giới hạn dưới đĩa 10m | thư mục dữ liệu sắp đầy | mở rộng/dọn dẹp; tới giới hạn dưới sẽ chặn producer |
| `SpeedMQQueueBacklogGrowing` | sẵn sàng >10000 và tăng đơn điệu trong 15m | queue tồn đọng liên tục | mở rộng consumer / kiểm tra phía consume; rà soát bất thường dead-letter/TTL |
| `SpeedMQQueueNoConsumers` | consumer=0 và có message sẵn sàng 15m | không ai consume | kiểm tra tiến trình phía consume; xác nhận consumer chưa mất kết nối |
| `SpeedMQUnackedPileUp` | chưa xác nhận >1000 15m | consumer bị kẹt/không ack | kiểm tra logic xử lý consumer và prefetch; khi cần thì đóng kết nối để gửi lại |
| `SpeedMQConnectionSpike` | kết nối >10000 10m | số kết nối bất thường | kiểm tra rò rỉ kết nối; client nên tái sử dụng kết nối |

> Các ngưỡng (10000 / 1000 v.v.) là **giá trị khởi điểm**, hãy điều chỉnh theo quy mô queue và đặc điểm nghiệp vụ của bạn.

---

## 5. Khoảng trống đã biết: cluster "mất đa số / không có leader" hiện chưa có chỉ số

- **Sự thật**: `/metrics` **không có** bất kỳ chỉ số dạng cluster nào (không có `speedmq_cluster_*`). Trạng thái cluster chỉ nằm trong JSON của `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Do đó** `prometheus-alerts.yml` **cố ý không viết** cảnh báo dựa trên chỉ số cluster —— viết cũng **mãi mãi không kích hoạt**
  (Prometheus không báo lỗi vì tên chỉ số không tồn tại), đó thuộc loại bàn giao "trông đúng nhưng thực ra vô hiệu".
- **Phương án tự dựng** (chọn một trong hai, đều cần bạn dựng ngoài SpeedMQ, không thuộc phạm vi kho này):
  1. dùng JSON exporter phổ thông thu thập `/api/cluster`, ánh xạ thành chỉ số tùy chỉnh (như `speedmq_cluster_has_quorum`), rồi cảnh báo trên chỉ số đó;
  2. dùng script probe định kỳ gọi `/api/cluster`, khi `has_quorum=false` hoặc `paused=true` thì bắn cảnh báo.
- Cách hiểu ngưỡng liên quan: `has_quorum=false` nghĩa là mất liên lạc với đa số; dưới `pause_minority` (mặc định) lúc này **dịch vụ sẽ tạm dừng và ngắt kết nối**.

---

## 6. Các mục **chưa kiểm chứng** khác

- Bảng điều khiển Grafana **chưa được kiểm chứng import trong Grafana thật** (chỉ kiểm tra cú pháp JSON qua).
- Quy tắc cảnh báo **chưa được kiểm chứng load trong Prometheus/Alertmanager thật** (máy cục bộ chưa dựng Prometheus).
  Nhưng **tên chỉ số trong quy tắc đã được đối chiếu từng dòng với đầu ra thật của `/metrics`** (xem §2/§3), không tồn tại vấn đề "viết sai tên dẫn đến mãi mãi không kích hoạt".
