# SpeedMQ การมอนิเตอร์และการแจ้งเตือน

ไดเรกทอรีนี้มีเทมเพลตการมอนิเตอร์ที่ใช้ได้ทันที:

| ไฟล์ | หน้าที่ |
| --- | --- |
| `prometheus-alerts.yml` | กฎการแจ้งเตือนของ Prometheus（`groups: - name: speedmq`） |
| `grafana-dashboard.json` | แดชบอร์ด Grafana ที่นำเข้าได้（แผงครอบคลุมสัญญาณสำคัญด้านล่าง） |
| `README.md` | วิธีใช้、รายการเมตริก、ความหมายและการรับมือของแต่ละการแจ้งเตือน、ช่องว่างที่ทราบ |

---

## 1. ใช้งานอย่างไร

### 1.1 การดึงข้อมูล（Prometheus）

ฝ่ายจัดการ（ค่าเริ่มต้น `:15672`）เปิดเผย `/metrics` ในรูปแบบข้อความ Prometheus **ต้องใช้ Basic Auth**:

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

> แนะนำให้สร้างบัญชีอ่านอย่างเดียวแยกสำหรับการมอนิเตอร์（แท็ก `monitoring` ก็อ่านเมตริกได้）อย่าใช้รหัสผ่านผู้ดูแลซ้ำ

ตรวจสอบว่าดึงข้อมูลเป็นปกติหรือไม่（PowerShell）:

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 กฎการแจ้งเตือน

วาง `prometheus-alerts.yml` ลงในไดเรกทอรี rules ของ Prometheus อ้างอิงใน `prometheus.yml` แล้ว reload:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

ในกฎใช้ `job="speedmq"` เป็นค่าเดียวกันทั้งหมด; ถ้าชื่อ job ของคุณต่างออกไป โปรดแทนทั้งไฟล์

### 1.3 แดชบอร์ด Grafana

นำเข้า `grafana-dashboard.json` ผ่าน **Dashboards → Import → อัปโหลด JSON** ตอนนำเข้าเลือก data source Prometheus ของคุณ
（ในแดชบอร์ดอ้างอิงด้วยตัวแปร `${DS_PROMETHEUS}`）ตัวแปรเทมเพลต `DS_PROMETHEUS` จะถูกกำหนดค่าในการแมปตอนนำเข้า

**【ยังไม่ได้ยืนยัน】** เครื่องนี้ยังไม่ได้สตาร์ทอินสแตนซ์ Grafana ยังไม่ได้ตรวจสอบการนำเข้าจริง; JSON นั้นตรวจสอบเพียงไวยากรณ์ JSON（14 แผง แยกวิเคราะห์ผ่าน）

---

## 2. ตัวอย่างจริงของ `/metrics`（หลักฐาน）

ต่อไปนี้คือ**output จริง**ของ `/metrics` ของอินสแตนซ์ `1.0.0` บนเครื่องนี้（สร้าง durable queue `persist.q` ไว้หนึ่งคิว
จึงมีเมตริกระดับ per-queue ที่มีป้าย `vhost`/`queue` ปรากฏขึ้น）:

```
# HELP speedmq_up 节点是否存活
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info 构建信息
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections 当前连接数
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels 当前通道数
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues 当前队列数
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges 当前交换机数
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers 当前消费者数
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages 就绪消息总数
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged 未确认消息总数
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes 物理内存总量
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes 数据目录可用空间
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark 内存水位比例
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready 队列中的就绪消息数
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers 队列上的消费者数
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes 队列内存占用估算值
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total 队列累计接收的消息数
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. รายการเมตริก（มีอยู่จริงทั้งหมด ที่มา `internal/management/metrics.go`）

| เมตริก | ชนิด | ป้าย | ความหมาย |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | โปรเซสประกาศตัวเองว่ายังอยู่（ตอนนี้เป็น 1 เสมอ） |
| `speedmq_build_info` | gauge | `version`,`node` | ข้อมูลการ build ค่า value เป็น 1 เสมอ |
| `speedmq_resource_blocked` | gauge | — | ระดับทรัพยากรบล็อกผู้ผลิตหรือไม่（1=กำลังบล็อก） |
| `speedmq_connections` | gauge | — | จำนวนการเชื่อมต่อปัจจุบัน |
| `speedmq_channels` | gauge | — | จำนวนช่องทางปัจจุบัน |
| `speedmq_queues` | gauge | — | จำนวนคิวปัจจุบัน |
| `speedmq_exchanges` | gauge | — | จำนวนเอ็กซ์เชนจ์ปัจจุบัน |
| `speedmq_consumers` | gauge | — | จำนวนผู้บริโภคปัจจุบัน |
| `speedmq_queue_messages` | gauge | — | จำนวนข้อความพร้อมใช้**ทั่วทั้งระบบ** |
| `speedmq_queue_messages_unacknowledged` | gauge | — | จำนวนข้อความที่ยังไม่ confirm **ทั่วทั้งระบบ** |
| `speedmq_process_memory_bytes` | gauge | — | หน่วยความจำ**ที่กำลังใช้**ของโปรเซส（`HeapInuse+StackInuse`） |
| `speedmq_memory_total_bytes` | gauge | — | หน่วยความจำกายภาพทั้งหมด |
| `speedmq_disk_free_bytes` | gauge | — | พื้นที่ว่างของไดเรกทอรีข้อมูล |
| `speedmq_memory_high_watermark` | gauge | — | สัดส่วนระดับน้ำหน่วยความจำ |
| `speedmq_disk_free_limit_bytes` | gauge | — | ขีดจำกัดล่างของพื้นที่ดิสก์ที่เหลือ |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | จำนวนข้อความพร้อมใช้ของคิวหนึ่ง |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | จำนวนข้อความที่ยังไม่ confirm ของคิวหนึ่ง |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | จำนวนผู้บริโภคของคิวหนึ่ง |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | ค่าประมาณหน่วยความจำของคิวหนึ่ง |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | จำนวนข้อความสะสมที่คิวรับมา |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | จำนวนข้อความสะสมที่คิวส่งมอบ |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | จำนวนข้อความสะสมที่คิวยืนยัน |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | ข้อมูลเมตาของปลั๊กอิน ค่า value เป็น 1 เสมอ |
| `speedmq_plugin_up` | gauge | `name`,`state` | ปลั๊กอินให้บริการอยู่หรือไม่（1=enabled，0=อื่น ๆ） |

### 3.1 ข้อควรระวังในการใช้（เพื่อไม่ให้เขียนผิด）

- **สองตระกูลอนุกรมที่ชื่อเดียวกันแต่ความหลากหลายของป้ายต่างกัน**: `speedmq_queue_messages_unacknowledged` **ทั้ง**มีอนุกรมทั่วทั้งระบบที่ไม่มีป้าย
  **และ**มีอนุกรม per-queue ที่มีป้าย; ส่วนฝั่ง「พร้อมใช้」ทั่วทั้งระบบชื่อ `speedmq_queue_messages`、per-queue ชื่อ
  `speedmq_queue_messages_ready`（ชื่อไม่สมมาตรกัน）เวลเขียนกฎให้ใช้ `{queue=~".+"}` ระบุให้ชัดว่าเอาเฉพาะตระกูล per-queue
- **หลักเกณฑ์ของ `speedmq_process_memory_bytes`**: การทำงานจริงคือ `MemStats.HeapInuse + StackInuse`（**หน่วยความจำที่กำลังใช้**）
  ใช้หลักเกณฑ์เดียวกับการตัดสินระดับน้ำหน่วยความจำของ kernel; แต่ข้อความ `# HELP` ของมันเขียนว่า「ไบต์หน่วยความจำที่ร้องขอจากระบบปฏิบัติการ」**ข้อความไม่ตรงกับหลักเกณฑ์จริง**
  ให้ยึดเอกสารนี้เป็นหลัก
- **อนุกรม per-queue ปรากฏเฉพาะเมื่อคิวมีอยู่**: หลังลบคิว อนุกรมนั้นจะหายไป（ฝั่ง Prometheus จะกลายเป็น stale）
  การแจ้งเตือนที่เกี่ยวกับ「คิวควรมีอยู่แต่ไม่มีข้อมูล」สามารถใช้ร่วมกับ `absent()` หรือ `or vector(0)` ของ Grafana
- **counter จะรีเซ็ตเป็นศูนย์หลังโปรเซสรีสตาร์ท**: `*_total` เป็นการสะสมภายในโปรเซส รีสตาร์ทก็เริ่มจาก 0; โปรดใช้ `rate()`/`increase()`
  อย่าตั้ง threshold กับค่าสัมบูรณ์โดยตรง
- **ไม่มีสัญญาณคลัสเตอร์ใน metrics**: ดู §5

---

## 4. ความหมายของการแจ้งเตือนและข้อเสนอการรับมือ（สอดคล้องกับ `prometheus-alerts.yml`）

| การแจ้งเตือน | เงื่อนไขที่ทริกเกอร์ | ความหมาย | ข้อเสนอการรับมือ |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | เป้าหมายการดึงทั้งหมดเข้าถึงไม่ได้ | ตรวจโปรเซส/พอร์ต/เครือข่าย/การยืนยัน; รีสตาร์ทและดู log การสตาร์ท |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | ดึงสำเร็จแต่โปรเซสประกาศว่าตัวเองไม่อยู่ | รายการสำรอง ดู log ที่ออกผิดปกติ |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | ปลั๊กอิน disabled/failed/down | `speedmqctl plugins show <name>` ดู `runtime_note`; ปลั๊กอินภายนอก `restart=always` มักหายเอง |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | ระดับน้ำหน่วยความจำ/ดิสก์ทริกเกอร์ ผู้ผลิตถูกบล็อก | ดูระดับน้ำหน่วยความจำและดิสก์ที่ว่าง; ยืนยันว่าผู้บริโภคเดินหน้าหรือไม่ |
| `SpeedMQMemoryWatermarkHigh` | สัดส่วนหน่วยความจำที่ใช้ > 0.9×ระดับน้ำ 10m | ใกล้ระดับน้ำหน่วยความจำ | ลดการสะสม/เพิ่มความเร็วการบริโภค กันการทริกเกอร์บล็อก |
| `SpeedMQDiskFreeLow` | ว่าง < 1.5×ขีดจำกัดล่างดิสก์ 10m | ไดเรกทอรีข้อมูลใกล้เต็ม | ขยาย/ล้าง; เมื่อถึงขีดจำกัดล่างจะบล็อกผู้ผลิต |
| `SpeedMQQueueBacklogGrowing` | พร้อมใช้ >10000 และเพิ่มขึ้นแบบ monotonic 15m | คิวสะสมต่อเนื่อง | เพิ่มผู้บริโภค / ตรวจฝั่งผู้บริโภค; ตรวจ dead letter/TTL ที่ผิดปกติ |
| `SpeedMQQueueNoConsumers` | ผู้บริโภค=0 และมีข้อความพร้อมใช้ 15m | ไม่มีใครบริโภค | ตรวจโปรเซสฝั่งผู้บริโภค; ยืนยันว่าผู้บริโภคไม่หลุด |
| `SpeedMQUnackedPileUp` | ยังไม่ confirm >1000 15m | ผู้บริโภคค้าง/ไม่ ack | ตรวจตรรกะการประมวลผลของผู้บริโภคและ prefetch; ถ้าจำเป็นปิดการเชื่อมต่อแล้วส่งใหม่ |
| `SpeedMQConnectionSpike` | การเชื่อมต่อ >10000 10m | จำนวนการเชื่อมต่อผิดปกติ | ตรวจการรั่วของการเชื่อมต่อ; ไคลเอนต์ควรใช้การเชื่อมต่อซ้ำ |

> threshold（10000 / 1000 เป็นต้น）เป็น**ค่าเริ่มต้น** โปรดปรับตามขนาดคิวและลักษณะธุรกิจของคุณ

---

## 5. ช่องว่างที่ทราบ: คลัสเตอร์「สูญเสียฝ่ายข้างมาก / ไม่มี leader」ตอนนี้ยังไม่มีเมตริก

- **ข้อเท็จจริง**: `/metrics` **ไม่มี**เมตริกประเภทคลัสเตอร์ใด ๆ（ไม่มี `speedmq_cluster_*`）สถานะคลัสเตอร์อยู่ใน JSON ของ `GET /api/cluster` เท่านั้น:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`
- **ดังนั้น** `prometheus-alerts.yml` **เจตนาไม่เขียน**การแจ้งเตือนที่อิงเมตริกคลัสเตอร์——เขียนไปก็**จะไม่ทริกเกอร์ตลอดกาล**
  （Prometheus จะไม่ error เพราะชื่อเมตริกไม่มีอยู่）นั่นถือเป็นงานส่งมอบที่「ดูเหมือนถูก แต่จริง ๆ ใช้ไม่ได้」
- **วิธีสร้างเอง**（เลือกหนึ่งในสอง ต้องสร้างนอก SpeedMQ ทั้งคู่ ไม่อยู่ในขอบเขตของ repository นี้）:
  1. ใช้ JSON exporter ทั่วไปดึง `/api/cluster` แมปเป็นเมตริกที่กำหนดเอง（เช่น `speedmq_cluster_has_quorum`）แล้วแจ้งเตือนกับเมตริกนั้น;
  2. ใช้สคริปต์โพรบเรียก `/api/cluster` เป็นระยะ เมื่อ `has_quorum=false` หรือ `paused=true` ก็ยิงการแจ้งเตือน
- หลักเกณฑ์ threshold ที่เกี่ยวข้อง: `has_quorum=false` หมายความว่าขาดการติดต่อกับฝ่ายข้างมาก; ภายใต้ `pause_minority`（ค่าเริ่มต้น）ตอนนั้น**บริการจะหยุดและตัดการเชื่อมต่อ**

---

## 6. รายการ**ยังไม่ได้ยืนยัน**อื่น ๆ

- แดชบอร์ด Grafana **ยังไม่ได้ตรวจสอบการนำเข้าใน Grafana จริง**（ตรวจสอบผ่านเฉพาะไวยากรณ์ JSON）
- กฎการแจ้งเตือน**ยังไม่ได้ตรวจสอบการโหลดใน Prometheus/Alertmanager จริง**（เครื่องนี้ยังไม่ได้สตาร์ท Prometheus）
  แต่**ชื่อเมตริกในกฎได้เทียบทีละรายการกับ output จริงของ `/metrics` แล้ว**（ดู §2/§3）จึงไม่มีปัญหา「เขียนชื่อผิดจนไม่ทริกเกอร์ตลอดกาล」
