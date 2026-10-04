<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | **ไทย** | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

มิดเดิลแวร์รับส่งข้อความที่**เข้ากันได้กับ RabbitMQ** เขียนด้วยภาษา Go ไคลเอนต์ RabbitMQ ที่มีอยู่**ไม่ต้องแก้โค้ด ไม่ต้องเปลี่ยน SDK** เพียงเปลี่ยนที่อยู่ที่เชื่อมต่อก็เข้าใช้งานได้

## ภาพรวม

- **เข้ากันได้กับโปรโตคอล**: AMQP 0-9-1 (รวมส่วนขยายของ RabbitMQ) และ MQTT 3.1.1; เกณฑ์อ้างอิงความเข้ากันได้คือ**ความหมายเชิงความหมายของ RabbitMQ 4.3**
- **ติดตั้งง่าย**: ไฟล์ไบนารีเดียว / คอนเทนเนอร์เดียว มี UI จัดการฝังอยู่แล้ว ไม่ต้องมี Nginx ฐานข้อมูล หรือรันไทม์ Node เพิ่มเติม
- **เพียงพอต่อการปฏิบัติการ**: UI จัดการ (คิว / เอ็กซ์เชนจ์ / การเชื่อมต่อ / สิทธิ์บัญชี / virtual host / นโยบาย / ลิมิต / คลัสเตอร์)、Prometheus `/metrics`、บรรทัดคำสั่ง `swiftmqctl`
- **พอร์ตเริ่มต้น**: `5672` (AMQP)、`1883` (MQTT)、`15672` (UI จัดการ / HTTP API / เมตริก)

ความสามารถที่มีอยู่แล้ว: การทำให้ข้อมูลคงทน (segment log + ระดับ fsync + การกู้คืนหลังล่ม)、publisher confirm、TTL / dead letter / การจำกัดความยาว、ลำดับความสำคัญของผู้บริโภค、Direct Reply-To、คลัสเตอร์ (Raft metadata + quorum queue + การส่งต่อข้ามโหนด)、การเปิด-ปิดปลั๊กอินแบบ hot

***

## เริ่มต้นอย่างรวดเร็ว

### วิธีที่หนึ่ง: Docker（แนะนำ）

**ไม่ต้อง clone repo ก็ใช้ได้: ดึงอิมเมจแล้วรันเลย**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.02
```

อิมเมจเผยแพร่ไว้สองที่ด้วยเนื้อหาเดียวกัน (เลือกที่เร็วกว่า): Docker Hub `houzch/swiftmq` และ GitHub GHCR `ghcr.io/houzch/swiftmq` ทั้งสองที่มี `linux/amd64` และ `linux/arm64`

- ข้อมูลอยู่บน named volume `swiftmq-data` สร้างคอนเทนเนอร์ใหม่ก็ไม่หาย
- หยุด / ลบ: `docker stop swiftmq`, `docker rm swiftmq` (volume ข้อมูลยังอยู่)

**ถ้าต้องแก้คอนฟิกหรือใช้ compose ให้ clone repo:**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # ใช้ภาพที่เผยแพร่แล้ว; เปลี่ยนเป็น up -d --build ถ้าต้องการบิลด์เองในเครื่อง

docker compose ps        # สถานะควรเป็น Up (healthy)
docker compose logs -f   # ดู log แบบต่อเนื่อง
```

- คอนฟิกถูก mount แบบอ่านอย่างเดียวจาก `configs/swiftmqd.json` แก้แล้วใช้ `docker compose restart` ให้มีผล
- หยุด: `docker compose down` (ข้อมูลยังอยู่); `docker compose down -v` (ลบข้อมูลด้วย)

### วิธีที่สอง: ไบนารีในเครื่อง（ต้องมี Go 1.24+）

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> ผลลัพธ์การ build ของ UI จัดการไม่ได้ถูกเก็บเข้า repository ถ้าต้องการใช้ UI ให้รัน `npm ci && npm run build` ใน `web/` ก่อน;
> ไม่ build ก็ยังสตาร์ทและรับส่งข้อความได้ตามปกติ เพียงแต่เมื่อเข้าถึง `/` จะแจ้งว่า「UI จัดการยังไม่ได้ build」

### เข้าสู่ระบบครั้งแรก（ต้องเปลี่ยนบัญชีเริ่มต้นก่อนเป็นอันดับแรก）

| ทางเข้า | ที่อยู่ / ข้อมูลรับรอง |
| --- | --- |
| UI จัดการ | <http://localhost:15672/>（ผู้ใช้ `guest`, รหัสผ่าน `guest`） |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883`（บัญชีเดียวกันกับด้านบน） |

บัญชีหลักของอินสแตนซ์ที่ติดตั้งใหม่จะมีการตั้งค่า「บังคับเปลี่ยนรหัสเมื่อเข้าสู่ระบบครั้งแรก」: หลังจากเข้าสู่ระบบ UI จัดการแล้วจะ**บังคับให้แก้ทั้งชื่อบัญชีและรหัสผ่านพร้อมกัน** แก้เสร็จจึงจะเข้าสู่หน้าจอหลังบ้านได้

หรือจะเรียก API โดยตรงก็ทำได้（เหมาะกับการทำงานอัตโนมัติ）:

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<รหัสผ่านใหม่>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ ค่าเริ่มต้น `guest/guest` มีพฤติกรรมเหมือน RabbitMQ: **อนุญาตให้เข้าสู่ระบบจากเครื่องเดียวกันเท่านั้น** การเชื่อมต่อจากภายนอกคอนเทนเนอร์ / จากระยะไกล ต้องเปิด `remote_access` ให้ผู้ใช้นั้นในคอนฟิก（คอนฟิกตัวอย่างเปิดไว้แล้วสำหรับกรณีคอนเทนเนอร์）
> **เมื่อบริการเปิดให้เข้าถึงจากภายนอกได้แล้ว โปรดเปลี่ยนข้อมูลรับรองทันที**

### เชื่อมต่อแอปพลิเคชันของคุณ（เพียงเปลี่ยนที่อยู่ที่เชื่อมต่อ）

```python
# Python（pika）
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT（ไคลเอนต์ mosquitto）
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

HTTP API สำหรับจัดการเข้ากันได้กับ `rabbitmqadmin`; การ「เพิ่มคิว / เอ็กซ์เชนจ์」ใน UI จัดการก็คือ endpoint ประกาศมาตรฐาน สคริปต์ก็ทำได้เช่นกัน:

```bash
# ประกาศคิว（quorum queue แสดงด้วย arguments: {"x-queue-type":"quorum"}）
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### การปฏิบัติการประจำวัน

| รายการ | ทางเข้า |
| --- | --- |
| UI จัดการ | <http://localhost:15672/>: คิว / เอ็กซ์เชนจ์ / การเชื่อมต่อ / สิทธิ์บัญชี / virtual host / นโยบาย / ลิมิต / feature flag / คลัสเตอร์ มุมขวาบนตั้งค่าการรีเฟรชอัตโนมัติและ**ภาษาของอินเทอร์เฟซ**ได้ |
| เมตริกการมอนิเตอร์ | <http://localhost:15672/metrics>（ข้อความรูปแบบ Prometheus ต้องยืนยันตัวตน）; แดชบอร์ดและการแจ้งเตือนดูที่ [docs/ops/monitoring](ops/monitoring/README.md) |
| บรรทัดคำสั่ง | `./bin/swiftmqctl status`、`list_queues`、`plugins list`、`plugins disable amqp091`（ปิดแบบ hot พอร์ตปิดทันที） |
| การตรวจสอบสุขภาพ | `nc -z 127.0.0.1 15672`（compose มี healthcheck ในตัวแล้ว） |
| สำรองและกู้คืน | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| การอัปเกรด | [docs/ops/upgrade.md](ops/upgrade.md) |
| เกณฑ์ความปลอดภัย | [docs/ops/security-baseline.md](ops/security-baseline.md) |

คอนฟิกที่ใช้บ่อย（ตัวอย่างเต็มดูที่ [configs/swiftmqd.json](../../../configs/swiftmqd.json) หรือใช้ตัวแปรสภาพแวดล้อม `SWIFTMQ_*` เขียนทับได้）:

| รายการคอนฟิก | คำอธิบาย | ค่าเริ่มต้น |
| --- | --- | --- |
| `data_dir` | ไดเรกทอรีข้อมูล（ข้อความ + metadata）**ต้องทำให้คงทนเสมอ** | `data` |
| `listeners` | ที่อยู่รับฟังของแต่ละโปรโตคอล ตั้งค่า TLS ได้ | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | ที่อยู่รับฟังของ UI จัดการ / API | `:15672` |
| `management.language` | ภาษาเริ่มต้นของ UI จัดการ; ถ้าเว้นว่างจะเลือกอัตโนมัติตามเขตเวลาของที่ติดตั้ง | อัตโนมัติ |
| `storage.fsync` | ระดับการเขียนลงดิสก์ `none / os / batch / always`（กำหนดจังหวะของ confirm ไปพร้อมกัน） | `os` |
| `storage.memory_high_watermark`、`storage.disk_free_limit` | ระดับทรัพยากร: เมื่อถึงเกณฑ์จะบล็อกผู้ผลิต **ไม่ทำข้อความหาย** | `0.4` / 50 MiB |
| `users` | ตารางผู้ใช้ในตัว（รหัสผ่าน + แท็ก + `remote_access`） | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | คลัสเตอร์หลายโหนด（ปิดเป็นค่าเริ่มต้น）การเปลี่ยนสมาชิกใช้ `swiftmqctl add_member` | ปิด |

> พอร์ตอาจถูกใช้งานอยู่: ใช้ `listeners` / `management.addr` เปลี่ยนเป็นพอร์ตอื่นก็ได้

***

## โครงสร้างโปรเจกต์

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # จุดเข้าโปรเซส broker（ตัวที่ต้องรันก็คืออันนี้）
│   └── swiftmqctl/      # CLI สำหรับปฏิบัติการ（ผ่าน HTTP API ของฝ่ายจัดการ แยกจากเวอร์ชันของ kernel）
├── internal/            # การทำงานของ kernel
│   ├── protocol/        # ปลั๊กอินโปรโตคอล: amqp091、mqtt（เข้ารหัส-ถอดรหัส / method / session）
│   ├── broker/          # kernel: vhost、เอ็กซ์เชนจ์、คิว、dead letter、การควบคุมการไหล、มุมมองฝ่ายจัดการ
│   ├── store/           # ความคงทน: segment log、ดัชนีคิว、การกู้คืนหลังล่ม
│   ├── raft/ meta/      # คลัสเตอร์: Raft ที่พัฒนาขึ้นเองและการจำลอง metadata
│   ├── management/      # HTTP API ฝ่ายจัดการ + เมตริก Prometheus + บริการไฟล์ static ของ UI ที่ฝังในตัว
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # สัญญาที่มั่นคงต่อภายนอก: API ปลั๊กอิน（plugin）และโปรโตคอลสายของปลั๊กอินโปรเซสภายนอก（sidecar）
├── web/                 # โปรเจกต์ฟรอนต์เอนด์ของ UI จัดการ（Vue 3 + Vite）ผลลัพธ์ตอน build ถูกฝังเข้าไบนารีผ่าน go:embed
├── configs/             # คอนฟิกตัวอย่าง
├── docs/ops/            # เอกสารปฏิบัติการ: สำรองกู้คืน / อัปเกรด / เกณฑ์ความปลอดภัย / มอนิเตอร์
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（QR code กลุ่มพูดคุย）
```

***

## การมีส่วนร่วม

ยินดีรับ Issue และ Pull Request จุดยืนของโปรเจกต์นี้คือ**ความเข้ากันได้กับโปรโตคอล** ดังนั้น:

- แก้ bug โปรดระบุพฤติกรรมของ RabbitMQ ที่เกี่ยวข้อง（เวอร์ชัน、ไคลเอนต์、ขั้นตอนการทำซ้ำ）;
- การเปลี่ยนแปลงที่เกี่ยวข้องกับรายละเอียดโปรโตคอล โปรดแนบผลการเทียบกับ RabbitMQ;
- ก่อนส่ง ให้แน่ใจว่า `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` ผ่านทั้งหมด

***

## สัญญาอนุญาต

โปรเจกต์นี้ใช้ [Apache License 2.0](../../../LICENSE)

อนุญาตให้ใช้ ดัดแปลง เผยแพร่（รวมถึงใช้เชิงพาณิชย์）ต้องคงข้อความลิขสิทธิ์และสัญญาอนุญาตไว้ และไม่มีการรับประกันใด ๆ

Copyright 2026 houzch（ดู [NOTICE](../../../NOTICE)）

***

## กิตติกรรมประกาศ

ข้อกำหนดโปรโตคอล AMQP 0-9-1 และความหมายเชิงพฤติกรรมของ [RabbitMQ](https://www.rabbitmq.com/) เป็นเกณฑ์อ้างอิงสำหรับงานความเข้ากันได้ของโปรเจกต์นี้ โปรเจกต์นี้เป็นการพัฒนาขึ้นเอง ไม่มีความเกี่ยวข้องกับทางการของ RabbitMQ และไม่ได้ใช้โค้ดของ RabbitMQ

***

## เข้าร่วมกลุ่มพูดคุย

สแกน QR code เพื่อเข้ากลุ่มพูดคุย SwiftMQ มีปัญหาสามารถถามในกลุ่มได้เลย:

![กลุ่มพูดคุย SwiftMQ](../../../1280X1280.PNG)
