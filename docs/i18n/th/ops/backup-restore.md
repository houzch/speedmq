# SpeedMQ การสำรองและการกู้คืน

> ข้อสรุปจาก「การทดสอบจริง」ในเอกสารนี้ทั้งหมดมาจากการซ้อมจริงหนึ่งครั้งบน **Windows + PowerShell 5.1**（`data_dir` ชั่วคราวและพอร์ตชั่วคราว）
> คำสั่งและ output สำคัญของการซ้อมถูกคัดลอกมาใน §6 ส่วนที่**【ยังไม่ได้ยืนยัน】**จะถูกระบุอย่างชัดเจน（การสำรอง/กู้คืนคลัสเตอร์、การสำรอง Docker volume เป็นต้น）

---

## 1. ต้องสำรองอะไรบ้าง

ภายใต้ `data_dir` **ต้องสำรองทั้งหมด** สิ่งสำคัญคือรายการต่อไปนี้（รูปแบบโครงสร้างดู `upgrade.md` §3）:

| เส้นทาง | หน้าที่ | ถ้าหายจะเกิดอะไร |
| --- | --- | --- |
| `meta/state.json` | snapshot metadata ของโหมดเครื่องเดียว: vhost / เอ็กซ์เชนจ์ / คิว / binding / ผู้ใช้ / สิทธิ์ / นโยบาย | โทโพโลยีและบัญชีหายทั้งหมด |
| `meta/raft.log`、`meta/raft.state`、`meta/snapshot.json` | 【คลัสเตอร์】Raft log / การลงคะแนนวาระ / snapshot + ตารางสมาชิก | ตัวตนของคลัสเตอร์และความสอดคล้องของ metadata หายไป |
| `meta/users.seeded`、`meta/vhosts.seeded` | เครื่องหมาย bootstrap | ถ้าหายจะทำให้ users/vhosts ในคอนฟิกถูก**เพาะซ้ำ**（บัญชี/vhost ที่ลบไปแล้วกลับมาอีก） |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | ข้อมูลข้อความและดัชนีของ classic queue | ข้อความที่คงทนหายไป |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【คลัสเตอร์】Raft log/snapshot ของ quorum queue | ข้อมูล quorum queue หายไป |
| ไฟล์ใบรับรอง（PEM ที่ `cert_file`/`key_file`/`ca_file` ในคอนฟิกชี้ไป） | ใบรับรอง TLS | ให้สำรองแยกจาก `data_dir` ถ้าไม่เช่นนั้นหลังรีสตาร์ท TLS จะสตาร์ทไม่ขึ้น |

> สถานะชั่วคราว（ข้อความที่ยังไม่ confirm、ผู้บริโภค、ตัวนับ prefetch）**อยู่ในหน่วยความจำเท่านั้น** ไม่เขียนลงดิสก์ การสำรอง**ไม่รวม**และไม่ควรรวมสถานะเหล่านี้

---

## 2. ข้อกำหนดด้านความสอดคล้อง: **ต้องหยุดโปรเซสก่อน**; การสำรองแบบร้อน（hot backup）**ไม่ปลอดภัย**

### 2.1 ข้อสรุป

- ✅ **วิธีที่ปลอดภัย**: **หยุดโปรเซส broker**（การออกอย่างสง่างามจะทำการ flush เก็บงาน）แล้วค่อยคัดลอก `data_dir`
- ❌ **การสำรองแบบร้อน（คัดลอกไฟล์ขณะโปรเซสกำลังรัน）: ไม่ปลอดภัย ไม่รับประกัน**

### 2.2 ทำไมการสำรองแบบร้อนจึงไม่ปลอดภัย

ที่เก็บข้อความประกอบด้วย**สองไฟล์**（ไฟล์ segment `*.seg` และไฟล์ดัชนี `index/*.idx`）ทั้งสอง**ไม่ใช่การคอมมิตแบบ atomic**:

- ตอนกู้คืน**ยึดดัชนีเป็นหลัก**เพื่อตัดสินว่า「ข้อความใดยังมีชีวิต」 แล้วอ่านจากไฟล์ segment ตาม `(หมายเลข segment, offset, ความยาว)` ในดัชนี
- ตอนสำรองแบบร้อนอาจคัดลอกได้สถานะกลาง ๆ ที่**ดัชนีอ้างถึงแล้ว แต่ไฟล์ segment ยังเขียนไม่ครบ**（หรือกลับกัน）:
  - ดัชนีอ้างถึงเรกคอร์ดที่ไม่มีใน segment → ข้อความนั้น**อ่านล้มเหลวและถูกข้าม**（เท่ากับข้อความคงทนที่ confirm แล้วหายไป）;
  - segment มีเรกคอร์ดแต่ดัชนีไม่ได้อ้างถึง → ข้อความนั้น**ไม่ถูกกู้คืน**
- แม้การกู้คืนจะใช้ CRC32 ทิ้ง**เรกคอร์ดที่เขียนค้างท้ายไฟล์** แต่นั้นครอบคลุมเพียง「การเขียนเสียที่ท้ายไฟล์เดียว」**ไม่สามารถซ่อมความไม่ซิงก์ระหว่างดัชนีกับ segment ได้**

### 2.3 เกี่ยวกับ「การเขียนลงดิสก์เกิดขึ้นเมื่อใด」（ข้อสังเกตจากการทดสอบจริง）

- ค่าเริ่มต้น `fsync: os` + `flush_interval_ms: 200`: ข้อความจะถูก `write()` ไปยังระบบปฏิบัติการโดย coroutine flush เบื้องหลังภายใน**ประมาณไม่เกิน 200 ms**
  （ไม่ fsync）และ publisher confirm ก็จะถูกส่งคืนหลังจากนั้น
- จากการทดสอบจริง: ตรวจขนาดไฟล์ segment **ทันที**หลังเผยแพร่ข้อความคงทน ก็เห็นข้อมูลแล้ว（`t=0ms seg=832`）; กล่าวคือ「ไบต์ที่ OS มองเห็นได้」เกือบจะซิงก์กับ confirm
- **ข้อควรระวัง**: นี่แสดงเพียงว่า「ไปถึงบัฟเฟอร์ของ OS」แล้ว **การ kill โปรเซสบังคับจะไม่หาย**（โปรเซสถูกฆ่าไม่ทำให้บัฟเฟอร์ OS หาย）แต่**ไฟดับจะหาย**
  ถ้าต้องการ「ได้รับ confirm คือ fsync ลงดิสก์แล้ว」ให้เปลี่ยน `storage.fsync` เป็น `batch` / `always`。**【ยังไม่ได้ทดสอบกรณีไฟดับ】**

---

## 3. ขั้นตอนการสำรอง

### 3.1 เครื่องเดียว（แนะนำ）

```powershell
# 1) หยุดโปรเซส（foreground: Ctrl+C; background: Stop-Process）
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) คัดลอก data_dir ทั้งหมด（พร้อม timestamp）
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) （ทางเลือก）ตรวจว่า snapshot metadata ในไฟล์สำรองแยกวิเคราะห์ได้
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 คลัสเตอร์

- **แต่ละโหนดสำรอง `data_dir` ของตัวเอง**（metadata ถูกจำลองผ่าน Raft ไปยังทุกโหนด ข้อมูลข้อความอยู่ที่โหนด Owner สำเนา quorum queue อยู่ในไดเรกทอรี Raft ของแต่ละตัว）
- ลำดับการหยุด: **หยุดครั้งละหนึ่งโหนดเท่านั้น**; อย่าหยุดสมาชิกที่ลงคะแนนหลายตัวพร้อมกัน（ดู `upgrade.md` §7.2）
- ถ้าต้องการ**snapshot ที่สอดคล้องกันทั้งคลัสเตอร์** ต้องหยุดทุกโหนดตามลำดับแล้วคัดลอกทีละตัว; ในโปรดักชันที่พบมากกว่าคือ「หยุด/คัดลอก/สตาร์ท ทีละโหนด」
- **【ยังไม่ได้ยืนยัน】** เครื่องนี้ยังไม่ได้ซ้อมการสำรอง/กู้คืนคลัสเตอร์จริง

### 3.3 Docker（named volume）

```powershell
# หลังหยุดคอนเทนเนอร์ ใช้คอนเทนเนอร์ครั้งเดียวบรรจุเนื้อหาของ volume แล้วคัดลอกออกมา
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【ยังไม่ได้ยืนยัน】**（เครื่องนี้ยังไม่ได้รัน Docker）。

---

## 4. ขั้นตอนการกู้คืน

### 4.1 เครื่องเดียว

```powershell
# 1) ยืนยันว่าโปรเซสถูกหยุดแล้ว
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) ย้ายออก（หรือลบ）data_dir ปัจจุบัน เพื่อไม่ให้ไฟล์ใหม่และเก่าปนกัน
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) กู้คืนด้วยไฟล์สำรอง
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) สตาร์ท
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

ประเด็นสำคัญ:
- **ต้องย้ายไดเรกทอรีเก่าออกก่อน** จะ「เอาไฟล์สำรองไปทับไดเรกทอรีที่เหลือค้างอยู่ครึ่ง ๆ」ไม่ได้;
- `data_dir` ที่กู้คืนต้องมี**ชุด vhost/คิวเดียวกับตอนสำรอง**（ชื่อไดเรกทอรีเป็นแบบเข้ารหัสแล้ว ใช้ข้ามเครื่องได้）;
- **อย่า**ฉวยโอกาสตอนกู้คืนไปแก้ `vhosts`/`users` ในไฟล์คอนฟิก（มีผลแค่ตอน bootstrap ครั้งแรก แก้ไปก็ไม่เกิดผล ดู `upgrade.md` §4.2）

### 4.2 คลัสเตอร์

- กู้คืนโหนดเดียว: กู้คืน `data_dir` ของโหนดนั้นตาม §4.1 แล้วสตาร์ท มันจะเข้าร่วมใหม่ในฐานะสมาชิกที่มีอยู่แล้วและตามทัน Raft log
- กู้คืนทั้งคลัสเตอร์: **กู้คืนและสตาร์ทโหนดฝ่ายข้างมากก่อน**（≥ ครึ่งหนึ่งของสมาชิกที่ลงคะแนน）คลัสเตอร์จึงจะเลือก leader ได้; แล้วจึงกู้คืนโหนดที่เหลือ
- **【ยังไม่ได้ยืนยัน】** การกู้คืนคลัสเตอร์ยังไม่ได้ทดสอบจริง

---

## 5. วิธีตรวจสอบหลังกู้คืน

ใช้ API ฝ่ายจัดการและไคลเอนต์จริงตรวจเทียบกัน（แนะนำให้ทำทั้งหมด）:

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) จำนวนออบเจ็กต์รวมและจำนวนข้อความรวม（จำนวนคิว/เอ็กซ์เชนจ์/binding/ผู้ใช้; messages/ready/unacked）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) ตรวจ messages / messages_ready ทีละคิว（เทียบกับที่บันทึกไว้ก่อนสำรองได้）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / ผู้ใช้ / นโยบาย ยังอยู่ครบหรือไม่
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) คลัสเตอร์（โหมดเครื่องเดียวจะคืนค่า enabled=false / mode=local / role=single）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **ดู log การสตาร์ท**: ควรมี `已从磁盘恢复队列消息 ... messages=N` และ `队列已恢复持久化消息 ... messages=N`; ค่า N ควรตรงกับก่อนสำรอง
- **คำเตือนใน log**: ถ้าคิวนั้นเคยมีข้อความถูกบริโภค/ล้างมาก่อน ตอนกู้คืนอาจมี
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` และ `恢复时清理了无存活消息的段`
  นี่คือส่วนที่ตกค้างของดัชนีของ**เรกคอร์ดที่ชำระบัญชีแล้ว（ack/purge）** ถือเป็น**สัญญาณรบกวนของ log ที่ทราบอยู่แล้ว ไม่กระทบความถูกต้องของข้อมูล**（ดู §7）
- **ไคลเอนต์จริง**: ดึงข้อความกลับจากคิวและตรวจจำนวน/เนื้อหา（ดู §6 ขั้นตอนที่ (7)）

---

## 6. การซ้อมทดสอบจริง（คำสั่งและ output จริง）

> สภาพแวดล้อม: `data_dir` อยู่ในไดเรกทอรีชั่วคราว, AMQP `127.0.0.1:5676`、ฝ่ายจัดการ `127.0.0.1:15677`、MQTT `127.0.0.1:1884`,
> บัญชีเริ่มต้น `guest/guest`。log การสตาร์ท:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) สร้างโทโพโลยีแบบ durable + ส่ง 5 ข้อความคงทน（ไคลเอนต์จริง `amqp091-go`）**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) สร้างผู้ใช้ / vhost / สิทธิ์ / นโยบาย（API ฝ่ายจัดการ）**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) สถานะก่อนสำรอง（API ฝ่ายจัดการ）**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) ไฟล์บนดิสก์ก่อนสำรอง**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) หยุดโปรเซส → สำรอง → ล้าง → กู้คืน**

```
listeners still up: 0                       # 5676/1884/15677 ปิดทั้งหมดแล้ว
=== 备份内容 ===   （เหมือนกับ (4) ทุกประการ คัดลอกทีละไบต์）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # ลบ data_dir เดิมแล้ว จำลองว่าข้อมูลหาย
=== 恢复后内容 ===   （คัดลอกกลับมาจากไฟล์สำรอง เหมือนกับ (4)）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) log การกู้คืนหลังรีสตาร์ท（บรรทัดสำคัญ）**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> พร้อมกันนั้นมี `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"` หลายบรรทัด:
> เหล่านี้คือส่วนที่ตกค้างของดัชนีที่เหลือจากข้อความ**ที่ถูก purge ไปก่อนหน้านี้**ในการซ้อมครั้งนี้（ชำระบัญชีแล้ว ข้อมูล segment ถูกเก็บคืนแล้ว）**ไม่กระทบการกู้คืนของ 5 ข้อความด้านล่าง**

**(7) การยืนยันหลังกู้คืน: API ฝ่ายจัดการ + ไคลเอนต์จริง**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**ข้อสรุป**: โทโพโลยี durable（เอ็กซ์เชนจ์+คิว+binding）、5 ข้อความคงทน、ผู้ใช้、vhost、สิทธิ์、นโยบาย**กู้คืนได้ทั้งหมด**
ไคลเอนต์จริงดึงข้อความทั้งหมดกลับมาได้ตามเดิม。**การซ้อมผ่าน**

### 6.1 การเทียบเคียง: การกู้คืนคิวที่ยังไม่ผ่านการบริโภค/purge จะ「เงียบ」กว่า

เพื่อแยกว่า WARN ข้างต้นเป็นปรากฏการณ์ทั่วไปหรือไม่ จึงทำ**การเทียบเคียงแบบควบคุม**อีกครั้ง: สร้าง durable queue ใหม่ `clean.q`、ส่ง 3 ข้อความคงทน、
**ไม่บริโภคไม่ล้าง** หยุดโปรเซสแล้วรีสตาร์ท:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**ไม่มี WARN ใด ๆ** แสดงว่า WARN ปรากฏเฉพาะในกรณี「ในดัชนียังเหลือเรกคอร์ดที่ชำระบัญชีแล้ว」（ดู §7）

---

## 7. ปัญหาที่ทราบและข้อจำกัด（บันทึกตามจริง）

1. **สัญญาณรบกวนใน log การกู้คืน（สังเกตเห็นจริง）**: เมื่อคิวเคยมีการบริโภค/ล้างในอดีต（ข้อความถูก ack/purge แล้ว）
   ดัชนีของมันยังคงอ้างถึงเรกคอร์ดที่ถูกเก็บคืนแล้ว ตอนกู้คืนจะ**พิมพ์ WARN `恢复消息失败，已跳过` ทีละเรกคอร์ด**
   และสร้าง/ล้าง `000000.seg` เปล่าใหม่หนึ่งไฟล์（log `恢复时清理了无存活消息的段`）
   **ไม่กระทบความถูกต้องของข้อมูล**（ข้อความที่ยังมีชีวิตและยังไม่ confirm กู้คืนได้ถูกต้อง）แต่จะ**ปนเปื้อน log** และในคิวใหญ่/ปริมาณงานสูงอาจพิมพ์รัวเต็มจอ
   แนะนำ: ให้ยึด `已从磁盘恢复队列消息 ... messages=N` เป็นหลัก มองข้าม WARN ที่เกี่ยวกับเรกคอร์ดที่ชำระบัญชีแล้วเหล่านี้;
   ถ้าปริมาณ log รับไม่ได้ โปรดแจ้งผู้ดูแล kernel（เอกสารนี้ไม่แก้โค้ด）
2. **การสำรองแบบร้อนไม่ปลอดภัย**（§2）: อย่าคัดลอก `data_dir` ขณะโปรเซสกำลังรัน
3. **`fsync: os` ไม่รับประกันว่าไฟดับแล้วไม่หาย**: ถ้าต้องการ「confirm คือลงดิสก์」ให้ใช้ `batch` / `always`
4. **รหัสผ่านเป็นข้อความล้วน**: รหัสผ่านผู้ใช้ใน `meta/state.json` เป็น**ข้อความล้วน**（จากการทดสอบจริงเห็น `"password":"drillpass"`）——
   ไฟล์สำรองจึง**ต้องจัดการเป็นข้อมูลอ่อนไหว**（ควบคุมการเข้าถึง เข้ารหัสจัดเก็บ）ดูรายละเอียดใน `security-baseline.md`
5. **【ยังไม่ได้ยืนยัน】** การสำรอง/กู้คืนคลัสเตอร์、การสำรอง/กู้คืน Docker volume、กรณีไฟดับ、การเขียนพร้อมกันระหว่างกระบวนการกู้คืน
