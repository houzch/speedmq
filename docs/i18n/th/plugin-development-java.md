# คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก SpeedMQ —— Java

> **กลุ่มเป้าหมาย**: นักพัฒนาที่เขียนปลั๊กอินโปรเซสภายนอก (sidecar) ให้ SpeedMQ ด้วย Java
> **อ่านก่อน**: [คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก (sidecar)](plugin-development.md) (แบบจำลองทางความคิด / ฟิลด์คอนฟิก / ตารางสรุปโปรโตคอลสาย)
> **โปรเจกต์ตัวอย่าง**: เวิร์กสเปซ `speedmq-plugin/java/SidecarPlugin.java` (ไฟล์เดียว เฉพาะไลบรารีมาตรฐาน JDK ไม่ต้องใช้ Maven/Gradle)

---

## 1. ตอนรันมันเป็นอย่างไร

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

สามประเด็นสำคัญ: **โปรเซสของคุณคือเซิร์ฟเวอร์** (รอเคอร์เนลมาเชื่อมต่อ); **พอร์ตภายนอกเปิดโดยเคอร์เนล** (`protocols[].listeners`);
**`prefix` ต้องไม่ว่าง** (คำนำหน้าว่าง = ไม่มีส่วนในการดักจับ การเชื่อมต่อจะไม่ถูกส่งให้คุณ; ทดสอบจริงจะถูกตัดทันที ≤8 ไบต์ ASCII)

---

## 2. สามขั้นตอนให้รันขึ้น

### ขั้นที่หนึ่ง: คอนฟิก

`speedmqd.json` (**คอนฟิกจริงเป็น JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**):

```json
{
  "plugins": {
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/speedmq/classes", "SidecarPlugin",
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

### ขั้นที่สอง: คอมไพล์และสตาร์ท

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### ขั้นที่สาม: ตรวจสอบ

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. ประเด็นการ implement

### 3.1 การแบ่งเฟรม

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Java ใช้ `DataInputStream`/`DataOutputStream` ง่ายที่สุด —— `readInt`/`writeInt` ของมันเป็น**บิ๊กเอนเดียน**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

ฝั่งเขียนต้อง**เป็นลำดับ** (ฮาร์ตบีต การตอบกลับ บล็อกข้อมูลมาจากเธรดต่างกัน):

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

เพย์โหลดเฟรมข้อมูล = `4 ไบต์ big-endian หมายเลขสตรีม + ไบต์ดิบ`

### 3.2 การจับมือและฮาร์ตบีต

เคอร์เนล**ส่ง Hello ก่อน** คุณตอบ `HelloAck`; เคอร์เนลตรวจสอบ `name` กับ `api_version` (ปัจจุบัน `v1`) แล้วจึงเชื่อมต่อ
หลังจากนั้น `Ping` ทุก 2 วินาที ตอบ `Pong`

### 3.3 แบบจำลองการทำงานพร้อมกัน (ฉบับ Java)

| บทบาท | เธรด |
| --- | --- |
| ลูปอ่านเฟรม | หนึ่งตัวต่อการเชื่อมต่อเคอร์เนลหนึ่งรายการ |
| การประมวลผลสตรีม | หนึ่งตัวต่อสตรีม (รองรับการเชื่อมต่อไคลเอนต์หลายรายการพร้อมกันได้) |
| การประมวลผลการเรียกไปข้างหน้า | หนึ่งตัวต่อการเรียกหนึ่งครั้ง |

**ในลูปอ่านห้ามรอการตอบกลับของการเรียกย้อนกลับแบบซิงโครนัส** (จะเดดล็อก): การประมวลผล `session.deliver` ต้องโยนไปเธรดแยก
เพราะภายในมันยังต้อง `session.settle` อีก (เป็นการเรียกย้อนกลับอีกครั้ง) ตัวอย่างก็ทำเช่นนี้

### 3.4 บริดจ์ความหมายเชิงความหมาย (ต้องยืนยันตัวตนก่อน)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` ข้ามไม่ได้: ระนาบการดำเนินการของเคอร์เนลของการเชื่อมต่อไม่มีตัวตนก่อนการยืนยันตัวตน เรียก `session.open` ตรง ๆ จะถูกปฏิเสธ
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`)

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

การนำส่งถูก**ดันกลับไปข้างหน้า**โดยเคอร์เนล (`method = "session.deliver"`) เมื่อประมวลผลแล้ว `session.settle`
(`ack` / `requeue` / `reject`; หมายเลขการนำส่งไม่ซ้ำทั้งระบบ ไม่มีหมายเลขสตรีม)

---

## 4. อ่านโค้ด (โปรเจกต์ตัวอย่าง)

`speedmq-plugin/java/SidecarPlugin.java` ประมาณ 470 บรรทัด (รวม JSON แบบมินิมอล):

| ตำแหน่ง | หน้าที่ |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | อ่าน-เขียนแบ่งเฟรม (`DataInputStream` + write lock) |
| `Conn.serve()` | ลูปอ่านเฟรมและการกระจาย |
| `Conn.call()` | การเรียกย้อนกลับ (ตาราง `pending` + คิวแบบบล็อก มีการป้องกัน timeout) |
| `Conn.handleHello()` | ตรวจสอบและตอบ HelloAck |
| `StreamState` | ฝั่งอ่านสตรีม (`BlockingQueue`, `STREAM_END` หมายถึงสิ้นสุด) |
| `Conn.handleForwardCall()` / `handleMethod()` | การเรียกไปข้างหน้า (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | ยืนยันตัวตน + ประกาศ + เผยแพร่ + บริโภค |
| `Json` (ท้ายไฟล์) | JSON แบบมินิมอลสำหรับอ่าน-เขียน เพียงเพื่อให้ตัวอย่างไม่มีการพึ่งพา |

> **คำแนะนำสำหรับโปรดักชัน**: เปลี่ยน `Json` เป็นไลบรารีที่คุณใช้ประจำ (Jackson / Gson) หรือใช้สแตกที่มีอยู่แล้วนอกเหนือจาก `java.net.http` ——
> ไม่เกี่ยวข้องกับสิ่งที่ตัวอย่างนี้ต้องการอธิบาย (โปรโตคอลสาย)

---

## 5. การทดสอบจริง (จำลองบนเครื่องนี้)

Windows + JDK 25; เคอร์เนลบน Docker (`speedmq:1.1.01`) ปลั๊กอินบนโฮสต์ (`tcp://host.docker.internal:19031`)

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

ครอบคลุม: **การจับมือ → การยืนยันตัวตน → บริดจ์ความหมายเชิงความหมาย → ดันการนำส่งกลับ → เคลียร์ → สะท้อนสตรีมไบต์**

---

## 6. ข้อควรระวังเฉพาะของ Java

- **`\uXXXX` ในซอร์สโค้ดจะถูกคอมไพเลอร์ประมวลผลทุกตำแหน่ง** (รวมถึงคอมเมนต์ด้วย!) คอมเมนต์ในตัวอย่างจงใจเขียนเป็น
  "NUL + ชื่อผู้ใช้ + NUL + รหัสผ่าน" แทนที่จะเขียน `\u0000` ตรง ๆ มิฉะนั้น javac จะรายงานอักขระผิดกฎ
- **ซอร์สโค้ดภาษาจีนต้อง `javac -encoding UTF-8`** มิฉะนั้นบนค่าเริ่มต้นของ Windows (GBK) จะรายงาน "อักขระที่แมปไม่ได้ของเอนโคดดิ้ง GBK"
  ขณะรันหากต้องการพิมพ์ภาษาจีนอย่างถูกต้อง ให้เพิ่ม `-Dfile.encoding=UTF-8`
- **ตัวแปรโลคัลที่ lambda จับต้องเป็น effectively final**: ในตัวอย่าง `name` ถูกกำหนดค่าใหม่ระหว่างแยกวิเคราะห์พารามิเตอร์
  ดังนั้นใน lambda จึงใช้ `opts.name` (ฟิลด์ที่กำหนดค่าเพียงครั้งเดียว)
- **`DataInputStream` เป็นแบบบล็อก**: เมื่อการเชื่อมต่อขาดจะโยน `EOFException`/`IOException` ให้ใช้จัดการปิดท้าย
- **base64**: `message.body`, `core.authenticate.response` ใน JSON เป็นสตริง base64
  (`Base64.getEncoder()/getDecoder()`)
- **ไลบรารีมาตรฐาน JDK ไม่มี JSON**: ตัวอย่างมี implement แบบมินิมอลในตัว; `Json.parse` ถอดจำนวนเต็มเป็น `Long` ทศนิยมเป็น `Double`
  เวลาดึง `id` ให้ใช้ `((Number) m.get("id")).longValue()`

---

## 7. ขั้นสูง

- แพ็กเกจเป็น jar ที่รันได้ (`Main-Class: SidecarPlugin`) หรือ `jlink` ย่อรันไทม์
  แล้วเปลี่ยน `spawn` เป็น `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`
- ปลั๊กอินมีหน้าจอจัดการในตัว: เพิ่ม `console_url` ในคอนฟิก (เอกสารหลัก §5.8) หน้า「การจัดการปลั๊กอิน」ของแบ็กเอนด์จัดการจะมีทางเข้าตรง
- ดีพลอยแยกส่วน: `spawn: []` + `address: "tcp://<ชื่อบริการ>:19031"` รับฟัง `0.0.0.0` ในคอนเทนเนอร์
