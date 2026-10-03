# คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก SwiftMQ —— Node.js

> **กลุ่มเป้าหมาย**: นักพัฒนาที่เขียนปลั๊กอินโปรเซสภายนอก (sidecar) ให้ SwiftMQ ด้วย Node.js
> **อ่านก่อน**: [คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก (sidecar)](plugin-development.md) (แบบจำลองทางความคิด / ฟิลด์คอนฟิก / ตารางสรุปโปรโตคอลสาย)
> **โปรเจกต์ตัวอย่าง**: เวิร์กสเปซ `swiftmq-plugin/nodejs/index.js` (เฉพาะไลบรารีมาตรฐานของ Node **ไม่ต้องมี dependency ของ npm**)

---

## 1. ตอนรันมันเป็นอย่างไร

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

สามประเด็นสำคัญ: **โปรเซสของคุณคือเซิร์ฟเวอร์** (รอเคอร์เนลมาเชื่อมต่อ); **พอร์ตภายนอกเปิดโดยเคอร์เนล** (คอนฟิก `protocols[].listeners`);
**`prefix` ต้องไม่ว่าง** (คำนำหน้าว่าง = ไม่มีส่วนในการดักจับ การเชื่อมต่อจะไม่ถูกส่งให้คุณ; ทดสอบจริงจะถูกตัดทันที ≤8 ไบต์ ASCII)

---

## 2. สามขั้นตอนให้รันขึ้น

### ขั้นที่หนึ่ง: คอนฟิก

`swiftmqd.json` (**คอนฟิกจริงเป็น JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**):

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

### ขั้นที่สอง: สตาร์ท

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### ขั้นที่สาม: ตรวจสอบ

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

ใน Node ใช้ `Buffer`: สะสมไบต์ที่ได้รับ พอครบหนึ่งเฟรมก็ตัดออกมาประมวลผล

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

หนึ่งสตรีม = หนึ่งการเชื่อมต่อไคลเอนต์; เพย์โหลดเฟรมข้อมูลคือ `4 ไบต์ big-endian หมายเลขสตรีม + ไบต์ดิบ`

### 3.2 การจับมือและฮาร์ตบีต

เคอร์เนล**ส่ง Hello ก่อน** คุณตอบ `HelloAck`; เคอร์เนลตรวจสอบ `name` กับ `api_version` (ปัจจุบัน `v1`) แล้วจึงเชื่อมต่อ
หลังจากนั้น `Ping` ทุก 2 วินาที ตอบ `Pong` ก็พอ (ลูปอ่านจัดการไปด้วย ไม่ต้องใช้ตัวตั้งเวลา)

### 3.3 แบบจำลองอะซิงโครนัส (ฉบับ Node)

อีเวนต์ลูปแบบเธรดเดียว หลีกเลี่ยงปัญหาการเขียนสลับกันได้โดยธรรมชาติ —— แต่ต้องระวัง**อย่าให้ลูปอ่าน await การเรียกย้อนกลับ**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` เป็น `async`: มันอาจ `await call('session.settle', …)` อีก จึงห้ามเขียนเป็นการรอแบบซิงโครนัสเด็ดขาด

### 3.4 บริดจ์ความหมายเชิงความหมาย (ต้องยืนยันตัวตนก่อน)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` ข้ามไม่ได้: ระนาบการดำเนินการของเคอร์เนลของการเชื่อมต่อไม่มีตัวตนก่อนการยืนยันตัวตน เรียก `session.open` ตรง ๆ จะถูกปฏิเสธ
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`)

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

การนำส่งถูก**ดันกลับไปข้างหน้า**โดยเคอร์เนล (`method = "session.deliver"`) เมื่อประมวลผลแล้ว `session.settle`
(`ack` / `requeue` / `reject`; หมายเลขการนำส่งไม่ซ้ำทั้งระบบ ไม่มีหมายเลขสตรีม)

---

## 4. อ่านโค้ด (โปรเจกต์ตัวอย่าง)

`swiftmq-plugin/nodejs/index.js` ประมาณ 330 บรรทัด:

| ตำแหน่ง | หน้าที่ |
| --- | --- |
| `u32()` / `Conn.send()` | อ่าน-เขียนแบ่งเฟรม |
| `Conn.drain()` / `dispatch()` | แยกวิเคราะห์และกระจายตามเฟรม |
| `Conn.call()` | การเรียกย้อนกลับ (`Promise` + ตาราง `pending` จับคู่ด้วย `reverse=true` กับ `id`) |
| `Stream` | ฝั่งอ่านสตรีม: `push/end/read` ประกอบเป็นคิวแบบอะซิงโครนัส |
| `handleHello` | ตรวจสอบและตอบ HelloAck |
| `handleForwardCall` / `handleMethod` | การเรียกไปข้างหน้า (`session.deliver` + settle, `stats`) |
| `sessionDemo` | ยืนยันตัวตน + ประกาศ + เผยแพร่ + บริโภค |

---

## 5. การทดสอบจริง (จำลองบนเครื่องนี้)

Windows + Node v24; เคอร์เนลบน Docker (`swiftmq:1.1.01`) ปลั๊กอินบนโฮสต์ (`tcp://host.docker.internal:19011`)

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

ครอบคลุม: **การจับมือ → การยืนยันตัวตน → บริดจ์ความหมายเชิงความหมาย → ดันการนำส่งกลับ → เคลียร์ → สะท้อนสตรีมไบต์**

---

## 6. ข้อควรระวังเฉพาะของ Node.js

- **`socket.write` เขียนหนึ่งเฟรมต่อครั้ง**: ตัวอย่างประกอบทั้งเฟรมเป็น `Buffer` เดียวแล้วค่อยเขียน จึงไม่ต้องล็อกเพิ่ม;
  หากคุณแยกหนึ่งเฟรมเป็นหลาย `write` ต้องรับประกันลำดับเอง
- **ขอบเขต chunk ของ `stream.on('data')` ไม่เกี่ยวข้องกับเฟรม**: ต้องสะสมบัฟเฟอร์เอง (ดู `drain()`)
- **base64**: `message.body`, `core.authenticate.response` ใน JSON เป็นสตริง base64
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`)
- **อย่า `await` ใน `drain()`**: มันเป็นฟังก์ชันตัดเฟรมแบบซิงโครนัส; มอบการประมวลผลแบบอะซิงโครนัสให้ `handleForwardCall`
- **ESM vs CJS**: ตัวอย่างใช้ CommonJS (`require`) เพื่อให้ `node index.js` รันได้ตรง ๆ; เปลี่ยนเป็น ESM เพียงเปลี่ยน `import`

---

## 7. ขั้นสูง

- ปลั๊กอินมีหน้าจอจัดการในตัว: เพิ่ม `console_url` ในคอนฟิก (เอกสารหลัก §5.8) หน้า「การจัดการปลั๊กอิน」ของแบ็กเอนด์จัดการจะมีทางเข้าตรง
- ดีพลอยแยกส่วน (K8s / systemd): `spawn: []` + `address: "tcp://<ชื่อบริการ>:19011"` รับฟัง `0.0.0.0` ในคอนเทนเนอร์
- ใช้พอร์ตร่วมกัน: หลายโปรโตคอลให้ `prefix` ต่างกัน เคอร์เนลกระจายการเชื่อมต่อไปยังปลั๊กอินแต่ละตัวตามคำนำหน้า
