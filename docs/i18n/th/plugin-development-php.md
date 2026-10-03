# คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก SwiftMQ —— PHP

> **กลุ่มเป้าหมาย**: นักพัฒนาที่เขียนปลั๊กอินโปรเซสภายนอก (sidecar) ให้ SwiftMQ ด้วย PHP
> **อ่านก่อน**: [คู่มือการพัฒนา ปลั๊กอินโปรเซสภายนอก (sidecar)](plugin-development.md) (แบบจำลองทางความคิด / ฟิลด์คอนฟิก / ตารางสรุปโปรโตคอลสาย)
> **โปรเจกต์ตัวอย่าง**: เวิร์กสเปซ `swiftmq-plugin/php/sidecar_plugin.php` (เฉพาะไลบรารีมาตรฐาน **ไม่ต้องมี dependency ของ composer**)

---

## 1. ตอนรันมันเป็นอย่างไร

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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

`swiftmqd.json` (**คอนฟิกจริงเป็น JSON มาตรฐาน ใส่คอมเมนต์ไม่ได้**):

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/swiftmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### ขั้นที่สอง: สตาร์ท

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### ขั้นที่สาม: ตรวจสอบ

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP ใช้ `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

เพย์โหลดเฟรมข้อมูล = `pack('N', $streamId) . ไบต์ดิบ`

### 3.2 การจับมือและฮาร์ตบีต

เคอร์เนล**ส่ง Hello ก่อน** คุณตอบ `HelloAck`; เคอร์เนลตรวจสอบ `name` กับ `api_version` (ปัจจุบัน `v1`)
หลังจากนั้น `Ping` ทุก 2 วินาที ตอบ `Pong`

### 3.3 แบบจำลองการทำงานพร้อมกัน: ปั๊มเฟรมแบบ reentrant (PHP ไม่มีเธรด)

PHP CLI เป็นแบบเธรดเดียวบล็อก ดังนั้นที่นี่ไม่ใช้ "หนึ่งเธรดต่อหนึ่งสตรีม" แต่เป็น:

- **ลูปอ่าน** (`serve()`) รับผิดชอบการจับมือ ฮาร์ตบีต การเปิดสตรีม การสะท้อนข้อมูล การประมวลผลการเรียกไปข้างหน้า;
- **การสะท้อนกลับ**ไม่ต้องมีสเตตแมชชีนเพิ่ม: เมื่อได้รับ `kindData` ก็เขียนกลับเป็น `kindData` ตามเดิมทันที;
- **การเรียกย้อนกลับ**ใช้ `callAndWait()`: หลังส่ง `kindCall` ให้อ่านเฟรมและกระจายไปด้วย
  จนกว่าจะอ่านเจอ**การตอบกลับของตัวเอง** (`reverse=true` และ `id` ตรงกัน) จึงคืนค่า

```php
public function callAndWait(string $method, $params = null)
{
    $id = ++$this->nextId;
    $this->sendJson(KIND_CALL, ['id' => $id, 'method' => $method, 'reverse' => true, 'params' => $params]);
    for (;;) {
        [$kind, $payload] = $this->readFrame();
        if ($kind === KIND_REPLY) {
            $reply = json_decode($payload, true);
            if (!empty($reply['reverse']) && $reply['id'] === $id) {
                if (empty($reply['ok'])) throw new RuntimeException($reply['error'] ?? '调用失败');
                return $reply['data'] ?? null;
            }
            continue;
        }
        $this->dispatchOther($kind, $payload);   // 心跳/数据/正向调用照常处理
    }
}
```

นั่นหมายความว่า `dispatchOther()` **ต้องเป็นแบบ reentrant**: มันอาจถูกเรียกซ้ำภายใน `callAndWait` หนึ่งครั้ง
(เช่นตอนประมวลผล `session.deliver` แล้วต้อง `session.settle` อีก) ตัวอย่างก็ทำเช่นนี้

### 3.4 บริดจ์ความหมายเชิงความหมาย (ต้องยืนยันตัวตนก่อน)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` ข้ามไม่ได้: ระนาบการดำเนินการของเคอร์เนลของการเชื่อมต่อไม่มีตัวตนก่อนการยืนยันตัวตน เรียก `session.open` ตรง ๆ จะถูกปฏิเสธ
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`)

```php
$ident = $this->callAndWait('core.authenticate', [
    'stream'    => $streamId,
    'mechanism' => 'PLAIN',
    // SASL PLAIN 响应：\x00<user>\x00<password>，字节在 JSON 里走 base64
    'response'  => base64_encode("\x00{$user}\x00{$password}"),
]);
$this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
$q = $this->callAndWait('session.declare_queue', ['stream' => $streamId, 'exclusive' => true, 'auto_delete' => true]);
$this->callAndWait('session.consume', ['stream' => $streamId, 'queue' => $q['name'], 'prefetch' => 32]);
```

การนำส่งถูก**ดันกลับไปข้างหน้า**โดยเคอร์เนล (`method = "session.deliver"`) เมื่อประมวลผลแล้ว `session.settle`
(`ack` / `requeue` / `reject`; หมายเลขการนำส่งไม่ซ้ำทั้งระบบ ไม่มีหมายเลขสตรีม)

---

## 4. อ่านโค้ด (โปรเจกต์ตัวอย่าง)

`swiftmq-plugin/php/sidecar_plugin.php` ประมาณ 320 บรรทัด:

| ตำแหน่ง | หน้าที่ |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | อ่าน-เขียนแบ่งเฟรม |
| `Conn::serve()` | ลูปอ่านหลัก |
| `Conn::dispatchOther()` | กระจายเฟรมที่ไม่ใช่การจับมือ (reentrant) |
| `Conn::callAndWait()` | การเรียกย้อนกลับ (ปั๊มเฟรมแบบ reentrant) |
| `Conn::handleHello()` | ตรวจสอบและตอบ HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | การเรียกไปข้างหน้า (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | ยืนยันตัวตน + ประกาศ + เผยแพร่ + บริโภค |

---

## 5. การทดสอบจริง (จำลองบนเครื่องนี้)

Windows + PHP 7.4; เคอร์เนลบน Docker (`swiftmq:1.1.01`) ปลั๊กอินบนโฮสต์ (`tcp://host.docker.internal:19021`)

```
php -l sidecar_plugin.php  → No syntax errors detected
plugin=php-sidecar state=enabled          # /api/plugins
echo=[PHhello]                            # 客户端连内核端口 19022 发 "PHhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=php-sidecar addr=0.0.0.0:19021 version=0.1.0
内核已接入 plugin=php-sidecar peer=127.0.0.1:51006
握手完成 plugin=php-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-955afebb58d6b5307ba36e
流已打开 plugin=php-sidecar stream=1 remote=172.17.0.1:59664 local=172.17.0.2:19022
收到投递（session.deliver） queue=amq.gen-955afebb58d6b5307ba36e delivery_id=1 body=hello from php sidecar
```

ครอบคลุม: **การจับมือ → การยืนยันตัวตน → บริดจ์ความหมายเชิงความหมาย → ดันการนำส่งกลับ → เคลียร์ → สะท้อนสตรีมไบต์**

---

## 6. ข้อควรระวังเฉพาะของ PHP

- **PHP 7.4 ไม่มี return type `mixed`** (มีใน PHP 8.0): ในตัวอย่างการเรียกย้อนกลับคืนค่า "ชนิดใดก็ได้"
  จึง**ไม่เขียนประกาศ return type** (ใช้คอมเมนต์ `@return mixed`) การเขียน `: mixed` บน 7.4 จะเป็น syntax error ทันที
- **ชนิดตัวเลขของ JSON**: `json_decode($s, true)` โดยค่าเริ่มต้นถอดจำนวนเต็มเป็น `int` จำนวนเต็มขนาดใหญ่อาจกลายเป็น `float`;
  หมายเลขการนำส่งในขนาดของตัวอย่างนี้ไม่มีปัญหา หากหมายเลขของคุณใหญ่มาก ให้พิจารณา `JSON_BIGINT_AS_STRING`
- **base64 จำเป็น**: `message.body` กับ `core.authenticate.response` ใน JSON เป็นสตริง base64
  (`base64_encode` / `base64_decode($s, true)`)
- **อย่าใช้ `pcntl_fork` ทำ concurrency**: Windows ไม่มี pcntl และหลัง fork จะทำลายสมมติฐาน "หนึ่งการเชื่อมต่อหนึ่งผู้เขียน";
  เธรดเดียว + ปั๊มเฟรมแบบ reentrant ก็เพียงพอแล้ว (เว้นแต่คุณต้องคำนวณหนักมากบนสตรีม ซึ่งเหมาะวางไว้ในบริการภายนอก)
- **`stream_socket_accept` เป็นแบบบล็อก**: วงจรชีวิตโปรเซสจัดการโดยเคอร์เนล (`spawn`) หรือ supervisor;
  อย่าลืมจัดการเมื่อ `fread` คืน `''` (EOF) → จบการเชื่อมต่อนั้นและกลับไป accept
- **บัฟเฟอร์เอาต์พุต**: ล็อกใช้ `fwrite(STDOUT, …)` แล้วตามด้วยขึ้นบรรทัดใหม่ สะดวกต่อการให้เคอร์เนลส่งต่อไปยังล็อกเคอร์เนลแบบรายบรรทัด

---

## 7. ขั้นสูง

- ปลั๊กอินมีหน้าจอจัดการในตัว: เพิ่ม `console_url` ในคอนฟิก (เอกสารหลัก §5.8) หน้า「การจัดการปลั๊กอิน」ของแบ็กเอนด์จัดการจะมีทางเข้าตรง
- ดีพลอยแยกส่วน: `spawn: []` + `address: "tcp://<ชื่อบริการ>:19021"` รับฟัง `0.0.0.0` ในคอนเทนเนอร์
- เมื่อต้องการ concurrency สูงขึ้น อาจเปลี่ยนปลั๊กอินเป็นแบบ daemon อย่าง Swoole / RoadRunner แต่**โปรโตคอลสายไม่เปลี่ยน** เพียงต้องรับประกันว่า:
  การเขียนเฟรมเป็นลำดับ ลูปอ่านไม่บล็อก การเรียกย้อนกลับจับคู่ด้วย `id`+`reverse`
