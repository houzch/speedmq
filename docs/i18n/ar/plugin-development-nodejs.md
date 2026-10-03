# دليل تطوير المكوّن الإضافي كعملية خارجية في SwiftMQ —— Node.js

> **الجمهور**: المطوّرون الذين يكتبون مكوّنًا إضافيًا كعملية خارجية (sidecar) لـSwiftMQ بلغة Node.js.
> **اقرأ أولًا**: [دليل تطوير المكوّن الإضافي كعملية خارجية (sidecar)](plugin-development.md) (النموذج الذهني / حقول الإعداد / الجدول الكامل لبروتوكول السلك).
> **المشروع النموذجي**: في مساحة العمل `swiftmq-plugin/nodejs/index.js` (مكتبة Node القياسية فقط، **بلا حاجة إلى اعتماديات npm**).

---

## 1. كيف يبدو عند تشغيله

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

ثلاث نقاط أساسية: **عمليتك هي الخادم** (تنتظر اتصال النواة)؛ **المنفذ الخارجي تفتحه النواة** (إعداد `protocols[].listeners`)؛
و**يجب أن يكون `prefix` غير فارغ** (البادئة الفارغة = عدم المشاركة في الاستكشاف، ولن يُسلَّم إليك الاتصال؛ وعمليًا يُقطع فورًا، ≤8 بايتات ASCII).

---

## 2. التشغيل في ثلاث خطوات

### الخطوة الأولى: الإعداد

`swiftmqd.json` (**الإعداد الفعلي هو JSON قياسي، ولا يقبل التعليقات**):

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

### الخطوة الثانية: التشغيل

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### الخطوة الثالثة: التحقّق

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. نقاط التنفيذ

### 3.1 تأطير الرسائل

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

في Node نستخدم `Buffer`: نراكم البايتات الواردة، وعند اكتمال إطار نقطعه للمعالجة.

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

التدفّق الواحد = اتصال عميل واحد؛ وحمل إطار البيانات هو `4 بايتات بترتيب كبير لرقم التدفّق + بايتات خام`.

### 3.2 المصافحة ونبضات القلب

ترسل النواة **`Hello` أولًا**، وتردّ أنت بـ`HelloAck`؛ وتتحقّق النواة من `name` و`api_version` (حاليًا `v1`) ثم تتصل.
بعد ذلك يأتي `Ping` كل 2s، ويكفي الردّ بـ`Pong` (تتولّاه حلقة القراءة تلقائيًا، دون حاجة إلى مؤقّت).

### 3.3 النموذج غير المتزامن (نسخة Node)

حلقة أحداث أحادية الخيط، تتجنّب بطبيعتها مشكلة "تداخل الكتابة" —— لكن انتبه إلى **ألا تجعل حلقة القراءة تنتظر (await) الاستدعاء العكسي**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` دالة `async`: فقد تستدعي مجددًا `await call('session.settle', …)`، ولذلك لا يجوز إطلاقًا كتابتها كانتظار متزامن.

### 3.4 جسر الدلالات (يجب المصادقة أولًا)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

لا يمكن إغفال `core.authenticate`: فلا هوية لسطح عمليات النواة للاتصال قبل المصادقة، وسيُرفض `session.open` مباشرةً
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

تُعاد التسليمات من النواة **باستدعاء أمامي** (`method = "session.deliver"`)، وبعد المعالجة `session.settle`
(`ack` / `requeue` / `reject`؛ ورقم التسليم فريد عالميًا، دون رقم تدفّق).

---

## 4. قراءة الشيفرة (المشروع النموذجي)

يبلغ `swiftmq-plugin/nodejs/index.js` نحو 330 سطرًا:

| الموضع | الوظيفة |
| --- | --- |
| `u32()` / `Conn.send()` | قراءة وكتابة الإطارات |
| `Conn.drain()` / `dispatch()` | التحليل والتوزيع حسب الإطار |
| `Conn.call()` | الاستدعاء العكسي (`Promise` + جدول `pending`، بالمطابقة عبر `reverse=true` و`id`) |
| `Stream` | جهة قراءة التدفّق: `push/end/read` تشكّل طابورًا غير متزامن |
| `handleHello` | التحقّق والردّ بـHelloAck |
| `handleForwardCall` / `handleMethod` | الاستدعاء الأمامي (`session.deliver` + settle، و`stats`) |
| `sessionDemo` | المصادقة + الإعلان + النشر + الاستهلاك |

---

## 5. اختبار فعلي (إعادة الإنتاج محليًا)

Windows + Node v24؛ والنواة في Docker (`swiftmq:1.1.01`)، والمكوّن الإضافي على المضيف (`tcp://host.docker.internal:19011`).

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

المُغطّى: **المصافحة → المصادقة → جسر الدلالات → إعادة دفع التسليم → التسوية → صدى تدفّق البايتات**.

---

## 6. ملاحظات خاصة بلغة Node.js

- **`socket.write` يكتب إطارًا واحدًا في المرة**: يجمع المثال الإطار كاملًا في `Buffer` واحد ثم يكتبه، فلا حاجة إلى قفل إضافي؛
  وإن قسّمت الإطار إلى عدة عمليات `write` فعليك ضمان الترتيب بنفسك.
- **حدود chunk في `stream.on('data')` لا علاقة لها بالإطارات**: يجب أن تتراكم المخزن بنفسك (انظر `drain()`).
- **base64**: كلٌّ من `message.body` و`core.authenticate.response` سلسلة base64 داخل JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **لا تستخدم `await` داخل `drain()`**: فهي دالة تقطيع إطارات متزامنة؛ وسلّم المعالجة غير المتزامنة إلى `handleForwardCall`.
- **ESM مقابل CJS**: يستخدم المثال CommonJS (`require`) ليُشغَّل `node index.js` مباشرةً؛ وللتحويل إلى ESM يكفي تبديل `import`.

---

## 7. متقدّم

- للمكوّن الإضافي واجهة إدارة خاصة به: أضف `console_url` في الإعداد (المستند الرئيسي §5.8)، وستظهر مدخل مباشر في صفحة «إدارة المكوّنات الإضافية» بلوحة الإدارة.
- النشر المستقل (K8s / systemd): `spawn: []` + `address: "tcp://<اسم الخدمة>:19011"`، مع الاستماع داخل الحاوية إلى `0.0.0.0`.
- إعادة استخدام المنافذ: أعطِ كل بروتوكول `prefix` مختلفة، وتوزّع النواة الاتصالات إلى المكوّنات الإضافية حسب البادئة.
