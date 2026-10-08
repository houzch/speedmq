# دليل تطوير المكوّن الإضافي كعملية خارجية في SpeedMQ —— PHP

> **الجمهور**: المطوّرون الذين يكتبون مكوّنًا إضافيًا كعملية خارجية (sidecar) لـSpeedMQ بلغة PHP.
> **اقرأ أولًا**: [دليل تطوير المكوّن الإضافي كعملية خارجية (sidecar)](plugin-development.md) (النموذج الذهني / حقول الإعداد / الجدول الكامل لبروتوكول السلك).
> **المشروع النموذجي**: في مساحة العمل `speedmq-plugin/php/sidecar_plugin.php` (المكتبة القياسية فقط، **بلا حاجة إلى اعتماديات composer**).

---

## 1. كيف يبدو عند تشغيله

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

ثلاث نقاط أساسية: **عمليتك هي الخادم** (تنتظر اتصال النواة)؛ **المنفذ الخارجي تفتحه النواة** (`protocols[].listeners`)؛
و**يجب أن يكون `prefix` غير فارغ** (البادئة الفارغة = عدم المشاركة في الاستكشاف، ولن يُسلَّم إليك الاتصال؛ وعمليًا يُقطع فورًا، ≤8 بايتات ASCII).

---

## 2. التشغيل في ثلاث خطوات

### الخطوة الأولى: الإعداد

`speedmqd.json` (**الإعداد الفعلي هو JSON قياسي، ولا يقبل التعليقات**):

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/speedmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### الخطوة الثانية: التشغيل

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### الخطوة الثالثة: التحقّق

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

يستخدم PHP `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

حمل إطار البيانات = `pack('N', $streamId) . بايتات خام`.

### 3.2 المصافحة ونبضات القلب

ترسل النواة **`Hello` أولًا**، وتردّ أنت بـ`HelloAck`؛ وتتحقّق النواة من `name` و`api_version` (حاليًا `v1`).
بعد ذلك يأتي `Ping` كل 2s، فردّ بـ`Pong`.

### 3.3 نموذج التزامن: مضخّة إطارات قابلة لإعادة الدخول (لا خيوط في PHP)

PHP CLI أحادي الخيط وحاجب، ولذلك لا نستخدم هنا "خيطًا لكل تدفّق"، بل:

- **حلقة القراءة** (`serve()`) تتولّى المصافحة، ونبضات القلب، وفتح التدفّقات، وصدى البيانات، ومعالجة الاستدعاءات الأمامية؛
- **الصدى** لا يحتاج آلة حالة إضافية: فبمجرد وصول `kindData` يُكتب كما هو فورًا مرة أخرى إلى `kindData`؛
- **الاستدعاء العكسي** يستخدم `callAndWait()`: بعد إرسال `kindCall`، يقرأ الإطارات ويوزّعها في الوقت نفسه،
  ولا يعود إلا عند قراءة **ردّه هو** (`reverse=true` مع مطابقة `id`).

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

وهذا يعني أن `dispatchOther()` **يجب أن يكون قابلًا لإعادة الدخول**: فقد يُستدعى مجددًا داخل استدعاء `callAndWait` واحد
(مثلًا عند معالجة `session.deliver` ثم الحاجة إلى `session.settle`). وهكذا يفعل المثال.

### 3.4 جسر الدلالات (يجب المصادقة أولًا)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

لا يمكن إغفال `core.authenticate`: فلا هوية لسطح عمليات النواة للاتصال قبل المصادقة، وسيُرفض `session.open` مباشرةً
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

تُعاد التسليمات من النواة **باستدعاء أمامي** (`method = "session.deliver"`)، وبعد المعالجة `session.settle`
(`ack` / `requeue` / `reject`؛ ورقم التسليم فريد عالميًا، دون رقم تدفّق).

---

## 4. قراءة الشيفرة (المشروع النموذجي)

يبلغ `speedmq-plugin/php/sidecar_plugin.php` نحو 320 سطرًا:

| الموضع | الوظيفة |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | قراءة وكتابة الإطارات |
| `Conn::serve()` | حلقة القراءة الرئيسية |
| `Conn::dispatchOther()` | توزيع الإطارات بخلاف المصافحة (قابل لإعادة الدخول) |
| `Conn::callAndWait()` | الاستدعاء العكسي (مضخّة إطارات قابلة لإعادة الدخول) |
| `Conn::handleHello()` | التحقّق والردّ بـHelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | الاستدعاء الأمامي (`session.deliver` + settle، و`stats`) |
| `Conn::sessionDemo()` | المصادقة + الإعلان + النشر + الاستهلاك |

---

## 5. اختبار فعلي (إعادة الإنتاج محليًا)

Windows + PHP 7.4؛ والنواة في Docker (`speedmq:1.1.01`)، والمكوّن الإضافي على المضيف (`tcp://host.docker.internal:19021`).

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

المُغطّى: **المصافحة → المصادقة → جسر الدلالات → إعادة دفع التسليم → التسوية → صدى تدفّق البايتات**.

---

## 6. ملاحظات خاصة بلغة PHP

- **لا يوجد في PHP 7.4 نوع إرجاع `mixed`** (موجود في PHP 8.0 فقط): فالاستدعاء العكسي في المثال يعيد "أي نوع"،
  ولذلك **لا نكتب تصريحًا بنوع الإرجاع** (بل تعليق `@return mixed`). وكتابة `: mixed` على 7.4 خطأ نحوي مباشر.
- **الأنواع الرقمية في JSON**: يفكّ `json_decode($s, true)` الأعداد الصحيحة افتراضيًا كـ`int`، وقد يتحوّل العدد الصحيح الكبير إلى `float`؛
  ورقم التسليم في نطاق هذا المثال لا مشكلة فيه، وإن كانت أرقامك كبيرة ففكّر في `JSON_BIGINT_AS_STRING`.
- **base64 ضرورية**: كلٌّ من `message.body` و`core.authenticate.response` سلسلة base64 داخل JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **لا تستخدم `pcntl_fork` للتزامن**: فلا يوجد pcntl على Windows، كما أن fork يُفسد افتراض "كاتب واحد لاتصال واحد"؛
  ويكفي خيط واحد + مضخّة إطارات قابلة لإعادة الدخول (إلا إذا كنت تريد حسابات ثقيلة على التدفّق، فتلك أنسب لخدمة خارجية).
- **`stream_socket_accept` حاجبة**: ودورة حياة العملية يديرها النواة (`spawn`) أو supervisor؛
  وتذكّر معالجة إرجاع `fread` للقيمة `''` (EOF) → إنهاء ذلك الاتصال والعودة إلى accept.
- **مخزن الإخراج**: استخدم للسجلات `fwrite(STDOUT, …)` متبوعًا بسطر جديد، ليُمرَّر سطرًا بسطر إلى سجل النواة.

---

## 7. متقدّم

- للمكوّن الإضافي واجهة إدارة خاصة به: أضف `console_url` في الإعداد (المستند الرئيسي §5.8)، وستظهر مدخل مباشر في صفحة «إدارة المكوّنات الإضافية» بلوحة الإدارة.
- النشر المستقل: `spawn: []` + `address: "tcp://<اسم الخدمة>:19021"`، مع الاستماع داخل الحاوية إلى `0.0.0.0`.
- عند الحاجة إلى تزامن أعلى، يمكن تحويل المكوّن الإضافي إلى خدمة دائمة مثل Swoole / RoadRunner، لكن **بروتوكول السلك لا يتغيّر**، ويجب فقط ضمان:
  تسلسل كتابة الإطارات، وعدم حجب حلقة القراءة، ومطابقة الاستدعاء العكسي عبر `id`+`reverse`.
