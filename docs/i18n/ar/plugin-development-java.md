# دليل تطوير المكوّن الإضافي كعملية خارجية في SwiftMQ —— Java

> **الجمهور**: المطوّرون الذين يكتبون مكوّنًا إضافيًا كعملية خارجية (sidecar) لـSwiftMQ بلغة Java.
> **اقرأ أولًا**: [دليل تطوير المكوّن الإضافي كعملية خارجية (sidecar)](plugin-development.md) (النموذج الذهني / حقول الإعداد / الجدول الكامل لبروتوكول السلك).
> **المشروع النموذجي**: في مساحة العمل `swiftmq-plugin/java/SidecarPlugin.java` (ملف واحد، مكتبة JDK القياسية فقط، بلا حاجة إلى Maven/Gradle).

---

## 1. كيف يبدو عند تشغيله

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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

`swiftmqd.json` (**الإعداد الفعلي هو JSON قياسي، ولا يقبل التعليقات**):

```json
{
  "plugins": {
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/swiftmq/classes", "SidecarPlugin",
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

### الخطوة الثانية: الترجمة والتشغيل

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### الخطوة الثالثة: التحقّق

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

في Java تعدّ `DataInputStream`/`DataOutputStream` الأبسط —— فـ`readInt`/`writeInt` فيهما هما **big-endian (ترتيب البايتات الكبير)**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

ويجب أن تكون جهة الكتابة **متسلسلة** (فنبضات القلب والردود وكتل البيانات تأتي من خيوط مختلفة):

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

حمل إطار البيانات = `4 بايتات بترتيب كبير لرقم التدفّق + بايتات خام`.

### 3.2 المصافحة ونبضات القلب

ترسل النواة **`Hello` أولًا**، وتردّ أنت بـ`HelloAck`؛ وتتحقّق النواة من `name` و`api_version` (حاليًا `v1`) ثم تتصل.
بعد ذلك يأتي `Ping` كل 2s، فردّ بـ`Pong`.

### 3.3 نموذج التزامن (نسخة Java)

| الدور | الخيط |
| --- | --- |
| حلقة قراءة الإطارات | واحد لكل اتصال نواة |
| معالجة التدفّق | واحد لكل تدفّق (ويمكن تزامن عدة اتصالات عملاء) |
| معالجة الاستدعاء الأمامي | واحد لكل استدعاء |

**لا يجوز انتظار ردّ الاستدعاء العكسي بشكل متزامن داخل حلقة القراءة** (سيحدث جمود): فعملية `session.deliver`
يجب إرسالها إلى خيط مستقل، لأنها تحتاج داخليًا إلى `session.settle` (وهو استدعاء عكسي آخر). وهكذا يفعل المثال.

### 3.4 جسر الدلالات (يجب المصادقة أولًا)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

لا يمكن إغفال `core.authenticate`: فلا هوية لسطح عمليات النواة للاتصال قبل المصادقة، وسيُرفض `session.open` مباشرةً
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

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

تُعاد التسليمات من النواة **باستدعاء أمامي** (`method = "session.deliver"`)، وبعد المعالجة `session.settle`
(`ack` / `requeue` / `reject`؛ ورقم التسليم فريد عالميًا، دون رقم تدفّق).

---

## 4. قراءة الشيفرة (المشروع النموذجي)

يبلغ `swiftmq-plugin/java/SidecarPlugin.java` نحو 470 سطرًا (مع JSON مبسّط جدًا):

| الموضع | الوظيفة |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | قراءة وكتابة الإطارات (`DataInputStream` + قفل الكتابة) |
| `Conn.serve()` | حلقة قراءة الإطارات والتوزيع |
| `Conn.call()` | الاستدعاء العكسي (جدول `pending` + طابور حاجب، مع حماية بمهلة) |
| `Conn.handleHello()` | التحقّق والردّ بـHelloAck |
| `StreamState` | جهة قراءة التدفّق (`BlockingQueue`، و`STREAM_END` تعني الانتهاء) |
| `Conn.handleForwardCall()` / `handleMethod()` | الاستدعاء الأمامي (`session.deliver` + settle، و`stats`) |
| `Conn.sessionDemo()` | المصادقة + الإعلان + النشر + الاستهلاك |
| `Json` (نهاية الملف) | قراءة/كتابة JSON مبسّطة، فقط لجعل المثال بلا اعتماديات |

> **توصية للإنتاج**: استبدل `Json` بالمكتبة التي اعتدتها (Jackson / Gson)، أو بأي حزمة قائمة بخلاف `java.net.http` ——
> فالأمر لا علاقة له بما يريد هذا المثال شرحه (بروتوكول السلك).

---

## 5. اختبار فعلي (إعادة الإنتاج محليًا)

Windows + JDK 25؛ والنواة في Docker (`swiftmq:1.1.01`)، والمكوّن الإضافي على المضيف (`tcp://host.docker.internal:19031`).

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

المُغطّى: **المصافحة → المصادقة → جسر الدلالات → إعادة دفع التسليم → التسوية → صدى تدفّق البايتات**.

---

## 6. ملاحظات خاصة بلغة Java

- **يُعالج المترجم `\uXXXX` في الشيفرة المصدرية في أي موضع** (حتى داخل التعليقات!). وقد كُتب التعليق في المثال عن قصد
  "NUL + اسم المستخدم + NUL + كلمة المرور" بدل كتابة `\u0000` مباشرةً، وإلا أبلغ javac عن محرف غير صالح.
- **يجب استخدام `javac -encoding UTF-8` للشيفرة المصدرية الصينية**، وإلا أبلغ على Windows (GBK افتراضيًا) عن "محرف غير قابل للتعيين بالترميز GBK".
  وإن أردت طباعة الصينية بشكل صحيح وقت التشغيل، فأضف `-Dfile.encoding=UTF-8`.
- **المتغيّرات المحلية الملتقَطة في lambda يجب أن تكون effectively final**: ففي المثال يُعاد إسناد `name` أثناء تحليل المعاملات،
  ولذلك يُستخدم داخل lambda المتغيّر `opts.name` (حقل يُسند مرة واحدة فقط).
- **`DataInputStream` حاجب**: وعند انقطاع الاتصال يرمي `EOFException`/`IOException`، وعليه تُنهي المعالجة.
- **base64**: كلٌّ من `message.body` و`core.authenticate.response` سلسلة base64 داخل JSON
  (`Base64.getEncoder()/getDecoder()`).
- **لا JSON في مكتبة JDK القياسية**: فالمثال يأتي بتنفيذ مبسّط خاص به؛ ويفكّ `Json.parse` الأعداد الصحيحة كـ`Long` والعشرية كـ`Double`،
  وعند أخذ `id` يُستخدم `((Number) m.get("id")).longValue()`.

---

## 7. متقدّم

- التغليف في jar قابل للتنفيذ (`Main-Class: SidecarPlugin`) أو تقليص بيئة التشغيل بـ`jlink`،
  ثم تغيير `spawn` إلى `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`.
- للمكوّن الإضافي واجهة إدارة خاصة به: أضف `console_url` في الإعداد (المستند الرئيسي §5.8)، وستظهر مدخل مباشر في صفحة «إدارة المكوّنات الإضافية» بلوحة الإدارة.
- النشر المستقل: `spawn: []` + `address: "tcp://<اسم الخدمة>:19031"`، مع الاستماع داخل الحاوية إلى `0.0.0.0`.
