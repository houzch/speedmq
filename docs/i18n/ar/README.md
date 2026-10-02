<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | **العربية** | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

برمجية وسيطة للرسائل **متوافقة مع RabbitMQ** مكتوبة بلغة Go. يمكن لعملاء RabbitMQ الحاليين الاتصال بها **دون تعديل الشيفرة ودون تبديل SDK**، بمجرد تغيير عنوان الاتصال.

## مقدمة

- **توافق البروتوكول**: AMQP 0-9-1 (مع امتدادات RabbitMQ) وMQTT 3.1.1؛ وخط أساس التوافق هو **دلالات RabbitMQ 4.3**.
- **نشر بسيط**: ملف تنفيذي واحد / حاوية واحدة، وواجهة الإدارة مدمجة، دون حاجة إلى Nginx أو قاعدة بيانات أو بيئة تشغيل Node إضافية.
- **كافٍ للعمليات**: واجهة الإدارة (الطوابير / المبادلات / الاتصالات / صلاحيات الحسابات / المضيفات الافتراضية / السياسات / الحدود / العنقود)، وPrometheus `/metrics`، وسطر أوامر `swiftmqctl`.
- **المنافذ الافتراضية**: `5672` (AMQP) و`1883` (MQTT) و`15672` (واجهة الإدارة / HTTP API / المقاييس).

القدرات المتوفرة حاليًا: الاستدامة (سجل المقاطع + مستويات fsync + الاستعادة بعد الانهيار)، وتأكيدات النشر، وTTL / الرسائل الميتة / حدود الطول، وأولوية المستهلكين، وDirect Reply-To، والعنقود (بيانات وصفية بنظام Raft + طوابير النصاب + إعادة التوجيه بين العقد)، والتشغيل/الإيقاف الساخن للإضافات.

***

## البدء السريع

### الطريقة 1: Docker (مُوصى بها)

**بدون استنساخ المستودع: اسحب الصورة وشغّلها مباشرة.**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.0
```

تُنشَر الصورة في مكانين بمحتوى متطابق (اختر الأسرع لديك): Docker Hub `houzch/swiftmq` وGitHub GHCR `ghcr.io/houzch/swiftmq`، وكلاهما يوفّر `linux/amd64` و`linux/arm64`.

- تُحفظ البيانات في الحجم المسمّى `swiftmq-data` وتبقى حتى بعد إعادة إنشاء الحاوية.
- الإيقاف / الحذف: `docker stop swiftmq` و`docker rm swiftmq` (يبقى حجم البيانات).

**لتعديل الإعدادات أو للاستخدام مع compose، استنسخ المستودع:**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # يستخدم الصورة المنشورة؛ غيّرها إلى up -d --build للبناء محليًا

docker compose ps        # يجب أن تكون الحالة Up (healthy)
docker compose logs -f   # متابعة السجلات
```

- يُربَط ملف الإعدادات `configs/swiftmqd.json` للقراءة فقط، وتُطبَّق التعديلات بعد `docker compose restart`.
- الإيقاف: `docker compose down` (يحتفظ بالبيانات)؛ `docker compose down -v` (يحذف البيانات أيضًا).

### الطريقة 2: ملف تنفيذي محلي (يتطلب Go 1.24+)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> ناتج بناء واجهة الإدارة لا يُودع في المستودع. إذا أردت استخدام الواجهة، نفِّذ أولًا `npm ci && npm run build` داخل `web/`؛
> ويمكنك التشغيل وإرسال الرسائل واستقبالها بشكل طبيعي دون بناء، لكن الوصول إلى `/` سيعرض «واجهة الإدارة غير مبنية».

### تسجيل الدخول الأول (احرص على تغيير الحساب الافتراضي أولًا)

| المدخل | العنوان / بيانات الاعتماد |
| --- | --- |
| واجهة الإدارة | <http://localhost:15672/> (اسم المستخدم `guest`، وكلمة المرور `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (نفس الحساب أعلاه) |

يحمل الحساب الرئيسي في التثبيت الجديد علامة «إلزام تغيير كلمة المرور عند أول تسجيل دخول»: بعد تسجيل الدخول إلى واجهة الإدارة **يُطلب إلزاميًا تعديل اسم الحساب وكلمة المرور معًا**، ولا يمكن الدخول إلى لوحة التحكم قبل ذلك.

ويمكن أيضًا إتمام ذلك مباشرة عبر API (مناسب للأتمتة):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ يتوافق `guest/guest` الافتراضي مع سلوك RabbitMQ: **يُسمح بتسجيل الدخول من الجهاز المحلي فقط**. يتطلب الاتصال من خارج الحاوية / عن بُعد تمكين `remote_access` لهذا المستخدم في الإعداد (وقد فعّلته الإعدادات النموذجية لسيناريو الحاوية).
> **بمجرد أن تصبح الخدمة متاحة للخارج، بادر فورًا إلى تغيير بيانات الاعتماد.**

### ربط تطبيقك (يكفي تغيير عنوان الاتصال)

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
# MQTT（عميل mosquitto）
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

واجهة HTTP الإدارية متوافقة مع `rabbitmqadmin`؛ و«إضافة طابور / مبادل» في واجهة الإدارة هو نفسه نقطة النهاية القياسية للإعلان، ويمكن للبرامج النصية فعل ذلك أيضًا:

```bash
# الإعلان عن طابور (يُعبَّر عن طابور النصاب عبر arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### العمليات اليومية

| البند | المدخل |
| --- | --- |
| واجهة الإدارة | <http://localhost:15672/>: الطوابير / المبادلات / الاتصالات / صلاحيات الحسابات / المضيفات الافتراضية / السياسات / الحدود / مفاتيح الميزات / العنقود، ويمكن ضبط التحديث التلقائي و**لغة الواجهة** من الزاوية العلوية اليمنى |
| مقاييس المراقبة | <http://localhost:15672/metrics> (نص Prometheus، يتطلب مصادقة)؛ انظر [docs/ops/monitoring](ops/monitoring/README.md) للوحات والتنبيهات |
| سطر الأوامر | `./bin/swiftmqctl status` و`list_queues` و`plugins list` و`plugins disable amqp091` (إيقاف ساخن، ويُغلق المنفذ فورًا) |
| فحص السلامة | `nc -z 127.0.0.1 15672` (مدمج في compose عبر healthcheck) |
| النسخ الاحتياطي والاستعادة | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| الترقية | [docs/ops/upgrade.md](ops/upgrade.md) |
| خط أساس الأمان | [docs/ops/security-baseline.md](ops/security-baseline.md) |

الإعدادات الشائعة (المثال الكامل في [configs/swiftmqd.json](../../../configs/swiftmqd.json)، ويمكن أيضًا تجاوزها بمتغيرات البيئة `SWIFTMQ_*`):

| عنصر الإعداد | الوصف | الافتراضي |
| --- | --- | --- |
| `data_dir` | دليل البيانات (الرسائل + البيانات الوصفية)، **احرص على استدامته** | `data` |
| `listeners` | عناوين الاستماع لكل بروتوكول، ويمكن ضبط TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | عنوان استماع واجهة الإدارة / API | `:15672` |
| `management.language` | اللغة الافتراضية لواجهة الإدارة؛ إن تُركت فارغة تُختار تلقائيًا حسب المنطقة الزمنية لمكان النشر | تلقائي |
| `storage.fsync` | مستوى الحفظ على القرص `none / os / batch / always` (ويحدد أيضًا توقيت confirm) | `os` |
| `storage.memory_high_watermark` و`storage.disk_free_limit` | عتبات الموارد: عند بلوغها يُحظر المنتِج، **دون فقدان الرسائل** | `0.4` / 50 MiB |
| `users` | جدول المستخدمين المدمجين (كلمة المرور + الوسوم + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | عنقود متعدد العقد (معطّل افتراضيًا)، وتغيير الأعضاء عبر `swiftmqctl add_member` | معطّل |

> قد يكون المنفذ مُستخدَمًا: يكفي تغييره إلى منفذ آخر عبر `listeners` / `management.addr`.

***

## بنية المشروع

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # مدخل عملية broker (وهو ما يجب تشغيله)
│   └── swiftmqctl/      # سطر أوامر العمليات (يمر عبر واجهة HTTP الإدارية، ومستقل عن نسخة النواة)
├── internal/            # تنفيذ النواة
│   ├── protocol/        # إضافات البروتوكول: amqp091 وmqtt (الترميز/فك الترميز / الطرق / الجلسات)
│   ├── broker/          # النواة: vhost، المبادلات، الطوابير، الرسائل الميتة، التحكم في التدفق، عرض المستوى الإداري
│   ├── store/           # الاستدامة: سجل المقاطع، فهرس الطوابير، الاستعادة بعد الانهيار
│   ├── raft/ meta/      # العنقود: Raft مطوّر ذاتيًا ونسخ البيانات الوصفية
│   ├── management/      # واجهة HTTP الإدارية + مقاييس Prometheus + خدمة ثابتة لواجهة مدمجة
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # العقود المستقرة للخارج: واجهة برمجة الإضافات (plugin) وبروتوكول سلك الإضافات الخارجية (sidecar)
├── web/                 # مشروع الواجهة الأمامية لواجهة الإدارة (Vue 3 + Vite)، ويُدمج ناتجه في الملف التنفيذي عبر go:embed عند البناء
├── configs/             # إعدادات نموذجية
├── docs/ops/            # مستندات العمليات: النسخ الاحتياطي والاستعادة / الترقية / خط أساس الأمان / المراقبة
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（رمز QR لمجموعة الدردشة）
```

***

## المساهمة

نرحّب بإرسال Issue وPull Request. أساس هذا المشروع هو **توافق البروتوكول**، ولذلك:

- عند إصلاح خطأ، اذكر سلوك RabbitMQ المقابل (الإصدار، العميل، خطوات إعادة الإنتاج)؛
- عند إجراء تغييرات تمس تفاصيل البروتوكول، أرفق نتائج المقارنة مع RabbitMQ؛
- قبل الإرسال تأكد من نجاح `go build ./...` و`go vet ./...` و`go test ./...` و`gofmt -l .`.

***

## الرخصة

يستخدم هذا المشروع [رخصة Apache 2.0](../../../LICENSE).

يُسمح بالاستخدام والتعديل والتوزيع (بما في ذلك الاستخدام التجاري)، مع وجوب الاحتفاظ بإشعار حقوق النشر والرخصة، ودون أي ضمانات.

Copyright 2026 houzch (انظر [NOTICE](../../../NOTICE))

***

## شكر وتقدير

تُعدّ مواصفة بروتوكول AMQP 0-9-1 والدلالات السلوكية لـ [RabbitMQ](https://www.rabbitmq.com/) مرجع المقارنة لعمل التوافق في هذا المشروع. هذا المشروع تنفيذ مستقل، ولا تربطه علاقة تبعية بمشروع RabbitMQ الرسمي، ولم يستخدم شيفرته.

***

## الانضمام إلى مجموعة الدردشة

امسح رمز QR للانضمام إلى مجموعة دردشة SwiftMQ، ويمكنك طرح أسئلتك مباشرة في المجموعة:

![مجموعة دردشة SwiftMQ](../../../1280X1280.PNG)
