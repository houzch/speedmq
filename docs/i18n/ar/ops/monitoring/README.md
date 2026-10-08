# مراقبة SpeedMQ والتنبيهات

يوفّر هذا الدليل قوالب مراقبة جاهزة للاستخدام:

| الملف | الوظيفة |
| --- | --- |
| `prometheus-alerts.yml` | قواعد تنبيه Prometheus (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | لوحة Grafana قابلة للاستيراد (تغطي اللوحات الإشارات الأساسية أدناه) |
| `README.md` | الاستخدام، وقائمة المقاييس، ومعنى كل تنبيه وطريقة معالجته، والثغرات المعروفة |

---

## 1. كيف تستخدمه

### 1.1 التجميع (Prometheus)

تكشف الواجهة الإدارية (افتراضيًا `:15672`) عن `/metrics` بصيغة Prometheus النصية، و**تتطلب Basic Auth**:

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

> يُوصى بإنشاء حساب للقراءة فقط خاص بالمراقبة (يكفي وسم `monitoring` لقراءة المقاييس)، ولا تعِد استخدام كلمة مرور المدير.

للتحقق من سلامة التجميع (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 قواعد التنبيه

ضع `prometheus-alerts.yml` في دليل قواعد Prometheus، وأشر إليه في `prometheus.yml` ثم أعد التحميل:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

تستخدم القواعد `job="speedmq"` بشكل موحّد؛ فإن كان اسم job لديك مختلفًا، فاستبدله في كل المواضع.

### 1.3 لوحة Grafana

يُستورد `grafana-dashboard.json` عبر **Dashboards → Import → رفع JSON**، واختر مصدر بيانات Prometheus لديك عند الاستيراد
(وتُشار إليه في اللوحة بمتغير `${DS_PROMETHEUS}`). ويُسند متغير القالب `DS_PROMETHEUS` في تعيين الاستيراد.

**【غير مُتحقَّق منه】** لم تُشغَّل نسخة Grafana على هذا الجهاز، ولم يُجرَّ تحقق استيراد حقيقي؛ وقد خضع ملف JSON فقط لتحقق نحوي (14 لوحة، والتحليل ناجح).

---

## 2. مقتطف حقيقي من `/metrics` (دليل)

ما يلي هو **المخرج الحقيقي** لـ `/metrics` من نسخة `1.0.0` على هذا الجهاز (وقد أُنشئ طابور durable واحد `persist.q`،
فظهرت مقاييس per-queue الموسومة بـ `vhost`/`queue`):

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

## 3. قائمة المقاييس (كلها موجودة فعليًا، المصدر `internal/management/metrics.go`)

| المقياس | النوع | الوسوم | الدلالة |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | العملية تُبلّغ عن بقائها حيّة (حاليًا ثابتة عند 1) |
| `speedmq_build_info` | gauge | `version`,`node` | معلومات البناء، والقيمة دائمًا 1 |
| `speedmq_resource_blocked` | gauge | — | هل حظرت عتبة الموارد المنتِج (1=محظور) |
| `speedmq_connections` | gauge | — | عدد الاتصالات الحالي |
| `speedmq_channels` | gauge | — | عدد القنوات الحالي |
| `speedmq_queues` | gauge | — | عدد الطوابير الحالي |
| `speedmq_exchanges` | gauge | — | عدد المبادلات الحالي |
| `speedmq_consumers` | gauge | — | عدد المستهلكين الحالي |
| `speedmq_queue_messages` | gauge | — | إجمالي الرسائل الجاهزة **على مستوى العموم** |
| `speedmq_queue_messages_unacknowledged` | gauge | — | إجمالي الرسائل غير المؤكَّدة **على مستوى العموم** |
| `speedmq_process_memory_bytes` | gauge | — | الذاكرة **قيد الاستخدام** للعملية (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | إجمالي الذاكرة الفعلية |
| `speedmq_disk_free_bytes` | gauge | — | المساحة المتاحة في دليل البيانات |
| `speedmq_memory_high_watermark` | gauge | — | نسبة عتبة الذاكرة |
| `speedmq_disk_free_limit_bytes` | gauge | — | الحد الأدنى للمساحة الحرة على القرص |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | عدد الرسائل الجاهزة في طابور معيّن |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | عدد الرسائل غير المؤكَّدة في طابور معيّن |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | عدد المستهلكين على طابور معيّن |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | تقدير استهلاك الذاكرة لطابور معيّن |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | العدد التراكمي للرسائل المستقبَلة في الطابور |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | العدد التراكمي للرسائل المُسلَّمة من الطابور |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | العدد التراكمي للرسائل المؤكَّدة في الطابور |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | بيانات وصفية للإضافة، والقيمة دائمًا 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | هل الإضافة في الخدمة (1=enabled، و0=غير ذلك) |

### 3.1 ملاحظات الاستخدام (لتجنّب الكتابة الخاطئة)

- **سلاسل من عائلتين باسم واحد وبأساس مختلف**: لدى `speedmq_queue_messages_unacknowledged` **كلٌّ من** سلسلة عمومية بلا وسوم
  **و**سلسلة per-queue موسومة؛ أما جانب «الجاهزة» فالعمومي يُسمّى `speedmq_queue_messages`، وper-queue يُسمّى
  `speedmq_queue_messages_ready` (الاسمان غير متماثلين). عند كتابة القواعد استخدم `{queue=~".+"}` لتأخذ عائلة per-queue فقط بوضوح.
- **ضبط `speedmq_process_memory_bytes`**: التنفيذ هو `MemStats.HeapInuse + StackInuse` (**الذاكرة قيد الاستخدام**)،
  بنفس ضبط تحديد عتبة ذاكرة النواة؛ لكن نص `# HELP` الخاص به مكتوب «عدد بايتات الذاكرة المطلوبة من نظام التشغيل»، و**النص لا يوافق الضبط الفعلي**،
  وهذا المستند هو المرجع.
- **سلسلة per-queue لا تظهر إلا عند وجود الطابور**: بعد حذف الطابور تختفي السلسلة (وتصبح stale على جانب Prometheus).
  ويمكن للتنبيهات المتعلقة بـ «يُفترض وجود طابور لكن لا بيانات له» أن تستعين بـ `absent()` أو `or vector(0)` في Grafana.
- **counter يعود إلى الصفر بعد إعادة تشغيل العملية**: القيم `*_total` تراكمية داخل العملية، وتعود إلى 0 عند إعادة التشغيل؛ فاستخدم `rate()`/`increase()`،
  ولا تضبط عتبات على القيم المطلقة مباشرة.
- **لا إشارات عنقود في metrics**: انظر §5.

---

## 4. معنى التنبيهات والمعالجة المقترحة (المقابلة لـ `prometheus-alerts.yml`)

| التنبيه | شرط الإطلاق | المعنى | المعالجة المقترحة |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | تعذّر الوصول إلى هدف التجميع كاملًا | افحص العملية/المنفذ/الشبكة/المصادقة؛ أعد التشغيل وراجع سجل البدء |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | التجميع نجح لكن العملية تُبلّغ عن عدم بقائها حيّة | بند احتياطي، راجع سجل الخروج غير الطبيعي |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | الإضافة disabled/failed/down | `speedmqctl plugins show <name>` لفحص `runtime_note`؛ والإضافات الخارجية ذات `restart=always` تشفى عادةً من تلقاء نفسها |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | بلوغ عتبة الذاكرة/القرص، وحظر المنتِج | راجع عتبة الذاكرة والمساحة المتاحة على القرص؛ وتأكد من تقدّم المستهلكين |
| `SpeedMQMemoryWatermarkHigh` | نسبة الذاكرة قيد الاستخدام > 0.9×العتبة 10m | الاقتراب من عتبة الذاكرة | قلّل التراكم/ارفع سرعة الاستهلاك، لمنع الإطلاق |
| `SpeedMQDiskFreeLow` | المتاح < 1.5×الحد الأدنى للقرص 10m | دليل البيانات على وشك الامتلاء | وسّع/نظّف؛ وبلوغ الحد الأدنى يحظر المنتِج |
| `SpeedMQQueueBacklogGrowing` | الجاهزة >10000 وتنمو بشكل رتيب خلال 15m | استمرار تراكم الطابور | وسّع المستهلكين / افحص جهة الاستهلاك؛ وافحص أعطال الرسائل الميتة/TTL |
| `SpeedMQQueueNoConsumers` | المستهلكون=0 مع وجود رسائل جاهزة 15m | لا أحد يستهلك | افحص عملية جهة الاستهلاك؛ وتأكد من عدم انقطاع المستهلك |
| `SpeedMQUnackedPileUp` | غير المؤكَّدة >1000 15m | توقّف المستهلك/عدم ack | افحص منطق معالجة المستهلك وprefetch؛ وأغلق الاتصال وأعد الإرسال عند الحاجة |
| `SpeedMQConnectionSpike` | الاتصالات >10000 10m | عدد اتصالات غير طبيعي | افحص تسرّب الاتصالات؛ وينبغي للعميل إعادة استخدام الاتصال |

> العتبات (10000 / 1000 وغيرها) هي **قيم ابتدائية**، فاضبطها وفق حجم طوابيرك وخصائص أعمالك.

---

## 5. الثغرة المعروفة: لا توجد حاليًا مقاييس لـ «فقدان الأغلبية / غياب leader» في العنقود

- **الواقع**: `/metrics` **لا يضم** أي مقاييس من نوع العنقود (لا وجود لـ `speedmq_cluster_*`). وحالة العنقود موجودة فقط في JSON العائد من `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **لذلك** **يتعمّد** `prometheus-alerts.yml` **عدم كتابة** تنبيهات مبنية على مقاييس العنقود —— فلو كُتبت **لن تُطلَق أبدًا**
  (فPrometheus لا يخطئ لعدم وجود اسم المقياس)، وذلك تسليم «يبدو صحيحًا لكنه بلا أثر».
- **حل بناء ذاتي** (اختيار من اثنين، وكلاهما يحتاج بنائه خارج SpeedMQ، وليس من نطاق هذا المستودع):
  1. استخدم مُصدِّر JSON عام لتجميع `/api/cluster`، واربطه بمقياس مخصص (مثل `speedmq_cluster_has_quorum`)، ثم نبّه على هذا المقياس؛
  2. استخدم سكربت مسبار يستدعي `/api/cluster` دوريًا، وينبّه عند `has_quorum=false` أو `paused=true`.
- ضبط العتبات ذو الصلة: يعني `has_quorum=false` فقدان الاتصال بالأغلبية؛ وفي ظل `pause_minority` (الافتراضي) **تتوقف الخدمة وتُقطع الاتصالات** عندئذ.

---

## 6. بنود أخرى **غير مُتحقَّق منها**

- لوحة Grafana **لم يُتحقق استيرادها في Grafana حقيقي** (واكتُفي باجتياز التحقق النحوي لـ JSON).
- قواعد التنبيه **لم يُتحقق تحميلها في Prometheus/Alertmanager حقيقي** (فلم تُشغَّل Prometheus على هذا الجهاز).
  غير أن **أسماء المقاييس في القواعد قُوبلت بندًا بندًا مع المخرج الحقيقي لـ `/metrics`** (انظر §2/§3)، فليس هناك مشكلة «اسم مكتوب خطأً يؤدي إلى عدم الإطلاق أبدًا».
