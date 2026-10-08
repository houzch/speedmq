# Руководство по разработке внешних процессных плагинов (sidecar) для SpeedMQ — Java

> **Для кого**: для разработчиков, пишущих внешние процессные плагины (sidecar) для SpeedMQ на Java.
> **Сначала прочитайте**: [Руководство по разработке внешних процессных плагинов (sidecar)](plugin-development.md) (ментальная модель / поля конфигурации / сводная таблица проводного протокола).
> **Пример проекта**: рабочая область `speedmq-plugin/java/SidecarPlugin.java` (один файл, только стандартная библиотека JDK, без Maven/Gradle).

---

## 1. Как это выглядит в работе

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Три ключевых момента: **ваш процесс — это сервер** (ждёт подключения ядра); **внешний порт открывает ядро** (`protocols[].listeners`);
**`prefix` обязан быть непустым** (пустой префикс = неучастие в сниффинге, соединение вам не передадут; проверено на практике — сразу разрывается, ≤8 байт ASCII).

---

## 2. Запуск в три шага

### Шаг первый: конфигурация

`speedmqd.json` (**реальная конфигурация — стандартный JSON, комментарии недопустимы**):

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

### Шаг второй: компиляция и запуск

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Шаг третий: проверка

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Ключевые моменты реализации

### 3.1 Разбиение на кадры

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

В Java проще всего использовать `DataInputStream`/`DataOutputStream` — их `readInt`/`writeInt` уже работают в **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Сторона записи обязана быть **последовательной** (heartbeat, ответы и блоки данных приходят из разных нитей):

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

Полезная нагрузка кадра данных = `4-байтовый big-endian номер потока + сырые байты`.

### 3.2 Рукопожатие и heartbeat

Ядро **первым отправляет Hello**, вы отвечаете `HelloAck`; после проверки `name` и `api_version` (сейчас `v1`) ядро подключает плагин.
Затем каждые 2 с приходит `Ping`, отвечайте `Pong`.

### 3.3 Модель параллелизма (версия для Java)

| Роль | Нить |
| --- | --- |
| Цикл чтения кадров | По одной на каждое соединение с ядром |
| Обработка потока | По одной на каждый поток (несколько клиентских соединений могут работать параллельно) |
| Обработка прямого вызова | По одной на каждый вызов |

**В цикле чтения нельзя синхронно ждать ответа обратного вызова** (это приведёт к взаимоблокировке): обработку `session.deliver` нужно выносить в отдельную нить,
потому что внутри неё ещё будет `session.settle` (ещё один обратный вызов). В примере сделано именно так.

### 3.4 Семантический мост (сначала обязательна аутентификация)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Опускать `core.authenticate` нельзя: у операционной поверхности ядра для соединения до аутентификации нет личности, и прямой `session.open` будет отклонён
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

Доставки **приходят обратно от ядра** прямым вызовом (`method = "session.deliver"`); после обработки — `session.settle`
(`ack` / `requeue` / `reject`; номер доставки глобально уникален, номер потока не передаётся).

---

## 4. Разбор кода (пример проекта)

`speedmq-plugin/java/SidecarPlugin.java` — около 470 строк (включая минимальный JSON):

| Место | Назначение |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Чтение и запись кадров (`DataInputStream` + блокировка записи) |
| `Conn.serve()` | Цикл чтения и диспетчеризация кадров |
| `Conn.call()` | Обратный вызов (таблица `pending` + блокирующая очередь, защита по тайм-ауту) |
| `Conn.handleHello()` | Проверка и ответ HelloAck |
| `StreamState` | Сторона чтения потока (`BlockingQueue`, `STREAM_END` означает завершение) |
| `Conn.handleForwardCall()` / `handleMethod()` | Прямой вызов (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Аутентификация + объявление + публикация + потребление |
| `Json` (в конце файла) | Минимальное чтение и запись JSON, только чтобы пример был без зависимостей |

> **Совет для продакшена**: замените `Json` на привычную вам библиотеку (Jackson / Gson) или используйте существующий стек, отличный от `java.net.http` —
> к тому, о чём рассказывает этот пример (проводной протокол), это отношения не имеет.

---

## 5. Практическая проверка (воспроизведено на локальной машине)

Windows + JDK 25; ядро в Docker (`speedmq:1.1.01`), плагин на хост-машине (`tcp://host.docker.internal:19031`).

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

Покрыто: **рукопожатие → аутентификация → семантический мост → обратная отправка доставки → расчёт → эхо потока байтов**.

---

## 6. Особенности, специфичные для Java

- **Последовательности `\uXXXX` в исходном коде обрабатываются компилятором в любом месте** (включая комментарии!). В комментариях примера намеренно написано
  "NUL + имя пользователя + NUL + пароль", а не прямой `\u0000`, иначе javac сообщит о недопустимом символе.
- **Китайский исходный текст требует `javac -encoding UTF-8`**, иначе в кодировке Windows по умолчанию (GBK) появится ошибка "неотображаемый символ в кодировке GBK".
  Чтобы китайский корректно выводился во время работы, добавьте `-Dfile.encoding=UTF-8`.
- **Локальные переменные, захватываемые lambda, должны быть effectively final**: в примере `name` переприсваивается при разборе аргументов,
  поэтому внутри lambda используется `opts.name` (поле, которому значение присваивается один раз).
- **`DataInputStream` блокирующий**: при разрыве соединения выбрасывается `EOFException`/`IOException`, по этому и завершаем работу.
- **base64**: `message.body` и `core.authenticate.response` в JSON являются строками base64
  (`Base64.getEncoder()/getDecoder()`).
- **В стандартной библиотеке JDK нет JSON**: в примере есть минимальная реализация; `Json.parse` разбирает целые как `Long`, а числа с плавающей точкой как `Double`,
  а для получения `id` используется `((Number) m.get("id")).longValue()`.

---

## 7. Дополнительно

- Соберите исполняемый jar (`Main-Class: SidecarPlugin`) или используйте `jlink` для компактной среды выполнения,
  а затем измените `spawn` на `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`.
- Собственный интерфейс управления плагина: добавьте в конфигурацию `console_url` (§5.8 основного документа), и на странице "управление плагинами" в панели управления появится прямая ссылка.
- Независимое развёртывание: `spawn: []` + `address: "tcp://<имя сервиса>:19031"`, внутри контейнера слушать `0.0.0.0`.
