# Руководство по разработке внешних процессных плагинов (sidecar) для SwiftMQ — PHP

> **Для кого**: для разработчиков, пишущих внешние процессные плагины (sidecar) для SwiftMQ на PHP.
> **Сначала прочитайте**: [Руководство по разработке внешних процессных плагинов (sidecar)](plugin-development.md) (ментальная модель / поля конфигурации / сводная таблица проводного протокола).
> **Пример проекта**: рабочая область `swiftmq-plugin/php/sidecar_plugin.php` (только стандартная библиотека, **без зависимостей composer**).

---

## 1. Как это выглядит в работе

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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

`swiftmqd.json` (**реальная конфигурация — стандартный JSON, комментарии недопустимы**):

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

### Шаг второй: запуск

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Шаг третий: проверка

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

В PHP используется `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Полезная нагрузка кадра данных = `pack('N', $streamId) . сырые байты`.

### 3.2 Рукопожатие и heartbeat

Ядро **первым отправляет Hello**, вы отвечаете `HelloAck`; ядро проверяет `name` и `api_version` (сейчас `v1`).
Затем каждые 2 с приходит `Ping`, отвечайте `Pong`.

### 3.3 Модель параллелизма: реентерабельный насос кадров (в PHP нет нитей)

PHP CLI однопоточный и блокирующий, поэтому здесь используется не "нить на каждый поток", а следующее:

- **Цикл чтения** (`serve()`) отвечает за рукопожатие, heartbeat, открытие потоков, эхо данных и обработку прямых вызовов;
- **Эхо** не требует дополнительной машины состояний: получили `kindData` — сразу пишем обратно `kindData` как есть;
- **Обратный вызов** реализуется через `callAndWait()`: отправив `kindCall`, он читает кадры и параллельно их диспетчеризует,
  пока не прочитает **свой** ответ (`reverse=true` и совпадение `id`), и только тогда возвращается.

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

Это означает, что `dispatchOther()` **обязан быть реентерабельным**: он может быть вызван повторно внутри одного `callAndWait`
(например, при обработке `session.deliver` нужно снова вызвать `session.settle`). Именно так и сделано в примере.

### 3.4 Семантический мост (сначала обязательна аутентификация)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Опускать `core.authenticate` нельзя: у операционной поверхности ядра для соединения до аутентификации нет личности, и прямой `session.open` будет отклонён
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

Доставки **приходят обратно от ядра** прямым вызовом (`method = "session.deliver"`); после обработки — `session.settle`
(`ack` / `requeue` / `reject`; номер доставки глобально уникален, номер потока не передаётся).

---

## 4. Разбор кода (пример проекта)

`swiftmq-plugin/php/sidecar_plugin.php` — около 320 строк:

| Место | Назначение |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Чтение и запись кадров |
| `Conn::serve()` | Основной цикл чтения |
| `Conn::dispatchOther()` | Диспетчеризация кадров, кроме рукопожатия (реентерабельно) |
| `Conn::callAndWait()` | Обратный вызов (реентерабельный насос кадров) |
| `Conn::handleHello()` | Проверка и ответ HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Прямой вызов (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Аутентификация + объявление + публикация + потребление |

---

## 5. Практическая проверка (воспроизведено на локальной машине)

Windows + PHP 7.4; ядро в Docker (`swiftmq:1.1.01`), плагин на хост-машине (`tcp://host.docker.internal:19021`).

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

Покрыто: **рукопожатие → аутентификация → семантический мост → обратная отправка доставки → расчёт → эхо потока байтов**.

---

## 6. Особенности, специфичные для PHP

- **В PHP 7.4 нет возвращаемого типа `mixed`** (он появился только в PHP 8.0): в примере обратный вызов возвращает "любой тип",
  поэтому **объявление возвращаемого типа не пишется** (используется комментарий `@return mixed`). На 7.4 запись `: mixed` вызовет ошибку синтаксиса.
- **Числовые типы в JSON**: `json_decode($s, true)` по умолчанию разбирает целые как `int`, а большие целые могут стать `float`;
  для номера доставки в масштабах этого примера проблемы нет, но если у вас номера большие, рассмотрите `JSON_BIGINT_AS_STRING`.
- **base64 обязателен**: `message.body` и `core.authenticate.response` в JSON являются строками base64
  (`base64_encode` / `base64_decode($s, true)`).
- **Не используйте `pcntl_fork` для параллелизма**: в Windows нет pcntl, к тому же после fork нарушается допущение "одно соединение — один писатель";
  одного потока + реентерабельного насоса кадров уже достаточно (разве что вам нужны очень тяжёлые вычисления на потоке — тогда это лучше вынести во внешний сервис).
- **`stream_socket_accept` блокирующий**: жизненным циклом процесса управляет ядро (`spawn`) или supervisor;
  не забудьте обрабатывать возврат `''` (EOF) из `fread` → завершить это соединение и вернуться к accept.
- **Буферизация вывода**: логи пишите через `fwrite(STDOUT, …)` с завершающим переводом строки, чтобы ядро могло построчно перенаправить их в свой журнал.

---

## 7. Дополнительно

- Собственный интерфейс управления плагина: добавьте в конфигурацию `console_url` (§5.8 основного документа), и на странице "управление плагинами" в панели управления появится прямая ссылка.
- Независимое развёртывание: `spawn: []` + `address: "tcp://<имя сервиса>:19021"`, внутри контейнера слушать `0.0.0.0`.
- При необходимости более высокой производительности можно перевести плагин на постоянный Swoole / RoadRunner или подобное, но **проводной протокол не меняется**; достаточно обеспечить:
  последовательную запись кадров, неблокирующий цикл чтения и сопоставление обратных вызовов по `id`+`reverse`.
