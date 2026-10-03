# Руководство по разработке внешних процессных плагинов (sidecar) для SwiftMQ — Node.js

> **Для кого**: для разработчиков, пишущих внешние процессные плагины (sidecar) для SwiftMQ на Node.js.
> **Сначала прочитайте**: [Руководство по разработке внешних процессных плагинов (sidecar)](plugin-development.md) (ментальная модель / поля конфигурации / сводная таблица проводного протокола).
> **Пример проекта**: рабочая область `swiftmq-plugin/nodejs/index.js` (только стандартная библиотека Node, **без зависимостей npm**).

---

## 1. Как это выглядит в работе

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Три ключевых момента: **ваш процесс — это сервер** (ждёт подключения ядра); **внешний порт открывает ядро** (конфигурация `protocols[].listeners`);
**`prefix` обязан быть непустым** (пустой префикс = неучастие в сниффинге, соединение вам не передадут; проверено на практике — сразу разрывается, ≤8 байт ASCII).

---

## 2. Запуск в три шага

### Шаг первый: конфигурация

`swiftmqd.json` (**реальная конфигурация — стандартный JSON, комментарии недопустимы**):

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

### Шаг второй: запуск

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Шаг третий: проверка

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

В Node используется `Buffer`: накапливайте принятые байты и, как только набирается целый кадр, извлекайте его для обработки.

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

Один поток = одно клиентское соединение; полезная нагрузка кадра данных — `4-байтовый big-endian номер потока + сырые байты`.

### 3.2 Рукопожатие и heartbeat

Ядро **первым отправляет Hello**, вы отвечаете `HelloAck`; после проверки `name` и `api_version` (сейчас `v1`) ядро подключает плагин.
Затем каждые 2 с приходит `Ping`, достаточно ответить `Pong` (это попутно обрабатывает цикл чтения, таймер не нужен).

### 3.3 Асинхронная модель (версия для Node)

Однопоточный цикл событий естественным образом избегает проблемы "перемежающейся записи" — но следите за тем, **чтобы цикл чтения не делал await обратного вызова**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` объявлен `async`: он может сам сделать `await call('session.settle', …)`, поэтому его ни в коем случае нельзя оформлять как синхронное ожидание.

### 3.4 Семантический мост (сначала обязательна аутентификация)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

Опускать `core.authenticate` нельзя: у операционной поверхности ядра для соединения до аутентификации нет личности, и прямой `session.open` будет отклонён
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

Доставки **приходят обратно от ядра** прямым вызовом (`method = "session.deliver"`); после обработки — `session.settle`
(`ack` / `requeue` / `reject`; номер доставки глобально уникален, номер потока не передаётся).

---

## 4. Разбор кода (пример проекта)

`swiftmq-plugin/nodejs/index.js` — около 330 строк:

| Место | Назначение |
| --- | --- |
| `u32()` / `Conn.send()` | Чтение и запись кадров |
| `Conn.drain()` / `dispatch()` | Разбор и диспетчеризация по кадрам |
| `Conn.call()` | Обратный вызов (`Promise` + таблица `pending`, сопоставление по `reverse=true` и `id`) |
| `Stream` | Сторона чтения потока: `push/end/read` образуют асинхронную очередь |
| `handleHello` | Проверка и ответ HelloAck |
| `handleForwardCall` / `handleMethod` | Прямой вызов (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Аутентификация + объявление + публикация + потребление |

---

## 5. Практическая проверка (воспроизведено на локальной машине)

Windows + Node v24; ядро в Docker (`swiftmq:1.1.01`), плагин на хост-машине (`tcp://host.docker.internal:19011`).

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

Покрыто: **рукопожатие → аутентификация → семантический мост → обратная отправка доставки → расчёт → эхо потока байтов**.

---

## 6. Особенности, специфичные для Node.js

- **`socket.write` пишет один кадр за раз**: в примере весь кадр собирается в один `Buffer` и только потом пишется, поэтому дополнительная блокировка не нужна;
  если вы разбиваете кадр на несколько `write`, порядок придётся обеспечивать самому.
- **Границы chunk у `stream.on('data')` не связаны с кадрами**: нужно накапливать буфер самостоятельно (см. `drain()`).
- **base64**: `message.body` и `core.authenticate.response` в JSON являются строками base64
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Не используйте `await` внутри `drain()`**: это синхронная функция разбиения на кадры; асинхронную обработку передавайте в `handleForwardCall`.
- **ESM vs CJS**: в примере используется CommonJS (`require`), чтобы можно было запускать `node index.js` напрямую; для перехода на ESM достаточно заменить на `import`.

---

## 7. Дополнительно

- Собственный интерфейс управления плагина: добавьте в конфигурацию `console_url` (§5.8 основного документа), и на странице "управление плагинами" в панели управления появится прямая ссылка.
- Независимое развёртывание (K8s / systemd): `spawn: []` + `address: "tcp://<имя сервиса>:19011"`, внутри контейнера слушать `0.0.0.0`.
- Совместное использование порта: разным протоколам — разные `prefix`, ядро распределяет соединения по префиксу к соответствующим плагинам.
