# Guía de desarrollo de plugins de proceso externo para SpeedMQ —— PHP

> **Dirigido a**: desarrolladores que escriben plugins de proceso externo (sidecar) para SpeedMQ con PHP.
> **Lee primero**: [Guía de desarrollo de plugins de proceso externo (sidecar)](plugin-development.md) (modelo mental / campos de configuración / tabla general del protocolo de cable).
> **Proyecto de ejemplo**: en el espacio de trabajo `speedmq-plugin/php/sidecar_plugin.php` (solo biblioteca estándar, **sin dependencias de composer**).

---

## 1. Cómo es cuando se ejecuta

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tres puntos clave: **tu proceso es el servidor** (espera a que el kernel se conecte); **el puerto externo lo abre el kernel** (`protocols[].listeners`);
**`prefix` debe ser no vacío** (prefijo vacío = no participa en el sniffing, la conexión no se te entregará; comprobado que se cierra inmediatamente, ≤8 bytes ASCII).

---

## 2. Ponerlo en marcha en tres pasos

### Paso uno: configuración

`speedmqd.json` (**la configuración real es JSON estándar y no puede contener comentarios**):

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

### Paso dos: arrancar

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Paso tres: verificar

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
```

---

## 3. Puntos clave de la implementación

### 3.1 Fragmentación

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

PHP usa `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Carga útil de la trama de datos = `pack('N', $streamId) . bytes sin procesar`.

### 3.2 Handshake y heartbeat

El kernel **envía primero Hello**, y tú respondes `HelloAck`; el kernel valida `name` y `api_version` (actualmente `v1`).
Después, un `Ping` cada 2s, y responde `Pong`.

### 3.3 Modelo de concurrencia: bomba de tramas reentrante (PHP no tiene hilos)

La CLI de PHP es monohilo y bloqueante, así que aquí no se usa "un hilo por stream", sino:

- El **bucle de lectura** (`serve()`) se encarga del handshake, el heartbeat, abrir streams, el eco de datos y el procesamiento de llamadas directas;
- El **eco** no necesita una máquina de estados adicional: al recibir `kindData`, reescríbelo tal cual como `kindData` inmediatamente;
- La **llamada inversa** usa `callAndWait()`: tras enviar `kindCall`, lee tramas y las despacha a la vez,
  hasta leer **su propia** respuesta (`reverse=true` e `id` coincidente) y entonces retornar.

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

Esto implica que `dispatchOther()` **debe ser reentrante**: puede volver a invocarse dentro de un `callAndWait`
(por ejemplo, al procesar `session.deliver` y necesitar también `session.settle`). El ejemplo lo hace así.

### 3.4 Puente semántico (requiere autenticación primero)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` no puede omitirse: la superficie de operación del kernel de la conexión no tiene identidad antes de la autenticación, y llamar directamente a `session.open` será rechazado
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

Las entregas son **reenviadas directamente** por el kernel (`method = "session.deliver"`); tras procesarlas, `session.settle`
(`ack` / `requeue` / `reject`; el número de entrega es único a nivel global y no lleva el número de stream).

---

## 4. Recorrido del código (proyecto de ejemplo)

`speedmq-plugin/php/sidecar_plugin.php` tiene unas 320 líneas:

| Ubicación | Función |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Lectura/escritura de tramas |
| `Conn::serve()` | Bucle de lectura principal |
| `Conn::dispatchOther()` | Despacha tramas que no son de handshake (reentrante) |
| `Conn::callAndWait()` | Llamada inversa (bomba de tramas reentrante) |
| `Conn::handleHello()` | Valida y responde HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Llamada directa (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Autenticación + declarar + publicar + consumir |

---

## 5. Verificación (reproducción en esta máquina)

Windows + PHP 7.4; el kernel en Docker (`speedmq:1.1.01`), el plugin en la máquina anfitriona (`tcp://host.docker.internal:19021`).

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

Cobertura: **handshake → autenticación → puente semántico → reenvío de la entrega → liquidación → eco del flujo de bytes**.

---

## 6. Puntos de atención específicos de PHP

- **PHP 7.4 no tiene el tipo de retorno `mixed`** (existe desde PHP 8.0): en el ejemplo, la llamada inversa devuelve "cualquier tipo",
  por lo que **no se declara el tipo de retorno** (se usa el comentario `@return mixed`). Escribir `: mixed` en 7.4 provoca directamente un error de sintaxis.
- **Tipos numéricos de JSON**: `json_decode($s, true)` convierte por defecto los enteros a `int`, y los enteros grandes pueden volverse `float`;
  el número de entrega no da problemas a la escala de este ejemplo; si tu numeración es muy grande, considera `JSON_BIGINT_AS_STRING`.
- **base64 es obligatorio**: `message.body` y `core.authenticate.response` son cadenas base64 en JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **No uses `pcntl_fork` para la concurrencia**: en Windows no hay pcntl, y tras el fork se rompe la suposición de "un solo escritor por conexión";
  un solo hilo + bomba de tramas reentrante ya es suficiente (salvo que necesites hacer cálculos muy pesados en el stream, en cuyo caso es mejor moverlos a un servicio externo).
- **`stream_socket_accept` es bloqueante**: el ciclo de vida del proceso lo gestiona el kernel (`spawn`) o un supervisor;
  recuerda tratar el `''` (EOF) devuelto por `fread` → termina esa conexión y vuelve a accept.
- **Búfer de salida**: usa `fwrite(STDOUT, …)` para los logs y añade un salto de línea, para que el kernel pueda reenviarlos línea a línea al log del kernel.

---

## 7. Avanzado

- El plugin incluye su propia interfaz de gestión: añade `console_url` a la configuración (§5.8 del documento principal) y aparecerá un acceso directo en la página "Gestión de plugins" de la consola de administración.
- Despliegue independiente: `spawn: []` + `address: "tcp://<nombre del servicio>:19021"`, escuchando en `0.0.0.0` dentro del contenedor.
- Si necesitas mayor concurrencia, puedes convertir el plugin en algo permanente como Swoole / RoadRunner, pero **el protocolo de cable no cambia**; solo debes garantizar:
  escritura de tramas serializada, bucle de lectura sin bloqueo y llamadas inversas emparejadas por `id`+`reverse`.
