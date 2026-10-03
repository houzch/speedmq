# Guía de desarrollo de plugins de proceso externo para SwiftMQ —— Python

> **Dirigido a**: desarrolladores que escriben plugins de proceso externo (sidecar) para SwiftMQ con Python.
> **Lee primero**: [Guía de desarrollo de plugins de proceso externo (sidecar)](plugin-development.md) —— allí se explican el modelo mental, los campos de configuración y la tabla general del protocolo de cable;
> este documento solo trata **cómo llevarlo a cabo en Python**, junto con los pasos y resultados verificados en esta máquina.
> **Proyecto de ejemplo**: en el espacio de trabajo `swiftmq-plugin/python/sidecar_plugin.py` (solo biblioteca estándar, cero dependencias de terceros).

---

## 1. Cómo es cuando se ejecuta

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tres puntos clave (fáciles de confundir, memorízalos primero):

1. **Tu proceso es el servidor**: escucha en una dirección local y espera a que el kernel se conecte (`plugins.<nombre>.sidecar.address`).
2. **El puerto de negocio externo lo abre el kernel**: los clientes se conectan al puerto del kernel y los bytes se te proxean (`protocols[].listeners`).
3. **`prefix` debe ser no vacío**: el kernel decide "a quién se entrega esta conexión" mediante el sniffing por prefijo. Un `prefix` vacío significa **no participar en el sniffing**,
   y la conexión en su propio listener tampoco se te entregará (comprobado: la conexión se cierra inmediatamente). La longitud del prefijo es ≤ 8 bytes, ASCII.

---

## 2. Ponerlo en marcha en tres pasos

### Paso uno: declarar el plugin en la configuración

`swiftmqd.json` (**la configuración real es JSON estándar y no puede contener comentarios**):

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` es **la dirección a la que el kernel se conecta para llegar a ti** (el kernel es el cliente, el plugin es el servidor).
- `spawn` vacío = el kernel solo se conecta y no lanza; el proceso lo gestionas tú (systemd / supervisor / compose).
- `prefix` **debe ser no vacío**: los primeros bytes que envía el cliente deben comenzar por él (el kernel decide a quién entregar la conexión mediante el sniffing por prefijo).
- `listeners` son los puertos externos, abiertos por el kernel (los clientes se conectan al kernel, no a ti).
- En un despliegue entre contenedores, usa el **nombre del servicio** en `address` (como `tcp://py-sidecar:19001`), y el plugin debe escuchar en `0.0.0.0`.

### Paso dos: arrancar

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Paso tres: verificar

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Puntos clave de la implementación

### 3.1 Fragmentación (la única capa de bytes que debes implementar bien por tu cuenta)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

La carga útil de una trama de datos = `4 bytes big-endian del número de stream + bytes sin procesar`; la carga útil del plano de control es JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake y heartbeat

Tras conectarse, el kernel **envía primero Hello**, y tú debes responder con una trama `HelloAck`; el kernel valida
`name == el nombre del plugin en la config` y `api_version == la APIVersion del kernel` (actualmente `v1`).
Después el kernel envía un `Ping` cada 2s, y basta con que respondas `Pong` (si no respondes te declarará muerto).

### 3.3 Flujo lógico

Llega `kindOpen` → **responde primero con `OpenAck`**, luego comienza el servicio; llega `kindData` → reescríbelo tal cual (o tras analizarlo según tu protocolo)
como `kindData`; al terminar el procesamiento → envía `kindClose`. Un stream = una conexión de cliente.

### 3.4 Llamadas inversas y puente semántico del kernel

Las llamadas de plugin → kernel van por `kindCall` con `"reverse": true`, y el kernel responde `kindReply` sobre la misma conexión.
**El orden es importante**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **no puede omitirse**: la superficie de operación del kernel de la conexión no tiene identidad antes de la autenticación, y llamar directamente a `session.open` será rechazado
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Los parámetros son la respuesta SASL que hayas analizado de tu propio protocolo:

```python
plain = b"\x00" + user.encode() + b"\x00" + password.encode()
ident = call("core.authenticate", {
    "stream": stream_id, "mechanism": "PLAIN",
    "response": base64.b64encode(plain).decode(),   # 字节在 JSON 里是 base64
})
# 拿到的会话与进程内协议插件完全同一套语义
call("session.open", {"stream": stream_id, "vhost": "/"})
q = call("session.declare_queue", {"stream": stream_id, "exclusive": True, "auto_delete": True})
call("session.publish", {"stream": stream_id, "routing_key": q["name"],
                         "message": {"body": base64.b64encode(b"hi").decode()}})
call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
```

Las entregas del consumo son **reenviadas directamente** por el kernel (`kindCall`, `method = "session.deliver"`); tras procesarlas, liquídalas con
`session.settle` (`ack` / `requeue` / `reject`; el número de entrega es único a nivel global y no necesita el número de stream).

### 3.5 Modelo de concurrencia (versión Python)

| Rol | Hilo |
| --- | --- |
| Bucle de lectura de tramas | Uno por cada conexión del kernel |
| Procesamiento de stream | Uno por cada stream (por eso varias conexiones de cliente pueden ir en paralelo) |
| Procesamiento de llamada directa | Uno por cada llamada |

**Atención obligatoria**: dentro del bucle de lectura **no** se puede esperar de forma síncrona la respuesta de una llamada inversa (se produciría un deadlock) —— las llamadas directas (`session.deliver`)
deben enviarse a un hilo independiente, porque durante su procesamiento puede a su vez iniciar `session.settle`. La escritura de tramas debe serializarse con un lock.

---

## 4. Recorrido del código (proyecto de ejemplo)

`swiftmq-plugin/python/sidecar_plugin.py` tiene unas 320 líneas; funciones destacadas:

| Ubicación | Función |
| --- | --- |
| `read_frame` / `Conn.send` | Lectura/escritura de tramas (prefijo de longitud + kind) |
| `Conn.call` | Llamada inversa: numerar → enviar → esperar la respuesta (coincidiendo por `reverse=true` e `id`) |
| `Conn.serve` | Bucle de lectura de tramas y despacho |
| `Conn._handle_hello` | Valida el nombre del plugin / la versión de API y responde HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Ciclo de vida del stream y eco |
| `Conn._dispatch` | Procesa llamadas directas: `session.deliver` (incluida la liquidación), `stats` |
| `Conn._session_demo` | Autenticación + declarar cola + publicar + consumir |

---

## 5. Verificación (reproducción en esta máquina)

Entorno: Windows + Python 3.12; el kernel corriendo en Docker (`swiftmq:1.1.01`), el plugin en la máquina anfitriona,
y el kernel se conecta a él con `tcp://host.docker.internal:19001`.

```
plugin=py-sidecar state=enabled           # /api/plugins
echo=[PYhello]                            # 客户端连内核端口 19002 发 "PYhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
内核已接入 plugin=py-sidecar peer=('127.0.0.1', 52864)
握手完成 plugin=py-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-9e5d0d695639bad12ce919
收到投递（session.deliver） queue=amq.gen-9e5d0d695639bad12ce919 delivery_id=1 body=hello from python sidecar
流已打开 plugin=py-sidecar stream=1 remote=172.17.0.1:43856 local=172.17.0.2:19002
```

Cadenas cubiertas: **handshake → autenticación → puente semántico (declarar/publicar/consumir) → reenvío de la entrega → liquidación → eco del flujo de bytes**.

---

## 6. Puntos de atención específicos de Python

- **No uses `time.sleep` para esperar el heartbeat**: el bucle de lectura es bloqueante, basta con mantener la conexión mediante el Ping del kernel;
  si estableces un timeout de lectura en el socket, recuerda tratar el timeout como "fin de la conexión" (cuando el kernel recibe `kill -9`, el socket puede no cerrarse a tiempo).
- **`json.dumps` añade espacios por defecto**: el ejemplo usa `separators=(",", ":")` solo para que los logs se vean mejor; el protocolo en sí no lo exige.
- **Los bytes son base64**: `message.body` y `core.authenticate.response` son cadenas base64 en JSON,
  no olvides `base64.b64encode/decode`.
- **La escritura de tramas necesita un lock**: el heartbeat, las respuestas y los bloques de datos provienen de hilos distintos, y una escritura entrelazada corromperá toda la conexión (el ejemplo usa `threading.Lock`).
- También puedes usar `asyncio`, pero debes garantizar "escritura serializada + bucle de lectura sin bloqueo"; la idea es la misma que en la versión con hilos.

---

## 7. Avanzado

- Si quieres dar al plugin su propia interfaz de gestión: añade `console_url` a la configuración y aparecerá un acceso directo en la página "Gestión de plugins" de la consola de administración
  (ver §5.8 del documento principal).
- Convertir el plugin en un servicio independiente gestionado por systemd / K8s: `spawn: []` + `restart: "never"`, lanzado externamente.
- Si necesitas varios protocolos coexistiendo: en el mismo listener, haz que varios plugins usen `prefix` distintos, o abre un puerto dedicado para cada uno.
