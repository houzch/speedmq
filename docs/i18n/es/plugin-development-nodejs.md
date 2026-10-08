# Guía de desarrollo de plugins de proceso externo para SpeedMQ —— Node.js

> **Dirigido a**: desarrolladores que escriben plugins de proceso externo (sidecar) para SpeedMQ con Node.js.
> **Lee primero**: [Guía de desarrollo de plugins de proceso externo (sidecar)](plugin-development.md) (modelo mental / campos de configuración / tabla general del protocolo de cable).
> **Proyecto de ejemplo**: en el espacio de trabajo `speedmq-plugin/nodejs/index.js` (solo la biblioteca estándar de Node, **sin dependencias npm**).

---

## 1. Cómo es cuando se ejecuta

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tres puntos clave: **tu proceso es el servidor** (espera a que el kernel se conecte); **el puerto externo lo abre el kernel** (configuración `protocols[].listeners`);
**`prefix` debe ser no vacío** (prefijo vacío = no participa en el sniffing, la conexión no se te entregará; comprobado que se cierra inmediatamente, ≤8 bytes ASCII).

---

## 2. Ponerlo en marcha en tres pasos

### Paso uno: configuración

`speedmqd.json` (**la configuración real es JSON estándar y no puede contener comentarios**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/speedmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Paso dos: arrancar

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Paso tres: verificar

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

En Node se usa `Buffer`: acumula los bytes recibidos y, cuando haya una trama completa, córtala y procésala.

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

Un stream = una conexión de cliente; la carga útil de una trama de datos es `4 bytes big-endian del número de stream + bytes sin procesar`.

### 3.2 Handshake y heartbeat

El kernel **envía primero Hello**, y tú respondes `HelloAck`; el kernel valida `name` y `api_version` (actualmente `v1`) y luego se conecta.
Después, un `Ping` cada 2s; basta con responder `Pong` (lo maneja el propio bucle de lectura, sin necesidad de un temporizador).

### 3.3 Modelo asíncrono (versión Node)

El bucle de eventos de un solo hilo evita naturalmente el problema de "escritura entrelazada" —— pero ten cuidado de **no hacer que el bucle de lectura espere (await) una llamada inversa**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` es `async`: puede volver a `await call('session.settle', …)`, por lo que jamás debe escribirse como una espera síncrona.

### 3.4 Puente semántico (requiere autenticación primero)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` no puede omitirse: la superficie de operación del kernel de la conexión no tiene identidad antes de la autenticación, y llamar directamente a `session.open` será rechazado
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

Las entregas son **reenviadas directamente** por el kernel (`method = "session.deliver"`); tras procesarlas, `session.settle`
(`ack` / `requeue` / `reject`; el número de entrega es único a nivel global y no lleva el número de stream).

---

## 4. Recorrido del código (proyecto de ejemplo)

`speedmq-plugin/nodejs/index.js` tiene unas 330 líneas:

| Ubicación | Función |
| --- | --- |
| `u32()` / `Conn.send()` | Lectura/escritura de tramas |
| `Conn.drain()` / `dispatch()` | Análisis y despacho por trama |
| `Conn.call()` | Llamada inversa (`Promise` + tabla `pending`, coincidiendo por `reverse=true` e `id`) |
| `Stream` | Lado de lectura del stream: `push/end/read` componen una cola asíncrona |
| `handleHello` | Valida y responde HelloAck |
| `handleForwardCall` / `handleMethod` | Llamada directa (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Autenticación + declarar + publicar + consumir |

---

## 5. Verificación (reproducción en esta máquina)

Windows + Node v24; el kernel en Docker (`speedmq:1.1.01`), el plugin en la máquina anfitriona (`tcp://host.docker.internal:19011`).

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

Cobertura: **handshake → autenticación → puente semántico → reenvío de la entrega → liquidación → eco del flujo de bytes**.

---

## 6. Puntos de atención específicos de Node.js

- **`socket.write` escribe una trama de una vez**: el ejemplo arma la trama completa en un `Buffer` y luego la escribe, por lo que no necesita un lock adicional;
  si divides una trama en varios `write`, debes garantizar tú mismo el orden.
- **El límite de los chunks de `stream.on('data')` no tiene relación con las tramas**: debes acumular el búfer tú mismo (ver `drain()`).
- **base64**: `message.body` y `core.authenticate.response` son cadenas base64 en JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **No uses `await` dentro de `drain()`**: es una función síncrona de corte de tramas; delega el procesamiento asíncrono a `handleForwardCall`.
- **ESM vs CJS**: el ejemplo usa CommonJS (`require`) para que `node index.js` se ejecute directamente; para pasar a ESM solo hay que cambiar a `import`.

---

## 7. Avanzado

- El plugin incluye su propia interfaz de gestión: añade `console_url` a la configuración (§5.8 del documento principal) y aparecerá un acceso directo en la página "Gestión de plugins" de la consola de administración.
- Despliegue independiente (K8s / systemd): `spawn: []` + `address: "tcp://<nombre del servicio>:19011"`, escuchando en `0.0.0.0` dentro del contenedor.
- Reutilización de puertos: da a varios protocolos `prefix` distintos y el kernel despachará las conexiones a cada plugin según el prefijo.
