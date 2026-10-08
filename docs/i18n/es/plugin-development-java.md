# Guía de desarrollo de plugins de proceso externo para SpeedMQ —— Java

> **Dirigido a**: desarrolladores que escriben plugins de proceso externo (sidecar) para SpeedMQ con Java.
> **Lee primero**: [Guía de desarrollo de plugins de proceso externo (sidecar)](plugin-development.md) (modelo mental / campos de configuración / tabla general del protocolo de cable).
> **Proyecto de ejemplo**: en el espacio de trabajo `speedmq-plugin/java/SidecarPlugin.java` (un solo archivo, solo la biblioteca estándar del JDK, sin Maven/Gradle).

---

## 1. Cómo es cuando se ejecuta

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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

### Paso dos: compilar y arrancar

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Paso tres: verificar

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

En Java, lo más sencillo es usar `DataInputStream`/`DataOutputStream` —— sus `readInt`/`writeInt` ya son **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

El lado de escritura debe ser **serializado** (el heartbeat, las respuestas y los bloques de datos provienen de hilos distintos):

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

Carga útil de la trama de datos = `4 bytes big-endian del número de stream + bytes sin procesar`.

### 3.2 Handshake y heartbeat

El kernel **envía primero Hello**, y tú respondes `HelloAck`; el kernel valida `name` y `api_version` (actualmente `v1`) y luego se conecta.
Después, un `Ping` cada 2s, y responde `Pong`.

### 3.3 Modelo de concurrencia (versión Java)

| Rol | Hilo |
| --- | --- |
| Bucle de lectura de tramas | Uno por cada conexión del kernel |
| Procesamiento de stream | Uno por cada stream (varias conexiones de cliente pueden ir en paralelo) |
| Procesamiento de llamada directa | Uno por cada llamada |

**Dentro del bucle de lectura no se puede esperar de forma síncrona la respuesta de una llamada inversa** (se produciría un deadlock): el procesamiento de `session.deliver` debe enviarse a un hilo independiente,
porque internamente también hace `session.settle` (otra llamada inversa). El ejemplo lo hace así.

### 3.4 Puente semántico (requiere autenticación primero)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` no puede omitirse: la superficie de operación del kernel de la conexión no tiene identidad antes de la autenticación, y llamar directamente a `session.open` será rechazado
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

Las entregas son **reenviadas directamente** por el kernel (`method = "session.deliver"`); tras procesarlas, `session.settle`
(`ack` / `requeue` / `reject`; el número de entrega es único a nivel global y no lleva el número de stream).

---

## 4. Recorrido del código (proyecto de ejemplo)

`speedmq-plugin/java/SidecarPlugin.java` tiene unas 470 líneas (incluye un JSON mínimo):

| Ubicación | Función |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Lectura/escritura de tramas (`DataInputStream` + lock de escritura) |
| `Conn.serve()` | Bucle de lectura de tramas y despacho |
| `Conn.call()` | Llamada inversa (tabla `pending` + cola bloqueante, con protección de timeout) |
| `Conn.handleHello()` | Valida y responde HelloAck |
| `StreamState` | Lado de lectura del stream (`BlockingQueue`, `STREAM_END` indica fin) |
| `Conn.handleForwardCall()` / `handleMethod()` | Llamada directa (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Autenticación + declarar + publicar + consumir |
| `Json` (al final del archivo) | Lectura/escritura JSON mínima, solo para que el ejemplo no tenga dependencias |

> **Sugerencia para producción**: sustituye `Json` por la librería que uses habitualmente (Jackson / Gson), o por cualquier stack existente aparte de `java.net.http` ——
> nada de esto tiene relación con lo que este ejemplo quiere explicar (el protocolo de cable).

---

## 5. Verificación (reproducción en esta máquina)

Windows + JDK 25; el kernel en Docker (`speedmq:1.1.01`), el plugin en la máquina anfitriona (`tcp://host.docker.internal:19031`).

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

Cobertura: **handshake → autenticación → puente semántico → reenvío de la entrega → liquidación → eco del flujo de bytes**.

---

## 6. Puntos de atención específicos de Java

- **El compilador procesa `\uXXXX` del código fuente en cualquier posición** (¡incluidos los comentarios!). En los comentarios del ejemplo se escribe deliberadamente
  "NUL + nombre de usuario + NUL + contraseña" en lugar de escribir `\u0000` directamente, porque javac reportaría un carácter ilegal.
- **El código fuente en chino requiere `javac -encoding UTF-8`**, de lo contrario en el valor por defecto de Windows (GBK) se reportará "carácter no asignable con codificación GBK".
  Si en tiempo de ejecución quieres imprimir correctamente el chino, añade `-Dfile.encoding=UTF-8`.
- **Las variables locales capturadas por una lambda deben ser effectively final**: en el ejemplo, `name` se reasigna durante el análisis de argumentos,
  por lo que dentro de la lambda se usa `opts.name` (un campo que se asigna una sola vez).
- **`DataInputStream` es bloqueante**: cuando la conexión se cierra lanza `EOFException`/`IOException`, y en función de eso se finaliza.
- **base64**: `message.body` y `core.authenticate.response` son cadenas base64 en JSON
  (`Base64.getEncoder()/getDecoder()`).
- **La biblioteca estándar del JDK no tiene JSON**: el ejemplo incluye una implementación mínima; `Json.parse` convierte los enteros a `Long` y los decimales a `Double`,
  y al obtener `id` se usa `((Number) m.get("id")).longValue()`.

---

## 7. Avanzado

- Empaquétalo como un jar ejecutable (`Main-Class: SidecarPlugin`) o usa `jlink` para reducir el runtime,
  y luego cambia `spawn` por `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`.
- El plugin incluye su propia interfaz de gestión: añade `console_url` a la configuración (§5.8 del documento principal) y aparecerá un acceso directo en la página "Gestión de plugins" de la consola de administración.
- Despliegue independiente: `spawn: []` + `address: "tcp://<nombre del servicio>:19031"`, escuchando en `0.0.0.0` dentro del contenedor.
