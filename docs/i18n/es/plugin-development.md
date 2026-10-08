# Guía de desarrollo de plugins de proceso externo (sidecar) para SpeedMQ

> **Dirigido a**: desarrolladores que no quieren hacer fork / recompilar el kernel y desean ampliar SpeedMQ con **cualquier lenguaje**.
> **Alcance**: este documento solo trata una forma de plugin —— el **plugin de proceso externo** (término del kernel `sidecar`). Los plugins de protocolo integrados en el kernel (AMQP 0-9-1 / MQTT) quedan fuera del alcance.
> **Cómo leerlo**: las secciones 1–2 construyen el modelo mental, la sección 3 trata de escribir código y **la sección 5 explica "cómo integrarlo y ejecutarlo junto con el kernel para servir a los clientes una vez terminado el desarrollo"**;
> **para otros lenguajes (Python / Node.js / PHP / Java), consulta las guías por lenguaje de §4** (cada una incluye un proyecto de ejemplo completo y verificado).
> El código de este documento es un esqueleto mínimo ejecutable que puedes copiar como punto de partida. El chino simplificado es el idioma de origen.

---

## 1. Qué es

Un **proceso independiente** que implementa cierto "protocolo" (analiza el flujo de bytes del cliente) en su propio proceso.
El kernel lo aloja según la configuración: **los puertos los abre el kernel y las conexiones las proxea el kernel**, y el registro / arranque-parada / auditoría / aislamiento reutilizan todos los mecanismos existentes del kernel.

Primero establece tres modelos mentales correctos (los puntos donde es más fácil equivocarse):

1. **El proceso del plugin es un "servicio local"**: solo escucha en una **dirección local** (TCP o unix socket) y espera a que **el kernel se conecte**.
   La dirección de la conexión es **kernel (cliente) → plugin (servidor)**, y el handshake también lo envía primero el kernel.
2. **El puerto de negocio externo no lo abre el plugin**: lo crea **el kernel** según `protocols[].listeners` de la configuración y lo expone a los clientes.
   Los clientes se conectan al **puerto del kernel**, y los bytes son proxeados por el kernel al proceso del plugin. El proceso del plugin **no necesita** abrir por sí mismo un puerto de negocio.
3. **La semántica es opcional**: el plugin puede limitarse a "transportar bytes" (implementando el protocolo enteramente por su cuenta),
   o puede alcanzar la semántica del kernel (colas / enrutamiento / permisos / confirmación) mediante **llamadas inversas** a `session.*`,
   compartiendo **exactamente la misma semántica** que los plugins de protocolo integrados (por lo que vhost, permisos, enrutamiento y comportamiento de cola no divergirán).

| Beneficios | Costes |
| --- | --- |
| Extender sin modificar ni recompilar el kernel | Una copia local extra de bytes en el plano de datos (proxy del kernel, sin paso de fd, consistente entre plataformas) |
| Implementar en cualquier lenguaje (solo necesitas implementar el protocolo de cable) | Una RPC local extra por cada llamada inversa (codificación/decodificación JSON + copia) |
| El plugin puede publicarse / actualizarse / reiniciarse de forma independiente | El sniffing permanece en el lado del kernel: solo puede reconocerse por "prefijo" o "puerto dedicado" |
| Un fallo solo afecta a ese plugin: el kernel lo marca `down` sin salir ni caerse | Solo la capacidad `net.listen` surte efecto realmente; los demás valores de capacidad son reservados (ver §7) |

---

## 2. Cómo funciona

Establecimiento de la conexión (**el kernel es el cliente y el plugin es el servidor**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

Handshake:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

Comienza el servicio:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Ciclo de vida** (el host `internal/plugin/sidecar` del lado del kernel):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Semántica de estados** (visible mediante `speedmqctl plugins show`):

| Estado | Significado | Acción del operador |
| --- | --- | --- |
| `enabled` | Conectado y sirviendo | — |
| `failed` | **No llegó a arrancar en el inicio** (error de configuración / handshake rechazado / el proceso no pudo lanzarse) | Revisa `RuntimeNote` y el log del kernel, corrige la configuración o el plugin; el kernel solo reintenta tras reiniciarse |
| `down` | **Ya había arrancado pero ahora no está** (el proceso se cayó / la conexión se perdió) | Ve a lanzar el proceso del plugin; el kernel se recuperará automáticamente según la política `restart` |
| `disabled` | `enabled=false` en la configuración, o deshabilitado en caliente por el operador | Recuperar con `plugins enable <nombre>` |

---

## 3. Desarrollo (Go)

### 3.1 Crear el proyecto

El plugin es un **módulo Go independiente** que solo depende de dos paquetes de contrato públicos:

- `github.com/houzch/speedmq/pkg/sidecar` —— el protocolo de cable y la implementación del lado del plugin (**obligatorio**)
- `github.com/houzch/speedmq/pkg/plugin` —— solo cuando necesites tipos como `plugin.Message` / `plugin.Error` (opcional)

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/speedmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/speedmq@v1.1.03
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/speedmq=../speedmq
```

> Al usar `replace` para la depuración conjunta, el plugin y el kernel deben usar **la misma copia del código fuente**; de lo contrario, aunque la versión de API (`v1`) coincida, los tipos pueden diferir.

### 3.2 Implementar `Handler` (tres métodos)

Toda la superficie de negocio del proceso del plugin es `sidecar.Handler` con `Hello` / `Call` / `Open`.
El handshake, el heartbeat, el multiplexado y la fragmentación los gestiona `pkg/sidecar`; no necesitas tocar las tramas.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/houzch/speedmq/pkg/sidecar"
)

const (
	pluginName = "my-sidecar" // 必须与内核配置里的插件名一致
	version    = "0.1.0"
	apiVersion = "v1" // 必须等于 sidecar / plugin 的 APIVersion
	protocol   = "myproto" // 应与配置里 protocols[].name 一致
)

func main() {
	addr := ":19001" // 内核来连的本机地址；可用 -addr 覆盖
	handler := &handler{}

	srv, err := sidecar.NewServer(handler, sidecar.ServerOptions{
		Address: "tcp://" + addr,
		Logger:  stdLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 监听失败: %v\n", err)
		os.Exit(1)
	}
	// Address() 能读回实际地址（配置里写 :0 时有用）。
	log.Printf("my-sidecar 已启动 name=%s version=%s addr=%s", pluginName, version, srv.Address())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 服务退出: %v\n", err)
		os.Exit(1)
	}
}

type handler struct {
	streams atomic.Int64
}

var _ sidecar.Handler = (*handler)(nil)

// Hello 处理握手：返回的 HelloAck 发给内核。返回 error 即拒绝接入（内核会明确隔离该插件）。
func (h *handler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	// 内核版本过旧时可以拒绝，避免带着不兼容跑起来。
	if hello.Plugin != pluginName {
		return sidecar.HelloAck{}, fmt.Errorf("插件名不匹配：内核声明 %q，本插件是 %q", hello.Plugin, pluginName)
	}
	if hello.APIVersion != apiVersion {
		return sidecar.HelloAck{}, fmt.Errorf("插件 API 版本不匹配：内核 %q，插件 %q", hello.APIVersion, apiVersion)
	}
	return sidecar.HelloAck{
		Name:         pluginName,
		Version:      version,
		APIVersion:   apiVersion,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{protocol}, // 展示用；真正生效的是内核配置里的 protocols
		Methods:      []string{"stats"},  // 展示用；内核不提供通用调用入口
	}, nil
}

// Call 处理方法调用。约定的系统方法 session.deliver 是"内核把消费投递回推给插件"，
// 必须在实现里处理（见 §3.4）。
func (h *handler) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		return h.handleDeliver(ctx, params)
	case "stats":
		return map[string]any{"streams": h.streams.Load()}, nil
	default:
		// 未知方法必须明确报错：静默成功会让调用方以为生效了。
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

// Open 处理一条新打开的流：一条流 = 内核侧的一条客户端连接。
// 通常**阻塞处理到流结束**再返回；返回后该流即结束（内核会关闭对应的客户端连接）。
func (h *handler) Open(ctx context.Context, stream *sidecar.Stream, meta sidecar.Open) error {
	h.streams.Add(1)
	// meta.Remote / meta.Local 是两端地址，meta.Peek 是嗅探阶段读到的前缀字节，可用于更细的分支判断。
	return h.serve(stream)
}

// stdLogger 把 pkg/sidecar 的最小日志接口接到标准库日志。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Info(msg string, args ...any) {
	s.l.Printf("%s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
func (s stdLogger) Warn(msg string, args ...any) {
	s.l.Printf("WARN %s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
```

### 3.3 Plano de datos: leer y escribir un `Stream`

`sidecar.Stream` implementa `io.ReadWriteCloser`, así que basta con tratarlo como "una conexión":

```go
func (h *handler) serve(stream *sidecar.Stream) error {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			// 你的协议解析在这里；示例先原样回显。
			if _, werr := stream.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			return nil // 对端关闭
		}
	}
}
```

Puntos clave:

- Cada conexión de cliente = un stream; el plugin puede empezar a enviar/recibir inmediatamente dentro de `Handler.Open`.
- Los mensajes grandes los fragmenta la librería automáticamente (cada trama ≤ 64 KiB), y **el uso de memoria es independiente del tamaño del mensaje**.
- Contrapresión: el búfer de recepción de un solo stream tiene un límite superior; cuando el búfer se llena, se bloquea la **goroutine de despacho** de esa conexión (todos los streams esperan juntos) ——
  este es el compromiso entre "memoria predecible" y "limitación de velocidad por stream"; consulta §7 para más detalles.

### 3.4 Usar el puente semántico del kernel (`session.*`)

Si quieres que el plugin reutilice la semántica de colas / enrutamiento / permisos / confirmación del kernel (en lugar de construir la suya propia), usa **llamadas inversas**.
En un stream, úsalas en el orden `session.open` → otros `session.*` → (`session.close`):

```go
import (
	"errors"

	"github.com/houzch/speedmq/pkg/plugin"
	"github.com/houzch/speedmq/pkg/sidecar"
)

func (h *handler) runDemo(ctx context.Context, stream *sidecar.Stream) error {
	br, ok := sidecar.BridgeFromContext(ctx) // 当前流的内核桥（内核在建流时注入 ctx）
	if !ok {
		return errors.New("ctx 中没有内核桥")
	}
	streamID := stream.ID()

	// 1) 认证：连接的内核操作面在认证前没有身份，会话一定打不开。
	//    response 就是你自己协议里的凭据（这里以 SASL PLAIN 为例）。
	plain := append([]byte("\x00guest\x00"), []byte("guest")...)
	var ident sidecar.AuthIdentityDTO
	if err := br.Call(ctx, sidecar.MethodCoreAuthenticate, sidecar.CoreAuthenticateParams{
		Stream: streamID, Mechanism: "PLAIN", Response: plain,
	}, &ident); err != nil {
		return err
	}

	// 2) 打开会话（内核会做与内置协议插件相同的权限校验）
	if err := br.Call(ctx, sidecar.MethodSessionOpen,
		sidecar.SessionOpenParams{Stream: streamID, VHost: "/"}, nil); err != nil {
		return err
	}

	// 3) 声明一个临时队列
	var q sidecar.QueueInfoResult
	if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue, sidecar.QueueDeclareParams{
		Stream: streamID, Exclusive: true, AutoDelete: true,
	}, &q); err != nil {
		return err
	}

	// 4) 发布一条消息（持久化等待在内核应答前已完成：调用返回即已按 fsync 档位落盘）
	if err := br.Call(ctx, sidecar.MethodSessionPublish, sidecar.PublishParams{
		Stream: streamID, RoutingKey: q.Name,
		Message: sidecar.MessageDTO{Body: []byte("hello")},
	}, nil); err != nil {
		return err
	}

	// 5) 注册消费者；投递随后以正向调用 session.deliver 到达 Handler.Call
	var c sidecar.ConsumeResult
	return br.Call(ctx, sidecar.MethodSessionConsume, sidecar.ConsumeParams{
		Stream: streamID, Queue: q.Name, Prefetch: 32,
	}, &c)
}

// 处理内核回推的投递并结算
func (h *handler) handleDeliver(ctx context.Context, params json.RawMessage) (any, error) {
	var p sidecar.DeliverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		return nil, errors.New("ctx 中没有内核桥")
	}
	// 结算不需要流号：投递编号在整条连接上全局唯一。
	// 动作三选一：Ack（消费完成）/ Requeue（重新入队）/ Reject（丢弃，可能进死信）。
	return nil, br.Call(ctx, sidecar.MethodSessionSettle, sidecar.SettleParams{
		DeliveryID: p.DeliveryID, Action: sidecar.SettleActionAck,
	}, nil)
}
```

**Métodos de llamada inversa de un vistazo** (constantes en `pkg/sidecar/bridge.go` → nombres de cable):

| Grupo | Nombre de cable (constante) | Descripción |
| --- | --- | --- |
| Autenticación | `core.authenticate`（`MethodCoreAuthenticate`） | **Debe hacerse primero**: entrega las credenciales de tu protocolo al kernel para su verificación; parámetros `{stream, mechanism, response}`, devuelve `{user}` |
| Sesión | `session.open`（`MethodSessionOpen`） | Abre una sesión para un vhost sobre el stream; solo puede hacerse **después de la autenticación** |
| | `session.close`（`MethodSessionClose`） | Libera la sesión del stream (cancela consumidores, elimina colas exclusivas) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Crear / eliminar (declarar pasivamente uno inexistente → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Enlaces de exchange a exchange |
| Cola | `session.declare_queue` / `session.delete_queue` | Crear / eliminar; el servidor genera uno cuando `name` está vacío |
| | `session.bind_queue` / `session.unbind_queue` | Enlaces de cola a exchange |
| | `session.purge_queue` | Vacía los mensajes listos (excluyendo los no confirmados) |
| Publicación | `session.publish` | Devuelve `{routed, rejected}`; la persistencia se completa antes de la respuesta |
| Get | `session.get` | Obtiene activamente un mensaje; `found=false` significa que la cola está vacía |
| Consumo | `session.consume` / `session.cancel` | Registra / cancela consumidores |
| Liquidación | `session.settle` | Liquida una entrega (`ack` / `requeue` / `reject`) |
| **Directa** | `session.deliver` | **Kernel → plugin**: reenvía una entrega (se maneja en tu `Handler.Call`) |

**Cuatro reglas que debes cumplir**:

1. **Primero `core.authenticate`**: la superficie de operación del kernel de la conexión no tiene identidad antes de la autenticación,
   por lo que en ese momento `session.open` se rechaza (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   El plugin es responsable de extraer las credenciales de su propio protocolo; la lógica de autenticación y la tabla de usuarios siguen en el kernel, y el plugin nunca toca el almacén de contraseñas.
2. **Después `session.open`**: llamar a otros métodos sin una sesión abierta hace que el kernel devuelva `KindPreconditionFailed` ("el stream N aún no ha abierto una sesión").
3. **Liquida cada entrega exactamente una vez**: elige uno de `Ack` / `Requeue` / `Reject`.
   Tanto `Ack` como `Reject` descartan el mensaje, pero **solo `Reject` va a la ruta de mensajes muertos**.
4. **Las entregas no liquidadas no se pierden**: cuando termina un stream (el cliente se desconecta / `Handler.Open` retorna) o se cae la conexión del plugin,
   el kernel trata todas las entregas no liquidadas **como "reencolar"**, evitando que los mensajes queden atascados.

**Restauración de errores**: el `*plugin.Error` del kernel llega a través del puente como un `*sidecar.RPCError` (campos `Kind` / `Text`),
y puede restaurarse a un `plugin.Error` categorizado en lugar de perder la categoría dentro de una cadena:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Mapeo de los `Kind` comunes a los protocolos integrados (para ayudarte a decidir cómo devolver errores a los clientes):

| `plugin.ErrorKind` | Semántica | Mapeo AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | El objeto no existe | 404 NOT_FOUND (cierra el Channel) |
| `KindPreconditionFailed` | Parámetros inconsistentes con un objeto existente / sesión no abierta | 406 PRECONDITION_FAILED (cierra el Channel) |
| `KindAccessRefused` | Permisos insuficientes / nombre reservado | 403 ACCESS_REFUSED (cierra el Channel) |
| `KindResourceLocked` | Recurso exclusivo ocupado | 405 RESOURCE_LOCKED (cierra el Channel) |
| `KindInvalidPath` | El vhost no existe | 402 INVALID_PATH (cierra la conexión) |
| `KindNotImplemented` | Capacidad no implementada | 540 NOT_IMPLEMENTED (cierra la conexión) |
| `KindInternal` | Error interno del kernel | 541 INTERNAL_ERROR (cierra la conexión) |

**Límite de fidelidad de tipos del mensaje**: la tabla de propiedades (`MessageDTO.Properties.Headers`) se retransmite a través de JSON,
por lo que **la información de tipo numérico que distingue `int32` / `double` como en una field-table de AMQP no está disponible**.
Cuando se requiera una fidelidad de tipos estricta, lleva esa información tú mismo en el cuerpo del mensaje (bytes sin procesar).

### 3.5 Estado y logs del lado del plugin

- El **estado de un plugin externo lo determina si la conexión está viva**, y el plugin no lo informa por sí mismo (el `StateReporter` de los plugins integrados no aplica a procesos externos).
- Logs: `sidecar.ServerOptions.Logger` escribe en el stdout/stderr del proceso del plugin;
  **cuando el kernel lo lanza mediante `spawn`, estas salidas son reenviadas por el kernel al log del kernel** (etiquetadas con `plugin`), lo que facilita la recolección centralizada.
- En un despliegue independiente (sin spawn), recolecta los logs del plugin a tu manera.

### 3.6 Autoprueba sin el kernel

`Handler` es una interfaz Go normal; en una prueba unitaria puedes instanciarlo directamente y llamar a `Hello` / `Call` / `Open` para cubrir la lógica de negocio, sin levantar ninguna red:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Para la verificación de extremo a extremo, consulta §5.

---

## 4. Desarrollar en otros lenguajes (especificación del protocolo de cable)

`pkg/sidecar` es un contrato público sin dependencias; el protocolo de cable en sí es sencillo y puede implementarse en cualquier lenguaje.
Para integrarte, necesitas implementar las siguientes convenciones "a nivel de byte" (ver el código fuente en `pkg/sidecar/frame.go`, `proto.go`).

> **Se proporcionan guías por lenguaje con proyectos de ejemplo completos** (los ejemplos pasaron todos la verificación: handshake → autenticación → puente semántico → entrega/liquidación → flujo de bytes):
>
> | Lenguaje | Guía | Proyecto de ejemplo (en el espacio de trabajo `speedmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (solo biblioteca estándar) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (solo biblioteca estándar) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (solo biblioteca estándar) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (un solo archivo, solo JDK) |
>
> Para la implementación de referencia completa en Go, consulta el proyecto de prueba independiente `speedmq-test/test/integration/echosidecar/` (usa `pkg/sidecar.Server` directamente,
> por lo que no necesitas preocuparte por los detalles a nivel de byte que siguen).

**Formato de trama** (uniforme para todas las tramas):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Tipos de trama `kind`**:

| kind | Nombre | Dirección | Carga útil |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | vacío |
| 4 | Pong | plugin → kernel | vacío |
| 5 | Call | bidireccional | JSON `Call` |
| 6 | Reply | bidireccional | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | bidireccional | `u32 BE stream` + bytes sin procesar |
| 10 | Close | bidireccional | JSON `Close` |

**Estructuras JSON del plano de control** (los nombres de campo coinciden con `proto.go`).

> Este apartado es un **ejemplo de mensajes del protocolo de cable** (en un mismo bloque se dan varios mensajes en orden, de ahí los separadores `//` a modo de explicación),
> y **no es configuración que pueda escribirse directamente en `speedmqd.json`**.

```jsonc
// Hello（内核 → 插件）
{ "plugin": "my-sidecar", "protocol_version": "v1", "kernel_version": "1.1.01", "api_version": "v1" }
// HelloAck（插件 → 内核）；deny 非空表示拒绝服务
{ "name": "my-sidecar", "version": "0.1.0", "api_version": "v1",
  "capabilities": ["net.listen"], "protocols": ["myproto"], "methods": ["stats"], "deny": "" }
// Call / Reply：方向字段 reverse 区分"内核→插件"（false）与"插件→内核"（true）
{ "id": 1, "method": "session.open", "reverse": false, "params": { } }
{ "id": 1, "reverse": false, "ok": true, "error": "", "data": { } }
// Open（内核 → 插件）／OpenAck（插件 → 内核）／Close（双向）
{ "stream": 7, "remote": "1.2.3.4:5000", "local": "0.0.0.0:19002", "peek": "" }
{ "stream": 7, "ok": true, "error": "" }
{ "stream": 7, "reason": "closed by peer" }
```

**Semántica que debes cumplir**:

- **Handshake**: el kernel envía `Hello` primero, y el plugin debe responder con una trama `HelloAck`.
  El kernel valida `HelloAck.name == el nombre del plugin en la config` y `HelloAck.api_version == la APIVersion del kernel`;
  un `deny` no vacío se trata como rechazo de conexión (el plugin queda aislado).
- **Heartbeat**: por defecto el kernel envía `Ping` cada 2s, y el plugin debe responder `Pong` en un plazo de 8s; del lado del plugin, si no recibe ninguna trama en 24s puede cerrar la conexión él mismo.
- **Dos espacios de ID**: el `reverse` de `Call` distingue la dirección, y cada dirección se incrementa desde 1 de forma independiente,
  así que `Reply` **debe llevar `reverse` de vuelta**, de lo contrario la respuesta se entregará al esperador equivocado.
- **Sin base64 en el plano de datos**: los datos grandes como los cuerpos de mensaje van directamente en la carga útil de la trama `Data` (`stream` + bytes sin procesar), fragmentados según sea necesario.

> Si usas Go, basta con usar `pkg/sidecar` directamente y no tendrás que implementar ninguno de los detalles anteriores.

---

## 5. Ejecutarlo junto con el kernel: integración, servicio a clientes, empaquetado ★

Esta sección responde a "cómo integrarlo en SpeedMQ y cómo servir a los clientes una vez terminado el desarrollo".

### 5.1 Declarar el plugin en la configuración

Un plugin externo está **totalmente gestionado por la configuración**, y no se necesita cambiar ningún código del kernel para él. Añade una entrada a la sección `plugins` de `speedmqd.json`
(**la configuración real es JSON estándar y no puede contener comentarios**):

```json
{
  "listeners": {
    "myproto": [{ "addr": ":19002" }]
  },
  "plugins": {
    "my-sidecar": {
      "builtin": false,
      "enabled": true,
      "required": false,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["/usr/local/bin/my-sidecar", "-addr", "tcp://127.0.0.1:19001"],
        "restart": "always",
        "protocols": [
          {
            "name": "myproto",
            "prefix": "MP",
            "listeners": [{ "name": "myproto", "addr": ":19002" }]
          }
        ]
      }
    }
  }
}
```

Explicación punto por punto (consulta la tabla siguiente para la lista de campos):

- El nombre de clave de `plugins.<nombre del plugin>` **debe coincidir con el `HelloAck.name` que el plugin declara de sí mismo**, de lo contrario el handshake se rechaza.
- `builtin: false`: declara explícitamente un plugin externo (si se omite, el plano de gestión lo muestra como integrado).
- `enabled`: desactivarlo = no se lanza el proceso, no se crea el listener.
- Con `required: true`, un fallo de arranque bloquea el inicio del kernel —— no lo actives para plugins externos.
- `address` es **la dirección a la que el kernel se conecta** (el kernel es el cliente); cuando `spawn` no está vacío, el kernel lanza el proceso en tu nombre.
- `protocols[].prefix` **debe ser no vacío** (ver las reglas de sniffing en §5.3).
- `listeners` son los **puertos externos del protocolo, abiertos por el kernel** (los clientes se conectan al kernel).

Lista de campos:

| Campo | Obligatorio | Descripción |
| --- | --- | --- |
| `sidecar.address` | ✅ | Dirección del proceso del plugin: `tcp://host:port` o `unix:///path` |
| `sidecar.spawn` | ✕ | Línea de comandos que el kernel lanza en tu nombre (el primer elemento es el ejecutable); **vacío = el kernel solo se conecta y no lanza**, y tú gestionas el proceso |
| `sidecar.restart` | ✕ | `always` (por defecto, se recupera solo tras una desconexión/caída) o `never` (solo marca `down` y espera la intervención del operador) |
| `sidecar.protocols[].name` | ✅ | Nombre del protocolo (único a nivel global, participa en la prioridad de sniffing) |
| `sidecar.protocols[].prefix` | ✕ | Prefijo de sniffing (ASCII); **vacío = no participa en el sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Listener externo de este protocolo (`name` + `addr`), creado por el kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | Sobrescribe el timeout del handshake (por defecto 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Sobrescribe el intervalo de heartbeat (por defecto 2s) |

> **Nombre del plugin vs. nombre del protocolo**: los dos **pueden diferir** (por ejemplo, el plugin `my-sidecar` proporciona el protocolo `myproto`).
> La desactivación en caliente primero encuentra todos los protocolos registrados por el nombre del plugin y luego cierra los puertos de esos protocolos, así que no hace falta usar deliberadamente el mismo nombre.

### 5.2 Tres modos de integración

| Modo | Configuración | Cuándo usarlo |
| --- | --- | --- |
| **Mismo host + el kernel lanza (spawn)** | `spawn: [...]`, `address` apunta a la dirección donde escucha | Despliegue en la misma máquina, un solo contenedor; lo más sencillo, el kernel gestiona el lanzamiento y la recuperación |
| **Mismo host + autogestionado (dial)** | `spawn: []`, `address` apunta a un proceso ya en ejecución | Gestiona el ciclo de vida del plugin con systemd / supervisor |
| **Entre hosts / entre contenedores (dial, debe ser tcp)** | `spawn: []`, `address: "tcp://<nombre del servicio>:19001"` | El plugin y el kernel desplegados en contenedores / máquinas separadas |

Elección de dirección:

- **En la misma máquina se recomienda un unix socket** (`unix:///tmp/my-sidecar.sock`): no ocupa un puerto TCP y no se ve afectado por la ocupación de puertos del host.
  Ten en cuenta que la ruta del socket debe ser escribible por el proceso del kernel (en un contenedor, el usuario `speedmq`, que no es root).
- **Entre contenedores se requiere TCP**, y el proceso del plugin debe escuchar en `0.0.0.0`, con `address` usando **el nombre del servicio en la red del contenedor**.

> No confundas la dirección: **la dirección en la que el plugin escucha** = `address`; **el puerto expuesto a los clientes** = `protocols[].listeners`.

### 5.3 Servir a los clientes: reconocido por `prefix`

Cuando la capa de acceso despacha conexiones, **solo mira el resultado del sniffing**: para cada protocolo habilitado pregunta `Sniff(peek)` en el orden de registro (peek es como máximo 8 bytes),
y el que coincide se hace cargo de la conexión. Por lo tanto:

1. **`prefix` debe ser no vacío** (ASCII, ≤ 8 bytes). Solo cuando los primeros bytes que envía el cliente coinciden con él se entregará la conexión a tu plugin.
   Ejemplo: `"prefix": "PY"` → los primeros bytes del cliente deben ser `PY` (puedes tratar el prefijo como la cabecera mágica de tu protocolo).
2. **Un `prefix` vacío significa que no participa en el sniffing**: tales conexiones **no** se entregarán al plugin (comprobado: las conexiones en el puerto de escucha se cierran inmediatamente).
   Por lo tanto, un `prefix` vacío solo es adecuado para el escenario "otro protocolo reenviará por ti en el mismo puerto"; **no** lo uses para construir un puerto dedicado.
3. `listeners[].addr` decide "en qué puerto servir externamente", y `prefix` decide "si esta conexión es tuya" ——
   ambos deben usarse juntos: **un puerto dedicado también necesita un `prefix` no vacío** (esta es también la razón por la que en la configuración de ejemplo del kernel
   `echo-sidecar` escribe tanto `prefix: "ECHO"` como `listeners: [":1885"]`).
4. El sniffing coincide en el orden de registro de protocolos, y **gana el primero que coincide**: cuando coexisten varios plugins, los prefijos deben ser distinguibles (por ejemplo, todos comenzando por el mismo byte se ocultarán entre sí).

### 5.4 Sobrescribir las direcciones de escucha y TLS

- La dirección de escucha externa puede darse en **dos lugares**: `sidecar.protocols[].listeners[].addr` (por defecto) y
  `listeners.<nombre del protocolo>` (que sobrescribe en bloque por nombre de protocolo). Cuando ambos existen, prevalece `listeners.<nombre del protocolo>`.
- Cuando se necesite TLS, proporciona el certificado en `listeners.<nombre del protocolo>[i].tls` (campos idénticos a los de los protocolos integrados).
  A continuación se muestra un fragmento de `listeners` (**JSON estándar, no puede contener comentarios**): el elemento 1 es en texto plano y el elemento 2 usa TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/speedmq/tls/cert.pem",
                                 "key_file":  "/etc/speedmq/tls/key.pem" } }
  ]
}
```

> El TLS lo termina **el kernel** en el lado del listener; el proceso del plugin recibe un flujo en texto plano —— el plugin no necesita gestionar TLS.

### 5.5 Empaquetado: hacer que el plugin se ejecute junto con el kernel

**Opción A —— Integrarlo en la misma imagen** (recomendado para plugins "publicados junto con el kernel"): añade una línea a la etapa de ejecución de `speedmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Luego, en la configuración pon `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
con `address` igual al mismo valor. El kernel lo lanzará al arrancar.

**Opción B —— Montar el binario** (sin cambiar la imagen, bueno para la depuración conjunta):

```yaml
services:
  speedmq:
    image: houzch/speedmq:1.1.03
    command: ["-config", "/etc/speedmq/speedmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/speedmqd.json:/etc/speedmq/speedmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

La configuración usa un unix socket (para evitar ocupar un puerto extra). A continuación el fragmento `sidecar` dentro de `plugins.my-sidecar`
(**JSON estándar, no puede contener comentarios**; `prefix` sigue debiendo ser no vacío, ver §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Opción C —— Contenedor separado** (el plugin se publica por separado / escala de forma independiente):

```yaml
services:
  speedmq:
    image: houzch/speedmq:1.1.03
    volumes: ["./configs/speedmqd.json:/etc/speedmq/speedmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

En la configuración, `spawn: []` (el kernel solo se conecta, no lanza) y `address: "tcp://my-sidecar:19001"` (el nombre del servicio de compose).

### 5.6 Arranque y verificación

```bash
# 1) 内核日志里应能看到握手与接入
docker compose logs speedmq | grep -E "外部插件已接入|外部插件进程"

# 2) 经 CLI 看插件状态（state=enabled 且 RuntimeNote 为空）
./bin/speedmqctl plugins list
./bin/speedmqctl plugins show my-sidecar

# 3) 经管理 API 看（等价入口）
curl -u guest:guest http://127.0.0.1:15672/api/plugins/my-sidecar

# 4) 实测对外服务：你的协议端口由内核监听，直接连它
#    若你的协议是文本行协议，可以这样冒烟：
printf 'hello\n' | nc 127.0.0.1 19002
```

En `plugins show`, presta especial atención a `state` y `RuntimeNote`:
`failed` viene con el motivo del fallo (handshake rechazado / no se pudo abrir el puerto…); `down` viene con el motivo de la desconexión (el proceso se cayó / la conexión se perdió).

### 5.7 Operaciones en tiempo de ejecución

| Operación | Comando / API | Efecto |
| --- | --- | --- |
| Deshabilitar en caliente | `speedmqctl plugins disable my-sidecar` o `PUT /api/plugins/my-sidecar/disable` | **Cierra los listeners externos de ese plugin** (deshabilitación a nivel de capacidad); el kernel y los demás plugins no se ven afectados |
| Habilitar en caliente | `speedmqctl plugins enable my-sidecar` | Reabre sus listeners; si un arranque anterior falló, reintenta una vez |
| Ver estado | `speedmqctl plugins list/show` | Estado + motivo de fallo/desconexión |
| Salida del kernel | — | Desconecta del plugin, recupera las sesiones puenteadas, **termina los procesos hijos lanzados por el kernel mediante `spawn`** |

> La deshabilitación en caliente solo cierra la "capacidad" (los puertos de escucha) y **no** mata el proceso del plugin lanzado mediante `spawn`; la recuperación del proceso ocurre cuando el kernel sale.

### 5.8 Ofrecer una entrada en la consola de administración (opcional)

Cuando el plugin incluye su propia interfaz de usuario, añade un `console_url` (la dirección de la interfaz de administración; para el resto de campos ver §5.1) a la sección `plugins.<nombre del plugin>`.
A continuación solo se muestra la entrada `plugins.my-sidecar` (**JSON estándar, no puede contener comentarios**; el contenido de la sección `sidecar` es el mismo que en §5.1):

```json
"plugins": {
  "my-sidecar": {
    "builtin": false,
    "enabled": true,
    "console_url": "http://127.0.0.1:19003/",
    "sidecar": { }
  }
}
```

- La página **Gestión de plugins** de la consola de administración (los datos provienen del campo `console_url` de `GET /api/plugins`) muestra para tales plugins un
  botón "Abrir interfaz de administración", que **se abre en una pestaña nueva**.
- Cuando no se declara `console_url`, el botón no está disponible y un tooltip dice "Este plugin no proporciona interfaz de administración".
- Son meramente **metadatos escritos en la configuración por quien despliega**: no forman parte de la API del plugin (`pkg/plugin`), no participan en el arranque-parada del plugin,
  y la interfaz en sí la aloja el plugin (puede vivir en el proceso del plugin o en cualquier servicio independiente).

---

## 6. Matriz de ciclo de vida y tolerancia a fallos

| Escenario | Comportamiento del kernel | Impacto en el lado del plugin |
| --- | --- | --- |
| El proceso del plugin no arrancó / handshake rechazado | Reintenta la conexión en 8s; si sigue fallando, marca `failed` y lo aísla (no bloquea el arranque del kernel) | Ninguno |
| El proceso lanzado por `spawn` termina | Lo registra; marca `down`; retrocede y reconecta / relanza según la política `restart` | El nuevo proceso realiza el handshake de nuevo |
| El proceso del plugin se cae (en tiempo de ejecución) | El kernel no se ve afectado; `down` + reconexión con retroceso | `pkg/sidecar.Server` cierra esa conexión |
| El kernel recibe `kill -9` | — | El lado del plugin recupera la conexión por sí mismo mediante el timeout de inactividad (sin tramas durante 24s por defecto), sin dejar zombis |
| El kernel sale normalmente | Llama a `Stop`: desconecta, recupera las entregas no liquidadas (como reencolado), hace `Kill` de los procesos hijos | Recibe SIGKILL |
| Entregas mientras la conexión del plugin está caída | Las entregas no liquidadas siempre se **reencolan** y nunca se pierden | — |
| El cliente se desconecta / `Open` retorna | Cierra el stream correspondiente y libera la sesión y los consumidores de ese stream | `Stream.Read` devuelve EOF |

---

## 7. Líneas rojas y límites conocidos

**Líneas rojas**

1. Un plugin solo puede depender de `pkg/sidecar` (y opcionalmente de `pkg/plugin`); **no debe** depender del `internal/**` del kernel.
2. El nombre del plugin debe coincidir con la configuración y `APIVersion` debe coincidir con el kernel, de lo contrario no puede conectarse (esto evita "ejecutarse en silencio sin surtir efecto").
3. Al usar `session.*`: **primero `core.authenticate`, luego `session.open`**, y liquida cada entrega **exactamente una vez**.
4. `protocols[].prefix` debe ser no vacío, de lo contrario las conexiones no se entregarán al plugin (ver §5.3).
5. Un rechazo en `Hello` debe **devolver explícitamente un error** (no quedarse en silencio) —— de lo contrario el kernel solo ve "conexión cerrada" y no puede localizar la causa.

**Límites conocidos**

- **El sniffing está en el lado del kernel**: los plugins externos no pueden definir funciones de sniff personalizadas y solo pueden coincidir por `prefix` (ASCII, ≤ 8 bytes);
  un `prefix` vacío significa "no se puede obtener ninguna conexión" (ver §5.3).
- **El plano de datos pasa por un proxy local**: no hay paso de fd (Windows no tiene `SCM_RIGHTS`), añadiendo una copia de memoria extra respecto al modo en proceso;
  cada llamada inversa también añade una RPC local extra.
- **Los tipos de la tabla de propiedades se degradan**: `Properties.Headers` se retransmite a través de JSON, por lo que distinciones como `int32` / `double` se pierden (ver §3.4).
- **La contrapresión de un solo stream afecta a toda la conexión**: cuando el búfer de recepción de un stream se llena, bloquea la goroutine de despacho de esa conexión; la limitación de velocidad por stream es una optimización futura.
- **Los fallos de autenticación solo transmiten texto**: un fallo de autenticación del kernel es un `*plugin.AuthError` (una clasificación distinta de `plugin.ErrorKind`),
  y solo el texto llega al plugin a través del puente; el plugin debe mapearlo a códigos de error de protocolo según su propia convención.
- **Solo la capacidad `net.listen` surte efecto realmente**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` son **ranuras reservadas**; declararlas solo participa en la auditoría (ver la visualización de gobernanza en §5.6), y actualmente no existe un punto de extensión correspondiente.

---

## 8. FAQ de resolución de problemas

| Síntoma | Causa y tratamiento |
| --- | --- |
| Estado `failed`, el motivo contiene "el nombre del plugin no coincide" | El nombre del plugin configurado ≠ `HelloAck.name`; hazlos coincidir |
| Estado `failed`, el motivo contiene "la versión de API no coincide" | `HelloAck.api_version` ≠ la `APIVersion` del kernel; hazlos coincidir |
| Estado `failed`, el motivo contiene "handshake rechazado" | El `Hello` del plugin devolvió un error (`deny`); revisa la salida del plugin reenviada en el log del kernel |
| Estado `failed`, el motivo contiene "fallo al conectar con el plugin externo" | El proceso no arrancó / `address` está mal escrito / la ruta del socket no es escribible (en contenedores, cuida los permisos del usuario `speedmq`) |
| Estado `down` | El proceso del plugin se cayó o la conexión se perdió; `restart=always` reconecta automáticamente, mientras que `never` requiere un relanzamiento manual |
| El puerto no está abierto / el cliente no puede conectarse | `protocols[].listeners` no está configurado o su dirección ha sido sobrescrita por `listeners.<nombre del protocolo>`; comprueba ambos lugares |
| El cliente se conecta a otro puerto y se desconecta de inmediato | Ese puerto no coincide con tu protocolo (`prefix` vacío o el prefijo no coincide); configura un `prefix` no vacío para el protocolo (ver §5.3) |
| Error `ACCESS_REFUSED - ... for user ''` | No hubo **autenticación** antes del puente semántico; llama a `core.authenticate` antes de `session.open` |
| La llamada inversa informa "el stream N aún no ha abierto una sesión" | Haz primero `core.authenticate`, luego `session.open`, y solo entonces llama a otros `session.*` |
| No se reciben entregas de consumo | Las entregas llegan a tu `Call` como una **llamada directa** `session.deliver`; confirma que ese método está manejado |
| El plugin está fuera del contenedor, el kernel dentro, y no conecta | Usa `address: tcp://host.docker.internal:<port>` (o pon también el plugin en el contenedor y usa el nombre del servicio); el plugin debe escuchar en `0.0.0.0` |

---

## 9. Referencia (índice del código fuente)

| Qué quieres ver | Archivo |
| --- | --- |
| Protocolo de cable e implementaciones de ambos lados (**lectura obligatoria para el desarrollo**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (tramas), `proto.go` (mensajes), `server.go` (lado del plugin), `client.go` (lado del kernel), `bridge.go` (contrato `session.*`), `stream.go` (streams) |
| Host sidecar del lado del kernel (integración/reconexión/proxy/estado) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Puente semántico del lado del kernel (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Tipos de la superficie de operación de sesión del kernel (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Escucha, sniffing, arranque-parada en caliente por plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Ciclo de vida y gobernanza del plugin (aislamiento/estado/auditoría) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Elementos de configuración y ejemplos (incluida la sección sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/speedmqd.json`](../../../configs/speedmqd.json) |
| Ensamblado de procesos (cómo se integra el sidecar en el kernel) | [`cmd/speedmqd/main.go`](../../../cmd/speedmqd/main.go) |
| Implementación de referencia en Go (usa `pkg/sidecar.Server`, con el puente `session.*` y `core.authenticate`) | Proyecto de prueba independiente `speedmq-test/test/integration/echosidecar/` |
| **Guías por lenguaje + proyectos de ejemplo** | En este directorio `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; los ejemplos están en el **espacio de trabajo** `speedmq-plugin/{python,nodejs,php,java}/` |
