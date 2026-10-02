<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | **Español** | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

Middleware de mensajería **compatible con RabbitMQ** escrito en Go. Los clientes de RabbitMQ existentes **no requieren cambios de código ni cambiar de SDK**: basta con cambiar la dirección de conexión para integrarse.

## Introducción

- **Compatibilidad de protocolo**: AMQP 0-9-1 (incluidas las extensiones de RabbitMQ) y MQTT 3.1.1; la base de compatibilidad es la **semántica de RabbitMQ 4.3**.
- **Despliegue sencillo**: un binario / un contenedor, con la UI de administración ya integrada; no requiere Nginx, base de datos ni runtime de Node adicionales.
- **Operación suficiente**: UI de administración (colas / exchanges / conexiones / permisos de cuentas / hosts virtuales / políticas / límites / clúster), Prometheus `/metrics` y la línea de comandos `swiftmqctl`.
- **Puertos predeterminados**: `5672` (AMQP), `1883` (MQTT), `15672` (UI de administración / API HTTP / métricas).

Capacidades ya disponibles: persistencia (registro por segmentos + niveles de fsync + recuperación ante fallos), confirmación de publicación, TTL / mensajes muertos / límite de longitud, prioridad de consumidores, Direct Reply-To, clúster (metadatos Raft + colas de quórum + reenvío entre nodos) y activación/desactivación en caliente de plugins.

***

## Inicio rápido

### Opción 1: Docker (recomendado)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose up -d --build

docker compose ps        # el estado debe ser Up (healthy)
docker compose logs -f   # seguir los logs
```

Equivalente con docker puro:

```bash
docker build -t swiftmq:1.0.0 .
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  swiftmq:1.0.0
```

- Los datos se guardan en el volumen con nombre `swiftmq-data`; al reconstruir el contenedor no se pierden. La configuración se monta en modo de solo lectura desde `configs/swiftmqd.json`; tras modificarla, `docker compose restart` la aplica.
- Detener: `docker compose down` (conserva los datos); `docker compose down -v` (elimina también los datos).

### Opción 2: binario local (requiere Go 1.24+)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> Los artefactos de compilación de la UI de administración no se incluyen en el repositorio. Si quieres usar la UI, ejecuta primero `npm ci && npm run build` en `web/`;
> sin compilarla también puedes iniciar y enviar/recibir mensajes con normalidad, solo que al acceder a `/` aparecerá el aviso «UI de administración no compilada».

### Primer inicio de sesión (cambia primero la cuenta predeterminada, sin falta)

| Punto de entrada | Dirección / credenciales |
| --- | --- |
| UI de administración | <http://localhost:15672/> (usuario `guest`, contraseña `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (mismas credenciales) |

La cuenta de superusuario de una instancia recién instalada lleva la marca de «cambio de contraseña obligatorio en el primer inicio de sesión»: tras iniciar sesión en la UI de administración, se **obligará a modificar a la vez el nombre de cuenta y la contraseña**, y solo después de cambiarlos se podrá acceder al panel.

También se puede hacer directamente mediante la API (adecuado para automatización):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ El `guest/guest` predeterminado se comporta igual que en RabbitMQ: **solo permite el inicio de sesión local**. Para conectar desde fuera del contenedor o de forma remota, hay que habilitar `remote_access` para ese usuario en la configuración (la configuración de ejemplo ya lo habilita para el escenario de contenedor).
> **En cuanto el servicio sea accesible desde el exterior, cambia las credenciales de inmediato.**

### Integra tu aplicación (basta con cambiar la dirección de conexión)

```python
# Python (pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (cliente mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

La API HTTP de administración es compatible con `rabbitmqadmin`; el «añadir cola / exchange» de la UI de administración es el endpoint de declaración estándar, y los scripts también pueden hacerlo:

```bash
# declarar la cola (para las colas de quórum, se expresa con arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Operación diaria

| Asunto | Punto de entrada |
| --- | --- |
| UI de administración | <http://localhost:15672/>: colas / exchanges / conexiones / permisos de cuentas / hosts virtuales / políticas / límites / interruptores de características / clúster; en la esquina superior derecha se pueden ajustar la actualización automática y el **idioma de la interfaz** |
| Métricas de monitorización | <http://localhost:15672/metrics> (texto de Prometheus, requiere autenticación); para paneles y alertas, consulta [docs/ops/monitoring](ops/monitoring/README.md) |
| Línea de comandos | `./bin/swiftmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (desactivación en caliente, el puerto se cierra de inmediato) |
| Comprobación de estado | `nc -z 127.0.0.1 15672` (compose ya incluye healthcheck) |
| Copia de seguridad y restauración | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Actualización | [docs/ops/upgrade.md](ops/upgrade.md) |
| Línea base de seguridad | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Configuración habitual (para el ejemplo completo, consulta [configs/swiftmqd.json](../../../configs/swiftmqd.json); también se puede sobrescribir con variables de entorno `SWIFTMQ_*`):

| Opción de configuración | Descripción | Valor predeterminado |
| --- | --- | --- |
| `data_dir` | Directorio de datos (mensajes + metadatos), **es imprescindible persistirlo** | `data` |
| `listeners` | Dirección de escucha de cada protocolo; se puede configurar TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Dirección de escucha de la UI / API de administración | `:15672` |
| `management.language` | Idioma predeterminado de la UI de administración; si se deja vacío, se elige automáticamente según la zona horaria del lugar de despliegue | Automático |
| `storage.fsync` | Nivel de escritura en disco `none / os / batch / always` (también determina el momento del confirm) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Umbrales de recursos: al activarse bloquean al productor, **sin perder mensajes** | `0.4` / 50 MiB |
| `users` | Tabla de usuarios integrada (contraseña + etiquetas + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Clúster multinodo (desactivado de forma predeterminada); para cambiar miembros usa `swiftmqctl add_member` | Desactivado |

> Es posible que el puerto esté ocupado: basta con cambiar a otro puerto mediante `listeners` / `management.addr`.

***

## Estructura del proyecto

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # punto de entrada del proceso broker (es el que se ejecuta)
│   └── swiftmqctl/      # CLI de operaciones (usa la API HTTP de administración, desacoplado de la versión del núcleo)
├── internal/            # implementación del núcleo
│   ├── protocol/        # plugins de protocolo: amqp091, mqtt (codificación/decodificación / métodos / sesiones)
│   ├── broker/          # núcleo: vhost, exchanges, colas, mensajes muertos, control de flujo, vistas del plano de administración
│   ├── store/           # persistencia: registro por segmentos, índice de colas, recuperación ante fallos
│   ├── raft/ meta/      # clúster: Raft propio y replicación de metadatos
│   ├── management/      # API HTTP de administración + métricas de Prometheus + servicio estático de la UI integrada
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # contrato estable externo: API de plugins (plugin) y protocolo de comunicación de plugins de proceso externos (sidecar)
├── web/                 # proyecto frontend de la UI de administración (Vue 3 + Vite); los artefactos se incorporan al binario mediante go:embed durante la compilación
├── configs/             # configuración de ejemplo
├── docs/ops/            # documentación de operaciones: copia de seguridad y restauración / actualización / línea base de seguridad / monitorización
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## Contribución

Se agradece el envío de Issues y Pull Requests. El fundamento de este proyecto es la **compatibilidad de protocolo**, por lo que:

- Al corregir un bug, describe el comportamiento correspondiente de RabbitMQ (versión, cliente, pasos de reproducción);
- Para cambios que afecten a detalles del protocolo, adjunta el resultado de la comparación con RabbitMQ;
- Antes de enviar, asegúrate de que `go build ./...`, `go vet ./...`, `go test ./...` y `gofmt -l .` pasen.

***

## Licencia

Este proyecto se distribuye bajo la [Apache License 2.0](../../../LICENSE).

Se permite su uso, modificación y distribución (incluido el uso comercial), siempre que se conserven los avisos de copyright y licencia, y sin ningún tipo de garantía.

Copyright 2026 houzch (véase [NOTICE](../../../NOTICE))

***

## Agradecimientos

La especificación del protocolo AMQP 0-9-1 y la semántica de comportamiento de [RabbitMQ](https://www.rabbitmq.com/) son la referencia de comparación del trabajo de compatibilidad de este proyecto. Este proyecto es una implementación independiente, sin relación de dependencia con el equipo oficial de RabbitMQ, y no utiliza su código.

***

## Únete al grupo de intercambio

Escanea el código para unirte al grupo de intercambio de SwiftMQ; si tienes dudas, puedes preguntar directamente en el grupo:

![Grupo de intercambio de SwiftMQ](../../../1280X1280.PNG)
