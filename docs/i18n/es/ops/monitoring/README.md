# Monitorización y alertas de SwiftMQ

Este directorio ofrece plantillas de monitorización listas para usar:

| Archivo | Función |
| --- | --- |
| `prometheus-alerts.yml` | Reglas de alerta de Prometheus (`groups: - name: swiftmq`) |
| `grafana-dashboard.json` | Panel de Grafana importable (los paneles cubren las señales clave que se describen a continuación) |
| `README.md` | Uso, lista de métricas, significado y tratamiento de cada alerta, y vacíos conocidos |

---

## 1. Cómo usarlo

### 1.1 Captura (Prometheus)

El plano de administración (`:15672` de forma predeterminada) expone `/metrics` en formato de texto de Prometheus y **requiere Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: swiftmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Se recomienda crear una cuenta de solo lectura específica para la monitorización (la etiqueta `monitoring` ya permite leer las métricas); no reutilices la contraseña de administrador.

Verificar que la captura funciona correctamente (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Reglas de alerta

Coloca `prometheus-alerts.yml` en el directorio de reglas de Prometheus, referéncialo en `prometheus.yml` y recarga:

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

En las reglas se usa `job="swiftmq"` de forma uniforme; si el nombre de tu job es distinto, reemplázalo en todo el texto.

### 1.3 Panel de Grafana

`grafana-dashboard.json` se importa mediante **Dashboards → Import → subir JSON**; al importar, selecciona tu fuente de datos de Prometheus
(en el panel se referencía con la variable `${DS_PROMETHEUS}`). La variable de plantilla `DS_PROMETHEUS` se asigna en el mapeo de importación.

**【No verificado】** No se levantó una instancia de Grafana en esta máquina ni se hizo una validación de importación real; ese JSON solo pasó una validación de sintaxis JSON (14 paneles, análisis correcto).

---

## 2. Fragmento real de `/metrics` (evidencia)

La siguiente es la **salida real** de `/metrics` de una instancia `1.0.0` de esta máquina (se había creado una cola durable `persist.q`,
por lo que aparecen las métricas por cola con las etiquetas `vhost`/`queue`):

```
# HELP swiftmq_up si el nodo está vivo
# TYPE swiftmq_up gauge
swiftmq_up 1
# HELP swiftmq_build_info información de compilación
# TYPE swiftmq_build_info gauge
swiftmq_build_info 1{version="1.0.0",node="swiftmq@DESKTOP-HBDCVPA"}
# HELP swiftmq_resource_blocked si el umbral de recursos está bloqueando a los productores (1=bloqueando)
# TYPE swiftmq_resource_blocked gauge
swiftmq_resource_blocked 0
# HELP swiftmq_connections número actual de conexiones
# TYPE swiftmq_connections gauge
swiftmq_connections 0
# HELP swiftmq_channels número actual de canales
# TYPE swiftmq_channels gauge
swiftmq_channels 0
# HELP swiftmq_queues número actual de colas
# TYPE swiftmq_queues gauge
swiftmq_queues 0
# HELP swiftmq_exchanges número actual de exchanges
# TYPE swiftmq_exchanges gauge
swiftmq_exchanges 6
# HELP swiftmq_consumers número actual de consumidores
# TYPE swiftmq_consumers gauge
swiftmq_consumers 0
# HELP swiftmq_queue_messages total de mensajes listos
# TYPE swiftmq_queue_messages gauge
swiftmq_queue_messages 0
# HELP swiftmq_queue_messages_unacknowledged total de mensajes sin confirmar
# TYPE swiftmq_queue_messages_unacknowledged gauge
swiftmq_queue_messages_unacknowledged 0
# HELP swiftmq_process_memory_bytes bytes de memoria solicitados al sistema operativo por este proceso
# TYPE swiftmq_process_memory_bytes gauge
swiftmq_process_memory_bytes 1564672
# HELP swiftmq_memory_total_bytes memoria física total
# TYPE swiftmq_memory_total_bytes gauge
swiftmq_memory_total_bytes 34181279744
# HELP swiftmq_disk_free_bytes espacio disponible en el directorio de datos
# TYPE swiftmq_disk_free_bytes gauge
swiftmq_disk_free_bytes 240091688960
# HELP swiftmq_memory_high_watermark proporción del umbral de memoria
# TYPE swiftmq_memory_high_watermark gauge
swiftmq_memory_high_watermark 0.4
# HELP swiftmq_disk_free_limit_bytes límite inferior de espacio libre en disco
# TYPE swiftmq_disk_free_limit_bytes gauge
swiftmq_disk_free_limit_bytes 52428800
# HELP swiftmq_queue_messages_ready número de mensajes listos en la cola
# TYPE swiftmq_queue_messages_ready gauge
# HELP swiftmq_queue_messages_unacknowledged número de mensajes sin confirmar en la cola
# TYPE swiftmq_queue_messages_unacknowledged gauge
# HELP swiftmq_queue_consumers número de consumidores en la cola
# TYPE swiftmq_queue_consumers gauge
# HELP swiftmq_queue_memory_bytes estimación del uso de memoria de la cola
# TYPE swiftmq_queue_memory_bytes gauge
# HELP swiftmq_queue_messages_published_total número acumulado de mensajes recibidos por la cola
# TYPE swiftmq_queue_messages_published_total counter
# HELP swiftmq_queue_messages_delivered_total número acumulado de mensajes entregados por la cola
# TYPE swiftmq_queue_messages_delivered_total counter
# HELP swiftmq_queue_messages_acked_total número acumulado de mensajes confirmados por la cola
# TYPE swiftmq_queue_messages_acked_total counter
swiftmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
swiftmq_queue_consumers{vhost="/",queue="persist.q"} 0
swiftmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
swiftmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
swiftmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP swiftmq_plugin_info metadatos del plugin (value siempre 1; el estado está en la etiqueta state)
# TYPE swiftmq_plugin_info gauge
# HELP swiftmq_plugin_up si el plugin está en servicio (1=enabled, 0=otros estados: disabled/failed/down)
# TYPE swiftmq_plugin_up gauge
swiftmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="amqp091",state="enabled"} 1
swiftmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Lista de métricas (todas existen realmente; fuente `internal/management/metrics.go`)

| Métrica | Tipo | Etiquetas | Semántica |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | el proceso se declara vivo (actualmente siempre 1) |
| `swiftmq_build_info` | gauge | `version`,`node` | información de compilación; value siempre 1 |
| `swiftmq_resource_blocked` | gauge | — | si el umbral de recursos está bloqueando a los productores (1=bloqueando) |
| `swiftmq_connections` | gauge | — | número actual de conexiones |
| `swiftmq_channels` | gauge | — | número actual de canales |
| `swiftmq_queues` | gauge | — | número actual de colas |
| `swiftmq_exchanges` | gauge | — | número actual de exchanges |
| `swiftmq_consumers` | gauge | — | número actual de consumidores |
| `swiftmq_queue_messages` | gauge | — | total **global** de mensajes listos |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | total **global** de mensajes sin confirmar |
| `swiftmq_process_memory_bytes` | gauge | — | memoria **en uso** del proceso (`HeapInuse+StackInuse`) |
| `swiftmq_memory_total_bytes` | gauge | — | memoria física total |
| `swiftmq_disk_free_bytes` | gauge | — | espacio disponible en el directorio de datos |
| `swiftmq_memory_high_watermark` | gauge | — | proporción del umbral de memoria |
| `swiftmq_disk_free_limit_bytes` | gauge | — | límite inferior de espacio libre en disco |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | número de mensajes listos de una cola |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | número de mensajes sin confirmar de una cola |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | número de consumidores de una cola |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | estimación del uso de memoria de una cola |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | número acumulado de mensajes recibidos por la cola |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | número acumulado de mensajes entregados por la cola |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | número acumulado de mensajes confirmados por la cola |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | metadatos del plugin; value siempre 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | si el plugin está en servicio (1=enabled, 0=otros) |

### 3.1 Precauciones de uso (para evitar errores)

- **Dos familias de series con el mismo nombre y distinta cardinalidad**: `swiftmq_queue_messages_unacknowledged` tiene **tanto** una serie global sin etiquetas
  **como** series por cola con etiquetas; en cambio, en el lado de "listos", la global se llama `swiftmq_queue_messages` y la de por cola
  `swiftmq_queue_messages_ready` (nombres asimétricos). Al escribir reglas, usa `{queue=~".+"}` para tomar explícitamente solo la familia por cola.
- **El criterio de `swiftmq_process_memory_bytes`**: la implementación es `MemStats.HeapInuse + StackInuse` (**memoria en uso**),
  el mismo criterio que el usado para determinar el umbral de memoria del núcleo; pero su texto `# HELP` dice "bytes de memoria solicitados al sistema operativo", **el texto no coincide con el criterio real**;
  prevalece lo indicado en este documento.
- **Las series por cola solo aparecen mientras la cola existe**: al eliminar la cola, esa serie desaparece (en el lado de Prometheus pasa a stale).
  Las alertas de "la cola debería existir pero no tiene datos" pueden combinarse con `absent()` o con `or vector(0)` de Grafana.
- **Los counter vuelven a cero tras reiniciar el proceso**: los `*_total` son acumulados dentro del proceso y se reinician desde 0 al reiniciar; usa `rate()`/`increase()`,
  y no establezcas umbrales directamente sobre los valores absolutos.
- **Señales de clúster sin métricas**: véase §5.

---

## 4. Significado de las alertas y tratamiento recomendado (correspondiente a `prometheus-alerts.yml`)

| Alerta | Condición de activación | Significado | Tratamiento recomendado |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` 1m | el objetivo de captura es totalmente inalcanzable | revisa el proceso/puerto/red/autenticación; reinicia y consulta el registro de arranque |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` 1m | la captura funciona pero el proceso se declara no vivo | elemento de red de seguridad; revisa el registro de salida anómala |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` 2m | plugin disabled/failed/down | usa `swiftmqctl plugins show <name>` para ver `runtime_note`; los plugins externos con `restart=always` normalmente se autorecuperan |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` 5m | se activan los umbrales de memoria/disco y los productores quedan bloqueados | revisa el umbral de memoria y el espacio libre en disco; confirma si los consumidores avanzan |
| `SwiftMQMemoryWatermarkHigh` | proporción de memoria en uso > 0.9×umbral 10m | se aproxima al umbral de memoria | reduce el backlog / aumenta la velocidad de consumo para evitar activar el bloqueo |
| `SwiftMQDiskFreeLow` | espacio libre < 1.5×límite inferior de disco 10m | el directorio de datos está a punto de llenarse | amplía/limpia; al alcanzar el límite inferior se bloquearán los productores |
| `SwiftMQQueueBacklogGrowing` | listos >10000 y crecimiento monótono durante 15m | la cola acumula backlog de forma continua | amplía los consumidores / revisa el lado consumidor; investiga anomalías de mensajes muertos/TTL |
| `SwiftMQQueueNoConsumers` | consumidores=0 y hay mensajes listos durante 15m | nadie consume | revisa el proceso del lado consumidor; confirma que los consumidores no se han desconectado |
| `SwiftMQUnackedPileUp` | sin confirmar >1000 15m | los consumidores están atascados / no hacen ack | revisa la lógica de procesamiento de los consumidores y el prefetch; si es necesario, cierra la conexión y reentrega |
| `SwiftMQConnectionSpike` | conexiones >10000 10m | número de conexiones anómalo | revisa fugas de conexiones; los clientes deberían reutilizar las conexiones |

> Los umbrales (10000 / 1000, etc.) son **valores de partida**; ajústalos según el tamaño de tus colas y las características de tu negocio.

---

## 5. Vacío conocido: actualmente no hay métricas para "el clúster pierde la mayoría / no hay líder"

- **Hecho**: `/metrics` **no** tiene ninguna métrica de tipo clúster (no hay `swiftmq_cluster_*`). El estado del clúster solo está en el JSON de `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Por lo tanto**, `prometheus-alerts.yml` **omite deliberadamente** las alertas basadas en métricas de clúster; aunque se escribieran, **nunca se activarían**
  (Prometheus no da error porque no exista un nombre de métrica), y eso sería una entrega que "parece correcta pero es inútil".
- **Soluciones propias** (elige una; ambas requieren que las montes fuera de SwiftMQ y quedan fuera del alcance de este repositorio):
  1. usar un exportador JSON genérico para capturar `/api/cluster`, mapearlo a métricas personalizadas (p. ej. `swiftmq_cluster_has_quorum`) y luego alertar sobre esa métrica;
  2. usar un script de sonda que llame periódicamente a `/api/cluster` y emita una alerta cuando `has_quorum=false` o `paused=true`.
- Criterio de umbral relacionado: `has_quorum=false` indica pérdida de contacto con la mayoría; con `pause_minority` (predeterminado), en ese momento **el servicio se suspende y se desconectan las conexiones**.

---

## 6. Otros elementos **no verificados**

- El panel de Grafana **no se validó importándolo en un Grafana real** (solo pasó la validación de sintaxis JSON).
- Las reglas de alerta **no se validaron cargándolas en un Prometheus/Alertmanager real** (no se levantó Prometheus en esta máquina).
  No obstante, los **nombres de métrica de las reglas se contrastaron uno a uno con la salida real de `/metrics`** (véanse §2/§3), por lo que no existe el problema de "un nombre mal escrito que impide que se active nunca".
