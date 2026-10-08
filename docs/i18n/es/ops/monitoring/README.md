# Monitorización y alertas de SpeedMQ

Este directorio ofrece plantillas de monitorización listas para usar:

| Archivo | Función |
| --- | --- |
| `prometheus-alerts.yml` | Reglas de alerta de Prometheus (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Panel de Grafana importable (los paneles cubren las señales clave que se describen a continuación) |
| `README.md` | Uso, lista de métricas, significado y tratamiento de cada alerta, y vacíos conocidos |

---

## 1. Cómo usarlo

### 1.1 Captura (Prometheus)

El plano de administración (`:15672` de forma predeterminada) expone `/metrics` en formato de texto de Prometheus y **requiere Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: speedmq
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
  - "rules/speedmq-alerts.yml"
```

En las reglas se usa `job="speedmq"` de forma uniforme; si el nombre de tu job es distinto, reemplázalo en todo el texto.

### 1.3 Panel de Grafana

`grafana-dashboard.json` se importa mediante **Dashboards → Import → subir JSON**; al importar, selecciona tu fuente de datos de Prometheus
(en el panel se referencía con la variable `${DS_PROMETHEUS}`). La variable de plantilla `DS_PROMETHEUS` se asigna en el mapeo de importación.

**【No verificado】** No se levantó una instancia de Grafana en esta máquina ni se hizo una validación de importación real; ese JSON solo pasó una validación de sintaxis JSON (14 paneles, análisis correcto).

---

## 2. Fragmento real de `/metrics` (evidencia)

La siguiente es la **salida real** de `/metrics` de una instancia `1.0.0` de esta máquina (se había creado una cola durable `persist.q`,
por lo que aparecen las métricas por cola con las etiquetas `vhost`/`queue`):

```
# HELP speedmq_up si el nodo está vivo
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info información de compilación
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked si el umbral de recursos está bloqueando a los productores (1=bloqueando)
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections número actual de conexiones
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels número actual de canales
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues número actual de colas
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges número actual de exchanges
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers número actual de consumidores
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages total de mensajes listos
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged total de mensajes sin confirmar
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes bytes de memoria solicitados al sistema operativo por este proceso
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes memoria física total
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes espacio disponible en el directorio de datos
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark proporción del umbral de memoria
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes límite inferior de espacio libre en disco
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready número de mensajes listos en la cola
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged número de mensajes sin confirmar en la cola
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers número de consumidores en la cola
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes estimación del uso de memoria de la cola
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total número acumulado de mensajes recibidos por la cola
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total número acumulado de mensajes entregados por la cola
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total número acumulado de mensajes confirmados por la cola
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info metadatos del plugin (value siempre 1; el estado está en la etiqueta state)
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up si el plugin está en servicio (1=enabled, 0=otros estados: disabled/failed/down)
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Lista de métricas (todas existen realmente; fuente `internal/management/metrics.go`)

| Métrica | Tipo | Etiquetas | Semántica |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | el proceso se declara vivo (actualmente siempre 1) |
| `speedmq_build_info` | gauge | `version`,`node` | información de compilación; value siempre 1 |
| `speedmq_resource_blocked` | gauge | — | si el umbral de recursos está bloqueando a los productores (1=bloqueando) |
| `speedmq_connections` | gauge | — | número actual de conexiones |
| `speedmq_channels` | gauge | — | número actual de canales |
| `speedmq_queues` | gauge | — | número actual de colas |
| `speedmq_exchanges` | gauge | — | número actual de exchanges |
| `speedmq_consumers` | gauge | — | número actual de consumidores |
| `speedmq_queue_messages` | gauge | — | total **global** de mensajes listos |
| `speedmq_queue_messages_unacknowledged` | gauge | — | total **global** de mensajes sin confirmar |
| `speedmq_process_memory_bytes` | gauge | — | memoria **en uso** del proceso (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | memoria física total |
| `speedmq_disk_free_bytes` | gauge | — | espacio disponible en el directorio de datos |
| `speedmq_memory_high_watermark` | gauge | — | proporción del umbral de memoria |
| `speedmq_disk_free_limit_bytes` | gauge | — | límite inferior de espacio libre en disco |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | número de mensajes listos de una cola |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | número de mensajes sin confirmar de una cola |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | número de consumidores de una cola |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | estimación del uso de memoria de una cola |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | número acumulado de mensajes recibidos por la cola |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | número acumulado de mensajes entregados por la cola |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | número acumulado de mensajes confirmados por la cola |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | metadatos del plugin; value siempre 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | si el plugin está en servicio (1=enabled, 0=otros) |

### 3.1 Precauciones de uso (para evitar errores)

- **Dos familias de series con el mismo nombre y distinta cardinalidad**: `speedmq_queue_messages_unacknowledged` tiene **tanto** una serie global sin etiquetas
  **como** series por cola con etiquetas; en cambio, en el lado de "listos", la global se llama `speedmq_queue_messages` y la de por cola
  `speedmq_queue_messages_ready` (nombres asimétricos). Al escribir reglas, usa `{queue=~".+"}` para tomar explícitamente solo la familia por cola.
- **El criterio de `speedmq_process_memory_bytes`**: la implementación es `MemStats.HeapInuse + StackInuse` (**memoria en uso**),
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
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | el objetivo de captura es totalmente inalcanzable | revisa el proceso/puerto/red/autenticación; reinicia y consulta el registro de arranque |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | la captura funciona pero el proceso se declara no vivo | elemento de red de seguridad; revisa el registro de salida anómala |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | plugin disabled/failed/down | usa `speedmqctl plugins show <name>` para ver `runtime_note`; los plugins externos con `restart=always` normalmente se autorecuperan |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | se activan los umbrales de memoria/disco y los productores quedan bloqueados | revisa el umbral de memoria y el espacio libre en disco; confirma si los consumidores avanzan |
| `SpeedMQMemoryWatermarkHigh` | proporción de memoria en uso > 0.9×umbral 10m | se aproxima al umbral de memoria | reduce el backlog / aumenta la velocidad de consumo para evitar activar el bloqueo |
| `SpeedMQDiskFreeLow` | espacio libre < 1.5×límite inferior de disco 10m | el directorio de datos está a punto de llenarse | amplía/limpia; al alcanzar el límite inferior se bloquearán los productores |
| `SpeedMQQueueBacklogGrowing` | listos >10000 y crecimiento monótono durante 15m | la cola acumula backlog de forma continua | amplía los consumidores / revisa el lado consumidor; investiga anomalías de mensajes muertos/TTL |
| `SpeedMQQueueNoConsumers` | consumidores=0 y hay mensajes listos durante 15m | nadie consume | revisa el proceso del lado consumidor; confirma que los consumidores no se han desconectado |
| `SpeedMQUnackedPileUp` | sin confirmar >1000 15m | los consumidores están atascados / no hacen ack | revisa la lógica de procesamiento de los consumidores y el prefetch; si es necesario, cierra la conexión y reentrega |
| `SpeedMQConnectionSpike` | conexiones >10000 10m | número de conexiones anómalo | revisa fugas de conexiones; los clientes deberían reutilizar las conexiones |

> Los umbrales (10000 / 1000, etc.) son **valores de partida**; ajústalos según el tamaño de tus colas y las características de tu negocio.

---

## 5. Vacío conocido: actualmente no hay métricas para "el clúster pierde la mayoría / no hay líder"

- **Hecho**: `/metrics` **no** tiene ninguna métrica de tipo clúster (no hay `speedmq_cluster_*`). El estado del clúster solo está en el JSON de `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Por lo tanto**, `prometheus-alerts.yml` **omite deliberadamente** las alertas basadas en métricas de clúster; aunque se escribieran, **nunca se activarían**
  (Prometheus no da error porque no exista un nombre de métrica), y eso sería una entrega que "parece correcta pero es inútil".
- **Soluciones propias** (elige una; ambas requieren que las montes fuera de SpeedMQ y quedan fuera del alcance de este repositorio):
  1. usar un exportador JSON genérico para capturar `/api/cluster`, mapearlo a métricas personalizadas (p. ej. `speedmq_cluster_has_quorum`) y luego alertar sobre esa métrica;
  2. usar un script de sonda que llame periódicamente a `/api/cluster` y emita una alerta cuando `has_quorum=false` o `paused=true`.
- Criterio de umbral relacionado: `has_quorum=false` indica pérdida de contacto con la mayoría; con `pause_minority` (predeterminado), en ese momento **el servicio se suspende y se desconectan las conexiones**.

---

## 6. Otros elementos **no verificados**

- El panel de Grafana **no se validó importándolo en un Grafana real** (solo pasó la validación de sintaxis JSON).
- Las reglas de alerta **no se validaron cargándolas en un Prometheus/Alertmanager real** (no se levantó Prometheus en esta máquina).
  No obstante, los **nombres de métrica de las reglas se contrastaron uno a uno con la salida real de `/metrics`** (véanse §2/§3), por lo que no existe el problema de "un nombre mal escrito que impide que se active nunca".
