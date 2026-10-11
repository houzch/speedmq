# Copia de seguridad y restauración de SpeedMQ

> Todas las conclusiones "medidas" de este documento provienen de un ensayo real sobre **Windows + PowerShell 5.1** (con `data_dir` y puertos temporales).
> Los comandos del ensayo y las salidas clave se incluyen tal cual en §6. Las partes marcadas **【No verificado】** se señalan de forma explícita (p. ej. un corte de corriente físico real).

> **【Verificado (2026-10-11, clúster de 3 nodos en Docker Desktop local)】** La muerte forzada de procesos (`docker kill`) y las escrituras concurrentes durante la restauración ya se han medido;
> los registros están en §6.2 / §6.3; la formulación sobre el "corte de corriente" de §2.3 y la lista de estados de verificación de §7 se han actualizado en consecuencia.

---

## 1. Qué hay que respaldar

Hay que **respaldar por completo** el contenido de `data_dir`; lo clave es lo siguiente (para la distribución, véase `upgrade.md` §3):

| Ruta | Función | Qué ocurre si se pierde |
| --- | --- | --- |
| `meta/state.json` | Instantánea de metadatos en modo de nodo único: vhost / exchanges / colas / bindings / usuarios / permisos / políticas | Se pierden toda la topología y las cuentas |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【Clúster】registro de Raft / voto de mandato / instantánea + tabla de miembros | Se pierden la identidad del clúster y la consistencia de los metadatos |
| `meta/users.seeded`, `meta/vhosts.seeded` | Marcas de arranque inicial | Si se pierden, los users/vhosts de la configuración se **siembran de nuevo** (las cuentas/vhost eliminados reaparecen) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Datos e índice de mensajes de colas clásicas | Se pierden los mensajes persistentes |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Clúster】registro/instantánea de Raft de las colas de quórum | Se pierden los datos de las colas de quórum |
| Archivos de certificado (los PEM a los que apuntan `cert_file`/`key_file`/`ca_file` en la configuración) | Certificados TLS | Se respaldan por separado de `data_dir`; tras reiniciar, TLS no arranca |

> El estado blando (mensajes sin confirmar, consumidores, contadores de prefetch) **solo está en memoria** y no se escribe en disco; la copia de seguridad **no los incluye** ni debería incluirlos.

---

## 2. Requisito de consistencia: **es obligatorio detener el proceso primero**; la copia en caliente **no es segura**

### 2.1 Conclusión

- ✅ **Práctica segura**: **detener el proceso broker** (una salida ordenada realiza el volcado final a disco) y luego copiar `data_dir`.
- ❌ **Copia en caliente (copiar archivos directamente con el proceso en ejecución): no es segura, no se garantiza.**

### 2.2 Por qué la copia en caliente no es segura

El almacenamiento de mensajes son **dos archivos** (el archivo de segmento `*.seg` y el archivo de índice `index/*.idx`), y ambos **no se confirman de forma atómica**:

- Al recuperar, **se toma el índice como referencia** para determinar "qué mensajes están vivos" y, a continuación, se lee del archivo de segmento según `(número de segmento, desplazamiento, longitud)` del índice.
- Durante la copia en caliente se puede capturar un estado intermedio en el que **el índice ya hace referencia a un registro que el archivo de segmento aún no ha escrito por completo** (o al contrario):
  - El índice hace referencia a un registro que no existe en el segmento → ese mensaje **falla al leerse y se omite** (equivale a perder un mensaje persistente ya confirmado);
  - El segmento contiene un registro pero el índice no lo referencia → ese mensaje **no se recupera**.
- Aunque la recuperación usa CRC32 para descartar **registros a medio escribir al final**, eso solo cubre "escrituras dañadas al final de un único archivo" y **no puede reparar la desincronización entre el índice y el segmento**.

### 2.3 Sobre "cuándo se escriben los datos en disco" (observación medida)

- Con los valores predeterminados `fsync: os` + `flush_interval_ms: 200`: la corrutina de volcado en segundo plano hace `write()` del mensaje al sistema operativo en **como máximo unos 200 ms**
  (sin fsync), y el publisher confirm también se devuelve después de esto.
- Medido: tras publicar un mensaje persistente, si se consulta **inmediatamente** el tamaño del archivo de segmento ya se ven los datos (`t=0ms seg=832`); es decir, "los bytes visibles para el SO" van prácticamente sincronizados con el confirm.
- **Atención**: esto solo demuestra que "llegó al búfer del SO"; **matar el proceso a la fuerza no provoca pérdida** (si se mata el proceso no se pierde el búfer del SO), pero **un corte de corriente sí provoca pérdida**.
  Si quieres que "al recibir el confirm ya esté fsync en disco", cambia `storage.fsync` a `batch` / `always`.
  **【Verificado (2026-10-11, clúster de 3 nodos en Docker Desktop local)】** Matar un nodo a la fuerza con `docker kill` (SIGKILL) durante la escritura se midió en ambos niveles sin perder mensajes confirmados:
  `fsync: os` matando un follower (50.000 mensajes todos confirmados, reconciliación missing=0), `fsync: always` matando el líder de la cola quorum (200.000 mensajes todos confirmados,
  missing=0; además, 1.355 mensajes ya escritos en disco pero no confirmados fueron reenviados, lo que la semántica at-least-once permite; basta con deduplicar por ID de mensaje en el consumidor para que sea idempotente).
  Extra: con el nivel predeterminado, `wsl --shutdown` detuvo bruscamente toda la VM de Docker durante la publicación (se aproxima a un corte de corriente de toda la máquina con pérdida del búfer del SO); tras la recuperación, los 69.669 mensajes confirmados
  se reconciliaron ID a ID con missing=0. **Aún no verificado**: un corte de corriente físico real (pérdida de alimentación del host, capa de caché de disco), que no se puede simular en esta máquina.

---

## 3. Pasos de copia de seguridad

### 3.1 Nodo único (recomendado)

```powershell
# 1) detener el proceso (en primer plano: Ctrl+C; en segundo plano: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) copiar todo el data_dir (con marca de tiempo)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (opcional) verificar que la instantánea de metadatos de la copia se puede analizar
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Clúster

- **Cada nodo respalda su propio `data_dir`** (los metadatos se replican a todos mediante Raft, los datos de mensajes están en el nodo Owner y las réplicas de las colas de quórum están en el directorio Raft de cada uno).
- Orden de parada: **detener solo un nodo a la vez**; no detengas varios miembros votantes al mismo tiempo (véase `upgrade.md` §7.2).
- Para obtener una **instantánea consistente de todo el clúster**, hay que detener todos los nodos de forma secuencial y luego copiar cada uno; en producción es más habitual "detener/copiar/arrancar nodo por nodo".
- **【Verificado (2026-10-10, clúster de 3 nodos en Docker Desktop local)】** En un clúster compose de 3 nodos se hizo "detener/copiar/arrancar" nodo por nodo: el volumen con nombre de cada nodo se empaquetó con un contenedor alpine efímero
  (`docker run --rm -v <volumen>:/data -v <directorio del host>:/backup alpine tar czf ...`), y el paquete contenía `meta/raft.log`, `meta/raft.state`, `meta/*.seeded`, `quorum/...`;
  tras la restauración, ese nodo pudo reincorporarse como miembro existente y ponerse al día (véase §4.2; la misma evidencia que en `upgrade.md` §7.2).

### 3.3 Docker (volumen con nombre)

```powershell
# tras detener el contenedor, usa un contenedor efímero para empaquetar y extraer el contenido del volumen
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【Verificado (2026-10-10, Docker Desktop local)】** Este comando se ejecutó realmente sobre los volúmenes con nombre de un clúster de 3 nodos (el paquete contenía `meta/`, `quorum/`),
> y a partir de ello se completó un ensayo de restauración del volumen de un solo nodo (véase §4.2).

---

## 4. Pasos de restauración

### 4.1 Nodo único

```powershell
# 1) confirmar que el proceso está detenido
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) mover (o eliminar) el data_dir actual para evitar mezclar archivos nuevos y antiguos
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) restaurar desde la copia de seguridad
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) arrancar
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Puntos clave:
- **Es obligatorio mover primero el directorio antiguo**; no se puede "sobrescribir con los archivos de la copia un directorio a medio eliminar";
- El `data_dir` restaurado debe tener **el mismo conjunto de vhost/colas** que en el momento de la copia (los nombres de directorio están codificados, por lo que sirven entre máquinas);
- **No** aproveches la restauración para modificar `vhosts`/`users` en el archivo de configuración (solo tienen efecto en el arranque inicial; cambiarlos no sirve, véase `upgrade.md` §4.2).

### 4.2 Clúster

- Restaurar un solo nodo: tras restaurar el `data_dir` de ese nodo según §4.1, arráncalo; se reincorporará como miembro existente y se pondrá al día con el registro de Raft.
- Restaurar todo el clúster: **restaura y arranca primero los nodos de la mayoría** (≥ la mitad de los miembros votantes) para que el clúster pueda elegir un líder; después restaura los demás nodos.
- **【Verificado (2026-10-10, clúster de 3 nodos en Docker Desktop local)】** Medición de "detener n3 → vaciar su volumen → sobrescribir con la copia de §3.3 → reiniciar":
  n3 se reincorporó como follower y se puso al día con el líder (`commit_index == last_applied`); el número de mensajes de las colas de quórum y la tabla de miembros se mantuvieron sin cambios.

---

## 5. Métodos de verificación tras la restauración

Verifica de forma cruzada con la API de administración y clientes reales (se recomienda hacerlo todo):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) totales de objetos y de mensajes (nº de colas/exchanges/bindings/usuarios; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) verificar cola por cola messages / messages_ready (se puede comparar con el registro previo a la copia)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) comprobar que están todos los vhost / usuarios / políticas
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) clúster (en nodo único devuelve enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Revisa el registro de arranque**: deben aparecer `已从磁盘恢复队列消息 ... messages=N` y `队列已恢复持久化消息 ... messages=N`; N debe coincidir con el valor previo a la copia.
- **Avisos en el registro**: si una cola tuvo mensajes consumidos/vaciados antes, durante la restauración pueden aparecer
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` y `恢复时清理了无存活消息的段`.
  Estos son residuos de índice de **registros ya liquidados (ack/purge)**; se trata de **ruido de registro conocido que no afecta a la corrección de los datos** (véase §7).
- **Cliente real**: recupera mensajes de la cola y verifica la cantidad/contenido (véase el paso (7) de §6).

---

## 6. Ensayo medido (comandos y salidas reales)

> Entorno: `data_dir` en un directorio temporal, AMQP `127.0.0.1:5676`, plano de administración `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> cuenta predeterminada `guest/guest`. Registro de arranque:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Crear topología durable + publicar 5 mensajes persistentes (cliente real `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Crear usuario / vhost / permisos / políticas (API de administración)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Estado previo a la copia (API de administración)**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) Archivos en disco previos a la copia**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Detener el proceso → copia → vaciado → restauración**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Registro de recuperación tras reiniciar (líneas clave)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Aparecen además varias líneas `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> son residuos de índice dejados por mensajes **previamente purgados** en este ensayo (ya liquidados, datos de segmento ya recuperados); **no afectan a la recuperación de los 5 mensajes siguientes**.

**(7) Aserciones posteriores a la restauración: API de administración + cliente real**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**Conclusión**: la topología durable (exchange + cola + binding), los 5 mensajes persistentes, el usuario, el vhost, los permisos y las políticas **se restauraron por completo**,
y el cliente real pudo recuperar todos los mensajes tal cual. **Ensayo superado.**

### 6.1 Control: la restauración de una cola sin consumo/purge es más "silenciosa"

Para distinguir si los WARN anteriores son un fenómeno general, se realizó un **control** aparte: se creó la cola durable `clean.q`, se publicaron 3 mensajes persistentes
**sin consumir ni vaciar**, y se detuvo el proceso y se reinició:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Sin ningún WARN**. Esto indica que los WARN solo aparecen en el escenario en que "el índice todavía conserva registros ya liquidados" (véase §7).

### 6.2 Muerte forzada de un nodo durante la escritura (`docker kill`, 2026-10-11)

Clúster de 3 nodos (`partition_policy=pause_minority`), cola quorum de 3 réplicas `pwr.q`; el cliente activa publisher confirms, cada mensaje lleva un ID único y los ID confirmados se guardan en disco para la reconciliación.

**Ronda 1 (`fsync: os`, matando el follower n3)**: `docker kill pwr-n3` mientras se publican 50.000 mensajes:

```
DONE published=50000 confirmed=50000 nacked=0 retries=0 elapsed=28.267s   # 杀后速率无可见变化
docker start pwr-n3 → 三节点 commit_index==last_applied==12，has_quorum=true
DRAIN total=50000 unique=50000 duplicates=0
confirmed_unique=50000 consumed_unique=50000 missing=0 extra=0
```

**Ronda 2 (`fsync: always`, matando el líder quorum n1)**: `docker kill pwr-n1` mientras se publican 200.000 mensajes:

```
DONE published=200000 confirmed=200000 nacked=3843 retries=3843 elapsed=2m7.494s
# nack 来自转发到已死 leader 的发布（"与节点 n1 通信失败"），客户端重投全部成功
docker start pwr-n1 → 三节点 commit_index==last_applied==16
DRAIN 对账：confirmed_unique=436785 consumed_unique=438140 missing=0 extra=1355
# extra=已落盘但 confirm 未返回的消息，at-least-once 允许，按 ID 去重即幂等
```

**Ronda 1b (`fsync: os`, `wsl --shutdown` detiene bruscamente toda la VM de Docker, lo que se aproxima a un corte de corriente de toda la máquina)**: detención brusca durante la publicación; el host había registrado 69.669 líneas confirmadas;
tras reconstruir la VM y recuperarse el clúster:

```
{"name":"pwr.q","messages":69669,"messages_ready":69669}
DRAIN total=69669 unique=69669 duplicates=0 ；missing=0 extra=0
```

Atención: `wsl --shutdown` solo se aproxima a "perder el búfer del SO"; **no equivale a un corte de corriente físico** (la pérdida de alimentación del host y la capa de caché de disco no están cubiertas y siguen sin verificar).

### 6.3 Escrituras concurrentes durante la restauración (2026-10-11)

En un clúster con `fsync: always`: `docker stop` al follower n2 → publicación acumulada de 50.000 mensajes (todos confirmados) → `docker start pwr-n2`,
y **desde el mismo segundo** se siguen publicando 20.000 mensajes hacia los nodos del lado mayoritario:

```
DONE published=20000 confirmed=20000 nacked=0 retries=0 elapsed=5.978s   # 约 3,300~6,400 条/秒
n2 日志：msg=已安装快照 component=broker index=4096 term=4              # 约 4 秒经快照追平
三节点 commit_index==last_applied==20；queue_totals.messages=82000       # 与四批发布总数精确相符
DRAIN total=82000 unique=80000 missing=0 extra=0                          # 2,000 重复为同 ID 重发批次的预期重复
```

Conclusión: las escrituras concurrentes durante la puesta al día no se pierden ni se corrompen; los recuentos son exactos.

---

## 7. Problemas y limitaciones conocidos (registrados con honestidad)

1. **Ruido en el registro de recuperación (observado realmente)**: cuando una cola ha tenido consumo/vaciado a lo largo de su historia (mensajes ya ack/purge),
   su índice todavía conserva referencias a registros ya recuperados y, durante la recuperación, se **imprime un WARN `恢复消息失败，已跳过` por cada uno**,
   y se crea/limpia un `000000.seg` vacío (registro `恢复时清理了无存活消息的段`).
   **No afecta a la corrección de los datos** (los mensajes sin confirmar que siguen vivos se recuperan correctamente), pero **contamina el registro** y, con colas grandes y alta capacidad de procesamiento (throughput), puede inundar la pantalla.
   Recomendación: toma como referencia `已从磁盘恢复队列消息 ... messages=N` e ignora estos WARN referidos a registros ya liquidados;
   si el volumen de registro no es aceptable, repórtalo a los mantenedores del núcleo (este documento no modifica el código).
2. **La copia en caliente no es segura** (§2): no copies `data_dir` directamente con el proceso en ejecución.
3. **`fsync: os` no garantiza que no haya pérdida ante un corte de corriente**: si quieres "confirm equivale a escritura en disco", usa `batch` / `always`.
4. **Contraseñas en texto claro**: en `meta/state.json` las contraseñas de usuario están en **texto claro** (en la medición se ve `"password":"drillpass"`) —
   por ello los archivos de copia de seguridad **deben tratarse como datos sensibles** (control de acceso, almacenamiento cifrado). Para más detalles, véase `security-baseline.md`.
5. **【Estado de verificación】** La muerte forzada de procesos (incluida la del líder quorum durante la escritura, en los dos niveles `fsync: os`/`always`) y las escrituras concurrentes durante la restauración (publicación a unas 4.000–6.000 msg/s durante la puesta al día, puesta al día por snapshot en 4 segundos, recuentos exactos, reconciliación missing=0) —— **【Verificado (2026-10-11, clúster de 3 nodos en Docker Desktop local)】**; `wsl --shutdown` deteniendo bruscamente toda la VM de Docker (se aproxima a un corte de corriente de toda la máquina) mostró cero pérdida de mensajes confirmados en una única muestra, pero **el corte de corriente físico real sigue sin verificar**; la copia de seguridad/restauración de clúster nodo por nodo y la copia de seguridad/restauración del volumen con nombre de Docker —— **【Verificado (2026-10-10, clúster de 3 nodos en Docker Desktop local)】** (véanse §3.2 / §3.3 / §4.2).
6. **Observaciones adicionales (registradas con honestidad durante el ensayo del 2026-10-11, no bloqueantes)**: ① tras matar a la fuerza al líder quorum, la ruta de reenvío de publicaciones de los followers siguió apuntando al líder muerto durante unos 76 segundos (las nuevas publicaciones recibieron nack durante esa ventana; los reintentos del cliente tuvieron éxito; no se perdió ningún mensaje confirmado) — existe una ventana de inestabilidad de disponibilidad; ② al vaciar de una pasada una cola grande (438.000 mensajes) se observó una amplificación de reenvíos de unas 153× (la semántica at-least-once se mantiene y finalmente quedó exactamente a cero); ③ mientras una cola grande se ponía al día, iniciar un consumidor a través de un follower provocó ocasionalmente `i/o timeout`, mientras que conectarse directamente al líder funcionaba.
