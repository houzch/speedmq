# Plan de actualización y migración de SpeedMQ

> Versión aplicable: `1.0.0` (`broker.Version`, véase `speedmq_build_info` de `/metrics`).
> Todas las conclusiones "medidas" de este documento provienen de ejecuciones reales en esta máquina; las que no se midieron se marcan explícitamente como **【No verificado】**.
> Entorno de esta máquina: Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` temporal + puertos no predeterminados.

---

## 1. Migración (de RabbitMQ a SpeedMQ)

Este proyecto se posiciona como **compatible a nivel de protocolo con AMQP 0-9-1**, por lo que la "migración" consiste principalmente en **cambiar la dirección de conexión**:

- Cero cambios en el código de negocio; solo se cambia `host/port/vhost` (documento de diseño G3 «migración sin coste»).
- La cadena de herramientas de administración (`rabbitmqadmin`, la UI de administración, los scripts de monitorización) solo tiene que apuntar al puerto del plano de administración; la forma de las interfaces está alineada con RabbitMQ (se adoptan convenciones como `amq.default`, `%2F`, `{error, reason}`).
- Los puertos predeterminados coinciden con los de RabbitMQ: AMQP `5672`, plano de administración `15672`; MQTT es `1883` y el RPC entre nodos es `25672`.

**Diferencias semánticas que hay que revisar antes de migrar** (todas son intencionadas en este repositorio, según el README / el documento de diseño):

| Elemento | Comportamiento de SpeedMQ | Impacto en la migración |
| --- | --- | --- |
| Colas transitorias (ni durables ni exclusivas) | **Rechaza la declaración** (541); `auto_delete` no exime | Los clientes antiguos que dependan de este tipo de colas fallarán; hay que cambiarlas a durable o exclusive |
| vhost predeterminado `/` | **No se puede eliminar** (400); RabbitMQ sí lo permite | Los scripts de automatización que eliminen el vhost predeterminado fallarán (esta es la única restricción de seguridad activa) |
| Datos de colas clásicas | **No se replican**; los datos solo están en el nodo Owner | Si necesitas redundancia entre nodos, cambia a colas de quórum con `x-queue-type=quorum` |
| Colas de quórum | Admite ampliar réplicas, **no admite reducirlas** | Planifica de una vez para el tamaño definitivo |
| Plugins | Sin ecosistema de plugins de Erlang; AMQP 1.0 / STOMP no están implementados | Los escenarios que usen estos protocolos aún no se pueden migrar |

**Migración de datos**: los formatos de almacenamiento de SpeedMQ y RabbitMQ son incompatibles, y **no se proporcionan herramientas de traslado de datos en línea ni fuera de línea**.
El modo de migración es "crear un SpeedMQ vacío → validación en doble ejecución → corte de tráfico gradual". **【No verificado】** Este documento no contiene ningún ensayo real de traslado de datos de RabbitMQ.

---

## 2. Principios generales de actualización

1. **Hacer copia de seguridad primero** (véase `backup-restore.md`): la red de seguridad ante un fallo de actualización.
2. **Detener el proceso antes de reemplazar** (el directorio de datos tiene la restricción de un único escritor, véase §4.2).
3. **Es obligatorio verificar tras la actualización**: que el proceso arranque, que `/api/overview` se pueda leer, que `/metrics` se pueda capturar y que el número de mensajes de las colas coincida con el previo a la copia.
4. La actualización del clúster es **rodante nodo a nodo**, moviendo solo un nodo a la vez (véase §5).

---

## 3. Distribución del directorio de datos (base fáctica para la actualización/migración)

Distribución de `data_dir` **medida** en una instancia de nodo único en esta máquina:

```
data/
├── meta/
│   ├── state.json        # instantánea de metadatos en modo de nodo único (vhost/exchanges/colas/bindings/usuarios/permisos/políticas)
│   ├── users.seeded      # marca de arranque inicial: los users de la configuración ya se sembraron
│   ├── vhosts.seeded     # marca de arranque inicial: los vhosts de la configuración ya se sembraron
│   ├── raft.state        # 【modo clúster】mandato/voto de Raft
│   ├── raft.log          # 【modo clúster】registro de Raft
│   └── snapshot.json     # 【modo clúster】instantánea de Raft + tabla de miembros
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # archivo de segmento (cuerpo del mensaje + propiedades); formato de registro: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # índice de cola: seq-id → (número de segmento, desplazamiento en el segmento, longitud, estado)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【clúster】cada cola de quórum tiene un grupo Raft propio (registro/instantánea)
```

**Atención (dos puntos que difieren de la intuición; ambos se basan en el código/mediciones)**:

- En modo clúster, los archivos de persistencia de Raft **se colocan directamente bajo `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  y **no existe el subdirectorio `meta/raft/`**. Base: las constantes de nombres de archivo de `internal/raft/log.go` más `Dir: filepath.Join(b.cfg.DataDir, "meta")` en `internal/broker/cluster.go`. **【Verificado（2026-10-10, clúster de 3 nodos de Docker Desktop local）】** —— El contenido de la copia del volumen de un nodo del clúster muestra directamente `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (no se observa el subdirectorio `meta/raft/`).
- Los nombres de directorio **no son los nombres originales de vhost / cola**, sino una codificación de `store.SafeDirName`: se añade el prefijo `q_` y los bytes que no son `[A-Za-z0-9._-]` se escapan como `%XX`.
  Medido: vhost `/` → directorio `q_%2F`; cola `persist.q` → directorio `q_persist.q`.
  Este diseño busca evitar el path traversal y los nombres de dispositivo reservados de Windows (`con`/`nul`, etc.).

---

## 4. Compatibilidad de datos

### 4.1 ¿Se pueden leer directamente los datos antiguos? Sí

- **El formato de índice es compatible hacia delante**: M8-1 añadió el campo "número de segmento" (25 bytes) al registro de índice; **el formato antiguo (21 bytes, sin número de segmento) aún se puede leer tal cual**,
  y al leerlo equivale a "solo hay un segmento (seg=1)"; **la actualización no requiere scripts de migración**.
  Base: las constantes `indexEntrySize` / `legacyIndexEntrySize` y la lógica de `recover()` en `internal/store/store.go`; README M8-1.
- **La semántica de fallos no cambia**: cada registro lleva prefijo de longitud + CRC32; al recuperar, **se descartan los registros a medio escribir o dañados al final** y se trunca.
  Medido (véase `backup-restore.md` §6): tras detener el proceso y reiniciarlo, los 5 mensajes persistentes de la cola durable **se recuperan todos**,
  y en el registro aparece `已从磁盘恢复队列消息 ... messages=5`.

**【Verificado（2026-10-10, máquina local）】** Se ha medido la lectura entre versiones mayores: los datos de una cola durable escritos con el tag `v1.1.0` se recuperan tal cual bajo `1.1.05` (comandos y salida, véase §5).

### 4.2 El criterio de `vhosts` / `users` en la configuración (el error más fácil de cometer al actualizar)

- Ambos **solo tienen efecto en el arranque inicial**: el primer arranque escribe los vhosts/users de la configuración en los metadatos y deja los archivos de marca
  `meta/vhosts.seeded` / `meta/users.seeded`; **a partir de entonces manda los metadatos**.
- Por lo tanto, **al actualizar/cambiar la configuración, no esperes poder añadir o eliminar cuentas o vhost modificando el archivo de configuración**; aunque lo cambies, no tendrá efecto;
  usa la API de administración o `speedmqctl`.
- A la inversa, la actualización **no** sobrescribe con la configuración las cuentas existentes: una contraseña cambiada en tiempo de ejecución no vuelve al valor antiguo de la configuración al reiniciar,
  ni una cuenta eliminada en tiempo de ejecución reaparece. Base: la lógica de marcas de siembra de `cluster.go`; README M8-4 / M8-7.

**【Verificado（2026-10-10, máquina local）】** Los cambios en tiempo de ejecución efectivamente no son sobrescritos por la configuración: antes de la actualización se cambió por la API de administración la contraseña de `guest` a `rotated`; tras reiniciar con `1.1.05` + el mismo `data_dir`, `guest/guest` (la contraseña antigua de la configuración) devuelve **401** y `guest/rotated` devuelve **200**.

### 4.3 Rotación de segmentos y recuperación de espacio en disco

- Los mensajes se dividen en segmentos por tamaño (8 MiB de forma predeterminada); **cuando todos los mensajes de un segmento han sido ack y el segmento se ha cerrado, el segmento entero se elimina**, y el índice se comprime y reescribe en consecuencia.
- La actualización no modifica este comportamiento; los archivos de un solo segmento dejados por instancias antiguas siguen funcionando con la nueva lógica de rotación de segmentos.

---

## 5. Actualización del binario (máquina física)

> **【Verificado（2026-10-10, máquina local）】** Prueba real entre versiones mayores: el binario antiguo construido con el tag `v1.1.0` escribió exchange/cola/bindings durable + 5 mensajes persistentes + vhost/cuenta/permisos/políticas y cambió la contraseña en tiempo de ejecución; tras detener el proceso se arrancó con el binario actual `1.1.05` + **el mismo `data_dir` + el mismo puerto** —— el proceso arranca con normalidad, los objetos y el número de mensajes de `/api/queues` coinciden, un cliente AMQP real recupera los 5 mensajes persistentes y `/metrics` reporta `speedmq_build_info{version="1.1.05"}`. Los pasos siguientes siguen siendo un procedimiento genérico aplicable a cualquier par de versiones adyacentes; **degradación**, véase §8.

### 5.1 Pasos

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) detener el proceso (una salida ordenada hace el volcado final a disco; véase la "consistencia" de §4)
#    si se ejecuta en primer plano: Ctrl+C; si se ejecuta como servicio: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) hacer copia de seguridad del directorio de datos (siempre después de detener el proceso)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) reemplazar el binario (colocar los speedmqd.exe / speedmqctl.exe de la nueva versión en la ruta original)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) arrancar
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) verificar: proceso vivo + API de administración legible
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Lista de verificación posterior a la actualización

- En el registro de arranque aparecen `SpeedMQ 启动中 ... version=<新版本>` y `管理面已启动`;
- Los `object_totals` / `queue_totals` de `/api/overview` coinciden con los previos a la copia (compárese con `backup-restore.md` §5);
- Los `messages` / `messages_ready` de cada cola durable en `/api/queues` coinciden con los previos a la copia;
- `/metrics` se puede capturar y `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Actualización de la imagen (contenedor)

La imagen ocupa unos 13 MB (binario enlazado estáticamente + alpine), **se ejecuta como no root (uid 10001)** y el directorio de datos se monta en `/var/lib/speedmq`.

```powershell
# 1) descargar/construir la nueva imagen (usa la nueva versión como tag para evitar confundir old/new)
docker build -t speedmq:1.0.0 .

# 2) detener el contenedor antiguo (compose conserva el volumen con nombre speedmq-data)
docker compose down

# 3) arrancar la nueva versión (cambia image al nuevo tag en el archivo compose)
docker compose up -d

# 4) estado y registros
docker compose ps
docker compose logs -f --tail 100
```

> **Tareas puntuales dentro del contenedor** (por ejemplo, ejecutar `speedmqctl` dentro del contenedor): el `run` de `docker compose ...` debe llevar `-T` en entornos no interactivos,
> de lo contrario fallará al solicitar una TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

La persistencia de datos depende del **volumen con nombre** `speedmq-data` de compose; al reconstruir el contenedor no se pierden datos (desde M4 realmente se escribe en disco).
Si necesitas hacer copia de seguridad del contenido del volumen antes de actualizar, equivale a respaldar `/var/lib/speedmq` (véase `backup-restore.md` §3.2).

> **【Verificado（2026-10-10, Docker Desktop local）】** Se realizó un ensayo real de actualización de imagen: se construyeron localmente la imagen antigua `speedmq:1.1.0-old` (git tag `v1.1.0`) y la nueva `speedmq:1.1.05-new` (código actual); con un compose de un solo nodo se arrancó la imagen antigua → se creó la cola durable `persist.q` y se publicaron 5 mensajes persistentes (`delivery_mode=2`) → `docker compose down` (**sin `-v`**, conservando el volumen con nombre `speedmq-data`) → tras cambiar el tag de image en compose a la nueva imagen, `docker compose up -d`.
> Resultado: el nuevo contenedor arranca con normalidad, en el log de arranque aparece `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` reporta `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` de `/api/overview` y `messages=5` de la cola `persist.q` coinciden antes y después de la actualización, y un cliente AMQP real (`rabbitmq/amqp091-go`) recupera uno a uno los 5 mensajes persistentes (`deliveryMode=2`).
> Nota: `v1.1.0` es la imagen de la marca `speedmq` anterior al cambio de nombre (binario `speedmqd`, métrica `speedmq_build_info`, directorio de datos predeterminado `/var/lib/speedmq`), con una nomenclatura distinta a la versión actual; en el ensayo se tomó como referencia la configuración `data_dir=/var/lib/speedmq` y se preajustó el propietario del volumen con nombre (en la imagen antigua no existe esa ruta escribible). Esto no afecta a la conclusión "reconstruir el contenedor cambiando el tag no pierde los datos del volumen con nombre".

---

## 7. Despliegue gradual y reversión

### 7.1 Nodo único

- **Despliegue gradual**: SpeedMQ en nodo único no incorpora la capacidad de "dos versiones antigua y nueva en el mismo proceso". Un despliegue gradual viable es la **sombra por derivación**:
  la instancia nueva se conecta primero al mismo tráfico de origen con **consumo de solo lectura / colas sombra** para observarla y, una vez confirmado que todo va bien, se cambia el emisor de escritura.
- **Reversión**:
  1. detener el proceso de la nueva versión;
  2. volver a poner el binario antiguo;
  3. si la nueva versión ya escribió datos, **es obligatorio restaurar `data_dir` desde la copia previa a la actualización** (véase más abajo).
  **No** existe garantía de que "la nueva versión haya escrito y la antigua lo lea directamente"; para la degradación entre versiones, véase §8.

### 7.2 Clúster (actualización rodante)

La plataforma no ofrece una "actualización rodante con un solo clic"; hay que operar manualmente nodo por nodo en el siguiente orden:

1. **Actualizar solo un nodo a la vez**: detener ese nodo → hacer copia de su `data_dir` → cambiar el binario → arrancar → esperar a que se reincorpore y se ponga al día
   (consulta `speedmqctl cluster_status` / `GET /api/cluster` para ver `role`, `commit_index`/`last_applied`).
2. **Orden recomendado**: actualizar primero los **learner / miembros no votantes** (sin impacto en la mayoría), luego los **follower** y por último el **leader**
   (actualizar el leader provoca una elección de líder, durante la cual hay un breve periodo de no escritura).
3. **Impacto de las paradas en la mayoría** (clave):
   - Clúster de 3 nodos: **como máximo se puede detener 1** miembro votante a la vez; detener 2 hace perder la mayoría y, con `pause_minority`, **todo el clúster suspende el servicio**.
   - Clúster de 2 nodos: detener 1 ya hace perder la mayoría; **no tiene capacidad de actualización rodante** (se recomiendan al menos 3 nodos).
   - Por lo tanto, durante la actualización rodante **está terminantemente prohibido detener varios miembros votantes a la vez**.
4. **No hacer cambios de miembros y actualización a la vez**: los cambios de miembros **no tienen joint consensus**; solo se permite un cambio de configuración no confirmado a la vez;
   durante la actualización, evita ejecutar `add_member` / `remove_member` al mismo tiempo.
5. Una vez completada la actualización, verifica que los `object_totals` de `GET /api/cluster` coincidan con los previos a la actualización.

> **【Verificado（2026-10-10, clúster de 3 nodos de Docker Desktop local）】** Se realizó un ensayo real de actualización rodante **entre versiones** de un clúster: la base fue `speedmq:1.1.0-old` construida localmente (git tag `v1.1.0`), actualizando nodo a nodo (n1 → n2 → n3, de uno en uno: primero detener, luego respaldar su volumen con nombre, luego cambiar la imagen y arrancar) hasta el actual `1.1.05` (construido localmente `speedmq:1.1.05-new`, compose cambiaba la imagen con un override por nodo). Durante todo el proceso el clúster conservó siempre leader y mayoría —— al detener el leader actual se eligió un nuevo leader en unos 0,6 s, `pause_minority` no provocó suspensión y no hubo interrupción externa durante la parada; tras la actualización los `object_totals` de `GET /api/cluster` coinciden con los previos (`queues:1`), el recuento de los 25 mensajes persistentes de la cola de quórum de 3 réplicas `qroll` no cambia y son recuperables uno a uno, y tras publicar 5 mensajes nuevos los 30 en total son recuperables.
> El orden anterior sigue cumpliendo las restricciones generales de Raft; este ensayo cubre el escenario de 3 nodos. **【Verificado（2026-10-10, Docker Desktop local）2 nodos: detener 1 ya pierde la mayoría】** Se creó además un clúster mínimo de 2 nodos (n1/n2, cuyos `peers` solo contienen esos dos, `partition_policy=pause_minority`) y se midió: con ambos nodos en línea, `GET /api/cluster` es `has_quorum=true / paused=false` (n1 leader, n2 follower); tras `docker stop` de n2, n1 pasa en unos 2 s a `has_quorum=false / paused=true` —— es decir, detener 1 miembro votante ya pierde la mayoría y `pause_minority` suspende el servicio, **confirmando que 2 nodos no tienen capacidad de actualización rodante**; tras `docker start` para restaurar n2, n1 vuelve en unos 2 s a `has_quorum=true / paused=false` (el servicio se recupera automáticamente).

> **【Verificado（2026-10-11, clúster de 3 nodos de Docker Desktop local）】La actualización rodante bajo partición de red ya se midió** (entre versiones `v1.1.0` → `1.1.05`; la partición usó una topología de doble red: el RPC Raft entre nodos solo va por `cluster-net` con IP estáticas, el tráfico de clientes/interfaz de gestión va por `client-net`, y `docker network disconnect/connect` conmuta la partición):
> - **Follower particionado**: el lado mayoritario conservó el quórum durante toda la partición, publicó/consumió con normalidad y completó el "detener → cambiar imagen → arrancar → ponerse al día" de un nodo mayoritario mientras la partición persistía; el clúster de versiones mixtas (1.1.0 + 1.1.05) siguió dando servicio. Al restablecerse la partición, el nodo aislado (versión antigua) se reincorporó y se puso al día; la reconciliación de mensajes por conjunto de ID únicos dio 35/35 sin pérdidas.
> - **Leader particionado**: la mayoría eligió un nuevo leader en unos **630 ms**; el antiguo leader aislado mantuvo `role=leader` pero con `has_quorum=false / paused=true` (`pause_minority` en vigor, sin elevar proactivamente su mandato); la actualización de otro nodo del lado mayoritario durante la partición tuvo éxito; tras el restablecimiento, el antiguo leader se reincorporó sin split-brain ni divergencia de datos.
> - **Comportamiento esperado sin PreVote (registrado con honestidad)**: el follower aislado inició de inmediato campañas repetidas, con el mandato subiendo unas +2 cada 2 s (los dos grupos Raft, de metadatos y de cola, elevándose alternadamente); en el instante del restablecimiento **se hizo con el liderazgo** con su mandato más alto (elegido en 1,4 s en el ensayo) — hay perturbación por cambio de leader, pero ningún problema de corrección.
> - **Dos trampas operativas**: ① si `peers` usa nombres de host y los contenedores están además conectados a una segunda red alcanzable, el tráfico Raft se desvía y provoca una "falsa partición" (el nodo aislado seguía mostrando `has_quorum=true` tras más de un minuto) — el RPC entre nodos debe ir por una única red (`peers` apuntando directamente a las IP estáticas de esa red); ② al restablecer, `docker network connect` debe llevar `--ip <IP original>`, de lo contrario el nodo obtiene una IP nueva mientras la mayoría sigue marcando la dirección antigua, y el nodo aislado —con un registro atrasado— hace campañas repetidas con un mandato inflado (en el ensayo arrastró el term del clúster de 320 a 488 y el commit de metadatos de 13 a 97), pudiendo aparecer un estado transitorio en el que el leader muestra `quorum=true` pero brevemente `paused=true`.
> - Durante la partición, la API de gestión del lado minoritario siguió siendo legible (útil para observar), mientras que la publicación AMQP era rechazada (`pause_minority` en vigor).

---

## 8. Partes no compatibles / no verificadas (enumeradas explícitamente)

- **Degradación entre versiones mayores: no compatible, no verificada**. Si la nueva versión ya escribió datos con el nuevo formato/nueva semántica, **no** hay garantía de "volver al binario antiguo y leer tal cual";
  la reversión solo puede basarse en la copia previa a la actualización.
- **El formato de configuración no cambia**: sigue siendo JSON + variables de entorno `SPEEDMQ_*`. **La configuración YAML aún no es compatible** (requiere introducir una dependencia de análisis; M8-17 pendiente de evaluación),
  la actualización no traerá YAML.
- **Actualización en caliente de plugins/protocolos en línea**: los plugins se compilan con el núcleo (forma A) o se lanzan mediante `spawn` según la configuración (forma B);
  actualizar el núcleo = reiniciar el proceso; **no** hay mecanismo de sustitución en caliente del binario in situ.
- **Migración in situ del motor de almacenamiento**: la rotación de segmentos y la compresión del índice son comportamientos en segundo plano en tiempo de ejecución; **no** hay un comando independiente de "migración/compresión de datos".
- **Actualización de clúster con disco lleno**: **no verificada**. (La actualización rodante bajo partición de red se midió el 2026-10-11; véase la nota al final de §7.)
- Este documento **no incluye** ninguna validación de traslado de datos entre SpeedMQ y otros brokers (RabbitMQ).
