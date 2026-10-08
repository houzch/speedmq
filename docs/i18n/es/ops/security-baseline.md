# Línea base de refuerzo de seguridad de SpeedMQ (lista de verificación marcable)

> Principio: **solo se documentan capacidades que este repositorio posee realmente**. Cada punto indica "por qué hacerlo + cómo verificar que se ha hecho", y todos los comandos de verificación se pueden ejecutar.
> Los puntos marcados **【Verificado】** indican que **realmente se ejecutaron** en esta máquina (Windows + PowerShell 5.1, `1.0.0`);
> **【No verificado】** indica que no se ejecutó o que actualmente no es posible; nunca se finge lo contrario.
> Todos los comandos se ofrecen en versión curl al estilo `/bin/sh`, acompañados de la versión de PowerShell (en PowerShell 5.1 usa
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Autenticación y control de acceso

### A-1. Modificar la cuenta predeterminada `guest/guest` 【Verificado】

- **Por qué**: `guest/guest` está integrado de forma predeterminada (etiqueta `administrator`); exponerlo al exterior equivale a dejar la puerta abierta.
- **Cómo hacerlo**: cambia la contraseña o elimina la cuenta en tiempo de ejecución; **no** modifiques el archivo de configuración (`users` solo tiene efecto en el arranque inicial).

```bash
# cambiar la contraseña
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# o eliminar directamente la cuenta predeterminada (asegúrate antes de haber creado el nuevo administrador)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Cómo verificar**: tras el cambio, la contraseña antigua debe devolver 401 y la nueva 200.
  **【Verificado】** Salida medida en esta máquina:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Cuenta predeterminada cambiada/eliminada

### A-2. Privilegio mínimo: expresiones regulares `configure` / `write` / `read` por vhost 【Verificado】

- **Por qué**: las tres categorías están alineadas con RabbitMQ: `configure` gobierna la declaración/eliminación de topología, `write` gobierna la publicación y los bindings, y `read` gobierna el consumo y la obtención;
  una operación no autorizada devuelve 403. A las cuentas de negocio dales solo lo que necesiten.
- **Cómo hacerlo**: `PUT /api/permissions/{vhost}/{user}`; por ejemplo, consumo de solo lectura: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Cómo verificar**: con la cuenta restringida, intenta una operación no autorizada; debe devolver 403 `ACCESS_REFUSED`.
  **【Verificado】** En esta máquina, con un cliente AMQP real, un usuario con `configure="^$"` intentó declarar un exchange; medido:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Cada cuenta de negocio recibe solo las expresiones regulares necesarias y no tiene las etiquetas `administrator`/`management`

### A-3. El criterio de la etiqueta `administrator` (permisos totales implícitos) — concédela con cautela 【Verificado】

- **Por qué**: **un usuario con la etiqueta `administrator` tiene permisos totales sobre todos los vhost que ve, sin necesidad de registros de permisos**
  (alineado con el criterio medido de RabbitMQ; véase el README / diseño M8-7). Es decir, basta con dar esta etiqueta
  para que las expresiones regulares de permisos dejen de tener efecto: es el privilegio máximo.
- **Cómo hacerlo**: concede `administrator` solo a las cuentas del plano de administración/operaciones; a las cuentas de negocio no les des ninguna etiqueta y guíate solo por las expresiones regulares de permisos.

- **Cómo verificar (demostración de permisos implícitos)**: en un vhost nuevo **sin ningún registro de permisos**, el usuario `administrator` debe poder usarlo directamente.
  **【Verificado】** Medido en esta máquina: tras crear el vhost `drillvh` (sin crear ningún registro de permisos), `guest` (administrator) declaró topología en él correctamente:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] La etiqueta `administrator` se concede solo a muy pocas cuentas de operaciones

### A-4. `remote_access`: restringir una cuenta a inicio de sesión solo local 【No verificado (no se puede simular un origen remoto en la misma máquina)】

- **Por qué**: alineado con RabbitMQ, el `guest` integrado solo permite inicio de sesión local de forma predeterminada; en despliegues expuestos al exterior hay que asegurar que el origen de las cuentas privilegiadas esté restringido.
- **Cómo hacerlo / criterio (limitación importante)**:
  - `remote_access` solo se puede escribir en `users.<name>.remote_access` del **archivo de configuración** y **solo tiene efecto en el arranque inicial**;
  - **Las cuentas creadas mediante la API de administración / `speedmqctl` siempre tienen `remote_access=true`** (permiten inicio de sesión desde cualquier origen) —
    según el comentario de `UpsertUser` en `internal/broker/observe.go` y el `"remote_access":true` medido en `meta/state.json`.
    Es decir, **actualmente la API no puede restringir una cuenta a solo local**.
- **Cómo verificar**: conecta con esa cuenta desde **otra máquina** (distinta de `127.0.0.1`); debe recibir 403; la conexión local debe funcionar.
  **【No verificado】**: el entorno local no permite construir un origen remoto real; no se midió.
- [ ] La restricción de origen de las cuentas privilegiadas se ha evaluado según el criterio anterior (ten en cuenta que las cuentas creadas por API abren el acceso remoto de forma predeterminada)

---

## B. Seguridad de la transmisión (TLS)

Opciones de configuración de TLS (la capa de acceso y el plano de administración **comparten** el mismo conjunto de campos): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Habilitar TLS y rechazar el arranque si está mal configurado 【Verificado】

- **Por qué**: los certificados se leen y validan **en el arranque**, de modo que una mala configuración rechaza el arranque de inmediato en lugar de quedar al descubierto cuando se conecte el primer cliente.
- **Cómo hacerlo**: en `listeners.<plugin>[].tls` o `management.tls`, proporciona `cert_file` + `key_file` (solo se habilita si se dan **ambos**).

- **Cómo verificar**: arranca con una configuración incorrecta; debe fallar de inmediato.
  **【Verificado】** En esta máquina se midieron tres configuraciones incorrectas; todas dieron `exit=1` y rechazaron el arranque:

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Cómo verificar (directo/inverso)**: el cliente TLS puede conectar; un cliente en texto claro que conecte al puerto TLS será rechazado.
  **【Verificado】** En esta máquina se levantó una instancia TLS (`amqp091` por TLS) y se usó una sonda de cliente real:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Los puertos de protocolo expuestos al exterior tienen TLS habilitado

### B-2. `min_version` al menos 1.2 【Verificado】

- **Por qué**: deshabilita versiones de TLS demasiado antiguas; el valor predeterminado ya es `1.2`, y se puede elegir `1.2` / `1.3`.
- **Cómo verificar**: pon `min_version` como `1.0`; el arranque debe dar error (véase la salida `badtls2` de B-1).
- [ ] `min_version` es `1.2` o `1.3`

### B-3. Autenticación mutua `client_auth: require_and_verify` (mTLS) 【Parcialmente verificado】

- **Por qué**: exige que el cliente presente y valide un certificado, evitando que clientes no autorizados se conecten a los puertos de protocolo.
- **Cómo hacerlo**: configura `ca_file` + `client_auth: require_and_verify` (los dos últimos exigen proporcionar `ca_file` a la vez).
- **【Verificado】**: la vía TLS de extremo a extremo y la vía de rechazo se validaron con clientes reales (B-1). **El mTLS (exigir y validar el certificado del cliente) no se ensayó por separado en esta máquina**.
- [ ] Los puertos que necesitan mTLS tienen `require_and_verify` + `ca_file`

### B-4. TLS en el plano de administración 【No verificado】

- **Por qué**: el plano de administración transmite la contraseña mediante Basic Auth, por lo que debe cifrarse.
- **Cómo hacerlo**: `management.tls` usa los mismos campos que las escuchas de protocolo.
- **【No verificado】**: en el ensayo local el plano de administración se vinculó a un puerto local en texto claro; no se levantó HTTPS del plano de administración por separado.
- [ ] El plano de administración tiene TLS habilitado (o está estrictamente restringido a una red de confianza)

---

## C. Reducción de la superficie de exposición

### C-1. Reducción del alcance de escucha del plano de administración 【Verificado (dirección de escucha medida)】

- **Por qué**: el plano de administración escucha en `:15672` de forma predeterminada (todas las interfaces de red). En despliegues expuestos al exterior hay que vincularlo a una dirección de intranet/loopback o restringir el origen con un cortafuegos.
- **Cómo hacerlo**: configura `management.addr` como `127.0.0.1:15672` o una dirección de intranet; o desactívalo por completo con `management.enabled=false`
  (al desactivarlo no hay puerto de administración, pero `speedmqctl` también deja de estar disponible).
- **Cómo verificar**:
  **【Verificado】** En esta máquina se configuró el plano de administración como `127.0.0.1:15677` y la dirección de escucha medida resultó efectivamente loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] La dirección de vinculación del plano de administración se ha reducido (o se ha desactivado)

### C-2. Abrir solo los puertos de protocolo necesarios 【No verificado】

- **Por qué**: de forma predeterminada se abren a la vez AMQP `5672` y MQTT `1883`; si no usas MQTT, ciérralo para reducir la superficie de ataque.
- **Cómo hacerlo**: `plugins.mqtt.enabled=false` (o elimínalo de `listeners`); la desactivación **cierra el puerto real**, no solo cambia un bit de estado.
- **Cómo verificar**: tras desactivarlo, el puerto correspondiente deja de escuchar (no aparece con `Get-NetTCPConnection -State Listen`).
  **【No verificado】**: en el ensayo local ambos protocolos estaban abiertos; no se verificó por separado la desaparición del puerto tras cerrarlo.
- [ ] Los plugins de protocolo no utilizados están deshabilitados

---

## D. Refuerzo de la ejecución en contenedores

Hechos de la imagen del repositorio (`Dockerfile`): binario enlazado estáticamente + alpine, **se ejecuta como no root (uid 10001, usuario `speedmq`)**,
y el directorio de datos `/var/lib/speedmq` es un volumen. `docker-compose.yml` usa un **volumen con nombre** para la persistencia, monta la configuración como **solo lectura** y rota los registros.

### D-1. Ejecución como no root 【No verificado (no se ejecutó Docker en esta máquina)】

- **Por qué**: privilegio mínimo, reduce el impacto tras un escape de contenedor.
- **Cómo hacerlo**: la imagen ya usa uid 10001 de forma predeterminada; **no** lo sobrescribas con `--user root`.
- **Cómo verificar**: `docker compose run -T --rm broker id` debe mostrar `uid=10001`. (el `run` debe llevar `-T` en entornos no interactivos)
- [ ] El contenedor se ejecuta como no root (sin sobrescribirlo con root)

### D-2. Sistema de archivos raíz de solo lectura + límites de recursos + recorte de capacidades (recomendado; el compose del repositorio no lo activa de forma predeterminada) 【No verificado】

- **Por qué**: un sistema de archivos raíz de solo lectura impide la manipulación del binario en tiempo de ejecución; los límites de recursos evitan que un solo contenedor derrumbe el host; y recortar las capabilities reduce la superficie de ataque del núcleo.
- **Cómo hacerlo** (ejemplo; fusiónalo según sea necesario en el servicio `broker` de compose):

```yaml
services:
  broker:
    read_only: true
    tmpfs:
      - /tmp
    security_opt:
      - no-new-privileges:true
    cap_drop: ["ALL"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8g
```

- **Cómo verificar**: intentar escribir en la ruta raíz dentro del contenedor debe fallar (solo lectura); los límites de recursos se ven con `docker inspect`.
  **【No verificado】** (no se ejecutó Docker en esta máquina); además, **con un sistema de archivos raíz de solo lectura hay que confirmar que `data_dir` está en un volumen escribible**, de lo contrario el núcleo no podrá escribir en disco.
- [ ] Se han evaluado el sistema de archivos raíz de solo lectura y los límites de recursos (ten en cuenta que `data_dir` debe estar en un volumen escribible)

---

## E. Limitaciones conocidas (lo que actualmente no se puede hacer; no cuentes con ello)

Los siguientes son **vacíos fácticos**; reconócelos explícitamente en el diseño de seguridad y no asumas que existen:

1. **Las contraseñas se almacenan y copian en texto claro**. El campo `password` en `meta/state.json` está en texto claro (**【Verificado】** en la medición se ve
   `"password":"drillpass"`); en el archivo de configuración también está en texto claro. **No** hay hash de contraseñas (el hash y los backends de autenticación externos quedan para los plugins de autenticación).
   → Consecuencia: **el directorio de datos y los archivos de copia de seguridad equivalen a credenciales sensibles**, y hay que protegerlos con permisos de archivo y cifrado.
2. **No hay registro de auditoría**. Las altas, bajas y modificaciones del plano de administración generan registros ordinarios (p. ej. `管理面更新用户 actor=... user=...`),
   pero **no** hay un flujo de auditoría independiente e inalterable, ni registros de nivel de cumplimiento de "quién cambió qué y cuándo".
3. **No hay autenticación externa como LDAP / OAuth2 / JWT**. La v1 solo incorpora `PLAIN` / `AMQPLAIN`
   (medido: `auth.Store.Mechanisms()` solo devuelve estos dos).
4. **SASL `EXTERNAL` no está implementado**: aunque se configure mTLS, la capa de protocolo **sigue usando autenticación por contraseña PLAIN**
   (no existe el paso de "usar el certificado de cliente para prescindir de la contraseña"). El certificado solo es una validación de la capa de transporte.
5. **`remote_access` no se puede configurar mediante la API**: las cuentas creadas por la API/CLI de administración siempre permiten el inicio de sesión remoto (véase A-4),
   y no se puede restringir una cuenta individual a solo local.
6. **El plano de administración no tiene lista blanca de orígenes independiente ni limitación de tasa**: solo se puede reducir la superficie de exposición mediante la dirección de vinculación, el cortafuegos y TLS.
7. **Sin entorno aislado (sandbox) de plugins**: los plugins de forma A comparten proceso con el núcleo; aunque los plugins externos de forma B tienen aislamiento de proceso, su **plano de datos pasa por un proxy de conexión local**
   y los plugins pueden invocar semántica del núcleo (sujeta a las comprobaciones de vhost y permisos); **no** es un sandbox de seguridad.
8. **Las etiquetas del plano de administración solo tienen tres niveles: `administrator`/`management`/`monitoring`**; no hay un RBAC por recurso más granular.

---

## F. Lista resumen

- [ ] A-1 Cuenta predeterminada cambiada/eliminada 【procedimiento verificado】
- [ ] A-2 Privilegio mínimo de las cuentas de negocio (expresiones regulares), sin etiqueta de administrador 【vía 403 verificada】
- [ ] A-3 La etiqueta `administrator` se concede solo a cuentas de operaciones 【criterio de permisos implícitos verificado】
- [ ] A-4 Restricción de origen de las cuentas privilegiadas evaluada (ten en cuenta que las cuentas creadas por API abren el acceso remoto de forma predeterminada)
- [ ] B-1 TLS habilitado en los puertos expuestos; una mala configuración rechaza el arranque 【verificado】
- [ ] B-2 `min_version` ≥ 1.2 【verificado】
- [ ] B-3 Puertos que necesitan mTLS con `require_and_verify` + `ca_file`
- [ ] B-4 TLS habilitado en el plano de administración
- [ ] C-1 Dirección de vinculación del plano de administración reducida 【dirección de escucha verificada】
- [ ] C-2 Plugins de protocolo no utilizados deshabilitados
- [ ] D-1 Contenedor ejecutándose como no root
- [ ] D-2 Sistema de archivos raíz de solo lectura / límites de recursos / recorte de capacidades evaluados
- [ ] E Limitaciones conocidas (contraseñas en texto claro, sin auditoría, sin LDAP/OAuth2, SASL EXTERNAL no implementado) reconocidas en el diseño de seguridad
