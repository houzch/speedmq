# Guide de développement des plugins en processus externe pour SpeedMQ —— Java

> **Public** : développeurs qui écrivent en Java des plugins en processus externe (sidecar) pour SpeedMQ.
> **À lire d'abord** : [Guide de développement des plugins en processus externe (sidecar)](plugin-development.md) (modèle mental / champs de configuration / tableau complet du protocole filaire).
> **Projet d'exemple** : espace de travail `speedmq-plugin/java/SidecarPlugin.java` (fichier unique, bibliothèque standard du JDK uniquement, sans Maven/Gradle).

---

## 1. À quoi cela ressemble une fois lancé

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Trois points essentiels : **votre processus est le serveur** (il attend que le noyau vienne se connecter) ; **le port externe est ouvert par le noyau** (`protocols[].listeners`) ;
**`prefix` doit être non vide** (un préfixe vide = ne pas participer au sniffing, la connexion ne vous sera pas confiée ; mesuré : elle est immédiatement coupée, ≤ 8 octets ASCII).

---

## 2. Démarrage en trois étapes

### Étape 1 : configuration

`speedmqd.json` (**la configuration réelle est du JSON standard, sans commentaires possibles**) :

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

### Étape 2 : compiler et lancer

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Étape 3 : vérifier

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Points d'implémentation

### 3.1 Découpage en trames

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

En Java, `DataInputStream`/`DataOutputStream` sont les plus simples — leurs `readInt`/`writeInt` sont **big-endian** :

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Le côté écriture doit être **sérialisé** (heartbeat, réponses et blocs de données proviennent de threads différents) :

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

Charge utile d'une trame de données = `numéro de flux big-endian (4 octets) + octets bruts`.

### 3.2 Poignée de main et heartbeat

Le noyau **envoie d'abord Hello**, vous répondez `HelloAck` ; le noyau vérifie `name` et `api_version` (actuellement `v1`) puis accepte la connexion.
Ensuite, un `Ping` toutes les 2 s, répondez `Pong`.

### 3.3 Modèle de concurrence (version Java)

| Rôle | Thread |
| --- | --- |
| Boucle de lecture des trames | un par connexion noyau |
| Traitement des flux | un par flux (plusieurs connexions client peuvent être traitées en concurrence) |
| Traitement des appels directs | un par appel |

**La boucle de lecture ne doit pas attendre de manière synchrone la réponse d'un appel inverse** (cela provoquerait un interblocage) : le traitement de `session.deliver` doit être déporté dans un thread dédié,
car il déclenche ensuite `session.settle` (encore un appel inverse). C'est ce que fait l'exemple.

### 3.4 Pont sémantique (authentification obligatoire d'abord)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` n'est pas optionnel : la surface d'opérations du noyau pour la connexion n'a pas d'identité avant l'authentification, et un `session.open` direct est refusé
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

Les livraisons sont **renvoyées en sens direct** par le noyau (`method = "session.deliver"`), puis réglées avec `session.settle`
(`ack` / `requeue` / `reject` ; le numéro de livraison est unique globalement, sans numéro de flux).

---

## 4. Lecture guidée du code (projet d'exemple)

`speedmq-plugin/java/SidecarPlugin.java` compte environ 470 lignes (JSON minimal inclus) :

| Emplacement | Rôle |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | lecture/écriture des trames (`DataInputStream` + verrou d'écriture) |
| `Conn.serve()` | boucle de lecture et distribution des trames |
| `Conn.call()` | appel inverse (table `pending` + file bloquante, protection par délai d'expiration) |
| `Conn.handleHello()` | vérifie et répond HelloAck |
| `StreamState` | côté lecture du flux (`BlockingQueue`, `STREAM_END` indique la fin) |
| `Conn.handleForwardCall()` / `handleMethod()` | appels directs (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | authentification + déclaration + publication + consommation |
| `Json` (en fin de fichier) | lecture/écriture JSON minimale, uniquement pour rendre l'exemple sans dépendance |

> **Recommandation de production** : remplacez `Json` par la bibliothèque que vous utilisez habituellement (Jackson / Gson), ou par une pile existante autre que `java.net.http` —
> cela n'a rien à voir avec ce que cet exemple veut montrer (le protocole filaire).

---

## 5. Mesures réelles (reproduction locale)

Windows + JDK 25 ; le noyau dans Docker (`speedmq:1.1.01`), le plugin sur la machine hôte (`tcp://host.docker.internal:19031`).

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

Couverte : **poignée de main → authentification → pont sémantique → renvoi de livraison → règlement → écho du flux d'octets**.

---

## 6. Points d'attention spécifiques à Java

- **Les `\uXXXX` du code source sont traités par le compilateur en toute position** (y compris dans les commentaires !). Dans l'exemple, les commentaires écrivent volontairement
  « NUL + nom d'utilisateur + NUL + mot de passe » plutôt que `\u0000` directement, sinon javac signale un caractère illégal.
- **Un code source en chinois exige impérativement `javac -encoding UTF-8`**, sinon sous Windows (GBK par défaut) javac signale « caractères non mappables dans l'encodage GBK ».
  À l'exécution, pour imprimer correctement ces caractères, ajoutez `-Dfile.encoding=UTF-8`.
- **Les variables locales capturées par une lambda doivent être effectively final** : dans l'exemple, `name` est réassigné lors de l'analyse des paramètres,
  la lambda utilise donc `opts.name` (un champ affecté une seule fois).
- **`DataInputStream` est bloquant** : à la déconnexion il lève `EOFException`/`IOException`, sur lesquelles on s'appuie pour conclure.
- **base64** : `message.body` et `core.authenticate.response` sont des chaînes base64 dans le JSON
  (`Base64.getEncoder()/getDecoder()`).
- **La bibliothèque standard du JDK n'a pas de JSON** : l'exemple embarque une implémentation minimale ; `Json.parse` décode les entiers en `Long` et les flottants en `Double`,
  et pour récupérer `id` on utilise `((Number) m.get("id")).longValue()`.

---

## 7. Pour aller plus loin

- Empaquetez-le en jar exécutable (`Main-Class: SidecarPlugin`) ou en runtime réduit avec `jlink`,
  puis remplacez `spawn` par `["java", "-jar", "/opt/speedmq/sidecar.jar", …]`.
- Le plugin embarque sa propre interface d'administration : ajoutez `console_url` dans la configuration (document principal §5.8), la page « Gestion des plugins » de la console d'administration fera apparaître une entrée directe.
- Déploiement autonome : `spawn: []` + `address: "tcp://<nom du service>:19031"`, écoute sur `0.0.0.0` dans le conteneur.
