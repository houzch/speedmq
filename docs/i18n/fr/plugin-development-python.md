# Guide de développement des plugins en processus externe pour SwiftMQ —— Python

> **Public** : développeurs qui écrivent en Python des plugins en processus externe (sidecar) pour SwiftMQ.
> **À lire d'abord** : [Guide de développement des plugins en processus externe (sidecar)](plugin-development.md) —— on y trouve le modèle mental, les champs de configuration et le tableau complet du protocole filaire ;
> ce document ne traite que **la mise en œuvre concrète en Python**, ainsi que les étapes et résultats mesurés sur cette machine.
> **Projet d'exemple** : espace de travail `swiftmq-plugin/python/sidecar_plugin.py` (bibliothèque standard uniquement, zéro dépendance tierce).

---

## 1. À quoi cela ressemble une fois lancé

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Trois points essentiels (faciles à confondre, à retenir d'abord) :

1. **Votre processus est le serveur** : il écoute une adresse locale et attend que le noyau vienne se connecter (`plugins.<nom>.sidecar.address`).
2. **Le port métier externe est ouvert par le noyau** : le client se connecte au port du noyau, et les octets vous sont proxifiés (`protocols[].listeners`).
3. **`prefix` doit être non vide** : le noyau décide par sniffing de préfixe « à qui confier cette connexion ». Un `prefix` vide signifie **ne pas participer au sniffing** ;
   même sur son propre écouteur, la connexion ne vous sera pas confiée (mesuré : la connexion est immédiatement coupée). La longueur du préfixe est ≤ 8 octets, en ASCII.

---

## 2. Démarrage en trois étapes

### Étape 1 : déclarer le plugin dans la configuration

`swiftmqd.json` (**la configuration réelle est du JSON standard, sans commentaires possibles**) :

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` est **l'adresse à laquelle le noyau vient se connecter** (le noyau est le client, le plugin est le serveur).
- `spawn` vide = le noyau se contente de se connecter sans lancer, le processus est géré par vous (systemd / supervisor / compose).
- `prefix` **doit être non vide** : les premiers octets envoyés par le client doivent commencer par lui (le noyau décide par sniffing de préfixe à qui confier la connexion).
- `listeners` sont les ports externes, ouverts par le noyau (le client se connecte au noyau, pas à vous).
- En déploiement inter-conteneurs, `address` utilise le **nom de service** (par ex. `tcp://py-sidecar:19001`), et le plugin doit écouter sur `0.0.0.0`.

### Étape 2 : lancer

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Étape 3 : vérifier

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Points d'implémentation

### 3.1 Découpage en trames (la seule couche d'octets à écrire correctement soi-même)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

La charge utile d'une trame de données = `numéro de flux big-endian (4 octets) + octets bruts` ; la charge utile du plan de contrôle est du JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Poignée de main et heartbeat

Une fois connecté, le noyau **envoie d'abord Hello** ; vous devez répondre par une trame `HelloAck` ; le noyau vérifie
`name == le nom du plugin dans la configuration` et `api_version == l'APIVersion du noyau` (actuellement `v1`).
Ensuite, le noyau envoie un `Ping` toutes les 2 s, il suffit de répondre `Pong` (ne pas répondre fait considérer la connexion comme morte).

### 3.3 Flux logiques

`kindOpen` arrive → **répondez d'abord `OpenAck`**, puis commencez à servir ; `kindData` arrive → réécrivez tel quel (ou après analyse selon votre protocole)
en `kindData` ; fin du traitement → envoyez `kindClose`. Un flux = une connexion client.

### 3.4 Appels inverses et pont sémantique du noyau

Les appels plugin → noyau passent par `kindCall` avec `"reverse": true`, et le noyau répond `kindReply` sur la même connexion.
**L'ordre est important** :

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **n'est pas optionnel** : la surface d'opérations du noyau pour la connexion n'a pas d'identité avant l'authentification, et un `session.open` direct est refusé
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Les paramètres sont la réponse SASL extraite de votre protocole :

```python
plain = b"\x00" + user.encode() + b"\x00" + password.encode()
ident = call("core.authenticate", {
    "stream": stream_id, "mechanism": "PLAIN",
    "response": base64.b64encode(plain).decode(),   # 字节在 JSON 里是 base64
})
# 拿到的会话与进程内协议插件完全同一套语义
call("session.open", {"stream": stream_id, "vhost": "/"})
q = call("session.declare_queue", {"stream": stream_id, "exclusive": True, "auto_delete": True})
call("session.publish", {"stream": stream_id, "routing_key": q["name"],
                         "message": {"body": base64.b64encode(b"hi").decode()}})
call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
```

Les livraisons de consommation sont **renvoyées en sens direct** par le noyau (`kindCall`, `method = "session.deliver"`) ; une fois traitée, réglez avec
`session.settle` (`ack` / `requeue` / `reject` ; le numéro de livraison est unique globalement, le numéro de flux n'est pas nécessaire).

### 3.5 Modèle de concurrence (version Python)

| Rôle | Thread |
| --- | --- |
| Boucle de lecture des trames | une par connexion noyau |
| Traitement des flux | un par flux (plusieurs connexions client peuvent donc être traitées en concurrence) |
| Traitement des appels directs | un par appel |

**Attention impérative** : dans la boucle de lecture, **il ne faut pas** attendre de manière synchrone la réponse d'un appel inverse (cela provoquerait un interblocage) — un appel direct (`session.deliver`)
doit être traité dans un thread dédié, car il peut lui-même déclencher un `session.settle` pendant son traitement. L'écriture des trames doit être sérialisée par un verrou.

---

## 4. Lecture guidée du code (projet d'exemple)

`swiftmq-plugin/python/sidecar_plugin.py` compte environ 320 lignes ; fonctions clés :

| Emplacement | Rôle |
| --- | --- |
| `read_frame` / `Conn.send` | lecture/écriture des trames (préfixe de longueur + kind) |
| `Conn.call` | appel inverse : numérotation → envoi → attente de la réponse (appariement par `reverse=true` et `id`) |
| `Conn.serve` | boucle de lecture et distribution des trames |
| `Conn._handle_hello` | vérifie le nom du plugin / la version d'API et répond HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | cycle de vie du flux et écho |
| `Conn._dispatch` | traite les appels directs : `session.deliver` (avec règlement), `stats` |
| `Conn._session_demo` | authentification + déclaration de file + publication + consommation |

---

## 5. Mesures réelles (reproduction locale)

Environnement : Windows + Python 3.12 ; le noyau tourne dans Docker (`swiftmq:1.1.01`), le plugin sur la machine hôte,
et le noyau s'y connecte via `tcp://host.docker.internal:19001`.

```
plugin=py-sidecar state=enabled           # /api/plugins
echo=[PYhello]                            # 客户端连内核端口 19002 发 "PYhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
内核已接入 plugin=py-sidecar peer=('127.0.0.1', 52864)
握手完成 plugin=py-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-9e5d0d695639bad12ce919
收到投递（session.deliver） queue=amq.gen-9e5d0d695639bad12ce919 delivery_id=1 body=hello from python sidecar
流已打开 plugin=py-sidecar stream=1 remote=172.17.0.1:43856 local=172.17.0.2:19002
```

Chaîne couverte : **poignée de main → authentification → pont sémantique (déclaration/publication/consommation) → renvoi de livraison → règlement → écho du flux d'octets**.

---

## 6. Points d'attention spécifiques à Python

- **N'utilisez pas `time.sleep` pour attendre le heartbeat** : la boucle de lecture est bloquante, il suffit de maintenir la connexion grâce au Ping du noyau ;
  si vous définissez un délai d'expiration en lecture sur le socket, pensez à traiter ce délai comme « fin de connexion » (lors d'un `kill -9` du noyau, le socket peut ne pas se fermer à temps).
- **`json.dumps` ajoute des espaces par défaut** : l'exemple utilise `separators=(",", ":")` uniquement pour la lisibilité des journaux, le protocole lui-même ne l'exige pas.
- **Les octets sont en base64** : `message.body` et `core.authenticate.response` sont des chaînes base64 dans le JSON,
  n'oubliez pas `base64.b64encode/decode`.
- **L'écriture des trames doit être verrouillée** : heartbeat, réponses et blocs de données proviennent de threads différents ; une écriture entrelacée corromprait toute la connexion (l'exemple utilise `threading.Lock`).
- `asyncio` convient aussi, mais il faut garantir « écriture sérialisée + boucle de lecture non bloquante », avec la même logique que la version à threads.

---

## 7. Pour aller plus loin

- Pour doter le plugin de sa propre interface d'administration : ajoutez `console_url` dans la configuration, la page « Gestion des plugins » de la console d'administration fera apparaître une entrée directe
  (voir le document principal §5.8).
- Pour en faire un service autonome géré par systemd / K8s : `spawn: []` + `restart: "never"`, lancé de l'extérieur.
- Pour faire coexister plusieurs protocoles : sur un même écouteur, faites utiliser des `prefix` différents à plusieurs plugins, ou ouvrez à chacun un port dédié.
