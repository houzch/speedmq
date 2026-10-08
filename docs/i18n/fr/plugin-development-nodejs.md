# Guide de développement des plugins en processus externe pour SpeedMQ —— Node.js

> **Public** : développeurs qui écrivent en Node.js des plugins en processus externe (sidecar) pour SpeedMQ.
> **À lire d'abord** : [Guide de développement des plugins en processus externe (sidecar)](plugin-development.md) (modèle mental / champs de configuration / tableau complet du protocole filaire).
> **Projet d'exemple** : espace de travail `speedmq-plugin/nodejs/index.js` (bibliothèque standard Node uniquement, **aucune dépendance npm**).

---

## 1. À quoi cela ressemble une fois lancé

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Trois points essentiels : **votre processus est le serveur** (il attend que le noyau vienne se connecter) ; **le port externe est ouvert par le noyau** (configuration `protocols[].listeners`) ;
**`prefix` doit être non vide** (un préfixe vide = ne pas participer au sniffing, la connexion ne vous sera pas confiée ; mesuré : elle est immédiatement coupée, ≤ 8 octets ASCII).

---

## 2. Démarrage en trois étapes

### Étape 1 : configuration

`speedmqd.json` (**la configuration réelle est du JSON standard, sans commentaires possibles**) :

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/speedmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Étape 2 : lancer

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Étape 3 : vérifier

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

En Node, utilisez `Buffer` : accumulez les octets reçus, et dès qu'une trame complète est disponible, découpez-la et traitez-la.

```js
drain() {
  while (this.buf.length >= 5) {
    const len = this.buf.readUInt32BE(0)
    if (this.buf.length < 4 + len) return      // 帧还没收全
    const kind = this.buf.readUInt8(4)
    const payload = this.buf.subarray(5, 4 + len)
    this.buf = this.buf.subarray(4 + len)
    this.dispatch(kind, payload)
  }
}
```

Un flux = une connexion client ; la charge utile d'une trame de données est `numéro de flux big-endian (4 octets) + octets bruts`.

### 3.2 Poignée de main et heartbeat

Le noyau **envoie d'abord Hello**, vous répondez `HelloAck` ; le noyau vérifie `name` et `api_version` (actuellement `v1`) puis accepte la connexion.
Ensuite, un `Ping` toutes les 2 s, il suffit de répondre `Pong` (traité au passage par la boucle de lecture, aucun minuteur n'est nécessaire).

### 3.3 Modèle asynchrone (version Node)

Boucle d'événements mono-thread, qui évite naturellement le problème des « écritures entrelacées » — mais attention à **ne pas laisser la boucle de lecture faire `await` sur un appel inverse** :

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` est `async` : il peut à son tour faire `await call('session.settle', …)`, il ne faut donc surtout pas l'écrire en attente synchrone.

### 3.4 Pont sémantique (authentification obligatoire d'abord)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` n'est pas optionnel : la surface d'opérations du noyau pour la connexion n'a pas d'identité avant l'authentification, et un `session.open` direct est refusé
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```js
const ident = await call('core.authenticate', {
  stream, mechanism: 'PLAIN',
  response: Buffer.concat([Buffer.from(`\x00${user}\x00`), Buffer.from(password)]).toString('base64'),
})
await call('session.open', { stream, vhost: '/' })
const q = await call('session.declare_queue', { stream, exclusive: true, auto_delete: true })
await call('session.publish', { stream, routing_key: q.name,
  message: { body: Buffer.from('hi').toString('base64') } })
await call('session.consume', { stream, queue: q.name, prefetch: 32 })
```

Les livraisons sont **renvoyées en sens direct** par le noyau (`method = "session.deliver"`), puis réglées avec `session.settle`
(`ack` / `requeue` / `reject` ; le numéro de livraison est unique globalement, sans numéro de flux).

---

## 4. Lecture guidée du code (projet d'exemple)

`speedmq-plugin/nodejs/index.js` compte environ 330 lignes :

| Emplacement | Rôle |
| --- | --- |
| `u32()` / `Conn.send()` | lecture/écriture des trames |
| `Conn.drain()` / `dispatch()` | analyse et distribution par trame |
| `Conn.call()` | appel inverse (table `Promise` + `pending`, appariement par `reverse=true` et `id`) |
| `Stream` | côté lecture du flux : `push/end/read` forment une file asynchrone |
| `handleHello` | vérifie et répond HelloAck |
| `handleForwardCall` / `handleMethod` | appels directs (`session.deliver` + settle, `stats`) |
| `sessionDemo` | authentification + déclaration + publication + consommation |

---

## 5. Mesures réelles (reproduction locale)

Windows + Node v24 ; le noyau dans Docker (`speedmq:1.1.01`), le plugin sur la machine hôte (`tcp://host.docker.internal:19011`).

```
plugin=node-sidecar state=enabled         # /api/plugins
echo=[NDhello]                            # 客户端连内核端口 19012 发 "NDhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=node-sidecar addr=0.0.0.0:19011 version=0.1.0
内核已接入 plugin=node-sidecar peer=127.0.0.1:50987
握手完成 plugin=node-sidecar kernel=1.1.01
认证通过 user=guest
收到投递（session.deliver） queue=amq.gen-893688b01c62b2ad5763a7 delivery_id=1 body=hello from node sidecar
session 演示完成 queue=amq.gen-893688b01c62b2ad5763a7
流已打开 plugin=node-sidecar stream=1 remote=172.17.0.1:40450 local=172.17.0.2:19012
```

Couverte : **poignée de main → authentification → pont sémantique → renvoi de livraison → règlement → écho du flux d'octets**.

---

## 6. Points d'attention spécifiques à Node.js

- **`socket.write` écrit une trame à la fois** : l'exemple assemble la trame entière en un seul `Buffer` avant l'écriture, aucun verrou supplémentaire n'est donc nécessaire ;
  si vous découpez une trame en plusieurs `write`, vous devez garantir vous-même l'ordre.
- **Les frontières de chunk de `stream.on('data')` n'ont rien à voir avec les trames** : vous devez accumuler vous-même le tampon (voir `drain()`).
- **base64** : `message.body` et `core.authenticate.response` sont des chaînes base64 dans le JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Ne pas faire `await` dans `drain()`** : c'est une fonction synchrone de découpage des trames ; confiez le traitement asynchrone à `handleForwardCall`.
- **ESM vs CJS** : l'exemple utilise CommonJS (`require`) pour que `node index.js` fonctionne directement ; passer à ESM ne demande que de remplacer par `import`.

---

## 7. Pour aller plus loin

- Le plugin embarque sa propre interface d'administration : ajoutez `console_url` dans la configuration (document principal §5.8), la page « Gestion des plugins » de la console d'administration fera apparaître une entrée directe.
- Déploiement autonome (K8s / systemd) : `spawn: []` + `address: "tcp://<nom du service>:19011"`, écoute sur `0.0.0.0` dans le conteneur.
- Réutilisation de port : donnez un `prefix` différent à chaque protocole, le noyau distribue les connexions vers chaque plugin selon le préfixe.
