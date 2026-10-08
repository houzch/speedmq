# Base de durcissement de la sécurité de SpeedMQ (liste à cocher)

> Principe : **n'écrire que les capacités réellement présentes dans ce dépôt**. Chaque point indique « pourquoi le faire + comment vérifier que c'est fait » ; les commandes de vérification sont toutes exécutables.
> Les mentions **【vérifié】** signifient que l'opération a **réellement été exécutée** sur cette machine (Windows + PowerShell 5.1, `1.0.0`) ;
> **【non vérifié】** signifie non exécuté ou actuellement impossible, sans jamais faire semblant.
> Toutes les commandes sont données en version curl de style `/bin/sh`, accompagnées de la version PowerShell (sous PowerShell 5.1, utilisez
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Authentification et contrôle d'accès

### A-1. Modifier le compte par défaut `guest/guest` 【vérifié】

- **Pourquoi** : `guest/guest` (étiquette `administrator`) est intégré par défaut ; l'exposer à l'extérieur revient à laisser la porte grande ouverte.
- **Comment** : modifier le mot de passe / supprimer le compte en cours d'exécution, **sans** toucher au fichier de configuration (les `users` ne prennent effet qu'au premier amorçage).

```bash
# modifier le mot de passe
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# ou supprimer directement le compte par défaut (assurez-vous d'abord qu'un nouvel administrateur est créé)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Comment vérifier** : après modification, l'ancien mot de passe doit renvoyer 401 et le nouveau 200.
  **【vérifié】** Sortie mesurée sur cette machine :

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Le compte par défaut a été modifié/supprimé

### A-2. Privilège minimal : expression régulière `configure` / `write` / `read` par vhost 【vérifié】

- **Pourquoi** : les trois catégories sont alignées sur RabbitMQ — `configure` gère la déclaration/suppression de la topologie, `write` la publication et les bindings, `read` la consommation et le pull ;
  tout dépassement de droits renvoie 403. N'ouvrez au compte métier que ce dont il a besoin.
- **Comment** : `PUT /api/permissions/{vhost}/{user}`, par exemple consommation en lecture seule : `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Comment vérifier** : avec un compte restreint, tentez une opération hors de ses droits ; vous devez obtenir 403 `ACCESS_REFUSED`.
  **【vérifié】** Sur cette machine, déclaration d'un exchange avec un vrai client AMQP par un utilisateur `configure="^$"`, mesuré :

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Chaque compte métier ne se voit accorder que les expressions régulières nécessaires et n'a pas les étiquettes `administrator`/`management`

### A-3. Portée de l'étiquette `administrator` (droits complets implicites) — à accorder avec prudence 【vérifié】

- **Pourquoi** : **un utilisateur portant l'étiquette `administrator` dispose de droits complets sur tous les vhosts qui lui sont visibles, sans nécessiter d'enregistrement de permissions**
  (aligné sur la portée mesurée de RabbitMQ, voir README / conception M8-7). Autrement dit, dès que cette étiquette est attribuée,
  les expressions régulières de permissions ne jouent plus — c'est le privilège le plus élevé.
- **Comment** : n'accorder `administrator` qu'aux comptes d'administration/d'exploitation ; aux comptes métier, ne jamais donner d'étiquette, uniquement les expressions régulières de permissions.

- **Comment vérifier (démonstration des droits implicites)** : sur un nouveau vhost **sans aucun enregistrement de permissions**, un utilisateur `administrator` doit pouvoir l'utiliser directement.
  **【vérifié】** Mesuré sur cette machine : après création du vhost `drillvh` (sans aucun enregistrement de permissions), `guest` (administrator) y déclare une topologie avec succès :

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] L'étiquette `administrator` n'est accordée qu'à un très petit nombre de comptes d'exploitation

### A-4. `remote_access` : restreindre un compte aux connexions locales 【non vérifié (impossible de simuler une source distante sur la même machine)】

- **Pourquoi** : comme dans RabbitMQ, le compte intégré `guest` n'autorise par défaut que les connexions locales ; en déploiement exposé, assurez-vous que la source des comptes privilégiés est restreinte.
- **Comment / portée (limite importante)** :
  - `remote_access` ne peut être défini que dans le **fichier de configuration**, sous `users.<name>.remote_access`, et **ne prend effet qu'au premier amorçage** ;
  - **tout compte créé via l'API d'administration / `speedmqctl` a systématiquement `remote_access=true`** (connexion depuis n'importe quelle source autorisée) —
    d'après le commentaire d'`UpsertUser` dans `internal/broker/observe.go` et `"remote_access":true` mesuré dans `meta/state.json`. Autrement dit, **l'API ne peut actuellement pas restreindre un compte aux connexions locales**.
- **Comment vérifier** : depuis **une autre machine** (autre que `127.0.0.1`), une connexion avec ce compte doit renvoyer 403 ; une connexion locale doit réussir.
  **【non vérifié】** : l'environnement de cette machine ne permet pas de construire une vraie source distante, donc non mesuré.
- [ ] La restriction de source des comptes privilégiés a été évaluée selon la portée ci-dessus (attention : les comptes créés via l'API ouvrent le distant par défaut)

---

## B. Sécurité du transport (TLS)

Éléments de configuration TLS (la couche d'accès et l'interface d'administration **partagent** le même jeu de champs) : `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Activer TLS et refuser le démarrage en cas de mauvaise configuration 【vérifié】

- **Pourquoi** : les certificats sont lus et vérifiés **au démarrage** — une mauvaise configuration refuse immédiatement le démarrage au lieu de n'apparaître qu'à la première connexion client.
- **Comment** : fournir `cert_file` + `key_file` dans `listeners.<plugin>[].tls` ou `management.tls` (**les deux ensemble** activent TLS).

- **Comment vérifier** : démarrer avec une mauvaise configuration ; l'échec doit être immédiat.
  **【vérifié】** Trois configurations erronées mesurées sur cette machine, toutes avec `exit=1` et refus de démarrage :

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Comment vérifier (sens direct/inverse)** : un client TLS peut se connecter, un client en clair visant le port TLS est refusé.
  **【vérifié】** Instance TLS démarrée sur cette machine (`amqp091` en TLS), avec un vrai client de sondage :

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] TLS est activé sur les ports de protocole exposés

### B-2. `min_version` au minimum 1.2 【vérifié】

- **Pourquoi** : interdire les versions TLS trop anciennes ; la valeur par défaut est déjà `1.2`, valeurs possibles `1.2` / `1.3`.
- **Comment vérifier** : indiquer `1.0` comme `min_version` ; le démarrage doit renvoyer une erreur (voir la sortie `badtls2` de B-1).
- [ ] `min_version` vaut `1.2` ou `1.3`

### B-3. Authentification mutuelle `client_auth: require_and_verify` (mTLS) 【partiellement vérifié】

- **Pourquoi** : exiger que le client présente et fasse vérifier un certificat, pour empêcher des clients non autorisés d'accéder aux ports de protocole.
- **Comment** : configurer `ca_file` + `client_auth: require_and_verify` (ces derniers exigent de fournir `ca_file`).
- **【vérifié】** : le TLS de bout en bout et le chemin de refus ont été vérifiés avec un vrai client (B-1). **Le mTLS (exiger et vérifier le certificat client) n'a pas fait l'objet d'un exercice distinct sur cette machine**.
- [ ] Les ports nécessitant mTLS sont configurés avec `require_and_verify` + `ca_file`

### B-4. TLS de l'interface d'administration 【non vérifié】

- **Pourquoi** : l'interface d'administration transmet les mots de passe via Basic Auth et doit être chiffrée.
- **Comment** : `management.tls` utilise les mêmes champs que les écoutes de protocole.
- **【non vérifié】** : l'exercice sur cette machine a lié l'interface d'administration à un port local en clair, sans monter de HTTPS pour l'interface d'administration.
- [ ] TLS est activé sur l'interface d'administration (ou celle-ci est strictement confinée à un réseau de confiance)

---

## C. Réduction de la surface d'exposition

### C-1. Réduction de la plage d'écoute de l'interface d'administration 【vérifié (adresse d'écoute mesurée)】

- **Pourquoi** : l'interface d'administration écoute par défaut sur `:15672` (toutes les cartes réseau). En déploiement exposé, liez-la à une adresse interne/de bouclage, ou limitez les sources via un pare-feu.
- **Comment** : configurer `management.addr` sur `127.0.0.1:15672` ou une adresse interne ; ou fermer complètement via `management.enabled=false`
  (une fois fermée, plus de port d'administration, mais `speedmqctl` devient également indisponible).
- **Comment vérifier** :
  **【vérifié】** L'interface d'administration a été configurée sur `127.0.0.1:15677` sur cette machine ; l'adresse d'écoute mesurée est bien le bouclage :

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] L'adresse de liaison de l'interface d'administration est réduite (ou elle est désactivée)

### C-2. N'ouvrir que les ports de protocole nécessaires 【non vérifié】

- **Pourquoi** : par défaut, AMQP `5672` et MQTT `1883` sont ouverts simultanément ; si MQTT n'est pas utilisé, fermez-le pour réduire la surface d'attaque.
- **Comment** : `plugins.mqtt.enabled=false` (ou retirer de `listeners`) ; la désactivation **ferme réellement le port**, il ne s'agit pas d'un simple changement de bit d'état.
- **Comment vérifier** : après désactivation, le port correspondant n'écoute plus (invisible avec `Get-NetTCPConnection -State Listen`).
  **【non vérifié】** : l'exercice sur cette machine gardait les deux protocoles ouverts ; la disparition du port après fermeture n'a pas été vérifiée séparément.
- [ ] Les plugins de protocole non utilisés sont désactivés

---

## D. Durcissement de l'exécution des conteneurs

Faits sur l'image du dépôt (`Dockerfile`) : binaire lié statiquement + alpine, **exécution en non-root (uid 10001, utilisateur `speedmq`)**,
répertoire de données `/var/lib/speedmq` en tant que volume. `docker-compose.yml` utilise un **volume nommé** pour la persistance, un **montage en lecture seule** de la configuration, et la rotation des logs.

### D-1. Exécution en non-root 【non vérifié (Docker non exécuté sur cette machine)】

- **Pourquoi** : privilège minimal, réduire l'impact après une évasion de conteneur.
- **Comment** : l'image est déjà en uid 10001 par défaut ; **n'**écrasez **pas** avec `--user root`.
- **Comment vérifier** : `docker compose run -T --rm broker id` doit afficher `uid=10001`. (`run` doit être accompagné de `-T` en environnement non interactif)
- [ ] Le conteneur s'exécute en non-root (non écrasé par root)

### D-2. Système de fichiers racine en lecture seule + limites de ressources + réduction des capacités (recommandé, non activé par défaut dans le compose du dépôt) 【non vérifié】

- **Pourquoi** : un système de fichiers racine en lecture seule empêche la modification du binaire en cours d'exécution ; les limites de ressources évitent qu'un conteneur épuise l'hôte ; la réduction des capabilities diminue la surface d'attaque du noyau.
- **Comment** (exemple, à fusionner selon les besoins dans le service `broker` de compose) :

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

- **Comment vérifier** : une tentative d'écriture à la racine du conteneur doit échouer (lecture seule) ; `docker inspect` montre les limites de ressources.
  **【non vérifié】** (Docker non exécuté sur cette machine) ; de plus, **avec un système de fichiers racine en lecture seule, il faut vérifier que `data_dir` se trouve sur un volume inscriptible**, sinon le noyau ne peut pas écrire sur disque.
- [ ] Le système de fichiers racine en lecture seule et les limites de ressources ont été évalués (attention : `data_dir` doit être sur un volume inscriptible)

---

## E. Limites connues (réellement impossibles actuellement, ne pas compter dessus)

Tous les points ci-dessous sont des **lacunes factuelles** ; reconnaissez-les explicitement dans votre conception de sécurité et ne supposez pas qu'ils existent :

1. **Les mots de passe sont stockés et copiés en clair**. Dans `meta/state.json`, le champ `password` est en clair (**【vérifié】** `"password":"drillpass"` visible en mesure) ; ils le sont aussi dans le fichier de configuration. Il **n'**y a **aucun** hachage de mot de passe (le hachage et les backends d'authentification externes sont laissés aux plugins d'authentification).
   → Conséquence : **le répertoire de données et les fichiers de sauvegarde équivalent à des identifiants sensibles** et doivent bénéficier de permissions de fichiers et d'une protection par chiffrement.
2. **Aucun journal d'audit**. Les ajouts/suppressions/modifications de l'interface d'administration produisent des logs ordinaires (par ex. `管理面更新用户 actor=... user=...`),
   mais il **n'**existe **aucun** flux d'audit indépendant et inviolable, ni d'enregistrement de niveau conformité indiquant « qui a modifié quoi et quand ».
3. **Aucune authentification externe LDAP / OAuth2 / JWT**. La v1 intègre uniquement `PLAIN` / `AMQPLAIN`
   (`auth.Store.Mechanisms()` ne retourne que ces deux mécanismes en mesure).
4. **SASL `EXTERNAL` non implémenté** : même avec mTLS configuré, la couche de protocole **passe toujours par l'authentification par mot de passe PLAIN**
   (l'étape « dispense de mot de passe via certificat client » n'existe pas). Le certificat ne sert qu'à la vérification de la couche de transport.
5. **`remote_access` ne peut pas être défini via l'API** : tout compte créé par l'API/CLI d'administration autorise le distant (voir A-4),
   impossible de restreindre un compte individuel aux connexions locales.
6. **Aucune liste blanche de sources dédiée / aucune limitation de débit sur l'interface d'administration** : la réduction de la surface d'exposition ne peut reposer que sur l'adresse de liaison, le pare-feu et TLS.
7. **Aucun bac à sable de plugin** : les plugins de forme A sont dans le même processus que le noyau ; les plugins externes de forme B bénéficient d'une isolation de processus, mais **leur plan de données passe par un proxy de connexion local**
   et les plugins peuvent appeler des sémantiques du noyau (soumises aux vérifications de vhost et de permissions) ; ce **n'**est **pas** un bac à sable de sécurité.
8. **Les étiquettes de l'interface d'administration ne comportent que trois niveaux `administrator`/`management`/`monitoring`**, sans RBAC plus fin par ressource.

---

## F. Liste récapitulative

- [ ] A-1 Compte par défaut modifié/supprimé 【procédure vérifiée】
- [ ] A-2 Privilège minimal du compte métier (régulier), sans étiquette d'administrateur 【chemin 403 vérifié】
- [ ] A-3 L'étiquette `administrator` n'est accordée qu'aux comptes d'exploitation 【portée des droits implicites vérifiée】
- [ ] A-4 Restriction de source des comptes privilégiés évaluée (attention : comptes créés via l'API ouvrent le distant par défaut)
- [ ] B-1 TLS activé sur les ports exposés, refus de démarrage en cas de mauvaise configuration 【vérifié】
- [ ] B-2 `min_version` ≥ 1.2 【vérifié】
- [ ] B-3 Les ports nécessitant mTLS configurés avec `require_and_verify` + `ca_file`
- [ ] B-4 TLS activé sur l'interface d'administration
- [ ] C-1 Adresse de liaison de l'interface d'administration réduite 【adresse d'écoute vérifiée】
- [ ] C-2 Plugins de protocole inutilisés désactivés
- [ ] D-1 Conteneur exécuté en non-root
- [ ] D-2 Système de fichiers racine en lecture seule / limites de ressources / réduction des capacités évalués
- [ ] E Limites connues (mots de passe en clair, absence d'audit, absence de LDAP/OAuth2, SASL EXTERNAL non implémenté) reconnues dans la conception de sécurité
