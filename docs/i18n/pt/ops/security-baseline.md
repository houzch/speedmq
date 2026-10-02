# Linha de base de reforço de segurança do SwiftMQ (checklist)

> Princípio: **escrever apenas o que este repositório realmente possui**. Cada item indica "por que fazer + como verificar que foi feito", e os comandos de verificação podem ser executados.
> Os itens marcados como **【verificado】** significam que **foram realmente executados** nesta máquina (Windows + PowerShell 5.1, `1.0.0`);
> **【não verificado】** significa que não foram executados ou que atualmente não é possível fazer, e nunca fingimos o contrário.
> Todos os comandos são dados na versão curl em estilo `/bin/sh`, acompanhados da versão PowerShell (no PowerShell 5.1 use
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Autenticação e controle de acesso

### A-1. Trocar a conta padrão `guest/guest` 【verificado】

- **Por quê**: há um `guest/guest` embutido por padrão (tag `administrator`); expô-lo externamente equivale a deixar a porta aberta.
- **Como fazer**: altere a senha / exclua a conta em tempo de execução, **não** altere o arquivo de configuração (os `users` só valem no primeiro bootstrap).

```bash
# Alterar a senha
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<nova senha>","tags":["administrator"]}'
# Ou excluir diretamente a conta padrão (garanta antes que o novo administrador já foi criado)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Como verificar**: após a alteração, a senha antiga deve retornar 401 e a nova, 200.
  **【verificado】** Saída medida nesta máquina:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Conta padrão já alterada/excluída

### A-2. Privilégio mínimo: regex de `configure` / `write` / `read` por vhost 【verificado】

- **Por quê**: as três categorias estão alinhadas ao RabbitMQ — `configure` cuida da declaração/exclusão de topologia, `write` cuida da publicação e do binding, `read` cuida do consumo e da coleta;
  excesso de permissão retorna 403. Para contas de negócio, abra apenas o que elas precisam.
- **Como fazer**: `PUT /api/permissions/{vhost}/{user}`, por exemplo consumo somente leitura: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Como verificar**: com a conta restrita, tente uma operação sem permissão; deve retornar 403 `ACCESS_REFUSED`.
  **【verificado】** Nesta máquina, usando um cliente AMQP real com um usuário `configure="^$"` para declarar um exchange, mediu-se:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Cada conta de negócio recebe apenas a regex necessária e não tem a tag `administrator`/`management`

### A-3. Critério da tag `administrator` (permissão total implícita) —— conceda com cautela 【verificado】

- **Por quê**: **um usuário com a tag `administrator` tem permissão total sobre todos os vhosts que consegue ver, sem precisar de registro de permissão**
  (alinhado ao critério medido do RabbitMQ, ver README / design M8-7). Ou seja, basta dar essa tag e a regex de permissão deixa de fazer efeito — é a permissão máxima.
- **Como fazer**: só contas de gerenciamento/operação recebem `administrator`; contas de negócio nunca recebem tag e usam apenas a regex de permissão.

- **Como verificar (demonstrar a permissão implícita)**: em um novo vhost **sem nenhum registro de permissão**, o usuário `administrator` deve funcionar diretamente.
  **【verificado】** Medido nesta máquina: após criar o vhost `drillvh` (sem nenhum registro de permissão), o `guest` (administrator) declarou topologia nele com sucesso:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] A tag `administrator` é concedida apenas a pouquíssimas contas de operação

### A-4. `remote_access`: restringir a conta a login apenas local 【não verificado (não é possível simular origem remota na mesma máquina)】

- **Por quê**: em alinhamento ao RabbitMQ, o `guest` embutido, por padrão, só permite login local; em implantação exposta externamente, garanta que a origem das contas privilegiadas seja restrita.
- **Como fazer / critério (limitação importante)**:
  - `remote_access` só pode ser escrito no `users.<name>.remote_access` do **arquivo de configuração** e **só vale no primeiro bootstrap**;
  - **contas criadas pela API de gerenciamento / `swiftmqctl` são sempre `remote_access=true`** (permitem login de qualquer origem) —
    base: o comentário de `UpsertUser` em `internal/broker/observe.go` e o `"remote_access":true` medido no `meta/state.json`.
    Ou seja, **a API atualmente não consegue restringir uma conta a apenas login local**.
- **Como verificar**: conecte-se de **outra máquina** (não `127.0.0.1`) com essa conta; deve receber 403; a conexão local deve funcionar.
  **【não verificado】**: o ambiente desta máquina não permite construir uma origem remota real, então não foi medido.
- [ ] A restrição de origem das contas privilegiadas foi avaliada conforme o critério acima (atenção: contas criadas por API liberam o acesso remoto por padrão)

---

## B. Segurança de transporte (TLS)

Itens de configuração TLS (a camada de acesso e o plano de gerenciamento **compartilham** o mesmo conjunto de campos): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Habilitar TLS e recusar a inicialização se estiver mal configurado 【verificado】

- **Por quê**: os certificados são lidos e validados **na inicialização** — uma configuração errada recusa a inicialização imediatamente, em vez de só se expor quando o primeiro cliente se conecta.
- **Como fazer**: forneça `cert_file` + `key_file` em `listeners.<plugin>[].tls` ou em `management.tls` (**os dois juntos** é que habilitam).

- **Como verificar**: inicie com uma configuração errada; deve falhar imediatamente.
  **【verificado】** Nesta máquina, medidos três tipos de configuração errada, todos com `exit=1` e recusa de inicialização:

  ```
  badtls1: swiftmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: swiftmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: swiftmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Como verificar (positivo/negativo)**: um cliente TLS consegue conectar; um cliente em texto claro conectando à porta TLS é recusado.
  **【verificado】** Nesta máquina, ao subir uma instância TLS (`amqp091` com TLS) e usar uma sonda de cliente real:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] As portas de protocolo expostas externamente já têm TLS habilitado

### B-2. `min_version` no mínimo 1.2 【verificado】

- **Por quê**: desabilitar versões de TLS muito antigas; o padrão já é `1.2`, com opções `1.2` / `1.3`.
- **Como verificar**: coloque `min_version` como `1.0`; a inicialização deve dar erro (ver a saída `badtls2` em B-1).
- [ ] `min_version` é `1.2` ou `1.3`

### B-3. Autenticação mútua `client_auth: require_and_verify` (mTLS) 【parcialmente verificado】

- **Por quê**: exige que o cliente apresente e valide um certificado, impedindo que clientes não autorizados acessem a porta do protocolo.
- **Como fazer**: configure `ca_file` + `client_auth: require_and_verify` (estes dois exigem que `ca_file` seja fornecido ao mesmo tempo).
- **【verificado】**: o TLS ponta a ponta e o caminho de recusa foram validados com cliente real (B-1). **O mTLS (exigir e validar o certificado do cliente) não foi ensaiado separadamente nesta máquina**.
- [ ] As portas que precisam de mTLS têm `require_and_verify` + `ca_file`

### B-4. TLS do plano de gerenciamento 【não verificado】

- **Por quê**: o plano de gerenciamento transmite a senha via Basic Auth, portanto precisa ser criptografado.
- **Como fazer**: `management.tls` usa os mesmos campos dos listeners de protocolo.
- **【não verificado】**: o ensaio nesta máquina prendeu o plano de gerenciamento a uma porta em texto claro local; não foi subido HTTPS do plano de gerenciamento em separado.
- [ ] O plano de gerenciamento tem TLS habilitado (ou está rigorosamente restrito a uma rede confiável)

---

## C. Redução da superfície exposta

### C-1. Reduzir o escopo de escuta do plano de gerenciamento 【verificado (medição do endereço de escuta)】

- **Por quê**: o plano de gerenciamento, por padrão, fica em `:15672` (todas as placas de rede). Em implantação exposta externamente, prenda-o a um endereço de rede interna/loopback, ou limite a origem por firewall.
- **Como fazer**: configure `management.addr` como `127.0.0.1:15672` ou um endereço de rede interna; ou desligue totalmente com `management.enabled=false`
  (após desligar, não há porta de gerenciamento, mas o `swiftmqctl` também deixa de funcionar).
- **Como verificar**:
  **【verificado】** Nesta máquina, ao configurar o plano de gerenciamento como `127.0.0.1:15677`, mediu-se que o endereço de escuta é de fato o loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] O endereço de bind do plano de gerenciamento foi reduzido (ou foi desativado)

### C-2. Abrir apenas as portas de protocolo necessárias 【não verificado】

- **Por quê**: por padrão, AMQP `5672` e MQTT `1883` ficam abertas ao mesmo tempo; se não usar MQTT, desligue-a para reduzir a superfície de ataque.
- **Como fazer**: `plugins.mqtt.enabled=false` (ou remova de `listeners`); desativar significa **fechar a porta de verdade**, não apenas mudar um bit de estado.
- **Como verificar**: após desativar, a porta correspondente deixa de escutar (`Get-NetTCPConnection -State Listen` não a mostra).
  **【não verificado】**: o ensaio nesta máquina manteve os dois protocolos abertos; não foi verificado separadamente o desaparecimento da porta após o desligamento.
- [ ] Plugins de protocolo não usados foram desabilitados

---

## D. Reforço de execução em contêiner

Fatos da imagem do repositório (`Dockerfile`): binário com ligação estática + alpine, **roda como não-root (uid 10001, usuário `swiftmq`)**,
e o diretório de dados `/var/lib/swiftmq` é um volume. O `docker-compose.yml` usa **volume nomeado** para persistência, **montagem somente leitura** da configuração e rotação de logs.

### D-1. Execução como não-root 【não verificado (Docker não foi executado nesta máquina)】

- **Por quê**: privilégio mínimo, reduz o impacto após uma eventual evasão do contêiner.
- **Como fazer**: a imagem já é uid 10001 por padrão; **não** sobrescreva com `--user root`.
- **Como verificar**: `docker compose run -T --rm broker id` deve mostrar `uid=10001`. (em ambiente não interativo, `run` precisa obrigatoriamente de `-T`)
- [ ] O contêiner roda como não-root (não foi sobrescrito por root)

### D-2. Sistema de arquivos raiz somente leitura + limite de recursos + corte de capabilities (sugestão; o compose do repositório não habilita por padrão) 【não verificado】

- **Por quê**: um sistema de arquivos raiz somente leitura pode impedir a adulteração do binário em tempo de execução; os limites de recursos evitam que um único contêiner derrube o host; o corte de capabilities reduz a superfície de ataque do kernel.
- **Como fazer** (exemplo; mescle no serviço `broker` do compose conforme necessário):

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

- **Como verificar**: tentar escrever no caminho raiz dentro do contêiner deve falhar (somente leitura); `docker inspect` mostra o limite de recursos.
  **【não verificado】** (Docker não foi executado nesta máquina); além disso, **o sistema de arquivos raiz somente leitura exige confirmar que o `data_dir` está em um volume gravável**, caso contrário o núcleo não conseguirá gravar em disco.
- [ ] O sistema de arquivos raiz somente leitura e os limites de recursos foram avaliados (atenção: o `data_dir` precisa estar em um volume gravável)

---

## E. Limitações conhecidas (o que atualmente realmente não é possível; não conte com isso)

Os itens a seguir são **lacunas factuais**; reconheça-as explicitamente no design de segurança e não presuma que existem:

1. **As senhas são armazenadas e copiadas em texto claro**. No `meta/state.json`, o campo `password` é texto claro (**【verificado】** medido, é possível ver
   `"password":"drillpass"`); no arquivo de configuração também é texto claro. **Não** há hash de senha (o hash e os backends de autenticação externa ficam para os plugins de autenticação).
   → Consequência: **o diretório de dados e os arquivos de backup equivalem a credenciais sensíveis** e devem ter proteção de permissão de arquivo e criptografia.
2. **Não há log de auditoria**. As operações de criação/exclusão/alteração do plano de gerenciamento geram logs comuns (como `管理面更新用户 actor=... user=...`),
   mas **não** há um fluxo de auditoria independente e à prova de adulteração, nem registros de nível de conformidade de "quem alterou o quê e quando".
3. **Não há autenticação externa como LDAP / OAuth2 / JWT**. A v1 embutida tem apenas `PLAIN` / `AMQPLAIN`
   (`auth.Store.Mechanisms()` medido retorna apenas esses dois).
4. **SASL `EXTERNAL` não implementado**: mesmo com mTLS configurado, a camada de protocolo **ainda usa autenticação por senha PLAIN**
   (o passo de "usar o certificado do cliente para dispensar a senha" não existe). O certificado é apenas uma validação na camada de transporte.
5. **`remote_access` não pode ser definido via API**: contas criadas pela API/CLI de gerenciamento sempre permitem login remoto (ver A-4),
   e não é possível restringir uma conta individual a apenas login local.
6. **O plano de gerenciamento não tem lista de origem própria / nem limitação de taxa**: só é possível reduzir a superfície exposta por bind de endereço, firewall e TLS.
7. **Sem sandbox de plugins**: plugins de forma A ficam no mesmo processo do núcleo; plugins externos de forma B têm isolamento de processo, mas **o plano de dados usa um proxy de conexão local**
   e o plugin pode invocar semânticas do núcleo (sujeitas à validação de vhost e de permissões), **não** sendo um sandbox de segurança.
8. **As tags do plano de gerenciamento têm apenas três níveis, `administrator`/`management`/`monitoring`**, sem RBAC mais granular por recurso.

---

## F. Checklist resumido

- [ ] A-1 Conta padrão alterada/excluída 【fluxo verificado】
- [ ] A-2 Privilégio mínimo das contas de negócio (regex), sem tag de administrador 【caminho 403 verificado】
- [ ] A-3 A tag `administrator` é concedida apenas a contas de operação 【critério de permissão implícita verificado】
- [ ] A-4 Restrição de origem das contas privilegiadas avaliada (atenção: contas criadas por API liberam o remoto por padrão)
- [ ] B-1 TLS habilitado nas portas expostas, recusando a inicialização se mal configurado 【verificado】
- [ ] B-2 `min_version` ≥ 1.2 【verificado】
- [ ] B-3 Portas que precisam de mTLS com `require_and_verify` + `ca_file`
- [ ] B-4 TLS habilitado no plano de gerenciamento
- [ ] C-1 Endereço de bind do plano de gerenciamento reduzido 【endereço de escuta verificado】
- [ ] C-2 Plugins de protocolo não usados desabilitados
- [ ] D-1 Contêiner rodando como não-root
- [ ] D-2 Sistema de arquivos raiz somente leitura / limites de recursos / corte de capabilities avaliados
- [ ] E Limitações conhecidas (senha em texto claro, sem auditoria, sem LDAP/OAuth2, SASL EXTERNAL não implementado) reconhecidas no design de segurança
