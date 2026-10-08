# Backup e restauração do SpeedMQ

> As conclusões "medidas" neste documento vêm todas de um ensaio real em **Windows + PowerShell 5.1** (com `data_dir` e portas temporárias).
> Os comandos do ensaio e as saídas principais estão reproduzidos tal como estão no §6. As partes **【não verificado】** são marcadas explicitamente (backup/restauração de cluster, backup de volume Docker etc.).

---

## 1. O que fazer backup

Tudo sob `data_dir` **precisa ser copiado por inteiro**; o essencial são os itens abaixo (a disposição está no `upgrade.md` §3):

| Caminho | Função | O que se perde se faltar |
| --- | --- | --- |
| `meta/state.json` | Instantâneo de metadados do modo autônomo: vhost / exchanges / filas / bindings / usuários / permissões / políticas | Perde-se toda a topologia e as contas |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【Cluster】Log Raft / voto de mandato / snapshot + tabela de membros | Perde-se a identidade do cluster e a consistência dos metadados |
| `meta/users.seeded`, `meta/vhosts.seeded` | Marcadores de bootstrap | Perdê-los faz os users/vhosts da configuração serem **semeados novamente** (contas/vhosts excluídos ressuscitam) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Dados e índice das mensagens de filas clássicas | Perdem-se mensagens persistentes |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Cluster】Log/snapshot Raft das filas quórum | Perdem-se os dados das filas quórum |
| Arquivos de certificado (os PEM apontados por `cert_file`/`key_file`/`ca_file` na configuração) | Certificados TLS | Faça backup separado do `data_dir`; após reiniciar, o TLS não sobe |

> O estado mole (mensagens não confirmadas, consumidores, contador de prefetch) fica **somente na memória**, não vai para o disco, o backup **não** os inclui e nem deveria incluí-los.

---

## 2. Requisito de consistência: **é obrigatório parar o processo antes**; backup a quente **não é seguro**

### 2.1 Conclusão

- ✅ **Prática segura**: **pare o processo do broker** (a saída graciosa faz a descarga final em disco) e só então copie o `data_dir`.
- ❌ **Backup a quente (copiar os arquivos com o processo em execução): não é seguro e não há garantia.**

### 2.2 Por que o backup a quente não é seguro

O armazenamento de mensagens é composto por **dois arquivos** (o arquivo de segmento `*.seg` e o arquivo de índice `index/*.idx`), e os dois **não são confirmados de forma atômica**:

- Na recuperação, **o índice é a fonte de verdade** para decidir "quais mensagens estão vivas", e então o `(número do segmento, deslocamento, tamanho)` do índice é usado para ler o arquivo de segmento.
- No backup a quente é possível copiar um estado intermediário em que **o índice já referencia algo que ainda não foi totalmente gravado no segmento** (ou vice-versa):
  - o índice referencia um registro que não existe no segmento → essa mensagem **falha na leitura e é pulada** (equivale a perder mensagens persistentes já confirmadas);
  - o segmento tem o registro, mas o índice não o referencia → essa mensagem **não é recuperada**.
- Embora a recuperação use CRC32 para descartar **registros parcialmente gravados no fim**, isso cobre apenas "corrupção no fim de um único arquivo" e **não corrige a dessincronização entre índice e segmento**.

### 2.3 Sobre "quando a escrita chega ao disco" (observação medida)

- `fsync: os` + `flush_interval_ms: 200` (padrão): a mensagem é levada pela goroutine de descarga em segundo plano em **no máximo cerca de 200 ms** via `write()` ao sistema operacional
  (sem fsync), e o publisher confirm só é retornado depois disso.
- Medido: após publicar uma mensagem persistente, consultar **imediatamente** o tamanho do arquivo de segmento já mostra os dados (`t=0ms seg=832`); ou seja, "os bytes visíveis ao SO" ficam basicamente sincronizados com o confirm.
- **Atenção**: isso só significa "chegou ao buffer do SO"; **matar o processo à força não perde** (processo morto não perde o buffer do SO), mas **uma queda de energia perde**.
  Para ter "confirm recebido = já gravado com fsync no disco", mude `storage.fsync` para `batch` / `always`. **【cenário de queda de energia não medido】**

---

## 3. Passos de backup

### 3.1 Autônomo (recomendado)

```powershell
# 1) Pare o processo (em primeiro plano: Ctrl+C; em segundo plano: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Copie todo o data_dir (com carimbo de data/hora)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (Opcional) Valide que o snapshot de metadados do backup pode ser analisado
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Cada nó faz backup do seu próprio `data_dir`** (os metadados são replicados a todos via Raft; os dados das mensagens ficam no nó Owner; as réplicas das filas quórum ficam em cada diretório Raft).
- Ordem de parada: **pare um nó por vez**; não pare vários membros votantes ao mesmo tempo (ver `upgrade.md` §7.2).
- Para obter um **instantâneo consistente de todo o cluster**, é preciso parar todos os nós em sequência e copiar cada um; em produção, o mais comum é "parar/copiar/iniciar nó a nó".
- **【não verificado】** Não foi feito ensaio real de backup/restauração de cluster nesta máquina.

### 3.3 Docker (volume nomeado)

```powershell
# Após parar o contêiner, use um contêiner efêmero para empacotar e extrair o conteúdo do volume
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【não verificado】** (Docker não foi executado nesta máquina).

---

## 4. Passos de restauração

### 4.1 Autônomo

```powershell
# 1) Confirme que o processo parou
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) Afaste (ou exclua) o data_dir atual, para não misturar arquivos novos e antigos
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) Restaure a partir do backup
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) Inicie
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Pontos-chave:
- **É obrigatório afastar primeiro o diretório antigo**; não se pode "sobrepor os arquivos do backup a um diretório parcialmente preenchido";
- O `data_dir` restaurado precisa ter o **mesmo conjunto de vhosts/filas** do momento do backup (os nomes de diretório são codificados e funcionam em outras máquinas);
- **Não** aproveite a restauração para alterar `vhosts`/`users` no arquivo de configuração (só valem no primeiro bootstrap; alterar não adianta, ver `upgrade.md` §4.2).

### 4.2 Cluster

- Restaurar um único nó: restaure o `data_dir` desse nó conforme o §4.1 e inicie; ele reentra como membro existente e alcança o log Raft.
- Restaurar o cluster inteiro: **restaure e inicie primeiro os nós da maioria** (≥ metade dos membros votantes) para que o cluster consiga eleger um leader; depois restaure os demais nós.
- **【não verificado】** A restauração de cluster não foi medida.

---

## 5. Como validar depois da restauração

Cruze as informações pela API de gerenciamento e por um cliente real (recomenda-se fazer tudo):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Totais de objetos e de mensagens (número de filas/exchanges/bindings/usuários; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Conferir messages / messages_ready fila a fila (pode comparar com o registrado antes do backup)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Se vhost / usuários / políticas continuam todos presentes
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Cluster (no modo autônomo retorna enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Veja o log de inicialização**: devem aparecer `已从磁盘恢复队列消息 ... messages=N` e `队列已恢复持久化消息 ... messages=N`; N deve ser igual ao de antes do backup.
- **Avisos no log**: se alguma fila já teve mensagens consumidas/esvaziadas antes, na recuperação podem aparecer
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` e `恢复时清理了无存活消息的段`.
  Esses são resíduos de índice de **registros já liquidados (ack/purge)** e são **ruído de log conhecido, que não afeta a correção dos dados** (ver §7).
- **Cliente real**: recupere as mensagens da fila e confira a quantidade/conteúdo (ver §6 passo (7)).

---

## 6. Ensaio medido (comandos e saídas reais)

> Ambiente: `data_dir` em diretório temporário, AMQP `127.0.0.1:5676`, plano de gerenciamento `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> conta padrão `guest/guest`. Log de inicialização:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Criar topologia durable + publicar 5 mensagens persistentes (cliente real `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Criar usuário / vhost / permissão / política (API de gerenciamento)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Estado antes do backup (API de gerenciamento)**

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

**(4) Arquivos em disco antes do backup**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Parar o processo → backup → esvaziar → restaurar**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Log de recuperação após reiniciar (linhas-chave)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Simultaneamente aparecem várias `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> são resíduos de índice deixados pelas mensagens **anteriormente removidas por purge** neste ensaio (já liquidadas, dados de segmento recuperados), e **não afetam a recuperação das 5 mensagens abaixo**.

**(7) Asserções após a restauração: API de gerenciamento + cliente real**

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

**Conclusão**: topologia durable (exchange + fila + binding), 5 mensagens persistentes, usuário, vhost, permissões e políticas **foram todos restaurados**,
e o cliente real conseguiu recuperar todas as mensagens tal como estavam. **Ensaio aprovado.**

### 6.1 Comparação: a restauração de uma fila sem consumo/purge é mais "silenciosa"

Para distinguir se os WARN acima são um fenômeno geral, foi feito outro **comparativo controlado**: criar a fila durable `clean.q`, publicar 3 mensagens persistentes,
**sem consumir nem esvaziar**, parar o processo e reiniciar:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Nenhum WARN**. Isso mostra que o WARN só aparece no cenário em que "o índice ainda guarda registros já liquidados" (ver §7).

---

## 7. Problemas conhecidos e limitações (registrados com honestidade)

1. **Ruído no log de recuperação (observado de fato)**: quando a fila passou historicamente por consumo/esvaziamento (mensagens já com ack/purge),
   seu índice ainda mantém referências a registros já recuperados, e na recuperação é impresso **um WARN `恢复消息失败，已跳过` para cada registro**,
   além de criar/limpar um `000000.seg` vazio (log `恢复时清理了无存活消息的段`).
   **Não afeta a correção dos dados** (as mensagens vivas não confirmadas são recuperadas corretamente), mas **polui o log** e, em filas grandes/alta vazão, pode inundar a tela.
   Recomendação: tome como referência `已从磁盘恢复队列消息 ... messages=N` e ignore esses WARN voltados a registros já liquidados;
   se o volume de log for inaceitável, reporte aos mantenedores do núcleo (este documento não altera código).
2. **Backup a quente não é seguro** (§2): não copie o `data_dir` com o processo em execução.
3. **`fsync: os` não garante ausência de perda em queda de energia**: para "confirm = já em disco", use `batch` / `always`.
4. **Senha em texto claro**: no `meta/state.json`, as senhas dos usuários ficam **em texto claro** (medido, é possível ver `"password":"drillpass"`) —
   portanto os arquivos de backup **devem ser tratados como dados sensíveis** (controle de acesso, armazenamento cifrado). Detalhes em `security-baseline.md`.
5. **【não verificado】** Backup/restauração de cluster, backup/restauração de volume Docker, cenário de queda de energia, escrita concorrente durante a restauração.
