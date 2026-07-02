# Onboarding de um cliente novo

Passo a passo para colocar um cliente novo replicando o Firebird on-prem dele
para o PostgreSQL na cloud, via push integrador (`fdb-agent` → `fdb-ingest`)
sobre mTLS.

Pré-requisito: a VPS já tem a CA criada (`ca-init`) e o `fdb-ingest` rodando.
Se ainda não tiver, faça primeiro a seção 1–3 do [`DEPLOY.md`](DEPLOY.md).

```
[cliente — Firebird, NAT, sem IP público]      [VPS — IP público]
 .fdb ──mmap──▶ fdb-agent ──outbound mTLS:8443──▶ fdb-ingest ──▶ postgres
                (só client cert)                  (creds do PG)    127.0.0.1
```

O cliente faz **apenas conexão outbound**. Nenhuma credencial do PostgreSQL vive
na máquina dele — o único segredo é o client cert daquele tenant.

---

## Lado VPS (servidor cloud)

### 1. Emitir o cert do tenant

CN do cert = id do tenant (usado no map `--tenant`).

```bash
fdb-ingest issue-client --pki pki --tenant CLIENTE --out-dir certs
#   -> certs/CLIENTE.crt  certs/CLIENTE.key
```

### 2. Registrar o tenant no gateway

Editar a unit `fdb-ingest.service`, adicionar um `--tenant CLIENTE=replica_CLIENTE`
(mapeia o CN do cert para o database de destino), depois recarregar:

```bash
sudo systemctl daemon-reload
sudo systemctl restart fdb-ingest
sudo journalctl -u fdb-ingest -f
```

O database `replica_CLIENTE` é criado automaticamente no primeiro FULL. Se for
pré-criar, dê ownership ao `fdb_writer`.

### 3. Entregar os arquivos ao cliente

Mandar para a máquina do cliente:

- `fdb-agent.exe`
- `ca.crt`
- `CLIENTE.crt` + `CLIENTE.key`

> ⚠️ **Nunca** enviar `ca.key` nem qualquer credencial do PostgreSQL.
> Se o client cert vazar, o blast radius é só os dados daquele tenant — nunca
> acesso ao PG.

---

## Lado cliente (on-prem, Firebird)

### 4. Copiar os arquivos

Colocar `fdb-agent.exe`, `ca.crt`, `CLIENTE.crt` e `CLIENTE.key` ao lado do `.fdb`
(ex.: `C:\fdb\`). O arquivo `.fdb` é aberto **read-only**.

### 5. Rodar o agent

**CDC contínuo** (seed FULL uma vez, depois stream de deltas). Rodar como serviço
de longa duração (via [nssm](https://nssm.cc) ou `sc create`):

```
fdb-agent.exe --gateway ingest.acme.com:8443 --server-name ingest.acme.com \
  --ca ca.crt --cert CLIENTE.crt --key CLIENTE.key \
  --database C:\firebird\BANCO.FDB --all-tables \
  --watch --watch-interval 5
```

Watermark persiste em `BANCO.FDB.cdc.json` — restart resume sem re-scan.

**Ou batch** (snapshot a cada 15 min) via Task Scheduler:

```bat
schtasks /Create /TN fdb-agent-batch /SC MINUTE /MO 15 /RL LIMITED /TR ^
 "C:\fdb\fdb-agent.exe --gateway ingest.acme.com:8443 --server-name ingest.acme.com ^
  --ca C:\fdb\ca.crt --cert C:\fdb\CLIENTE.crt --key C:\fdb\CLIENTE.key ^
  --database C:\firebird\BANCO.FDB --all-tables --unlogged"
```

---

## 6. Verificar

- [ ] Agent com o cert certo empurra; counts batem em `replica_CLIENTE`.
- [ ] Durante um FULL re-load a tabela nunca fica vazia (swap atômico de staging).
- [ ] Cert de outra CA é rejeitado no handshake (`fdb-ingest` loga
      `invalid peer certificate: UnknownIssuer`).
- [ ] CN de tenant desconhecido é rejeitado com `unknown tenant '<cn>'`.
- [ ] `psql` de fora da VPS para o PG **falha** (PG bound em localhost).

---

## Rotação / revogação

- **Rotacionar** um tenant: reemitir com `issue-client` e substituir cert/key no
  cliente. O cert antigo para de funcionar ao sair da trust da CA (ou via CRL).
- **Revogar**: remover o cert da trust e o `--tenant` da unit; restart.

Runbook completo (PKI, hardening do PostgreSQL, systemd): [`DEPLOY.md`](DEPLOY.md).
