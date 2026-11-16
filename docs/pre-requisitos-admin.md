# Pré-requisitos para o administrador (antes do workshop)

Este guia resume **tudo o que o admin do cliente deve preparar** antes do workshop de **Lakeflow
SDP + Data Quality com Genie code**. A ideia central:

> Compartilha-se **apenas o catálogo**. Cada participante cria o seu **próprio schema**
> (`lakeflow_workshop_<usuario>`) ao rodar o notebook e, por ser **owner** desse schema, herda
> automaticamente as permissões para criar seus volumes, tabelas e o seu pipeline. **Não é preciso
> conceder permissões objeto por objeto.**

---

## 1. Resumo (o mínimo)

| # | O quê | Detalhe |
|---|---|---|
| 1 | **Grupo** | Um grupo (ex. `workshop_lakeflow`) com os participantes. |
| 2 | **Catálogo compartilhado** | Com *managed storage* e vinculado/acessível ao workspace do workshop. |
| 3 | **Privilégios UC** | Apenas `USE CATALOG` + `CREATE SCHEMA` sobre o catálogo, ao grupo. |
| 4 | **Entitlements** | **Workspace access** (obrigatório); **Databricks SQL access** (opcional). |
| 5 | **Compute** | **Serverless** habilitado para notebooks e pipelines (ou um cluster compartilhado). |

---

## 2. Por que basta `CREATE SCHEMA`

No Unity Catalog, **quem cria um objeto é o seu owner** e tem **todos os privilégios** sobre ele.
Quando um participante roda o notebook:

1. Cria o seu schema `lakeflow_workshop_<usuario>` → fica como **owner**.
2. Como owner, pode criar dentro dele: **volumes**, **tabelas**, **streaming tables** e
   **materialized views** (as que o pipeline SDP gera).

Por isso **não** é preciso conceder `CREATE VOLUME`, `CREATE TABLE`, etc. — são herdados.

---

## 3. SQL exato para o admin

```sql
-- (1) Criar o grupo 'workshop_lakeflow' no console de administração
--     e adicionar os participantes (Admin Settings > Identity and access > Groups).

-- (2) Privilégios sobre o catálogo compartilhado (substitua <CATALOGO>):
GRANT USE CATALOG   ON CATALOG <CATALOGO> TO `workshop_lakeflow`;
GRANT CREATE SCHEMA ON CATALOG <CATALOGO> TO `workshop_lakeflow`;
```

> O catálogo deve ter uma **localização de armazenamento gerenciada** (managed storage) para poder
> criar volumes e tabelas gerenciadas, e estar **vinculado** ao workspace do workshop (Workspace
> binding, se o catálogo for *isolated*).

---

## 4. Entitlements do grupo (console de administração)

| Entitlement | Obrigatório? | Para quê |
|---|---|---|
| **Workspace access** | ✅ Sim | Criar notebooks, **pipelines** e jobs. |
| **Databricks SQL access** | ⬜ Opcional | Consultar resultados no SQL editor / warehouse. |
| **Allow unrestricted cluster creation** | ❌ Não | Não é necessário se usarem serverless. |

---

## 5. Compute

- **Opção recomendada — Serverless:** habilitar **Serverless** para notebooks e para pipelines (o
  mais simples, sem gerenciar clusters). Se a organização usa **budget policies** de serverless,
  atribuir uma ao grupo `workshop_lakeflow`.
- **Alternativa — Cluster compartilhado:** um cluster all-purpose com permissão **Can Attach To**
  para o grupo, caso não haja serverless disponível.

---

## 6. O que NÃO é preciso

- ❌ Grants de `CREATE VOLUME` / `CREATE TABLE` (herdados como owner do schema).
- ❌ `ALL PRIVILEGES` sobre o catálogo.
- ❌ Papel de administrador de metastore.
- ❌ Permissão especial para "criar jobs": o workshop roda o notebook de forma **interativa**
  (Run all). Criar **pipelines** já vem com *Workspace access*.

---

## 7. Mensagem pronta para enviar ao admin

> **Assunto: Preparação do ambiente — Workshop Lakeflow SDP + Data Quality**
>
> Olá [nome], para o workshop precisaríamos que você preparasse o seguinte no workspace
> **[URL do workspace]** antes da sessão:
>
> **1. Grupo de participantes**
> - Criar um grupo (ex. `workshop_lakeflow`) e adicionar os participantes.
>
> **2. Um catálogo compartilhado para o workshop** (ex. `workshop_varejo`)
> - Que tenha *managed storage* e esteja vinculado/acessível a este workspace.
> - Conceder ao grupo apenas estes dois privilégios:
>   ```sql
>   GRANT USE CATALOG   ON CATALOG workshop_varejo TO `workshop_lakeflow`;
>   GRANT CREATE SCHEMA ON CATALOG workshop_varejo TO `workshop_lakeflow`;
>   ```
> - Com isso, cada participante cria o seu **próprio schema** (`lakeflow_workshop_<usuario>`) e fica
>   como owner, com permissão para criar seus volumes, tabelas e o seu pipeline, sem atrapalhar
>   ninguém.
>
> **3. Entitlements do grupo**
> - **Workspace access** (habilita notebooks e pipelines).
> - (Opcional) **Databricks SQL access** para consultar resultados.
>
> **4. Compute**
> - **Serverless habilitado** para notebooks e pipelines (se usam budget policy de serverless,
>   atribuir uma ao grupo). Se não houver serverless, um cluster compartilhado com *Can Attach To*
>   para o grupo.
>
> Nada mais: não são necessárias permissões de administrador de metastore, nem grants a nível de
> tabela/volume (os participantes os herdam ao criar o seu próprio schema). Obrigado!
