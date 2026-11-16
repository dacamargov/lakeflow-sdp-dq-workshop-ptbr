# Módulos avançados de Lakeflow SDP (opcional)

Depois de construir o pipeline básico bronze → silver → gold (ver `README.md`, seção 5), estes
módulos levam o workshop ao que o **Lakeflow SDP** tem de mais interessante. Todos continuam sendo
feitos **com Genie code** dentro do pipeline.

> 🔁 **Pré-requisito de dados:** os módulos de **ingestão incremental**, **evolução de schema** e
> **CDC/SCD2** precisam de um segundo lote de dados. Rode o notebook
> [`notebooks/02_lote_incremental.py`](../notebooks/02_lote_incremental.py) **depois** do `01`
> (ele adiciona arquivos `*_lote2` ao seu landing, sem apagar nada, incluindo uma coluna nova
> `cupom_desconto` e versões atualizadas de clientes).

Cada prompt assume que você já está dentro do seu pipeline Lakeflow SDP, apontando para o seu
schema `lakeflow_workshop_<usuario>`.

---

## 1. CDC / SCD Tipo 2 com `AUTO CDC` ⭐

A feature mais marcante do SDP: historizar mudanças de uma dimensão **sem escrever MERGE**. Use as
versões atualizadas de clientes que o notebook 02 gera.

> **Prompt:** "Com Lakeflow SDP, crie uma dimensão `dim_clientes_scd2` do tipo SCD Tipo 2 usando
> AUTO CDC a partir da tabela bronze de clientes. Use `id_cliente` como chave e `data_atualizacao`
> como coluna de sequência. Quero manter o histórico de mudanças de `segmento` e `nivel_fidelidade`,
> com colunas de início e fim de vigência e uma flag da versão atual."

- O que observar: ao reler, cada cliente atualizado tem **duas linhas** (a versão antiga fechada e a
  nova vigente). Em SCD Tipo 1 só ficaria a última.
- Variante: peça também uma versão **SCD Tipo 1** (`dim_clientes_atual`) para comparar.

---

## 2. Quarentena de dados ruins (não só descartar) ⭐

Em vez de `expect_or_drop` (que joga fora a linha), roteie as linhas ruins para uma tabela de
quarentena para auditoria/reprocessamento.

> **Prompt:** "Com Lakeflow SDP, a partir da bronze de pedidos crie duas tabelas: `pedidos_validos`
> com as linhas que passam nas expectativas (valor_total numérico e >= 0, id_cliente e id_loja
> existentes, moeda válida) e `pedidos_quarentena` com as linhas que **falham**, adicionando uma
> coluna `motivo_falha` que indique qual regra foi violada e a data de detecção."

- O que observar: `count(pedidos_validos) + count(pedidos_quarentena)` = total da bronze. Nada se
  perde.

---

## 3. Observabilidade com o Event Log do pipeline ⭐

O SDP grava um **event log** com métricas de cada expectativa. Dá para publicá-lo como tabela e
fechar o ciclo de qualidade.

> **Prompt:** "Com Lakeflow SDP, crie uma tabela `qualidade_expectativas` que leia o event log do
> pipeline (eventos do tipo `flow_progress`) e extraia, por dataset e por expectativa, quantos
> registros passaram e quantos falharam. Depois crie uma view que compare esses números contra a
> tabela `_log_geracao` (erros injetados) para mostrar **injetado vs. detectado**."

- Dica: o event log pode ser consultado com a função `event_log(...)` ou publicado via a
  configuração `event_log` do pipeline. No SQL: `SELECT * FROM event_log(TABLE(<tabela_do_pipeline>))`.

---

## 4. Ingestão incremental (streaming de verdade)

Depois de rodar o notebook 02 (que adiciona `pedidos_lote2.json`), rode **de novo** o seu pipeline
e observe no event log que o Auto Loader processou **apenas os arquivos novos**.

> **Prompt (se ainda não tiver bronze streaming):** "Com Lakeflow SDP, garanta que a bronze de
> pedidos é uma **streaming table** com Auto Loader, de forma que uma nova execução só processe os
> arquivos novos que chegaram ao diretório de landing, sem reprocessar os antigos."

- O que observar: `num_output_rows` da bronze no segundo update ≈ só as linhas do lote 2.

---

## 5. Evolução de schema

O lote 2 de `pedidos` traz a coluna nova `cupom_desconto`. Mostre que o pipeline a absorve sem
quebrar.

> **Prompt:** "Com Lakeflow SDP e Auto Loader, configure a ingestão de pedidos com
> **evolução de schema** (`cloudFiles.schemaEvolutionMode = addNewColumns`) para que a nova coluna
> `cupom_desconto` seja adicionada automaticamente ao bronze quando aparecer nos novos arquivos."

- O que observar: a coluna aparece no bronze, **nula** nas linhas antigas e preenchida nas novas.

---

## 6. Qualidade dirigida por metadados

Em vez de regras fixas no código, leia as expectativas de uma **tabela de configuração** — escala
para dezenas de regras.

> **Prompt:** "Com Lakeflow SDP, crie uma tabela `regras_qualidade` (coluna_alvo, nome_regra,
> condicao_sql, acao) com várias regras de pedidos, e aplique essas expectativas dinamicamente na
> silver lendo as regras dessa tabela (usando `expect_all`), em vez de escrevê-las uma a uma."

---

## 7. Append flows (unificar várias fontes)

Simule que cada região chega como um arquivo separado e una tudo em uma só streaming table.

> **Prompt:** "Com Lakeflow SDP, crie uma streaming table `pedidos_todos` e vários **append flows**
> que façam ingestão de diferentes arquivos/pastas de pedidos (por exemplo, por lote) para a mesma
> tabela de destino, sem duplicar a definição da tabela."

---

## 8. Genie Space sobre a camada gold (fecha o tema "Genie")

Fora do pipeline, crie um **Genie Space** sobre as tabelas gold para perguntar em linguagem natural.

> Passos: **Genie → New → selecione o schema `lakeflow_workshop_<usuario>`** e as tabelas gold
> (fato de vendas + dimensões). Faça perguntas como *"vendas por estado no último mês"* ou
> *"top 10 produtos por receita"*. Amarra o fio: você construiu o pipeline com Genie code e agora
> consulta com Genie.

---

## 9. Dashboard AI/BI sobre a gold

Crie um dashboard AI/BI com os KPIs mensais da camada gold.

> Passos: **Dashboards → Create → use o SQL warehouse** e as tabelas gold. Gráficos sugeridos:
> receita por mês, receita por loja/estado, mix por categoria. Opcionalmente peça ao Assistant para
> gerar as queries.

---

### Sugestão de sequência para o workshop

1. Pipeline base bronze/silver/gold (README seção 5).
2. **Quarentena (#2)** e **Event Log (#3)** → tema Data Quality.
3. Rodar notebook 02 → **Incremental (#4)** + **Evolução de schema (#5)**.
4. **SCD2 (#1)** com os clientes atualizados.
5. Remate: **Genie Space (#8)** e/ou **Dashboard (#9)**.
