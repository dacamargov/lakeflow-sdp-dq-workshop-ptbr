# Workshop · Lakeflow SDP + Data Quality com Genie code

Workshop hands-on para construir um pipeline de dados completo com Databricks Lakeflow Spark
Declarative Pipelines (SDP) e qualidade de dados, tudo com Genie code (Databricks Assistant) e sem
escrever o código à mão.

O percurso é o seguinte:

1. Você roda um notebook que gera dados crus de um varejista brasileiro fictício (Mercado Verde) e
   os deixa como arquivos num volume do Unity Catalog.
2. A partir daí, constrói o pipeline com Genie code. Primeiro o medallion bronze → silver → gold
   com expectativas de qualidade, deduplicação e métricas; depois os recursos que fazem o SDP valer
   a pena: CDC/SCD2, quarentena de dados, observabilidade pelo event log, ingestão incremental e
   evolução de schema.

Cada pessoa roda o notebook no seu próprio workspace, então tudo é parametrizado — não há nada fixo
no código.

---

## 1. Pré-requisitos

| Requisito | Detalhe |
|---|---|
| **Workspace Databricks** | Com **Unity Catalog** habilitado. |
| **Compute Serverless** | O notebook foi pensado para serverless (ou qualquer cluster com Spark). Não instala bibliotecas. |
| **Um catálogo existente** | Você precisa de um catálogo sobre o qual tenha `USE CATALOG`, `CREATE SCHEMA` e `CREATE VOLUME`. O notebook **cria o schema e o volume**, mas **não cria o catálogo**. |
| **Genie code / Databricks Assistant** | Para a segunda parte do workshop (construir o pipeline). |
| **Databricks CLI** (opcional) | Só se preferir importar o notebook pela linha de comando em vez da UI. |

> O notebook usa **apenas a biblioteca padrão de Python + Spark**: não faz `pip install` de nada,
> então roda igual em qualquer workspace.

> 👤 **Você é o admin do cliente?** O que você deve preparar antes do workshop (grupo, permissões de
> catálogo, entitlements e compute) está em
> **[`docs/pre-requisitos-admin.md`](docs/pre-requisitos-admin.md)**, com o SQL exato e uma mensagem
> pronta para reenviar.

---

## 2. Configuração por variáveis de ambiente

Como o notebook roda em **workspaces diferentes**, cada parâmetro é resolvido nesta ordem:

1. **Variável de ambiente** (`WORKSHOP_*`) — útil para jobs / automação.
2. **Widget** do notebook — útil para rodar à mão pela UI.
3. **Valor padrão**.

| Parâmetro (widget) | Variável de ambiente | Padrão | Descrição |
|---|---|---|---|
| `catalogo`       | `WORKSHOP_CATALOG`     | `dacamargovws_catalog` | Catálogo de destino, **compartilhado** (**troque pelo seu**). |
| `volume`         | `WORKSHOP_VOLUME`      | `landing`              | Volume para os arquivos crus. |
| `n_clientes`     | `WORKSHOP_N_CUSTOMERS` | `2500`                 | Tamanho do universo de clientes. |
| `n_pedidos`      | `WORKSHOP_N_ORDERS`    | `15000`                | Número de pedidos a gerar. |
| `pct_erro`       | `WORKSHOP_PCT_ERROR`   | `0.15`                 | Proporção de registros com erro de negócio injetado. |
| `semente`        | `WORKSHOP_SEED`        | `20260101`             | Semente de reprodutibilidade. |
| `limpar_landing` | `WORKSHOP_CLEAN`       | `true`                 | Esvazia o volume antes de escrever (reexecução idempotente). |

> **Participantes:** a única coisa que você *precisa* ajustar é **`catalogo`** para apontar para um
> catálogo onde você tenha permissões. O resto funciona com os padrões.

### Vários participantes ao mesmo tempo

Compartilha-se **apenas o catálogo**. Cada participante escreve no seu **próprio schema**, derivado
automaticamente de `current_user()` (`lakeflow_workshop_{usuario}`), com seu próprio volume de
`landing` dentro. Não há nada para configurar nem coordenar.

```
dacamargovws_catalog                          ← único recurso compartilhado
├── lakeflow_workshop_ana_souza               ← schema por usuário
│   ├── landing/                              ← volume por usuário (dentro do seu schema)
│   ├── _log_geracao
│   └── (bronze/silver/gold que você cria com Genie code)   ← suas tabelas, isoladas
├── lakeflow_workshop_maria_souza
│   └── ...
└── lakeflow_workshop_joao_pereira
    └── ...
```

Por que assim: tendo cada um o seu próprio schema, **as tabelas do pipeline que você construir
depois com Genie code ficam isoladas** e o catálogo não vira um pântano de tabelas de todo mundo.
Cada participante só precisa de `USE CATALOG` + `CREATE SCHEMA` sobre o catálogo compartilhado.

> Por exemplo, para `ana.souza@empresa.com` o destino seria
> `<catalogo>.lakeflow_workshop_ana_souza`, com o volume `landing` dentro.

Para fixar por variável de ambiente (por exemplo num cluster ou job):

```bash
export WORKSHOP_CATALOG=meu_catalogo
```

---

## 3. Como executar o notebook

### Opção A — Pela UI (recomendada para o workshop)

1. Suba `notebooks/01_gerar_dados_varejo.py` no seu workspace
   (**Workspace → Import → File**). O Databricks o reconhece como notebook Python.
2. Abra-o e conecte ao **Serverless**.
3. **Execute só a primeira célula de código** (a de configuração, com `dbutils.widgets...`).
   Isso **habilita os widgets** que aparecem no topo do notebook.
4. No widget **`catalogo`**, escreva o seu catálogo (e ajuste os outros se quiser).
5. Agora **Run all** (ou rode o resto das células). Em ~1–2 minutos os dados ficam no volume.

> ⚠️ Os widgets **só aparecem depois de executar a célula que os cria**. Se você fizer *Run all* de
> uma vez sem passar pelos passos 3–4, o notebook usa os **valores padrão** (incluindo o catálogo
> `dacamargovws_catalog`).

### Opção B — Com a CLI

```bash
# Autenticar (uma única vez)
databricks auth login -p meu-perfil

# Importar o notebook
databricks workspace import \
  "/Users/<seu-usuario>/lakeflow_workshop/01_gerar_dados_varejo" \
  --file notebooks/01_gerar_dados_varejo.py \
  --language PYTHON --format SOURCE --overwrite -p meu-perfil
```

Depois execute-o pela UI, ou como run de job serverless apontando para esse notebook.

Ao terminar, o notebook imprime um resumo JSON e valida a viagem de ida e volta
(escrever → reler). Se você vir `"bate": true`, os dados estão prontos.

---

## 4. O que ele gera

São criados **5 datasets crus** em `{catalogo}.lakeflow_workshop_{usuario}`, dentro do volume
`landing`:

```
/Volumes/<catalogo>/<schema>/landing/
├── clientes/       clientes.json        (dimensão)
├── produtos/       produtos.csv         (dimensão)
├── lojas/          lojas.csv            (dimensão)
├── pedidos/        pedidos.json         (fato, cabeçalho)
└── pedidos_itens/  pedidos_itens.json   (fato, grão de linha)
```

Mais uma tabela de controle: `{catalogo}.lakeflow_workshop_{usuario}._log_geracao` com a contagem de
registros e **quantos erros foram injetados de propósito** (para contrastar *injetado vs. detectado*
quando suas regras de qualidade estiverem rodando).

### Modelo (estrela)

```
         clientes ─┐
         lojas    ─┼──< pedidos >──< pedidos_itens >── produtos
```

São **dados crus e transacionais**: uma linha = um evento. **Não há nenhum agregado**
(`total_`, `sum_`, `avg_`): essas métricas são calculadas pela camada *gold* que você constrói com
Genie code.

### Qualidade de dados intencional

Tudo é escrito **como texto**, e a sujeira é deliberada para que as regras de qualidade tenham algo
para capturar:

- **Ruído cosmético** (sempre): maiúsculas/minúsculas, acentos, espaços, formatos de data e
  telefone diferentes, estado escrito de várias formas (`SP`, `São Paulo`, `SAO PAULO`…).
- **Erros de negócio** (conforme `pct_erro`):
  - **clientes:** email inválido ou vazio, data de cadastro futura, data ilegível, nome muito
    curto, cidade vazia, duplicados.
  - **produtos:** preço negativo ou não numérico, margem negativa (preço < custo), categoria
    vazia, SKU duplicado.
  - **lojas:** latitude/longitude fora de faixa, cidade vazia.
  - **pedidos:** cliente/loja órfãos (FK inexistente), valor negativo ou não numérico, data futura,
    status/moeda desconhecidos, pedido duplicado.
  - **pedidos_itens:** quantidade inválida, preço negativo, desconto fora de `[0,100]`, SKU órfão,
    número de linha não numérico.

---

## 5. Próximo passo: construir o pipeline com Genie code

Esta é a parte central do workshop. Com os dados já no volume, abra um
**Lakeflow Spark Declarative Pipeline** e peça ao **Genie code** para construí-lo por camadas.

> 🔑 **Cada participante usa OS SEUS próprios valores.** Como os dados vivem no seu schema
> `lakeflow_workshop_{usuario}`, os prompts apontam para a **sua** rota de landing. Para ninguém
> errar, **o notebook imprime no final (seção 12) os 4 prompts já preenchidos com os seus valores**
> — basta copiar e colar no Genie code dentro do pipeline.

Não precisa criar nem nomear o pipeline à mão: **deixe o Genie code criá-lo** ao colar os prompts.
Mesmo que dois participantes terminem com um pipeline de mesmo nome, cada um fica no nome do seu
**owner** e escreve no **seu próprio schema**, então não há colisão de dados.

> 💡 Para encontrar o seu na lista **Jobs & Pipelines → Pipelines**, filtre por **"Created by me"**
> (ou owner): você vê só o seu, mesmo que haja nomes repetidos.

Todos os prompts assumem um pipeline **Lakeflow SDP (Spark Declarative Pipelines)** e reforçam isso
explicitamente, para que o Genie use os idioms do SDP.

**Bronze (ingestão crua com Auto Loader):**
> "Usando Lakeflow SDP (Spark Declarative Pipelines), crie a camada bronze com tabelas de streaming
> que façam ingestão com Auto Loader dos arquivos de `<SEU_CAMINHO_LANDING>/` — `clientes`,
> `pedidos` e `pedidos_itens` são JSON, `produtos` e `lojas` são CSV com header. Deixe todas as
> colunas como string e adicione colunas de linhagem com o nome do arquivo e a data de ingestão."

**Silver (tipagem, normalização e expectativas de qualidade):**
> "Com Lakeflow SDP, crie a camada silver com tabelas que tipem e normalizem cada entidade a partir
> das tabelas bronze. Adicione expectativas de qualidade (expectations do SDP): email com formato
> válido, `valor_total` numérico e >= 0, `data_pedido` não futura, `desconto_pct` entre 0 e 100,
> quantidades > 0, e que `id_cliente` e `id_loja` existam nas suas dimensões. Normalize o estado
> para a sigla de UF (2 letras) e deduplique clientes e produtos."

**Gold (modelo dimensional e métricas):**
> "Com Lakeflow SDP, crie a camada gold a partir das tabelas silver, com tabelas de dimensões limpas
> de clientes, produtos e lojas, um fato de vendas no grão de linha, e KPIs mensais de vendas por
> loja e por categoria. Use `CLUSTER BY` em vez de particionar."

**Observabilidade de qualidade:**
> "Com Lakeflow SDP, crie a camada de observabilidade de qualidade com uma tabela que publique as
> métricas das expectativas do pipeline e a compare contra
> `{catalogo}.lakeflow_workshop_{usuario}._log_geracao` para mostrar *injetado vs. detectado*."

> O objetivo pedagógico: ver como o Genie code monta o DAG, a incrementalidade e a qualidade
> declarativa com muito menos código do que um pipeline tradicional.

---

Com o pipeline base no ar, o workshop segue para os **recursos de SDP além do medallion** — o que
realmente diferencia o Lakeflow SDP. Todos continuam sendo construídos com Genie code dentro do
mesmo pipeline, apontando para o seu schema `lakeflow_workshop_<usuario>`.

Os recursos de CDC/SCD2, ingestão incremental e evolução de schema trabalham sobre um segundo lote
de dados: rode o notebook [`notebooks/02_lote_incremental.py`](notebooks/02_lote_incremental.py)
depois do `01` (ele preserva o landing e só acrescenta arquivos novos, com a coluna `cupom_desconto`
e versões atualizadas de clientes). Roda igual ao notebook 01.

## 6. CDC / SCD Tipo 2 com `AUTO CDC`

Historizar as mudanças de uma dimensão **sem escrever MERGE**, usando as versões atualizadas de
clientes que o notebook 02 gera.

> "Com Lakeflow SDP, crie uma dimensão `dim_clientes_scd2` do tipo SCD Tipo 2 usando AUTO CDC a
> partir da tabela bronze de clientes. Use `id_cliente` como chave e `data_atualizacao` como coluna
> de sequência. Mantenha o histórico de mudanças de `segmento` e `nivel_fidelidade`, com colunas de
> início e fim de vigência e uma flag da versão atual."

Cada cliente atualizado fica com duas linhas (a versão antiga fechada e a nova vigente). Como
variante, peça uma versão SCD Tipo 1 (`dim_clientes_atual`) para comparar.

## 7. Quarentena de dados ruins

Em vez de `expect_or_drop` (que descarta a linha), roteie as linhas ruins para uma tabela de
quarentena, para auditar ou reprocessar.

> "Com Lakeflow SDP, a partir da bronze de pedidos crie duas tabelas: `pedidos_validos` com as
> linhas que passam nas expectativas (valor_total numérico e >= 0, id_cliente e id_loja existentes,
> moeda válida) e `pedidos_quarentena` com as linhas que falham, adicionando uma coluna
> `motivo_falha` que indique qual regra foi violada e a data de detecção."

No fim, `count(pedidos_validos) + count(pedidos_quarentena)` = total da bronze: nada se perde.

## 8. Observabilidade com o Event Log do pipeline

O SDP grava um event log com as métricas de cada expectativa (quantos registros passam e falham por
dataset). Para fechar o ciclo de qualidade, publique-o no Unity Catalog como tabela consultável e
compare-o contra os erros que você injetou.

**Passo 1 — Publique o event log no Unity Catalog.** Na configuração do pipeline (**Settings →
Advanced**, ou o JSON do pipeline) adicione um destino para o event log no seu próprio schema:

```json
"event_log": {
  "catalog": "<seu_catalogo>",
  "schema": "lakeflow_workshop_<usuario>",
  "name": "event_log_pipeline"
}
```

Assim o event log vira uma **tabela normal** no seu schema: consultável e compartilhável. A função
`event_log(...)` direta só pode ser lida pelo owner do pipeline e não pode ser compartilhada, por
isso publicá-lo é o caminho recomendado.

**Passo 2 — Construa a observabilidade com Genie code:**
> "Com Lakeflow SDP, crie uma view em SQL `qualidade_expectativas` que leia a tabela do event log
> publicado (eventos do tipo `flow_progress`) e extraia, por dataset e por expectativa, quantos
> registros passaram e quantos falharam na última atualização terminada. Depois compare-a contra a
> tabela `_log_geracao` (erros injetados) para mostrar injetado vs. detectado, com a diferença e o %
> de detecção. Calcule todos os contagens ao vivo a partir das tabelas (incluindo os duplicados,
> como bronze menos silver): não deixe nenhum número fixo no SQL. Use uma **view normal** (não
> materializada) para que sempre reflita a última execução terminada."

> 💡 Uma view normal é calculada ao consultá-la, então sempre mostra a última atualização terminada;
> uma view materializada se atualizaria durante a mesma execução que ainda está escrevendo o event
> log e ficaria uma atualização atrasada. Na primeira execução a view estará vazia até o pipeline
> terminar — é o esperado.

## 9. Ingestão incremental (streaming de verdade)

Depois de rodar o notebook 02 (que adiciona `pedidos_lote2.json`), rode de novo o seu pipeline e
observe no event log que o Auto Loader processou só os arquivos novos.

> "Com Lakeflow SDP, garanta que a bronze de pedidos é uma streaming table com Auto Loader, de forma
> que uma nova execução só processe os arquivos novos que chegaram ao diretório de landing, sem
> reprocessar os antigos."

O `num_output_rows` da bronze no segundo update deve ser ≈ só as linhas do lote 2.

## 10. Evolução de schema

O lote 2 de `pedidos` traz a coluna nova `cupom_desconto`; mostre que o pipeline a absorve sem
quebrar.

> "Com Lakeflow SDP e Auto Loader, configure a ingestão de pedidos com evolução de schema
> (`cloudFiles.schemaEvolutionMode = addNewColumns`) para que a nova coluna `cupom_desconto` seja
> adicionada automaticamente ao bronze quando aparecer nos novos arquivos."

A coluna aparece no bronze: nula nas linhas antigas e preenchida nas novas.

## 11. Qualidade dirigida por metadados

Em vez de regras fixas no código, leia as expectativas de uma tabela de configuração, para escalar a
dezenas de regras.

> "Com Lakeflow SDP, crie uma tabela `regras_qualidade` (coluna_alvo, nome_regra, condicao_sql,
> acao) com várias regras de pedidos, e aplique essas expectativas dinamicamente na silver lendo as
> regras dessa tabela (usando `expect_all`), em vez de escrevê-las uma a uma."

## 12. Append flows (unificar várias fontes)

Simule que cada região chega como um arquivo separado e una tudo numa só streaming table.

> "Com Lakeflow SDP, crie uma streaming table `pedidos_todos` e vários append flows que façam
> ingestão de diferentes arquivos/pastas de pedidos (por exemplo, por lote) para a mesma tabela de
> destino, sem duplicar a definição da tabela."

## 13. Genie Space sobre a camada gold

Fora do pipeline, crie um Genie Space sobre as tabelas gold para perguntar em linguagem natural, e
assim fechar o fio: você construiu o pipeline com Genie code e agora o consulta com Genie.

> Passos: **Genie → New**, selecione o schema `lakeflow_workshop_<usuario>` e as tabelas gold (fato
> de vendas + dimensões). Teste perguntas como *"vendas por estado no último mês"* ou
> *"top 10 produtos por receita"*.

## 14. Dashboard AI/BI sobre a camada gold

Crie um dashboard AI/BI com os KPIs mensais da camada gold.

> Passos: **Dashboards → Create**, use o SQL warehouse e as tabelas gold. Gráficos sugeridos:
> receita por mês, receita por loja/estado e mix por categoria.

> Sequência sugerida: pipeline base (seção 5) → quarentena (7) e event log (8) → notebook 02 e
> incremental (9) + evolução de schema (10) → SCD2 (6) → fechamento com Genie Space (13) e dashboard (14).

---

## 15. Estrutura do repo

```
lakeflow-sdp-dq-workshop-ptbr/
├── README.md
├── docs/
│   └── pre-requisitos-admin.md     # o que o admin do cliente prepara (permissões, grupo, compute)
└── notebooks/
    ├── 01_gerar_dados_varejo.py    # gera os dados crus de varejo no seu volume
    └── 02_lote_incremental.py      # segundo lote: alimenta CDC/SCD2, incremental e evolução de schema
```
