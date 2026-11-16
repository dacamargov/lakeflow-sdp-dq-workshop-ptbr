# Workshop · Lakeflow SDP + Data Quality com Genie code

Workshop hands-on para construir um pipeline de dados **de ponta a ponta com Databricks Lakeflow
Spark Declarative Pipelines (SDP)** e **regras de qualidade de dados**, mas com uma reviravolta:
**todo o pipeline é construído com Genie code** (Databricks Assistant), sem escrever o código à
mão.

A ideia é simples:

1. **Você executa um único notebook** que gera dados sintéticos crus de um varejista brasileiro
   fictício (**Mercado Verde**) e os deixa como arquivos num volume do Unity Catalog.
2. **A partir daí, você constrói o pipeline com Genie code**: bronze → silver → gold, com
   expectativas de qualidade (`@dlt.expect`), deduplicação, normalização e métricas.

Cada participante roda o notebook **no seu próprio workspace**, por isso **tudo é parametrizado** e
não há nada fixo no código.

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
├── lakeflow_workshop_daniel_vargas           ← schema por usuário
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

> Exemplo real (validado) para `daniel.vargas@databricks.com`:
> `dacamargovws_catalog.lakeflow_workshop_daniel_vargas` · volume `landing`.

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

## 6. Módulos avançados de SDP (opcional)

Quer ir além do medallion básico? O guia
**[`docs/modulos-avancados-sdp.md`](docs/modulos-avancados-sdp.md)** traz 9 módulos extras com
prompts prontos para Genie code, explorando o que o Lakeflow SDP tem de mais interessante:

1. **CDC / SCD Tipo 2** com `AUTO CDC` (historizar mudanças de clientes sem MERGE) ⭐
2. **Quarentena** de dados ruins (rotear em vez de descartar) ⭐
3. **Observabilidade** com o **Event Log** do pipeline (injetado vs. detectado) ⭐
4. **Ingestão incremental** (streaming de verdade: só processa o que chegou)
5. **Evolução de schema** (coluna nova absorvida automaticamente)
6. **Qualidade dirigida por metadados** (regras numa tabela de config)
7. **Append flows** (unificar várias fontes numa streaming table)
8. **Genie Space** sobre a camada gold (fecha o tema "Genie")
9. **Dashboard AI/BI** sobre a gold

Os módulos 1, 4 e 5 precisam de um **segundo lote de dados**: rode o notebook
**[`notebooks/02_lote_incremental.py`](notebooks/02_lote_incremental.py)** depois do `01`. Ele
**não apaga** o landing; apenas adiciona arquivos `*_lote2` com uma coluna nova (`cupom_desconto`) e
versões atualizadas de clientes (para o SCD2). Execução igual à do notebook 01 (rode a primeira
célula para habilitar os widgets, confirme o `catalogo`, *Run all*).

---

## 7. Estrutura do repo

```
lakeflow-sdp-dq-workshop-ptbr/
├── README.md
├── docs/
│   ├── pre-requisitos-admin.md     # o que o admin do cliente deve preparar (permissões, grupo, compute)
│   └── modulos-avancados-sdp.md    # 9 módulos avançados com prompts de Genie code
└── notebooks/
    ├── 01_gerar_dados_varejo.py    # o único passo obrigatório que os participantes executam
    └── 02_lote_incremental.py      # lote incremental (opcional): CDC/SCD2, evolução de schema
```

---

## 8. Validado de ponta a ponta

O gerador foi testado end-to-end num workspace serverless: escreve os 5 datasets no volume, relê e
confirma que as contagens batem. Exemplo de execução (com `n_clientes=800`, `n_pedidos=4000`):

| Entidade | Linhas |
|---|---|
| clientes | ~825 |
| produtos | 43 |
| lojas | 25 |
| pedidos | ~4.048 |
| pedidos_itens | ~17.900 |

E os problemas de qualidade ficam presentes e consultáveis (exemplo sobre `pedidos`): valores não
numéricos, valores negativos e pedidos com cliente órfão — exatamente o que as suas regras de
qualidade devem capturar.
