# Workshop · Lakeflow SDP + Data Quality com Genie Code

Workshop hands-on para construir um pipeline de dados completo com **Databricks Lakeflow** —as
*pipelines* declarativas da Databricks (evolução do Delta Live Tables / DLT), baseadas no padrão
aberto **Apache Spark Declarative Pipelines (SDP)**— e qualidade de dados, tudo com **Genie Code**
(antes *Databricks Assistant*) e sem escrever o código à mão.

O percurso é o seguinte:

1. Você roda um notebook que gera dados crus de um varejista brasileiro fictício (Mercado Verde) e
   os deixa como arquivos num volume do Unity Catalog.
2. A partir daí, constrói o pipeline com Genie Code. Primeiro o medallion bronze → silver → gold
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
| **Genie Code** (antes *Databricks Assistant*) | Para a segunda parte do workshop (construir o pipeline). |
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
│   └── (bronze/silver/gold que você cria com Genie Code)   ← suas tabelas, isoladas
├── lakeflow_workshop_maria_souza
│   └── ...
└── lakeflow_workshop_joao_pereira
    └── ...
```

Por que assim: tendo cada um o seu próprio schema, **as tabelas do pipeline que você construir
depois com Genie Code ficam isoladas** e o catálogo não vira um pântano de tabelas de todo mundo.
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
Genie Code.

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

## 5. Próximo passo: construir o pipeline com Genie Code

Esta é a parte central do workshop. Com os dados já no volume, abra um
**Lakeflow Spark Declarative Pipeline** e peça ao **Genie Code** para construí-lo por camadas.

> 🔑 **Cada participante usa OS SEUS próprios valores.** Como os dados vivem no seu schema
> `lakeflow_workshop_{usuario}`, os prompts apontam para a **sua** rota de landing. Para ninguém
> errar, **o notebook imprime no final (seção 12) os 4 prompts já preenchidos com os seus valores**
> — basta copiar e colar no Genie Code dentro do pipeline.

Não precisa criar nem nomear o pipeline à mão: **deixe o Genie Code criá-lo** ao colar os prompts.
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
> "Com Lakeflow SDP, crie a camada silver a partir das tabelas bronze: uma tabela por entidade, com
> cada coluna convertida ao seu tipo correto (datas, números e textos) e os valores normalizados.
> Adicione expectativas de qualidade do SDP que validem que: o email do cliente tem formato válido;
> o valor total do pedido é numérico e não negativo; a data do pedido não está no futuro; o desconto
> está entre 0 e 100; as quantidades são maiores que zero; e que cada pedido aponta para um cliente e
> uma loja que existem nas suas dimensões. Por fim, normalize o estado para a sigla de UF (2 letras)
> e remova os clientes e produtos duplicados."

**Gold (modelo dimensional e métricas):**
> "Com Lakeflow SDP, crie a camada gold a partir das tabelas silver: tabelas de dimensões limpas de
> clientes, produtos e lojas; uma tabela de fatos de vendas em que cada linha é um item de pedido (um
> produto dentro de um pedido); e KPIs mensais de vendas por loja e por categoria. Use `CLUSTER BY`
> em vez de particionar."

**Observabilidade de qualidade:**

É o **boletim de notas** da sua camada de qualidade. Cruza os erros que o notebook 01 injetou de
propósito (registrados em `_log_geracao`) contra os que suas expectativas realmente pegaram, para
responder *as minhas regras estão pegando a sujeira que plantei?*. O resultado é uma tabela
consultável —`injetado | detectado | % detecção` por regra— que você pode plotar e compartilhar. Em
produção, onde você não tem essa "verdade" injetada, o mesmo padrão serve para **monitorar a taxa de
falhas por regra ao longo do tempo** e detectar quando uma fonte se degrada.

> "Com Lakeflow SDP, crie a camada de observabilidade de qualidade. Crie uma view em SQL que leia o
> event log do pipeline —o registro que o SDP grava sozinho em cada execução— e mostre, para cada
> tabela e cada expectativa, quantos registros passaram e quantos falharam na última execução. Depois
> compare esses números com a tabela
> `{catalogo}.lakeflow_workshop_{usuario}._log_geracao`, onde o notebook 01 anotou quantos erros
> injetou de propósito, para mostrar lado a lado *injetado vs. detectado* por regra. Calcule todas as
> contagens ao vivo a partir das tabelas (sem números fixos) e use uma view normal, não materializada,
> para que sempre reflita a última execução."

> 💡 A view lê o event log **diretamente** do pipeline: funciona na hora para o dono do pipeline e
> sempre mostra a última execução, sem passos extras de configuração. Se além disso você quiser deixar
> o event log como uma tabela que outras pessoas possam consultar, pode publicá-lo no Unity Catalog em
> **Settings → Advanced → Event logs**; nesse caso use sempre um **nome de tabela novo e livre** (se o
> nome já existir, o pipeline não o adota e continua escrevendo no seu registro interno). Para o
> workshop, ler o event log diretamente é o mais simples e confiável.

> O objetivo pedagógico de toda esta seção: ver como o Genie Code monta o DAG, a incrementalidade e
> a qualidade declarativa com muito menos código do que um pipeline tradicional.

---

Com o pipeline base no ar, o workshop segue para os **recursos de SDP além do medallion** — o que
realmente diferencia o Lakeflow SDP. Todos continuam sendo construídos com Genie Code dentro do
mesmo pipeline, apontando para o seu schema `lakeflow_workshop_<usuario>`.

Os recursos de CDC/SCD2, ingestão incremental e evolução de schema trabalham sobre um segundo lote
de dados: rode o notebook [`notebooks/02_lote_incremental.py`](notebooks/02_lote_incremental.py)
depois do `01` (ele preserva o landing e só acrescenta arquivos novos, com a coluna `cupom_desconto`
e versões atualizadas de clientes). Roda igual ao notebook 01.

> 🔁 **Antes de seguir: o que é um *flow*?** Um *flow* é a unidade que **move dados para uma tabela**
> (streaming table ou materialized view) no Lakeflow. O detalhe-chave: **tudo o que você já construiu
> usa flows**, mesmo sem nomeá-los. Quando você definiu uma tabela junto com sua consulta em um único
> passo (bronze, silver), o SDP criou por baixo um *flow padrão* com o mesmo nome da tabela. Os
> recursos a seguir são, no fundo, **tipos de flow**: o AUTO CDC do ponto 6 é um flow de captura de
> mudanças, e os *append flows* do ponto 12 são flows que você pode **definir separadamente** da sua
> tabela de destino para que **várias fontes alimentem uma mesma tabela**. Tenha isso como o conceito
> que une todo o pipeline.

## 6. CDC / SCD Tipo 2 com `AUTO CDC`

**O que é.** *CDC* (Change Data Capture) é capturar as mudanças que chegam de uma fonte (inclusões e
modificações). *SCD Tipo 2* é a forma de guardar uma dimensão preservando o **histórico**: em vez de
sobrescrever, fecha a versão antiga e abre uma nova. *AUTO CDC* (em Python, `create_auto_cdc_flow`;
antes `apply_changes`) é o idiom do SDP que aplica essas mudanças por você, **sem escrever um
`MERGE`**, sobre as versões atualizadas de
clientes que o notebook 02 gera. Em termos de SDP, o AUTO CDC define um **flow de captura de
mudanças** sobre uma streaming table de destino (outro tipo de flow, além do padrão).

> "Com Lakeflow SDP, crie uma dimensão de clientes `dim_clientes_scd2` do tipo SCD Tipo 2, usando a
> captura de mudanças automática que o SDP oferece para isso. A partir da tabela bronze de clientes,
> identifique cada cliente por `id_cliente` e use `data_atualizacao` para ordenar as mudanças no
> tempo. Mantenha o histórico de `segmento` e `nivel_fidelidade`: quando algum desses valores mudar,
> preserve a versão anterior e abra uma nova. **Deixe o SDP cuidar sozinho do controle de vigência de
> cada versão com suas colunas padrão; não acrescente colunas de data nem flags próprias.**"

**O que esperar.** Uma tabela `dim_clientes_scd2` com as colunas de vigência `__START_AT` / `__END_AT`
que o **SDP adiciona automaticamente** (não se escrevem à mão). Cada cliente atualizado fica com
**duas linhas**: a antiga fechada (`__END_AT` com data) e a nova vigente (`__END_AT` em `NULL`); os
clientes sem mudanças têm uma única linha. Com os defaults, atualiza-se ~10% dos clientes. Como
variante, peça uma versão SCD Tipo 1 (`dim_clientes_atual`) para comparar: ali cada cliente teria uma
única linha, sem histórico.

> 💡 Se o Genie Code gerar a tabela **sem** `__START_AT` / `__END_AT` (ex.: uma única linha por
> cliente, ou colunas de data com nomes próprios como `data_inicio_vigencia`), é porque **não** usou
> AUTO CDC nativo: ficou como SCD Tipo 1. Verifique com
> `DESCRIBE TABLE {catalogo}.lakeflow_workshop_{usuario}.dim_clientes_scd2;` e, se faltar, peça o
> recurso de novo insistindo em `STORED AS SCD TYPE 2` / `stored_as_scd_type=2`. Para ver o histórico
> dos clientes com mudanças (use o caminho completo do **seu** schema):
>
> ```sql
> SELECT id_cliente, segmento, nivel_fidelidade, __START_AT, __END_AT
> FROM {catalogo}.lakeflow_workshop_{usuario}.dim_clientes_scd2
> WHERE id_cliente IN (
>   SELECT id_cliente
>   FROM {catalogo}.lakeflow_workshop_{usuario}.dim_clientes_scd2
>   GROUP BY id_cliente HAVING count(*) > 1)
> ORDER BY id_cliente, __START_AT;
> ```

## 7. Quarentena de dados ruins

**O que é.** A *quarentena* é um padrão de qualidade: em vez de descartar as linhas que falham nas
regras (como faria `expect_or_drop`) ou deixar que quebrem o pipeline, você as **desvia para uma
tabela à parte** para auditar ou reprocessar. Assim nada se perde e você guarda o porquê de cada
rejeição.

> "Com Lakeflow SDP, a partir da bronze de pedidos crie duas tabelas: `pedidos_validos` com as
> linhas que passam nas expectativas (valor_total numérico e >= 0, id_cliente e id_loja existentes,
> moeda válida) e `pedidos_quarentena` com as linhas que falham, adicionando uma coluna
> `motivo_falha` que indique qual regra foi violada e a data de detecção."

**O que esperar.** Duas tabelas: `pedidos_validos` e `pedidos_quarentena` (esta última com a coluna
`motivo_falha`). Vale que `count(pedidos_validos) + count(pedidos_quarentena)` = total da bronze:
**nenhuma linha se perde**, apenas separam-se as boas das ruins.

## 8. Observabilidade com o Event Log do pipeline

**O que é.** O *event log* é um registro que o **SDP grava sozinho** a cada execução: estados dos
fluxos, linhagem e, sobretudo, as métricas de qualidade (quantos registros passaram e falharam cada
expectativa). Publicá-lo como tabela permite monitorar a qualidade com SQL ou um dashboard. (É o
mesmo mecanismo do item de observabilidade da seção 5, aqui como módulo à parte.)

> "Com Lakeflow SDP, crie uma tabela `qualidade_expectativas` que leia o event log do pipeline
> (eventos do tipo `flow_progress`) e extraia, por dataset e por expectativa, quantos registros
> passaram e quantos falharam. Depois crie uma view que compare esses números contra a tabela
> `_log_geracao` (erros injetados) para mostrar injetado vs. detectado."

**O que esperar.** Uma tabela com a contagem de passados/falhados por expectativa e dataset, e uma
view que a cruza contra `_log_geracao` (injetado vs. detectado). O event log é consultado com a
função `event_log(...)`: `SELECT * FROM event_log(TABLE(<tabela_do_pipeline>))`.

## 9. Ingestão incremental (streaming de verdade)

**O que é.** *Ingestão incremental* significa processar **só os dados novos** a cada execução, sem
reprocessar o que já foi ingerido. Com *Auto Loader* sobre uma *streaming table*, o SDP controla
quais arquivos já leu, então quando chega um lote novo ele pega só esses.

Depois de rodar o notebook 02 (que adiciona `pedidos_lote2.json`), rode de novo o seu pipeline e
observe no event log que o Auto Loader processou só os arquivos novos.

> "Com Lakeflow SDP, garanta que a bronze de pedidos é uma streaming table com Auto Loader, de forma
> que uma nova execução só processe os arquivos novos que chegaram ao diretório de landing, sem
> reprocessar os antigos."

**O que esperar.** No segundo update, o `num_output_rows` da bronze de pedidos deve ser ≈ só as
linhas do lote 2 (não o total), confirmando que não reprocessou o antigo.

## 10. Evolução de schema

**O que é.** A *evolução de schema* é a capacidade do Auto Loader de **se adaptar quando a origem
muda de forma** —por exemplo, surge uma coluna nova— sem que você precise redefinir nada nem que o
pipeline quebre. Com `schemaEvolutionMode = addNewColumns`, a coluna nova se soma sozinha.

O lote 2 de `pedidos` traz a coluna nova `cupom_desconto`; mostre que o pipeline a absorve sem
quebrar.

> "Com Lakeflow SDP e Auto Loader, configure a ingestão de pedidos com evolução de schema
> (`cloudFiles.schemaEvolutionMode = addNewColumns`) para que a nova coluna `cupom_desconto` seja
> adicionada automaticamente ao bronze quando aparecer nos novos arquivos."

**O que esperar.** A coluna `cupom_desconto` aparece no bronze: **nula nas linhas antigas** (lote 1,
que não a traziam) e **preenchida nas novas** (lote 2). O Auto Loader pode reiniciar o update ao
detectar a mudança de schema e retomá-lo sozinho — é o comportamento esperado, não um erro.

### Modo live (opcional): ver ao vivo com um fluxo contínuo

O lote 2 demonstra a ingestão incremental (ponto 9) e a evolução de schema (ponto 10) de forma
**estática**: você solta os arquivos e re-executa. Se quiser ver **ao vivo** —arquivos caindo a cada
poucos segundos e o Auto Loader reagindo— rode o notebook
[`notebooks/03_fluxo_continuo.py`](notebooks/03_fluxo_continuo.py).

Durante uma janela limitada ele escreve pequenas tandas de pedidos novos no landing a cada
`intervalo_segundos`, e **a partir da tanda `tanda_nova_coluna` passa a incluir uma coluna nova
(`canal_captacao`)**. Coloque seu pipeline em **modo contínuo** (Settings → *Continuous*) e inicie-o;
você verá no event log como ele ingere só as tandas novas e como absorve a coluna nova quando ela
aparece.

> ⚠️ É um modo de **demonstração** (ideal para o apresentador conduzir). Roda por `duracao_minutos` e
> para sozinho; se colocar o pipeline em contínuo, **desligue-o ao terminar** para não deixar compute
> ligado.

## 11. Qualidade dirigida por metadados

**O que é.** Em vez de escrever cada expectativa fixa no código, você as *define como dados* numa
tabela de configuração e o pipeline as **lê e aplica dinamicamente** (com `expect_all`). Assim você
escala para dezenas de regras e as muda sem tocar no código do pipeline.

> "Com Lakeflow SDP, crie uma tabela `regras_qualidade` (coluna_alvo, nome_regra, condicao_sql,
> acao) com várias regras de pedidos, e aplique essas expectativas dinamicamente na silver lendo as
> regras dessa tabela (usando `expect_all`), em vez de escrevê-las uma a uma."

**O que esperar.** Uma tabela `regras_qualidade` com as regras como linhas e uma silver cujas
expectativas saem dessa tabela. Adicionar ou remover uma regra é editar a tabela, não o pipeline; as
métricas de cada regra aparecem no event log igual às expectativas escritas à mão.

## 12. Append flows (unificar várias fontes)

**O que é.** Até aqui cada tabela tinha **um** flow (o que é criado junto com ela). Um *append flow*
é um flow que você **define separadamente** da sua tabela de destino, o que permite que **várias
fontes independentes alimentem uma mesma streaming table** sem duplicar a sua definição e **sem full
refresh**. É a alternativa incremental ao `UNION`: em vez de uma consulta que une tudo (e obriga a
reprocessar inteiro), você soma um flow por fonte e pode **acrescentar fontes novas sem reprocessar**
o que já foi carregado. Serve para unir dados que chegam separados (por região, por lote, por
sistema) numa única tabela de destino.

Simule que cada região chega como um arquivo separado e una tudo numa só streaming table.

> "Com Lakeflow SDP, crie uma streaming table `pedidos_todos` e vários append flows que façam
> ingestão de diferentes arquivos/pastas de pedidos (por exemplo, por lote) para a mesma tabela de
> destino, sem duplicar a definição da tabela."

**O que esperar.** Uma única tabela `pedidos_todos` alimentada por vários fluxos; sua contagem é a
**soma de todas as fontes** unidas, com uma única definição da tabela de destino.

## 13. Genie Agent sobre a camada gold

**O que é.** Um *Genie Agent* (antes *Genie Space*) é um espaço do Databricks onde você faz
**perguntas em linguagem natural** sobre suas tabelas e o Genie gera o SQL por você. Fecha o fio do
workshop: você construiu o pipeline com Genie Code e agora o consulta com Genie.

> Passos: na barra lateral abra **Genie** e crie um novo agente (**New**); selecione o schema
> `lakeflow_workshop_<usuario>` e as tabelas gold (fato de vendas + dimensões). Teste perguntas como
> *"vendas por estado no último mês"* ou *"top 10 produtos por receita"*.

**O que esperar.** Um agente que responde suas perguntas com tabelas/gráficos e mostra o SQL que
gerou sobre a camada gold.

## 14. Dashboard AI/BI sobre a camada gold

**O que é.** Um *dashboard AI/BI* é o painel visual do Databricks para publicar os KPIs da camada
gold para usuários de negócio, com gráficos e filtros.

Crie um dashboard AI/BI com os KPIs mensais da camada gold.

> Passos: **Dashboards → Create**, use o SQL warehouse e as tabelas gold. Gráficos sugeridos:
> receita por mês, receita por loja/estado e mix por categoria.

**O que esperar.** Um dashboard com os KPIs mensais (receita por mês, por loja/estado e mix por
categoria) consultando diretamente a camada gold.

> Sequência sugerida: pipeline base (seção 5) → quarentena (7) e event log (8) → notebook 02 e
> incremental (9) + evolução de schema (10) → SCD2 (6) → fechamento com Genie Agent (13) e dashboard (14).

---

## 15. Estrutura do repo

```
lakeflow-sdp-dq-workshop-ptbr/
├── README.md
├── docs/
│   └── pre-requisitos-admin.md     # o que o admin do cliente prepara (permissões, grupo, compute)
└── notebooks/
    ├── 01_gerar_dados_varejo.py    # gera os dados crus de varejo no seu volume
    ├── 02_lote_incremental.py      # segundo lote: alimenta CDC/SCD2, incremental e evolução de schema
    └── 03_fluxo_continuo.py        # (opcional) modo live: pedidos a cada poucos segundos + coluna nova ao vivo
```
