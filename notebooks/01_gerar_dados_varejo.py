# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Gerador de dados sintéticos de varejo (camada *landing*)
# MAGIC
# MAGIC Este notebook é o **único passo que os participantes do workshop executam**. Ele gera as
# MAGIC extrações planas de um varejista brasileiro fictício (**Mercado Verde**) e as deixa como
# MAGIC arquivos crus em um **volume do Unity Catalog**. A partir daí, o resto do workshop
# MAGIC —ingestão *bronze → silver → gold* com **Lakeflow Spark Declarative Pipelines (SDP)** e as
# MAGIC **regras de qualidade de dados**— é construído **com Genie code** (Databricks Assistant),
# MAGIC não com código escrito à mão.
# MAGIC
# MAGIC ## O que produz
# MAGIC
# MAGIC | Arquivo | Entidade | Formato | Papel no modelo |
# MAGIC |---|---|---|---|
# MAGIC | `clientes`      | Clientes              | JSON | Dimensão |
# MAGIC | `produtos`      | Catálogo de produtos  | CSV  | Dimensão |
# MAGIC | `lojas`         | Lojas / filiais       | CSV  | Dimensão |
# MAGIC | `pedidos`       | Pedidos (cabeçalho)   | JSON | Fato |
# MAGIC | `pedidos_itens` | Detalhe do pedido     | JSON | Fato (grão de linha) |
# MAGIC
# MAGIC Tudo é escrito **como texto**, igual a uma extração plana real de um sistema de origem.
# MAGIC A "sujeira" é **intencional** para que as regras de qualidade tenham algo para capturar:
# MAGIC
# MAGIC 1. **Ruído cosmético** (sempre presente): maiúsculas/minúsculas, acentos, espaços a mais,
# MAGIC    diferentes formatos de data e telefone.
# MAGIC 2. **Erros de negócio** (controlados por `pct_erro`): e-mails inválidos, valores negativos,
# MAGIC    datas futuras, referências órfãs (FK que não existe), duplicados, valores fora de faixa.
# MAGIC
# MAGIC ## Compute
# MAGIC
# MAGIC Pensado para **Serverless** (ou qualquer cluster com Spark). Não instala bibliotecas: usa só a
# MAGIC biblioteca padrão de Python + Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração
# MAGIC
# MAGIC ### ▶️ Como executar (a ordem importa)
# MAGIC
# MAGIC Os widgets **só aparecem depois de executar a célula que os cria**. Por isso:
# MAGIC
# MAGIC 1. **Execute só a célula de código abaixo** (a primeira, com `dbutils.widgets...`).
# MAGIC    No topo do notebook vai aparecer a barra de **widgets**.
# MAGIC 2. **Escreva o seu catálogo** no widget `catalogo` (e ajuste os outros se quiser).
# MAGIC 3. Agora sim, **execute o resto do notebook** (*Run all* ou célula por célula).
# MAGIC
# MAGIC > Se fizer *Run all* de uma vez sem mexer em nada, ele usa os **valores padrão**. Para mudar
# MAGIC > o catálogo, primeiro rode a célula abaixo, coloque o valor no widget e rode novamente.
# MAGIC
# MAGIC ### Resolução de parâmetros
# MAGIC
# MAGIC O notebook roda em **workspaces diferentes**, então **nada está fixo no código**. Cada
# MAGIC parâmetro é resolvido nesta ordem:
# MAGIC
# MAGIC 1. **Variável de ambiente** (`WORKSHOP_*`) — útil para jobs / automação.
# MAGIC 2. **Widget** do notebook — útil para rodar à mão pela UI.
# MAGIC 3. **Valor padrão**.
# MAGIC
# MAGIC | Parâmetro | Variável de ambiente | Padrão | Descrição |
# MAGIC |---|---|---|---|
# MAGIC | `catalogo`       | `WORKSHOP_CATALOG`      | `dacamargovws_catalog` | Catálogo de destino, **compartilhado** (deve existir) |
# MAGIC | `volume`         | `WORKSHOP_VOLUME`       | `landing`              | Volume para os arquivos crus |
# MAGIC | `n_clientes`     | `WORKSHOP_N_CUSTOMERS`  | `2500`                 | Tamanho do universo de clientes |
# MAGIC | `n_pedidos`      | `WORKSHOP_N_ORDERS`     | `15000`                | Número de pedidos a gerar |
# MAGIC | `pct_erro`       | `WORKSHOP_PCT_ERROR`    | `0.15`                 | Proporção de registros com erro de negócio |
# MAGIC | `semente`        | `WORKSHOP_SEED`         | `20260101`             | Semente de reprodutibilidade |
# MAGIC | `limpar_landing` | `WORKSHOP_CLEAN`        | `true`                 | Apaga o volume antes de escrever |
# MAGIC
# MAGIC > **Multiusuário:** compartilha-se **apenas o catálogo**. Cada participante escreve no seu
# MAGIC > próprio schema derivado de `current_user()` — p. ex. `lakeflow_workshop_daniel_vargas` — com
# MAGIC > seu próprio volume de `landing` dentro. Assim vários podem fazer o workshop **ao mesmo tempo**
# MAGIC > sem conflito, e as tabelas que cada um criar depois com Genie code ficam no seu schema
# MAGIC > (nada de um único schema virando um pântano de tabelas). Cada participante só precisa de
# MAGIC > permissão `USE CATALOG` + `CREATE SCHEMA` sobre o catálogo compartilhado.

# COMMAND ----------

import csv
import io
import json
import os
import random
from datetime import date, datetime, timedelta

# --- Declarar widgets (ignora o erro se já existem ou se não há UI) ---
_PADROES = {
    "catalogo": "dacamargovws_catalog",
    "volume": "landing",
    "n_clientes": "2500",
    "n_pedidos": "15000",
    "pct_erro": "0.15",
    "semente": "20260101",
    "limpar_landing": "true",
}
for _nome, _valor in _PADROES.items():
    try:
        dbutils.widgets.text(_nome, _valor)
    except Exception:
        pass


def parametro(nome: str, variavel_ambiente: str) -> str:
    """Resolve um parâmetro: variável de ambiente > widget > padrão."""
    valor_env = os.environ.get(variavel_ambiente)
    if valor_env not in (None, ""):
        return valor_env
    try:
        valor_widget = dbutils.widgets.get(nome)
        if valor_widget not in (None, ""):
            return valor_widget
    except Exception:
        pass
    return _PADROES[nome]


CATALOGO = parametro("catalogo", "WORKSHOP_CATALOG")
VOLUME = parametro("volume", "WORKSHOP_VOLUME")
# Prefixo fixo do schema: o schema real é derivado do usuário ({SCHEMA_BASE}_{usuario}),
# então basta indicar o catálogo. Não precisa de widget.
SCHEMA_BASE = "lakeflow_workshop"
N_CLIENTES = int(parametro("n_clientes", "WORKSHOP_N_CUSTOMERS"))
N_PEDIDOS = int(parametro("n_pedidos", "WORKSHOP_N_ORDERS"))
PCT_ERRO = float(parametro("pct_erro", "WORKSHOP_PCT_ERROR"))
SEMENTE = int(parametro("semente", "WORKSHOP_SEED"))
LIMPAR_LANDING = parametro("limpar_landing", "WORKSHOP_CLEAN").lower() == "true"


def slug_identificador(texto: str) -> str:
    """Converte um texto em um identificador SQL válido (minúsculas, letras/dígitos/_)."""
    limpo = "".join(c if c.isalnum() else "_" for c in texto.lower())
    while "__" in limpo:
        limpo = limpo.replace("__", "_")
    limpo = limpo.strip("_")
    if limpo and limpo[0].isdigit():
        limpo = f"u_{limpo}"
    return limpo[:60] or "usuario"


# Isolamento por usuário (sempre): compartilha-se SÓ o catálogo; cada participante escreve no seu
# próprio schema derivado da sua identidade (current_user), p. ex. `lakeflow_workshop_daniel_vargas`.
# Assim vários podem rodar o workshop em paralelo sem conflito, e as tabelas que cada um criar
# depois com Genie code ficam no seu próprio schema (não vira um pântano de tabelas compartilhadas).
USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
SUFIXO_USUARIO = slug_identificador(USUARIO.split("@")[0])
SCHEMA = f"{SCHEMA_BASE}_{SUFIXO_USUARIO}"

CAMINHO_LANDING = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
HOJE = date.today()
# Janela de dados: últimos ~6 meses até hoje, para que os dashboards fiquem "vivos".
INICIO_JANELA = HOJE - timedelta(days=180)

print("Configuração resolvida:")
print(f"  usuario        = {USUARIO}")
print(f"  destino        = {CATALOGO}.{SCHEMA}  (volume: {VOLUME})")
print(f"  caminho landing= {CAMINHO_LANDING}")
print(f"  n_clientes     = {N_CLIENTES}")
print(f"  n_pedidos      = {N_PEDIDOS}")
print(f"  pct_erro       = {PCT_ERRO}")
print(f"  semente        = {SEMENTE}")
print(f"  limpar_landing = {LIMPAR_LANDING}")
print(f"  janela         = {INICIO_JANELA} -> {HOJE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Infraestrutura no Unity Catalog
# MAGIC
# MAGIC Cria o schema e o volume se não existirem. **Não tenta criar o catálogo** (assume que já
# MAGIC existe e que você tem permissão `USE CATALOG` + `CREATE SCHEMA`/`CREATE VOLUME` sobre ele).

# COMMAND ----------

# O catálogo é assumido como existente e compartilhado (best-effort, caso você tenha permissão de criá-lo).
# O schema e o volume são próprios do usuário, então são criados aqui.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
except Exception as error:
    print(f"(aviso) não foi possível criar o catálogo '{CATALOGO}', assume-se existente: {error}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")
print(f"OK · schema e volume prontos em {CATALOGO}.{SCHEMA}  (volume: {VOLUME})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Catálogos de referência do negócio

# COMMAND ----------

ESTADOS = [
    ("SP", "São Paulo", ["São Paulo", "Campinas", "Santos", "Guarulhos", "Ribeirão Preto", "Sorocaba"]),
    ("RJ", "Rio de Janeiro", ["Rio de Janeiro", "Niterói", "Petrópolis", "Campos dos Goytacazes", "Nova Iguaçu"]),
    ("MG", "Minas Gerais", ["Belo Horizonte", "Uberlândia", "Contagem", "Juiz de Fora"]),
    ("BA", "Bahia", ["Salvador", "Feira de Santana", "Vitória da Conquista", "Camaçari"]),
    ("RS", "Rio Grande do Sul", ["Porto Alegre", "Caxias do Sul", "Pelotas", "Canoas"]),
    ("PR", "Paraná", ["Curitiba", "Londrina", "Maringá", "Ponta Grossa"]),
]
# Variantes "sujas" de como cada sistema escreve o estado (a normalização é tarefa do silver).
ESTADO_VARIANTES = {
    "SP": ["SP", "São Paulo", "SAO PAULO", "Sao Paulo", "sp"],
    "RJ": ["RJ", "Rio de Janeiro", "RIO DE JANEIRO", "rj"],
    "MG": ["MG", "Minas Gerais", "MINAS GERAIS", "mg"],
    "BA": ["BA", "Bahia", "BAHIA", "ba"],
    "RS": ["RS", "Rio Grande do Sul", "RIO GRANDE DO SUL", "rs"],
    "PR": ["PR", "Paraná", "PARANA", "pr"],
}

SEGMENTOS = ["Novo", "Recorrente", "VIP", "novo", "RECORRENTE", "vip", "Inativo"]
NIVEIS_FIDELIDADE = ["Bronze", "Prata", "Ouro", "Platina", "bronze", "OURO"]
CANAIS = ["Web", "App", "Loja", "Call Center", "Marketplace", "web", "APP", "loja"]
METODOS_PAGAMENTO = ["Cartão", "Dinheiro", "Pix", "Boleto", "Transferência", "cartão", "PIX"]
MOEDAS = ["BRL", "R$", "brl", "Real", "BRL"]
STATUS_PEDIDO = ["Criado", "Pago", "Enviado", "Entregue", "Cancelado",
                 "PAGO", "enviado", "entregue", "cancelado"]

NOMES = ["Maria", "José", "João", "Ana", "Luiz", "Carla", "Jorge", "Sofia", "Pedro", "Lúcia",
         "Miguel", "Valentina", "Diego", "Camila", "André", "Daniela", "Carlos", "Paula",
         "Fernando", "Gabriela", "Ricardo", "Isabela", "Rafael", "Mariana", "Roberto", "Natália",
         "Bruno", "Juliana", "Thiago", "Larissa"]
SOBRENOMES = ["Silva", "Santos", "Oliveira", "Souza", "Lima", "Pereira", "Ferreira", "Costa",
              "Rodrigues", "Almeida", "Nascimento", "Carvalho", "Araújo", "Ribeiro", "Gomes",
              "Martins", "Rocha", "Barbosa", "Alves", "Mendes", "Dias", "Castro", "Cardoso",
              "Teixeira", "Moreira"]
DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com.br", "outlook.com", "uol.com.br", "bol.com.br"]

CATALOGO_PRODUTOS = [
    ("Mercearia", "MER", ["Arroz 5kg", "Feijão 1kg", "Óleo de soja 900ml", "Açúcar 1kg",
                          "Farinha de trigo 1kg", "Macarrão 500g", "Sal 1kg", "Café moído 250g",
                          "Atum em lata", "Lentilha 500g"]),
    ("Bebidas", "BEB", ["Água 1,5L", "Refrigerante 2L", "Suco de laranja 1L", "Cerveja 350ml",
                        "Energético 250ml", "Chá gelado 500ml", "Leite 1L"]),
    ("Higiene pessoal", "HIG", ["Shampoo 400ml", "Sabonete 3un", "Creme dental 90g",
                                "Desodorante 150ml", "Papel higiênico 4un", "Aparelho de barbear 5un"]),
    ("Limpeza", "LIM", ["Detergente 2kg", "Água sanitária 1L", "Detergente líquido 500ml",
                        "Limpa vidros 1L", "Saco de lixo 30un"]),
    ("Snacks", "SNK", ["Batata frita 150g", "Biscoito 200g", "Chocolate 100g", "Amendoim 200g",
                       "Chiclete 10un"]),
    ("Casa", "CAS", ["Lâmpada LED 9W", "Pilha AA 4un", "Vela aromática", "Toalha de banho",
                     "Jogo de copos 6un"]),
    ("Eletrônicos", "ELE", ["Fone de ouvido", "Cabo USB-C", "Carregador 20W", "Mouse sem fio"]),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Utilidades de formato, ruído e erros

# COMMAND ----------

contador_erros = {}


def registrar_erro(entidade: str, erro: str) -> None:
    chave = (entidade, erro)
    contador_erros[chave] = contador_erros.get(chave, 0) + 1


def ruido_texto(texto: str, rnd: random.Random) -> str:
    """Ruído cosmético: o mesmo dado escrito de formas diferentes em sistemas diferentes."""
    estilo = rnd.random()
    if estilo < 0.55:
        return texto
    if estilo < 0.70:
        return f"  {texto} "
    if estilo < 0.82:
        return texto.lower()
    if estilo < 0.92:
        return texto.upper()
    return texto.title()


def formatar_data(valor: date, rnd: random.Random) -> str:
    estilo = rnd.random()
    if estilo < 0.6:
        return valor.strftime("%Y-%m-%d")
    if estilo < 0.85:
        return valor.strftime("%d/%m/%Y")
    return valor.strftime("%m-%d-%Y")


def formatar_timestamp(valor: datetime, rnd: random.Random) -> str:
    return valor.strftime("%Y-%m-%d %H:%M:%S") if rnd.random() < 0.8 else valor.strftime("%d/%m/%Y %H:%M")


def formatar_telefone(rnd: random.Random) -> str:
    ddd = rnd.randint(11, 99)
    numero = f"9{rnd.randint(10000000, 99999999)}"
    estilo = rnd.random()
    if estilo < 0.35:
        return f"{ddd}{numero}"
    if estilo < 0.6:
        return f"+55 ({ddd}) {numero[:5]}-{numero[5:]}"
    if estilo < 0.8:
        return f"({ddd}) {numero[:5]}-{numero[5:]}"
    return f"{ddd}-{numero}"


def slug(texto: str) -> str:
    limpo = (texto.lower()
             .replace("á", "a").replace("ã", "a").replace("â", "a").replace("à", "a")
             .replace("é", "e").replace("ê", "e").replace("í", "i")
             .replace("ó", "o").replace("õ", "o").replace("ô", "o")
             .replace("ú", "u").replace("ç", "c"))
    return "".join(c for c in limpo if c.isalnum())


def data_na_janela(rnd: random.Random) -> date:
    return INICIO_JANELA + timedelta(days=rnd.randint(0, 180))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clientes (dimensão)
# MAGIC
# MAGIC Universo reproduzível: cada cliente é gerado com sua própria semente derivada do índice.

# COMMAND ----------

clientes = []
ids_cliente_validos = []

for i in range(N_CLIENTES):
    rnd = random.Random(f"{SEMENTE}-cliente-{i}")
    sigla_estado, nome_estado, cidades = rnd.choice(ESTADOS)
    nome = rnd.choice(NOMES)
    sobrenome = rnd.choice(SOBRENOMES)
    nome_completo = f"{nome} {sobrenome}"
    id_cliente = f"CLI-{i:06d}"
    ids_cliente_validos.append(id_cliente)

    registro = {
        "id_cliente": id_cliente,
        "nome_completo": ruido_texto(nome_completo, rnd),
        "email": f"{slug(nome)}.{slug(sobrenome)}{rnd.randint(1, 999)}@{rnd.choice(DOMINIOS)}",
        "telefone": formatar_telefone(rnd),
        "data_nascimento": formatar_data(
            date(rnd.randint(1955, 2006), rnd.randint(1, 12), rnd.randint(1, 28)), rnd),
        "data_cadastro": formatar_data(
            HOJE - timedelta(days=rnd.randint(30, 2200)), rnd),
        "cidade": ruido_texto(rnd.choice(cidades), rnd),
        "estado": rnd.choice(ESTADO_VARIANTES[sigla_estado]),
        "segmento": rnd.choice(SEGMENTOS),
        "nivel_fidelidade": rnd.choice(NIVEIS_FIDELIDADE),
        "data_atualizacao": formatar_timestamp(
            datetime.now() - timedelta(minutes=rnd.randint(0, 5000)), rnd),
    }

    # --- Injeção de erros de negócio ---
    rnd_err = random.Random(f"{SEMENTE}-cliente-err-{i}")
    if rnd_err.random() < PCT_ERRO:
        erro = rnd_err.choices(
            ["email_invalido", "email_vazio", "data_cadastro_futura", "data_ilegivel",
             "nome_curto", "cidade_vazia", "nascimento_invalido"],
            weights=[24, 14, 14, 12, 10, 14, 12],
        )[0]
        if erro == "email_invalido":
            registro["email"] = registro["email"].replace("@", ".")
        elif erro == "email_vazio":
            registro["email"] = ""
        elif erro == "data_cadastro_futura":
            registro["data_cadastro"] = formatar_data(HOJE + timedelta(days=rnd_err.randint(5, 300)), rnd_err)
        elif erro == "data_ilegivel":
            registro["data_cadastro"] = rnd_err.choice(["N/D", "0000-00-00", "31/02/2025", "sem data"])
        elif erro == "nome_curto":
            registro["nome_completo"] = rnd_err.choice(["X", "-", "AB"])
        elif erro == "cidade_vazia":
            registro["cidade"] = ""
        else:
            registro["data_nascimento"] = rnd_err.choice(["N/D", "1800-01-01", "32/13/1990"])
        registrar_erro("clientes", erro)

    clientes.append(registro)

    # Duplicado real: o mesmo cliente capturado duas vezes com outro id (resolvido por DQ/dedupe).
    if rnd.random() < 0.03:
        dup = dict(registro)
        dup["id_cliente"] = f"{id_cliente}-D"
        dup["telefone"] = formatar_telefone(rnd)
        clientes.append(dup)
        registrar_erro("clientes", "duplicado")

print(f"clientes gerados: {len(clientes)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Produtos (dimensão)

# COMMAND ----------

produtos = []
skus_validos = []

for categoria, codigo, nomes in CATALOGO_PRODUTOS:
    for j, nome in enumerate(nomes):
        rnd = random.Random(f"{SEMENTE}-produto-{codigo}-{j}")
        sku = f"{codigo}-{j:03d}"
        skus_validos.append(sku)
        custo = round(rnd.uniform(5, 500), 2)
        preco = round(custo * rnd.uniform(1.2, 2.1), 2)
        registro = {
            "sku": sku,
            "nome_produto": nome,
            "categoria": categoria,
            "marca": rnd.choice(["Genérica", "Premium", "Nacional", "Importada", "Própria"]),
            "preco_lista": f"{preco:.2f}",
            "custo_unitario": f"{custo:.2f}",
            "unidade": rnd.choice(["UN", "un", "PCT", "pct", "CX"]),
            "ativo": rnd.choice(["true", "false", "1", "0", "SIM", "NAO"]),
            "data_atualizacao": formatar_timestamp(
                datetime.now() - timedelta(days=rnd.randint(0, 120)), rnd),
        }
        rnd_err = random.Random(f"{SEMENTE}-produto-err-{codigo}-{j}")
        if rnd_err.random() < PCT_ERRO:
            erro = rnd_err.choices(
                ["preco_negativo", "preco_nao_numerico", "margem_negativa",
                 "categoria_vazia", "custo_vazio"],
                weights=[22, 20, 24, 18, 16],
            )[0]
            if erro == "preco_negativo":
                registro["preco_lista"] = f"-{preco:.2f}"
            elif erro == "preco_nao_numerico":
                registro["preco_lista"] = rnd_err.choice(["N/D", "consultar", "-"])
            elif erro == "margem_negativa":
                registro["preco_lista"] = f"{round(custo * 0.85, 2):.2f}"
            elif erro == "categoria_vazia":
                registro["categoria"] = ""
            else:
                registro["custo_unitario"] = ""
            registrar_erro("produtos", erro)
        produtos.append(registro)

# SKU duplicado com capitalização diferente (caso típico de dedupe de catálogo).
if produtos:
    dup = dict(produtos[0])
    dup["sku"] = dup["sku"].lower()
    dup["nome_produto"] = dup["nome_produto"].upper()
    produtos.append(dup)
    registrar_erro("produtos", "sku_duplicado")

print(f"produtos gerados: {len(produtos)}  (skus únicos: {len(skus_validos)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lojas (dimensão)

# COMMAND ----------

lojas = []
ids_loja_validos = []
N_LOJAS = 25

for i in range(N_LOJAS):
    rnd = random.Random(f"{SEMENTE}-loja-{i}")
    sigla_estado, nome_estado, cidades = ESTADOS[i % len(ESTADOS)]
    cidade = rnd.choice(cidades)
    id_loja = f"LOJ-{i:03d}"
    ids_loja_validos.append(id_loja)
    registro = {
        "id_loja": id_loja,
        "nome_loja": f"Mercado Verde {cidade} {i:02d}",
        "cidade": cidade,
        "estado": rnd.choice(ESTADO_VARIANTES[sigla_estado]),
        "formato": rnd.choice(["Express", "Supermercado", "Atacado", "express", "SUPERMERCADO"]),
        "latitude": f"{round(rnd.uniform(-33, 5), 5)}",
        "longitude": f"{round(rnd.uniform(-74, -34), 5)}",
        "data_abertura": formatar_data(HOJE - timedelta(days=rnd.randint(200, 5000)), rnd),
        "ativa": rnd.choice(["true", "1", "SIM"]),
        "data_atualizacao": formatar_timestamp(
            datetime.now() - timedelta(days=rnd.randint(0, 30)), rnd),
    }
    rnd_err = random.Random(f"{SEMENTE}-loja-err-{i}")
    if rnd_err.random() < PCT_ERRO:
        erro = rnd_err.choice(["latitude_fora_faixa", "longitude_fora_faixa", "cidade_vazia"])
        if erro == "latitude_fora_faixa":
            registro["latitude"] = "999.9"
        elif erro == "longitude_fora_faixa":
            registro["longitude"] = "999.9"
        else:
            registro["cidade"] = ""
        registrar_erro("lojas", erro)
    lojas.append(registro)

print(f"lojas geradas: {len(lojas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pedidos e detalhe (fatos)
# MAGIC
# MAGIC Dados **crus e transacionais**: uma linha = um evento. **Não se calcula nenhum agregado**
# MAGIC (`total_`, `sum_`, `avg_`…): essas métricas são computadas pela camada *gold* do pipeline que
# MAGIC é construído depois com Genie code.

# COMMAND ----------

pedidos = []
pedidos_itens = []

for k in range(N_PEDIDOS):
    rnd = random.Random(f"{SEMENTE}-pedido-{k}")
    id_pedido = f"PED-{k:07d}"
    id_cliente = rnd.choice(ids_cliente_validos)
    id_loja = rnd.choice(ids_loja_validos)
    data_pedido = data_na_janela(rnd)
    momento = datetime.combine(data_pedido, datetime.min.time()) + timedelta(
        hours=rnd.randint(8, 22), minutes=rnd.randint(0, 59))

    n_linhas = rnd.randint(1, 8)
    total = 0.0
    for linha in range(1, n_linhas + 1):
        sku = rnd.choice(skus_validos)
        quantidade = rnd.randint(1, 20)
        preco_unitario = round(rnd.uniform(8, 600), 2)
        desconto = rnd.choice([0, 0, 0, 5, 10, 15, 20])
        total += quantidade * preco_unitario * (1 - desconto / 100.0)
        item = {
            "id_pedido": id_pedido,
            "num_linha": str(linha),
            "sku": sku,
            "quantidade": str(quantidade),
            "preco_unitario": f"{preco_unitario:.2f}",
            "desconto_pct": str(desconto),
        }
        rnd_err = random.Random(f"{SEMENTE}-item-err-{k}-{linha}")
        if rnd_err.random() < PCT_ERRO:
            erro = rnd_err.choices(
                ["quantidade_invalida", "preco_negativo", "desconto_fora_faixa",
                 "sku_orfao", "linha_nao_numerica"],
                weights=[26, 22, 22, 20, 10],
            )[0]
            if erro == "quantidade_invalida":
                item["quantidade"] = rnd_err.choice(["0", "-2", "muitas"])
            elif erro == "preco_negativo":
                item["preco_unitario"] = f"-{preco_unitario:.2f}"
            elif erro == "desconto_fora_faixa":
                item["desconto_pct"] = rnd_err.choice(["150", "-10"])
            elif erro == "sku_orfao":
                item["sku"] = f"XXX-{rnd_err.randint(900, 999)}"
            else:
                item["num_linha"] = "um"
            registrar_erro("pedidos_itens", erro)
        pedidos_itens.append(item)

    pedido = {
        "id_pedido": id_pedido,
        "id_cliente": id_cliente,
        "id_loja": id_loja,
        "data_pedido": formatar_data(data_pedido, rnd),
        "timestamp_pedido": formatar_timestamp(momento, rnd),
        "canal": rnd.choice(CANAIS),
        "metodo_pagamento": rnd.choice(METODOS_PAGAMENTO),
        "moeda": rnd.choice(MOEDAS),
        "status": rnd.choice(STATUS_PEDIDO),
        "valor_total": f"{total:.2f}",
        "data_atualizacao": formatar_timestamp(momento + timedelta(hours=rnd.randint(1, 72)), rnd),
    }
    rnd_err = random.Random(f"{SEMENTE}-pedido-err-{k}")
    if rnd_err.random() < PCT_ERRO:
        erro = rnd_err.choices(
            ["cliente_orfao", "loja_orfa", "valor_negativo", "valor_nao_numerico",
             "data_futura", "status_desconhecido", "moeda_desconhecida"],
            weights=[16, 12, 14, 10, 14, 18, 16],
        )[0]
        if erro == "cliente_orfao":
            pedido["id_cliente"] = f"CLI-9{rnd_err.randint(100000, 999999)}"
        elif erro == "loja_orfa":
            pedido["id_loja"] = f"LOJ-9{rnd_err.randint(10, 99)}"
        elif erro == "valor_negativo":
            pedido["valor_total"] = f"-{total:.2f}"
        elif erro == "valor_nao_numerico":
            pedido["valor_total"] = "N/D"
        elif erro == "data_futura":
            pedido["data_pedido"] = formatar_data(HOJE + timedelta(days=rnd_err.randint(3, 60)), rnd_err)
        elif erro == "status_desconhecido":
            pedido["status"] = rnd_err.choice(["??", "EM REVISAO", "X"])
        else:
            pedido["moeda"] = rnd_err.choice(["USD", "EUR", "??", "dólar"])
        registrar_erro("pedidos", erro)

    pedidos.append(pedido)

    # Pedido duplicado na origem (resolvido pela unicidade na gold).
    if rnd.random() < 0.01:
        pedidos.append(dict(pedido))
        registrar_erro("pedidos", "pedido_duplicado")

print(f"pedidos gerados: {len(pedidos)}  ·  itens: {len(pedidos_itens)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Escrita no volume de *landing*
# MAGIC
# MAGIC Cada entidade é escrita na sua própria pasta. Se `limpar_landing=true` o volume é esvaziado
# MAGIC primeiro, de modo que reexecutar o notebook deixe um conjunto limpo e reproduzível.

# COMMAND ----------

ENTIDADES = ["clientes", "produtos", "lojas", "pedidos", "pedidos_itens"]

if LIMPAR_LANDING:
    for entidade in ENTIDADES:
        try:
            dbutils.fs.rm(f"{CAMINHO_LANDING}/{entidade}", recurse=True)
        except Exception as error:
            print(f"  (nada para limpar em {entidade}: {error})")
    print(f"volume de landing limpo: {CAMINHO_LANDING}")


def escrever_json(entidade: str, registros: list) -> None:
    diretorio = f"{CAMINHO_LANDING}/{entidade}"
    dbutils.fs.mkdirs(diretorio)
    with open(f"{diretorio}/{entidade}.json", "w", encoding="utf-8") as fh:
        for registro in registros:
            fh.write(json.dumps({k: ("" if v is None else str(v)) for k, v in registro.items()},
                                ensure_ascii=False) + "\n")
    print(f"  {entidade:14} JSON  {len(registros):>7} linhas")


def escrever_csv(entidade: str, registros: list) -> None:
    diretorio = f"{CAMINHO_LANDING}/{entidade}"
    dbutils.fs.mkdirs(diretorio)
    buffer = io.StringIO()
    escritor = csv.DictWriter(buffer, fieldnames=list(registros[0].keys()), quoting=csv.QUOTE_MINIMAL)
    escritor.writeheader()
    for registro in registros:
        escritor.writerow({k: ("" if v is None else str(v)) for k, v in registro.items()})
    with open(f"{diretorio}/{entidade}.csv", "w", encoding="utf-8") as fh:
        fh.write(buffer.getvalue())
    print(f"  {entidade:14} CSV   {len(registros):>7} linhas")


print("Escrevendo arquivos crus:")
escrever_json("clientes", clientes)
escrever_csv("produtos", produtos)
escrever_csv("lojas", lojas)
escrever_json("pedidos", pedidos)
escrever_json("pedidos_itens", pedidos_itens)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Log de geração (opcional, útil para o módulo de qualidade)
# MAGIC
# MAGIC Guarda quantos erros foram injetados de propósito. No fim do workshop, quando o pipeline SDP
# MAGIC estiver rodando, dá para contrastar **injetado vs. detectado** pelas regras de qualidade.

# COMMAND ----------

from pyspark.sql import Row

TABELA_CONTROLE = f"{CATALOGO}.{SCHEMA}._log_geracao"
GERADO_EM = datetime.now()

contagens = {
    "clientes": len(clientes),
    "produtos": len(produtos),
    "lojas": len(lojas),
    "pedidos": len(pedidos),
    "pedidos_itens": len(pedidos_itens),
}

linhas = [
    Row(entidade=entidade, tipo="total_registros", detalhe="(total)", quantidade=quantidade, gerado_em=GERADO_EM)
    for entidade, quantidade in contagens.items()
] + [
    Row(entidade=entidade, tipo="erro_injetado", detalhe=erro, quantidade=quantidade, gerado_em=GERADO_EM)
    for (entidade, erro), quantidade in sorted(contador_erros.items())
]

(spark.createDataFrame(linhas)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(TABELA_CONTROLE))

total_erros = sum(v for _, v in contador_erros.items())
print(f"log escrito em {TABELA_CONTROLE}: {sum(contagens.values())} registros, "
      f"{total_erros} erros injetados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validação: reler do volume
# MAGIC
# MAGIC Testa a viagem de ida e volta (escrever → ler). Se isto bater, os arquivos estão prontos para
# MAGIC você construir o pipeline SDP com Genie code.

# COMMAND ----------

leitura = {
    "clientes": spark.read.json(f"{CAMINHO_LANDING}/clientes"),
    "produtos": spark.read.option("header", "true").csv(f"{CAMINHO_LANDING}/produtos"),
    "lojas": spark.read.option("header", "true").csv(f"{CAMINHO_LANDING}/lojas"),
    "pedidos": spark.read.json(f"{CAMINHO_LANDING}/pedidos"),
    "pedidos_itens": spark.read.json(f"{CAMINHO_LANDING}/pedidos_itens"),
}

contagens_lidas = {}
print("Verificação (linhas relidas do volume):")
for entidade, df in leitura.items():
    n = df.count()
    contagens_lidas[entidade] = n
    esperado = contagens[entidade]
    marca = "OK " if n == esperado else "!! "
    print(f"  {marca}{entidade:14} lidas={n:>7}  esperadas={esperado:>7}")

print("\nAmostra de clientes:")
leitura["clientes"].select("id_cliente", "nome_completo", "email", "cidade", "estado").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Sua folha de dados para construir o pipeline com Genie code
# MAGIC
# MAGIC **Importante:** cada participante tem seu próprio schema e volume. A célula abaixo imprime os
# MAGIC 4 prompts **já preenchidos com os seus caminhos** — basta copiar e colar no Genie code dentro
# MAGIC de um pipeline Lakeflow; deixe o Genie criar as tabelas (você não precisa nomear nada à mão).
# MAGIC
# MAGIC - O **catálogo** é compartilhado, mas o **schema** é seu: `lakeflow_workshop_{usuario}`.
# MAGIC - Seu pipeline fica no seu nome (**owner**) e escreve no seu schema → não colide com ninguém.
# MAGIC - Para encontrá-lo na lista de pipelines, filtre por **"Created by me"**.

# COMMAND ----------

folha = f"""
================================================================================
  FOLHA DE DADOS · {USUARIO}
================================================================================
  Catálogo (compartilhado) : {CATALOGO}
  SEU schema               : {SCHEMA}
  SEU caminho de landing   : {CAMINHO_LANDING}

  Arquivos de origem (use-os como estão nos seus prompts):
    JSON : {CAMINHO_LANDING}/clientes
    CSV  : {CAMINHO_LANDING}/produtos      (header = true)
    CSV  : {CAMINHO_LANDING}/lojas         (header = true)
    JSON : {CAMINHO_LANDING}/pedidos
    JSON : {CAMINHO_LANDING}/pedidos_itens
--------------------------------------------------------------------------------
  COMO USAR:
   1. Abra um pipeline Lakeflow (SDP) e cole os prompts abaixo no Genie code:
      deixe o Genie criar as tabelas; você não precisa nomear nada à mão.
   2. Seu pipeline fica no seu nome (owner) e escreve no SEU schema, então não
      colide com o de ninguém mesmo que o nome se repita.
   DICA: para achar seu pipeline na lista Jobs & Pipelines, filtre por
         'Created by me' (vê só o seu, mesmo que haja nomes repetidos).
--------------------------------------------------------------------------------
  (Todos os prompts assumem um pipeline Lakeflow SDP / Spark Declarative Pipelines.)

  PROMPT 1 · BRONZE (ingestão com Auto Loader)
  "Usando Lakeflow SDP (Spark Declarative Pipelines), crie a camada bronze com tabelas
   de streaming que façam ingestão com Auto Loader a partir de {CAMINHO_LANDING}/ .
   clientes, pedidos e pedidos_itens são JSON; produtos e lojas são CSV com header.
   Deixe todas as colunas como string e adicione colunas de linhagem com o nome do
   arquivo e a data de ingestão."

  PROMPT 2 · SILVER (tipagem, normalização e qualidade)
  "Com Lakeflow SDP, crie a camada silver com tabelas que tipem e normalizem cada
   entidade a partir das tabelas bronze, com expectativas de qualidade (expectations
   do SDP): email válido, valor_total numérico e >= 0, data_pedido não futura,
   desconto_pct entre 0 e 100, quantidade > 0, e id_cliente/id_loja/sku que existam
   nas suas dimensões. Normalize o estado para a sigla de UF (2 letras) e deduplique
   clientes e produtos."

  PROMPT 3 · GOLD (modelo dimensional e métricas)
  "Com Lakeflow SDP, crie a camada gold a partir das tabelas silver, com tabelas de
   dimensões limpas de clientes, produtos e lojas, um fato de vendas no grão de linha,
   e KPIs mensais de vendas por loja e por categoria. Use CLUSTER BY em vez de
   particionar."

  PROMPT 4 · OBSERVABILIDADE DE QUALIDADE
  "Com Lakeflow SDP, crie a camada de observabilidade de qualidade com uma tabela que
   publique as métricas das expectativas do pipeline e as compare contra a tabela
   {TABELA_CONTROLE} para mostrar injetado vs. detectado."
================================================================================
"""
print(folha)

# COMMAND ----------

resumo = {
    "usuario": USUARIO,
    "destino": f"{CATALOGO}.{SCHEMA}",
    "caminho_landing": CAMINHO_LANDING,
    "entidades": ENTIDADES,
    "contagens_escritas": contagens,
    "contagens_lidas": contagens_lidas,
    "erros_injetados": total_erros,
    "bate": contagens == contagens_lidas,
    "gerado_em": GERADO_EM.isoformat(),
}
print(json.dumps(resumo, ensure_ascii=False, indent=2))

dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False))
