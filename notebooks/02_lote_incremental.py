# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Lote incremental (ingestão incremental + evolução de schema + CDC/SCD2)
# MAGIC
# MAGIC Este notebook é **opcional** e serve para os módulos avançados do workshop. Depois de já ter
# MAGIC rodado o `01_gerar_dados_varejo`, execute este para simular que **chegaram novos dados** ao
# MAGIC mesmo volume de *landing*, **sem apagar** os arquivos existentes. Assim você consegue
# MAGIC demonstrar, com Genie code + Lakeflow SDP:
# MAGIC
# MAGIC 1. **Ingestão incremental** — o Auto Loader processa **apenas os arquivos novos** no próximo
# MAGIC    update do pipeline (nada de reprocessar tudo).
# MAGIC 2. **Evolução de schema** — o novo lote de `pedidos` traz uma coluna a mais
# MAGIC    (`cupom_desconto`) que não existia no lote original.
# MAGIC 3. **CDC / SCD Tipo 2** — um novo arquivo de `clientes` traz **versões atualizadas** de
# MAGIC    clientes existentes (mesmo `id_cliente`, `data_atualizacao` mais recente), para você
# MAGIC    historizar com `AUTO CDC`.
# MAGIC
# MAGIC > ⚠️ **Não** execute com `limpar_landing=true`: este notebook **sempre** preserva o landing e
# MAGIC > apenas **adiciona** arquivos novos (sufixo `_lote2`).
# MAGIC
# MAGIC ### ▶️ Como executar
# MAGIC 1. Rode a primeira célula de código abaixo (habilita os widgets).
# MAGIC 2. Confirme que o `catalogo` é o **mesmo** que usou no notebook 01.
# MAGIC 3. *Run all*.

# COMMAND ----------

import json
import os
import random
from datetime import date, datetime, timedelta

_PADROES = {
    "catalogo": "dacamargovws_catalog",
    "volume": "landing",
    "n_clientes": "2500",
    "n_pedidos_lote": "3000",
    "pct_erro": "0.15",
    "semente": "20260101",
}
for _nome, _valor in _PADROES.items():
    try:
        dbutils.widgets.text(_nome, _valor)
    except Exception:
        pass


def parametro(nome: str, variavel_ambiente: str) -> str:
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


def slug_identificador(texto: str) -> str:
    limpo = "".join(c if c.isalnum() else "_" for c in texto.lower())
    while "__" in limpo:
        limpo = limpo.replace("__", "_")
    limpo = limpo.strip("_")
    if limpo and limpo[0].isdigit():
        limpo = f"u_{limpo}"
    return limpo[:60] or "usuario"


CATALOGO = parametro("catalogo", "WORKSHOP_CATALOG")
VOLUME = parametro("volume", "WORKSHOP_VOLUME")
SCHEMA_BASE = "lakeflow_workshop"
N_CLIENTES = int(parametro("n_clientes", "WORKSHOP_N_CUSTOMERS"))
N_PEDIDOS_LOTE = int(parametro("n_pedidos_lote", "WORKSHOP_N_ORDERS_BATCH"))
PCT_ERRO = float(parametro("pct_erro", "WORKSHOP_PCT_ERROR"))
SEMENTE = int(parametro("semente", "WORKSHOP_SEED"))

USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
SUFIXO_USUARIO = slug_identificador(USUARIO.split("@")[0])
SCHEMA = f"{SCHEMA_BASE}_{SUFIXO_USUARIO}"
CAMINHO_LANDING = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
HOJE = date.today()
INICIO_JANELA = HOJE - timedelta(days=180)
AGORA = datetime.now()

print("Lote incremental — configuração:")
print(f"  usuario        = {USUARIO}")
print(f"  destino        = {CATALOGO}.{SCHEMA}  (volume: {VOLUME})")
print(f"  caminho landing= {CAMINHO_LANDING}")
print(f"  n_pedidos_lote = {N_PEDIDOS_LOTE}")
print(f"  (o landing NÃO é apagado; só adicionamos arquivos *_lote2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogos de referência (mesmos do notebook 01, para reproduzir IDs válidos)

# COMMAND ----------

ESTADOS = [
    ("SP", "São Paulo", ["São Paulo", "Campinas", "Santos", "Guarulhos", "Ribeirão Preto", "Sorocaba"]),
    ("RJ", "Rio de Janeiro", ["Rio de Janeiro", "Niterói", "Petrópolis", "Campos dos Goytacazes", "Nova Iguaçu"]),
    ("MG", "Minas Gerais", ["Belo Horizonte", "Uberlândia", "Contagem", "Juiz de Fora"]),
    ("BA", "Bahia", ["Salvador", "Feira de Santana", "Vitória da Conquista", "Camaçari"]),
    ("RS", "Rio Grande do Sul", ["Porto Alegre", "Caxias do Sul", "Pelotas", "Canoas"]),
    ("PR", "Paraná", ["Curitiba", "Londrina", "Maringá", "Ponta Grossa"]),
]
ESTADO_VARIANTES = {
    "SP": ["SP", "São Paulo", "SAO PAULO", "Sao Paulo", "sp"],
    "RJ": ["RJ", "Rio de Janeiro", "RIO DE JANEIRO", "rj"],
    "MG": ["MG", "Minas Gerais", "MINAS GERAIS", "mg"],
    "BA": ["BA", "Bahia", "BAHIA", "ba"],
    "RS": ["RS", "Rio Grande do Sul", "RIO GRANDE DO SUL", "rs"],
    "PR": ["PR", "Paraná", "PARANA", "pr"],
}
SEGMENTOS = ["Novo", "Recorrente", "VIP", "Inativo"]
NIVEIS_FIDELIDADE = ["Bronze", "Prata", "Ouro", "Platina"]
CANAIS = ["Web", "App", "Loja", "Call Center", "Marketplace", "web", "APP", "loja"]
METODOS_PAGAMENTO = ["Cartão", "Dinheiro", "Pix", "Boleto", "Transferência", "cartão", "PIX"]
MOEDAS = ["BRL", "R$", "brl", "Real", "BRL"]
STATUS_PEDIDO = ["Criado", "Pago", "Enviado", "Entregue", "Cancelado",
                 "PAGO", "enviado", "entregue", "cancelado"]
CUPONS = ["", "", "", "CUPOM5", "CUPOM10", "FRETEGRATIS", "BLACKFRIDAY"]
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
    ("Mercearia", "MER", 10), ("Bebidas", "BEB", 7), ("Higiene pessoal", "HIG", 6),
    ("Limpeza", "LIM", 5), ("Snacks", "SNK", 5), ("Casa", "CAS", 5), ("Eletrônicos", "ELE", 4),
]
N_LOJAS = 25

ids_cliente_validos = [f"CLI-{i:06d}" for i in range(N_CLIENTES)]
ids_loja_validos = [f"LOJ-{i:03d}" for i in range(N_LOJAS)]
skus_validos = [f"{codigo}-{j:03d}" for _, codigo, n in CATALOGO_PRODUTOS for j in range(n)]


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
    return f"+55 ({ddd}) {numero[:5]}-{numero[5:]}"


def slug(texto: str) -> str:
    limpo = (texto.lower()
             .replace("á", "a").replace("ã", "a").replace("â", "a").replace("à", "a")
             .replace("é", "e").replace("ê", "e").replace("í", "i")
             .replace("ó", "o").replace("õ", "o").replace("ô", "o")
             .replace("ú", "u").replace("ç", "c"))
    return "".join(c for c in limpo if c.isalnum())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Novo lote de pedidos (ingestão incremental + nova coluna `cupom_desconto`)

# COMMAND ----------

contador_erros = {}


def registrar_erro(entidade, erro):
    contador_erros[(entidade, erro)] = contador_erros.get((entidade, erro), 0) + 1


pedidos_lote2 = []
pedidos_itens_lote2 = []

for k in range(N_PEDIDOS_LOTE):
    rnd = random.Random(f"{SEMENTE}-lote2-pedido-{k}")
    id_pedido = f"PED-L2-{k:07d}"
    id_cliente = rnd.choice(ids_cliente_validos)
    id_loja = rnd.choice(ids_loja_validos)
    # Novos pedidos são recentes (últimos 20 dias): reforça a ideia de "dados que chegaram agora".
    data_pedido = HOJE - timedelta(days=rnd.randint(0, 20))
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
        rnd_err = random.Random(f"{SEMENTE}-lote2-item-err-{k}-{linha}")
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
        pedidos_itens_lote2.append(item)

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
        # >>> Coluna NOVA, inexistente no lote original: evolução de schema <<<
        "cupom_desconto": rnd.choice(CUPONS),
        "data_atualizacao": formatar_timestamp(momento + timedelta(hours=rnd.randint(1, 72)), rnd),
    }
    rnd_err = random.Random(f"{SEMENTE}-lote2-pedido-err-{k}")
    if rnd_err.random() < PCT_ERRO:
        erro = rnd_err.choices(
            ["cliente_orfao", "loja_orfa", "valor_negativo", "valor_nao_numerico",
             "status_desconhecido", "moeda_desconhecida"],
            weights=[16, 12, 14, 10, 24, 24],
        )[0]
        if erro == "cliente_orfao":
            pedido["id_cliente"] = f"CLI-9{rnd_err.randint(100000, 999999)}"
        elif erro == "loja_orfa":
            pedido["id_loja"] = f"LOJ-9{rnd_err.randint(10, 99)}"
        elif erro == "valor_negativo":
            pedido["valor_total"] = f"-{total:.2f}"
        elif erro == "valor_nao_numerico":
            pedido["valor_total"] = "N/D"
        elif erro == "status_desconhecido":
            pedido["status"] = rnd_err.choice(["??", "EM REVISAO", "X"])
        else:
            pedido["moeda"] = rnd_err.choice(["USD", "EUR", "??", "dólar"])
        registrar_erro("pedidos", erro)
    pedidos_lote2.append(pedido)

print(f"novos pedidos (lote 2): {len(pedidos_lote2)}  ·  itens: {len(pedidos_itens_lote2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clientes atualizados (feed para CDC / SCD Tipo 2)
# MAGIC
# MAGIC Pega um subconjunto de clientes existentes e emite **versões novas** com `data_atualizacao`
# MAGIC mais recente e atributos mudados (`segmento`, `nivel_fidelidade`). Mesma chave `id_cliente`.

# COMMAND ----------

clientes_atualizados = []
n_atualizar = max(1, N_CLIENTES // 10)

for i in range(n_atualizar):
    rnd = random.Random(f"{SEMENTE}-lote2-cliente-upd-{i}")
    # Escolhe um id existente do universo original.
    idx = rnd.randint(0, N_CLIENTES - 1)
    id_cliente = f"CLI-{idx:06d}"
    sigla_estado, _, cidades = rnd.choice(ESTADOS)
    nome = rnd.choice(NOMES)
    sobrenome = rnd.choice(SOBRENOMES)
    registro = {
        "id_cliente": id_cliente,
        "nome_completo": f"{nome} {sobrenome}",
        "email": f"{slug(nome)}.{slug(sobrenome)}{rnd.randint(1, 999)}@{rnd.choice(DOMINIOS)}",
        "telefone": formatar_telefone(rnd),
        "data_nascimento": formatar_data(
            date(rnd.randint(1955, 2006), rnd.randint(1, 12), rnd.randint(1, 28)), rnd),
        "data_cadastro": formatar_data(HOJE - timedelta(days=rnd.randint(30, 2200)), rnd),
        "cidade": rnd.choice(cidades),
        "estado": rnd.choice(ESTADO_VARIANTES[sigla_estado]),
        # Atributos que "mudaram" — o foco do SCD2:
        "segmento": rnd.choice(SEGMENTOS),
        "nivel_fidelidade": rnd.choice(NIVEIS_FIDELIDADE),
        # data_atualizacao MAIS RECENTE que a do lote original (chave de sequência do CDC):
        "data_atualizacao": formatar_timestamp(AGORA - timedelta(minutes=rnd.randint(0, 120)), rnd),
    }
    clientes_atualizados.append(registro)

print(f"clientes atualizados (versões novas p/ SCD2): {len(clientes_atualizados)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escrita incremental no landing (arquivos *_lote2, sem apagar nada)

# COMMAND ----------

import csv
import io


def anexar_json(entidade: str, registros: list, sufixo: str) -> None:
    diretorio = f"{CAMINHO_LANDING}/{entidade}"
    dbutils.fs.mkdirs(diretorio)
    with open(f"{diretorio}/{entidade}_{sufixo}.json", "w", encoding="utf-8") as fh:
        for registro in registros:
            fh.write(json.dumps({k: ("" if v is None else str(v)) for k, v in registro.items()},
                                ensure_ascii=False) + "\n")
    print(f"  + {entidade}/{entidade}_{sufixo}.json  ({len(registros)} linhas)")


print("Adicionando novos arquivos ao landing (incremental):")
anexar_json("pedidos", pedidos_lote2, "lote2")
anexar_json("pedidos_itens", pedidos_itens_lote2, "lote2")
anexar_json("clientes", clientes_atualizados, "lote2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificação e dicas
# MAGIC
# MAGIC Relê a pasta de `pedidos` completa (lote 1 + lote 2). Observe que a nova coluna
# MAGIC `cupom_desconto` aparece só nas linhas do lote 2 (nula nas antigas) — isso é **evolução de
# MAGIC schema** que o Auto Loader resolve no bronze.

# COMMAND ----------

df_pedidos = spark.read.option("recursiveFileLookup", "true").json(f"{CAMINHO_LANDING}/pedidos")
tem_cupom = "cupom_desconto" in df_pedidos.columns
total_pedidos = df_pedidos.count()
com_cupom = df_pedidos.where("cupom_desconto is not null").count() if tem_cupom else 0

print(f"pedidos no landing (lote1+lote2): {total_pedidos}")
print(f"coluna cupom_desconto presente : {tem_cupom}")
print(f"linhas com cupom_desconto      : {com_cupom}")
print(f"clientes com versão nova (SCD2): {len(clientes_atualizados)}")

resumo = {
    "usuario": USUARIO,
    "destino": f"{CATALOGO}.{SCHEMA}",
    "caminho_landing": CAMINHO_LANDING,
    "novos_pedidos": len(pedidos_lote2),
    "novos_itens": len(pedidos_itens_lote2),
    "clientes_atualizados": len(clientes_atualizados),
    "coluna_nova": "cupom_desconto",
    "coluna_nova_presente": tem_cupom,
    "total_pedidos_landing": total_pedidos,
    "gerado_em": AGORA.isoformat(),
}
print()
print(json.dumps(resumo, ensure_ascii=False, indent=2))

dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False))
