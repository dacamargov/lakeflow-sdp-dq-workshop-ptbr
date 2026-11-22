# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Fluxo contínuo (modo live: ingestão incremental + evolução de schema ao vivo)
# MAGIC
# MAGIC Notebook **opcional** para ver o Auto Loader agindo sobre dados que chegam **de verdade**, não
# MAGIC estáticos. Em vez de soltar um único lote (como o `02`), este notebook escreve pequenas tandas
# MAGIC de pedidos novos no landing **a cada poucos segundos**, durante uma janela de tempo limitada. E,
# MAGIC a partir de certa tanda, os pedidos passam a trazer uma **coluna nova** (`canal_captacao`) que
# MAGIC não existia antes — para ver a **evolução de schema** acontecendo ao vivo.
# MAGIC
# MAGIC ### ▶️ Como usar (modo live)
# MAGIC 1. Rode primeiro o `01` (e, se quiser, o `02`) para ter a carga base.
# MAGIC 2. Coloque **seu pipeline em modo contínuo** (Settings → *Continuous*) e inicie-o, ou
# MAGIC    re-execute de tempos em tempos em modo *triggered*.
# MAGIC 3. Rode este notebook (à mão ou como **job**). Você verá no event log do pipeline como o Auto
# MAGIC    Loader vai ingerindo **só** as tandas novas e como, ao chegar a coluna nova, o bronze a
# MAGIC    **absorve** (com `schemaEvolutionMode = addNewColumns` o fluxo pode reiniciar sozinho uma vez
# MAGIC    e retomar — é o esperado).
# MAGIC
# MAGIC > ⚠️ É um modo de **demonstração**. Roda por `duracao_minutos` e para sozinho. Se colocar o
# MAGIC > pipeline em contínuo, **lembre-se de desligá-lo** ao terminar para não deixar compute ligado.
# MAGIC > Não apaga o landing: só **acrescenta** arquivos `pedidos_stream_*.json`.

# COMMAND ----------

import json
import os
import random
import time
from datetime import date, datetime, timedelta

_PADROES = {
    "catalogo": "dacamargovws_catalog",
    "volume": "landing",
    "n_clientes": "2500",
    "linhas_por_tanda": "50",
    "intervalo_segundos": "20",
    "duracao_minutos": "10",
    "tanda_nova_coluna": "4",
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
LINHAS_POR_TANDA = int(parametro("linhas_por_tanda", "WORKSHOP_STREAM_ROWS"))
INTERVALO_SEGUNDOS = int(parametro("intervalo_segundos", "WORKSHOP_STREAM_INTERVAL"))
DURACAO_MINUTOS = float(parametro("duracao_minutos", "WORKSHOP_STREAM_MINUTES"))
TANDA_NOVA_COLUNA = int(parametro("tanda_nova_coluna", "WORKSHOP_STREAM_NEWCOL_BATCH"))
PCT_ERRO = float(parametro("pct_erro", "WORKSHOP_PCT_ERROR"))
SEMENTE = int(parametro("semente", "WORKSHOP_SEED"))

USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
SUFIXO_USUARIO = slug_identificador(USUARIO.split("@")[0])
SCHEMA = f"{SCHEMA_BASE}_{SUFIXO_USUARIO}"
CAMINHO_LANDING = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"

print("Fluxo contínuo (modo live) — configuração:")
print(f"  usuario             = {USUARIO}")
print(f"  destino             = {CATALOGO}.{SCHEMA}  (volume: {VOLUME})")
print(f"  caminho landing     = {CAMINHO_LANDING}")
print(f"  linhas por tanda    = {LINHAS_POR_TANDA}")
print(f"  intervalo (seg)     = {INTERVALO_SEGUNDOS}")
print(f"  duração (min)       = {DURACAO_MINUTOS}")
print(f"  coluna nova a partir= tanda {TANDA_NOVA_COLUNA}  (canal_captacao)")
print(f"  (o landing NÃO é apagado; só adicionamos arquivos pedidos_stream_*)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogos de referência (IDs válidos, como no notebook 01)

# COMMAND ----------

CANAIS = ["Web", "App", "Loja", "Call Center", "Marketplace", "web", "APP", "loja"]
METODOS_PAGAMENTO = ["Cartão", "Dinheiro", "Pix", "Boleto", "Transferência", "cartão", "PIX"]
MOEDAS = ["BRL", "R$", "brl", "Real", "BRL"]
STATUS_PEDIDO = ["Criado", "Pago", "Enviado", "Entregue", "Cancelado",
                 "PAGO", "enviado", "entregue", "cancelado"]
# Valores da COLUNA NOVA que aparece no meio do fluxo (evolução de schema):
CANAIS_CAPTACAO = ["SEO", "Ads", "Indicação", "Email", "Orgânico", "Afiliados"]
N_LOJAS = 25

ids_cliente_validos = [f"CLI-{i:06d}" for i in range(N_CLIENTES)]
ids_loja_validos = [f"LOJ-{i:03d}" for i in range(N_LOJAS)]


def formatar_data(valor: date, rnd: random.Random) -> str:
    estilo = rnd.random()
    if estilo < 0.6:
        return valor.strftime("%Y-%m-%d")
    if estilo < 0.85:
        return valor.strftime("%d/%m/%Y")
    return valor.strftime("%m-%d-%Y")


def formatar_timestamp(valor: datetime, rnd: random.Random) -> str:
    return valor.strftime("%Y-%m-%d %H:%M:%S") if rnd.random() < 0.8 else valor.strftime("%d/%m/%Y %H:%M")


def gerar_pedido(rnd: random.Random, id_pedido: str, com_coluna_nova: bool) -> dict:
    agora = datetime.now()
    data_pedido = agora.date() - timedelta(days=rnd.randint(0, 3))
    momento = datetime.combine(data_pedido, datetime.min.time()) + timedelta(
        hours=rnd.randint(8, 22), minutes=rnd.randint(0, 59))
    valor = round(rnd.uniform(50, 5000), 2)
    pedido = {
        "id_pedido": id_pedido,
        "id_cliente": rnd.choice(ids_cliente_validos),
        "id_loja": rnd.choice(ids_loja_validos),
        "data_pedido": formatar_data(data_pedido, rnd),
        "timestamp_pedido": formatar_timestamp(momento, rnd),
        "canal": rnd.choice(CANAIS),
        "metodo_pagamento": rnd.choice(METODOS_PAGAMENTO),
        "moeda": rnd.choice(MOEDAS),
        "status": rnd.choice(STATUS_PEDIDO),
        "valor_total": f"{valor:.2f}",
        "data_atualizacao": formatar_timestamp(agora, rnd),
    }
    if com_coluna_nova:
        # >>> Coluna NOVA, inexistente nas tandas anteriores: evolução de schema <<<
        pedido["canal_captacao"] = rnd.choice(CANAIS_CAPTACAO)
    # Sujeira intencional (mesmas regras de negócio do lote 2):
    if rnd.random() < PCT_ERRO:
        erro = rnd.choices(
            ["cliente_orfao", "loja_orfa", "valor_negativo", "valor_nao_numerico",
             "status_desconhecido", "moeda_desconhecida"],
            weights=[16, 12, 14, 10, 24, 24],
        )[0]
        if erro == "cliente_orfao":
            pedido["id_cliente"] = f"CLI-9{rnd.randint(100000, 999999)}"
        elif erro == "loja_orfa":
            pedido["id_loja"] = f"LOJ-9{rnd.randint(10, 99)}"
        elif erro == "valor_negativo":
            pedido["valor_total"] = f"-{valor:.2f}"
        elif erro == "valor_nao_numerico":
            pedido["valor_total"] = "N/D"
        elif erro == "status_desconhecido":
            pedido["status"] = rnd.choice(["??", "EM REVISAO", "X"])
        else:
            pedido["moeda"] = rnd.choice(["USD", "EUR", "??", "dólar"])
    return pedido


# COMMAND ----------

# MAGIC %md
# MAGIC ## Loop: uma tanda de pedidos novos a cada `intervalo_segundos`

# COMMAND ----------

diretorio = f"{CAMINHO_LANDING}/pedidos"
dbutils.fs.mkdirs(diretorio)

inicio = time.time()
fim = inicio + DURACAO_MINUTOS * 60
tanda = 0
total_pedidos = 0

print(f"Gerando pedidos ao vivo a cada {INTERVALO_SEGUNDOS}s durante {DURACAO_MINUTOS} min...\n")
while time.time() < fim:
    tanda += 1
    com_coluna_nova = tanda >= TANDA_NOVA_COLUNA
    registros = []
    for k in range(LINHAS_POR_TANDA):
        rnd = random.Random(f"{SEMENTE}-stream-{tanda}-{k}-{time.time()}")
        id_pedido = f"PED-S{tanda:04d}-{k:05d}"
        registros.append(gerar_pedido(rnd, id_pedido, com_coluna_nova))

    marca_tempo = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo = f"{diretorio}/pedidos_stream_{tanda:04d}_{marca_tempo}.json"
    with open(arquivo, "w", encoding="utf-8") as fh:
        for registro in registros:
            fh.write(json.dumps({k: ("" if v is None else str(v)) for k, v in registro.items()},
                                ensure_ascii=False) + "\n")
    total_pedidos += len(registros)
    marca = "  ← coluna nova canal_captacao!" if com_coluna_nova else ""
    print(f"  tanda {tanda:>3}: +{len(registros)} pedidos → {arquivo.split('/')[-1]}{marca}")

    if time.time() < fim:
        time.sleep(INTERVALO_SEGUNDOS)

print(f"\nPronto. Tandas escritas: {tanda}  ·  pedidos novos: {total_pedidos}")
print(f"A coluna `canal_captacao` começou na tanda {TANDA_NOVA_COLUNA}.")
print("Confira o bronze de pedidos: a coluna nova aparece nula nas linhas antigas e preenchida nas novas.")

resumo = {
    "usuario": USUARIO,
    "destino": f"{CATALOGO}.{SCHEMA}",
    "caminho_landing": CAMINHO_LANDING,
    "tandas": tanda,
    "pedidos_novos": total_pedidos,
    "coluna_nova": "canal_captacao",
    "tanda_nova_coluna": TANDA_NOVA_COLUNA,
    "gerado_em": datetime.now().isoformat(),
}
print()
print(json.dumps(resumo, ensure_ascii=False, indent=2))
dbutils.notebook.exit(json.dumps(resumo, ensure_ascii=False))
