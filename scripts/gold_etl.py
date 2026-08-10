"""
Camada GOLD - Medallion Architecture (Pipeline FUNCEME)
============================================================

Responsabilidade desta camada:
    - Ler o dataset consolidado da SILVER (S3, Parquet).
    - Calcular as métricas de negócio por posto: dias/meses/anos de
      falha, percentuais, médias mensais e anual de precipitação.
    - Gerar os artefatos finais prontos para consumo:
        * gold/postos_resumo.parquet  -> analítico (BI / notebooks)
        * gold/postos_resumo.csv      -> mesma coisa em CSV
        * gold/postos_resumo.json     -> consumido pelo site estático

Design goals em relação ao notebook original:
    - A célula 13 do notebook (médias mensais) fazia um loop manual
      acumulando totais/contadores mês a mês por posto -- substituído
      por um `pivot_table` vetorizado (mesma lógica, ordens de
      grandeza mais rápido em bases grandes).
    - Sem escrita em disco local: tudo via boto3 + BytesIO, compatível
      com Lambda.
    - Não depende mais de arquivos auxiliares locais (`links.csv`,
      `municipios.csv`) para rodar -- isso fica marcado como um TODO /
      próximo passo no roadmap, com um parâmetro opcional de join.
    - A parte de inserção no Supabase (Postgres) do notebook original
      foi deliberadamente removida daqui: é uma responsabilidade de
      *serving*, não de transformação Gold. Fica como um step separado
      e opcional (ver função `load_to_postgres`, desligada por padrão).
"""

from __future__ import annotations

import io
import json
import logging
import os
import unicodedata
from dataclasses import dataclass
from typing import Optional

import boto3
import numpy as np
import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("gold_etl")

FALHA_DIA_SENTINEL = 999.0
DIA_COLS = [f"Dia{i}" for i in range(1, 32)]
MESES_NOMES = ["Jan", "Fev", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


@dataclass(frozen=True)
class Settings:
    silver_bucket: str = os.environ.get("SILVER_BUCKET", "medallion-silver")
    gold_bucket: str = os.environ.get("GOLD_BUCKET", "medallion-gold")
    silver_key: str = os.environ.get(
        "SILVER_KEY", "silver/postos_pluviometricos/latest/postos_pluviometricos.parquet"
    )
    gold_prefix: str = os.environ.get("GOLD_PREFIX", "gold/postos_resumo")
    s3_endpoint_url: Optional[str] = os.environ.get("S3_ENDPOINT_URL") or None
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    aws_region: str = os.environ.get("AWS_REGION", "us-east-1")


def get_s3_client(settings: Settings):
    kwargs = {"region_name": settings.aws_region}
    if settings.s3_endpoint_url:
        kwargs.update(
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
    return boto3.client("s3", **kwargs)


def remover_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )


# --------------------------------------------------------------------------
# 1. Leitura da silver
# --------------------------------------------------------------------------
def ler_silver(s3_client, settings: Settings) -> pd.DataFrame:
    logger.info("Lendo silver de s3://%s/%s", settings.silver_bucket, settings.silver_key)
    obj = s3_client.get_object(Bucket=settings.silver_bucket, Key=settings.silver_key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")


# --------------------------------------------------------------------------
# 2. Métricas de falha (dias / meses / anos)
# --------------------------------------------------------------------------
def calcular_metricas_falha(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ano_atual = df["Anos"].max() + 1  # mesma referência usada no notebook original

    # dias de falha por linha (mês) e por posto
    df["Dias_de_Falha_mes"] = (df[DIA_COLS] == FALHA_DIA_SENTINEL).sum(axis=1)
    total_falhas = df.groupby("id")["Dias_de_Falha_mes"].sum().rename("Total_Falhas")

    def intervalo_dias(ano_inicial: int) -> int:
        dias = (ano_atual - ano_inicial) * 365
        dias += sum(
            1
            for ano in range(ano_inicial, ano_atual + 1)
            if (ano % 4 == 0 and ano % 100 != 0) or (ano % 400 == 0)
        )
        return dias

    limites = df.groupby("id").agg(
        Ano_inicial=("Ano_inicial", "first"),
        Ano_final=("Ano_final", "first"),
        Primeiro_mes=("Primeiro_mes", "first"),
        Ultimo_mes=("Ultimo_mes", "first"),
    )
    limites["Intervalo_dias"] = limites["Ano_inicial"].apply(intervalo_dias)
    limites["Intervalo_anos"] = (limites["Ano_final"] - limites["Ano_inicial"]) + 1
    limites["Total_meses_intervalo"] = limites["Intervalo_anos"] * 12

    limites = limites.join(total_falhas)
    limites["dias_medidos"] = limites["Intervalo_dias"] - limites["Total_Falhas"]
    limites["Percentual_dias_falhos"] = (
        (limites["Total_Falhas"] / limites["Intervalo_dias"]) * 100
    ).round(2)

    # meses com pelo menos um dia de falha
    df["Mes_com_falha"] = df["Dias_de_Falha_mes"] > 0
    meses_falha = df.groupby("id")["Mes_com_falha"].sum().rename("Numero_meses_falha")
    limites = limites.join(meses_falha)
    limites["Numero_meses_completos"] = (
        limites["Total_meses_intervalo"] - limites["Numero_meses_falha"]
    )
    limites["Percentual_meses_falha"] = (
        (limites["Numero_meses_falha"] / limites["Total_meses_intervalo"]) * 100
    ).round(2)

    # anos com ao menos um mês de falha
    anos_falha = (
        df.groupby(["id", "Anos"])["Mes_com_falha"]
        .any()
        .groupby("id")
        .sum()
        .rename("Numero_anos_falha")
    )
    limites = limites.join(anos_falha)
    limites["Numero_anos_completos"] = limites["Intervalo_anos"] - limites["Numero_anos_falha"]
    limites["Percentual_anos_falha"] = (
        (limites["Numero_anos_falha"] / limites["Intervalo_anos"]) * 100
    ).round(2)

    return limites.reset_index()


# --------------------------------------------------------------------------
# 3. Médias mensais e anual (vetorizado, substitui o loop da célula 13)
# --------------------------------------------------------------------------
def calcular_medias(df: pd.DataFrame) -> pd.DataFrame:
    validos = df[~(df[DIA_COLS] == FALHA_DIA_SENTINEL).any(axis=1)].copy()

    medias_mensais = (
        validos.pivot_table(index="id", columns="Meses", values="Total", aggfunc="mean")
        .reindex(columns=range(1, 13))
    )
    medias_mensais.columns = [f"Media_{nome}" for nome in MESES_NOMES]
    medias_mensais = medias_mensais.fillna(999.0)

    soma_anual = validos.groupby(["id", "Anos"])["Total"].sum().groupby("id").sum()
    intervalo_anos = df.groupby("id")["Ano_final"].first() - df.groupby("id")["Ano_inicial"].first() + 1
    media_anual = (soma_anual / intervalo_anos).rename("Precipitacao_media_anual")

    # posto sem nenhum mês válido -> sentinela, igual ao notebook original
    sem_dados = ~medias_mensais.index.isin(validos["id"].unique())
    media_anual = media_anual.reindex(medias_mensais.index)
    media_anual[sem_dados] = 999.0
    media_anual = media_anual.fillna(999.0).round(2)

    resultado = medias_mensais.round(2).reset_index()
    resultado = resultado.merge(media_anual.reset_index(), on="id")
    return resultado


# --------------------------------------------------------------------------
# 4. Montagem do resumo final por posto
# --------------------------------------------------------------------------
def montar_resumo(df: pd.DataFrame) -> pd.DataFrame:
    metadados = df.groupby("id").agg(
        Nome_Posto=("Postos", "first"),
        Nome_Municipio=("Municipios", "first"),
        Coordenada_Y=("Latitude", "first"),
        Coordenada_X=("Longitude", "first"),
        Ano_Inicio=("Ano_inicial", "first"),
        Ano_Fim=("Ano_final", "first"),
        Mes_Inicio=("Primeiro_mes", "first"),
        Mes_Fim=("Ultimo_mes", "first"),
    ).reset_index()

    metricas_falha = calcular_metricas_falha(df)
    medias = calcular_medias(df)

    resumo = metadados.merge(metricas_falha, on="id").merge(medias, on="id")
    resumo.rename(columns={"id": "ID"}, inplace=True)
    resumo["ID"] = pd.to_numeric(resumo["ID"], errors="coerce")
    resumo.dropna(subset=["ID"], inplace=True)
    resumo.sort_values(by="ID", inplace=True)
    resumo.reset_index(drop=True, inplace=True)

    duplicados = resumo["ID"].duplicated().sum()
    if duplicados:
        logger.warning("%d ID(s) duplicados encontrados no resumo gold.", duplicados)

    return resumo


# --------------------------------------------------------------------------
# 5. Escrita dos artefatos gold
# --------------------------------------------------------------------------
def escrever_gold(resumo: pd.DataFrame, s3_client, settings: Settings) -> dict:
    saidas = {}

    # Parquet (analítico)
    buf_parquet = io.BytesIO()
    resumo.to_parquet(buf_parquet, index=False, engine="pyarrow", compression="snappy")
    key_parquet = f"{settings.gold_prefix}/postos_resumo.parquet"
    s3_client.put_object(Bucket=settings.gold_bucket, Key=key_parquet, Body=buf_parquet.getvalue())
    saidas["parquet"] = key_parquet

    # CSV
    buf_csv = io.StringIO()
    resumo.to_csv(buf_csv, index=False, decimal=",")
    key_csv = f"{settings.gold_prefix}/postos_resumo.csv"
    s3_client.put_object(Bucket=settings.gold_bucket, Key=key_csv, Body=buf_csv.getvalue().encode("utf-8"))
    saidas["csv"] = key_csv

    # JSON (consumo direto pelo site estático)
    resumo_json = resumo.copy()
    resumo_json = resumo_json.map(lambda x: str(x) if isinstance(x, (int, float, np.floating)) else x)
    resumo_json = resumo_json.map(lambda x: remover_acentos(x) if isinstance(x, str) else x)
    registros = resumo_json.to_dict(orient="records")

    key_json = f"{settings.gold_prefix}/postos_resumo.json"
    body = json.dumps(registros, ensure_ascii=False, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=settings.gold_bucket,
        Key=key_json,
        Body=body,
        ContentType="application/json",
    )
    saidas["json"] = key_json

    logger.info("Artefatos gold escritos em s3://%s/{%s}", settings.gold_bucket, ", ".join(saidas.values()))
    return saidas


# --------------------------------------------------------------------------
# Serving opcional (Postgres/Supabase) -- desligado por padrão
# --------------------------------------------------------------------------
def load_to_postgres(resumo: pd.DataFrame, dsn: str, batch_size: int = 5000) -> None:
    """Opcional: empurra o resumo gold para um Postgres (ex: Supabase).
    Mantido separado do ETL gold em si -- é uma etapa de *serving*, não
    de transformação, e idealmente roda como um job/step próprio (para
    poder trocar de destino sem tocar no pipeline de dados)."""
    import psycopg2
    import psycopg2.extras

    registros = resumo.to_dict(orient="records")
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        for i in range(0, len(registros), batch_size):
            lote = registros[i : i + batch_size]
            colunas = lote[0].keys()
            valores = [[r[c] for c in colunas] for r in lote]
            query = f"INSERT INTO posto ({', '.join(colunas)}) VALUES %s"
            psycopg2.extras.execute_values(cur, query, valores)
            logger.info("Lote %d inserido no Postgres.", i // batch_size + 1)
        conn.commit()


# --------------------------------------------------------------------------
# Orquestração
# --------------------------------------------------------------------------
def run(settings: Optional[Settings] = None) -> dict:
    settings = settings or Settings()
    s3_client = get_s3_client(settings)

    df = ler_silver(s3_client, settings)
    resumo = montar_resumo(df)
    saidas = escrever_gold(resumo, s3_client, settings)

    logger.info("Camada gold concluída: %d postos no resumo final.", len(resumo))
    return saidas


def lambda_handler(event, context):  # noqa: ANN001 - assinatura padrão do Lambda
    try:
        saidas = run()
        return {"statusCode": 200, "body": saidas}
    except Exception:
        logger.exception("Falha no ETL da camada gold.")
        raise


if __name__ == "__main__":
    run()
