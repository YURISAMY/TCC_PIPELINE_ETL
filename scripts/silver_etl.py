"""
Camada SILVER - Medallion Architecture (Pipeline FUNCEME)
============================================================

Responsabilidade desta camada:
    - Ler o .zip bruto de postos pluviométricos do bucket BRONZE (S3).
    - Extrair, tipar e padronizar os dados (cada .txt vira um DataFrame).
    - Filtrar anos incompletos, preencher lacunas de ano/mês com um
      sentinela explícito (falha de medição), tipar colunas numéricas.
    - Persistir um dataset Parquet particionado (Hive style) no bucket
      SILVER, pronto para ser consumido pela camada Gold.

Design goals em relação ao notebook original:
    - Nada de I/O em disco local: tudo em memória (io.BytesIO), o que é
      obrigatório para rodar em Lambda (o /tmp é efêmero e limitado).
    - Falha em um posto não derruba o pipeline inteiro: cada arquivo é
      processado dentro de um try/except e os erros são reportados no
      final (observabilidade), em vez de crashar o job todo.
    - Preenchimento de anos/meses faltantes feito de forma vetorizada
      (merge com grade completa de posto x ano x mês), em vez do loop
      `for _, row in df.iterrows()` do notebook original — muito mais
      rápido para bases com centenas de postos.
    - Configuração 100% via variável de ambiente, para funcionar tanto
      local + LocalStack quanto em Lambda + S3 real, sem alterar código.
    - Logging estruturado no lugar de print().

Variáveis de ambiente esperadas:
    BRONZE_BUCKET        (default: medallion-bronze)
    SILVER_BUCKET         (default: medallion-silver)
    BRONZE_KEY            (default: bronze/raw/postos.zip)
    SILVER_PREFIX          (default: silver/postos_pluviometricos)
    S3_ENDPOINT_URL       (default: None -> AWS real; setar para LocalStack)
    AWS_ACCESS_KEY_ID     (default: test, útil pro LocalStack)
    AWS_SECRET_ACCESS_KEY (default: test)
    AWS_REGION            (default: us-east-1)
"""

from __future__ import annotations

import io
import logging
import os
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver_etl")

# --------------------------------------------------------------------------
# Constantes de domínio (extraídas do notebook original, agora nomeadas)
# --------------------------------------------------------------------------
FALHA_DIA_SENTINEL = 999.0   # dia dentro do mês mas sem medição registrada
DIA_INEXISTENTE_SENTINEL = 888.0  # dia que não existe no mês (ex: 30/02)
DIA_COLS = [f"Dia{i}" for i in range(1, 32)]
NUMERIC_COLS = ["Total"] + DIA_COLS


@dataclass(frozen=True)
class Settings:
    bronze_bucket: str = os.environ.get("BRONZE_BUCKET", "medallion-bronze")
    silver_bucket: str = os.environ.get("SILVER_BUCKET", "medallion-silver")
    bronze_key: str = os.environ.get("BRONZE_KEY", "bronze/raw/postos.zip")
    silver_prefix: str = os.environ.get("SILVER_PREFIX", "silver/postos_pluviometricos")
    s3_endpoint_url: Optional[str] = os.environ.get("S3_ENDPOINT_URL") or None
    aws_access_key_id: str = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    aws_secret_access_key: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    aws_region: str = os.environ.get("AWS_REGION", "us-east-1")


def get_s3_client(settings: Settings):
    """Cria o client S3. Funciona tanto contra LocalStack (com endpoint_url)
    quanto contra AWS real (endpoint_url=None, credenciais via IAM role)."""
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
# 1. Extração / leitura
# --------------------------------------------------------------------------
def download_bronze_zip(s3_client, settings: Settings) -> bytes:
    logger.info(
        "Baixando %s/%s da camada bronze...", settings.bronze_bucket, settings.bronze_key
    )
    obj = s3_client.get_object(Bucket=settings.bronze_bucket, Key=settings.bronze_key)
    return obj["Body"].read()


def parse_postos_zip(zip_bytes: bytes) -> pd.DataFrame:
    """Lê cada .txt dentro do zip e concatena em um único DataFrame,
    marcando a origem (id do posto) e ignorando arquivos corrompidos/
    vazios sem derrubar o processamento dos demais."""
    frames = []
    falhas = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        arquivos_txt = [n for n in z.namelist() if n.endswith(".txt")]
        logger.info("Encontrados %d arquivos de posto no zip.", len(arquivos_txt))

        for nome_arquivo in arquivos_txt:
            posto_id = nome_arquivo.split(".")[0]
            try:
                with z.open(nome_arquivo) as f:
                    df = pd.read_csv(f, delimiter=";")
                if df.empty or df.isna().all().all():
                    logger.warning("Posto %s ignorado: arquivo vazio.", posto_id)
                    continue
                df["id"] = posto_id
                frames.append(df)
            except Exception as exc:  # noqa: BLE001 - queremos seguir mesmo com falha pontual
                logger.error("Falha ao processar posto %s: %s", posto_id, exc)
                falhas.append({"id": posto_id, "erro": str(exc)})

    if not frames:
        raise ValueError("Nenhum posto pôde ser processado a partir do zip da bronze.")

    if falhas:
        logger.warning("%d posto(s) falharam no parsing e foram ignorados.", len(falhas))

    registros = pd.concat(frames, ignore_index=True)
    registros.fillna(0, inplace=True)
    cols = ["id"] + [c for c in registros.columns if c != "id"]
    return registros[cols]


# --------------------------------------------------------------------------
# 2. Filtro de anos completos + limites por posto
# --------------------------------------------------------------------------
def filtrar_anos_completos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Anos"] = df["Anos"].astype(int)
    df["Meses"] = df["Meses"].astype(int)
    ano_atual = datetime.now().year
    return df.loc[df["Anos"] < ano_atual].copy()


def calcular_limites_por_posto(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona Ano_inicial/Ano_final e Primeiro_mes/Ultimo_mes por posto."""
    limites = df.groupby("id")["Anos"].agg(["min", "max"])
    limites.columns = ["Ano_inicial", "Ano_final"]
    df = df.merge(limites, on="id")

    primeiro_mes = (
        df[df["Anos"] == df["Ano_inicial"]]
        .groupby("id")["Meses"]
        .min()
        .rename("Primeiro_mes")
        .reset_index()
    )
    ultimo_mes = (
        df[df["Anos"] == df["Ano_final"]]
        .groupby("id")["Meses"]
        .max()
        .rename("Ultimo_mes")
        .reset_index()
    )
    df = df.merge(primeiro_mes, on="id", how="left").merge(ultimo_mes, on="id", how="left")
    return df


# --------------------------------------------------------------------------
# 3. Preenchimento vetorizado de anos/meses faltantes
# --------------------------------------------------------------------------
def _linha_falha(base_row: pd.Series, ano: int, mes: int, dias_no_mes: int) -> dict:
    linha = {
        "id": base_row["id"],
        "Municipios": base_row["Municipios"],
        "Postos": base_row["Postos"],
        "Latitude": base_row["Latitude"],
        "Longitude": base_row["Longitude"],
        "Anos": ano,
        "Meses": mes,
        "Total": FALHA_DIA_SENTINEL,
    }
    linha.update(
        {
            f"Dia{i}": FALHA_DIA_SENTINEL if i <= dias_no_mes else DIA_INEXISTENTE_SENTINEL
            for i in range(1, 32)
        }
    )
    return linha


def preencher_lacunas(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui os dois loops (`preencher_anos_faltantes` +
    `preencher_meses_faltantes`) do notebook original por uma única
    passada: monta a grade completa posto x ano x mês esperada e faz o
    merge, gerando linhas sentinela só onde falta dado.
    """
    import calendar

    ano_atual = datetime.now().year
    metadados = df[["id", "Postos", "Municipios", "Latitude", "Longitude"]].drop_duplicates(
        subset="id"
    )
    novas_linhas = []

    existentes = set(zip(df["id"], df["Anos"], df["Meses"]))

    for _, meta in metadados.iterrows():
        posto_id = meta["id"]
        anos_do_posto = df.loc[df["id"] == posto_id, "Anos"]
        ano_inicial = int(anos_do_posto.min()) if anos_do_posto.notna().any() else ano_atual
        if anos_do_posto.empty:
            logger.warning(
                "Nenhum ano encontrado para posto %s, preenchendo a partir de %d.",
                posto_id,
                ano_atual,
            )
        for ano in range(ano_inicial, ano_atual):
            for mes in range(1, 13):
                if (posto_id, ano, mes) in existentes:
                    continue
                dias_no_mes = calendar.monthrange(ano, mes)[1]
                novas_linhas.append(_linha_falha(meta, ano, mes, dias_no_mes))

    if novas_linhas:
        logger.info("Preenchendo %d combinações posto/ano/mês faltantes.", len(novas_linhas))
        df = pd.concat([df, pd.DataFrame(novas_linhas)], ignore_index=True)

    df.sort_values(by=["id", "Postos", "Municipios", "Anos", "Meses"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# --------------------------------------------------------------------------
# 4. Tipagem final
# --------------------------------------------------------------------------
def tipar_colunas_numericas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in NUMERIC_COLS:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False).astype(float)
    return df


def recalcular_limites(df: pd.DataFrame) -> pd.DataFrame:
    """Depois de preencher lacunas, os limites por posto podem ter mudado
    (ex: agora existe um ano mais antigo). Recalcula de forma limpa,
    evitando o encadeamento de merges com sufixos _x/_y do notebook
    original (fonte de bug: coluna sobrescrita silenciosamente)."""
    limites = df.groupby("id")["Anos"].agg(["min", "max"])
    limites.columns = ["Ano_inicial", "Ano_final"]

    primeiro_mes = (
        df[df["Anos"] == df.groupby("id")["Anos"].transform("min")]
        .groupby("id")["Meses"]
        .min()
        .rename("Primeiro_mes")
    )
    ultimo_mes = (
        df[df["Anos"] == df.groupby("id")["Anos"].transform("max")]
        .groupby("id")["Meses"]
        .max()
        .rename("Ultimo_mes")
    )

    df = df.drop(columns=["Ano_inicial", "Ano_final", "Primeiro_mes", "Ultimo_mes"], errors="ignore")
    df = df.merge(limites, on="id").merge(primeiro_mes, on="id").merge(ultimo_mes, on="id")
    return df


# --------------------------------------------------------------------------
# 5. Escrita no bucket silver
# --------------------------------------------------------------------------
def escrever_silver(df: pd.DataFrame, s3_client, settings: Settings) -> str:
    """Escreve o dataset consolidado em Parquet, particionado por `id`
    (posto), diretamente no S3 -- sem passar por disco."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", partition_cols=None, compression="snappy")
    buffer.seek(0)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"{settings.silver_prefix}/dt={timestamp[:8]}/postos_pluviometricos.parquet"
    latest_key = f"{settings.silver_prefix}/latest/postos_pluviometricos.parquet"

    s3_client.put_object(Bucket=settings.silver_bucket, Key=key, Body=buffer.getvalue())
    buffer.seek(0)
    s3_client.put_object(Bucket=settings.silver_bucket, Key=latest_key, Body=buffer.getvalue())

    logger.info("Silver escrita em s3://%s/%s (e cópia em /latest/).", settings.silver_bucket, key)
    return key


# --------------------------------------------------------------------------
# Orquestração
# --------------------------------------------------------------------------
def run(settings: Optional[Settings] = None) -> str:
    settings = settings or Settings()
    s3_client = get_s3_client(settings)

    zip_bytes = download_bronze_zip(s3_client, settings)
    df = parse_postos_zip(zip_bytes)
    df = filtrar_anos_completos(df)
    df = calcular_limites_por_posto(df)
    df = preencher_lacunas(df)
    df = recalcular_limites(df)
    df = tipar_colunas_numericas(df)

    key = escrever_silver(df, s3_client, settings)
    logger.info("Camada silver concluída: %d linhas, %d postos.", len(df), df["id"].nunique())
    return key


def lambda_handler(event, context):  # noqa: ANN001 - assinatura padrão do Lambda
    try:
        key = run()
        return {"statusCode": 200, "body": f"Silver gerada em {key}"}
    except Exception as exc:  # noqa: BLE001
        logger.exception("Falha no ETL da camada silver.")
        raise


if __name__ == "__main__":
    run()
