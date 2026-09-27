import io
import json
import os
import boto3
import numpy as np
import pandas as pd

FALHA_MEDICAO = 999.0
DIA_INEXISTENTE = 888.0
COLUNAS_DIAS = [f"Dia{i}" for i in range(1, 32)]

MESES_NOME = [
    "Jan_chuva", "Fev_chuva", "Mar_chuva", "Apr_chuva",
    "May_chuva", "Jun_chuva", "Jul_chuva", "Aug_chuva",
    "Sep_chuva", "Oct_chuva", "Nov_chuva", "Dec_chuva"
]


def carregar_dados_silver(s3_client, bucket_silver: str, chave_parquet: str) -> pd.DataFrame:
    objeto_s3 = s3_client.get_object(Bucket=bucket_silver, Key=chave_parquet)
    buffer = io.BytesIO(objeto_s3["Body"].read())
    return pd.read_parquet(buffer, engine="pyarrow")


def calcular_estatisticas_posto(df_posto: pd.DataFrame, bucket_gold: str, regiao: str = "us-east-1") -> dict:
    primeira_linha = df_posto.iloc[0]
    id_posto = int(primeira_linha["id"])

    matriz_dias = df_posto[COLUNAS_DIAS].to_numpy()

    total_dias_validos_calendario = np.sum(matriz_dias != DIA_INEXISTENTE)
    dias_falhos = int(np.sum(matriz_dias == FALHA_MEDICAO))
    dias_medidos = int(np.sum((matriz_dias != DIA_INEXISTENTE) & (matriz_dias != FALHA_MEDICAO)))

    pct_dias_falhos = round((dias_falhos / total_dias_validos_calendario) * 100, 2) if total_dias_validos_calendario > 0 else 0.0

    meses_com_falha_mask = np.any(matriz_dias == FALHA_MEDICAO, axis=1)
    total_meses = len(df_posto)
    meses_falha = int(np.sum(meses_com_falha_mask))
    meses_completos = total_meses - meses_falha
    pct_meses_falha = round((meses_falha / total_meses) * 100, 2) if total_meses > 0 else 0.0

    df_posto_aux = df_posto[["Anos", "Total"]].copy()
    df_posto_aux["tem_falha"] = meses_com_falha_mask

    anos_agrupados = df_posto_aux.groupby("Anos").agg(
        qtd_meses=("Total", "count"),
        teve_falha=("tem_falha", "any"),
        soma_ano=("Total", lambda s: s[~df_posto_aux.loc[s.index, "tem_falha"]].sum())
    )

    total_anos = len(anos_agrupados)
    anos_completos = int(np.sum((anos_agrupados["qtd_meses"] == 12) & (~anos_agrupados["teve_falha"])))
    anos_falha = total_anos - anos_completos
    pct_anos_falha = round((anos_falha / total_anos) * 100, 2) if total_anos > 0 else 0.0

    precipitacao_media_anual = round(float(anos_agrupados["soma_ano"].mean()), 2) if total_anos > 0 else 0.0

    medias_mensais = {}
    for num_mes, nome_mes in enumerate(MESES_NOME, start=1):
        dados_mes = df_posto_aux[(df_posto["Meses"] == num_mes) & (~df_posto_aux["tem_falha"])]
        media_val = dados_mes["Total"].mean() if not dados_mes.empty else 0.0
        medias_mensais[nome_mes] = str(round(float(media_val), 1))

    link_download_csv = f"https://{bucket_gold}.s3.{regiao}.amazonaws.com/gold/postos_csv/{id_posto}.csv"

    resumo_posto = {
        "Chave_ID": str(id_posto),
        "link_csv": link_download_csv,
        "Nome_Municipio": str(primeira_linha["Municipios"]).strip(),
        "Nome_Posto": str(primeira_linha["Postos"]).strip(),
        "Coordenada_Y": str(primeira_linha["Latitude"]),
        "Coordenada_X": str(primeira_linha["Longitude"]),
        "Ano_Inicio": str(int(primeira_linha["Ano_inicial"])),
        "Ano_Fim": str(int(primeira_linha["Ano_final"])),
        "Mes_Inicio": str(int(primeira_linha["Primeiro_mes"])),
        "Mes_Fim": str(int(primeira_linha["Ultimo_mes"])),
        "Total_dias_intervalo": str(int(total_dias_validos_calendario)),
        "Dias_dados_medidos": str(dias_medidos),
        "Dias_falhos": str(dias_falhos),
        "Percentual_dias_falhos": str(pct_dias_falhos),
        "Total_meses_intervalo": str(total_meses),
        "Numero_meses_completos": str(meses_completos),
        "Numero_meses_falha": str(meses_falha),
        "Percentual_meses_falha": str(pct_meses_falha),
        "Total_anos_intervalo": str(total_anos),
        "Numero_anos_completos": str(anos_completos),
        "Numero_anos_falha": str(anos_falha),
        "Percentual_anos_falha": str(pct_anos_falha),
        "Precipitacao_media_anual": str(precipitacao_media_anual),
    }

    resumo_posto.update(medias_mensais)
    return resumo_posto


def salvar_csv_posto(s3_client, bucket_gold: str, id_posto: int, df_posto: pd.DataFrame):
    """Salva a série histórica tratada do posto em CSV diretamente no S3."""
    buffer_csv = io.StringIO()
    df_posto.to_csv(buffer_csv, index=False, sep=";")
    
    chave_s3 = f"gold/postos_csv/{id_posto}.csv"
    s3_client.put_object(
        Bucket=bucket_gold,
        Key=chave_s3,
        Body=buffer_csv.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )

import time

def processar_camada_gold(s3_client, bucket_silver: str, chave_silver: str, bucket_gold: str, gerar_csvs: bool = True):
    tempo_inicio_total = time.time()
    
    print(f"Lendo Parquet de s3://{bucket_silver}/{chave_silver}...")
    df_silver = carregar_dados_silver(s3_client, bucket_silver, chave_silver)

    lista_resumos = []
    ids_unicos = df_silver["id"].unique()
    total_postos = len(ids_unicos)
    print(f"Iniciando agregação para {total_postos} postos...")

    tempo_bloco = time.time()

    for idx, id_posto in enumerate(ids_unicos, start=1):
        df_posto = df_silver[df_silver["id"] == id_posto]
        resumo = calcular_estatisticas_posto(df_posto, bucket_gold=bucket_gold)
        lista_resumos.append(resumo)

        if gerar_csvs:
            salvar_csv_posto(s3_client, bucket_gold, id_posto, df_posto)

        if idx % 100 == 0 or idx == total_postos:
            tempo_decorrido_bloco = time.time() - tempo_bloco
            print(
                f"Progresso: {idx}/{total_postos} postos processados "
                f"| Tempo dos últimos {idx % 100 or 100} postos: {tempo_decorrido_bloco:.2f}s"
            )
            tempo_bloco = time.time()

    # Salva o arquivo JSON consolidado
    print(f"Gravando postos_resumo.json em s3://{bucket_gold}/gold/...")
    conteudo_json = json.dumps(lista_resumos, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=bucket_gold,
        Key="gold/postos_resumo.json",
        Body=conteudo_json.encode("utf-8"),
        ContentType="application/json"
    )

    tempo_total = time.time() - tempo_inicio_total
    print("=" * 60)
    print("CAMADA GOLD FINALIZADA COM SUCESSO!")
    print(f"Total de postos gerados: {total_postos}")
    print(f"Tempo total de execução: {tempo_total:.2f} segundos ({tempo_total / 60:.2f} minutos)")
    print("=" * 60)


def lambda_handler(event, context):

    print("\nExecução iniciada na camada Gold com sucesso!")
    
    """Handler executado pela AWS Lambda via trigger S3 ou invocação manual."""
    bucket_silver = os.environ.get("BUCKET_SILVER_NAME", "medallion-silver")
    bucket_gold = os.environ.get("BUCKET_GOLD_NAME", "medallion-gold")
    chave_silver = "silver/postos_pluviometricos.parquet"

    # Se for disparado por evento S3 real
    if "Records" in event:
        chave_silver = event["Records"][0]["s3"]["object"]["key"]
        bucket_silver = event["Records"][0]["s3"]["bucket"]["name"]

    s3_client = boto3.client("s3")
    processar_camada_gold(
        s3_client=s3_client,
        bucket_silver=bucket_silver,
        chave_silver=chave_silver,
        bucket_gold=bucket_gold,
        gerar_csvs=True
    )

    print("Processamento da Camada Gold finalizado com sucesso!")

    return {"statusCode": 200, "body": "Camada Gold gerada com sucesso!"}


if __name__ == "__main__":
    s3_local = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    processar_camada_gold(
        s3_client=s3_local,
        bucket_silver="medallion-silver",
        chave_silver="silver/postos_pluviometricos.parquet",
        bucket_gold="medallion-gold",
        gerar_csvs=True
    )