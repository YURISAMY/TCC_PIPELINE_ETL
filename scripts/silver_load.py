import calendar
from datetime import datetime
import io
import os
import urllib.parse
import zipfile
import boto3
import pandas as pd

COLUNAS_DIAS = [f"Dia{i}" for i in range(1, 32)]
COLUNAS_NUMERICAS = ["Total"] + COLUNAS_DIAS

FALHA_MEDICAO = 999.0     # Dia existiu no calendário, mas não houve medição
DIA_INEXISTENTE = 888.0   # Dia não existe no calendário daquele mês (ex: 31 de abril)


def obter_s3_client():
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1"
        )
    return boto3.client("s3")


def lambda_handler(event, context):
    s3_client = obter_s3_client()
    
    # 1. Se foi chamada por evento S3, extrai as informações
    chave_zip = None
    bucket_bronze = os.environ.get("BUCKET_BRONZE_NAME")
    bucket_silver = os.environ.get("BUCKET_SILVER_NAME")
    
    if event and "Records" in event and len(event["Records"]) > 0:
        record = event["Records"][0]
        # Ignora eventos de teste do S3 (s3:TestEvent)
        if record.get("eventName") == "s3:TestEvent":
            print("Evento de teste do S3 recebido. Ignorando...")
            return {"statusCode": 200, "body": "Test event ignored"}
            
        if "s3" in record:
            bucket_bronze = record["s3"]["bucket"]["name"]
            chave_zip = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    
    # Se não veio do evento, tenta usar o valor de ambiente padrão
    if not chave_zip:
        chave_zip = os.environ.get("CHAVE_ZIP", "bronze/raw/postos.zip")

    # 2. Verifica se o arquivo realmente existe antes de tentar o get_object
    try:
        s3_client.head_object(Bucket=bucket_bronze, Key=chave_zip)
    except Exception:
        print(f"Aviso: O arquivo {chave_zip} ainda não existe no bucket {bucket_bronze}. Aguardando ingestão...")
        return {"statusCode": 200, "body": "Arquivo ainda não disponível"}

    print(f"Iniciando processamento. Bucket: {bucket_bronze} | Chave: {chave_zip}")
    
    # 3. Baixa e processa normalmente
    objeto_s3 = s3_client.get_object(Bucket=bucket_bronze, Key=chave_zip)
    conteudo_zip_bytes = objeto_s3["Body"].read()
    processar_camada_silver(conteudo_zip_bytes, s3_client, bucket_silver)

    print("\nExecução concluída com sucesso! Arquivo Parquet gerado na camada Silver.")
    
    return {
        "statusCode": 200,
        "body": "Camada Silver processada e salva em Parquet com sucesso!"
    }


def extrair_dados_do_zip(conteudo_zip_bytes: bytes) -> pd.DataFrame:
    lista_dfs = []
    postos_com_erro = []
    
    with zipfile.ZipFile(io.BytesIO(conteudo_zip_bytes)) as z:
        arquivos = [nome for nome in z.namelist() if nome.endswith(".txt")]
        
        for nome_arquivo in arquivos:
            posto_id = nome_arquivo.split(".")[0]
            
            try:
                with z.open(nome_arquivo) as f:
                    df_posto = pd.read_csv(
                        f, 
                        delimiter=";", 
                        encoding="utf-8"
                    )
                    
                    if df_posto.empty:
                        continue
                        
                    df_posto["id"] = posto_id
                    lista_dfs.append(df_posto)
                    
            except Exception as erro:
                postos_com_erro.append((posto_id, str(erro)))
                
    if not lista_dfs:
        raise ValueError("Nenhum posto válido pôde ser extraído do arquivo ZIP.")
        
    return pd.concat(lista_dfs, ignore_index=True)


def tratar_e_tipar_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ano_atual = datetime.now().year
    df["Anos"] = df["Anos"].astype(int)
    df["Meses"] = df["Meses"].astype(int)
    df = df[df["Anos"] < ano_atual].copy()

    for col in COLUNAS_NUMERICAS:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    df["id"] = df["id"].astype(int)
    df["Municipios"] = df["Municipios"].astype(str).str.strip()
    df["Postos"] = df["Postos"].astype(str).str.strip()
    df["Latitude"] = df["Latitude"].astype(float)
    df["Longitude"] = df["Longitude"].astype(float)

    return df


def calcular_limites_historicos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    limites_anos = df.groupby("id")["Anos"].agg(["min", "max"])
    limites_anos.columns = ["Ano_inicial", "Ano_final"]
    df = df.merge(limites_anos, on="id", how="left")

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

    return df.merge(primeiro_mes, on="id", how="left").merge(ultimo_mes, on="id", how="left")


def preencher_lacunas_temporais(df: pd.DataFrame) -> pd.DataFrame:
    ano_atual = datetime.now().year
    
    metadados = df[
        ["id", "Postos", "Municipios", "Latitude", "Longitude", "Ano_final", "Primeiro_mes", "Ultimo_mes"]
    ].drop_duplicates(subset="id")
    
    existentes = set(zip(df["id"], df["Anos"], df["Meses"]))
    linhas_faltantes = []
    
    for _, meta in metadados.iterrows():
        posto_id = meta["id"]
        ano_inicial = int(df.loc[df["id"] == posto_id, "Ano_inicial"].iloc[0])
        
        for ano in range(ano_inicial, ano_atual):
            for mes in range(1, 13):
                if (posto_id, ano, mes) not in existentes:
                    _, total_dias_mes = calendar.monthrange(ano, mes)
                    
                    linha = {
                        "id": posto_id,
                        "Municipios": meta["Municipios"],
                        "Postos": meta["Postos"],
                        "Latitude": meta["Latitude"],
                        "Longitude": meta["Longitude"],
                        "Anos": ano,
                        "Meses": mes,
                        "Total": FALHA_MEDICAO,
                        "Ano_inicial": ano_inicial,
                        "Ano_final": int(meta["Ano_final"]),
                        "Primeiro_mes": int(meta["Primeiro_mes"]),
                        "Ultimo_mes": int(meta["Ultimo_mes"]),
                    }
                    
                    for dia in range(1, 32):
                        if dia <= total_dias_mes:
                            linha[f"Dia{dia}"] = FALHA_MEDICAO
                        else:
                            linha[f"Dia{dia}"] = DIA_INEXISTENTE
                            
                    linhas_faltantes.append(linha)
                    
    if linhas_faltantes:
        df_faltantes = pd.DataFrame(linhas_faltantes)
        df = pd.concat([df, df_faltantes], ignore_index=True)
        
    df.sort_values(by=["id", "Anos", "Meses"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def salvar_silver_parquet(df: pd.DataFrame, s3_client, bucket_silver: str, key_destino: str = "silver/postos_pluviometricos.parquet"):
    buffer = io.BytesIO()
    df.to_parquet(
        buffer, 
        index=False, 
        engine="pyarrow", 
        compression="snappy"
    )
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket_silver,
        Key=key_destino,
        Body=buffer.getvalue()
    )


def processar_camada_silver(conteudo_zip_bytes: bytes, s3_client, bucket_silver: str):
    df_bruto = extrair_dados_do_zip(conteudo_zip_bytes)
    df_tipado = tratar_e_tipar_dados(df_bruto)
    df_com_limites = calcular_limites_historicos(df_tipado)
    df_silver = preencher_lacunas_temporais(df_com_limites)
    salvar_silver_parquet(df_silver, s3_client, bucket_silver)


if __name__ == "__main__":
    print("Iniciando teste local da camada Silver...")

    # 1. Simula as variáveis de ambiente que o Terraform injetará na Lambda
    os.environ["BUCKET_BRONZE_NAME"] = "medallion-bronze"
    os.environ["BUCKET_SILVER_NAME"] = "medallion-silver"
    os.environ["CHAVE_ZIP"] = "bronze/raw/postos.zip" 

    # 2. Configura boto3 para falar com o LocalStack (porta 4566)
    # Na nuvem real, essas credenciais são ignoradas pelo IAM Role
    s3_local = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    # 3. Baixa o zip do LocalStack e roda a pipeline
    print(f"Baixando postos.zip de {os.environ['BUCKET_BRONZE_NAME']}...")
    obj = s3_local.get_object(Bucket=os.environ["BUCKET_BRONZE_NAME"], Key=os.environ["CHAVE_ZIP"])
    conteudo_bytes = obj["Body"].read()

    print("Processando dados e gerando Parquet...")
    processar_camada_silver(conteudo_bytes, s3_local, os.environ["BUCKET_SILVER_NAME"])

    print("\nExecução concluída com sucesso!")
