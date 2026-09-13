import io
import boto3
import pandas as pd

print("1. Conectando ao S3 do LocalStack...")
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

print("2. Baixando postos_pluviometricos.parquet da camada Silver...")
obj = s3.get_object(
    Bucket="medallion-silver", 
    Key="silver/postos_pluviometricos.parquet"
)

# Lê o binário Parquet da memória
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

# Caminho local de saída
caminho_csv = "d:/tcc/Nova pasta/TCC_PIPELINE_ETL/silver_inspecao.csv"

print(f"3. Exportando {len(df)} registros para CSV...")
# Usamos sep=';' e decimal=',' caso queira abrir direto no Excel em português,
# ou sep=',' e decimal='.' se preferir o padrão internacional de engenharia.
df.to_csv(caminho_csv, index=False, sep=";", decimal=",", encoding="utf-8-sig")

print(f"\nArquivo gerado com sucesso em:\n{caminho_csv}")