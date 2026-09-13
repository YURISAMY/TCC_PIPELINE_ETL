import io
import boto3
import pandas as pd

# 1. Conecta ao S3 do LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# 2. Baixa o Parquet direto para a RAM
print("Baixando postos_pluviometricos.parquet do bucket Silver...")
obj = s3.get_object(
    Bucket="medallion-silver", 
    Key="silver/postos_pluviometricos.parquet"
)

# 3. Lê com o Pandas usando pyarrow
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

# 4. Diagnósticos do dado
print("\n--- INFORMAÇÕES DO DATAFRAME ---")
print(df.info())

print("\n--- AMOSTRA DAS PRIMEIRAS LINHAS ---")
print(df.head(5))

print("\n--- CONTAGEM DE SENTINELAS ---")
print(f"Total de registros: {len(df)}")
print(f"Postos únicos: {df['id'].nunique()}")
print(f"Total de falhas (999.0 no Dia1): {(df['Dia1'] == 999.0).sum()}")
print(f"Total de dias inexistentes (888.0 no Dia31): {(df['Dia31'] == 888.0).sum()}")