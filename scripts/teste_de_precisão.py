import io
import boto3
import pandas as pd

# 1. Carrega o dado gerado na Silver (LocalStack)
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)
obj_silver = s3.get_object(
    Bucket="medallion-silver", Key="silver/postos_pluviometricos.parquet"
)
df_silver = pd.read_parquet(io.BytesIO(obj_silver["Body"].read()))

# 2. Carrega o CSV antigo (separador por vírgula e encoding latin1/utf-8)
caminho_csv_antigo = "d:/tcc/Nova pasta/TCC_PIPELINE_ETL/data/369_ANTONIO_DIOGO.csv"

try:
    df_antigo = pd.read_csv(caminho_csv_antigo, sep=",", encoding="utf-8")
except UnicodeDecodeError:
    df_antigo = pd.read_csv(caminho_csv_antigo, sep=",", encoding="latin1")

# 3. Filtra apenas o posto Antonio Diogo no DataFrame novo
df_novo_posto = df_silver[
    df_silver["Postos"].str.contains("ANTONIO DIOGO", case=False, na=False)
].copy()

# 4. Alinha os anos em comum (garante que comparamos o mesmo período)
anos_em_comum = set(df_antigo["Anos"].unique()).intersection(
    set(df_novo_posto["Anos"].unique())
)
df_antigo = df_antigo[df_antigo["Anos"].isin(anos_em_comum)].copy()
df_novo_posto = df_novo_posto[df_novo_posto["Anos"].isin(anos_em_comum)].copy()

colunas_chave = ["Anos", "Meses"]
df_antigo = df_antigo.sort_values(by=colunas_chave).reset_index(drop=True)
df_novo_posto = df_novo_posto.sort_values(by=colunas_chave).reset_index(
    drop=True
)

# 5. Colunas a comparar
colunas_comparar = ["Total"] + [f"Dia{i}" for i in range(1, 32)]

# Força conversão para float para comparar número com número
for col in colunas_comparar:
    df_antigo[col] = (
        df_antigo[col].astype(str).str.replace(",", ".").astype(float)
    )
    df_novo_posto[col] = df_novo_posto[col].astype(float)

# 6. Comparação célula a célula
diferencas = 0
for col in colunas_comparar:
    iguais = (df_antigo[col] == df_novo_posto[col]) | (
        df_antigo[col].isna() & df_novo_posto[col].isna()
    )
    diff_count = (~iguais).sum()
    if diff_count > 0:
        print(f"Divergência na coluna {col}: {diff_count} valores diferentes.")
        diferencas += diff_count

if diferencas == 0:
    print(
        f"\n✅ VALIDAÇÃO PERFEITA: Todos os {len(df_antigo)} meses comparados bateram 100% (chuva, totais e sentinelas)!"
    )
else:
    print(f"\n⚠️ Total de {diferencas} divergências encontradas.")