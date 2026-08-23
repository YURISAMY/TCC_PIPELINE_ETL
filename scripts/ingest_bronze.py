import os
import requests
import boto3

BUCKET_BRONZE = os.environ["BUCKET_BRONZE_NAME"]

def ingest_bronze_def(event, context):
    url = "https://cdn.funceme.br/calendario/postos/postos.zip"
    arquivo_local = "/tmp/postos.zip"

    print("Baixando arquivo da FUNCEME...")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(arquivo_local, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    print("Enviando arquivo para o bucket S3...")

    s3_client.upload_file(
        Filename=arquivo_local,
        Bucket=BUCKET_BRONZE,
        Key="bronze/raw/postos.zip"
    )

    print("Arquivo enviado com sucesso para o bucket S3.")

if __name__ == "__main__":
    # Só roda quando você executa "python ingest_bronze.py" direto
    os.environ["BUCKET_BRONZE_NAME"] = "medallion-bronze"  # simula a variável que a Lambda injetaria
    ingest_bronze_def(None, None)  # event e context não importam pro seu script, então passa None