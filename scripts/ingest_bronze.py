import os
import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def ingest_bronze_def(event, context):
    url = "https://cdn.funceme.br/calendario/postos/postos.zip"
    arquivo_local = "/tmp/postos.zip"
    
    # 1. Recupera o nome do bucket da variável de ambiente
    bucket_bronze = os.environ["BUCKET_BRONZE_NAME"]

    # 2. Configura a sessão HTTP com Retry e Exponential Backoff
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,                      # Delays: 2s, 4s, 8s, 16s, 32s
        status_forcelist=[500, 502, 503, 504], # Erros comuns de instabilidade de rede/servidor
        raise_on_status=True
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    print("Baixando arquivo da FUNCEME com controle de retries...")
    # Timeout: 5s para estabelecer conexão, 30s esperando chunks de dados
    response = session.get(url, stream=True, timeout=(5, 30))
    response.raise_for_status()

    with open(arquivo_local, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    # 3. Cria o client S3 adaptável (LocalStack se a env existir, ou AWS real se for None)
    s3_kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if endpoint_url:
        s3_kwargs["endpoint_url"] = endpoint_url
        s3_kwargs["aws_access_key_id"] = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        s3_kwargs["aws_secret_access_key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

    s3_client = boto3.client("s3", **s3_kwargs)

    print(f"Enviando arquivo para o bucket S3: {bucket_bronze}...")
    s3_client.upload_file(
        Filename=arquivo_local,
        Bucket=bucket_bronze,
        Key="bronze/raw/postos.zip"
    )

    print("Arquivo enviado com sucesso para a camada Bronze.")


if __name__ == "__main__":
    os.environ["BUCKET_BRONZE_NAME"] = "medallion-bronze"
    os.environ["S3_ENDPOINT_URL"] = "http://localhost:4566"
    ingest_bronze_def(None, None)