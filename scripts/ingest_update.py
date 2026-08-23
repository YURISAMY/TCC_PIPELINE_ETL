
import requests
import boto3
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

def ingest_bronze_def(event, context):
    url = "https://cdn.funceme.br/calendario/postos/postos.zip"
    arquivo_local = "/tmp/postos.zip"

    print("Configurando cliente HTTP com Exponential Backoff...")
    session = requests.Session()

    # Configura o Backoff Exponencial
    retries = Retry(
        total=5,                  # Tenta no máximo 5 vezes
        backoff_factor=2,         # Delays: 2s, 4s, 8s, 16s, 32s...
        status_forcelist=[500, 502, 503, 504], # Erros de servidor/indisponibilidade
        raise_on_status=True
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    print("Baixando arquivo da FUNCEME...")
    
    # Adicionado timeout de conexão/leitura para não travar infinitamente
    response = session.get(url, stream=True, timeout=(5, 30))
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
        Bucket="BUCKET_BRONZE",
        Key="bronze/raw/postos.zip"
    )

    print("Arquivo enviado com sucesso para o bucket S3.")
