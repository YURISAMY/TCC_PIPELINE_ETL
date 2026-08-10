import requests
import boto3

# STEP 1: Baixar o arquivo da FUNCEME
url = "https://cdn.funceme.br/calendario/postos/postos.zip"
arquivo_local = "postos.zip"

print("Baixando arquivo da FUNCEME...")

response = requests.get(url, stream=True)

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

s3_client.upload_file(Filename=arquivo_local, Bucket="medallion-bronze", Key="bronze/raw/postos.zip")

print("Arquivo enviado com sucesso para o bucket S3.")

#pode ser cadastrado como lambda function no AWS para rodar periodicamente, por exemplo, a cada 6 horas.