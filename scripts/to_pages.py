import io
import json
import boto3

# 1. Conexão direta com o MiniStack local
s3_local = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

BUCKET_GOLD = "medallion-gold"
KEY_GOLD = "gold/postos_resumo.json"

print(f"Baixando {KEY_GOLD} do bucket local {BUCKET_GOLD}...")
objeto = s3_local.get_object(Bucket=BUCKET_GOLD, Key=KEY_GOLD)
dados_gold = json.loads(objeto["Body"].read().decode("utf-8"))
print(f"Total de postos recebidos: {len(dados_gold)}")

# 2. Mapeamento de compatibilidade específico para o GitHub Pages
mapa_meses = {
    "Jan_chuva": "Mes_Jan",
    "Fev_chuva": "Mes_Fev",
    "Mar_chuva": "Mes_Mar",
    "Apr_chuva": "Mes_Apr",
    "May_chuva": "Mes_May",
    "Jun_chuva": "Mes_Jun",
    "Jul_chuva": "Mes_Jul",
    "Aug_chuva": "Mes_Aug",
    "Sep_chuva": "Mes_Sep",
    "Oct_chuva": "Mes_Oct",
    "Nov_chuva": "Mes_Nov",
    "Dec_chuva": "Mes_Dec",
}

dados_adaptados = []

for item in dados_gold:
    id_posto = item.get("Chave_ID", item.get("ID"))
    nome_posto = item.get("Nome_Posto", "").strip().replace(" ", "_")

    link_cdn = f"https://cdn.jsdelivr.net/gh/infracosteira/pluviometria@main/data/postos_solo/{id_posto}_{nome_posto}.csv"

    novo_item = {
        "ID": str(id_posto),
        "link_csv": link_cdn,
        "Nome_Posto": item.get("Nome_Posto"),
        "Nome_Municipio": item.get("Nome_Municipio"),
        "Coordenada_Y": item.get("Coordenada_Y"),
        "Coordenada_X": item.get("Coordenada_X"),
        "Ano_Inicio": item.get("Ano_Inicio"),
        "Ano_Fim": item.get("Ano_Fim"),
        "Mes_Inicio": item.get("Mes_Inicio"),
        "Mes_Fim": item.get("Mes_Fim"),
        "Total_dias_intervalo": item.get("Total_dias_intervalo"),
        "Dias_dados_medidos": item.get("Dias_dados_medidos"),
        "Dias_falhos": item.get("Dias_falhos"),
        "Percentual_dias_falhos": item.get("Percentual_dias_falhos"),
        "Total_meses_intervalo": item.get("Total_meses_intervalo"),
        "Numero_meses_completos": item.get("Numero_meses_completos"),
        "Numero_meses_falha": item.get("Numero_meses_falha"),
        "Percentual_meses_falha": item.get("Percentual_meses_falha"),
        "Total_anos_intervalo": item.get("Total_anos_intervalo"),
        "Numero_anos_completos": item.get("Numero_anos_completos"),
        "Numero_anos_falha": item.get("Numero_anos_falha"),
        "Percentual_anos_falha": item.get("Percentual_anos_falha"),
        "Precipitacao_media_anual": item.get("Precipitacao_media_anual"),
    }

    for mes_gold, mes_site in mapa_meses.items():
        novo_item[mes_site] = item.get(mes_gold, "0.0")

    dados_adaptados.append(novo_item)

# 3. Salva o arquivo pronto para substituir o do GitHub
NOME_ARQUIVO_FINAL = "dados_formatados_resumo.json"
with open(NOME_ARQUIVO_FINAL, "w", encoding="utf-8") as f:
    json.dump(dados_adaptados, f, ensure_ascii=False, indent=2)

print(f"\nSucesso! Arquivo '{NOME_ARQUIVO_FINAL}' gerado.")