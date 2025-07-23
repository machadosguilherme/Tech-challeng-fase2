import base64
import json
from datetime import date, datetime
import requests
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Iniciando o processo de extração de dados.")

# Imports específicos do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, date_format, regexp_replace, trim, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, DateType

def generate_b3_url(day):
    """
    Gera a URL da B3 para um dia específico.
    """
    base_url = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"
    
    # Objeto JSON com os parâmetros da requisição
    params = {
        "language": "pt-br",
        "pageNumber": 1,
        "pageSize": 150,
        "index": "IBOV",
        "segment": "2",
        "refDate": day.strftime('%Y-%m-%d')
    }

    # Converte o dicionário para uma string JSON
    json_params = json.dumps(params)

    # Codifica a string JSON em Base64
    base64_params = base64.b64encode(json_params.encode('utf-8')).decode('utf-8')
    # Retorna a URL completa
    return f"{base_url}{base64_params}"

def extract_data_from_page_spark(url, spark):
    """
    Extrai dados da API da B3, enriquece com uma coluna de partição e salva no S3.
    """
    logging.info(f"Extraindo dados da URL: {url}")
    res = requests.get(url)
    if res.status_code != 200:
        logging.error(f"Erro ao acessar a URL: Status {res.status_code} | URL: {url}")
        # Em um pipeline real, você poderia decidir parar ou apenas logar e continuar
        return

    data = json.loads(res.text)
    if 'results' not in data or not data['results']:
        logging.warning("Alerta: Dados não encontrados ou lista de 'results' vazia. Nenhum dado será ingerido.")
        return

    logging.info("Criando o DataFrame Spark a partir dos dados extraídos.")

    # 1. Schema para os dados dos ativos
    results_schema = StructType([
        StructField("segment", StringType(), True),
        StructField("cod", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("type", StringType(), True),
        StructField("part", StringType(), True),
        StructField("partAcum", StringType(), True),
        StructField("theoricalQty", StringType(), True)
    ])

    df = spark.createDataFrame(data['results'], schema=results_schema)
    header_data = data.get('header', {})

    df_with_metadata = df.withColumn("header_reductor", lit(header_data.get("reductor"))) \
                         .withColumn("header_theoricalQty", lit(header_data.get("theoricalQty")))

    df_final = df_with_metadata.withColumn(
        "dt_referencia",
        date_format(to_date(lit(header_data.get("date")), "dd/MM/yy"), "yyyyMMdd")
    )

    # logging.info(f"Salvando dados no S3 em: {s3_base_path}")
    # df_final.write \
    #     .partitionBy("dt_referencia") \
    #     .mode("overwrite") \
    #     .parquet(s3_base_path)

    # logging.info(f"Dados brutos particionados salvos com sucesso em: {s3_base_path}")
    # print("Amostra dos dados salvos no S3:")
    df_final.show(truncate=False)





if __name__ == "__main__":

    # 1. Inicializar a SparkSession (ponto de entrada para qualquer app Spark)
    spark = SparkSession.builder.appName("B3_IBOV_ETL").getOrCreate()

    # 2. Gerar a URL para o dia desejado
    today = date.today()
    url = generate_b3_url(today)
    print(f"Buscando dados para a URL: {url}")

    # 3. Chamar a função de extração
    df_spark = extract_data_from_page_spark(url, spark)
    spark.stop()
