import base64
import json
from datetime import date, datetime
import requests

# Imports específicos do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, date_format, regexp_replace, trim
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
        "segment": "1",
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
    Extrai dados da API da B3 e os transforma usando PySpark.
    """
    res = requests.get(url)

    if res.status_code == 200:
        data = json.loads(res.text)
        if 'results' not in data or not data['results']:
            print("Alerta: Dados não encontrados ou lista de 'results' vazia. Retornando DataFrame vazio.")
            return spark.createDataFrame([], schema="data_referencia date, ticker string, nome_ativo string, tipo_ativo string, quantidade_teorica long, participacao_percentual float")
        
        results_schema = StructType([
            StructField("segment", StringType(), True),
            StructField("cod", StringType(), True),
            StructField("asset", StringType(), True),
            StructField("type", StringType(), True),
            StructField("part", StringType(), True),
            StructField("partAcum", StringType(), True),
            StructField("theoricalQty", StringType(), True)
        ])

        df_raw = spark.createDataFrame(data['results'], schema=results_schema)
        header_data = data.get('header', {})
        for key, value in header_data.items():
            column_literal = lit(value)
            if value is None:
                column_literal = column_literal.cast(StringType())

            df_raw = df_raw.withColumn(f"header_{key}", column_literal)

        raw_data_path = "./data/raw"
        df_raw.write.mode("overwrite").parquet(raw_data_path)
        print(f"Dados brutos salvos com sucesso em: {raw_data_path}")

    else:
        raise Exception(f"Erro ao acessar a URL: Status {res.status_code} | URL: {url}")

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
