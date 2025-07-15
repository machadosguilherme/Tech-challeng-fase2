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
            # Retorna um DataFrame vazio com o esquema esperado para evitar erros downstream
            return spark.createDataFrame([], schema="data_referencia date, ticker string, nome_ativo string, tipo_ativo string, quantidade_teorica long, participacao_percentual float")
        
        schema = StructType([
            StructField("segment", StringType(), True),
            StructField("cod", StringType(), True),
            StructField("asset", StringType(), True),
            StructField("type", StringType(), True),
            StructField("part", StringType(), True),
            StructField("partAcum", StringType(), True),
            StructField("theoricalQty", StringType(), True)
        ])
            
        #Criar o dataframe
        df = spark.createDataFrame(data['results'], schema=schema)

        #Coluna de data de processamento
        df = df.withColumn("dataproc", date_format(current_date(), 'yyyyMMdd').cast("int"))

        # 2. Aplicar as transformações usando withColumn
        df_transformed = df.withColumn(
            # Adiciona a data de referência da carteira
            "data_referencia", lit(datetime.strptime(data['header']['date'], '%d/%m/%y').date()).cast(DateType())
        ).withColumn(
            # Converte a coluna de participação para Float
            "participacao_percentual", regexp_replace(col('part'), ',', '.').cast(FloatType())
        ).withColumn(
            # Converte a quantidade teórica para Long (Int64)
            # É preciso escapar o ponto (\\.) para que o replace o trate como um caractere literal
            "quantidade_teorica", regexp_replace(col('theoricalQty'), '\\.', '').cast(LongType())
        ).withColumn(
            # Limpa espaços em branco
            "tipo_ativo", trim(col('type'))
        )

        # 3. Selecionar e renomear as colunas para o formato final
        df_final = df_transformed.select(
            col('data_referencia'),
            col('cod').alias('ticker'),
            col('asset').alias('nome_ativo'),
            col('tipo_ativo'),
            col('quantidade_teorica'),
            col('participacao_percentual'),
            col('dataproc')
        )
        
        print("DataFrame PySpark processado com sucesso!")
        return df_final

    else:
        raise Exception(f"Erro ao acessar a URL: Status {res.status_code} | URL: {url}")

if __name__ == "__main__":

    # 1. Inicializar a SparkSession (ponto de entrada para qualquer app Spark)
    spark = SparkSession.builder.appName("B3_IBOV_ETL").getOrCreate()

    # 2. Gerar a URL para o dia desejado
    # Usando uma data fixa para garantir que haja dados (ajuste conforme necessário)
    today = date.today()
    url = generate_b3_url(today)
    print(f"Buscando dados para a URL: {url}")

    # 3. Chamar a função de extração e transformação
    df_spark = extract_data_from_page_spark(url, spark)

    #Salvar em um arquivo .csv
    df_spark.write.csv("ibov_14_07")



    
    # 4. Salvar os dados processados em um arquivo Parquet
    if df_spark.count() > 0:
        output_path = "./data/raw"
        
        # O método de escrita do Spark é um pouco diferente
        df_spark.write.mode("overwrite").parquet(output_path)
        
        print(f"DataFrame salvo com sucesso em '{output_path}'!")
        print("Amostra dos dados:")
        df_spark.show(5)
    else:
        print("Nenhum dado foi processado ou salvo.")

    # 5. Parar a sessão Spark (importante para liberar recursos)
    spark.stop()