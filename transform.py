import base64
import json
from datetime import date, datetime
import requests

# Imports específicos do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, date_format, regexp_replace, trim, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, DateType

def transform_data(spark):
    """
    Transforma os dados extraídos da API da B3 usando PySpark.
    Esta função assume que os dados já foram extraídos e estão disponíveis em um DataFrame PySpark.
    """

    raw_data_path = "./data/raw"
    df = spark.read.parquet(raw_data_path)

    if df.rdd.isEmpty():
        print("Alerta: DataFrame vazio. Retornando DataFrame vazio.")
        return spark.createDataFrame([], schema="data_referencia date, ticker string, nome_ativo string, tipo_ativo string, quantidade_teorica long, participacao_percentual float")


    # Carrega os dados da camada RAW que você acabou de salvar
    df_raw = spark.read.parquet("./data/raw")

    # Transforma os dados brutos em uma tabela limpa
    df_transformed = df_raw.select(
        to_date(col("header_date"), "dd/MM/yy").alias("data_referencia"),
        col("cod").alias("ticker"),
        col("asset").alias("nome_ativo"),
        trim(col("type")).alias("tipo_ativo"),
        regexp_replace(col("part"), ',', '.').cast(FloatType()).alias("participacao_percentual"),
        regexp_replace(col("theoricalQty"), '\\.', '').cast(LongType()).alias("quantidade_teorica")
    )

    #Coluna de data de processamento
    df_transformed = df_transformed.withColumn("dataproc", date_format(current_date(), 'yyyyMMdd').cast("int"))
    df_transformed

    # Salva a tabela limpa na camada "transformed"
    transformed_data_path = "./data/transformed"
    df_transformed.write.mode("overwrite").parquet(transformed_data_path)

    # Visualiza o resultado final e limpo
    print("Tabela final e limpa da camada 'Transformed':")
    df_transformed.show()

if __name__ == "__main__":
    # 1. Criar uma sessão Spark
    spark = SparkSession.builder.appName("B3_IBOV_ETL").getOrCreate()
    df_spark = transform_data(spark)


