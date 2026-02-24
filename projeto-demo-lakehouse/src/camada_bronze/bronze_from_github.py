# Databricks notebook source


# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

# Criar sessão Spark
spark = SparkSession.builder.appName("BronzeLayerFiltered").getOrCreate()

# URL base do repositório GitHub
github_raw_url = "https://raw.githubusercontent.com/AprenderDados/datasets/main/AdventureWorksOLTP/"

# Criar Database Bronze no Databricks
spark.sql("CREATE DATABASE IF NOT EXISTS adventure_works_bronze")

# Dicionário com as tabelas selecionadas (ordenadas alfabeticamente)
dict_tables = {
    "person_address": "Person_Address.parquet",
    "person_countryregion": "Person_CountryRegion.parquet",
    "person_emailaddress": "Person_EmailAddress.parquet",
    "person_person": "Person_Person.parquet",
    "person_personphone": "Person_PersonPhone.parquet",
    "person_stateprovince": "Person_StateProvince.parquet",
    "production_product": "Production_Product.parquet",
    "production_productcategory": "Production_ProductCategory.parquet",
    "production_productdescription": "Production_ProductDescription.parquet",
    "production_productmodel": "Production_ProductModel.parquet",
    "production_productsubcategory": "Production_ProductSubcategory.parquet",
    "sales_currency": "Sales_Currency.parquet",
    "sales_customer": "Sales_Customer.parquet",
    "sales_salesorderdetail": "Sales_SalesOrderDetail.parquet",
    "sales_salesorderheader": "Sales_SalesOrderHeader.parquet",
    "sales_salesreason": "Sales_SalesReason.parquet",
    "sales_salesterritory": "Sales_SalesTerritory.parquet",
    "sales_specialoffer": "Sales_SpecialOffer.parquet"
}

# Função para ler e processar cada tabela
def ler_e_processar(table_name, file_name):
    url = f"{github_raw_url}{file_name}"

    try:
        # Ler com Pandas
        df_pandas = pd.read_parquet(url)

        # Converter para Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas)

        # Salvar como Tabela Delta (modo overwrite)
        df_spark.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"adventure_works_bronze.{table_name}")

        print(f"🚀 Tabela 'adventure_works_bronze.{table_name}' criada com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao processar {table_name}: {str(e)}")



# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# Número de threads (ajuste conforme a necessidade)
num_threads = min(8, len(dict_tables))  # Máximo de 8 threads ou o número de tabelas disponíveis

# Executar a carga em paralelo
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = {executor.submit(ler_e_processar, table, file): table for table, file in dict_tables.items()}

    # Monitorar o progresso
    for future in futures:
        try:
            future.result()  # Aguarda a execução da thread
        except Exception as e:
            print(f"⚠ Erro ao processar tabela {futures[future]}: {e}")

print("\n🎯 Todas as tabelas Bronze foram criadas com sucesso em paralelo! 🚀")


# COMMAND ----------


