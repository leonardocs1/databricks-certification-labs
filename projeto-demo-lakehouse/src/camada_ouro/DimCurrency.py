# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimCurrency"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["CurrencyKey"]

# Informações da Tabela Fonte
currency_df = spark.read.table("adventure_works_silver.sales_currency")

# COMMAND ----------

expected_schema = StructType([
    StructField("CurrencyKey", StringType(), False),
    StructField("CurrencyAlternateKey", StringType(), False),
    StructField("CurrencyName", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimCurrency`

# COMMAND ----------

def transform_dim_currency(currency_df: DataFrame) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimCurrency

    Parâmetros:
        currency_df (DataFrame): DataFrame da tabela Currency.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela Currency com base na chave primária e ordenada pela coluna 'ModifiedDate'
    window_spec = Window.partitionBy("CurrencyCode").orderBy(F.col("ModifiedDate").desc())
    currency_df = currency_df.withColumn("row_num", F.row_number().over(window_spec))
    currency_df = currency_df.filter(F.col("row_num") == 1).drop("row_num")

    # Selecionando colunas e renomeando conforme o modelo do DW
    dim_currency = currency_df.select(
        F.col("CurrencyCode").alias("CurrencyKey"),
        F.col("CurrencyCode").alias("CurrencyAlternateKey"),
        F.col("Name").alias("CurrencyName")
    )

    return dim_currency

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_currency(currency_df=currency_df)

# Verificação da contagem e do schema
transformed_df.count()  # Verifica o número de linhas
transformed_df.printSchema()  # Verifica o schema do DataFrame

# COMMAND ----------

# Validação do Schema
is_schema_valid = _validate_schema(transformed_df, expected_schema)

if is_schema_valid:
    print("O schema do DataFrame está correto.")
else:
    print("O schema do DataFrame está incorreto.")
    raise Exception("Schema validation failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita da Tabela `DimCurrency` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
