# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimSalesReason"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesReasonKey"]

# Informações da Tabela Fonte
sales_reason_df = spark.read.table("adventure_works_silver.sales_salesreason")

# COMMAND ----------

expected_schema = StructType([
    StructField("SalesReasonKey", IntegerType(), False),
    StructField("SalesReasonName", StringType(), False),
    StructField("SalesReasonType", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimSalesReason`

# COMMAND ----------

def transform_dim_sales_reason(sales_reason_df: DataFrame) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimSalesReason

    Parâmetros:
        sales_reason_df (DataFrame): DataFrame da tabela SalesReason.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela SalesReason com base em SalesReasonID e ordenada pela coluna 'ModifiedDate'
    window_spec = Window.partitionBy("SalesReasonID").orderBy(F.col("ModifiedDate").desc())
    sales_reason_df = sales_reason_df.withColumn("row_num", F.row_number().over(window_spec))
    sales_reason_df = sales_reason_df.filter(F.col("row_num") == 1).drop("row_num")

    # Selecionando e renomeando colunas conforme o modelo do DW
    dim_sales_reason = sales_reason_df.select(
        F.col("SalesReasonID").alias("SalesReasonKey"),
        F.col("Name").alias("SalesReasonName"),
        F.col("ReasonType").alias("SalesReasonType")
    )

    return dim_sales_reason

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_sales_reason(sales_reason_df=sales_reason_df)

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
# MAGIC ## Escrita da Tabela `DimSalesReason` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
