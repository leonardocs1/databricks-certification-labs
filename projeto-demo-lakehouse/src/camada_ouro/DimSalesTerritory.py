# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimSalesTerritory"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesTerritoryKey"]

# Informações da Tabela Fonte
sales_territory_df = spark.read.table("adventure_works_silver.sales_salesterritory")

# COMMAND ----------

expected_schema = StructType([
    StructField("SalesTerritoryKey", IntegerType(), False),
    StructField("SalesTerritoryAlternateKey", IntegerType(), False),
    StructField("SalesTerritoryRegion", StringType(), False),
    StructField("SalesTerritoryCountry", StringType(), False),
    StructField("SalesTerritoryGroup", StringType(), False),
    # StructField("StartDate", TimestampType(), False),
    # StructField("EndDate", TimestampType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimSalesTerritory`

# COMMAND ----------

def transform_dim_sales_territory(sales_territory_df: DataFrame) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimSalesTerritory

    Parâmetros:
        sales_territory_df (DataFrame): DataFrame da tabela SalesTerritory.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela SalesTerritory com base em TerritoryID e ordenada pela coluna 'ModifiedDate'
    window_spec = Window.partitionBy("TerritoryID").orderBy(F.col("ModifiedDate").desc())
    sales_territory_df = sales_territory_df.withColumn("row_num", F.row_number().over(window_spec))
    sales_territory_df = sales_territory_df.filter(F.col("row_num") == 1).drop("row_num")

    # Selecionando e renomeando colunas conforme o modelo do DW
    dim_sales_territory = sales_territory_df.select(
        F.col("TerritoryID").alias("SalesTerritoryKey"),
        F.col("TerritoryID").alias("SalesTerritoryAlternateKey"),
        F.col("Name").alias("SalesTerritoryRegion"),
        F.col("CountryRegionCode").alias("SalesTerritoryCountry"),
        F.col("Group").alias("SalesTerritoryGroup"),
        # F.col("StartDate").alias("StartDate"),
        # F.col("EndDate").alias("EndDate")
    )

    return dim_sales_territory

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_sales_territory(sales_territory_df=sales_territory_df)

# Verificação da contagem e do schema
transformed_df.count()
transformed_df.printSchema()

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
# MAGIC ## Escrita da Tabela `DimSalesTerritory` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
