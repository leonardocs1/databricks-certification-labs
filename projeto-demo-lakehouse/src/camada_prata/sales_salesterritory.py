# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: SalesTerritory

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, DecimalType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "sales_salesterritory"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "sales_salesterritory"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["TerritoryID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DecimalType, TimestampType)

expected_schema = StructType([
    StructField("TerritoryID", IntegerType(), False),          # int NOT NULL
    StructField("Name", StringType(), False),                  # Name NOT NULL
    StructField("CountryRegionCode", StringType(), False),     # nvarchar(3) NOT NULL
    StructField("Group", StringType(), False),                 # nvarchar(50) NOT NULL
    StructField("SalesYTD", DecimalType(19, 4), False),        # money NOT NULL
    StructField("SalesLastYear", DecimalType(19, 4), False),   # money NOT NULL
    StructField("CostYTD", DecimalType(19, 4), False),         # money NOT NULL
    StructField("CostLastYear", DecimalType(19, 4), False),    # money NOT NULL
    StructField("rowguid", StringType(), False),               # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)        # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_SalesTerritory(SalesTerritory: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: SalesTerritory

    Parâmetros:
        SalesTerritory (DataFrame): DataFrame contendo os dados da tabela SalesTerritory

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    SalesTerritory = SalesTerritory.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    SalesTerritory = SalesTerritory.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Aplicando checks de integridade
    SalesTerritory = SalesTerritory.filter(F.col('SalesYTD') >= 0.00)
    SalesTerritory = SalesTerritory.filter(F.col('SalesLastYear') >= 0.00)
    SalesTerritory = SalesTerritory.filter(F.col('CostYTD') >= 0.00)
    SalesTerritory = SalesTerritory.filter(F.col('CostLastYear') >= 0.00)
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('TerritoryID').orderBy(F.col('ModifiedDate').desc())
    SalesTerritory = SalesTerritory.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesTerritory = SalesTerritory.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesTerritory = SalesTerritory.select(
        F.col('TerritoryID').cast(IntegerType()).alias('TerritoryID'),
        F.col('Name').cast(StringType()).alias('Name'),
        F.col('CountryRegionCode').cast(StringType()).alias('CountryRegionCode'),
        F.col('Group').cast(StringType()).alias('Group'),
        F.col('SalesYTD').cast(DecimalType(19, 4)).alias('SalesYTD'),
        F.col('SalesLastYear').cast(DecimalType(19, 4)).alias('SalesLastYear'),
        F.col('CostYTD').cast(DecimalType(19, 4)).alias('CostYTD'),
        F.col('CostLastYear').cast(DecimalType(19, 4)).alias('CostLastYear'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesTerritory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_SalesTerritory(SalesTerritory = bronze_source_table)

transformed_df.count() #quick way to check rows
transformed_df.printSchema() #quick way to check schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação do Schema

# COMMAND ----------

# Verificação do Schema
is_schema_valid = _validate_schema(transformed_df, expected_schema)

if is_schema_valid:
    print("O schema do DataFrame está correto.")
else:
    print("O schema do DataFrame está incorreto.")
    raise Exception("Schema validation failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar o Upsert na Tabela Prata

# COMMAND ----------

# Chamar a função para realizar o upsert
_upsert_silver_table(transformed_df, target_table, primary_keys, not_matched_by_source_action="DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar os Dados na Tabela Prata

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from adventure_works_silver.sales_salesterritory limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.sales_salesterritory
