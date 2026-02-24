# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: StateProvince

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, BooleanType, TimestampType, StructType, StructField
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "person_stateprovince"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Person_StateProvince"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["StateProvinceID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

expected_schema = StructType([
    StructField("StateProvinceID", IntegerType(), False),          # int NOT NULL
    StructField("StateProvinceCode", StringType(), False),         # nchar(3) NOT NULL
    StructField("CountryRegionCode", StringType(), False),         # nvarchar(3) NOT NULL
    StructField("IsOnlyStateProvinceFlag", BooleanType(), False),  # Flag NOT NULL
    StructField("Name", StringType(), False),                      # Name NOT NULL
    StructField("TerritoryID", IntegerType(), False),              # int NOT NULL
    StructField("rowguid", StringType(), False),                   # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)            # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_Person_StateProvince(StateProvince: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Person_StateProvince

    Parâmetros:
        StateProvince (DataFrame): DataFrame contendo os dados da tabela Person_StateProvince

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    StateProvince = StateProvince.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    StateProvince = StateProvince.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('StateProvinceID').orderBy(F.col('ModifiedDate').desc())
    StateProvince = StateProvince.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    StateProvince = StateProvince.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    StateProvince = StateProvince.select(
        F.col('StateProvinceID').cast(IntegerType()).alias('StateProvinceID'),
        F.col('StateProvinceCode').cast(StringType()).alias('StateProvinceCode'),
        F.col('CountryRegionCode').cast(StringType()).alias('CountryRegionCode'),
        F.col('IsOnlyStateProvinceFlag').cast(BooleanType()).alias('IsOnlyStateProvinceFlag'),
        F.col('Name').cast(StringType()).alias('Name'),
        F.col('TerritoryID').cast(IntegerType()).alias('TerritoryID'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return StateProvince

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Person_StateProvince(StateProvince = bronze_source_table)

transformed_df.count() # quick way to check rows
transformed_df.printSchema() # quick way to check schema

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
# MAGIC select * from adventure_works_silver.Person_StateProvince limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.Person_StateProvince
