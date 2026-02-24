# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: Address

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, TimestampType, StructType, StructField
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "person_address"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "person_address"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["AddressID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

expected_schema = StructType([
    StructField("AddressID", IntegerType(), False),             # int IDENTITY(1,1) NOT NULL
    StructField("AddressLine1", StringType(), False),           # nvarchar(60) NOT NULL
    StructField("AddressLine2", StringType(), True),            # nvarchar(60) NULL
    StructField("City", StringType(), False),                   # nvarchar(30) NOT NULL
    StructField("StateProvinceID", IntegerType(), False),       # int NOT NULL
    StructField("PostalCode", StringType(), False),             # nvarchar(15) NOT NULL
   # StructField("SpatialLocation", StringType(), True),         # geography NULL (geography type handled as StringType)
    StructField("rowguid", StringType(), False),                # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)         # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_Address(Address: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Address

    Parâmetros:
        Address (DataFrame): DataFrame contendo os dados da tabela Address

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    Address = Address.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    Address = Address.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('AddressID').orderBy(F.col('ModifiedDate').desc())
    Address = Address.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    Address = Address.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    Address = Address.select(
        F.col('AddressID').cast(IntegerType()).alias('AddressID'),
        F.col('AddressLine1').cast(StringType()).alias('AddressLine1'),
        F.col('AddressLine2').cast(StringType()).alias('AddressLine2'),
        F.col('City').cast(StringType()).alias('City'),
        F.col('StateProvinceID').cast(IntegerType()).alias('StateProvinceID'),
        F.col('PostalCode').cast(StringType()).alias('PostalCode'),
       # F.col('SpatialLocation').cast(StringType()).alias('SpatialLocation'),  # geography handled as StringType
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Address(Address = bronze_source_table)

transformed_df.printSchema() # quick way to check schema
transformed_df.count() # quick way to check rows


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
# MAGIC select * from adventure_works_silver.person_address limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.person_address
