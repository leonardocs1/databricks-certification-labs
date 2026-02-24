# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: SalesReason

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "sales_salesreason"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "sales_salesreason"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesReasonID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, TimestampType)

expected_schema = StructType([
    StructField("SalesReasonID", IntegerType(), False),          # int NOT NULL
    StructField("Name", StringType(), False),                    # Name NOT NULL
    StructField("ReasonType", StringType(), False),              # ReasonType NOT NULL
    StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_SalesReason(SalesReason: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: SalesReason

    Parâmetros:
        SalesReason (DataFrame): DataFrame contendo os dados da tabela SalesReason

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    SalesReason = SalesReason.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesReasonID').orderBy(F.col('ModifiedDate').desc())
    SalesReason = SalesReason.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesReason = SalesReason.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesReason = SalesReason.select(
        F.col('SalesReasonID').cast(IntegerType()).alias('SalesReasonID'),
        F.col('Name').cast(StringType()).alias('Name'),
        F.col('ReasonType').cast(StringType()).alias('ReasonType'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesReason

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_SalesReason(SalesReason = bronze_source_table)

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
# MAGIC select * from adventure_works_silver.sales_salesreason limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.sales_salesreason
