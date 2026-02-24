# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: SpecialOffer

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
source_table = "sales_specialoffer"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "sales_specialoffer"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SpecialOfferID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DecimalType, TimestampType)

expected_schema = StructType([
    StructField("SpecialOfferID", IntegerType(), False),         # int IDENTITY(1,1) NOT NULL
    StructField("Description", StringType(), False),             # nvarchar(255) NOT NULL
    StructField("DiscountPct", DecimalType(10, 4), False),       # smallmoney NOT NULL
    StructField("Type", StringType(), False),                    # nvarchar(50) NOT NULL
    StructField("Category", StringType(), False),                # nvarchar(50) NOT NULL
    StructField("StartDate", TimestampType(), False),            # datetime NOT NULL
    StructField("EndDate", TimestampType(), False),              # datetime NOT NULL
    StructField("MinQty", IntegerType(), False),                 # int NOT NULL
    StructField("MaxQty", IntegerType(), True),                  # int NULL
    StructField("rowguid", StringType(), False),                 # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_SpecialOffer(SpecialOffer: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: SpecialOffer

    Parâmetros:
        SpecialOffer (DataFrame): DataFrame contendo os dados da tabela SpecialOffer

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define valores padrão para campos que podem ser nulos
    SpecialOffer = SpecialOffer.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    SpecialOffer = SpecialOffer.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Aplicando checks de integridade
    SpecialOffer = SpecialOffer.filter(F.col('DiscountPct') >= 0.00)
    SpecialOffer = SpecialOffer.filter(F.col('EndDate') >= F.col('StartDate'))
    SpecialOffer = SpecialOffer.filter((F.col('MaxQty') >= 0) | (F.col('MaxQty').isNull()))
    SpecialOffer = SpecialOffer.filter(F.col('MinQty') >= 0)
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SpecialOfferID').orderBy(F.col('ModifiedDate').desc())
    SpecialOffer = SpecialOffer.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SpecialOffer = SpecialOffer.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SpecialOffer = SpecialOffer.select(
        F.col('SpecialOfferID').cast(IntegerType()).alias('SpecialOfferID'),
        F.col('Description').cast(StringType()).alias('Description'),
        F.col('DiscountPct').cast(DecimalType(10, 4)).alias('DiscountPct'),
        F.col('Type').cast(StringType()).alias('Type'),
        F.col('Category').cast(StringType()).alias('Category'),
        F.col('StartDate').cast(TimestampType()).alias('StartDate'),
        F.col('EndDate').cast(TimestampType()).alias('EndDate'),
        F.col('MinQty').cast(IntegerType()).alias('MinQty'),
        F.col('MaxQty').cast(IntegerType()).alias('MaxQty'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SpecialOffer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_SpecialOffer(SpecialOffer = bronze_source_table)

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
# MAGIC select * from adventure_works_silver.sales_specialoffer limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.sales_specialoffer
