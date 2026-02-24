# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: Person

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
source_table = "person_person"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "person_person"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["BusinessEntityID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

expected_schema = StructType([
    StructField("BusinessEntityID", IntegerType(), False),                # int NOT NULL
    StructField("PersonType", StringType(), False),                       # nchar(2) NOT NULL
    StructField("NameStyle", IntegerType(), False),                       # dbo.NameStyle NOT NULL (Assumed IntegerType)
    StructField("Title", StringType(), True),                             # nvarchar(8) NULL
    StructField("FirstName", StringType(), False),                        # dbo.Name NOT NULL
    StructField("MiddleName", StringType(), True),                        # dbo.Name NULL
    StructField("LastName", StringType(), False),                         # dbo.Name NOT NULL
    StructField("Suffix", StringType(), True),                            # nvarchar(10) NULL
    StructField("EmailPromotion", IntegerType(), False),                  # int NOT NULL
    StructField("AdditionalContactInfo", StringType(), True),             # xml NULL
    StructField("Demographics", StringType(), True),                      # xml NULL
    StructField("rowguid", StringType(), False),                          # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)                   # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_Person(Person: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Person

    Parâmetros:
        Person (DataFrame): DataFrame contendo os dados da tabela Person

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    Person = Person.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    Person = Person.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Aplicando checks de integridade
    Person = Person.filter((F.col('EmailPromotion') >= 0) & (F.col('EmailPromotion') <= 2))
    Person = Person.filter(F.col('PersonType').isin(['GC', 'SP', 'EM', 'IN', 'VC', 'SC']))
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('BusinessEntityID').orderBy(F.col('ModifiedDate').desc())
    Person = Person.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    Person = Person.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    Person = Person.select(
        F.col('BusinessEntityID').cast(IntegerType()).alias('BusinessEntityID'),
        F.col('PersonType').cast(StringType()).alias('PersonType'),
        F.col('NameStyle').cast(IntegerType()).alias('NameStyle'),
        F.col('Title').cast(StringType()).alias('Title'),
        F.col('FirstName').cast(StringType()).alias('FirstName'),
        F.col('MiddleName').cast(StringType()).alias('MiddleName'),
        F.col('LastName').cast(StringType()).alias('LastName'),
        F.col('Suffix').cast(StringType()).alias('Suffix'),
        F.col('EmailPromotion').cast(IntegerType()).alias('EmailPromotion'),
        F.col('AdditionalContactInfo').cast(StringType()).alias('AdditionalContactInfo'),  # XML handled as StringType
        F.col('Demographics').cast(StringType()).alias('Demographics'),                    # XML handled as StringType
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return Person

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Person(Person = bronze_source_table)

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
# MAGIC select * from adventure_works_silver.person_person limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.person_person
