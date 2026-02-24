# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: sales_salesorderdetail

# COMMAND ----------

# MAGIC
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, ShortType, TimestampType, StringType,
    DecimalType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "sales_salesorderdetail"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Sales_SalesOrderDetail"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderID", "SalesOrderDetailID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DecimalType, TimestampType, ShortType)

expected_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),           # int NOT NULL
    StructField("SalesOrderDetailID", IntegerType(), False),     # int IDENTITY(1,1) NOT NULL
    StructField("CarrierTrackingNumber", StringType(), True),    # nvarchar(25) NULL
    StructField("OrderQty", ShortType(), False),                 # smallint NOT NULL
    StructField("ProductID", IntegerType(), False),              # int NOT NULL
    StructField("SpecialOfferID", IntegerType(), False),         # int NOT NULL
    StructField("UnitPrice", DecimalType(19, 4), False),         # money NOT NULL
    StructField("UnitPriceDiscount", DecimalType(19, 4), False), # money NOT NULL
    StructField("LineTotal", DecimalType(38, 6), True),          # Calculado: UnitPrice * (1 - UnitPriceDiscount) * OrderQty
    StructField("rowguid", StringType(), False),                 # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_Sales_SalesOrderDetail(SalesOrderDetail: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Sales_SalesOrderDetail

    Parâmetros:
        SalesOrderDetail (DataFrame): DataFrame contendo os dados da tabela Sales_SalesOrderDetail

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define valores padrão para campos que podem ser nulos
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'LineTotal',
        F.expr("UnitPrice * (1.0 - UnitPriceDiscount) * OrderQty").cast(DecimalType(38, 6))
    )
    
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    SalesOrderDetail = SalesOrderDetail.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Aplicando checks de integridade
    SalesOrderDetail = SalesOrderDetail.filter(F.col('OrderQty') > 0)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('UnitPrice') >= 0.00)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('UnitPriceDiscount') >= 0.00)
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesOrderID', 'SalesOrderDetailID').orderBy(F.col('ModifiedDate').desc())
    SalesOrderDetail = SalesOrderDetail.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesOrderDetail = SalesOrderDetail.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesOrderDetail = SalesOrderDetail.select(
        F.col('SalesOrderID').cast(IntegerType()).alias('SalesOrderID'),
        F.col('SalesOrderDetailID').cast(IntegerType()).alias('SalesOrderDetailID'),
        F.col('CarrierTrackingNumber').cast(StringType()).alias('CarrierTrackingNumber'),
        F.col('OrderQty').cast(ShortType()).alias('OrderQty'),
        F.col('ProductID').cast(IntegerType()).alias('ProductID'),
        F.col('SpecialOfferID').cast(IntegerType()).alias('SpecialOfferID'),
        F.col('UnitPrice').cast(DecimalType(19, 4)).alias('UnitPrice'),
        F.col('UnitPriceDiscount').cast(DecimalType(19, 4)).alias('UnitPriceDiscount'),
        F.col('LineTotal').cast(DecimalType(38, 6)).alias('LineTotal'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesOrderDetail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Sales_SalesOrderDetail(SalesOrderDetail = bronze_source_table)

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
# MAGIC select * from adventure_works_silver.Sales_SalesOrderDetail limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.Sales_SalesOrderDetail
