# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: sales_salesorderheader

# COMMAND ----------

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
source_table = "sales_salesorderheader"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Sales_SalesOrderHeader"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DoubleType, DecimalType, TimestampType, ShortType)

expected_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),           # int IDENTITY(1,1) NOT NULL
    StructField("RevisionNumber", ShortType(), False),           # tinyint NOT NULL
    StructField("OrderDate", TimestampType(), False),            # datetime NOT NULL
    StructField("DueDate", TimestampType(), False),              # datetime NOT NULL
    StructField("ShipDate", TimestampType(), True),              # datetime NULL
    StructField("Status", ShortType(), False),                   # tinyint NOT NULL
    StructField("OnlineOrderFlag", IntegerType(), False),        # Flag NOT NULL 
    StructField("SalesOrderNumber", StringType(), True),         # nvarchar(23) NOT NULL
    StructField("PurchaseOrderNumber", StringType(), True),      # OrderNumber NULL
    StructField("AccountNumber", StringType(), True),            # AccountNumber NULL
    StructField("CustomerID", IntegerType(), False),             # int NOT NULL
    StructField("SalesPersonID", IntegerType(), True),           # int NULL
    StructField("TerritoryID", IntegerType(), True),             # int NULL
    StructField("BillToAddressID", IntegerType(), False),        # int NOT NULL
    StructField("ShipToAddressID", IntegerType(), False),        # int NOT NULL
    StructField("ShipMethodID", IntegerType(), False),           # int NOT NULL
    StructField("CreditCardID", IntegerType(), True),            # int NULL
    StructField("CreditCardApprovalCode", StringType(), True),   # varchar(15) NULL
    StructField("CurrencyRateID", IntegerType(), True),          # int NULL
    StructField("SubTotal", DecimalType(19, 4), False),          # money NOT NULL
    StructField("TaxAmt", DecimalType(19, 4), False),            # money NOT NULL
    StructField("Freight", DecimalType(19, 4), False),           # money NOT NULL
    StructField("TotalDue", DecimalType(38, 6), True),           # numeric(38, 6) NOT NULL
  
    StructField("rowguid", StringType(), False),                 # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)          # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, ShortType, TimestampType, StringType,
    DecimalType
)

def transform_Sales_SalesOrderHeader(SalesOrderHeader: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Sales_SalesOrderHeader

    Parâmetros:
        SalesOrderHeader (DataFrame): DataFrame contendo os dados da tabela Sales_SalesOrderHeader

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define valores padrão para campos que podem ser nulos
    SalesOrderHeader = SalesOrderHeader.withColumn(
        'TotalDue',
        F.when(F.col('SubTotal').isNotNull() & F.col('TaxAmt').isNotNull() & F.col('Freight').isNotNull(),
               F.col('SubTotal') + F.col('TaxAmt') + F.col('Freight')).otherwise(0.0)
    )
    
    SalesOrderHeader = SalesOrderHeader.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    SalesOrderHeader = SalesOrderHeader.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # Aplicando checks de integridade
    SalesOrderHeader = SalesOrderHeader.filter(F.col('SubTotal') >= 0.00)
    SalesOrderHeader = SalesOrderHeader.filter(F.col('TaxAmt') >= 0.00)
    SalesOrderHeader = SalesOrderHeader.filter(F.col('Freight') >= 0.00)
    SalesOrderHeader = SalesOrderHeader.filter((F.col('Status') >= 0) & (F.col('Status') <= 8))
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('SalesOrderID').orderBy(F.col('ModifiedDate').desc())
    SalesOrderHeader = SalesOrderHeader.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    SalesOrderHeader = SalesOrderHeader.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    SalesOrderHeader = SalesOrderHeader.select(
        F.col('SalesOrderID').cast(IntegerType()).alias('SalesOrderID'),
        F.col('RevisionNumber').cast(ShortType()).alias('RevisionNumber'),
        F.col('OrderDate').cast(TimestampType()).alias('OrderDate'),
        F.col('DueDate').cast(TimestampType()).alias('DueDate'),
        F.col('ShipDate').cast(TimestampType()).alias('ShipDate'),
        F.col('Status').cast(ShortType()).alias('Status'),
        F.col('OnlineOrderFlag').cast(IntegerType()).alias('OnlineOrderFlag'),
        F.col('SalesOrderNumber').cast(StringType()).alias('SalesOrderNumber'),
        F.col('PurchaseOrderNumber').cast(StringType()).alias('PurchaseOrderNumber'),
        F.col('AccountNumber').cast(StringType()).alias('AccountNumber'),
        F.col('CustomerID').cast(IntegerType()).alias('CustomerID'),
        F.col('SalesPersonID').cast(IntegerType()).alias('SalesPersonID'),
        F.col('TerritoryID').cast(IntegerType()).alias('TerritoryID'),
        F.col('BillToAddressID').cast(IntegerType()).alias('BillToAddressID'),
        F.col('ShipToAddressID').cast(IntegerType()).alias('ShipToAddressID'),
        F.col('ShipMethodID').cast(IntegerType()).alias('ShipMethodID'),
        F.col('CreditCardID').cast(IntegerType()).alias('CreditCardID'),
        F.col('CreditCardApprovalCode').cast(StringType()).alias('CreditCardApprovalCode'),
        F.col('CurrencyRateID').cast(IntegerType()).alias('CurrencyRateID'),
        F.col('SubTotal').cast(DecimalType(19,4)).alias('SubTotal'),
        F.col('TaxAmt').cast(DecimalType(19,4)).alias('TaxAmt'),
        F.col('Freight').cast(DecimalType(19,4)).alias('Freight'),
        F.col('TotalDue').cast(DecimalType(38,6)).alias('TotalDue'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return SalesOrderHeader

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Sales_SalesOrderHeader(SalesOrderHeader = bronze_source_table)

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
_upsert_silver_table(transformed_df, target_table, primary_keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar os Dados na Tabela Prata

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from adventure_works_silver.Sales_SalesOrderHeader limit 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from adventure_works_silver.Sales_SalesOrderHeader

# COMMAND ----------



# COMMAND ----------

#declarar Schema

# COMMAND ----------

#declarar função de transformação

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


