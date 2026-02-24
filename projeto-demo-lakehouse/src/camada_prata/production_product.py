# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformação camada prata: Production_Product

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, ShortType, StringType, DecimalType,
    TimestampType, BooleanType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir Tabelas Fonte e Destino

# COMMAND ----------

# Informações da Tabela Fonte
source_table = "production_product"
source_database = "adventure_works_bronze"
bronze_source_table = spark.read.table(f"{source_database}.{source_table}")

# Informações da Tabela Destino (target)
target_table_name = "Production_Product"
target_database = "adventure_works_silver"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["ProductID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declarar Schema Esperado

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DecimalType, TimestampType, ShortType, BooleanType)

expected_schema = StructType([
    StructField("ProductID", IntegerType(), False),                  # int IDENTITY(1,1) NOT NULL
    StructField("Name", StringType(), False),                        # Name NOT NULL
    StructField("ProductNumber", StringType(), False),               # nvarchar(25) NOT NULL
    StructField("MakeFlag", BooleanType(), False),                   # Flag NOT NULL
    StructField("FinishedGoodsFlag", BooleanType(), False),          # Flag NOT NULL
    StructField("Color", StringType(), True),                        # nvarchar(15) NULL
    StructField("SafetyStockLevel", ShortType(), False),             # smallint NOT NULL
    StructField("ReorderPoint", ShortType(), False),                 # smallint NOT NULL
    StructField("StandardCost", DecimalType(19, 4), False),          # money NOT NULL
    StructField("ListPrice", DecimalType(19, 4), False),             # money NOT NULL
    StructField("Size", StringType(), True),                         # nvarchar(5) NULL
    StructField("SizeUnitMeasureCode", StringType(), True),          # nchar(3) NULL
    StructField("WeightUnitMeasureCode", StringType(), True),        # nchar(3) NULL
    StructField("Weight", DecimalType(8, 2), True),                  # decimal(8, 2) NULL
    StructField("DaysToManufacture", IntegerType(), False),          # int NOT NULL
    StructField("ProductLine", StringType(), True),                  # nchar(2) NULL
    StructField("Class", StringType(), True),                        # nchar(2) NULL
    StructField("Style", StringType(), True),                        # nchar(2) NULL
    StructField("ProductSubcategoryID", IntegerType(), True),        # int NULL
    StructField("ProductModelID", IntegerType(), True),              # int NULL
    StructField("SellStartDate", TimestampType(), False),            # datetime NOT NULL
    StructField("SellEndDate", TimestampType(), True),               # datetime NULL
    StructField("DiscontinuedDate", TimestampType(), True),          # datetime NULL
    StructField("rowguid", StringType(), False),                     # uniqueidentifier NOT NULL
    StructField("ModifiedDate", TimestampType(), False)              # datetime NOT NULL
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação

# COMMAND ----------

def transform_Production_Product(Product: DataFrame) -> DataFrame:
    '''
    Transformação da tabela: Production_Product

    Parâmetros:
        Product (DataFrame): DataFrame contendo os dados da tabela Production_Product

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''
    
    # Define valores padrão para campos que podem ser nulos
    Product = Product.withColumn(
        'rowguid',
        F.when(F.col('rowguid').isNull(), F.expr('uuid()')).otherwise(F.col('rowguid'))
    )
    
    Product = Product.withColumn(
        'ModifiedDate',
        F.when(F.col('ModifiedDate').isNull(), F.current_timestamp()).otherwise(F.col('ModifiedDate'))
    )
    
    # # Aplicando checks de integridade
    # Product = Product.filter(F.col('SafetyStockLevel') > 0)
    # Product = Product.filter(F.col('ReorderPoint') > 0)
    # Product = Product.filter(F.col('StandardCost') >= 0.00)
    # Product = Product.filter(F.col('ListPrice') >= 0.00)
    # Product = Product.filter(F.col('DaysToManufacture') >= 0)
    # Product = Product.filter((F.upper(F.col('Class')).isin('H', 'M', 'L')) | F.col('Class').isNull())
    # Product = Product.filter((F.upper(F.col('ProductLine')).isin('R', 'M', 'T', 'S')) | F.col('ProductLine').isNull())
    # Product = Product.filter((F.upper(F.col('Style')).isin('U', 'M', 'W')) | F.col('Style').isNull())
    # Product = Product.filter((F.col('Weight') > 0.00) | F.col('Weight').isNull())
    # Product = Product.filter((F.col('SellEndDate') >= F.col('SellStartDate')) | F.col('SellEndDate').isNull())
    
    # Define a função de janela para deduplicar com base nas chaves primárias
    window_spec = Window.partitionBy('ProductID').orderBy(F.col('ModifiedDate').desc())
    Product = Product.withColumn('row_num', F.row_number().over(window_spec))

    # Filtra para manter apenas a primeira linha em cada partição (sem duplicatas)
    Product = Product.filter(F.col('row_num') == 1).drop('row_num')

    # Seleção final com CAST explícito dos tipos de dados
    Product = Product.select(
        F.col('ProductID').cast(IntegerType()).alias('ProductID'),
        F.col('Name').cast(StringType()).alias('Name'),
        F.col('ProductNumber').cast(StringType()).alias('ProductNumber'),
        F.col('MakeFlag').cast(BooleanType()).alias('MakeFlag'),
        F.col('FinishedGoodsFlag').cast(BooleanType()).alias('FinishedGoodsFlag'),
        F.col('Color').cast(StringType()).alias('Color'),
        F.col('SafetyStockLevel').cast(ShortType()).alias('SafetyStockLevel'),
        F.col('ReorderPoint').cast(ShortType()).alias('ReorderPoint'),
        F.col('StandardCost').cast(DecimalType(19, 4)).alias('StandardCost'),
        F.col('ListPrice').cast(DecimalType(19, 4)).alias('ListPrice'),
        F.col('Size').cast(StringType()).alias('Size'),
        F.col('SizeUnitMeasureCode').cast(StringType()).alias('SizeUnitMeasureCode'),
        F.col('WeightUnitMeasureCode').cast(StringType()).alias('WeightUnitMeasureCode'),
        F.col('Weight').cast(DecimalType(8, 2)).alias('Weight'),
        F.col('DaysToManufacture').cast(IntegerType()).alias('DaysToManufacture'),
        F.col('ProductLine').cast(StringType()).alias('ProductLine'),
        F.col('Class').cast(StringType()).alias('Class'),
        F.col('Style').cast(StringType()).alias('Style'),
        F.col('ProductSubcategoryID').cast(IntegerType()).alias('ProductSubcategoryID'),
        F.col('ProductModelID').cast(IntegerType()).alias('ProductModelID'),
        F.col('SellStartDate').cast(TimestampType()).alias('SellStartDate'),
        F.col('SellEndDate').cast(TimestampType()).alias('SellEndDate'),
        F.col('DiscontinuedDate').cast(TimestampType()).alias('DiscontinuedDate'),
        F.col('rowguid').cast(StringType()).alias('rowguid'),
        F.col('ModifiedDate').cast(TimestampType()).alias('ModifiedDate')
    )

    return Product

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar Transformação

# COMMAND ----------

transformed_df = transform_Production_Product(Product = bronze_source_table)

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
