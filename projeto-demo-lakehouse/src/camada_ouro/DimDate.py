# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimDate"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["DateKey"]

# Informações da Tabela Fonte
sales_order_header_df = spark.read.table("adventure_works_silver.sales_salesorderheader")

# COMMAND ----------

expected_schema = StructType([
    StructField("DateKey", TimestampType(), False),
    StructField("FullDateAlternateKey", TimestampType(), False),
    StructField("DayNumberOfWeek", IntegerType(), False),
    StructField("EnglishDayNameOfWeek", StringType(), False),
    StructField("DayNumberOfMonth", IntegerType(), False),
    StructField("DayNumberOfYear", IntegerType(), False),
    StructField("WeekNumberOfYear", IntegerType(), False),
    StructField("EnglishMonthName", StringType(), False),
    StructField("MonthNumberOfYear", IntegerType(), False),
    StructField("CalendarQuarter", IntegerType(), False),
    StructField("CalendarYear", IntegerType(), False),
    StructField("CalendarSemester", IntegerType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação para `DimDate`

# COMMAND ----------

def transform_dim_date(sales_order_header_df: DataFrame) -> DataFrame:
    '''
    Transformação para criar a tabela DimDate.

    Parâmetros:
        sales_order_header_df (DataFrame): DataFrame da tabela SalesOrderHeader.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação.
    '''

    # Selecionando e transformando a coluna `OrderDate`
    dim_date = sales_order_header_df.select(
        F.col("OrderDate").alias("DateKey"),
        F.col("OrderDate").alias("FullDateAlternateKey"),
        ((F.dayofweek(F.col("OrderDate")) + 5) % 7 + 1).alias("DayNumberOfWeek"),  # Número do dia da semana ajustado para iniciar na segunda-feira
        F.date_format(F.col("OrderDate"), "EEEE").alias("EnglishDayNameOfWeek"),    # Nome do dia da semana
        F.dayofmonth(F.col("OrderDate")).alias("DayNumberOfMonth"),                 # Número do dia do mês
        F.dayofyear(F.col("OrderDate")).alias("DayNumberOfYear"),                   # Número do dia do ano
        F.weekofyear(F.col("OrderDate")).alias("WeekNumberOfYear"),                 # Número da semana do ano
        F.date_format(F.col("OrderDate"), "MMMM").alias("EnglishMonthName"),        # Nome do mês
        F.month(F.col("OrderDate")).alias("MonthNumberOfYear"),                     # Número do mês do ano
        F.quarter(F.col("OrderDate")).alias("CalendarQuarter"),                     # Trimestre
        F.year(F.col("OrderDate")).alias("CalendarYear"),                           # Ano
        (F.when(F.month(F.col("OrderDate")).between(1, 6), 1).otherwise(2)).alias("CalendarSemester")  # Semestre
    ).distinct()

    return dim_date


# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_date(sales_order_header_df=sales_order_header_df)

# Verificação da contagem e do schema
transformed_df.count()  # Verifica o número de linhas
transformed_df.printSchema()  # Verifica o schema do DataFrame

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
# MAGIC ## Escrita da Tabela `DimDate` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)

# COMMAND ----------


