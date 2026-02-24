# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimGeography"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["GeographyKey"]

# Informações das Tabelas Fonte
address_df = spark.read.table("adventure_works_silver.person_address")
state_province_df = spark.read.table("adventure_works_silver.person_stateprovince")
country_region_df = spark.read.table("adventure_works_silver.person_countryregion")

# COMMAND ----------

expected_schema = StructType([
    StructField("GeographyKey", IntegerType(), False),
    StructField("City", StringType(), False),
    StructField("StateProvince", StringType(), False),
    StructField("CountryRegion", StringType(), False),
    StructField("PostalCode", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimGeography`

# COMMAND ----------

def transform_dim_geography(
    address_df: DataFrame,
    state_province_df: DataFrame,
    country_region_df: DataFrame
) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimGeography

    Parâmetros:
        address_df (DataFrame): DataFrame da tabela Address.
        state_province_df (DataFrame): DataFrame da tabela StateProvince.
        country_region_df (DataFrame): DataFrame da tabela CountryRegion.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela Address com base em AddressID e ordenada pela coluna 'ModifiedDate'
    window_spec_address = Window.partitionBy("AddressID").orderBy(F.col("ModifiedDate").desc())
    address_df = address_df.withColumn("row_num", F.row_number().over(window_spec_address))
    address_df = address_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela StateProvince
    window_spec_state = Window.partitionBy("StateProvinceID").orderBy(F.col("ModifiedDate").desc())
    state_province_df = state_province_df.withColumn("row_num", F.row_number().over(window_spec_state))
    state_province_df = state_province_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela CountryRegion
    window_spec_country = Window.partitionBy("CountryRegionCode").orderBy(F.col("ModifiedDate").desc())
    country_region_df = country_region_df.withColumn("row_num", F.row_number().over(window_spec_country))
    country_region_df = country_region_df.filter(F.col("row_num") == 1).drop("row_num")

    # Join das tabelas
    dim_geography = (
        address_df.alias("a")
            .join(state_province_df.alias("sp"), F.col("a.StateProvinceID") == F.col("sp.StateProvinceID"), "inner")
            .join(country_region_df.alias("cr"), F.col("sp.CountryRegionCode") == F.col("cr.CountryRegionCode"), "inner")
    )

    # Selecionando colunas
    dim_geography = dim_geography.select(
        F.col("a.AddressID").alias("GeographyKey"),
        F.col("a.City").alias("City"),
        F.col("sp.Name").alias("StateProvince"),
        F.col("cr.Name").alias("CountryRegion"),
        F.col("a.PostalCode").alias("PostalCode")
    )

    return dim_geography

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_geography(
    address_df=address_df,
    state_province_df=state_province_df,
    country_region_df=country_region_df
)

# Verificação da contagem e do schema
transformed_df.count()
transformed_df.printSchema()

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
# MAGIC ## Escrita da Tabela

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)

# COMMAND ----------


