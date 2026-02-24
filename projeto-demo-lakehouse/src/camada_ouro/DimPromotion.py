# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimPromotion"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["PromotionKey"]

# Informações da Tabela Fonte
special_offer_df = spark.read.table("adventure_works_silver.sales_specialoffer")

# COMMAND ----------

expected_schema = StructType([
    StructField("PromotionKey", IntegerType(), False),
    StructField("PromotionAlternateKey", IntegerType(), False),
    StructField("EnglishPromotionName", StringType(), False),
    StructField("DiscountPct", DecimalType(10,4), False),
    StructField("EnglishPromotionType", StringType(), False),
    StructField("StartDate", TimestampType(), False),
    StructField("EndDate", TimestampType(), False),
    StructField("MinQty", IntegerType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimPromotion`

# COMMAND ----------

def transform_dim_promotion(special_offer_df: DataFrame) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimPromotion

    Parâmetros:
        special_offer_df (DataFrame): DataFrame da tabela SpecialOffer.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela SpecialOffer com base em SpecialOfferID e ordenada pela coluna 'ModifiedDate'
    window_spec = Window.partitionBy("SpecialOfferID").orderBy(F.col("ModifiedDate").desc())
    special_offer_df = special_offer_df.withColumn("row_num", F.row_number().over(window_spec))
    special_offer_df = special_offer_df.filter(F.col("row_num") == 1).drop("row_num")

    # Selecionando e renomeando colunas conforme o modelo do DW
    dim_promotion = special_offer_df.select(
        F.col("SpecialOfferID").alias("PromotionKey"),
        F.col("SpecialOfferID").alias("PromotionAlternateKey"),
        F.col("Description").alias("EnglishPromotionName"),
        F.col("DiscountPct").alias("DiscountPct"),
        F.col("Type").alias("EnglishPromotionType"),
        F.col("StartDate").alias("StartDate"),
        F.col("EndDate").alias("EndDate"),
        F.col("MinQty").alias("MinQty")
    )

    return dim_promotion

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_promotion(special_offer_df=special_offer_df)

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
# MAGIC ## Escrita da Tabela `DimPromotion` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
