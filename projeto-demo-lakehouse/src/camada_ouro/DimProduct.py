# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimProduct"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["ProductKey"]

# Informações das Tabelas Fonte
product_df = spark.read.table("adventure_works_silver.production_product")
subcategory_df = spark.read.table("adventure_works_silver.production_productsubcategory")
category_df = spark.read.table("adventure_works_silver.production_productcategory")
model_df = spark.read.table("adventure_works_silver.production_productmodel")

# COMMAND ----------

expected_schema = StructType([
    StructField("ProductKey", IntegerType(), False),
    StructField("ProductAlternateKey", StringType(), False),
    StructField("ProductName", StringType(), False),
    StructField("ModelName", StringType(), False),
    StructField("SubcategoryName", StringType(), False),
    StructField("CategoryName", StringType(), False),
    StructField("ProductSubcategoryKey", IntegerType(), False),
    StructField("ProductCategoryKey", IntegerType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimProduct`

# COMMAND ----------

def transform_dim_product(
    product_df: DataFrame,
    category_df: DataFrame,
    model_df: DataFrame,
    subcategory_df: DataFrame
) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimProduct

    Parâmetros:
        product_df (DataFrame): DataFrame da tabela Product.
        subcategory_df (DataFrame): DataFrame da tabela ProductSubcategory.
        category_df (DataFrame): DataFrame da tabela Productcategory.
        model_df (DataFrame): DataFrame da tabela ProductModel.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela Product com base em ProductID e ordenada pela coluna 'ModifiedDate'
    window_spec_product = Window.partitionBy("ProductID").orderBy(F.col("ModifiedDate").desc())
    product_df = product_df.withColumn("row_num", F.row_number().over(window_spec_product))
    product_df = product_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela ProductSubcategory
    window_spec_subcategory = Window.partitionBy("ProductSubcategoryID").orderBy(F.col("ModifiedDate").desc())
    subcategory_df = subcategory_df.withColumn("row_num", F.row_number().over(window_spec_subcategory))
    subcategory_df = subcategory_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela ProductModel
    window_spec_model = Window.partitionBy("ProductModelID").orderBy(F.col("ModifiedDate").desc())
    model_df = model_df.withColumn("row_num", F.row_number().over(window_spec_model))
    model_df = model_df.filter(F.col("row_num") == 1).drop("row_num")

    # Join das tabelas
    dim_product = (
        product_df.alias("p")
            .join(subcategory_df.alias("ps"), F.col("p.ProductSubcategoryID") == F.col("ps.ProductSubcategoryID"), "left")
            .join(category_df.alias("pc"), F.col("ps.ProductCategoryID") == F.col("pc.ProductCategoryID"), "left")
            .join(model_df.alias("pm"), F.col("p.ProductModelID") == F.col("pm.ProductModelID"), "left")
    )

    # Selecionando colunas
    dim_product = dim_product.select(
        F.col("p.ProductID").alias("ProductKey"),
        F.col("p.ProductNumber").alias("ProductAlternateKey"),
        F.col("p.Name").alias("ProductName"),
        F.col("pm.Name").alias("ModelName"),
        F.col("ps.Name").alias("SubcategoryName"),
        F.col("pc.Name").alias("CategoryName"),
        F.col("ps.ProductSubcategoryID").alias("ProductSubcategoryKey"),
        F.col("ps.ProductCategoryID").alias("ProductCategoryKey"),
       
    )

    return dim_product

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_product(
    product_df=product_df,
    subcategory_df=subcategory_df,
    category_df=category_df,
    model_df=model_df
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
# MAGIC ## Escrita da Tabela `DimProduct` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)

# COMMAND ----------


