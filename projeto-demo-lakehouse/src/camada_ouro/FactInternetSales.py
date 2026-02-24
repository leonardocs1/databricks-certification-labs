# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, ShortType, TimestampType, StringType,
    DecimalType, StructType, StructField
)

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "FactInternetSales"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["SalesOrderID", "SalesOrderDetailID"]

# Informações da Tabela Fonte
sales_order_header_df = spark.read.table("adventure_works_silver.sales_salesorderheader")
sales_order_detail_df = spark.read.table("adventure_works_silver.sales_salesorderdetail")
product_df = spark.read.table("adventure_works_silver.production_product")
customer_df = spark.read.table("adventure_works_silver.sales_customer")
special_offer_df = spark.read.table("adventure_works_silver.sales_specialoffer")
sales_territory_df = spark.read.table("adventure_works_silver.sales_salesterritory")


# COMMAND ----------

expected_schema = StructType([
    StructField("ProductKey", IntegerType(), False),
    StructField("OrderDateKey", TimestampType(), False),
    StructField("DueDateKey", TimestampType(), False),
    StructField("ShipDateKey", TimestampType(), False),
    StructField("CustomerKey", IntegerType(), False),
    StructField("PromotionKey", IntegerType(), False),
    StructField("SalesTerritoryKey", IntegerType(), False),
    StructField("SalesOrderID", StringType(), False),
    StructField("SalesOrderDetailID", IntegerType(), False),
    StructField("RevisionNumber", ShortType(), False),
    StructField("OrderQty", IntegerType(), False),
    StructField("UnitPrice", DecimalType(19, 4), False),
    StructField("UnitPriceDiscount", DecimalType(19, 4), False),
    StructField("LineTotal", DecimalType(38, 6), False),
])

# COMMAND ----------

def transform_fact_internet_sales(
    sales_order_header_df: DataFrame,
    sales_order_detail_df: DataFrame,
    product_df: DataFrame,
    customer_df: DataFrame,
    special_offer_df: DataFrame,
    sales_territory_df: DataFrame,
) -> DataFrame:
    '''
    Transformação da tabela: FactInternetSales
    '''
    # Deduplicação das tabelas antes do JOIN
    def deduplicate(df, partition_col, order_col="ModifiedDate"):
        window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
        return df.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")
    
    sales_order_header_df = deduplicate(sales_order_header_df, "SalesOrderID").filter(F.col("OnlineOrderFlag") == 1)
    sales_order_detail_df = deduplicate(sales_order_detail_df, "SalesOrderDetailID")
    product_df = deduplicate(product_df, "ProductID")
    customer_df = deduplicate(customer_df, "CustomerID")
    special_offer_df = deduplicate(special_offer_df, "SpecialOfferID")
    sales_territory_df = deduplicate(sales_territory_df, "TerritoryID")

    # Join das tabelas
    fact_internet_sales = (
        sales_order_detail_df.alias("sod")
            .join(sales_order_header_df.alias("soh"), "SalesOrderID", "inner")
            .join(product_df.alias("p"), "ProductID", "left")
            .join(customer_df.alias("c"), "CustomerID", "left")
            .join(special_offer_df.alias("sfo"), "SpecialOfferID", "left")
            .join(sales_territory_df.alias("st"), "TerritoryID", "left")
    )

    fact_internet_sales = fact_internet_sales.select(
        F.col("p.ProductID").alias("ProductKey"),
        F.col("soh.OrderDate").alias("OrderDateKey"),
        F.col("soh.DueDate").alias("DueDateKey"),
        F.col("soh.ShipDate").alias("ShipDateKey"),
        F.col("c.CustomerID").alias("CustomerKey"),
        F.col("sfo.SpecialOfferID").alias("PromotionKey"),
        F.col("st.TerritoryID").alias("SalesTerritoryKey"),
        F.col("soh.SalesOrderID"),
        F.col("sod.SalesOrderDetailID"),
        F.col("soh.RevisionNumber"),
        F.col("sod.OrderQty"),
        F.col("sod.UnitPrice"),
        F.col("sod.UnitPriceDiscount"),
        (F.col("sod.OrderQty") * F.col("sod.UnitPrice")).alias("ExtendedAmount"),
        (F.col("sod.UnitPrice") * F.col("sod.UnitPriceDiscount")).alias("DiscountAmount"),
        ((F.col("sod.OrderQty") * F.col("sod.UnitPrice")) - (F.col("sod.UnitPrice") * F.col("sod.UnitPriceDiscount"))).alias("LineTotal")
    )

    # Aplicando checks de integridade
    fact_internet_sales = fact_internet_sales.filter((F.col('OrderQty') > 0) & (F.col('UnitPrice') > 0))

    # Passo final: garantir conformidade com o esquema definido
    fact_internet_sales = fact_internet_sales.select(
        F.col("ProductKey").cast(IntegerType()).alias("ProductKey"),
        F.col("OrderDateKey").cast(TimestampType()).alias("OrderDateKey"),
        F.col("DueDateKey").cast(TimestampType()).alias("DueDateKey"),
        F.col("ShipDateKey").cast(TimestampType()).alias("ShipDateKey"),
        F.col("CustomerKey").cast(IntegerType()).alias("CustomerKey"),
        F.col("PromotionKey").cast(IntegerType()).alias("PromotionKey"),
        F.col("SalesTerritoryKey").cast(IntegerType()).alias("SalesTerritoryKey"),
        F.col("SalesOrderID").cast(StringType()).alias("SalesOrderID"),
        F.col("SalesOrderDetailID").cast(IntegerType()).alias("SalesOrderDetailID"),
        F.col("RevisionNumber").cast(ShortType()).alias("RevisionNumber"),
        F.col("OrderQty").cast(IntegerType()).alias("OrderQty"),
        F.col("UnitPrice").cast(DecimalType(19, 4)).alias("UnitPrice"),
        F.col("UnitPriceDiscount").cast(DecimalType(19, 4)).alias("UnitPriceDiscount"),
        F.col("LineTotal").cast(DecimalType(38, 6)).alias("LineTotal")
    )

    return fact_internet_sales

# COMMAND ----------

transformed_df = transform_fact_internet_sales(
    sales_order_header_df=sales_order_header_df,
    sales_order_detail_df=sales_order_detail_df,
    product_df=product_df,
    customer_df=customer_df,
    special_offer_df=special_offer_df,
    sales_territory_df=sales_territory_df,
)

transformed_df.display()

# COMMAND ----------

# Validação do schema
def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
    current_schema = {field.name: field.dataType for field in df.schema.fields}
    expected_schema_dict = {field.name: field.dataType for field in expected_schema}
    for col, expected_type in expected_schema_dict.items():
        if col not in current_schema or current_schema[col] != expected_type:
            print(f"Erro: Coluna {col} - Esperado: {expected_type}, Encontrado: {current_schema.get(col)}")
            return False
    return True

if validate_schema(transformed_df, expected_schema):
    print("Schema validado com sucesso.")
else:
    raise Exception("Falha na validação do schema.")

# COMMAND ----------

# Escreve para a tabela destino
transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
