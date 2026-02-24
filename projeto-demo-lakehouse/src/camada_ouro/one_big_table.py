# Databricks notebook source
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, TimestampType, DecimalType, StructType, StructField
)

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "OneBigTable"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"


# Informações das Tabelas Fonte
sales_order_header_df = spark.read.table("adventure_works_silver.sales_salesorderheader")
sales_order_detail_df = spark.read.table("adventure_works_silver.sales_salesorderdetail")
product_df = spark.read.table("adventure_works_silver.production_product")
product_subcategory_df = spark.read.table("adventure_works_silver.production_productsubcategory")
product_category_df = spark.read.table("adventure_works_silver.production_productcategory")
sales_territory_df = spark.read.table("adventure_works_silver.sales_salesterritory")
customer_df = spark.read.table("adventure_works_silver.person_person")

# COMMAND ----------

expected_schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),
    StructField("SalesOrderDetailID", IntegerType(), False),
    StructField("OrderDate", TimestampType(), False),
    StructField("DueDate", TimestampType(), False),
    StructField("SubTotal", DecimalType(19, 4), False),
    StructField("TaxAmt", DecimalType(19, 4), False),
    StructField("Freight", DecimalType(19, 4), False),
    StructField("TotalDue", DecimalType(19, 4), False),
    StructField("LineTotal", DecimalType(19, 4), False),
    StructField("OrderQty", IntegerType(), False),
    StructField("UnitPrice", DecimalType(19, 4), False),
    StructField("UnitPriceDiscount", DecimalType(19, 4), False),
    StructField("ProductName", StringType(), False),
    StructField("ProductNumber", StringType(), False),
    StructField("ProductCategoryName", StringType(), False),
    StructField("ProductSubcategoryName", StringType(), False),
    StructField("TerritoryName", StringType(), False),
    StructField("CustomerName", StringType(), False)
])

# COMMAND ----------

# Deduplicação das tabelas antes do JOIN
def deduplicate(df, partition_col, order_col="ModifiedDate"):
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    return df.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")


def transform_one_big_table(
    sales_order_header_df: DataFrame,
    sales_order_detail_df: DataFrame,
    product_df: DataFrame,
    product_subcategory_df: DataFrame,
    product_category_df: DataFrame,
    sales_territory_df: DataFrame,
    customer_df: DataFrame
) -> DataFrame:
    '''
    Transformação da tabela: OneBigTable
    '''

    sales_order_header_df = deduplicate(sales_order_header_df, "SalesOrderID").filter(F.col("OnlineOrderFlag") == 1)
    sales_order_detail_df = deduplicate(sales_order_detail_df, "SalesOrderDetailID")
    product_df = deduplicate(product_df, "ProductID")
    product_subcategory_df = deduplicate(product_subcategory_df, "ProductSubcategoryID")
    product_category_df = deduplicate(product_category_df, "ProductCategoryID")
    sales_territory_df = deduplicate(sales_territory_df, "TerritoryID")
    customer_df = deduplicate(customer_df, "BusinessEntityID")

    # Join das tabelas
    one_big_table = (
        sales_order_detail_df.alias("sod")
            .join(sales_order_header_df.alias("soh"), "SalesOrderID", "inner")
            .join(product_df.alias("p"), F.col("sod.ProductID") == F.col("p.ProductID"), "left")
            .join(product_subcategory_df.alias("psc"), F.col("p.ProductSubcategoryID") == F.col("psc.ProductSubcategoryID"), "left")
            .join(product_category_df.alias("pc"), F.col("psc.ProductCategoryID") == F.col("pc.ProductCategoryID"), "left")
            .join(sales_territory_df.alias("st"), F.col("soh.TerritoryID") == F.col("st.TerritoryID"), "left")
            .join(customer_df.alias("c"), F.col("soh.CustomerID") == F.col("c.BusinessEntityID"), "left")
    )

    one_big_table = one_big_table.select(
        F.col("soh.SalesOrderID"),
        F.col("sod.SalesOrderDetailID"),
        F.col("soh.OrderDate"),
        F.col("soh.DueDate"),
        F.col("soh.SubTotal"),
        F.col("soh.TaxAmt"),
        F.col("soh.Freight"),
        F.col("soh.TotalDue"),
        F.col("sod.LineTotal"),
        F.col("sod.OrderQty").cast("integer").alias("OrderQty"),
        F.col("sod.UnitPrice"),
        F.col("sod.UnitPriceDiscount"),
        F.col("p.Name").alias("ProductName"),
        F.col("p.ProductNumber"),
        F.col("pc.Name").alias("ProductCategoryName"),
        F.col("psc.Name").alias("ProductSubcategoryName"),
        F.col("st.Name").alias("TerritoryName"),
        F.concat(F.col("c.FirstName"), F.lit(" "), F.col("c.LastName")).alias("CustomerName")
    )

    return one_big_table

# COMMAND ----------

transformed_df = transform_one_big_table(
    sales_order_header_df=sales_order_header_df,
    sales_order_detail_df=sales_order_detail_df,
    product_df=product_df,
    product_subcategory_df=product_subcategory_df,
    product_category_df=product_category_df,
    sales_territory_df=sales_territory_df,
    customer_df=customer_df
)

display(transformed_df)

# COMMAND ----------

# Escreve para a tabela destino
transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
