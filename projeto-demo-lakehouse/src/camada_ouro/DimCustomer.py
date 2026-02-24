# Databricks notebook source
# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, StructType, StructField

# COMMAND ----------

# Informações da Tabela Destino (target)
target_table_name = "DimCustomer"
target_database = "adventure_works_gold"
target_table = f"{target_database}.{target_table_name}"

primary_keys = ["CustomerKey"]

# Informações das Tabelas Fonte
customer_df = spark.read.table("adventure_works_silver.sales_customer")
person_df = spark.read.table("adventure_works_silver.person_person")
address_df = spark.read.table("adventure_works_silver.person_address")
email_address_df = spark.read.table("adventure_works_silver.person_emailaddress")
phone_df = spark.read.table("adventure_works_silver.person_personphone")

# COMMAND ----------

expected_schema = StructType([
    StructField("CustomerKey", IntegerType(), False),
    StructField("GeographyKey", IntegerType(), False),
    StructField("CustomerAlternateKey", StringType(), False),
    StructField("Title", StringType(), True),
    StructField("FirstName", StringType(), False),
    StructField("MiddleName", StringType(), True),
    StructField("LastName", StringType(), False),
    StructField("NameStyle", IntegerType(), False),
    StructField("EmailAddress", StringType(), True),
    StructField("Phone", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função de Transformação e Deduplicação para `DimCustomer`

# COMMAND ----------

def transform_dim_customer(
    customer_df: DataFrame,
    person_df: DataFrame,
    address_df: DataFrame,
    email_address_df: DataFrame,
    phone_df: DataFrame
) -> DataFrame:
    '''
    Transformação e deduplicação da tabela: DimCustomer

    Parâmetros:
        customer_df (DataFrame): DataFrame da tabela Customer.
        person_df (DataFrame): DataFrame da tabela Person.
        address_df (DataFrame): DataFrame da tabela Address.
        email_address_df (DataFrame): DataFrame da tabela EmailAddress.
        phone_df (DataFrame): DataFrame da tabela PersonPhone.

    Retorna:
        DataFrame: O DataFrame resultante após a transformação e deduplicação.
    '''

    # Deduplicação da tabela Person
    window_spec_person = Window.partitionBy("BusinessEntityID").orderBy(F.col("ModifiedDate").desc())
    person_df = person_df.withColumn("row_num", F.row_number().over(window_spec_person))
    person_df = person_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela Address
    window_spec_address = Window.partitionBy("AddressID").orderBy(F.col("ModifiedDate").desc())
    address_df = address_df.withColumn("row_num", F.row_number().over(window_spec_address))
    address_df = address_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela EmailAddress
    window_spec_email = Window.partitionBy("BusinessEntityID").orderBy(F.col("ModifiedDate").desc())
    email_address_df = email_address_df.withColumn("row_num", F.row_number().over(window_spec_email))
    email_address_df = email_address_df.filter(F.col("row_num") == 1).drop("row_num")

    # Deduplicação da tabela Phone
    window_spec_phone = Window.partitionBy("BusinessEntityID").orderBy(F.col("ModifiedDate").desc())
    phone_df = phone_df.withColumn("row_num", F.row_number().over(window_spec_phone))
    phone_df = phone_df.filter(F.col("row_num") == 1).drop("row_num")

    # Realizando os joins após a deduplicação
    dim_customer = (
        customer_df.alias("c")
            .join(person_df.alias("p"), F.col("c.CustomerID") == F.col("p.BusinessEntityID"), "inner")
            .join(address_df.alias("a"), F.col("c.CustomerID") == F.col("a.AddressID"), "left")
            .join(email_address_df.alias("ea"), F.col("c.CustomerID") == F.col("ea.BusinessEntityID"), "left")
            .join(phone_df.alias("pp"), F.col("c.CustomerID") == F.col("pp.BusinessEntityID"), "left")
    )

    # Selecionando e renomeando colunas conforme o modelo do DW
    dim_customer = dim_customer.select(
        F.col("c.CustomerID").alias("CustomerKey"),
        F.col("a.AddressID").alias("GeographyKey"),
        F.col("c.AccountNumber").alias("CustomerAlternateKey"),
        F.col("p.Title").alias("Title"),
        F.col("p.FirstName").alias("FirstName"),
        F.col("p.MiddleName").alias("MiddleName"),
        F.col("p.LastName").alias("LastName"),
        F.col("p.NameStyle").alias("NameStyle"),
        F.col("ea.EmailAddress").alias("EmailAddress"),
        F.col("pp.PhoneNumber").alias("Phone")
    )

    return dim_customer

# COMMAND ----------

# Transformando os dados
transformed_df = transform_dim_customer(
    customer_df=customer_df,
    person_df=person_df,
    address_df=address_df,
    email_address_df=email_address_df,
    phone_df=phone_df
)

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
# MAGIC ## Escrita da Tabela `DimCustomer` no Data Warehouse

# COMMAND ----------

transformed_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(target_table)
