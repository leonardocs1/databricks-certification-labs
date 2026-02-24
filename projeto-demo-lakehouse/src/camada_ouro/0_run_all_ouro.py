# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS adventure_works_gold

# COMMAND ----------

# MAGIC %run "./DimCurrency"

# COMMAND ----------

# MAGIC %run "./DimCustomer"

# COMMAND ----------

# MAGIC %run "./DimDate"

# COMMAND ----------

# MAGIC %run "./DimGeography"

# COMMAND ----------

# MAGIC %run "./DimGeography"

# COMMAND ----------

# MAGIC %run "./DimProduct"

# COMMAND ----------

# MAGIC %run "./DimPromotion"

# COMMAND ----------

# MAGIC %run "./DimSalesReason"

# COMMAND ----------

# MAGIC %run "./DimSalesTerritory"

# COMMAND ----------

# MAGIC %run "./FactInternetSales"

# COMMAND ----------

# MAGIC %run "./ft_agg_product_day"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


