# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM adventure_works_bronze.sales_salesorderdetail

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works_silver.sales_salesorderdetail

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works_gold.dimproduct

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adventure_works_gold.factinternetsales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   p.CategoryName AS Categoria,
# MAGIC   p.ProductName AS Produto,
# MAGIC   SUM(f.LineTotal) AS TotalVendas
# MAGIC FROM 
# MAGIC   adventure_works_gold.factinternetsales f
# MAGIC JOIN 
# MAGIC   adventure_works_gold.dimproduct p 
# MAGIC   ON f.ProductKey = p.ProductKey
# MAGIC WHERE 
# MAGIC   p.CategoryName IS NOT NULL
# MAGIC GROUP BY 
# MAGIC   p.CategoryName, p.ProductName
# MAGIC ORDER BY 
# MAGIC   p.CategoryName, TotalVendas DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   d.CalendarYear AS Ano,
# MAGIC   p.CategoryName AS Categoria,
# MAGIC   p.ProductName AS Produto,
# MAGIC   SUM(f.LineTotal) AS TotalVendas
# MAGIC FROM 
# MAGIC   adventure_works_gold.factinternetsales f
# MAGIC JOIN 
# MAGIC   adventure_works_gold.dimproduct p 
# MAGIC     ON f.ProductKey = p.ProductKey
# MAGIC JOIN 
# MAGIC   adventure_works_gold.dimdate d 
# MAGIC     ON f.OrderDateKey = d.DateKey
# MAGIC WHERE 
# MAGIC   p.CategoryName IS NOT NULL
# MAGIC GROUP BY 
# MAGIC   d.CalendarYear, p.CategoryName, p.ProductName
# MAGIC ORDER BY 
# MAGIC   d.CalendarYear, p.CategoryName, TotalVendas DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW adventure_works_gold.vw_internet_sales AS
# MAGIC SELECT 
# MAGIC   f.SalesOrderID,
# MAGIC   f.SalesOrderDetailID,
# MAGIC   f.OrderDateKey AS order_date,
# MAGIC   f.DueDateKey AS due_date,
# MAGIC   f.ShipDateKey AS ship_date,
# MAGIC   f.CustomerKey,
# MAGIC   f.OrderQty,
# MAGIC   f.UnitPrice,
# MAGIC   f.LineTotal,
# MAGIC
# MAGIC   p.ProductAlternateKey AS product_code,
# MAGIC   p.ProductName AS product_name,
# MAGIC   p.ModelName AS model,
# MAGIC   p.SubcategoryName AS subcategory,
# MAGIC   p.CategoryName AS category,
# MAGIC
# MAGIC   d.FullDateAlternateKey AS full_date,
# MAGIC   d.EnglishDayNameOfWeek AS day_of_week,
# MAGIC   d.EnglishMonthName AS month_name,
# MAGIC   d.CalendarYear AS year,
# MAGIC   d.CalendarQuarter AS quarter
# MAGIC
# MAGIC FROM 
# MAGIC   adventure_works_gold.factinternetsales f
# MAGIC JOIN 
# MAGIC   adventure_works_gold.dimproduct p 
# MAGIC     ON f.ProductKey = p.ProductKey
# MAGIC JOIN 
# MAGIC   adventure_works_gold.dimdate d 
# MAGIC     ON f.OrderDateKey = d.DateKey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM  adventure_works_gold.vw_internet_sales 

# COMMAND ----------


