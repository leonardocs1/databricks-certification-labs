# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# %sql
# DROP DATABASE adventure_works_bronze CASCADE;
# DROP DATABASE adventure_works_silver CASCADE;
# DROP DATABASE adventure_works_gold CASCADE;

# COMMAND ----------

# MAGIC %run "./camada_bronze/bronze_from_github"

# COMMAND ----------

# MAGIC %run "./camada_prata/0_run_all_silver"

# COMMAND ----------

# MAGIC %run "./camada_ouro/0_run_all_ouro"

# COMMAND ----------


