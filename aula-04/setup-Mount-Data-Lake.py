# Databricks notebook source
# storage_account_name = "aulabricks"

# client_id            = dbutils.secrets.get(scope="databricks-adls-scope", key="databricks-app-client-id")
# tenant_id            = dbutils.secrets.get(scope="databricks-adls-scope", key="databricks-app-tenant-id")
# client_secret        = dbutils.secrets.get(scope="databricks-adls-scope", key="databricks-app-client-secret")

# COMMAND ----------

storage_account_name = "adlsaprenderdados"


client_id            = ""
tenant_id            = ""
client_secret        = ""

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# dbtuils.fs.mount( source = "", mount_point = 'mnt/caminho...', extra_configs = {})

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls('bronze')

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'mnt/adlsaprenderdados/landing-zone/novo-formula1'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'mnt/adlsaprenderdados/landing-zone/novo-formula1'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE TESTE
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE TESTE.Teste1 (a int)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE mentoria_formula1_bronze
# MAGIC LOCATION '/mnt/adlsaprenderdados/bronze/mentoria_formula1_bronze'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE mentoria_formula1_bronze.Teste1 (a int)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE mentoria_formula1_bronze.circuits AS
# MAGIC
# MAGIC SELECT * FROM formula1.circuits
# MAGIC
# MAGIC WHERE circuitId < 40

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mentoria_formula1_bronze.circuits
