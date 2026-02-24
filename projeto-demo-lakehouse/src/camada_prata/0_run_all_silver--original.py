# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# Definição do dicionário com as tabelas da camada prata usando nomes relativos dos notebooks
dict_tables = {
    "person_address": {
        "active": 1,
        "notebook_path": "person_address"
    },
    "person_countryregion": {
        "active": 1,
        "notebook_path": "person_countryregion"
    },
    "person_emailaddress": {
        "active": 1,
        "notebook_path": "person_emailaddress"
    },
    "person_person": {
        "active": 1,
        "notebook_path": "person_person"
    },
    "person_personphone": {
        "active": 1,
        "notebook_path": "person_personphone"
    },
    "person_stateprovince": {
        "active": 1,
        "notebook_path": "person_stateprovince"
    },
    "production_product": {
        "active": 1,
        "notebook_path": "production_product"
    },
    "production_productdescription": {
        "active": 1,
        "notebook_path": "production_productdescription"
    },
    "production_productmodel": {
        "active": 1,
        "notebook_path": "production_productmodel"
    },
    "production_productsubcategory": {
        "active": 1,
        "notebook_path": "production_productsubcategory"
    },
    "production_productcategory": {
        "active": 1,
        "notebook_path": "production_productcategory"
    },
    "sales_currency": {
        "active": 1,
        "notebook_path": "sales_currency"
    },
    "sales_customer": {
        "active": 1,
        "notebook_path": "sales_customer"
    },
    "sales_salesorderdetail": {
        "active": 1,
        "notebook_path": "sales_salesorderdetail"
    },
    "sales_salesorderheader": {
        "active": 1,
        "notebook_path": "sales_salesorderheader"
    },
     "sales_salesorderheadersalesreason": {
        "active": 1,
        "notebook_path": "sales_salesorderheadersalesreason"
    },
    "sales_salesreason": {
        "active": 1,
        "notebook_path": "sales_salesreason"
    },
    "sales_salesterritory": {
        "active": 1,
        "notebook_path": "sales_salesterritory"
    },
    "sales_specialoffer": {
        "active": 1,
        "notebook_path": "sales_specialoffer"
    }
}



# COMMAND ----------



# COMMAND ----------

def run_notebook(config):
    """Função para executar o notebook e capturar o resultado."""
    try:
        result = dbutils.notebook.run(config['notebook_path'], 0)  # timeout = 0 indica espera indefinida
        #print(f"Notebook {config['notebook_path']} executado com sucesso. Resultado: {result}")
        return result
    except Exception as e:
        print(f"Erro ao executar o notebook {config['notebook_path']}: {e}")
        return None

# COMMAND ----------

# Configura o número de threads (ajuste conforme necessário)
max_workers = 10

# Executor para gerenciar os threads
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submeter notebooks ativos para execução paralela
    futures = {executor.submit(run_notebook, config): table for table, config in dict_tables.items() if config['active'] == 1}

    # Coleta os resultados conforme cada tarefa é concluída
    for future in as_completed(futures):
        table = futures[future]
        try:
            result = future.result()
            print(f"Notebook para {table} processado com sucesso.")
        except Exception as e:
            print(f"Erro ao processar {table}: {e}")

# COMMAND ----------

# # Executando cada notebook se estiver ativo
# for table, config in dict_tables.items():
#     if config['active'] == 1:
#         print(f"Executing notebook for {table} at {config['notebook_path']}")
#         result = dbutils.notebook.run(config['notebook_path'], 0)  # timeout = 0 indica espera indefinida
#         print(f"Result for {table}: {result}")
