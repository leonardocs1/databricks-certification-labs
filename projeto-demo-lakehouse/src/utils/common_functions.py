# Databricks notebook source
print("carregando funções")

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField,
        IntegerType, StringType, DoubleType, DecimalType, TimestampType, ShortType)


# COMMAND ----------



def _validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
    """
    Valida se o schema do DataFrame corresponde ao schema esperado.

    Parâmetros:
        df (DataFrame): O DataFrame a ser validado.
        expected_schema (StructType): O schema esperado.

    Retorna:
        bool: True se o schema corresponder, False caso contrário.
    """
    actual_schema = df.schema

    # Verifica se o número de campos corresponde
    if len(expected_schema.fields) != len(actual_schema.fields):
        return False

    # Verifica cada campo e tipo de dado
    for i, field in enumerate(actual_schema.fields):
        expected_field = expected_schema.fields[i]
        if field.name != expected_field.name or not isinstance(field.dataType, type(expected_field.dataType)):
            print(f"Discrepância encontrada na coluna: {field.name}")
            print(f"Esperado: {expected_field}, Encontrado: {field}")
            return False

    return True

print("Carregada a Função: _validate_schema(df: DataFrame, expected_schema: StructType) ")



# COMMAND ----------

# from delta.tables import DeltaTable
# from pyspark.sql import DataFrame, functions as F
# from pyspark.sql.utils import AnalysisException

# def _upsert_silver_table(
#     transformed_df: DataFrame, 
#     target_table: str, 
#     primary_keys: list, 
#     not_matched_by_source_action: str = None, 
#     not_matched_by_source_condition: str = None
# ) -> None:
#     """
#     Realiza o upsert (update e insert) na tabela Delta da camada prata,
#     suportando a evolução do esquema e construindo dinamicamente a condição de merge.

#     Melhorias:
#     - Verifica se a tabela existe e se é uma tabela Delta antes de criar
#     - Trata o caso onde a pasta existe, mas não é Delta
#     - Adiciona logging para ajudar no troubleshooting

#     Parâmetros:
#         transformed_df (DataFrame): DataFrame contendo os dados transformados para inserção na camada prata.
#         target_table (str): Nome da tabela de destino (ex: "adventure_works_silver.person_address").
#         primary_keys (list): Lista de chaves primárias para o merge.
#         not_matched_by_source_action (str, opcional): Ação para linhas sem correspondência na origem ("DELETE" ou "UPDATE").
#         not_matched_by_source_condition (str, opcional): Condição adicional para aplicar a ação definida em not_matched_by_source_action.
#     """

#     try:
#         # Verificar se a tabela existe no catálogo do Databricks
#         if not spark.catalog.tableExists(target_table):
#             print(f"A tabela {target_table} não existe no catálogo. Verificando o diretório de armazenamento...")

#             # Obter o caminho físico da tabela no DBFS
#             table_location = f"dbfs:/user/hive/warehouse/{target_table.replace('.', '/')}"
            
#             # Verificar se a pasta existe
#             if not dbutils.fs.ls(table_location):
#                 print(f"Diretório {table_location} não existe. Criando a tabela...")
#                 transformed_df.write.format("delta").saveAsTable(target_table)
#                 print(f"Tabela {target_table} criada com sucesso.")
#                 return
#             else:
#                 print(f"O diretório {table_location} já existe. Verificando se é uma tabela Delta...")

#                 try:
#                     # Tentar carregar a tabela Delta
#                     DeltaTable.forPath(spark, table_location)
#                     print(f"O diretório {table_location} contém uma tabela Delta válida. Registrando no catálogo...")
                    
#                     # Criar uma nova entrada no catálogo apontando para essa pasta
#                     spark.sql(f"CREATE TABLE {target_table} USING DELTA LOCATION '{table_location}'")
#                     print(f"Tabela {target_table} registrada no catálogo com sucesso.")
                
#                 except:
#                     print(f"ERRO: O diretório {table_location} contém arquivos, mas não são uma tabela Delta.")
#                     raise Exception(f"Erro ao criar a tabela {target_table}: O diretório contém arquivos inválidos.")

#         # Construir a condição de merge com base nas chaves primárias
#         merge_condition = " AND ".join([f"s.{key} = t.{key}" for key in primary_keys])

#         # Carregar a tabela Delta existente
#         delta_table = DeltaTable.forName(spark, target_table)

#         # Construir a operação de merge
#         merge_builder = delta_table.alias("t").merge(
#             transformed_df.alias("s"),
#             merge_condition
#         ).whenMatchedUpdateAll().whenNotMatchedInsertAll()

#         # Se for necessário excluir registros não encontrados na origem
#         if not_matched_by_source_action and not_matched_by_source_action.upper() == "DELETE":
#             unmatched_rows = delta_table.toDF().alias("t").join(
#                 transformed_df.alias("s"),
#                 on=[F.col(f"t.{key}") == F.col(f"s.{key}") for key in primary_keys],
#                 how="left_anti"
#             )

#             if not_matched_by_source_condition:
#                 unmatched_rows = unmatched_rows.filter(not_matched_by_source_condition)

#                 unmatched_rows.alias("s"),
#                 merge_condition
#             ).whenMatchedDelete().execute()

#         # Executar o merge
#         merge_builder.execute()
        
#         print(f"Upsert executado com sucesso para {target_table}.")
    
#     except AnalysisException as e:
#         print(f"Erro de análise ao processar {target_table}: {str(e)}")
#         raise
#     except Exception as e:
#         print(f"Erro inesperado ao processar {target_table}: {str(e)}")
#         raise





# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.utils import AnalysisException

def _upsert_silver_table(
    transformed_df: DataFrame, 
    target_table: str, 
    primary_keys: list, 
    not_matched_by_source_action: str = None, 
    not_matched_by_source_condition: str = None
) -> None:

    print(f"🔄 Iniciando upsert para {target_table}...")

    # Caminho físico no DBFS
    db_location = "dbfs:/user/hive/warehouse/adventure_works_silver.db"
    table_name = target_table.split('.')[-1]  # Nome da tabela sem o schema
    table_location = f"{db_location}/{table_name.lower()}"  # Evita problemas de case-sensitive
    
    try:
        # Verificar se a tabela existe no catálogo
        if not spark.catalog.tableExists(target_table):
            print(f"⚠️ Tabela {target_table} não existe no catálogo. Verificando diretório {table_location}...")

            try:
                # Verificar se o diretório já existe no DBFS
                if dbutils.fs.ls(table_location):
                    print(f"📂 O diretório {table_location} existe. Verificando se é uma tabela Delta...")

                    try:
                        DeltaTable.forPath(spark, table_location)
                        print(f"✅ O diretório contém uma tabela Delta válida. Registrando no catálogo...")
                        spark.sql(f"CREATE TABLE {target_table} USING DELTA LOCATION '{table_location}'")
                        print(f"✅ Tabela {target_table} registrada no catálogo.")
                    except:
                        print(f"🚨 ERRO: O diretório {table_location} contém arquivos inválidos! Removendo a pasta...")
                        dbutils.fs.rm(table_location, recurse=True)
                        print(f"🗑️ Diretório {table_location} removido. Criando a tabela do zero...")
                        transformed_df.write.format("delta").saveAsTable(target_table)
                        print(f"🎉 Tabela {target_table} criada com sucesso.")
                        return
                else:
                    print(f"📂 Diretório {table_location} não encontrado. Criando a tabela do zero...")
                    transformed_df.write.format("delta").saveAsTable(target_table)
                    print(f"🎉 Tabela {target_table} criada com sucesso.")
                    return
            except:
                print(f"📂 O diretório {table_location} não existe. Criando a tabela...")
                transformed_df.write.format("delta").saveAsTable(target_table)
                print(f"🎉 Tabela {target_table} criada com sucesso.")
                return

        else:
            print(f"✅ A tabela {target_table} já existe. Usando OVERWRITE para substituir...")

            # Sobrescreve a tabela existente
            transformed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            print(f"🎉 Tabela {target_table} substituída com sucesso.")

    except AnalysisException as e:
        print(f"🚨 Erro ao processar {target_table}: {str(e)}")

        if "TABLE_OR_VIEW_ALREADY_EXISTS" in str(e) or "not a Delta table" in str(e):
            print(f"🗑️ Deletando diretório {table_location} e recriando a tabela...")

            # Deletar a pasta e recriar a tabela do zero
            dbutils.fs.rm(table_location, recurse=True)
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")

            transformed_df.write.format("delta").saveAsTable(target_table)
            print(f"🎉 Tabela {target_table} recriada com sucesso.")

        else:
            raise  # Levanta o erro se não for problema de tabela já existente ou arquivos inválidos

    except Exception as e:
        print(f"🚨 Erro inesperado ao processar {target_table}: {str(e)}")
        raise


# COMMAND ----------

print("Função _upsert_silver_table carregada com melhorias para tratamento de erros.")

# COMMAND ----------


