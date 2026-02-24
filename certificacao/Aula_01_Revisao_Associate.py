# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 🧠 Este notebook foi desenvolvido pela **@Aprender Dados** e faz parte dos nossos materiais educacionais para alunos e participantes dos treinamentos.
# MAGIC
# MAGIC 📘 O conteúdo aqui apresentado pode ser utilizado livremente para fins de estudo. Caso você tenha recebido este notebook de terceiros, saiba que ele é parte de um curso completo com vídeo-aulas, exercícios guiados e suporte da comunidade.
# MAGIC
# MAGIC 🚀 Quer aprender mais e se tornar um engenheiro de dados profissional?  
# MAGIC
# MAGIC
# MAGIC 👉 [Conheça nossos treinamentos](https://pay.kiwify.com.br/4OxeVMk)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/AprenderDados/public-files/main/public-files/ad/roadmap_databricks.jpg" alt="Aprender Dados" width="1080"/>
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 📅 Aula 01 – Revisão Associate
# MAGIC
# MAGIC Nesta aula, vamos revisar os principais conceitos da certificação Databricks Associate e iniciar nosso curso com exemplos práticos, conectando teoria com execução.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ O que vamos abordar:
# MAGIC - Revisão Spark, Delta Lake e Workflows
# MAGIC - Comandos SQL e PySpark com Delta Tables
# MAGIC - Demonstração prática do ACID
# MAGIC - Time Travel, ZORDER e Particionamento
# MAGIC - Setup para execução dos cadernos
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Dica:** Você pode executar este caderno usando um cluster Serverless gratuito no Databricks Free.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📖 Referências:
# MAGIC - [Databricks Certification Guide](https://www.databricks.com/learn/certification)
# MAGIC - [Documentação Delta Lake](https://docs.databricks.com/delta/index.html)
# MAGIC - [Spark Programming Guide](https://spark.apache.org/docs/latest/)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 🧠 Delta Lake e Transações ACID
# MAGIC
# MAGIC O Delta Lake é a tecnologia de armazenamento transacional utilizada no Databricks.
# MAGIC
# MAGIC Nesta aula, você vai:
# MAGIC - Criar e manipular tabelas Delta com SQL e PySpark
# MAGIC - Entender na prática os conceitos de ACID
# MAGIC - Explorar o Delta Log
# MAGIC - Comparar tabelas gerenciadas (managed) vs externas
# MAGIC - Ver como o `DROP` e o `DELETE` afetam os dados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.demo;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ✨ Revisão: Lakehouse + Spark + Delta Lake
# MAGIC
# MAGIC O modelo Lakehouse combina a escalabilidade dos data lakes com a confiabilidade dos data warehouses. 
# MAGIC Delta Lake é a tecnologia que habilita isso, trazendo:
# MAGIC - Transações ACID
# MAGIC - Controle de versão
# MAGIC - Performance (ZORDER, OPTIMIZE, Particionamento)
# MAGIC - Time Travel
# MAGIC
# MAGIC Spark é o motor de processamento distribuído. Suporta SQL, Python, R, Scala e Java.
# MAGIC
# MAGIC ### ⚡ Demonstração: Criando Tabela Delta
# MAGIC
# MAGIC Vamos criar uma tabela Delta fictícia com dados de vendas:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ✳️ Criando uma tabela Delta com SQL e PySpark
# MAGIC
# MAGIC Vamos criar uma tabela simples no Delta Lake com dados fictícios.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cria a tabela Delta com os dados
# MAGIC CREATE OR REPLACE TABLE workspace.demo.tabela_usuarios (
# MAGIC   nome STRING,
# MAGIC   idade INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insere os dados
# MAGIC INSERT INTO workspace.demo.tabela_usuarios (nome, idade) VALUES
# MAGIC   ('Alice', 30),
# MAGIC   ('Bob', 25),
# MAGIC   ('Carol', 40);

# COMMAND ----------

from pyspark.sql.types import IntegerType

dados = [("Alice", 30), ("Bob", 25), ("Carol", 40)]
colunas = ["nome", "idade"]

df = spark.createDataFrame(dados, colunas)
df = df.withColumn("idade", df["idade"].cast(IntegerType()))

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("workspace.demo.tabela_usuarios")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.tabela_usuarios;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🔁 Inserindo e Atualizando com SQL
# MAGIC
# MAGIC Vamos adicionar novos registros e atualizar um deles.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- INSERT
# MAGIC INSERT INTO workspace.demo.tabela_usuarios VALUES
# MAGIC   ("Daniel", 22),
# MAGIC   ("Eva", 33);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE
# MAGIC UPDATE workspace.demo.tabela_usuarios
# MAGIC SET idade = 28
# MAGIC WHERE nome = "Bob";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.tabela_usuarios;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🧪 Transações ACID em Ação
# MAGIC
# MAGIC ### Conceito:
# MAGIC ACID = **Atomicidade, Consistência, Isolamento, Durabilidade**
# MAGIC
# MAGIC Delta Lake garante:
# MAGIC - **Atomicidade**: ou tudo ou nada
# MAGIC - **Consistência**: dados válidos
# MAGIC - **Isolamento**: uma transação não interfere em outra
# MAGIC - **Durabilidade**: os dados são persistidos mesmo após falhas
# MAGIC
# MAGIC
# MAGIC Vamos simular um erro para observar o comportamento de rollback.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ⚗️ A — Atomicidade
# MAGIC
# MAGIC Uma operação é toda ou nada. Se falhar no meio, nada é aplicado.
# MAGIC

# COMMAND ----------

try:
    # Simulando erro no meio da escrita
    df_error = spark.createDataFrame([("Zoe", "idade_invalida")], ["nome", "idade"])
    df_error.write.mode("append").saveAsTable("workspace.demo.tabela_usuarios")
except Exception as e:
    print(f"Erro capturado: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A tabela continua íntegra mesmo após erro!  
# MAGIC 🔒 Isso acontece por causa das transações ACID do Delta Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ✅ C — Consistência
# MAGIC
# MAGIC Os dados respeitam o esquema e regras da tabela.
# MAGIC
# MAGIC Vamos tentar escrever um DataFrame com colunas em ordem errada.
# MAGIC

# COMMAND ----------

df_invalido = spark.createDataFrame([(32, "João")], ["nome", "idade"])
df_valido = spark.createDataFrame([(32, "João")], ["idade", "nome"])

try:
    df_invalido.write.mode("append").format("delta").saveAsTable("workspace.demo.tabela_usuarios")
    #df_valido.write.mode("append").format("delta").saveAsTable("workspace.demo.tabela_usuarios")
except Exception as e:
    print(f"🚫 Esquema inválido: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 🔒 I — Isolamento (Concorrência)
# MAGIC
# MAGIC Delta Lake garante que múltiplas transações paralelas não corrompam os dados.
# MAGIC
# MAGIC Vamos simular múltiplos `append` ao mesmo tempo.
# MAGIC

# COMMAND ----------

from threading import Thread

def insert_nome(nome):
    df = spark.createDataFrame([(nome, 99)], ["nome", "idade"])
    df.write.mode("append").format("delta").saveAsTable("workspace.demo.tabela_usuarios")

# Criar threads concorrentes
t1 = Thread(target=insert_nome, args=("Concorrente_1",))
t2 = Thread(target=insert_nome, args=("Concorrente_2",))

t1.start()
t2.start()
t1.join()
t2.join()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar se os dois inserts ocorreram corretamente
# MAGIC SELECT * FROM workspace.demo.tabela_usuarios;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 💾 D — Durabilidade
# MAGIC
# MAGIC Os dados são preservados mesmo após falhas.
# MAGIC
# MAGIC Reiniciar o cluster ou simular uma falha de sistema **não** afeta os dados persistidos no Delta Lake.
# MAGIC
# MAGIC Vamos ver o histórico de transações:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.demo.tabela_usuarios;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 📁 Entendendo o Delta Log
# MAGIC
# MAGIC Cada tabela Delta mantém um histórico de mudanças. Vamos ver isso:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 🧬 Estrutura de Arquivos no Delta Lake
# MAGIC
# MAGIC Ao salvar uma tabela Delta particionada, os dados são armazenados em **pastas por partição** e o Delta mantém um controle completo de todas as alterações em uma pasta chamada `_delta_log`.
# MAGIC
# MAGIC ### 📁 Exemplo: Estrutura da Tabela Particionada por `ano_mes`
# MAGIC
# MAGIC /Volumes/main/default/minicurso/tabela_vendas_zorder/  
# MAGIC
# MAGIC - `/Volumes/main/default/minicurso/tabela_vendas_zorder/`
# MAGIC   - `_delta_log/`
# MAGIC     - `00000000000000000000.json`
# MAGIC     - `00000000000000000001.json`
# MAGIC     - `...` *(arquivos de log que registram cada transação)*
# MAGIC   - `ano_mes=2022-01/`
# MAGIC     - `part-00000-*.snappy.parquet`
# MAGIC     - `...`
# MAGIC   - `ano_mes=2022-02/`
# MAGIC     - `part-00000-*.snappy.parquet`
# MAGIC     - `...`
# MAGIC   - `ano_mes=2023-01/`
# MAGIC     - `part-00000-*.snappy.parquet`
# MAGIC     - `...`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧱 _delta_log: Como funciona?
# MAGIC
# MAGIC A pasta `_delta_log` registra **tudo que acontece na tabela**:  
# MAGIC - Cada arquivo `.json` representa uma transação (append, update, delete, optimize…)  
# MAGIC - O Delta usa isso para garantir **ACID** e **Time Travel**  
# MAGIC - Os arquivos `.checkpoint.parquet` (não visíveis aqui) ajudam a acelerar a leitura do log  
# MAGIC
# MAGIC ### 🧠 Curiosidade
# MAGIC
# MAGIC Mesmo após um `DELETE` ou `UPDATE`, os dados ainda estão armazenados no Delta, até que você faça um `VACUUM`.  
# MAGIC Isso permite **Time Travel** e **rollback seguro**!
# MAGIC
# MAGIC 🔍 Quer ver isso na prática?  
# MAGIC Tente rodar:
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/main/default/minicurso/tabela_vendas_zorder`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.demo.tabela_usuarios;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🆚 Tabelas Managed vs External
# MAGIC
# MAGIC - **Managed**: o Databricks gerencia os arquivos (padrão do `saveAsTable`)
# MAGIC - **External**: você define o caminho do armazenamento (ideal com Unity Catalog)
# MAGIC
# MAGIC ### Criando tabela Managed

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Removendo uma linha
# MAGIC DELETE FROM workspace.demo.tabela_usuarios WHERE nome = "Alice";
# MAGIC
# MAGIC -- Drop completo da tabela
# MAGIC DROP TABLE IF EXISTS workspace.demo.tabela_usuarios;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ⚔️ Concurrency Control com Delta Lake
# MAGIC
# MAGIC Uma das vantagens do Delta Lake é permitir múltiplas gravações simultâneas com segurança.  
# MAGIC Vamos simular múltiplas inserções e garantir que a consistência seja mantida.

# COMMAND ----------

# Simulando múltiplas inserções rápidas
from time import time
from pyspark.sql.functions import lit

for i in range(10):
    df_novo = spark.createDataFrame([("User_" + str(i), 20 + i)], ["nome", "idade"])
    df_novo = df_novo.withColumn("insercao_id", lit(i))
    df_novo.write.mode("append").saveAsTable("workspace.demo.tabela_concorrencia")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.tabela_concorrencia;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_linhas, MAX(insercao_id) AS ultima_insercao
# MAGIC FROM workspace.demo.tabela_concorrencia;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🕰️ Time Travel com Delta Lake
# MAGIC
# MAGIC Podemos viajar no tempo para uma versão anterior da tabela.  
# MAGIC Use `DESCRIBE HISTORY` para descobrir versões anteriores.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.demo.tabela_concorrencia;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualizar uma versão antiga (exemplo: 0)
# MAGIC SELECT * FROM workspace.demo.tabela_concorrencia VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.tabela_concorrencia@v9

# COMMAND ----------

data = spark.table("workspace.demo.tabela_concorrencia@v9").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ⚙️ Otimizações com OPTIMIZE e ZORDER
# MAGIC
# MAGIC No Delta Lake, o comando `OPTIMIZE` compacta pequenos arquivos.  
# MAGIC Com `ZORDER`, otimizamos a leitura por colunas mais usadas em filtros.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Otimizando a tabela para reduzir arquivos pequenos
# MAGIC OPTIMIZE workspace.demo.tabela_concorrencia;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Z-Ordering pela coluna mais filtrada
# MAGIC OPTIMIZE workspace.demo.tabela_concorrencia ZORDER BY (nome);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🧱 Particionamento
# MAGIC
# MAGIC O particionamento melhora a leitura em grandes tabelas ao dividir os dados fisicamente com base em uma coluna.  
# MAGIC Isso permite que o mecanismo de execução leia apenas as partições necessárias para responder à consulta, economizando tempo e recursos.
# MAGIC
# MAGIC Vamos comparar:
# MAGIC
# MAGIC - Uma tabela sem particionamento
# MAGIC - Uma tabela com particionamento por `ano_mes`
# MAGIC
# MAGIC E verificar como os arquivos são organizados no Volume.
# MAGIC
# MAGIC ---

# COMMAND ----------

from pyspark.sql.functions import col, rand, expr, when, date_format
from datetime import datetime, timedelta
import random

# Categorias e datas variadas para gerar múltiplas partições
categorias = ["Eletrônicos", "Roupas", "Alimentos", "Livros", "Brinquedos"]
datas = [f"2023-{str(m).zfill(2)}-01" for m in range(1, 13)]  # 12 meses

# Gerando 1 milhão de registros distribuídos por categorias e meses
df = spark.range(1_000_000_000).selectExpr("id as dummy") \
    .withColumn("data_venda", expr(f"date('{random.choice(datas)}')")) \
    .withColumn("categoria", when(rand() < 0.2, categorias[0])
                             .when(rand() < 0.4, categorias[1])
                             .when(rand() < 0.6, categorias[2])
                             .when(rand() < 0.8, categorias[3])
                             .otherwise(categorias[4])) \
    .withColumn("valor", (rand() * 1000).cast("double")) \
    .withColumn("ano_mes", date_format("data_venda", "yyyy-MM")) \
    .drop("dummy")

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.demo.com_particao;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.demo.sem_particao;

# COMMAND ----------

# Caminhos para salvar
caminho_sem_particao = "/Volumes/workspace/demo/sem_particao"
caminho_com_particao = "/Volumes/workspace/demo/com_particao"

# COMMAND ----------

# Salvando SEM particionamento
df.write.format("delta").mode("overwrite").save(caminho_sem_particao)

# COMMAND ----------

# Salvando COM particionamento
df.write.format("delta").mode("overwrite").partitionBy("ano_mes").save(caminho_com_particao)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 🔍 Explorando as pastas geradas
# MAGIC
# MAGIC No Databricks, podemos usar comandos `ls` no caminho do volume para observar a diferença:

# COMMAND ----------

# Lista da tabela sem particionamento
display(dbutils.fs.ls(caminho_sem_particao))

# COMMAND ----------

# Lista da tabela com particionamento
display(dbutils.fs.ls(caminho_com_particao))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 📊 Consulta com filtro
# MAGIC
# MAGIC A consulta com filtro funciona de maneira igual, mas a leitura será muito mais eficiente na versão particionada.

# COMMAND ----------

from time import time

print("Consulta SEM particionamento")
start = time()
spark.read.format("delta").load(caminho_sem_particao).filter("ano_mes = '2023-03'").count()
print(f"Tempo: {time() - start:.2f} segundos")

print("\nConsulta COM particionamento")
start = time()
spark.read.format("delta").load(caminho_com_particao).filter("ano_mes = '2023-03'").count()
print(f"Tempo: {time() - start:.2f} segundos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Experimento com múltiplos appends (Auto Optimize desativado)
# MAGIC
# MAGIC Vamos criar uma tabela Delta externa no volume `main.default`, gerar vários appends com 1 milhão de linhas por iteração, e medir o impacto da fragmentação.
# MAGIC
# MAGIC No final, aplicaremos `OPTIMIZE` + `ZORDER BY (categoria)` e compararemos os planos com `.explain()`.
# MAGIC
# MAGIC Auto Optimize será desativado para simular fragmentação real.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Gerar dados sintéticos e criar tabela Delta externa
# MAGIC Vamos gerar 10 milhões de registros de vendas, particionados por `ano_mes`, e salvar como tabela Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.demo.tabela_vendas_zorder;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta.`/Volumes/workspace/demo/tabela_vendas_zorder` 

# COMMAND ----------

dbutils.fs.rm("/Volumes/workspace/demo/tabela_vendas_zorder", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta.`/Volumes/workspace/demo/tabela_vendas_zorder` (
# MAGIC   data_venda DATE,
# MAGIC   categoria STRING,
# MAGIC   valor DOUBLE,
# MAGIC   ano_mes STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (ano_mes)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/demo/tabela_vendas_zorder`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/demo/tabela_vendas_zorder`
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'delta.autoOptimize.autoCompact' = 'false'
# MAGIC );

# COMMAND ----------

from pyspark.sql.functions import col, rand, expr, when, floor
from datetime import datetime
import random

# Categorias e intervalo de datas
categorias = ["Eletrônicos", "Roupas", "Alimentos", "Livros", "Brinquedos"]
data_inicio = datetime.strptime("2022-01-01", "%Y-%m-%d")
data_fim = datetime.strptime("2023-12-31", "%Y-%m-%d")
dias_total = (data_fim - data_inicio).days

# Expressão condicional para categorias baseada em rand()
categoria_expr = when(rand() < 0.2, categorias[0]) \
    .when(rand() < 0.4, categorias[1]) \
    .when(rand() < 0.6, categorias[2]) \
    .when(rand() < 0.8, categorias[3]) \
    .otherwise(categorias[4])

# Loop para gerar múltiplos appends
for i in range(200):
    print(f"Append {i+1}...")

    df_append = spark.range(1_000_000).selectExpr("id as dummy") \
        .withColumn("rand_days", floor(rand() * dias_total).cast("int")) \
        .withColumn("data_venda", expr(f"date_add('{data_inicio.strftime('%Y-%m-%d')}', rand_days)")) \
        .withColumn("categoria", categoria_expr) \
        .withColumn("valor", (rand() * 1000).cast("double")) \
        .withColumn("ano_mes", expr("date_format(data_venda, 'yyyy-MM')")) \
        .drop("dummy", "rand_days")

    df_append.write.format("delta").mode("append") \
        .partitionBy("ano_mes") \
        .save("/Volumes/workspace/demo/tabela_vendas_zorder")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 🧪 Consulta antes da otimização
# MAGIC
# MAGIC Vamos consultar um valor específico (ex: `estado = 'SP'`) e usar `.explain()` para entender o plano de execução atual.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta buscando uma categoria específica (sem otimização ainda)
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/demo/tabela_vendas_zorder`
# MAGIC WHERE categoria = 'Eletrônicos'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXPLAIN antes da otimização
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/demo/tabela_vendas_zorder`
# MAGIC WHERE categoria = 'Eletrônicos'

# COMMAND ----------

# Medindo tempo ANTES do OPTIMIZE com agregação forçando leitura ampla
import time
from pyspark.sql.functions import * 

start = time.time()
df = spark.read.format("delta").load("/Volumes/workspace/demo/tabela_vendas_zorder")

# Agregação por categoria
resultado = df.groupBy("categoria").agg(
    count("*").alias("total_vendas"),
    round(sum("valor"), 2).alias("total_valor")
).collect()

end = time.time()

print(f"Tempo de execução (antes do OPTIMIZE): {end - start:.2f} segundos")
display(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ⚙️ Aplicando OPTIMIZE + ZORDER
# MAGIC
# MAGIC Vamos compactar os arquivos e aplicar ordenação por `estado`, que é a coluna mais usada em filtros.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicando OPTIMIZE com ZORDER BY na coluna "categoria"
# MAGIC OPTIMIZE delta.`/Volumes/workspace/demo/tabela_vendas_zorder`
# MAGIC ZORDER BY (categoria)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 🧪 Consulta após da otimização

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXPLAIN depois da otimização
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/demo/tabela_vendas_zorder`
# MAGIC WHERE categoria = 'Eletrônicos'

# COMMAND ----------

# Medindo tempo DEPOIS do OPTIMIZE com agregação forçando leitura ampla
import time
from pyspark.sql.functions import * 

start = time.time()
df = spark.read.format("delta").load("/Volumes/workspace/demo/tabela_vendas_zorder")

# Agregação por categoria
resultado = df.groupBy("categoria").agg(
    count("*").alias("total_vendas"),
    round(sum("valor"), 2).alias("total_valor")
).collect()

end = time.time()

print(f"Tempo de execução (depois do OPTIMIZE): {end - start:.2f} segundos")
display(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 🔄 DEMO End-to-End + Setup
# MAGIC
# MAGIC - Rodar bronze, silver e gold com dados do AdventureWorks
# MAGIC - Usar workflows
# MAGIC - Simular novos dados chegando
# MAGIC - Atualizar as tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 💻 Setup do Ambiente
# MAGIC
# MAGIC ### Databricks Free
# MAGIC - Crie conta: https://www.databricks.com/learn/free-edition
# MAGIC - Crie cluster: `Serverless`
# MAGIC - Importe o projeto
# MAGIC
# MAGIC
# MAGIC ### Azure
# MAGIC - Configure Unity Catalog (ver aula dedicada)
# MAGIC - Crie cluster com UC habilitado
# MAGIC - Monte os dados no DBFS ou no ADLS
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC [🔗 Link para o guia de estudos no GitHub:](https://github.com/AprenderDados/quero_aprender_dados/blob/main/Aprendendo_Databricks/guia_de_estudos_certificacao_databricks.md)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Links e Referências
# MAGIC
# MAGIC - 🔗 [Documentação Oficial do Delta Lake (Databricks)](https://docs.databricks.com/delta/index.html)
# MAGIC - 🔗 [What is Delta Lake? | Databricks](https://www.databricks.com/discover/delta-lake)
# MAGIC - 🔗 [ACID Transactions in Delta Lake](https://docs.databricks.com/delta/delta-acid.html)
# MAGIC - 🔗 [Time Travel with Delta Lake](https://docs.databricks.com/delta/delta-time-travel.html)
# MAGIC - 🔗 [Z-Ordering para otimização de leitura](https://docs.databricks.com/delta/optimize.html#z-order-by-multidimensional-clustering)
# MAGIC - 🔗 [Unity Catalog - Visão Geral](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - 🔗 [Databricks Lakehouse Architecture](https://www.databricks.com/solutions/data-lakehouse)
# MAGIC - 🔗 [Exemplos e tutoriais da Databricks Academy](https://academy.databricks.com)
# MAGIC
# MAGIC Essas referências ajudam a consolidar os conceitos apresentados na aula e trazem caminhos para aprofundamento técnico e prático sobre Delta Lake, arquitetura Lakehouse e práticas recomendadas em projetos com Databricks.

# COMMAND ----------


