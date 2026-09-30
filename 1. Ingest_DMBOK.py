# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Ingérer le document DMBOK 
# MAGIC ### 2. Décomposition en morceau verctoriel par l'automate prévu 

# COMMAND ----------

import pyspark.sql.functions as F 

# COMMAND ----------

# 1. Lire les fichiers PDF au format binaire depuis le volume
volume_path = "/Volumes/dama/documents/incoming/"
raw_df = spark.read.format("binaryFile").load(volume_path)

# 2. Appliquer la fonction d'extraction IA
parsed_df = raw_df.withColumn("parsed_content", F.expr("ai_parse_document(content)"))

# 3. Sauvegarder le résultat dans une table Delta managée
target_table = "dama.documents.dmbok"
parsed_df.select("path", "modificationTime", "parsed_content") \
         .write.mode("overwrite") \
         .saveAsTable(target_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dama.documents.dmbok