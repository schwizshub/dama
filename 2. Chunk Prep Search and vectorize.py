# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # DAMA-DMBOK : chunking automatique avec `ai_prep_search` + Vector Search
# MAGIC Remplace le chunking manuel (pandas) du notebook 02 par la fonction SQL **`ai_prep_search`** (Beta).
# MAGIC
# MAGIC Prérequis :
# MAGIC - Preview **ai_prep_search** activée par un admin (Settings → Previews)
# MAGIC - Serverless environnement **v3+** ou DBR **18.2+**
# MAGIC - Table `dama.documents.dmbok` produite par `ai_parse_document` (étape 1)
# MAGIC
# MAGIC Flux : `dmbok` (VARIANT parsé) → `ai_prep_search` → chunks (Delta + CDF) → index Delta Sync (embeddings managés)

# COMMAND ----------

# MAGIC %pip install -U databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

CATALOG, SCHEMA = "dama", "documents"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.dmbok"
PREP_TABLE   = f"{CATALOG}.{SCHEMA}.dmbok_prep"          # sortie brute d'ai_prep_search (VARIANT)
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.dmbok_chunks_auto"
INDEX_NAME   = f"{CATALOG}.{SCHEMA}.dmbok_chunks_auto_index"
VS_ENDPOINT  = "dama_vs_endpoint"
EMBEDDING_EP = "databricks-gte-large-en"

# COMMAND ----------

# MAGIC %md ## 1. Chunking automatique
# MAGIC `ai_prep_search` prend directement le VARIANT d'`ai_parse_document` et renvoie des chunks avec :
# MAGIC - `chunk_to_retrieve` : texte à afficher / passer au LLM
# MAGIC - `chunk_to_embed` : texte **enrichi** (résumé du document, métadonnées, résumés de tableaux, questions associées) à vectoriser
# MAGIC - `pages[]` : pages sources de chaque chunk

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {PREP_TABLE} AS
  SELECT path,
         modificationTime,
         ai_prep_search(parsed_content) AS prepped
  FROM {SOURCE_TABLE}
  WHERE parsed_content:error_status IS NULL
""")

# Contrôle d'erreurs
display(spark.sql(f"""
  SELECT path,
         prepped:error_status AS error_status,
         size(cast(prepped:document:contents AS ARRAY<VARIANT>)) AS nb_chunks
  FROM {PREP_TABLE}
"""))

# COMMAND ----------

# MAGIC %md ## 2. Aplatir les chunks

# COMMAND ----------

chunks_df = spark.sql(f"""
  SELECT
    concat(substr(md5(p.path), 1, 12), '-', c.value:chunk_id::string)          AS chunk_id,   -- unique multi-documents
    element_at(split(p.path, '/'), -1)                                          AS file_name,
    c.value:chunk_position::int                                                 AS chunk_position,
    array_min(transform(cast(c.value:pages AS ARRAY<VARIANT>), x -> x:page_id::int)) + 1 AS page_start,  -- page_id 0-based
    array_max(transform(cast(c.value:pages AS ARRAY<VARIANT>), x -> x:page_id::int)) + 1 AS page_end,
    c.value:chunk_to_embed::string                                              AS chunk_to_embed,
    c.value:chunk_to_retrieve::string                                           AS chunk_text,
    to_json(c.value:metadata)                                                   AS metadata
  FROM {PREP_TABLE} p,
  LATERAL variant_explode(p.prepped:document:contents) AS c
""")
display(chunks_df.limit(20))

# COMMAND ----------

# MAGIC %md ## 3. Table source de l'index (PK + Change Data Feed)

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE} (
    chunk_id       STRING NOT NULL,
    file_name      STRING,
    chunk_position INT,
    page_start     INT,
    page_end       INT,
    chunk_to_embed STRING,
    chunk_text     STRING,
    metadata       STRING,
    CONSTRAINT dmbok_chunks_auto_pk PRIMARY KEY (chunk_id)
  )
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

chunks_df.createOrReplaceTempView("new_chunks")
spark.sql(f"""
  MERGE INTO {CHUNKS_TABLE} t
  USING new_chunks s ON t.chunk_id = s.chunk_id
  WHEN MATCHED AND t.chunk_to_embed <> s.chunk_to_embed THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
print(spark.table(CHUNKS_TABLE).count(), "chunks")

# COMMAND ----------

# MAGIC %md ## 4. Index Delta Sync : on vectorise `chunk_to_embed`, on renvoie `chunk_text`

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient(disable_notice=True)

try:
    vsc.get_endpoint(VS_ENDPOINT)
except Exception:
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")

try:
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)
    index.sync()
except Exception:
    index = vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT,
        index_name=INDEX_NAME,
        source_table_name=CHUNKS_TABLE,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",
        embedding_source_column="chunk_to_embed",
        embedding_model_endpoint_name=EMBEDDING_EP,
        columns_to_sync=["chunk_id", "file_name", "page_start", "page_end", "chunk_text"],
    )

# COMMAND ----------

res = index.similarity_search(
    query_text="What are the dimensions of data quality?",
    columns=["page_start", "chunk_text"],
    num_results=5,
    query_type="HYBRID",
)
for row in res["result"]["data_array"]:
    print(f"p.{row[0]} | score={row[-1]:.3f}\n{row[1][:300]}\n")