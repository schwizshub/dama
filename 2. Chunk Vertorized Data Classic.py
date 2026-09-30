# Databricks notebook source
# MAGIC %md
# MAGIC # DAMA-DMBOK : chunking + Vector Search (embeddings gérés par Databricks)
# MAGIC Prérequis : la table `dama.documents.dmbok` produite par `ai_parse_document` (étape 1).
# MAGIC Flux : `dmbok` (VARIANT) → éléments → chunks (Delta + CDF) → index Delta Sync (embeddings managés) → requêtes

# COMMAND ----------

# MAGIC %pip install -U databricks-vectorsearch

# COMMAND ----------

# MAGIC %md
# MAGIC # %restart_python

# COMMAND ----------

# DBTITLE 1,Librairies
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Variables
CATALOG, SCHEMA = "dama", "documents"
SOURCE_TABLE   = f"{CATALOG}.{SCHEMA}.dmbok"
CHUNKS_TABLE   = f"{CATALOG}.{SCHEMA}.dmbok_chunks"
INDEX_NAME     = f"{CATALOG}.{SCHEMA}.dmbok_chunks_index"
VS_ENDPOINT    = "dama_vs_endpoint"
EMBEDDING_EP   = "databricks-gte-large-en"   # modèle d'embedding Foundation Model API (pay-per-token)

MAX_CHARS = 2000   # ~500 tokens
OVERLAP   = 200
MIN_CHARS = 40


# COMMAND ----------

# DBTITLE 1,Check
display(spark.sql(f"""
  SELECT path,
         parsed_content:error_status                           AS error_status,
         size(cast(parsed_content:document:pages AS ARRAY<VARIANT>))    AS nb_pages,
         size(cast(parsed_content:document:elements AS ARRAY<VARIANT>)) AS nb_elements
  FROM {SOURCE_TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Éclater le VARIANT en éléments (texte, titres, tableaux, figures…)

# COMMAND ----------

# DBTITLE 1,Vérifier les éléements
elements_df = spark.sql(f"""
  SELECT path,
         e.pos                                  AS element_idx,
         e.value:type::string                   AS element_type,
         e.value:content::string                AS content,
         e.value:description::string            AS description,   -- description IA des figures
         e.value:bbox[0]:page_id::int           AS page_id
  FROM {SOURCE_TABLE},
  LATERAL variant_explode(parsed_content:document:elements) AS e
""")
display(elements_df.groupBy("element_type").count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Chunking par section (titre courant préfixé dans chaque chunk)

# COMMAND ----------

# DBTITLE 1,Chunk en sections
import pandas as pd

SKIP     = {"page_header", "page_footer"}
HEADINGS = {"title", "section_header"}

def chunk_doc(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("element_idx")
    path = pdf["path"].iloc[0]
    out = []
    state = {"section": "", "buf": [], "len": 0, "pages": [], "fresh": 0}

    def flush(keep_overlap=True):
        text = "\n".join(state["buf"]).strip()
        if state["fresh"] > 0 and len(text) >= MIN_CHARS:
            pages = state["pages"]
            out.append({
                "path": path,
                "chunk_seq": len(out),
                "section": state["section"],
                "page_start": min(pages) if pages else None,
                "page_end":   max(pages) if pages else None,
                "chunk_text": (f"[{state['section']}]\n" if state["section"] else "") + text,
            })
        tail = text[-OVERLAP:] if keep_overlap and text else ""
        state.update(buf=[tail] if tail else [], len=len(tail),
                     pages=state["pages"][-1:] if tail else [], fresh=0)

    for r in pdf.itertuples(index=False):
        if r.element_type in SKIP:
            continue
        content = r.content if isinstance(r.content, str) else ""
        descr = r.description if isinstance(r.description, str) else ""
        txt = (content or descr).strip()
        if not txt:
            continue
        if r.element_type in HEADINGS:          # nouvelle section → on ferme le chunk courant
            flush(keep_overlap=False)
            state["section"] = txt[:200]
            continue
        for i in range(0, len(txt), MAX_CHARS):  # découpe des éléments très longs
            piece = txt[i:i + MAX_CHARS]
            if state["len"] + len(piece) > MAX_CHARS and state["buf"]:
                flush()
            state["buf"].append(piece)
            state["len"] += len(piece) + 1
            state["fresh"] += 1
            if pd.notna(r.page_id):
                state["pages"].append(int(r.page_id) + 1)  # page_id est 0-based → n° de page lisible
    flush(keep_overlap=False)
    res = pd.DataFrame(out, columns=["path", "chunk_seq", "section", "page_start", "page_end", "chunk_text"])
    return res.astype({"chunk_seq": "int32", "page_start": "Int32", "page_end": "Int32"})

# COMMAND ----------

chunks_df = (
    elements_df.groupBy("path")
    .applyInPandas(chunk_doc, schema="path string, chunk_seq int, section string, page_start int, page_end int, chunk_text string")
    .withColumn("chunk_id", F.concat_ws("-", F.substring(F.md5("path"), 1, 12), F.lpad(F.col("chunk_seq").cast("string"), 6, "0")))
    .withColumn("file_name", F.element_at(F.split("path", "/"), -1))
)
display(chunks_df.select("chunk_id", "section", "page_start", F.length("chunk_text").alias("len"), "chunk_text").limit(20))

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table source de l'index : clé primaire + Change Data Feed (obligatoire pour Delta Sync)

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE} (
    chunk_id   STRING NOT NULL,
    path       STRING,
    file_name  STRING,
    section    STRING,
    page_start INT,
    page_end   INT,
    chunk_text STRING,
    CONSTRAINT dmbok_chunks_pk PRIMARY KEY (chunk_id)
  )
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")


# COMMAND ----------

# MERGE plutôt qu'overwrite : la synchro de l'index reste incrémentale

# COMMAND ----------

chunks_df.select("chunk_id", "path", "file_name", "section", "page_start", "page_end", "chunk_text") \
         .createOrReplaceTempView("new_chunks")

spark.sql(f"""
  MERGE INTO {CHUNKS_TABLE} t
  USING new_chunks s ON t.chunk_id = s.chunk_id
  WHEN MATCHED AND t.chunk_text <> s.chunk_text THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
print(spark.table(CHUNKS_TABLE).count(), "chunks")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Endpoint Vector Search + index Delta Sync avec embeddings managés

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient(disable_notice=True)

try:
    vsc.get_endpoint(VS_ENDPOINT)
except Exception:
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")

try:
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)
    index.sync()                                   # re-synchro après un nouveau MERGE
except Exception:
    index = vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT,
        index_name=INDEX_NAME,
        source_table_name=CHUNKS_TABLE,
        primary_key="chunk_id",
        pipeline_type="TRIGGERED",                 # ou "CONTINUOUS"
        embedding_source_column="chunk_text",      # Databricks calcule les embeddings
        embedding_model_endpoint_name=EMBEDDING_EP,
        columns_to_sync=["chunk_id", "file_name", "section", "page_start", "page_end", "chunk_text"],
    )



# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test de recherche (hybride = vecteur + mots-clés)

# COMMAND ----------

res = index.similarity_search(
    query_text="What are the dimensions of data quality?",
    columns=["section", "page_start", "chunk_text"],
    num_results=5,
    query_type="HYBRID",
)
for row in res["result"]["data_array"]:
    print(f"p.{row[1]} | {row[0]} | score={row[-1]:.3f}\n{row[2][:300]}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. RAG 100 % SQL (vector_search + ai_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ctx AS 
# MAGIC (
# MAGIC     SELECT concat_ws('\n---\n', collect_list(concat('[p.', page_start, ' | ', section, '] ', chunk_text))) AS context
# MAGIC     FROM vector_search(index => 'dama.documents.dmbok_chunks_index',
# MAGIC                       query_text => 'What is the difference between data governance and data management?',
# MAGIC                       num_results => 6)
# MAGIC )
# MAGIC    SELECT ai_query
# MAGIC     (
# MAGIC        'databricks-claude-sonnet-4-5',
# MAGIC        concat('Réponds en français uniquement à partir du contexte DAMA-DMBOK ci-dessous, en citant les pages.\n\nCONTEXTE:\n', context, '\n\nQUESTION: What is the difference between data governance and data management?')
# MAGIC     ) AS answer
# MAGIC FROM ctx 