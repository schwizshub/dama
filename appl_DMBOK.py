# Databricks notebook source
# MAGIC %pip install -r /Workspace/Users/schwizs@gmail.com/dama/requirements.txt

# COMMAND ----------

# DBTITLE 1,Modules
import os
import streamlit as st
from databricks.sdk import WorkspaceClient 

# COMMAND ----------

INDEX = os.getenv("VS_INDEX", "dama.documents.dmbok_chunks_index")
LLM = os.getenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
K = int(os.getenv("TOP_K", "6"))
 
SYSTEM = (
    "Tu es un expert DAMA-DMBOK. Réponds uniquement à partir des extraits fournis. "
    "Cite tes sources sous la forme (p. X – Section). "
    "Si l'information n'est pas dans les extraits, dis-le clairement. "
    "Réponds dans la langue de la question."
)
 
w = WorkspaceClient()                      # auth automatique via le service principal de l'App
llm = w.serving_endpoints.get_open_ai_client()
 
 
def retrieve(question: str, k: int = K):
    res = w.vector_search_indexes.query_index(
        index_name=INDEX,
        columns=["chunk_id", "section", "page_start", "page_end", "chunk_text"],
        query_text=question,
        num_results=k,
        query_type="HYBRID",
    )
    cols = [c.name for c in res.manifest.columns]
    return [dict(zip(cols, row)) for row in (res.result.data_array or [])]
 
 
def answer(question: str, history: list):
    chunks = retrieve(question)
    context = "\n\n---\n\n".join(
        f"[p. {c['page_start']} – {c['section']}]\n{c['chunk_text']}" for c in chunks
    )
    messages = [{"role": "system", "content": SYSTEM}]
    messages += history[-6:]               # un peu de mémoire conversationnelle
    messages.append({"role": "user", "content": f"EXTRAITS DMBOK:\n{context}\n\nQUESTION: {question}"})
    resp = llm.chat.completions.create(model=LLM, messages=messages, temperature=0.1, max_tokens=1200)
    return resp.choices[0].message.content, chunks
 
 
st.set_page_config(page_title="DAMA-DMBOK Assistant", page_icon="📘", layout="wide")
st.title("📘 DAMA-DMBOK – base de référence")
st.caption(f"Index : `{INDEX}` · Modèle : `{LLM}`")
 
if "history" not in st.session_state:
    st.session_state.history = []
 
for msg in st.session_state.history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
 
if q := st.chat_input("Pose ta question sur le DMBOK…"):
    with st.chat_message("user"):
        st.markdown(q)
    with st.chat_message("assistant"):
        with st.spinner("Recherche dans le DMBOK…"):
            text, sources = answer(q, st.session_state.history)
        st.markdown(text)
        with st.expander(f"Sources ({len(sources)})"):
            for s in sources:
                st.markdown(f"**p. {s['page_start']}–{s['page_end']} · {s['section']}** (score {s.get('score', 0):.3f})")
                st.text(s["chunk_text"][:800])
    st.session_state.history += [
        {"role": "user", "content": q},
        {"role": "assistant", "content": text},
    ]
 