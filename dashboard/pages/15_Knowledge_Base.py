import streamlit as st
from pathlib import Path
from services.rag import RAGEngine

st.set_page_config(page_title="Knowledge Base", page_icon="📚", layout="wide")

st.title("📚 Knowledge Base & RAG Manager")

tab_sources, tab_ingest, tab_search = st.tabs(["📁 Sources", "📥 Ingest", "🔍 Search"])

@st.cache_resource
def get_rag():
    return RAGEngine()

rag = get_rag()

# --- SOURCES TAB ---
with tab_sources:
    st.subheader("Ingested Sources")
    
    sources = rag.get_sources()
    
    if not sources:
        st.info("No sources ingested yet. Use the 'Ingest' tab to add documents.")
    else:
        # Tier filter
        tiers = list(set(s.get("knowledge_tier", "unknown") for s in sources))
        tier_filter = st.selectbox("Filter by Tier", ["All"] + sorted(tiers))
        
        filtered = sources if tier_filter == "All" else [s for s in sources if s.get("knowledge_tier") == tier_filter]
        
        for src in filtered:
            status_emoji = "🟢" if src.get("status") == "active" else "🔴"
            with st.expander(f"{status_emoji} **{src['title']}** — {src.get('knowledge_tier', 'N/A')} ({src['chunk_count']} chunks)"):
                c1, c2, c3 = st.columns(3)
                c1.metric("Chunks", src["chunk_count"])
                c2.metric("Pages", src.get("page_count", "N/A"))
                c3.metric("OCR Applied", "Yes" if src.get("ocr_applied") else "No")
                
                st.caption(f"Source: {src.get('source_name')} | Ingested: {src.get('date_ingested', 'N/A')[:10]}")
                
                if src.get("warnings"):
                    for w in src["warnings"]:
                        st.warning(w)
                
                bc1, bc2 = st.columns(2)
                is_active = src.get("status") == "active"
                if bc1.button(
                    "🔴 Disable" if is_active else "🟢 Enable",
                    key=f"toggle_{src['id']}"
                ):
                    rag.toggle_source(src["id"], not is_active)
                    st.rerun()
                
                if bc2.button("🗑️ Delete", key=f"del_{src['id']}"):
                    rag.delete_source(src["id"])
                    st.rerun()

# --- INGEST TAB ---
with tab_ingest:
    st.subheader("Ingest New Document")
    
    with st.form("ingest_form"):
        uploaded_file = st.file_uploader(
            "Upload Document",
            type=["pdf", "docx", "txt", "md"],
            help="Supported: PDF, DOCX, TXT, MD"
        )
        
        title = st.text_input("Document Title")
        source_type = st.selectbox("Source Type", ["recipe", "sop", "reference", "training", "vendor", "other"])
        knowledge_tier = st.selectbox("Knowledge Tier", [
            "tier1_recipe_ops", 
            "tier2_notes_sops", 
            "tier3_reference_theory"
        ])
        summary = st.text_area("Summary (optional)")
        
        if st.form_submit_button("📥 Ingest"):
            if uploaded_file:
                # Save uploaded file
                upload_dir = Path("data/uploads")
                upload_dir.mkdir(parents=True, exist_ok=True)
                file_path = upload_dir / uploaded_file.name
                file_path.write_bytes(uploaded_file.read())
                
                with st.spinner("Ingesting..."):
                    ok, result = rag.ingest_file(
                        str(file_path),
                        extra_metadata={
                            "source_title": title or uploaded_file.name,
                            "source_type": source_type,
                            "knowledge_tier": knowledge_tier,
                            "summary": summary
                        }
                    )
                
                if ok:
                    st.success(f"✅ Ingested! {result['num_chunks']} chunks created.")
                    if result.get("warnings"):
                        for w in result["warnings"]:
                            st.warning(w)
                else:
                    st.error(f"❌ {result}")
            else:
                st.error("Please upload a file.")

# --- SEARCH TAB ---
with tab_search:
    st.subheader("Search Knowledge Base")
    
    query = st.text_input("Enter your query")
    
    c1, c2 = st.columns(2)
    n_results = c1.slider("Results", 1, 20, 5)
    tier_search = c2.selectbox("Search Tier", ["All", "tier1_recipe_ops", "tier2_notes_sops", "tier3_reference_theory"])
    
    if st.button("🔍 Search") and query:
        tiers = [tier_search] if tier_search != "All" else None
        results = rag.search(query, n_results=n_results, source_tiers=tiers)
        
        if not results:
            st.info("No results found.")
        else:
            for i, r in enumerate(results, 1):
                with st.expander(f"**{i}.** {r.get('source', 'unknown')} — {r.get('heading', '')} (dist: {r.get('distance', 0):.3f})"):
                    st.write(r["content"])
                    st.caption(f"Tier: {r.get('knowledge_tier', 'N/A')}")
