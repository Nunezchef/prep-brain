import streamlit as st
import pandas as pd
from pathlib import Path
from services.rag import rag_engine
import datetime

st.set_page_config(page_title="Knowledge Base", page_icon="📚", layout="wide")

st.title("📚 Knowledge Base")

# --- Tabs ---
tab_ingested, tab_upload = st.tabs(["Ingested Sources", "Manual Upload"])

with tab_ingested:
    st.markdown("### Ingested Sources")
    st.markdown("Inspect, manage, and audit the assistant's reference memory.")

    sources = rag_engine.get_sources()

    if not sources:
        st.info("No knowledge sources ingested yet.")
    else:
        # Sort by date descending
        sources.sort(key=lambda x: x.get("date_ingested", ""), reverse=True)
        
        for s in sources:
            status_color = "green" if s.get("status") == "active" else "red"
            status_text = s.get("status", "active").upper()
            title = s.get("title", s.get("source_name"))
            
            with st.expander(f"**{title}**  ( {s.get('chunk_count', 0)} chunks )  -  :{status_color}[{status_text}]"):
                
                # Detail View Layout
                c1, c2 = st.columns([2, 1])
                
                with c1:
                    st.subheader("What was learned")
                    summary = s.get("summary", "No summary available.")
                    st.info(summary, icon="🧠")
                    
                    st.subheader("Metadata")
                    st.json({
                        "Source File": s.get("source_name"),
                        "ID": s["id"],
                        "Type": s.get("type"),
                        "Date Ingested": s.get("date_ingested"),
                        "Chunking Strategy": "Semantic/Paragraph" 
                    })

                with c2:
                    st.subheader("Actions")
                    
                    # Toggle Status
                    is_active = s.get("status") == "active"
                    if st.button(f"{'Disable' if is_active else 'Enable'} Source", key=f"toggle_{s['id']}"):
                        rag_engine.toggle_source(s["id"], not is_active)
                        st.rerun()
                    
                    st.warning("Destructive Actions")
                    if st.button("🗑️ Remove Source", key=f"delete_{s['id']}", type="primary"):
                        if rag_engine.delete_source(s["id"]):
                            st.success(f"Deleted {title}")
                            st.rerun()
                        else:
                            st.error("Failed to delete source.")

with tab_upload:
    st.markdown("### Manual Upload")
    st.write("Upload text or PDF files to add them to the system manually.")
    
    uploaded_file = st.file_uploader("Choose a file", type=["txt", "pdf"])
    if uploaded_file:
        if st.button("Ingest File"):
            with st.spinner("Ingesting..."):
                # Save temp file
                temp_path = Path(f"data/documents/{uploaded_file.name}")
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Ingest
                title = uploaded_file.name.split('.')[0].replace("_", " ").title()
                extra_meta = {
                    "source_title": title,
                    "source_type": "upload",
                    "summary": f"Manually uploaded document: {title}"
                }
                success, msg = rag_engine.ingest_file(str(temp_path), extra_metadata=extra_meta)
                
                if success:
                    st.success(f"Ingested {msg['num_chunks']} chunks from {msg['source_title']}!")
                    st.info("Please refresh to see it in the list.")
                else:
                    st.error(f"Failed: {msg}")
