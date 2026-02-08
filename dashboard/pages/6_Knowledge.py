import streamlit as st
from pathlib import Path

from services.rag import rag_engine

st.set_page_config(page_title="Knowledge Base", page_icon="📚", layout="wide")

st.title("📚 Knowledge Base")
st.caption("Inspect, manage, and audit the assistant's reference memory.")

# --- Tabs ---
tab_ingested, tab_upload = st.tabs(["Ingested Sources", "Manual Upload"])

with tab_ingested:
    st.markdown("### Ingested Sources")

    sources = rag_engine.get_sources()

    if not sources:
        st.info("No knowledge sources ingested yet.")
    else:
        sources.sort(key=lambda source: source.get("date_ingested", ""), reverse=True)

        for source in sources:
            status_color = "green" if source.get("status") == "active" else "red"
            status_text = source.get("status", "active").upper()
            title = source.get("title", source.get("source_name"))

            flags = []
            if source.get("image_rich"):
                flags.append("IMAGE-RICH")
            if source.get("ocr_applied"):
                flags.append("OCR APPLIED")
            elif source.get("ocr_required"):
                flags.append("OCR REQUIRED")

            flag_suffix = f" | {' | '.join(flags)}" if flags else ""
            expander_title = (
                f"**{title}**  ({source.get('chunk_count', 0)} chunks)"
                f"  -  :{status_color}[{status_text}]{flag_suffix}"
            )

            with st.expander(expander_title):
                left_col, right_col = st.columns([2, 1])

                with left_col:
                    st.subheader("What was learned")
                    summary = source.get("summary", "No summary available.")
                    st.info(summary, icon="🧠")

                    ocr_applied = bool(source.get("ocr_applied"))
                    ocr_required = bool(source.get("ocr_required"))
                    image_rich = bool(source.get("image_rich"))

                    if image_rich and not ocr_applied:
                        st.error(
                            "Image-rich source without OCR detected. This source should be re-ingested "
                            "after OCR preprocessing to avoid partial indexing."
                        )

                    if ocr_applied:
                        st.success("OCR was applied during ingestion.")
                    elif ocr_required:
                        st.warning("OCR was required for this source.")
                    else:
                        st.caption("OCR was not required for this source.")

                    st.subheader("Metadata")
                    st.json(
                        {
                            "Source File": source.get("source_name"),
                            "ID": source.get("id"),
                            "Type": source.get("type"),
                            "Date Ingested": source.get("date_ingested"),
                            "Chunk Count": source.get("chunk_count", 0),
                            "Image Rich": image_rich,
                            "OCR Required": ocr_required,
                            "OCR Applied": ocr_applied,
                            "OCR Tool": source.get("ocr_tool", ""),
                            "Page Count": source.get("page_count", 0),
                            "Image Count": source.get("image_count", 0),
                            "Image/Page Ratio": source.get("image_page_ratio", 0.0),
                            "Text Chars (Before OCR)": source.get("text_chars_before_ocr", 0),
                            "Text Chars (After OCR)": source.get("text_chars_after_ocr", 0),
                            "Images Extracted": source.get("images_extracted", 0),
                            "Vision Descriptions": source.get("vision_descriptions_count", 0),
                            "Pipeline": source.get("ingestion_pipeline", {}),
                        }
                    )

                    warnings = source.get("warnings") or []
                    if warnings:
                        st.subheader("Ingestion Warnings")
                        for warning in warnings:
                            st.warning(warning)

                with right_col:
                    st.subheader("Actions")

                    is_active = source.get("status") == "active"
                    if st.button(
                        f"{'Disable' if is_active else 'Enable'} Source",
                        key=f"toggle_{source['id']}",
                    ):
                        rag_engine.toggle_source(source["id"], not is_active)
                        st.rerun()

                    st.warning("Destructive Actions")
                    if st.button("🗑️ Remove Source", key=f"delete_{source['id']}", type="primary"):
                        if rag_engine.delete_source(source["id"]):
                            st.success(f"Deleted {title}")
                            st.rerun()
                        else:
                            st.error("Failed to delete source.")

with tab_upload:
    st.markdown("### Manual Upload")
    st.write("Upload text or PDF files to add them to the system manually.")

    st.info(
        "For scanned or image-heavy PDFs, OCR is mandatory. "
        "Recommended preprocessing command: `ocrmypdf --skip-text input.pdf output_ocr.pdf`"
    )

    uploaded_file = st.file_uploader("Choose a file", type=["txt", "pdf"])

    ingest_options = {
        "extract_images": st.checkbox("Extract images during ingestion", value=False),
        "vision_descriptions": st.checkbox(
            "Generate vision-to-text descriptions (manual toggle)",
            value=False,
            help="Requires a configured vision-capable Ollama model.",
        ),
    }

    if uploaded_file and st.button("Ingest File"):
        with st.spinner("Ingesting..."):
            temp_path = Path(f"data/documents/{uploaded_file.name}")
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            title = uploaded_file.name.split(".")[0].replace("_", " ").title()
            extra_meta = {
                "source_title": title,
                "source_type": "upload",
                "summary": f"Manually uploaded document: {title}",
            }

            success, result = rag_engine.ingest_file(
                str(temp_path),
                extra_metadata=extra_meta,
                ingestion_options=ingest_options,
            )

            if success:
                st.success(f"Ingested {result['num_chunks']} chunks from {result['source_title']}.")

                if result.get("image_rich"):
                    st.warning("This source is image-rich.")
                if result.get("ocr_applied"):
                    st.success("OCR was applied.")

                details = {
                    "OCR Required": result.get("ocr_required", False),
                    "OCR Applied": result.get("ocr_applied", False),
                    "Image Rich": result.get("image_rich", False),
                    "Images Extracted": result.get("images_extracted", 0),
                    "Vision Descriptions": result.get("vision_descriptions_count", 0),
                }
                st.json(details)

                warnings = result.get("warnings") or []
                for warning in warnings:
                    st.warning(warning)
            else:
                st.error(f"Ingestion failed: {result}")
