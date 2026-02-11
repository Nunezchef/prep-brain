import json
from pathlib import Path

from services.rag import rag_engine


def test_ingest_report_is_written_and_loadable(tmp_path):
    ingest_id = "unit_ingest_123"
    report_dir = tmp_path / "ingest_reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    original_dir = rag_engine.ingest_reports_dir
    rag_engine.ingest_reports_dir = report_dir
    try:
        payload = {
            "ingest_id": ingest_id,
            "status": "ok",
            "raw_document": {"filename": "sample.docx"},
            "chunking_metrics": {"produced_chunk_count": 12},
        }
        rag_engine._write_ingest_report(ingest_id, payload)
        path = report_dir / f"{ingest_id}.json"
        assert path.exists()
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded["ingest_id"] == ingest_id
        fetched = rag_engine.load_ingest_report(ingest_id)
        assert fetched is not None
        assert fetched["chunking_metrics"]["produced_chunk_count"] == 12
    finally:
        rag_engine.ingest_reports_dir = original_dir
