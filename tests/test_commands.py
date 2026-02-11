import os
import json
from pathlib import Path

import pytest

from services import commands, memory, ordering
from services import autonomy as autonomy_service

TEST_DB = "test_commands.db"


@pytest.fixture(autouse=True)
def setup_db():
    original = memory.DB_PATH
    memory.DB_PATH = Path(TEST_DB)
    memory.init_db()
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    memory.DB_PATH = original


def test_parse_command_flags_and_scope():
    req = commands.parse_command('/drafts 5 -r kitchen_a --detail --json')
    assert req is not None
    assert req.name == "drafts"
    assert req.args == ["5"]
    assert req.scope == "kitchen_a"
    assert req.detail is True
    assert req.json_output is True


def test_help_ordering_contains_vendor_commands():
    req = commands.parse_command('/help ordering')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "<b>Ordering</b>" in res.text
    assert "Routing Rules" in res.text


def test_help_default_grouped_ops_console():
    req = commands.parse_command('/help')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Prep-Brain - Ops Console" in res.text
    assert "Status &amp; Control" in res.text
    assert "/help ordering" in res.text


def test_commands_generated_from_registry():
    req = commands.parse_command('/commands')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Canonical Commands" in res.text
    assert "/status [--detail]" in res.text
    assert "(enabled)" in res.text


def test_status_command_runs():
    req = commands.parse_command('/status')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Status" in res.text
    assert "drafts" in res.text


def test_autonomy_command_runs():
    req = commands.parse_command('/autonomy')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Autonomy:" in res.text
    assert "Queue:" in res.text


def test_jobs_command_shows_queued_job():
    queued = autonomy_service.queue_ingest_job(
        source_filename="Fire Recipes 2024.docx",
        source_type="restaurant_recipes",
        restaurant_tag="FIRE",
    )
    assert queued["ok"] is True

    req = commands.parse_command('/jobs')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Ingest Jobs" in res.text
    assert "Fire Recipes 2024.docx" in res.text

    con = memory.get_conn()
    try:
        row = con.execute(
            "SELECT status FROM doc_sources WHERE ingest_id = ?",
            (queued["ingest_id"],),
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert row["status"] == "queued"


def test_recipes_new_lists_latest():
    con = memory.get_conn()
    try:
        con.execute(
            "INSERT INTO recipes (name, method, created_at) VALUES ('Test New Recipe', 'Cook.', CURRENT_TIMESTAMP)"
        )
        con.commit()
    finally:
        con.close()

    req = commands.parse_command('/recipes new 1')
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "New Recipes" in res.text
    assert "Test New Recipe" in res.text


def test_doc_sources_table_exists():
    con = memory.get_conn()
    try:
        row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='doc_sources'"
        ).fetchone()
        audit_row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_events'"
        ).fetchone()
        status_row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='autonomy_status'"
        ).fetchone()
        jobs_row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_jobs'"
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert audit_row is not None
    assert status_row is not None
    assert jobs_row is not None


def test_review_vendor_returns_html_pre():
    con = memory.get_conn()
    try:
        cur = con.execute(
            "INSERT INTO vendors (name, email, ordering_method, cutoff_time) VALUES ('CW', 'orders@cw.test', 'email', '15:00')"
        )
        vendor_id = int(cur.lastrowid)
        con.commit()
    finally:
        con.close()

    ordering.add_routed_order(
        telegram_chat_id=1,
        added_by="Chef",
        item_name="white onions",
        normalized_item_name="white onion",
        quantity=22679.6185,
        unit="g",
        canonical_value=22679.6185,
        canonical_unit="g",
        display_original="50#",
        display_pretty="22,679.619 g (50#)",
        vendor_id=vendor_id,
    )

    req = commands.parse_command(f"/review vendor {vendor_id}")
    res = commands.execute_command(req, {}, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "<pre>" in res.text
    assert "TO:" in res.text


def test_debug_ingest_reads_report_file():
    reports_dir = Path("data/ingest_reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    ingest_id = "testdebug123"
    report_path = reports_dir / f"{ingest_id}.json"
    report_path.write_text(
        json.dumps(
            {
                "ingest_id": ingest_id,
                "status": "ok",
                "raw_document": {"filename": "fire.docx"},
                "extraction_metrics": {
                    "docx_paragraph_count": 12,
                    "extracted_from_paragraphs_chars": 3456,
                    "docx_table_count": 3,
                    "extracted_from_tables_chars": 8901,
                    "embedded_image_count": 0,
                },
                "chunking_metrics": {
                    "chunk_size_config": 3500,
                    "chunk_overlap_config": 400,
                    "produced_chunk_count": 42,
                },
                "vector_store_metrics": {
                    "attempted_add_count": 42,
                    "successfully_added_count": 42,
                },
                "warnings": [],
            }
        ),
        encoding="utf-8",
    )

    chat_state = {"last_ingest_id": ingest_id}
    try:
        on_req = commands.parse_command("/debug on")
        commands.execute_command(on_req, chat_state, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
        req = commands.parse_command("/debug ingest last")
        res = commands.execute_command(req, chat_state, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
        assert "Debug Ingest" in res.text
        assert "chunks produced: 42" in res.text
    finally:
        if report_path.exists():
            report_path.unlink()


def test_debug_recipe_reports_sections(monkeypatch):
    chat_state = {"debug_enabled": True}

    def fake_debug_house_recipe(**kwargs):
        assert "cucumber spice" in kwargs.get("query_text", "").lower()
        return {
            "status": "ok",
            "query": kwargs.get("query_text"),
            "source_title": "Fire Recipes 2024",
            "matched_recipe_name": "Cucumber spice",
            "chunks_used": 2,
            "sections_detected": ["Base", "Grind and add", "Method"],
            "missing_sections": [],
            "confidence": 0.93,
        }

    monkeypatch.setattr(commands.rag_engine, "debug_house_recipe", fake_debug_house_recipe)

    req = commands.parse_command("/debug recipe cucumber spice")
    res = commands.execute_command(req, chat_state, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Debug Recipe" in res.text
    assert "chunks used 2" in res.text
    assert "Grind and add" in res.text


def test_debug_chunks_includes_three_chunk_previews():
    reports_dir = Path("data/ingest_reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    ingest_id = "testchunks3"
    report_path = reports_dir / f"{ingest_id}.json"
    report_path.write_text(
        json.dumps(
            {
                "ingest_id": ingest_id,
                "status": "warn",
                "raw_document": {"filename": "fire.docx"},
                "extraction_metrics": {
                    "extracted_text_chars": 8200,
                    "extracted_text_lines": 320,
                    "docx_paragraph_count": 400,
                    "docx_table_count": 0,
                    "docx_table_cell_count": 0,
                    "extracted_from_tables_chars": 0,
                    "extracted_from_paragraphs_chars": 8000,
                },
                "chunking_metrics": {
                    "chunk_size_config": 3500,
                    "chunk_overlap_config": 400,
                    "produced_chunk_count": 3,
                    "avg_chunk_chars": 3100.0,
                    "min_chunk_chars": 2800,
                    "max_chunk_chars": 3490,
                    "pre_dedupe_chunk_samples": [
                        {"chunk_id": 0, "text_preview": "alpha " * 80},
                        {"chunk_id": 1, "text_preview": "beta " * 80},
                        {"chunk_id": 2, "text_preview": "gamma " * 80},
                    ],
                },
                "dedupe_metrics": {
                    "dedupe_enabled": True,
                    "pre_dedupe_count": 3,
                    "post_dedupe_count": 3,
                    "top_repeated_hashes": [],
                },
                "vector_store_metrics": {
                    "attempted_add_count": 3,
                    "successfully_added_count": 3,
                },
                "warnings": ["low_chunk_count: extracted_text_chars=8200 produced_chunk_count=3"],
            }
        ),
        encoding="utf-8",
    )

    chat_state = {"debug_enabled": True, "last_ingest_id": ingest_id}
    try:
        req = commands.parse_command("/debug chunks last")
        res = commands.execute_command(req, chat_state, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
        assert "Debug Chunks" in res.text
        assert "#0 alpha" in res.text
        assert "#1 beta" in res.text
        assert "#2 gamma" in res.text
    finally:
        if report_path.exists():
            report_path.unlink()


def test_debug_sources_reads_doc_sources_rows():
    con = memory.get_conn()
    try:
        con.execute(
            """
            INSERT INTO doc_sources (
                ingest_id, filename, source_type, status, extracted_text_chars, chunk_count, chunks_added
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("ingabc123", "fire.docx", "restaurant_recipes", "queued", 0, 0, 0),
        )
        con.commit()
    finally:
        con.close()

    chat_state = {"debug_enabled": True}
    req = commands.parse_command("/debug sources")
    res = commands.execute_command(req, chat_state, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Debug Sources" in res.text
    assert "fire.docx" in res.text


def test_debug_db_reports_path_and_counts():
    chat_state = {"debug_enabled": True}
    req = commands.parse_command("/debug db")
    res = commands.execute_command(req, chat_state, telegram_chat_id=1, telegram_user_id=2, display_name="Chef")
    assert "Debug DB" in res.text
    assert "ingest_jobs" in res.text
    assert "doc_sources" in res.text
