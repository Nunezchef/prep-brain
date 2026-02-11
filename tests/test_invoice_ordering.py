import os
from datetime import datetime
from pathlib import Path

import pytest

from services import invoice_ingest, memory, ordering

TEST_DB = "test_invoice_ordering.db"


@pytest.fixture(autouse=True)
def setup_teardown_db():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = Path(TEST_DB)
    memory.init_db()

    con = memory.get_conn()
    try:
        con.execute(
            """
            INSERT INTO vendors (name, email, ordering_method, cutoff_time)
            VALUES ('Coastal Wholesale', 'orders@coastal.example', 'email', '11:00')
            """
        )
        con.execute(
            """
            INSERT INTO vendors (name, email, ordering_method, cutoff_time)
            VALUES ('Prime Produce', 'orders@prime.example', 'email', '11:00')
            """
        )
        con.commit()
    finally:
        con.close()

    yield

    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    memory.DB_PATH = original_db_path


def _vendor_id_by_name(name: str) -> int:
    con = memory.get_conn()
    try:
        row = con.execute("SELECT id FROM vendors WHERE name = ?", (name,)).fetchone()
        return int(row["id"])
    finally:
        con.close()


def test_invoice_ingest_pending_and_assign(monkeypatch):
    monkeypatch.setattr(
        invoice_ingest,
        "_ocr_tesseract",
        lambda _path: """
            INVOICE #A100
            2 cs yuzu 45.00 90.00
            50 # white onions 0.85 42.50
        """,
    )

    result = invoice_ingest.ingest_invoice_image(
        image_path="/tmp/test_invoice.jpg",
        telegram_chat_id=10,
        telegram_user_id=20,
        vendor_confidence_threshold=0.75,
    )
    assert result["ok"] is True
    assert result["status"] == "pending_vendor"

    coastal_id = _vendor_id_by_name("Coastal Wholesale")
    assign = invoice_ingest.assign_vendor(invoice_ingest_id=int(result["invoice_ingest_id"]), vendor_id=coastal_id)
    assert assign["ok"] is True

    con = memory.get_conn()
    try:
        ingest = con.execute("SELECT status, vendor_id FROM invoice_ingests").fetchone()
        assert ingest["status"] == "parsed"
        assert int(ingest["vendor_id"]) == coastal_id

        line_count = con.execute("SELECT COUNT(*) FROM invoice_line_items").fetchone()[0]
        assert line_count >= 1

        affinity_count = con.execute(
            "SELECT COUNT(*) FROM vendor_item_affinity WHERE vendor_id = ?", (coastal_id,)
        ).fetchone()[0]
        assert affinity_count >= 1

        receiving_count = con.execute("SELECT COUNT(*) FROM receiving_log WHERE vendor_id = ?", (coastal_id,)).fetchone()[0]
        assert receiving_count >= 1
    finally:
        con.close()


def test_invoice_ingest_high_confidence_auto_vendor(monkeypatch):
    monkeypatch.setattr(
        invoice_ingest,
        "_ocr_tesseract",
        lambda _path: """
            Coastal Wholesale
            INVOICE #A101
            1 case lemons 32.00 32.00
        """,
    )

    result = invoice_ingest.ingest_invoice_image(
        image_path="/tmp/test_invoice_2.jpg",
        telegram_chat_id=11,
        telegram_user_id=21,
        vendor_confidence_threshold=0.75,
    )
    assert result["ok"] is True
    assert result["status"] == "parsed"
    assert result["vendor_id"] == _vendor_id_by_name("Coastal Wholesale")


def test_order_routing_affinity_last_vendor_and_ambiguity():
    coastal_id = _vendor_id_by_name("Coastal Wholesale")
    prime_id = _vendor_id_by_name("Prime Produce")
    blis_key = invoice_ingest.normalize_item_name("BLIS soy")

    con = memory.get_conn()
    try:
        con.execute(
            """
            INSERT INTO vendor_item_affinity (normalized_item_name, vendor_id, purchase_count, score)
            VALUES ('white onion', ?, 4, 2.1)
            """,
            (coastal_id,),
        )
        con.execute(
            """
            INSERT INTO vendor_item_affinity (normalized_item_name, vendor_id, purchase_count, score)
            VALUES (?, ?, 3, 1.0)
            """,
            (blis_key, coastal_id),
        )
        con.execute(
            """
            INSERT INTO vendor_item_affinity (normalized_item_name, vendor_id, purchase_count, score)
            VALUES (?, ?, 3, 0.95)
            """,
            (blis_key, prime_id),
        )
        con.commit()
    finally:
        con.close()

    routed = ordering.route_order_text(
        text="add 50# white onions",
        telegram_chat_id=100,
        added_by="Chef",
    )
    assert routed["ok"] is True
    assert routed["vendor_id"] == coastal_id
    assert routed["quantity"] == pytest.approx(22679.6185, rel=1e-6)
    assert routed["unit"] == "g"
    assert routed["display_original"].startswith("50")

    fallback = ordering.route_order_text(
        text="add 2 cs yuzu",
        telegram_chat_id=100,
        added_by="Chef",
    )
    assert fallback["ok"] is True
    assert fallback["vendor_id"] == coastal_id

    ambiguous = ordering.route_order_text(
        text="put 1 case BLIS soy on the order",
        telegram_chat_id=200,
        added_by="Chef",
    )
    assert ambiguous["ok"] is False
    assert ambiguous["needs_vendor"] is True
    assert ambiguous["reason"] == "ambiguous_affinity"
    assert len(ambiguous["candidates"]) >= 2


def test_vendor_email_draft_and_preview_send():
    coastal_id = _vendor_id_by_name("Coastal Wholesale")

    ordering.add_routed_order(
        telegram_chat_id=300,
        added_by="Chef",
        item_name="white onions",
        normalized_item_name="white onion",
        quantity=50,
        unit="lb",
        vendor_id=coastal_id,
    )

    draft = ordering.build_vendor_email_draft(coastal_id)
    assert draft["ok"] is True
    assert draft["items_count"] == 1
    assert "white onions" in draft["body"].lower()

    sent = ordering.send_vendor_draft(coastal_id)
    assert sent["ok"] is True
    assert sent["sent"] is False
    assert "TO:" in sent["preview"]


def test_cutoff_reminder_dedupe_by_vendor_date_offset():
    coastal_id = _vendor_id_by_name("Coastal Wholesale")

    ordering.add_routed_order(
        telegram_chat_id=400,
        added_by="Chef",
        item_name="lemons",
        normalized_item_name="lemon",
        quantity=2,
        unit="case",
        vendor_id=coastal_id,
    )

    now = datetime(2026, 2, 10, 10, 0, 0)
    due = ordering.get_due_cutoff_reminders(
        reminder_offsets_minutes=[60, 15],
        quiet_hours=None,
        now=now,
    )
    assert any(item["vendor_id"] == coastal_id and item["offset_minutes"] == 60 for item in due)

    ordering.mark_cutoff_reminder_sent(vendor_id=coastal_id, reminder_date="2026-02-10", offset_minutes=60)
    due_again = ordering.get_due_cutoff_reminders(
        reminder_offsets_minutes=[60, 15],
        quiet_hours=None,
        now=now,
    )
    assert not any(item["vendor_id"] == coastal_id and item["offset_minutes"] == 60 for item in due_again)
