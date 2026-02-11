import asyncio
import importlib
import json
import os
import sys
import types

import pytest

from services import memory, recipes, inventory


def _import_autonomy_with_stubbed_rag(monkeypatch):
    rag_stub = types.ModuleType("services.rag")
    rag_stub.TIER_1_RECIPE_OPS = "tier1_recipe_ops"
    rag_stub.TIER_2_NOTES_SOPS = "tier2_notes_sops"
    rag_stub.TIER_3_REFERENCE_THEORY = "tier3_reference_theory"

    def normalize_knowledge_tier(value):
        if not value:
            return None
        value = str(value).strip().lower()
        aliases = {
            "tier1": rag_stub.TIER_1_RECIPE_OPS,
            "tier1_recipe_ops": rag_stub.TIER_1_RECIPE_OPS,
            "tier2": rag_stub.TIER_2_NOTES_SOPS,
            "tier2_notes_sops": rag_stub.TIER_2_NOTES_SOPS,
            "tier3": rag_stub.TIER_3_REFERENCE_THEORY,
            "tier3_reference_theory": rag_stub.TIER_3_REFERENCE_THEORY,
        }
        return aliases.get(value)

    rag_stub.normalize_knowledge_tier = normalize_knowledge_tier
    rag_stub.infer_knowledge_tier = lambda **kwargs: rag_stub.TIER_1_RECIPE_OPS

    class _Collection:
        @staticmethod
        def get(**kwargs):
            return {"documents": [], "metadatas": []}

    class _Engine:
        collection = _Collection()

        @staticmethod
        def get_sources():
            return []

    rag_stub.rag_engine = _Engine()

    monkeypatch.setitem(sys.modules, "services.rag", rag_stub)
    sys.modules.pop("services.autonomy", None)
    return importlib.import_module("services.autonomy")


@pytest.fixture(autouse=True)
def setup_test_db(monkeypatch):
    test_db = "test_autonomy_security.db"
    original_paths = (memory.DB_PATH, recipes.DB_PATH, inventory.DB_PATH)
    db_path = inventory.Path(test_db)
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = db_path
    memory.init_db()
    yield
    if os.path.exists(test_db):
        os.remove(test_db)
    memory.DB_PATH, recipes.DB_PATH, inventory.DB_PATH = original_paths


def test_secrets_are_redacted_in_autonomy_logs(monkeypatch):
    autonomy = _import_autonomy_with_stubbed_rag(monkeypatch)
    worker = autonomy.AutonomyWorker()

    worker.log_action(
        action="security_test",
        detail="token=abc123 password=hunter2 Bearer very.secret.token",
    )

    con = memory.get_conn()
    try:
        row = con.execute(
            "SELECT detail FROM autonomy_log WHERE action = 'security_test' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        con.close()

    detail = row["detail"]
    assert "abc123" not in detail
    assert "hunter2" not in detail
    assert "very.secret.token" not in detail
    assert "[REDACTED]" in detail


def test_web_price_estimate_is_stored_as_general_knowledge_web(monkeypatch):
    autonomy = _import_autonomy_with_stubbed_rag(monkeypatch)
    worker = autonomy.AutonomyWorker()

    con = memory.get_conn()
    try:
        worker._save_price_estimate(
            con,
            item_name="Vanilla Beans",
            low_price=12.50,
            high_price=18.75,
            unit="oz",
            source_urls=["https://example.com/vanilla-price"],
            retrieved_at="2026-02-10T00:00:00Z",
        )
        con.commit()
        row = con.execute(
            "SELECT knowledge_tier, source_urls FROM price_estimates WHERE item_name = 'Vanilla Beans'"
        ).fetchone()
    finally:
        con.close()

    assert row["knowledge_tier"] == "general_knowledge_web"
    assert "example.com/vanilla-price" in row["source_urls"]


def test_general_knowledge_drafts_never_promote_into_recipe_db(monkeypatch):
    autonomy = _import_autonomy_with_stubbed_rag(monkeypatch)
    worker = autonomy.AutonomyWorker()

    con = memory.get_conn()
    try:
        con.execute(
            """
            INSERT INTO recipe_drafts
            (name, method, ingredients_json, confidence, status, knowledge_tier)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "Reference Demi-Glace",
                "Reduce and mount.",
                json.dumps([{"item_name_text": "Bones", "quantity": 2, "unit": "kg"}]),
                0.99,
                "enriched",
                "tier3_reference_theory",
            ),
        )
        con.commit()
    finally:
        con.close()

    asyncio.run(worker.promote_drafts())

    con = memory.get_conn()
    try:
        recipe_count = con.execute("SELECT COUNT(*) FROM recipes").fetchone()[0]
        draft_row = con.execute(
            "SELECT status, rejection_reason FROM recipe_drafts WHERE name = 'Reference Demi-Glace' LIMIT 1"
        ).fetchone()
    finally:
        con.close()

    assert recipe_count == 0
    assert draft_row["status"] == "rejected"
    assert "boundary" in (draft_row["rejection_reason"] or "").lower()


def test_web_estimates_do_not_override_authoritative_costs(monkeypatch):
    autonomy = _import_autonomy_with_stubbed_rag(monkeypatch)
    worker = autonomy.AutonomyWorker()

    # Authoritative inventory cost exists.
    inventory.create_item({"name": "Sugar", "quantity": 5, "unit": "lb", "cost": 2.0})
    con = memory.get_conn()
    try:
        item_row = con.execute("SELECT id FROM inventory_items WHERE name='Sugar'").fetchone()
        con.execute(
            """
            INSERT INTO recipes (name, method, is_active) VALUES ('Sugar Syrup', 'Boil and cool.', 1)
            """
        )
        recipe_id = con.execute("SELECT id FROM recipes WHERE name='Sugar Syrup'").fetchone()["id"]
        con.execute(
            """
            INSERT INTO recipe_ingredients (recipe_id, inventory_item_id, item_name_text, quantity, unit)
            VALUES (?, ?, ?, ?, ?)
            """,
            (recipe_id, int(item_row["id"]), "Sugar", 1.0, "lb"),
        )
        con.commit()
    finally:
        con.close()

    class _FakeWebClient:
        def __init__(self):
            self.called = False

        def research_price_estimate(self, **kwargs):
            self.called = True
            return {"low_price": 10.0, "high_price": 12.0, "unit": "lb", "source_urls": []}

    fake_client = _FakeWebClient()
    worker.web_client = fake_client
    worker.web_enabled = True
    worker.web_mode = "research_only"

    con = memory.get_conn()
    try:
        created = worker._web_estimate_missing_costs(con)
        con.commit()
        count_estimates = con.execute("SELECT COUNT(*) FROM price_estimates").fetchone()[0]
    finally:
        con.close()

    assert created == 0
    assert count_estimates == 0
    assert fake_client.called is False
