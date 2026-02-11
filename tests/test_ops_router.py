import os
from pathlib import Path

import pytest

from services import memory, ops_router

TEST_DB = "test_ops_router.db"


@pytest.fixture(autouse=True)
def setup_db():
    original = memory.DB_PATH
    memory.DB_PATH = Path(TEST_DB)
    memory.init_db()
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    memory.DB_PATH = original


def _seed_recipe(name: str) -> int:
    con = memory.get_conn()
    try:
        cur = con.execute("INSERT INTO recipes (name, method, is_active) VALUES (?, 'Cook.', 1)", (name,))
        con.commit()
        return int(cur.lastrowid)
    finally:
        con.close()


def test_detect_ops_intent_for_price_update():
    intent = ops_router.detect_ops_intent("Update the price of the braised ribs to 4.32 dollars a portion")
    assert intent is not None
    assert intent["intent"] == "update_recipe_sales_price"
    assert intent["target_name"].lower() == "braised ribs"
    assert intent["price"] == pytest.approx(4.32)
    assert intent["unit"] == "portion"


def test_update_sales_price_writes_recipe_menu_and_audit():
    recipe_id = _seed_recipe("Braised Ribs")
    con = memory.get_conn()
    try:
        con.execute(
            "INSERT INTO menu_items (name, recipe_id, selling_price, is_active) VALUES (?, ?, ?, 1)",
            ("Braised Ribs", recipe_id, 0.0),
        )
        con.commit()
    finally:
        con.close()

    intent = ops_router.detect_ops_intent("Set braised ribs price to $5")
    assert intent is not None
    result = ops_router.execute_ops_intent(
        intent,
        actor_telegram_user_id=101,
        actor_display_name="Chef",
    )
    assert result["status"] == "updated"
    assert result["recipe_name"] == "Braised Ribs"
    assert result["price"] == pytest.approx(5.0)

    con = memory.get_conn()
    try:
        row = con.execute("SELECT sales_price, unit FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
        assert float(row["sales_price"]) == pytest.approx(5.0)
        assert str(row["unit"] or "") == "portion"

        menu_row = con.execute(
            "SELECT selling_price FROM menu_items WHERE recipe_id = ? LIMIT 1",
            (recipe_id,),
        ).fetchone()
        assert float(menu_row["selling_price"]) == pytest.approx(5.0)

        events = con.execute("SELECT action_type, entity_type FROM audit_events ORDER BY id ASC").fetchall()
        assert len(events) >= 1
        assert any(str(e["action_type"]) == "update_sales_price" and str(e["entity_type"]) == "recipe" for e in events)
    finally:
        con.close()


def test_ambiguous_update_returns_choices():
    _seed_recipe("Braised Ribs")
    _seed_recipe("Short Ribs")
    intent = ops_router.detect_ops_intent("Update price of ribs to 4.32")
    assert intent is not None

    result = ops_router.execute_ops_intent(
        intent,
        actor_telegram_user_id=101,
        actor_display_name="Chef",
    )
    assert result["status"] == "needs_choice"
    assert len(result["choices"]) >= 2


def test_cost_request_requires_clarification():
    _seed_recipe("Braised Ribs")
    intent = ops_router.detect_ops_intent("Update cost of braised ribs to 4.32")
    assert intent is not None
    assert intent["intent"] == "cost_clarification"

    result = ops_router.execute_ops_intent(
        intent,
        actor_telegram_user_id=101,
        actor_display_name="Chef",
    )
    assert result["status"] == "needs_clarification"
    assert result["reason"] == "cost_is_calculated"


def test_parse_currency_decimal_comma():
    intent = ops_router.detect_ops_intent("Set braised ribs price to 4,32 dollars a portion")
    assert intent is not None
    assert intent["price"] == pytest.approx(4.32)


def test_high_price_requires_confirmation():
    _seed_recipe("Braised Ribs")
    intent = ops_router.detect_ops_intent("Set braised ribs price to 600")
    assert intent is not None
    result = ops_router.execute_ops_intent(
        intent,
        actor_telegram_user_id=101,
        actor_display_name="Chef",
    )
    assert result["status"] == "needs_confirmation"

    confirmed = dict(intent)
    confirmed["confirmed"] = True
    result2 = ops_router.execute_ops_intent(
        confirmed,
        actor_telegram_user_id=101,
        actor_display_name="Chef",
    )
    assert result2["status"] == "updated"
