from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from prep_brain.ops import layer


def _make_update(text: str):
    return SimpleNamespace(message=SimpleNamespace(text=text))


def _make_context():
    return SimpleNamespace(user_data={})


def _init_ops_db(path: str) -> None:
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE recipes (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            yield_amount REAL,
            yield_unit TEXT,
            station TEXT,
            method TEXT,
            is_active INTEGER DEFAULT 1,
            cost REAL,
            sales_price REAL,
            recent_sales_count INTEGER DEFAULT 0
        )
        """
    )
    con.execute(
        """
        CREATE TABLE recipe_ingredients (
            id INTEGER PRIMARY KEY,
            recipe_id INTEGER NOT NULL,
            inventory_item_id INTEGER,
            item_name_text TEXT,
            quantity REAL,
            unit TEXT,
            notes TEXT
        )
        """
    )
    con.commit()
    con.close()


def test_ops_layer_add_recipe_draft_flow(tmp_path, monkeypatch):
    db_path = tmp_path / "ops.db"
    _init_ops_db(str(db_path))
    monkeypatch.setattr(layer, "CONFIG", {"memory": {"db_path": str(db_path)}})

    context = _make_context()
    text = (
        "Add the following recipe to the system\n"
        "Mushroom oil\n\n"
        "60g Cumin seed\n"
        "150g Fenugreek seed\n"
    )

    first = layer.try_handle_text(_make_update(text), context)
    assert first.handled is True
    assert "Yield for this recipe?" in (first.reply or "")
    assert context.user_data.get("pending_ops", {}).get("type") == "add_recipe"

    second = layer.try_handle_text(_make_update("3 L"), context)
    assert second.handled is True
    assert "Which station?" in (second.reply or "")

    third = layer.try_handle_text(_make_update("Prep"), context)
    assert third.handled is True
    assert "✅ Saved: Mushroom oil" in (third.reply or "")
    assert "pending_ops" not in context.user_data

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    recipe = con.execute("SELECT * FROM recipes WHERE name='Mushroom oil'").fetchone()
    assert recipe is not None
    assert float(recipe["yield_amount"]) == 3.0
    assert recipe["yield_unit"] == "l"
    assert recipe["station"] == "Prep"
    ing_count = int(
        con.execute("SELECT COUNT(*) FROM recipe_ingredients WHERE recipe_id=?", (recipe["id"],))
        .fetchone()[0]
    )
    con.close()
    assert ing_count == 2


def test_ops_layer_update_recipe_cost_by_fuzzy_name(tmp_path, monkeypatch):
    db_path = tmp_path / "ops.db"
    _init_ops_db(str(db_path))
    monkeypatch.setattr(layer, "CONFIG", {"memory": {"db_path": str(db_path)}})

    con = sqlite3.connect(str(db_path))
    con.execute(
        "INSERT INTO recipes (name, cost, recent_sales_count, is_active) VALUES (?, ?, ?, ?)",
        ("Braised Beef Ribs", 0.0, 0, 1),
    )
    con.commit()
    con.close()

    context = _make_context()
    result = layer.try_handle_text(_make_update("Update cost of braised ribs to 4.32"), context)
    assert result.handled is True
    assert "cost set to $4.32" in (result.reply or "")

    con = sqlite3.connect(str(db_path))
    cost = float(con.execute("SELECT cost FROM recipes WHERE name=?", ("Braised Beef Ribs",)).fetchone()[0])
    con.close()
    assert cost == 4.32
