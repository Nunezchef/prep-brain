import pytest
import os
import sqlite3
from services import prep, recipes, memory, inventory

TEST_DB = "test_prep.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = prep.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = prep.DB_PATH = original_db_path

def test_par_setting():
    recipes.create_recipe({"name": "Soup", "yield_unit": "L"}, [])
    r_id = recipes.get_all_recipes()[0]["id"]
    
    res = prep.set_recipe_par(r_id, 10.0)
    assert "updated" in res
    
    # Verify DB
    con = memory.get_conn()
    row = con.execute("SELECT par_level FROM recipes WHERE id=?", (r_id,)).fetchone()
    assert row["par_level"] == 10.0
    con.close()

def test_generate_list_logic():
    # 1. Create Output Item (Inventory) with 2L
    inventory.create_item({"name": "Soup Batch", "quantity": 2.0})
    con = memory.get_conn()
    inv_id = con.execute("SELECT id FROM inventory_items WHERE name='Soup Batch'").fetchone()[0]
    con.close()
    
    # 2. Create Recipe with Par 5L, linked to Output
    recipes.create_recipe({"name": "Soup", "yield_unit": "L"}, [])
    r_id = recipes.get_all_recipes()[0]["id"]
    prep.set_recipe_par(r_id, 5.0, inv_id)
    
    # 3. Generate List -> Need = 5 - 2 = 3L
    res = prep.generate_prep_list()
    assert "Generated 1" in res
    
    # 4. Verify Item
    items = prep.get_prep_list('todo')
    assert len(items) == 1
    assert items[0]["need_quantity"] == 3.0
    assert items[0]["recipe_name"] == "Soup"
    
def test_generate_list_no_link():
    # Recipe with Par 5L, no link -> Need = 5L
    recipes.create_recipe({"name": "Bread", "yield_unit": "Loaves"}, [])
    r_id = recipes.get_all_recipes()[0]["id"]
    prep.set_recipe_par(r_id, 5.0) # No link
    
    prep.generate_prep_list()
    items = prep.get_prep_list('todo')
    assert items[0]["need_quantity"] == 5.0
