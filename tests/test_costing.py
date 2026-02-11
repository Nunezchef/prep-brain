import pytest
import os
import sqlite3
from services import recipes, memory, inventory, costing

TEST_DB = "test_costing.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = costing.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = costing.DB_PATH = original_db_path

def test_cost_calculation():
    # 1. Create Inventory Item with Cost
    inventory.create_item({"name": "Gold flakes", "quantity": 10, "cost": 100.0}) # Cost $100/unit
    
    # 2. Get ID
    con = memory.get_conn()
    cur = con.execute("SELECT id FROM inventory_items WHERE name='Gold flakes'")
    item_id = cur.fetchone()[0]
    con.close()
    
    # 3. Create Recipe using 0.5 units
    recipes.create_recipe(
        {"name": "Fancy Water", "yield_amount": 1, "yield_unit": "glass"},
        [{"inventory_item_id": item_id, "quantity": 0.5, "unit": "unit"}]
    )
    
    # 4. Get Recipe ID
    all_r = recipes.get_all_recipes()
    r_id = all_r[0]["id"]
    
    # 5. Update Costs (Should set ingredient cost to 0.5 * 100 = 50)
    costing.update_ingredient_costs(r_id)
    
    # 6. Calc Cost
    metrics = costing.calculate_recipe_cost(r_id)
    assert metrics["total_cost"] == 50.0
    assert metrics["cost_per_yield"] == 50.0

def test_manual_recipe_update():
    # If no inventory link, cost stays 0 or None initially
    recipes.create_recipe(
        {"name": "Air", "yield_amount": 10},
        [{"item_name_text": "Love", "quantity": 10}]
    )
    r_id = recipes.get_all_recipes()[0]["id"]
    
    costing.update_ingredient_costs(r_id) # Should do nothing
    metrics = costing.calculate_recipe_cost(r_id)
    assert metrics["total_cost"] == 0.0
