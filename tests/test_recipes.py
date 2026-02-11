import pytest
import os
import sqlite3
from services import recipes, memory, inventory

TEST_DB = "test_recipes.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = original_db_path

def test_create_recipe():
    ingredients = [
        {"item_name_text": "Salt", "quantity": 10, "unit": "g"},
        {"item_name_text": "Pepper", "quantity": 5, "unit": "g"}
    ]
    
    result = recipes.create_recipe(
        {"name": "Seasoning Mix", "yield_amount": 15, "yield_unit": "g", "method": "Mix it."},
        ingredients
    )
    
    assert "successfully" in result
    
    all_r = recipes.get_all_recipes()
    assert len(all_r) == 1
    assert all_r[0]["name"] == "Seasoning Mix"

def test_get_recipe_details():
    # Create inventory item to link
    inventory.create_item({"name": "Cheese", "quantity": 100})
    
    # Get ID
    con = sqlite3.connect(TEST_DB)
    cur = con.execute("SELECT id FROM inventory_items WHERE name='Cheese'")
    cheese_id = cur.fetchone()[0]
    con.close()
    
    ingredients = [
        {"inventory_item_id": cheese_id, "quantity": 50, "unit": "g"}
    ]
    
    recipes.create_recipe(
        {"name": "Cheese Plate", "station": "Pantry"},
        ingredients
    )
    
    # Get Details
    all_r = recipes.get_all_recipes()
    r_id = all_r[0]["id"]
    
    details = recipes.get_recipe_details(r_id)
    assert details["name"] == "Cheese Plate"
    assert len(details["ingredients"]) == 1
    assert details["ingredients"][0]["display_name"] == "Cheese"
    assert details["ingredients"][0]["quantity"] == 50.0
