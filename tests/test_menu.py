import pytest
import os
import sqlite3
import pandas as pd
from services import menu, recipes, memory, inventory, costing

TEST_DB = "test_menu.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = costing.DB_PATH = menu.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = costing.DB_PATH = menu.DB_PATH = original_db_path

def test_create_and_log_sales():
    # Create Item
    menu.create_menu_item({"name": "Burger", "selling_price": 10.0, "category": "Main"})
    
    # Get ID
    items = menu.get_menu_items()
    item_id = items[0]["id"]
    
    # Log Sales
    res = menu.log_sales([{"menu_item_id": item_id, "quantity_sold": 5}])
    assert "Logged sales" in res
    
    # Verify Matrix Data
    df = menu.get_matrix_data()
    assert not df.empty
    row = df.iloc[0]
    assert row["name"] == "Burger"
    assert row["total_sold"] == 5
    assert row["revenue"] == 50.0

def test_matrix_classification():
    # Setup: 
    # Item A: High Sales, High Margin (Star)
    # Item B: Low Sales, Low Margin (Dog)
    
    # We need recipes to set cost? Or can we test logic without recipes (cost=0)?
    # If cost=0, Margin = Price.
    
    # Item A: Price 20 (Margin 20), Sales 100
    menu.create_menu_item({"name": "Star Item", "selling_price": 20.0, "category": "Main"})
    # Item B: Price 5 (Margin 5), Sales 10
    menu.create_menu_item({"name": "Dog Item", "selling_price": 5.0, "category": "Main"})
    
    items = menu.get_menu_items()
    id_a = [i for i in items if i["name"] == "Star Item"][0]["id"]
    id_b = [i for i in items if i["name"] == "Dog Item"][0]["id"]
    
    menu.log_sales([
        {"menu_item_id": id_a, "quantity_sold": 100},
        {"menu_item_id": id_b, "quantity_sold": 10}
    ])
    
    df = menu.get_matrix_data()
    # Avg Sales = 55. Avg Margin = 12.5.
    
    row_a = df[df["name"] == "Star Item"].iloc[0]
    # Sales 100 > 55, Margin 20 > 12.5 -> Star
    assert row_a["class"] == "Star"
    
    row_b = df[df["name"] == "Dog Item"].iloc[0]
    # Sales 10 < 55, Margin 5 < 12.5 -> Dog
    assert row_b["class"] == "Dog"
