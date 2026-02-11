import pytest
import os
import sqlite3
from services import inventory, memory

TEST_DB = "test_inventory_entry.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = inventory.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = inventory.DB_PATH = original_db_path

def test_submit_count():
    # Setup item
    inventory.create_item({"name": "Milk", "quantity": 2.0, "cost": 5.0})
    
    # Get ID
    con = sqlite3.connect(TEST_DB)
    cur = con.execute("SELECT id FROM inventory_items WHERE name='Milk'")
    item_id = cur.fetchone()[0]
    con.close()
    
    # Submit new count
    result = inventory.submit_count([{"item_id": item_id, "quantity": 5.0}], user="Tester")
    
    assert "updated 1 items" in result
    
    # Verify Update
    con = sqlite3.connect(TEST_DB)
    cur = con.execute("SELECT quantity FROM inventory_items WHERE id=?", (item_id,))
    assert cur.fetchone()[0] == 5.0
    
    # Verify History
    cur = con.execute("SELECT * FROM inventory_counts WHERE item_name='Milk'")
    row = cur.fetchone()
    assert row is not None
    assert row[2] == 5.0 # quantity
    assert row[3] == 2.0 # previous
    assert row[4] == 3.0 # variance (5-2)
    con.close()

def test_inventory_value():
    inventory.create_item({"name": "A", "quantity": 10, "cost": 2.0}) # 20
    inventory.create_item({"name": "B", "quantity": 5, "cost": 3.0})  # 15
    
    val = inventory.get_inventory_value()
    assert val == 35.0
