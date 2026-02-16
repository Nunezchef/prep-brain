import pytest
import sqlite3
import time
from services import soft_delete, memory, recipes, inventory, providers, menu

@pytest.fixture
def db_conn():
    # Setup in-memory DB or temporary file DB via memory service if possible, 
    # but memory service uses a specific path. 
    # For these tests, we rely on the memory service connecting to the test DB if configured,
    # or we can mock it.
    # However, since we are testing actual SQL updates, integration style with a temp DB is best.
    # Assuming the environment or conftest.py handles DB init for tests, or memory.get_db_path uses a test path.
    # We will assume standard pytest setup for this project.
    con = memory.get_conn()
    yield con
    con.close()

def setup_module(module):
    # Ensure tables exist
    memory.init_db()

def test_get_active_where_clause():
    assert soft_delete.get_active_where_clause() == "deleted_at IS NULL"
    assert soft_delete.get_active_where_clause("t") == "t.deleted_at IS NULL"

def test_soft_delete_flow(db_conn):
    # Create a dummy recipe
    recipe_data = {
        "name": "Test Soft Delete Recipe",
        "yield_amount": 1,
        "yield_unit": "portion",
        "station": "Test",
        "category": "Test",
        "method": "Test"
    }
    msg = recipes.create_recipe(recipe_data, [])
    assert "created successfully" in msg
    
    # Get ID
    cur = db_conn.execute("SELECT id FROM recipes WHERE name = ?", (recipe_data["name"],))
    row = cur.fetchone()
    assert row is not None
    recipe_id = row[0]
    
    # Verify it is in get_all_recipes
    all_recipes = recipes.get_all_recipes()
    assert any(r["id"] == recipe_id for r in all_recipes)
    
    # Soft Delete
    success = recipes.delete_recipe(recipe_id, "Tester")
    assert success is True
    
    # Verify it is NOT in get_all_recipes
    all_recipes = recipes.get_all_recipes()
    assert not any(r["id"] == recipe_id for r in all_recipes)
    
    # Verify it still exists in DB with deleted_at
    cur = db_conn.execute("SELECT deleted_at FROM recipes WHERE id = ?", (recipe_id,))
    row = cur.fetchone()
    assert row["deleted_at"] is not None
    
    # Restore
    success = soft_delete.restore("recipes", recipe_id, "Tester")
    assert success is True
    
    # Verify it is back in get_all_recipes
    all_recipes = recipes.get_all_recipes()
    assert any(r["id"] == recipe_id for r in all_recipes)

def test_soft_delete_inventory(db_conn):
    # Create Item
    item_name = "Soft Delete Item"
    inventory.create_item({"name": item_name, "quantity": 10, "unit": "kg"})
    
    cur = db_conn.execute("SELECT id FROM inventory_items WHERE name = ?", (item_name,))
    item_id = cur.fetchone()[0]
    
    # Delete
    assert inventory.delete_item(item_id) is True
    
    # Verify sheet data doesn't have it
    sheet = inventory.get_sheet_data()
    # Flatten sheet
    all_items = []
    for items in sheet.values():
        all_items.extend(items)
    
    assert not any(i["id"] == item_id for i in all_items)
    
    # Re-create (Upsert/Restore)
    inventory.create_item({"name": item_name, "quantity": 5, "unit": "kg"})
    
    # Check if restored (deleted_at should be NULL)
    cur = db_conn.execute("SELECT deleted_at, quantity FROM inventory_items WHERE id = ?", (item_id,))
    row = cur.fetchone()
    assert row["deleted_at"] is None
    assert row["quantity"] == 5

def test_soft_delete_vendor(db_conn):
    # Create Vendor
    vendor_name = "Soft Delete Vendor"
    providers.create_vendor({"name": vendor_name})
    
    cur = db_conn.execute("SELECT id FROM vendors WHERE name = ?", (vendor_name,))
    vendor_id = cur.fetchone()[0]
    
    # Verify get_all_vendors has it
    all_vendors = providers.get_all_vendors()
    assert any(v["id"] == vendor_id for v in all_vendors)
    
    # Delete
    assert "deleted successfully" in providers.delete_vendor(vendor_id)
    
    # Verify gone from get_all_vendors
    all_vendors = providers.get_all_vendors()
    assert not any(v["id"] == vendor_id for v in all_vendors)
    
    # Verify gone from get_vendor
    assert providers.get_vendor(vendor_id) is None

def test_soft_delete_menu(db_conn):
    # Create Menu Item
    menu_item = {"name": "SD Menu Item", "selling_price": 10}
    menu.create_menu_item(menu_item)
    
    cur = db_conn.execute("SELECT id FROM menu_items WHERE name = ?", (menu_item["name"],))
    menu_id = cur.fetchone()[0]
    
    # Delete
    assert menu.delete_menu_item(menu_id) is True
    
    # Verify gone
    items = menu.get_menu_items()
    assert not any(i["id"] == menu_id for i in items)
