import pytest
import os
from services import inventory, memory

TEST_DB = "test_inventory.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = inventory.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = inventory.DB_PATH = original_db_path

def test_create_and_get_areas():
    inventory.create_area("Walk-in", 1)
    inventory.create_area("Freezer", 2)
    
    areas = inventory.get_areas()
    assert len(areas) == 2
    assert areas[0]['name'] == "Walk-in"
    assert areas[1]['name'] == "Freezer"

def test_create_item_in_area():
    inventory.create_area("Dry Storage")
    areas = inventory.get_areas()
    area_id = areas[0]['id']
    
    inventory.create_item({
        "name": "Flour",
        "quantity": 10,
        "storage_area_id": area_id
    })
    
    sheet = inventory.get_sheet_data()
    assert "Dry Storage" in sheet
    assert len(sheet["Dry Storage"]) == 1
    assert sheet["Dry Storage"][0]['name'] == "Flour"

def test_unassigned_items():
    inventory.create_item({"name": "Mystery Meat"})
    
    sheet = inventory.get_sheet_data()
    assert "Unassigned" in sheet
    assert len(sheet["Unassigned"]) == 1
    assert sheet["Unassigned"][0]['name'] == "Mystery Meat"
