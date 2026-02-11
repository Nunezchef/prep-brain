import pytest
import os
from services import order_guide, providers, memory

TEST_DB = "test_order_guide.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = providers.DB_PATH = order_guide.DB_PATH = order_guide.Path(TEST_DB)
    
    memory.init_db()
    
    # Create a dummy vendor
    providers.create_vendor({"name": "Test Vendor"})
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = providers.DB_PATH = order_guide.DB_PATH = original_db_path

def test_add_item():
    vendors = providers.get_all_vendors()
    vendor_id = vendors[0]['id']
    
    result = order_guide.add_item({
        "vendor_id": vendor_id,
        "item_name": "Carrots",
        "pack_size": "50lb",
        "price": 25.00
    })
    
    assert "successfully" in result
    
    items = order_guide.get_items_by_vendor(vendor_id)
    assert len(items) == 1
    assert items[0]['item_name'] == "Carrots"
    assert items[0]['price'] == 25.00

def test_add_duplicate_item():
    vendors = providers.get_all_vendors()
    vendor_id = vendors[0]['id']
    
    order_guide.add_item({"vendor_id": vendor_id, "item_name": "Carrots"})
    result = order_guide.add_item({"vendor_id": vendor_id, "item_name": "Carrots"})
    
    assert "already exists" in result

def test_update_item():
    vendors = providers.get_all_vendors()
    vendor_id = vendors[0]['id']
    order_guide.add_item({"vendor_id": vendor_id, "item_name": "Carrots"})
    
    items = order_guide.get_items_by_vendor(vendor_id)
    item_id = items[0]['id']
    
    result = order_guide.update_item(item_id, {"price": 30.00})
    assert "successfully" in result
    
    updated_items = order_guide.get_items_by_vendor(vendor_id)
    assert updated_items[0]['price'] == 30.00
