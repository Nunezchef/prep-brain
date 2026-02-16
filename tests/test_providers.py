import pytest
import sqlite3
import os
from pathlib import Path
from services import providers, memory

# Use a test database
TEST_DB = "test_memory.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    # Setup: Override DB path for testing
    original_db_path = memory.DB_PATH
    memory.DB_PATH = Path(TEST_DB)
    
    # Initialize DB (creates tables)
    memory.init_db()
    
    yield
    
    # Teardown: Remove test DB
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    # Restore DB path
    memory.DB_PATH = original_db_path

def test_create_vendor():
    data = {
        "name": "Test Vendor",
        "contact_name": "John Doe",
        "ordering_method": "Email"
    }
    result = providers.create_vendor(data)
    assert "successfully" in result
    
    vendors = providers.get_all_vendors()
    assert len(vendors) == 1
    assert vendors[0]['name'] == "Test Vendor"

def test_create_duplicate_vendor():
    data = {"name": "Test Vendor"}
    providers.create_vendor(data)
    result = providers.create_vendor(data)
    assert "already exists" in result

def test_update_vendor():
    providers.create_vendor({"name": "Old Name"})
    vendors = providers.get_all_vendors()
    vendor_id = vendors[0]['id']
    
    result = providers.update_vendor(vendor_id, {"name": "New Name", "notes": "Updated"})
    assert "successfully" in result
    
    updated_vendor = providers.get_vendor(vendor_id)
    assert updated_vendor['name'] == "New Name"
    assert updated_vendor['notes'] == "Updated"

def test_delete_vendor():
    providers.create_vendor({"name": "To Delete"})
    vendors = providers.get_all_vendors()
    vendor_id = vendors[0]['id']
    
    result = providers.delete_vendor(vendor_id)
    assert "successfully" in result
    
    assert len(providers.get_all_vendors()) == 0
