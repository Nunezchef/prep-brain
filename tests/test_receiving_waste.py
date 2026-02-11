import pytest
import os
from services import receiving, waste, memory, inventory

TEST_DB = "test_receiving_waste.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = receiving.DB_PATH = waste.DB_PATH = inventory.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = receiving.DB_PATH = waste.DB_PATH = inventory.DB_PATH = original_db_path

def test_log_receiving():
    data = {
        "item_name": "Tomatoes",
        "quantity_received": 50,
        "unit": "lbs",
        "unit_cost": 2.50,
        "total_cost": 125.0,
        "quality_ok": 1,
        "received_by": "Chef"
    }
    res = receiving.log_receiving(data)
    assert "Received" in res
    
    history = receiving.get_receiving_history()
    assert len(history) == 1
    assert history[0]["item_name"] == "Tomatoes"
    assert history[0]["total_cost"] == 125.0

def test_invoice_summary():
    receiving.log_receiving({"item_name": "Flour", "invoice_number": "INV-001", "total_cost": 50.0})
    receiving.log_receiving({"item_name": "Sugar", "invoice_number": "INV-001", "total_cost": 30.0})
    
    summary = receiving.get_invoice_summary("INV-001")
    assert summary["item_count"] == 2
    assert summary["total"] == 80.0

def test_log_waste():
    data = {
        "item_name": "Lettuce",
        "quantity": 5,
        "unit": "heads",
        "reason": "Expired",
        "category": "Raw",
        "dollar_value": 12.50
    }
    res = waste.log_waste(data)
    assert "Waste logged" in res
    
    history = waste.get_waste_history()
    assert len(history) == 1

def test_waste_summary():
    waste.log_waste({"item_name": "Bread", "quantity": 2, "reason": "Expired", "dollar_value": 5.0})
    waste.log_waste({"item_name": "Milk", "quantity": 1, "reason": "Spoiled", "dollar_value": 3.0})
    waste.log_waste({"item_name": "Rolls", "quantity": 10, "reason": "Expired", "dollar_value": 8.0})
    
    summary = waste.get_waste_summary()
    assert summary["grand_total"] == 16.0
    
    expired = [r for r in summary["by_reason"] if r["reason"] == "Expired"]
    assert expired[0]["total_value"] == 13.0
