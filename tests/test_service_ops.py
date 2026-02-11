import pytest
import os
from services import eighty_six, service_notes, memory, inventory

TEST_DB = "test_service_ops.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = eighty_six.DB_PATH = service_notes.DB_PATH = inventory.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = eighty_six.DB_PATH = service_notes.DB_PATH = inventory.DB_PATH = original_db_path

# ── 86 Board Tests ──
def test_eighty_six_item():
    res = eighty_six.eighty_six_item("Halibut", "Out of stock", "Cod", "Chef")
    assert "86'd" in res
    
    active = eighty_six.get_active_86()
    assert len(active) == 1
    assert active[0]["item_name"] == "Halibut"
    assert active[0]["substitution"] == "Cod"

def test_resolve_86():
    eighty_six.eighty_six_item("Avocado", "Out of stock")
    active = eighty_six.get_active_86()
    item_id = active[0]["id"]
    
    eighty_six.resolve_86(item_id)
    active_after = eighty_six.get_active_86()
    assert len(active_after) == 0

def test_substitution_lookup():
    eighty_six.eighty_six_item("Salmon", "Out of stock", "Trout")
    sub = eighty_six.get_substitution("Salmon")
    assert sub == "Trout"

# ── Service Notes Tests ──
def test_create_service_note():
    data = {
        "service_date": "2026-02-10",
        "shift": "Dinner",
        "covers": 142,
        "notes": "Busy night",
        "highlights": "New dessert was a hit",
        "issues": "Ran low on halibut",
        "action_items": "Order more fish"
    }
    res = service_notes.create_service_note(data)
    assert "saved" in res
    
    notes = service_notes.get_service_notes()
    assert len(notes) == 1
    assert notes[0]["covers"] == 142
    assert notes[0]["shift"] == "Dinner"
