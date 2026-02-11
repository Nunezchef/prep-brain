import pytest
import os
from services import station_packs, recipes, memory, inventory, costing

TEST_DB = "test_station_packs.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = costing.DB_PATH = station_packs.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = costing.DB_PATH = station_packs.DB_PATH = original_db_path

def test_get_stations():
    recipes.create_recipe({"name": "Grilled Chicken", "station": "Grill", "yield_amount": 1, "yield_unit": "portion"}, [])
    recipes.create_recipe({"name": "Caesar Salad", "station": "Pantry", "yield_amount": 1, "yield_unit": "portion"}, [])
    
    stations = station_packs.get_stations()
    assert "Grill" in stations
    assert "Pantry" in stations

def test_station_pack():
    recipes.create_recipe(
        {"name": "Steak", "station": "Grill", "yield_amount": 1, "yield_unit": "portion", "method": "Sear 3 min each side."},
        [{"item_name_text": "Ribeye", "quantity": 1, "unit": "pc"}]
    )
    
    pack = station_packs.get_station_pack("Grill")
    assert pack["station"] == "Grill"
    assert len(pack["recipes"]) == 1
    assert pack["recipes"][0]["name"] == "Steak"

def test_training_card_generation():
    recipes.create_recipe(
        {"name": "Pasta", "station": "Sauté", "yield_amount": 4, "yield_unit": "portions", "method": "Boil and toss."},
        [{"item_name_text": "Spaghetti", "quantity": 500, "unit": "g"}]
    )
    r_id = recipes.get_all_recipes()[0]["id"]
    
    md = station_packs.generate_training_card_md(r_id)
    assert "# Pasta" in md
    assert "Spaghetti" in md
    assert "Boil and toss" in md
