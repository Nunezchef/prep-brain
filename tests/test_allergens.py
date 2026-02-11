import pytest
import os
from services import allergens, recipes, memory, inventory

TEST_DB = "test_allergens.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = allergens.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = allergens.DB_PATH = original_db_path

def test_allergens_prepopulated():
    all_a = allergens.get_all_allergens()
    names = [a["name"] for a in all_a]
    assert "Milk" in names
    assert "Eggs" in names
    assert "Sesame" in names
    assert len(all_a) >= 9

def test_tag_recipe_allergens():
    recipes.create_recipe({"name": "Mac & Cheese", "station": "Pantry"}, [])
    r_id = recipes.get_all_recipes()[0]["id"]
    
    all_a = allergens.get_all_allergens()
    milk_id = [a for a in all_a if a["name"] == "Milk"][0]["id"]
    wheat_id = [a for a in all_a if a["name"] == "Wheat"][0]["id"]
    eggs_id = [a for a in all_a if a["name"] == "Eggs"][0]["id"]
    
    allergens.set_recipe_allergens(r_id, [milk_id, wheat_id, eggs_id])
    
    tagged = allergens.get_recipe_allergens(r_id)
    tag_names = [t["name"] for t in tagged]
    assert "Milk" in tag_names
    assert "Wheat" in tag_names
    assert "Eggs" in tag_names
    assert len(tagged) == 3

def test_allergen_matrix():
    recipes.create_recipe({"name": "Grilled Fish", "station": "Grill"}, [])
    r_id = recipes.get_all_recipes()[0]["id"]
    
    all_a = allergens.get_all_allergens()
    fish_id = [a for a in all_a if a["name"] == "Fish"][0]["id"]
    allergens.set_recipe_allergens(r_id, [fish_id])
    
    matrix = allergens.get_allergen_matrix()
    assert len(matrix["allergens"]) >= 9
    
    fish_recipe = [r for r in matrix["recipes"] if r["name"] == "Grilled Fish"][0]
    assert fish_recipe["allergens"]["Fish"] == True
    assert fish_recipe["allergens"]["Milk"] == False
