import pytest
import os
import sqlite3
from datetime import date
from services import schedule, prep, recipes, memory, inventory

TEST_DB = "test_schedule.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = prep.DB_PATH = schedule.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = recipes.DB_PATH = inventory.DB_PATH = prep.DB_PATH = schedule.DB_PATH = original_db_path

def test_staff_management():
    res = schedule.create_staff("Chef Mike", "Chef")
    assert "created" in res
    
    staff = schedule.get_active_staff()
    assert len(staff) == 1
    assert staff[0]["name"] == "Chef Mike"

def test_task_assignment():
    # 1. Setup Data
    schedule.create_staff("Sous Chef", "Sous")
    staff_id = schedule.get_active_staff()[0]["id"]
    
    recipes.create_recipe({"name": "Stock", "yield_unit": "L"}, [])
    r_id = recipes.get_all_recipes()[0]["id"]
    prep.set_recipe_par(r_id, 10.0) # Need 10
    
    prep.generate_prep_list()
    items = prep.get_prep_list('todo')
    item_id = items[0]["id"]
    
    # 2. Assign
    today = date.today().strftime("%Y-%m-%d")
    res = schedule.assign_prep_item(item_id, staff_id, today, "AM")
    assert "assigned" in res
    
    # 3. Verify Schedule
    sched = schedule.get_schedule(today)
    assert len(sched) == 1
    assert sched[0]["staff_name"] == "Sous Chef"
    assert sched[0]["recipe_name"] == "Stock"
    
    # 4. Unassign
    assign_id = sched[0]["id"]
    schedule.unassign_item(assign_id)
    sched_after = schedule.get_schedule(today)
    assert len(sched_after) == 0
