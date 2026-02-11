import pytest
import os
from services import chef_questions, memory, inventory

TEST_DB = "test_chef_questions.db"

@pytest.fixture(autouse=True)
def setup_teardown():
    original_db_path = memory.DB_PATH
    memory.DB_PATH = chef_questions.DB_PATH = inventory.DB_PATH = inventory.Path(TEST_DB)
    
    memory.init_db()
    
    yield
    
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    
    memory.DB_PATH = chef_questions.DB_PATH = inventory.DB_PATH = original_db_path

def test_create_question():
    res = chef_questions.create_question(
        "What is the par level for chicken stock?",
        "10 liters",
        "Operations"
    )
    assert "added" in res
    
    questions = chef_questions.get_all_questions()
    assert len(questions) == 1
    assert "chicken stock" in questions[0]["question"]

def test_update_result():
    chef_questions.create_question("How many vendors?", "5", "Vendors")
    q = chef_questions.get_all_questions()[0]
    
    chef_questions.update_test_result(q["id"], "Pass")
    
    updated = chef_questions.get_all_questions()[0]
    assert updated["last_result"] == "Pass"

def test_summary():
    chef_questions.create_question("Q1", "A1", "General")
    chef_questions.create_question("Q2", "A2", "General")
    chef_questions.create_question("Q3", "A3", "General")
    
    qs = chef_questions.get_all_questions()
    chef_questions.update_test_result(qs[0]["id"], "Pass")
    chef_questions.update_test_result(qs[1]["id"], "Fail")
    
    summary = chef_questions.get_test_summary()
    assert summary["total"] == 3
    assert summary["passed"] == 1
    assert summary["failed"] == 1
    assert summary["untested"] == 1
