import os
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from services import kitchen_ops as ops
from services.memory import init_db

def test_kitchen_ops():
    print("Initializing DB...")
    init_db()
    
    print("\n--- Testing Inventory ---")
    print(ops.update_inventory("Onions", 10.0, "kg"))
    print(ops.update_inventory("Carrots", 5.0, "kg"))
    
    inv = ops.get_inventory_status()
    print(f"Inventory: {inv}")
    assert len(inv) >= 2
    
    print(ops.update_inventory("Onions", 8.0, "kg")) # Update
    inv_onions = ops.get_inventory_status("Onions")
    print(f"Onions: {inv_onions}")
    assert inv_onions[0]['quantity'] == 8.0
    
    print("\n--- Testing Waste ---")
    print(ops.log_waste("Onions", 2.0, "Rotten", "Chef Test"))
    inv_onions = ops.get_inventory_status("Onions")
    print(f"Onions after waste: {inv_onions}")
    assert inv_onions[0]['quantity'] == 6.0 # 8 - 2
    
    print("\n--- Testing Prep ---")
    print(ops.add_prep_task("Chop onions", "Chef Test"))
    print(ops.add_prep_task("Peel carrots", "Chef Test"))
    
    prep = ops.get_prep_list()
    print(f"Prep List: {prep}")
    assert len(prep) >= 2
    
    task_id = prep[0]['id']
    print(ops.complete_prep_task(task_id))
    
    prep_todo = ops.get_prep_list('todo')
    print(f"Prep Todo: {prep_todo}")
    assert len(prep_todo) == len(prep) - 1
    
    print("\n--- Testing Order Guide ---")
    print(ops.add_to_order_guide("Garlic", 1.0, "kg", "Chef Test"))
    guide = ops.get_order_guide()
    print(f"Order Guide: {guide}")
    assert len(guide) >= 1
    
    print(ops.clear_order_guide())
    guide = ops.get_order_guide()
    assert len(guide) == 0
    
    print("\n--- Testing 86 ---")
    print(ops.set_item_unavailable("Carrots"))
    inv_carrots = ops.get_inventory_status("Carrots")
    print(f"Carrots 86'd: {inv_carrots}")
    assert inv_carrots[0]['quantity'] == 0

    print("\nALL TESTS PASSED")

if __name__ == "__main__":
    test_kitchen_ops()
