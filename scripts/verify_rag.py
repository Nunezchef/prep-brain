import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.rag import rag_engine

def test_rag():
    print("Testing RAG Engine...")
    
    # Create a dummy file
    test_file = "data/documents/test_rag.txt"
    os.makedirs("data/documents", exist_ok=True)
    with open(test_file, "w") as f:
        f.write("The secret ingredient for the special sauce is smoked paprika and maple syrup.\n")
        f.write("Prep for the lunch service begins at 10:00 AM sharp.\n")
    
    # Ingest
    print(f"Ingesting {test_file}...")
    success, msg = rag_engine.ingest_file(test_file)
    if not success:
        print(f"Ingestion failed: {msg}")
        return

    # Query
    print("Querying: 'What is the secret ingredient?'")
    results = rag_engine.query("What is the secret ingredient?", n_results=1)
    
    if results:
        print("Result found:")
        print(f"Content: {results[0]['content']}")
        print(f"Source: {results[0]['source']}")
    else:
        print("No results found.")

    # Cleanup
    rag_engine.clear_database()
    print("Database cleared.")
    os.remove(test_file)

if __name__ == "__main__":
    test_rag()
