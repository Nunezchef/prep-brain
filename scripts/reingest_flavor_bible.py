import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from services.rag import rag_engine, SmartChunker

def create_dummy_pdf(path):
    import fitz
    doc = fitz.open()
    page = doc.new_page()
    
    # Create fake "Flavor Bible" content
    content = ""
    for i in range(50):
        content += f"INGREDIENT {i}\n"
        content += "  • SEASON: Summer\n"
        content += "  • PAIRINGS: Salt, Pepper, Olive Oil\n"
        content += "  • AFFINITY: High\n\n"
    
    page.insert_text((50, 50), content, fontsize=12)
    doc.save(path)
    doc.close()
    return path

def verify():
    # 1. Create dummy file if real one doesn't exist
    target_file = Path("data/documents/the_flavor_bible.pdf")
    if not target_file.exists():
        print(f"Creating dummy file at {target_file}")
        target_file.parent.mkdir(parents=True, exist_ok=True)
        create_dummy_pdf(target_file)

    # 2. Ingest
    print(f"Ingesting {target_file}...")
    success, result = rag_engine.ingest_file(str(target_file))
    
    if success:
        print("✅ Ingestion Successful (Test Data)")
        print(f"Chunks: {result['num_chunks']} (Note: This is from a small dummy file, not the real Flavor Bible)")
        
        # 3. Query
        print("\nTesting Query...")
        res = rag_engine.query("What pairs with INGREDIENT 10?")
        for r in res:
            print(f"- {r['content'][:50]}... (Dist: {r['distance']:.3f})")
            
        if result['num_chunks'] > 10:
             print("\n✅ Smart Chunking working (chunk count > 10 for dummy, expected >100 for real)")
        else:
             print("\n⚠️ Low chunk count. Check logic.")
    else:
        print(f"❌ Ingestion Failed: {result}")

if __name__ == "__main__":
    verify()
