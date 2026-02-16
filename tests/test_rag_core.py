import pytest
from unittest.mock import MagicMock, patch
from services import rag

def test_normalize_knowledge_tier_debug():
    print(f"DEBUG: TIER_ALIASES keys: {list(rag.TIER_ALIASES.keys())}")
    assert rag.normalize_knowledge_tier("tier1") == rag.TIER_1_RECIPE_OPS
    assert rag.normalize_knowledge_tier("TIER1") == rag.TIER_1_RECIPE_OPS

def test_smart_chunker_initialization():
    chunker = rag.SmartChunker(target_size=500, overlap=50)
    assert chunker.target_size == 500
    assert chunker.overlap == 50

@patch("services.rag.chromadb.PersistentClient")
@patch("services.rag.SentenceTransformer")
def test_rag_engine_initialization(mock_transformer, mock_client):
    engine = rag.RAGEngine()
    assert engine.chroma_client is not None
    assert engine.collection is not None
    assert engine.embedding_model is not None

@patch("services.rag.chromadb.PersistentClient")
@patch("services.rag.SentenceTransformer")
@patch("services.rag.load_runtime_config")
def test_rag_search_filters(mock_config, mock_transformer, mock_client):
    # Setup mocks
    mock_collection = MagicMock()
    mock_client.return_value.get_or_create_collection.return_value = mock_collection
    
    # Mock config to avoid runtime errors
    mock_config.return_value = {"rag": {}}
    
    engine = rag.RAGEngine()
    
    # Mock _load_sources method on the instance
    test_sources = [
        {"source_name": "src1", "status": "active", "collection_name": rag.COLLECTION_NAME, "knowledge_tier": rag.TIER_1_RECIPE_OPS},
        {"source_name": "src2", "status": "active", "collection_name": rag.COLLECTION_NAME, "knowledge_tier": rag.TIER_3_REFERENCE_THEORY},
        {"source_name": "inactive", "status": "inactive", "collection_name": rag.COLLECTION_NAME},
    ]
    engine._load_sources = MagicMock(return_value=test_sources)
    
    # Mock query return value to prevent fallback query
    mock_collection.query.return_value = {
        "ids": [["1"]],
        "documents": [["doc"]],
        "metadatas": [[{"source": "src1", "chunk_id": 0}]],
        "distances": [[0.1]],
    }
    
    # search with Tier 1
    engine.search("query", source_tiers=[rag.TIER_1_RECIPE_OPS])
    
    # Verify query call usage
    mock_collection.query.assert_called()
    
    # Get args from the FIRST call (which should have the filter)
    # If fallback logic runs, there might be multiple calls, so we check the first one specifically
    call_args = mock_collection.query.call_args_list[0]
    _, kwargs = call_args
    where_clause = kwargs.get("where", {})
    
    # Should check that "source" is in "src1" only (since src2 is tier 3 and inactive is inactive)
    assert "source" in where_clause
    assert where_clause["source"] == {"$in": ["src1"]}

