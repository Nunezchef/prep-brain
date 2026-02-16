import pytest
from unittest.mock import patch, MagicMock
from services import brain

@pytest.fixture
def mock_config():
    return {
        "ollama": {"base_url": "http://test-ollama", "model": "test-model"},
        "rag": {"enabled": False},
        "system_prompt": "You are a test bot.",
        "response_style": "concise"
    }

@patch("services.brain.load_config")
def test_resolve_response_style(mock_load, mock_config):
    mock_load.return_value = mock_config
    
    # Default from config
    assert brain._resolve_response_style(mock_config, response_style=None, mode=None) == "concise"
    
    # Override via arg
    assert brain._resolve_response_style(mock_config, response_style="explain", mode=None) == "explain"
    
    # Override via mode
    assert brain._resolve_response_style(mock_config, response_style=None, mode="chef_kitchen") == "concise" # Default for this mode in brain.py if mapped
    
    # Test fallback - unknown style defaults to concise
    mock_config["response_style"] = "unknown_style"
    assert brain._resolve_response_style(mock_config, response_style=None, mode=None) == "concise"

def test_is_recipe_query():
    assert brain._is_recipe_query("how to make pasta") is True
    assert brain._is_recipe_query("recipe for cake") is True
    # "ingredients" and "method" are NOT in the regex patterns currently
    assert brain._is_recipe_query("ingredients for soup") is False
    assert brain._is_recipe_query("method for steak") is False
    assert brain._is_recipe_query("hello world") is False
    assert brain._is_recipe_query("who are you") is False

def test_is_citation_request():
    assert brain._is_citation_request("cite source") is True
    assert brain._is_citation_request("quote this") is True
    assert brain._is_citation_request("verbatim please") is True
    assert brain._is_citation_request("hello") is False

@patch("services.brain.requests.post")
@patch("services.brain.load_config")
def test_chat_simple_llm_call(mock_load, mock_post, mock_config):
    mock_load.return_value = mock_config
    
    # Mock LLM response
    mock_response = MagicMock()
    mock_response.json.return_value = {"message": {"content": "Hello human"}}
    mock_post.return_value = mock_response
    
    response = brain.chat([("user", "Hello")])
    
    assert response == "Hello human"
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert kwargs["json"]["model"] == "test-model"
    assert len(kwargs["json"]["messages"]) > 0

@patch("services.brain.requests.post")
@patch("services.brain.load_config")
def test_chat_with_rag_no_hits(mock_load, mock_post, mock_config):
    # Enable RAG
    mock_config["rag"]["enabled"] = True
    mock_load.return_value = mock_config
    
    # Patch rag_engine in services.rag because that's where brain imports it from
    with patch("services.rag.rag_engine") as mock_rag:
        mock_rag.search.return_value = []
        mock_rag.assemble_house_recipe.return_value = {"status": "none"}
        
        # Mock LLM response
        mock_response = MagicMock()
        mock_response.json.return_value = {"message": {"content": "General knowledge answer"}}
        mock_post.return_value = mock_response
        
        response = brain.chat([("user", "Tell me about history")])
        
        assert response == "General knowledge answer"
        # RAG search should have been called (via _retrieve_rag_results)
        mock_rag.search.assert_called()

@patch("services.brain.requests.post")
@patch("services.brain.load_config")
def test_chat_llm_error(mock_load, mock_post, mock_config):
    mock_load.return_value = mock_config
    
    # Mock error
    mock_post.side_effect = Exception("Ollama down")
    
    response = brain.chat([("user", "Hi")])
    
    assert "Error connecting to Brain: Ollama down" in response
