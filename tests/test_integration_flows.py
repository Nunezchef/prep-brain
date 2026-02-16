import unittest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from services import brain

class TestIntegrationFlows(unittest.TestCase):
    @patch('services.brain.requests.post')
    @patch('services.brain.load_config')
    @patch('services.rag.rag_engine')
    def test_brain_chat_flow_rag_and_llm(self, mock_rag_engine, mock_load_config, mock_post):
        # Setup Config
        mock_load_config.return_value = {
            'ollama': {'url': 'http://mock', 'model': 'test-model'},
            'rag': {'enabled': True}
        }
        
        # Setup RAG mock
        # Normal search - mocking the return value of search()
        # brain.py calls rag_engine.search(query_text=..., n_results=...)
        mock_rag_engine.search.return_value = [
            {'content': 'Recipe for tomato soup', 'source': 'cookbook', 'chunk_id': 1}
        ]
        
        # Setup LLM mock
        mock_response = MagicMock()
        mock_response.json.return_value = {"message": {"content": "Here is a recipe for tomato soup."}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response
        
        # Action
        response = brain.chat([("user", "How do I make tomato soup?")])
        
        # Assertions
        # 1. Check RAG was called
        mock_rag_engine.search.assert_called()
        
        # 2. Check LLM was called with context
        self.assertTrue(mock_post.called)
        args, kwargs = mock_post.call_args
        json_body = kwargs['json']
        messages = json_body['messages']
        
        # System prompt should contain RAG context
        system_msgs = [m for m in messages if m['role'] == 'system']
        found_context = False
        for m in system_msgs:
            if "Recipe for tomato soup" in m['content']:
                found_context = True
                break
        self.assertTrue(found_context, "RAG context not found in LLM system prompts")
        
        # 3. Check response
        self.assertEqual(response, "Here is a recipe for tomato soup.")

    @patch('services.brain.requests.post')
    @patch('services.brain.load_config')
    @patch('services.rag.rag_engine') 
    def test_brain_chat_house_recipe_assembly(self, mock_rag_engine, mock_load_config, mock_post):
        # Setup Config
        mock_load_config.return_value = {
            'rag': {'enabled': True, 'recipes': {'completeness_confidence_threshold': 0.75}}
        }
        
        # Mock assemble_house_recipe to return success
        # Note: assemble_house_recipe is imported inside brain.chat, but we patched services.rag.rag_engine
        # which should affect the module attribute.
        mock_rag_engine.assemble_house_recipe.return_value = {
            "status": "ok",
            "html": "<b>House Recipe HTML</b>",
            "confidence": 0.9
        }
        
        # Action - query that looks like a recipe request
        response = brain.chat([("user", "recipe for burger")]) # 'recipe' keyword
        
        # Assertions
        # Should return the HTML directly without calling LLM
        self.assertEqual(response, "<b>House Recipe HTML</b>")
        mock_post.assert_not_called()

if __name__ == '__main__':
    unittest.main()
