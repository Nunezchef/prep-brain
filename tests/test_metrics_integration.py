import unittest
from unittest.mock import MagicMock, patch
import time
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from services import metrics, brain, command_runner, rag, autonomy

class TestMetricsIntegration(unittest.TestCase):
    def setUp(self):
        # Reset metrics for each test
        # metrics.metrics = metrics.MetricsCollector() # Re-instantiate if needed, or just clear.
        # But metrics module instantiates a global 'metrics' object.
        # We can just mock the record methods on the module if we want strict unit testing, 
        # or we can check the actual metrics object state.
        # Given we want to verification integration, let's mock the 'metrics' object in the services modules
        # or better yet, mocks the 'record_*' functions in the services.metrics module to verify calls.
        pass

    @patch('services.metrics.record_llm_call')
    @patch('services.brain.requests.post')
    @patch('services.brain.load_config')
    def test_brain_metrics(self, mock_load_config, mock_post, mock_record_llm):
        mock_load_config.return_value = {'ollama': {'url': 'http://mock', 'model': 'test-model'}}
        
        mock_response = MagicMock()
        mock_response.json.return_value = {"message": {"content": "Test response"}}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response
        
        brain.chat([("user", "hello")])
            
        self.assertTrue(mock_record_llm.called)
        # Check args
        args, kwargs = mock_record_llm.call_args
        self.assertEqual(kwargs['model'], "test-model")
        self.assertTrue(kwargs['success'])

    @patch('services.metrics.record_command')
    @patch('subprocess.run')
    def test_command_runner_metrics(self, mock_subprocess, mock_record_command):
        mock_subprocess.return_value = MagicMock(returncode=0)
        
        runner = command_runner.CommandRunner(allowed_commands=["ls"])
        runner.run(["ls", "-la"])
        
        self.assertTrue(mock_record_command.called)
        args, kwargs = mock_record_command.call_args
        self.assertEqual(kwargs['command'], "ls")
        self.assertTrue(kwargs['success'])

    @patch('services.metrics.record_rag_query')
    def test_rag_metrics(self, mock_record_rag):
        engine = rag.RAGEngine()
        # Mock internal methods to avoid real DB usage
        engine._load_sources = MagicMock(return_value=[
            {"source_name": "test_source", "status": "active", "collection_name": "prep_brain_knowledge"}
        ])
        engine.collection = MagicMock()
        engine.collection.count.return_value = 10
        engine.collection.query.return_value = {
            "documents": [["doc1"]], 
            "metadatas": [[{"source": "test_source"}]], 
            "distances": [[0.1]]
        }
        
        engine.search("test query")
        
        self.assertTrue(mock_record_rag.called)
        args, kwargs = mock_record_rag.call_args
        self.assertEqual(kwargs['query_type'], "search")

    @patch('services.metrics.record_autonomy_tick')
    async def test_autonomy_metrics(self, mock_record_tick):
        # Autonomy worker is async
        worker = autonomy.AutonomyWorker()
        
        # Mock internals
        worker._set_status = MagicMock()
        worker._refresh_status_queues = MagicMock()
        worker.process_ingest_jobs = MagicMock()
        # Async mocks are tricky, need AsyncMock if python 3.8+
        async def async_return(*args, **kwargs): return 0
        worker.process_ingest_jobs.side_effect = async_return
        
        # We also need to mock prep_list and memory to avoid DB calls
        with patch('services.autonomy.memory'), \
             patch('services.autonomy.prep_list'), \
             patch('services.autonomy.costing'):
                 
            await worker.run_background_tick()
            
        self.assertTrue(mock_record_tick.called)
        args, kwargs = mock_record_tick.call_args
        self.assertEqual(kwargs['action'], "tick")

if __name__ == '__main__':
    unittest.main()
