import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from unittest.mock import MagicMock

# Mock heavy modules
sys.modules["services.rag"] = MagicMock()
sys.modules["services.rag.RAGEngine"] = MagicMock()

import services.autonomy
import services.dashboard_api
import services.commands
import services.bot

print("Imports successful!")
