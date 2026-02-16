import unittest
from unittest.mock import MagicMock, patch, ANY, mock_open
import sys
from pathlib import Path
import time
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from services import backup

class TestBackupService(unittest.TestCase):

    @patch('services.backup.load_config')
    @patch('services.backup.resolve_path')
    def test_get_backup_dir(self, mock_resolve, mock_config):
        # Case 1: Configured directory
        mock_config.return_value = {'backup': {'directory': 'custom/backups'}}
        mock_path_obj = MagicMock(spec=Path)
        mock_resolve.return_value = mock_path_obj
        
        d = backup.get_backup_dir()
        self.assertEqual(d, mock_path_obj)
        mock_path_obj.mkdir.assert_called_with(parents=True, exist_ok=True)
        
        # Case 2: Default
        mock_config.return_value = {}
        # NOTE: DEFAULT_BACKUP_DIR is a global in backup.py, resolved at import time.
        # We can patch it or just check it calls mkdir on it.
        # Since we can't easily patch the global variable which is already resolved,
        # let's just patch Path.mkdir to be safe if we want to test default path which is real.
        with patch.object(Path, 'mkdir') as mock_mkdir:
            d = backup.get_backup_dir()
            self.assertEqual(d, backup.DEFAULT_BACKUP_DIR)
            mock_mkdir.assert_called()

    @patch('services.backup.get_db_path')
    @patch('services.backup.get_backup_dir')
    @patch('sqlite3.connect')
    @patch('gzip.open')
    @patch('shutil.copyfileobj')
    @patch('builtins.open', new_callable=mock_open)
    def test_create_backup(self, mock_file_open, mock_copy, mock_gzip, mock_connect, mock_get_dir, mock_get_db):
        mock_db_path = Path('/app/data/memory.db')
        mock_backup_dir = Path('/app/data/backups')
        mock_get_db.return_value = mock_db_path
        mock_get_dir.return_value = mock_backup_dir
        
        # Mock Path.stat().st_size for logging
        with patch.object(Path, 'stat') as mock_stat:
            mock_stat.return_value.st_size = 1024 * 1024 # 1MB

            # Mock unlink for temp file
            with patch.object(Path, 'unlink') as mock_unlink:
                 # Mock write_text for timestamp marker
                with patch.object(Path, 'write_text') as mock_write:
                    
                    ret_path = backup.create_backup(compress=True)
                    
                    # Verify DB backup called
                    self.assertTrue(mock_connect.called)
                    source_conn = mock_connect.return_value
                    self.assertTrue(source_conn.backup.called)
                    
                    # Verify compression
                    self.assertTrue(mock_gzip.called)
                    
                    # Verify marker updated
                    mock_write.assert_called()
                    
                    self.assertTrue(str(ret_path).endswith('.gz'))

    @patch('services.backup.get_backup_dir')
    def test_rotate_backups(self, mock_get_dir):
        mock_dir = MagicMock()
        mock_get_dir.return_value = mock_dir
        
        # Create mock file objects with different mtimes
        files = []
        for i in range(10):
            p = MagicMock(spec=Path)
            p.name = f"memory_backup_{i}.db"
            p.stat.return_value.st_mtime = 1000 + i # Increasing time
            files.append(p)
            
        # glob returns iterator
        mock_dir.glob.return_value = files
        
        # Keep 7, so 3 should be removed (the oldest ones: 0, 1, 2)
        # Note: rotate_backups sorts reverse=True (newest first). 
        # So indices 0-6 are kept, 7-9 are removed.
        # files[9] is newest (time 1009). files[0] is oldest (time 1000).
        # Sorted: [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
        # Keep 7: [9, 8, 7, 6, 5, 4, 3]
        # Remove: [2, 1, 0]
        
        removed_count = backup.rotate_backups(max_backups=7)
        
        self.assertEqual(removed_count, 3)
        # Verify unlink called on the 3 oldest
        files[0].unlink.assert_called()
        files[1].unlink.assert_called()
        files[2].unlink.assert_called()
        files[9].unlink.assert_not_called()

    @patch('services.backup.load_config')
    @patch('services.backup._get_last_backup_time')
    @patch('services.backup.create_backup')
    @patch('services.backup.rotate_backups')
    def test_run_backup_if_due(self, mock_rotate, mock_create, mock_get_last, mock_config):
        mock_config.return_value = {'backup': {'enabled': True, 'interval_hours': 24}}
        
        # Case 1: Not due
        mock_get_last.return_value = time.time() - 3600 # 1 hour ago
        
        ran = backup.run_backup_if_due()
        self.assertFalse(ran)
        mock_create.assert_not_called()
        
        # Case 2: Due
        mock_get_last.return_value = time.time() - (25 * 3600) # 25 hours ago
        
        ran = backup.run_backup_if_due()
        self.assertTrue(ran)
        mock_create.assert_called()
        mock_rotate.assert_called()

    @patch('services.backup.load_config')
    def test_run_backup_disabled(self, mock_config):
        mock_config.return_value = {'backup': {'enabled': False}}
        ran = backup.run_backup_if_due()
        self.assertFalse(ran)

if __name__ == '__main__':
    unittest.main()
