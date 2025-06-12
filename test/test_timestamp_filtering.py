import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime

import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)


from pipeline import TimestampFilter 

class TestTimestampFilter(unittest.TestCase):

    def setUp(self):
        self.project_id = 'test-project'
        self.instance_id = 'test-instance'
        self.database_id = 'test-db'

    @patch('pipeline.CheckpointManager')  
    def test_check_new_record(self, MockCheckpointManager):
        mock_manager = MockCheckpointManager.return_value
        mock_manager.get_checkpoint.return_value = datetime.fromisoformat('2024-01-01T00:00:00+00:00')

        record = {
            'customer_id': 'CUST10001',
            'created_at': '2025-01-01T10:00:00Z'
        }

        fn = TimestampFilter(self.project_id, self.instance_id, self.database_id)
        fn.setup()
        fn.checkpoint_manager = mock_manager  

        output = list(fn.process(record))
        self.assertEqual(output, [record])
        mock_manager.update_checkpoint.assert_called_once()

    @patch('pipeline.CheckpointManager')
    def test_skip_old_record(self, MockCheckpointManager):
        mock_manager = MockCheckpointManager.return_value
        mock_manager.get_checkpoint.return_value = datetime.fromisoformat('2025-01-01T10:00:00+00:00')

        record = {
            'customer_id': 'CUST10001',
            'created_at': '2025-01-01T09:59:59Z'
        }

        fn = TimestampFilter(self.project_id, self.instance_id, self.database_id)
        fn.setup()
        fn.checkpoint_manager = mock_manager

        output = list(fn.process(record))
        self.assertEqual(output, [])
        mock_manager.update_checkpoint.assert_not_called()

    @patch('pipeline.CheckpointManager')
    def test_invalid_timestamp(self, MockCheckpointManager):
        mock_manager = MockCheckpointManager.return_value
        mock_manager.get_checkpoint.return_value = datetime.fromisoformat('2025-01-01T10:00:00+00:00')

        record = {
            'customer_id': 'CUST10001',
            'created_at': '20342:43:22'    #Invalid Timestamp
        }

        fn = TimestampFilter(self.project_id, self.instance_id, self.database_id)
        fn.setup()
        fn.checkpoint_manager = mock_manager

        with self.assertLogs(level='ERROR') as cm:
            output = list(fn.process(record))
            self.assertIn("Error in timestamp filter", cm.output[0])
        self.assertEqual(output, [])


if __name__ == '__main__':
    unittest.main()

