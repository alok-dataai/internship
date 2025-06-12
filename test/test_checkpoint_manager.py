import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime,timezone


import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)


from pipeline import CheckpointManager

class TestCheckpointManager(unittest.TestCase):
    def setUp(self):
        self.project_id = "test-project"
        self.instance_id = "test-instance"
        self.database_id = "test-database"
        self.manager=CheckpointManager(self.project_id,self.instance_id,self.database_id)

    def test_get_checkpoint_default(self):
        default = self.manager.get_checkpoint("non_existing")
        self.assertEqual(default, datetime.min.replace(tzinfo=timezone.utc))

    def test_get_checkpoint_existing(self):
        test_time = datetime(2025, 5, 1, 12, 0, tzinfo=timezone.utc)
        self.manager.cached_timestamps["CUST10001"] = test_time
        self.assertEqual(self.manager.get_checkpoint("CUST10001"), test_time)

    @patch("pipeline.spanner")
    def test_update_checkpoint_update_new_timestamp(self,mock_spanner):
        mock_db=MagicMock()
        mock_batch=MagicMock()
        mock_spanner.Client.return_value.instance.return_value.database.return_value=mock_db
        mock_db.batch.return_value.__enter__.return_value=mock_batch

        manager = CheckpointManager(self.project_id, self.instance_id, self.database_id)
        manager.database = mock_db
        manager.cached_timestamps = {"CUST10001": datetime(2025, 1, 1, tzinfo=timezone.utc)}

        new_timestamp = datetime(2025, 1, 2, tzinfo=timezone.utc)
        manager.update_checkpoint("CUST10001", new_timestamp)

        mock_batch.insert_or_update.assert_called_once_with(
            table="DataIngestionCheckpoint",
            columns=["entity_name", "last_timestamp"],
            values=[("CUST10001", new_timestamp)]
        )
        self.assertEqual(manager.cached_timestamps["CUST10001"], new_timestamp)

    def test_update_checkpoint_skip_older_timestamp(self):
        manager = CheckpointManager(self.project_id, self.instance_id, self.database_id)
        manager.database = MagicMock()
        manager.cached_timestamps = {"entity_1": datetime(2025, 1, 5, tzinfo=timezone.utc)}

        # Older timestamp
        manager.update_checkpoint("entity_1", datetime(2025, 1, 1, tzinfo=timezone.utc))


        self.assertEqual(manager.cached_timestamps["entity_1"], datetime(2025, 1, 5, tzinfo=timezone.utc))
        manager.database.batch.return_value.__enter__.return_value.insert_or_update.assert_not_called()


    @patch("pipeline.spanner") 
    def test_load_timestamps(self, mock_spanner):
        mock_snapshot = MagicMock()
        mock_snapshot.execute_sql.return_value = [
            ("CUST10001", datetime(2025, 1, 1, tzinfo=timezone.utc)),
            ("CUST10002", datetime(2025, 1, 5, tzinfo=timezone.utc))
        ]

        mock_db = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot

        mock_spanner.Client.return_value.instance.return_value.database.return_value = mock_db

        manager = CheckpointManager(self.project_id, self.instance_id, self.database_id)
        manager.database = mock_db

        manager._load_timestamps()

        self.assertEqual(manager.cached_timestamps["CUST10001"], datetime(2025, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(manager.cached_timestamps["CUST10002"], datetime(2025, 1, 5, tzinfo=timezone.utc))

        



if __name__ == '__main__':
    unittest.main()

