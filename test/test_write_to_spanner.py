import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime

import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)


from pipeline import WriteToSpannerFn

class TestWritetoSpanner(unittest.TestCase):
    
    @patch("pipeline.spanner")
    def test_write_to_spanner(self, mock_spanner):
        mock_batch = MagicMock()
        mock_db = MagicMock()
        mock_db.batch.return_value.__enter__.return_value = mock_batch
        mock_spanner.Client.return_value.instance.return_value.database.return_value = mock_db

        fn = WriteToSpannerFn("test-project", "test-instance", "test-database")
        fn.setup()

        grouped_data=(
            "CUST100000",
            [
            ("Customers", {
            'customer_id': 'CUST100000',
            'first_name': 'Danielle',
            'last_name': 'Johnson',
            'date_of_birth': '1971-12-01',
            'gender': 'Male',
            'email': 'danielle.johnson@example.com',
            'phone': '3321819600',
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Address", {
            'address_id': 'c9a98540ddd045d4',
            'customer_id': 'CUST100000',
            'street': '123 Main St',
            'city': 'Springfield',
            'state': 'IL',
            'postal_code': '62704',
            'country': 'USA',
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Accounts", {
            'account_id': 'ACC12345',
            'customer_id': 'CUST100000',
            'status': 'Active',
            'start_date': '2023-01-01',
            'balance': 100.0,
            'last_payment_date': '2025-04-01',
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Plans", {
            'customer_id': 'CUST100000',
            'account_id': 'ACC12345',
            'plan_id': 'PLAN678',
            'plan_name': 'Unlimited Plus',
            'plan_type': 'Postpaid',
            'price_per_month': 49.99,
            'data_limit_gb': 100,
            'call_minutes': 1000,
            'sms_limit': 500,
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Usage", {
            'customer_id': 'CUST100000',
            'account_id': 'ACC12345',
            'plan_id': 'PLAN678',
            'period': '2025-04',
            'data_used_gb': 25.3,
            'call_minutes_spent': 450,
            'sms_sent': 230,
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Billing", {
            'customer_id': 'CUST100000',
            'account_id': 'ACC12345',
            'billing_id': 'BILL789',
            'billing_period': '2025-04',
            'total_amount': 52.49,
            'due_date': '2025-05-05',
            'payment_status': 'Pending',
            'payment_method': 'Credit Card',
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Devices", {
            'customer_id': 'CUST100000',
            'account_id': 'ACC12345',
            'device_id': 'DEV9876',
            'brand': 'Samsung',
            'model': 'Galaxy S21',
            'sim_number': 'SIM112233',
            'status': 'Active',
            'created_at': '2025-04-21T12:00:00Z'
        }),
        ("Tickets", {
            'customer_id': 'CUST100000',
            'account_id': 'ACC12345',
            'ticket_id': 'TICKET555',
            'issue_type': 'Billing',
            'description': 'Overcharged for April',
            'status': 'Resolved',
            'created_at': '2025-04-21T12:00:00Z',
            'resolved_at': '2025-04-22T15:30:00Z'
                 })
            ]
        )

        fn.process(grouped_data)

        self.assertEqual(mock_batch.insert_or_update.call_count, 8)

        mock_batch.insert_or_update.assert_any_call(
            table="Customers",
            columns=["customer_id", "first_name", "last_name", "date_of_birth", "gender", "email", "phone"],
            values=[["CUST100000", "Danielle", "Johnson", "1971-12-01", "Male", "danielle.johnson@example.com", "3321819600"]]
        )

        mock_batch.insert_or_update.assert_any_call(
            table="Accounts",
            columns=["account_id", "customer_id", "status", "start_date", "balance", "last_payment_date"],
            values=[["ACC12345", "CUST100000", "Active", "2023-01-01", 100.0, "2025-04-01"]]
        )

    def test_spanner_insert_error(self):
        mock_db = MagicMock()
        mock_batch = MagicMock()
        mock_batch.insert_or_update.side_effect = Exception("Insert failure")
        mock_db.batch.return_value.__enter__.return_value = mock_batch

        with patch("pipeline.spanner") as mock_spanner, patch("logging.error") as mock_logger:
            mock_spanner.Client.return_value.instance.return_value.database.return_value = mock_db
            fn = WriteToSpannerFn("test-project", "test-instance", "test-database")
            fn.setup()
            fn.database = mock_db

            grouped_input = (
                "CUST_FAIL",
                [("Customers", {
                    "customer_id": "CUST_FAIL",
                    "first_name": "John",
                    "created_at": "2025-04-21T12:00:00Z"
                })]
            )

            fn.process(grouped_input)
            mock_logger.assert_called()
    

    @patch("pipeline.spanner")
    def test_empty_input(self,mock_spanner):
            mock_db=MagicMock()
            mock_batch=MagicMock()
            mock_db.return_value.__enter__.return_value=mock_batch

            mock_spanner.Client.return_value.instance.return_value.database.return_value = mock_db


            fn = WriteToSpannerFn("test-project", "test-instance", "test-database")
            fn.setup()
            fn.database = mock_db

            grouped_input = ("CUST_EMPTY", [])     # Empty  Input - No records

        
            fn.process(grouped_input)

        
            mock_batch.insert_or_update.assert_not_called()

    
if __name__ == "__main__":
    unittest.main()