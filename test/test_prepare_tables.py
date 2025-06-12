import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that,equal_to
from apache_beam.testing.test_stream import TestStream
from unittest.mock import patch
import uuid


import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)




from pipeline import (
PrepareCustomersTable,
PrepareAddressTable,
PrepareAccountsTable,
PreparePlansTable,
PrepareUsageTable,
PrepareBillingTable,
PrepareDeviceTable,
PrepareTicketsTable)


customer_record = {
    "customer_id": "CUST100000",
    "personal_info": {
        "first_name": "Danielle",
        "last_name": "Johnson",
        "date_of_birth": "1971-12-01",
        "gender": "Male",
        "email": "danielle.johnson@example.com",
        "phone": "3321819600",
        "address": {
            "street": "386 Shane Harbors",
            "city": "Port Lindachester",
            "state": "KY",
            "zip_code": "20880",
            "country": "USA"
        }
    },
    "account_info": {
        "account_id": "ACC12345",
        "status": "Active",
        "start_date": "2023-01-01",
        "balance": 150.75,
        "last_payment_date": "2025-04-20"
    },
    "plan_details": {
        "plan_id": "PLAN678",
        "name": "Unlimited Talk & Data",
        "type": "Premium",
        "price_per_month": 79.99,
        "data_limit_gb": 100,
        "call_minutes": 1000,
        "sms_limit": 1000
    },
    "usage": {
        "period": "2025-04",
        "data_used_gb": 23.4,
        "call_minutes_used": 450,
        "sms_sent": 120
    },
    "billing": {
        "billing_id": "BILL999",
        "billing_period": "2025-04",
        "total_amount": 79.99,
        "due_date": "2025-05-05",
        "payment_status": "Paid",
        "payment_method": "Credit Card"
    },
    "devices": [
        {
            "device_id": "DEV1001",
            "brand": "BrandX",
            "model": "XPhone 11",
            "sim_number": "SIM1234567890",
            "status": "Active"
        }
    ],
    "support_tickets": [
        {
            "ticket_id": "TICK100",
            "issue_type": "Network",
            "description": "No service in area",
            "status": "Resolved",
            "created_at": "2025-04-01T10:00:00Z",
            "resolved_at": "2025-04-03T15:00:00Z"
        }
    ],
    "created_at": "2025-04-21T12:00:00Z"
}




class TestPrepareTables(unittest.TestCase):

    def _run_test(self, dofn, expected):
        data=[customer_record]
        with TestPipeline() as p:
            output = (
                p | beam.Create(data)
                  | beam.ParDo(dofn)
            )
            assert_that(output, equal_to(expected))


    def test_prepare_customers(self):
        expected = [("Customers", {
            'customer_id': 'CUST100000',
            'first_name': 'Danielle',
            'last_name': 'Johnson',
            'date_of_birth': '1971-12-01',
            'gender': 'Male',
            'email': 'danielle.johnson@example.com',
            'phone': '3321819600',
            'created_at': '2025-04-21T12:00:00Z'
        })]
        self._run_test(PrepareCustomersTable(), expected)

    @patch('pipeline.uuid.uuid4')
    def test_prepare_address_table(self,mock_uuid):
            address_data = customer_record['personal_info']['address']
            mock_uuid.return_value = uuid.UUID("12345678-1234-5678-1234-567812345678")
            expected = [("Address", {
                'address_id': '1234567812345678',  
                'customer_id': 'CUST100000',
                'street': address_data['street'],
                'city': address_data['city'],
                'state': address_data['state'],
                'postal_code': address_data['zip_code'],
                'country': address_data['country'],
                'created_at': '2025-04-21T12:00:00Z'
            })]
            self._run_test(PrepareAddressTable(), expected)

    def test_prepare_accounts_table(self):
            account_info = customer_record['account_info']
            expected = [("Accounts", {
                'account_id': account_info['account_id'],
                'customer_id': 'CUST100000',
                'status': account_info['status'],
                'start_date': account_info['start_date'],
                'balance': account_info['balance'],
                'last_payment_date': account_info['last_payment_date'],
                'created_at': '2025-04-21T12:00:00Z'
            })]
            self._run_test(PrepareAccountsTable(), expected)


    def test_prepare_plans_table(self):
            plan = customer_record['plan_details']
            expected = [("Plans", {
                'customer_id': 'CUST100000',
                'account_id': 'ACC12345',
                'plan_id': plan['plan_id'],
                'plan_name': plan['name'],
                'plan_type': plan['type'],
                'price_per_month': plan['price_per_month'],
                'data_limit_gb': plan['data_limit_gb'],
                'call_minutes': plan['call_minutes'],
                'sms_limit': plan['sms_limit'],
                'created_at': '2025-04-21T12:00:00Z'
            })]
            self._run_test(PreparePlansTable(), expected)



    def test_prepare_usage_table(self):
            usage = customer_record['usage']
            expected = [("Usage", {
                'customer_id': 'CUST100000',
                'account_id': 'ACC12345',
                'plan_id': 'PLAN678',
                'period': usage['period'],
                'data_used_gb': usage['data_used_gb'],
                'call_minutes_spent': usage['call_minutes_used'],
                'sms_sent': usage['sms_sent'],
                'created_at': '2025-04-21T12:00:00Z'
            })]
            self._run_test(PrepareUsageTable(), expected)



    def test_prepare_billing_table(self):
            billing = customer_record['billing']
            expected = [("Billing", {
                'customer_id': 'CUST100000',
                'account_id': 'ACC12345',
                'billing_id': billing['billing_id'],
                'billing_period': billing['billing_period'],
                'total_amount': billing['total_amount'],
                'due_date': billing['due_date'],
                'payment_status': billing['payment_status'],
                'payment_method': billing['payment_method'],
                'created_at': '2025-04-21T12:00:00Z'
            })]
            self._run_test(PrepareBillingTable(), expected)

    def test_prepare_devices_table(self):
            device = customer_record['devices'][0]
            expected = [("Devices", {
                'customer_id': 'CUST100000',
                'account_id': 'ACC12345',
                'device_id': device['device_id'],
                'brand': device['brand'],
                'model': device['model'],
                'sim_number': device['sim_number'],
                'status': device['status'],
                'created_at': '2025-04-21T12:00:00Z'
            })]
            self._run_test(PrepareDeviceTable(), expected)

    def test_prepare_tickets_table(self):
            ticket = customer_record['support_tickets'][0]
            expected = [("Tickets", {
                'customer_id': 'CUST100000',
                'account_id': 'ACC12345',
                'ticket_id': ticket['ticket_id'],
                'issue_type': ticket['issue_type'],
                'description': ticket['description'],
                'status': ticket['status'],
                'created_at': '2025-04-21T12:00:00Z',  
                'resolved_at': ticket['resolved_at']
            })]
            self._run_test(PrepareTicketsTable(), expected)


if __name__=='__main__':
    unittest.main()








































        
