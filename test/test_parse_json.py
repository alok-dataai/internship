import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that,equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.pvalue import TaggedOutput

import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)




import json
from pipeline import ParseJson

class TestParseJson(unittest.TestCase):
    def test_valid_json(self):
        record={
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
                        "account_id": "ACC200000",
                        "status": "Active",
                        "start_date": "2023-05-23",
                        "plan_id": "PLAN_BASIC",
                        "balance": 37.08,
                        "last_payment_date": "2024-12-18"
                      },
                      "plan_details": {
                        "plan_id": "PLAN_BASIC",
                        "name": "Basic",
                        "type": "Prepaid",
                        "price_per_month": 20.0,
                        "data_limit_gb": 5,
                        "call_minutes": 200,
                        "sms_limit": 100
                      },
                      "usage": {
                        "period": "2023-12-22",
                        "data_used_gb": 1.73,
                        "call_minutes_used": 150,
                        "sms_sent": 62
                      },
                      "billing": {
                        "billing_id": "BILL20250000",
                        "billing_period": "2023-12-22",
                        "total_amount": 18.46,
                        "due_date": "2024-01-11",
                        "payment_status": "Overdue",
                        "payment_method": "Credit Card"
                      },
                      "devices": [
                        {
                          "device_id": "IMEI7686579303",
                          "brand": "Apple",
                          "model": "Model 1",
                          "sim_number": "SIM1119540831",
                          "status": "Active"
                        }
                      ],
                      "support_tickets": [
                        {
                          "ticket_id": "TCKT3000",
                          "issue_type": "Call Quality",
                          "description": "Not receiving verification SMS.",
                          "status": "Open",
                          "created_at": "2023-12-22",
                          "resolved_at": "2023-12-31"
                        }
                      ],
                      "created_at": "2025-02-17T13:16:43Z"
                    }

        input_data=[json.dumps(record).encode("utf-8")]
    
        with TestPipeline() as p:
            parsed=(p
            | beam.Create(input_data)
            | beam.ParDo(ParseJson()).with_outputs("done",main="main")
            )

            assert_that(parsed.main,equal_to([record]),label="CheckMain")
            assert_that(parsed.done,equal_to([]),label="CheckDone")

    def test_done_signal(self):
        input_data=[b'{"done": true}']
        with TestPipeline() as p:
            parsed=(p
            | beam.Create(input_data)
            | beam.ParDo(ParseJson()).with_outputs("done",main="main")
            )
        
            assert_that(parsed.main,equal_to([]),label="CheckMainDone")
            assert_that(parsed.done,equal_to([True]),label="CheckDoneSignal")

    def test_invalid_json(self):
        input_data=[b'{"customer_id:']
        with TestPipeline() as p:
            parsed= (p
            | beam.Create(input_data)
            | beam.ParDo(ParseJson()).with_outputs("done",main="main")
            )
            

            assert_that(parsed.main,equal_to([]),label="CheckMainInvalid")
            assert_that(parsed.done,equal_to([]),label="CheckDoneInvalid")


if __name__=='__main__':
    unittest.main()