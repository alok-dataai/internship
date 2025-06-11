from google.cloud import spanner
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows,GlobalWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, AfterAll
from apache_beam.transforms.core import DoFn, Map
import json
import logging
import uuid
import time
from datetime import datetime,timezone


PROJECT_ID ="playground-s-11-1a274b22"
INSTANCE_ID = "spanner-instance-or"
DATABASE_ID = "telecom-db"
TOPIC = f"projects/{PROJECT_ID}/topics/raw-telecom-data"
REGION = "us-central1" 
TEMP_LOCATION = "gs://telecom-bucket-2323/temp"
STAGING_LOCATION = "gs://telecom-bucket-2323/staging"
CHECKPOINT_TABLE = "DataIngestionCheckpoint"

# Define the table insertion order
TABLE_ORDER = {
    "Customers": 1,
    "Address": 2,
    "Accounts": 3,
    "Plans": 4,
    "Usage": 5,
    "Billing": 6,
    "Devices": 7,
    "Tickets": 8
}


class CheckpointManager:

    
    def __init__(self, project_id, instance_id, database_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.client = None
        self.database = None
        self.cached_timestamps = {}
        
    def setup(self):
        self.client = spanner.Client(project=self.project_id)
        self.instance = self.client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)
        self._load_timestamps()
    
    def _load_timestamps(self):
        try:
            ddl = f"""
            CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
                entity_name STRING(255) NOT NULL,
                last_timestamp TIMESTAMP NOT NULL,
            ) PRIMARY KEY (entity_name)
            """
            
            operation = self.database.update_ddl([ddl])
            operation.result()  
            
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(f"SELECT entity_name, last_timestamp FROM {CHECKPOINT_TABLE}")
                for row in results:
                    self.cached_timestamps[row[0]] = row[1]
            logging.info(f"Loaded timestamps: {self.cached_timestamps}")
        except Exception as e:
            logging.error(f"Error initializing checkpoint table: {e}")
    
    def get_checkpoint(self, entity_name):
       
        return self.cached_timestamps.get(entity_name, datetime.min.replace(tzinfo=timezone.utc))
    
    def update_checkpoint(self, entity_name, timestamp):

        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        
        current = self.get_checkpoint(entity_name)
        if timestamp > current:
            self.cached_timestamps[entity_name] = timestamp
            try:
                with self.database.batch() as batch:
                    batch.insert_or_update(
                        table=CHECKPOINT_TABLE,
                        columns=["entity_name", "last_timestamp"],
                        values=[(entity_name, timestamp)]
                    )
                logging.info(f"Updated checkpoint for {entity_name} to {timestamp}")
            except Exception as e:
                logging.error(f"Error updating checkpoint: {e}")

class TimestampFilter(beam.DoFn):
    
    def __init__(self, project_id, instance_id, database_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.checkpoint_manager = None
    
    def setup(self):
        self.checkpoint_manager = CheckpointManager(self.project_id, self.instance_id, self.database_id)
        self.checkpoint_manager.setup()
    
    def process(self, element):
        try:
            created_at = element['created_at']
            if created_at.endswith('Z'):
                created_at = created_at.replace('Z', '+00:00')
                
            record_timestamp = datetime.fromisoformat(created_at)
            
            customer_id = element['customer_id']
            checkpoint_key = f"customer_{customer_id}"
            last_timestamp = self.checkpoint_manager.get_checkpoint(checkpoint_key)
            
            # Process only if the record is newer than the checkpoint
            if record_timestamp > last_timestamp:
                logging.info(f"Processing new record for {customer_id} with timestamp {record_timestamp}")
                
                # Update checkpoint with this record's timestamp
                self.checkpoint_manager.update_checkpoint(checkpoint_key, record_timestamp)
                
                yield element
            else:
                logging.info(f"Skipping old record for {customer_id}: {record_timestamp} <= {last_timestamp}")
        except Exception as e:
            logging.error(f"Error in timestamp filter: {e}")





class ParseJson(beam.DoFn):
    def process(self, element):
        try:
        
            if element.decode("utf-8").strip() == '{"done": true}':
                logging.info("Received done signal")
                yield beam.pvalue.TaggedOutput("done", True)
                return
                
            data = json.loads(element.decode("utf-8"))
            logging.info(f"Successfully parsed record: {data['customer_id']}")
            yield data
        except Exception as e:
            logging.error(f"Error parsing message: {e}")


class PrepareCustomersTable(beam.DoFn):
    def process(self, element):
        try:
            customer={
                'customer_id':element['customer_id'],
                'first_name':element['personal_info']['first_name'],
                'last_name':element['personal_info']['last_name'],
                'date_of_birth':element['personal_info']['date_of_birth'],
                'gender':element['personal_info']['gender'],
                'email':element['personal_info']['email'],
                'phone':element['personal_info']['phone'],
                'created_at':element['created_at']
            }
            yield ("Customers", customer)
        except Exception as e:
            logging.error(f"Error parsing customer data: {e}")

class PrepareAddressTable(beam.DoFn):
    def process(self, element):
        try:
            address={
                'address_id': str(uuid.uuid4()).replace("-", "")[:16] ,
                'customer_id':element['customer_id'],
                'street':element['personal_info']['address']['street'],
                'city':element['personal_info']['address']['city'],
                'state':element['personal_info']['address']['state'],
                'postal_code':element['personal_info']['address']['zip_code'],
                'country':element['personal_info']['address']['country'],
                'created_at':element['created_at']
            }
            yield ("Address", address)
        except Exception as e:
            logging.error(f"Error parsing address data: {e}")

class PrepareAccountsTable(beam.DoFn):
    def process(self, element):
        try:
            account={
                'account_id':element['account_info']['account_id'],
                'customer_id':element['customer_id'],
                'status':element['account_info']['status'],
                'start_date':element['account_info']['start_date'],
                'balance':element['account_info']['balance'],
                'last_payment_date':element['account_info']['last_payment_date'],
                'created_at':element['created_at']
            }
            yield ("Accounts", account)
        except Exception as e:
            logging.error(f"Error parsing account data: {e}")

class PreparePlansTable(beam.DoFn):
    def process(self, element):
        try:
            plan={
                'customer_id':element['customer_id'],
                'account_id':element['account_info']['account_id'],
                'plan_id':element['plan_details']['plan_id'],
                'plan_name':element['plan_details']['name'],
                'plan_type':element['plan_details']['type'],
                'price_per_month':element['plan_details']['price_per_month'],
                'data_limit_gb':element['plan_details']['data_limit_gb'],
                'call_minutes':element['plan_details']['call_minutes'],
                'sms_limit':element['plan_details']['sms_limit'],
                'created_at':element['created_at']
            }
            yield ("Plans", plan)
        except Exception as e:
            logging.error(f"Error parsing plan data: {e}")

class PrepareUsageTable(beam.DoFn):
    def process(self, element):
        try:
            usage={
             'customer_id':element['customer_id'],
             'account_id':element['account_info']['account_id'],
             'plan_id':element['plan_details']['plan_id'],
             'period':element['usage']['period'],
             'data_used_gb':element['usage']['data_used_gb'],
             'call_minutes_spent':element['usage']['call_minutes_used'],
             'sms_sent':element['usage']['sms_sent'],
             'created_at':element['created_at']
            }
            yield ("Usage", usage)
        except Exception as e:
            logging.error(f"Error parsing usage data: {e}")    

class PrepareBillingTable(beam.DoFn):
    def process(self, element):
        try:
            billing={
                'customer_id':element['customer_id'],
                'account_id':element['account_info']['account_id'],
                'billing_id':element['billing']['billing_id'],
                'billing_period':element['billing']['billing_period'],
                'total_amount':element['billing']['total_amount'],
                'due_date':element['billing']['due_date'],
                'payment_status':element['billing']['payment_status'],
                'payment_method':element['billing']['payment_method'],
                'created_at':element['created_at']
            }
            yield ("Billing", billing)
        except Exception as e:
            logging.error(f"Error parsing billing data: {e}")

class PrepareDeviceTable(beam.DoFn):
    def process(self, element):
        try:
            for device in element['devices']:
                device_data = {
                    'customer_id': element['customer_id'],
                    'account_id': element['account_info']['account_id'],
                    'device_id': device['device_id'],
                    'brand': device['brand'],
                    'model': device['model'],
                    'sim_number': device['sim_number'],
                    'status': device['status'],
                    'created_at':element['created_at']
                }
                yield ('Devices', device_data)
        except Exception as e:
            logging.error(f"Error preparing device data: {e}")
               
class PrepareTicketsTable(beam.DoFn):
    def process(self, element):
        try:
            for ticket in element['support_tickets']:
                ticket_data = {
                    'customer_id': element['customer_id'],
                    'account_id': element['account_info']['account_id'],
                    'ticket_id': ticket['ticket_id'],
                    'issue_type': ticket['issue_type'],
                    'description': ticket['description'],
                    'status': ticket['status'],
                    'created_at': ticket['created_at'],
                    'resolved_at': ticket['resolved_at'],
                    'created_at':element['created_at']
                }
                yield ('Tickets', ticket_data)
        except Exception as e:
            logging.error(f"Error preparing ticket data: {e}")


class WriteToSpannerFn(beam.DoFn):
    def __init__(self, project_id, instance_id, database_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
    
    def setup(self):
        self.client = spanner.Client(project=self.project_id)
        self.instance = self.client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)
    
    def process(self, element):
        customer_id, grouped_records = element
        
       
        table_data = {}
        for table_name, row in grouped_records:
            if table_name not in table_data:
                table_data[table_name] = []
            table_data[table_name].append(row)
        
     
        for table_name, order in sorted(TABLE_ORDER.items(), key=lambda x: x[1]):
            if table_name in table_data and table_data[table_name]:
                try:

                    sorted_rows = sorted(
                    table_data[table_name],
                    key=lambda r: datetime.fromisoformat(r['created_at'].replace("Z", ""))
                    )
                    with self.database.batch() as batch:
                        for row in sorted_rows:
                            row_copy = dict(row)
                            row_copy.pop('created_at', None)
                            batch.insert_or_update(
                                table=table_name,
                                columns=list(row_copy.keys()),
                                values=[list(row_copy.values())]
                            )
                    logging.info(f"Inserted {len(table_data[table_name])} rows into table '{table_name}' for customer {customer_id}")
                except Exception as e:
                    logging.error(f"Error inserting into table {table_name}: {e}")


class ForceDrainFn(beam.DoFn):
    def process(self, element):
        logging.info("Processing drain signal and terminating pipeline")
        yield beam.pvalue.TaggedOutput("drain_done", True)

def run():
    pipeline_options = PipelineOptions([
        f"--project={PROJECT_ID}",
        f"--runner=DirectRunner",  # Change to DataflowRunner for production
        f"--region={REGION}",
        f"--temp_location={TEMP_LOCATION}",
        f"--staging_location={STAGING_LOCATION}",
        "--streaming",
        "--requirements_file=requirements.txt",
        "--allow_unsafe_triggers"
    ])
    pipeline_options.view_as(StandardOptions).streaming = True
    
    with beam.Pipeline(options=pipeline_options) as p:
        messages = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(topic=TOPIC)
            | "ParseJson" >> beam.ParDo(ParseJson()).with_outputs("done", main="data")
        )

        data_records = messages.data     
        done_signal = messages.done

        # Filter records based on timestamp
        filtered_records = (
            data_records 
            | "FilterByTimestamp" >> beam.ParDo(TimestampFilter(PROJECT_ID, INSTANCE_ID, DATABASE_ID))
        )

        force_drain = done_signal | "ForceDrain" >> beam.ParDo(ForceDrainFn()).with_outputs("drain_done", main="drain")

        customer_data = filtered_records | "PrepareCustomersTable" >> beam.ParDo(PrepareCustomersTable())
        address_data = filtered_records | "PrepareAddressTable" >> beam.ParDo(PrepareAddressTable())
        account_data = filtered_records | "PrepareAccountsTable" >> beam.ParDo(PrepareAccountsTable())
        plan_data = filtered_records | "PreparePlansTable" >> beam.ParDo(PreparePlansTable())
        usage_data = filtered_records | "PrepareUsageTable" >> beam.ParDo(PrepareUsageTable())
        billing_data = filtered_records | "PrepareBillingTable" >> beam.ParDo(PrepareBillingTable())
        device_data = filtered_records | "PrepareDeviceTable" >> beam.ParDo(PrepareDeviceTable())
        ticket_data = filtered_records | "PrepareTicketsTable" >> beam.ParDo(PrepareTicketsTable())

        # Flatten all data
        all_data = (
            (customer_data, address_data, account_data, plan_data, 
             usage_data, billing_data, device_data, ticket_data)
            | "FlattenAllTables" >> beam.Flatten()
        )
        
    
        windowed_data = (
            all_data 
            | "ApplyWindow" >> beam.WindowInto(
                GlobalWindows(),
                trigger=AfterAll(
                    AfterProcessingTime(10),
                    AfterWatermark(early=AfterProcessingTime(5))
                ),
                accumulation_mode=AccumulationMode.DISCARDING
            )
        )
        

        keyed_by_customer = (
            windowed_data
            | "KeyByCustomerId" >> beam.Map(
                lambda table_row: (table_row[1].get('customer_id', str(uuid.uuid4())), table_row)
            )
            | "GroupByCustomerId" >> beam.GroupByKey()
        )
        
        keyed_by_customer | "WriteToSpanner" >> beam.ParDo(
            WriteToSpannerFn(PROJECT_ID, INSTANCE_ID, DATABASE_ID)
        )

        done_signal | "LogCompletion" >> beam.Map(
            lambda x: logging.info("Pipeline received done signal - flushing final data")
        )
        
        force_drain.drain_done | "FinalizeAndTerminate" >> beam.Map(
            lambda x: logging.info("All data processed and pipeline terminating")
        )
        
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()