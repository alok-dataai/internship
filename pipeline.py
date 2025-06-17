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
import google.auth
from datetime import datetime,timezone
import yaml



_,PROJECT_ID =google.auth.default()
INSTANCE_ID = "spanner-instance-or"
DATABASE_ID = "telecom-db"
TOPIC = f"projects/{PROJECT_ID}/topics/raw-telecom-data"
REGION = "us-central1" 
TEMP_LOCATION = "gs://telecom-bucket-2356/temp"
STAGING_LOCATION = "gs://telecom-bucket-2356/staging"
CHECKPOINT_TABLE = "DataIngestionCheckpoint"



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




def get_nested_value(obj, path):
    try:
        for part in path.split('.'):
            if isinstance(obj, list):
                obj = obj[0]  
            elif isinstance(obj, dict):
                obj = obj.get(part)
            else:
                return None
        return obj
    except Exception:
        return None


class TimestampFilter(beam.DoFn):

    def __init__(self, project_id, instance_id, database_id, yaml_config):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.yaml_config = yaml_config
        self.checkpoint_manager = None
        self.table_config = []

    def setup(self):
        self.checkpoint_manager = CheckpointManager(self.project_id, self.instance_id, self.database_id)
        self.checkpoint_manager.setup()

        for table in self.yaml_config.get("tables", []):
            if table.get("enabled", True):
                fields = table.get("fields", {})
                pk_field = table.get("primary_key")
                
                pk_path = fields.get(pk_field, pk_field) if pk_field else None
                
                self.table_config.append({
                    "table_name": table["name"],
                    "document_type": table["document_type"],
                    "primary_key": pk_path,
                    "created_at_path": self._infer_created_at_path(fields),
                    "fields": fields,
                    "is_repeated": table.get("is_repeated", False),
                    "repeated_path": table.get("repeated_path")
                })
        
       
        logging.info(f"[TimestampFilter] Loaded table config: {self.table_config}")

     
    
    def _find_key_recursive(self, obj, key):
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                result = self._find_key_recursive(v, key)
                if result is not None:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = self._find_key_recursive(item, key)
                if result is not None:
                    return result
        return None

    def _infer_created_at_path(self, fields):

        if not fields:
            return "created_at"  
            
       
        if "created_at" in fields:
            return fields["created_at"]
            
      
        for k, v in fields.items():
            if "created_at" in k.lower():
                return v if isinstance(v, str) else k
        return "created_at"  

    def process(self, row):
        try:
           
            document_type = row.get("_document_type") or row.get("document_type")
            if not document_type:
                logging.warning(f"Missing document_type in row: {row}")
                return

            matching_tables = [t for t in self.table_config if t["document_type"] == document_type]
            
            if not matching_tables:
                logging.warning(f" No table config found for document_type: {document_type}")
                logging.info(f" Available document types: {[t['document_type'] for t in self.table_config]}")
                return

            emitted = False
            for table in matching_tables:
                table_name = table["table_name"]
                pk_path = table["primary_key"]
                created_at_path = table["created_at_path"]
                is_repeated = table.get("is_repeated", False)
                repeated_path = table.get("repeated_path")

                if is_repeated and repeated_path:
                    # Handle repeated fields 
                    repeated_items = get_nested_value(row, repeated_path)
                    if repeated_items and isinstance(repeated_items, list):
                        base_pk_value = get_nested_value(row, pk_path) if pk_path else None
                        created_at_raw = get_nested_value(row, created_at_path)
                        
                        if base_pk_value and created_at_raw:
                
                            if isinstance(created_at_raw, str) and created_at_raw.endswith("Z"):
                                created_at_raw = created_at_raw.replace("Z", "+00:00")

                            try:
                                record_timestamp = datetime.fromisoformat(str(created_at_raw))
                            except ValueError as e:
                                logging.error(f"Invalid timestamp format '{created_at_raw}': {e}")
                                continue

                            checkpoint_key = f"{table_name}_{base_pk_value}"
                            last_ts = self.checkpoint_manager.get_checkpoint(checkpoint_key)

                            if record_timestamp > last_ts:
                                self.checkpoint_manager.update_checkpoint(checkpoint_key, record_timestamp)
                                logging.info(f" Emitting repeated record for {table_name} | {pk_path}={base_pk_value} | items={len(repeated_items)}")
                                yield row
                                emitted = True
                                break
                            else:
                                logging.info(f" Skipped old repeated record for {checkpoint_key}")
                    continue

            
                pk_value = get_nested_value(row, pk_path) if pk_path else None
                created_at_raw = get_nested_value(row, created_at_path)

               
                logging.info(f" Table: {table_name}, PK Path: {pk_path}, PK Value: {pk_value}")

                if not pk_value:
                    logging.warning(f" No primary key found at path '{pk_path}' for table {table_name}")
                    continue
                    
                if not created_at_raw:
                    logging.warning(f" No created_at found at path '{created_at_path}' for table {table_name}")
                    continue

                
                if isinstance(created_at_raw, str) and created_at_raw.endswith("Z"):
                    created_at_raw = created_at_raw.replace("Z", "+00:00")

                try:
                    record_timestamp = datetime.fromisoformat(str(created_at_raw))
                except ValueError as e:
                    logging.error(f" Invalid timestamp format '{created_at_raw}': {e}")
                    continue

                checkpoint_key = f"{table_name}_{pk_value}"
                last_ts = self.checkpoint_manager.get_checkpoint(checkpoint_key)

                if record_timestamp > last_ts:
                    self.checkpoint_manager.update_checkpoint(checkpoint_key, record_timestamp)
                    logging.info(f" Emitting for {table_name} | {pk_path}={pk_value} | timestamp={record_timestamp}")
                    yield row
                    emitted = True
                    break
                else:
                    logging.info(f"Skipped old record for {checkpoint_key} | record_ts={record_timestamp} <= last_ts={last_ts}")

            if not emitted:
                logging.warning(f" No records emitted for document_type: {document_type}")

        except Exception as e:
            logging.error(f" Error processing element: {e}")
            logging.error(f" Row data: {row}")
            import traceback
            logging.error(f" Traceback: {traceback.format_exc()}")


with open("products_config.yml", "r") as f:
    config = yaml.safe_load(f)


class PrepareFromYamlFn(beam.DoFn):
    def __init__(self, config):
        self.config = config

    def process(self, element):
        results = []
        for table_config in self.config.get("tables", []):
            if not table_config.get("enabled", True):
                continue

            table_name = table_config["name"]
            fields = table_config["fields"]
            is_repeated = table_config.get("is_repeated", False)

            base_path = table_config.get("repeated_path")
            if is_repeated and not base_path and isinstance(fields, dict):
                sample_path = next(iter(fields.values()), None)
                base_path = sample_path.split(".")[0] if sample_path else None

            try:
                if is_repeated:
                    items = self.get_value_by_path(element, base_path) or []
                    if isinstance(items, dict):
                        items = [items]
                    elif not isinstance(items, list):
                        logging.warning(f"Expected list for repeated table '{table_name}', got {type(items)}. Skipping.")
                        continue

                    for item in items:
                        row = self.extract_fields(element, item, fields)
                        if row:
                            results.append((table_name, row))
                else:
                    row = self.extract_fields(element, element, fields)
                    if row:
                        results.append((table_name, row))
            except Exception as e:
                logging.error(f"Error processing table '{table_name}': {e}")
          

        return results

    def extract_fields(self, root, context, fields):
        row = {}
        if isinstance(fields, dict):
            fields = [{"name": k, "path": v} for k, v in fields.items()]

        for field in fields:
            name = field.get("name")
            path = field.get("path")
            required = field.get("required", False)

            if not name or not path:
                continue

            value = self.get_value_by_path(context, path, root=root)

     
            if name.endswith("_id"):
                if value is None or not isinstance(value, (str, int)):
                    value = uuid.uuid4().hex[:16]
                else:
                 
                    value = str(value).strip()
                    if not value:
                        value = uuid.uuid4().hex[:16]

            if value is None and required:
                logging.warning(f"Required field '{name}' at path '{path}' is missing.")

                if name.endswith("_id"):
                    value = uuid.uuid4().hex[:16]
                elif isinstance(field.get("default"), str):
                    value = field.get("default", "")
                else:
                    value = ""

           
            if isinstance(value, str):
                value = value.strip()
            elif value is not None and not isinstance(value, (int, float, bool)):
       
                value = str(value)

            row[name] = value

        return row if any(v is not None for v in row.values()) else None

    def get_value_by_path(self, context, path, root=None):
        if not path or not isinstance(path, str):
            return None
        try:
            keys = path.split(".")
            current = context

            for key in keys:
                if current is None:
                    return None

                if isinstance(current, dict):
                    if key in current:
                        current = current[key]
                    elif root and isinstance(root, dict) and key in root:
                        current = root[key]
                    else:
                        return None
                elif isinstance(current, list) and key.isdigit():
                    index = int(key)
                    current = current[index] if 0 <= index < len(current) else None
                else:
                    return None

            return current
        except (KeyError, IndexError, ValueError, TypeError) as e:
            logging.debug(f"Error accessing path '{path}': {e}")
            return None



def get_nested_value(obj, path):
    try:
        for part in path.split('.'):
            if isinstance(obj, list):
                obj = obj[0]
            if isinstance(obj, dict):
                obj = obj.get(part)
            else:
                return None
        return obj
    except Exception:
        return None


class ParseJson(beam.DoFn):
    def __init__(self, yaml_config):
        self.yaml_config = yaml_config
        self.doc_type_config = {}

    def setup(self):
        for table in self.yaml_config.get("tables", []):
            doc_type = table.get("document_type")
            if doc_type:
                self.doc_type_config.setdefault(doc_type, []).append(table)

    def process(self, element):
        try:
            decoded = element.decode("utf-8").strip()

            if decoded == '{"done": true}':
                logging.info("[ParseJsonFn] Received done signal")
                yield beam.pvalue.TaggedOutput("done", True)
                return

            data = json.loads(decoded)

            doc_type = data.get("document_type")
            if doc_type:
                data["_document_type"] = doc_type
            else:
                logging.warning(" document_type not found in record")
                return

            table_configs = self.doc_type_config.get(doc_type)
            if not table_configs:
                logging.warning(f" No YAML config found for document_type: {doc_type}")
                return

            primary_table = sorted(table_configs, key=lambda t: t.get("write_order", 0))[0]
            pk_field = primary_table.get("primary_key")
            fields_map = primary_table.get("fields", {})

            primary_key_path = fields_map.get(pk_field) if pk_field else None
            primary_id = get_nested_value(data, primary_key_path) if primary_key_path else None

            logging.info(f" Successfully parsed record: {primary_id or '[no ID found]'}")
            yield data

        except Exception as e:
            logging.error(f" Error parsing message: {e}")
            logging.error(f" Raw element: {element}")




class WriteToSpannerFn(beam.DoFn):
    def __init__(self, project_id, instance_id, database_id, config):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.config = config

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

       
        table_configs = {table['name']: table for table in self.config.get('tables', [])}
        ordered_tables = sorted(
            table_data.items(),
            key=lambda item: table_configs.get(item[0], {}).get("write_order", float("inf"))
        )

        for table_name, rows in ordered_tables:
            if not rows:
                continue

            try:
              
                sorted_rows = sorted(
                    rows,
                    key=lambda r: datetime.fromisoformat(r['created_at'].replace("Z", "")) 
                              if r.get('created_at') and isinstance(r['created_at'], str) 
                              else datetime.min
                )

                with self.database.batch() as batch:
                    for row in sorted_rows:
                        row_copy = dict(row)
                        row_copy.pop('created_at', None)
                        
                       
                        cleaned_row = {k: v for k, v in row_copy.items() if v is not None}
                        
                        if cleaned_row:
                            batch.insert_or_update(
                                table=table_name,
                                columns=list(cleaned_row.keys()),
                                values=[list(cleaned_row.values())]
                            )

                logging.info(f"Inserted {len(rows)} rows into table '{table_name}' for customer {customer_id}")
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
            | "ParseJson" >> beam.ParDo(ParseJson(config)).with_outputs("done", main="data",)
        )
      

        data_records = messages["data"]
        done_signal = messages["done"]

        # Filter records based on timestamp
        filtered_records = (
            data_records 
            | "FilterByTimestamp" >> beam.ParDo(TimestampFilter(PROJECT_ID, INSTANCE_ID, DATABASE_ID,config))
        )

        force_drain = done_signal | "ForceDrain" >> beam.ParDo(ForceDrainFn()).with_outputs("drain_done", main="drain")

       # Preparing all the data
        all_data = (
                    filtered_records 
                    | "PrepareFromYamlFn" >> beam.ParDo(PrepareFromYamlFn(config))
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
            WriteToSpannerFn(PROJECT_ID, INSTANCE_ID, DATABASE_ID,config)
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