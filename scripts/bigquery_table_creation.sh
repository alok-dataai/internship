#!/bin/bash

PROJECT_ID="playground-s-11-1a274b22"
DATASET_ID="telecom_dt"
SCHEMA_FILE="schema.json"


echo "Creating BigQuery dataset..."
bq --location=US mk --dataset --description "Dataset for telecom changes" $PROJECT_ID:$DATASET_ID



TABLE_NAMES=$(jq -r 'keys[]' $SCHEMA_FILE)


for TABLE in $TABLE_NAMES; do
    echo "Creating table: $TABLE"
    
  
    SCHEMA=$(jq -c ".$TABLE" $SCHEMA_FILE)
 
    echo "$SCHEMA" > temp_schema.json
    

    bq mk --table $PROJECT_ID:$DATASET_ID.$TABLE temp_schema.json
done


rm temp_schema.json

echo "BigQuery tables created successfully!"

echo "Creating staging table..."
bq query --use_legacy_sql=false "
CREATE TABLE \`$PROJECT_ID.$DATASET_ID.staging\` (
    table_name STRING NOT NULL,
    mod_type STRING NOT NULL,
    record_timestamp TIMESTAMP NOT NULL,
    data_json STRING
)
PARTITION BY DATE(record_timestamp)
CLUSTER BY table_name, mod_type;
"

echo "BigQuery setup completed successfully!"
