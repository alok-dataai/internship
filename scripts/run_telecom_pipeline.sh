#!/bin/bash

# Set Project Variables
PROJECT_ID="playground-s-11-1a274b22"
TOPIC_NAME="raw-telecom-data"
INSTANCE_ID="spanner-instance-or"
CHANGE_STREAM_NAME="change_stream_telecom"
DATABASE_ID="telecom-db"
BQ_DATASET="telecom_dt"





echo "Running setup script for dependencies..."
./setup_gcp_environment.sh

echo "Creating Spanner tables..."
./spanner_table_creation.sh

echo "Creating BigQuery tables..."
./bigquery_table_creation.sh




echo "Creating Pub/Sub topic..."
gcloud pubsub topics create $TOPIC_NAME



cd ..
echo "Running data pipeline..."

python3 pipeline.py --project=$PROJECT_ID &

sleep 10


echo "Publishing messages to Pub/Sub..."

python3 publish.py --project=$PROJECT_ID --topic=$TOPIC_NAME --input-file=telecom_data.json


echo "Pipeline execution completed!"
