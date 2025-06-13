#!/bin/bash

# Set Project Variables
PROJECT_ID=$(gcloud config get-value project)
BUCKET_NAME="telecom-bucket-2354"
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


echo "Creating GCS Bucket with - staging and temp"
gcloud storage buckets create gs://$BUCKET_NAME \
  --location=us-central1 \
  --default-storage-class=STANDARD \
  --no-uniform-bucket-level-access

echo "staging folder" | gsutil cp - gs://$BUCKET_NAME/staging/.keep
echo "temp folder" | gsutil cp - gs://$BUCKET_NAME/temp/.keep


echo "Creating Pub/Sub topic..."
gcloud pubsub topics create $TOPIC_NAME


cd ..
echo "Running data pipeline..."

python3 pipeline.py --project=$PROJECT_ID &
PIPELINE_PID=$!
sleep 10


echo "Publishing messages to Pub/Sub..."

python3 publish.py --project=$PROJECT_ID --topic=$TOPIC_NAME --input-file=telecom_data.json 

while true; do
  RAW_OUTPUT=$(gcloud spanner databases execute-sql $DATABASE_ID \
  --instance=$INSTANCE_ID \
  --sql="SELECT COUNT(entity_name) AS total FROM DataIngestionCheckpoint")

  COUNT=$(echo "$RAW_OUTPUT" | grep -o '[0-9]\+')

  if [ "$COUNT" -ge 20 ]; then
    pkill -f pipeline.py
    break
  fi

  sleep 5
done


echo "Pipeline execution completed!"

cd scripts

echo "Running Spanner Change Stream to BigQuery..."
./run_change_streams.sh





