PROJECT_ID=$(gcloud config get-value project)
TOPIC_NAME="raw-telecom-data"
INSTANCE_ID="spanner-instance-or"
CHANGE_STREAM_NAME="change_stream_telecom"
DATABASE_ID="telecom-db"
BQ_DATASET="telecom_dt"

# Moving to the root directory where target file is present
cd ..


mvn clean compile

mvn compile exec:java \
  -Dexec.mainClass=com.example.SpannerChangeStreamToBigQuery \
  -Dexec.args="\
    --projectId=$PROJECT_ID \
    --instanceId=$INSTANCE_ID \
    --databaseId=$DATABASE_ID \
    --changeStreamName=$CHANGE_STREAM_NAME \
    --bigQueryDataset=$BQ_DATASET"