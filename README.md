# Internship Project 


# Telecom Data Pipeline: Pub/Sub to Cloud Spanner with Change Streams to BigQuery

## Description
This repository contains a data ingestion pipeline for telecom data in JSON format. The pipeline utilizes **Google Cloud Pub/Sub** to publish messages, **Cloud Spanner** to store structured telecom data, and **Change Streams** to track data modifications, forwarding them to **BigQuery** for analytics. This ensures efficient ingestion, storage, and processing of telecom data.

## Process Overview
1. **Publishing Data to Pub/Sub**  
   JSON-formatted telecom data is published to a Pub/Sub topic using `publish.py`.

2. **Ingesting Data into Cloud Spanner**  
   A pipeline (`pipeline.py`) reads messages from Pub/Sub and inserts structured data into Cloud Spanner tables.

3. **Implementing Change Streams in Cloud Spanner**  
   - Change Streams capture inserts, updates, and deletes from Spanner tables.
   - Change data is streamed to BigQuery via `SpannerChangeStreamToBigquery.java`.

4. **Processing and Analyzing Data in BigQuery**  
   The streamed change data is stored in BigQuery tables for further analysis.

##  Setup Instructions

Follow these steps to set up and run the pipeline end-to-end:

### 1. Clone the Repository

```bash
git clone https://github.com/alok-dataai/internship.git
```

### 2. Navigate into the scripts Directory
```bash
cd internship/scripts
```
### 3. Run the Pipeline Script
```bash
bash run_telecom_pipeline.sh
```
   
