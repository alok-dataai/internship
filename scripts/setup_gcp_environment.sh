#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)

echo "Installing  dependencies..."
pip install --upgrade google-cloud-storage google-cloud-functions google-cloud-bigquery google-cloud-spanner apache-beam[gcp]

echo "Enabling required Google Cloud APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    bigquery.googleapis.com \
    spanner.googleapis.com \
    storage.googleapis.com \
    pubsub.googleapis.com \
    dataflow.googleapis.com

echo "Verifying API status..."
gcloud services list --enabled --project=$PROJECT_ID

echo "Setup completed successfully!"
