#!/bin/bash

BUCKET_NAME=$(
  gcloud composer environments describe fifaeng \
    --location us-central1 \
    --format="get(config.dagGcsPrefix)"
)
echo "$BUCKET_NAME"
gsutil cp /Users/airtongessner/projetos/fifaengineering/airflow_dags/players.py "$BUCKET_NAME" &&
   gsutil cp -r /Users/airtongessner/projetos/fifaengineering/scrapy_sofifa/ "$BUCKET_NAME"/dependencies/
