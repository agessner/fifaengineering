#!/bin/bash

gcloud composer environments create fifaeng \
    --location us-central1 \
    --zone us-central1-f \
    --machine-type n1-standard-2 \
    --disk-size 20 \
    --env-variables SCRAPY_PATH=/home/airflow/gcs/dags/dependencies/scrapy_sofifa/ \
    --python-version 3 \
    --image-version composer-latest-airflow-1.10.3 \
    --airflow-configs core-worker_concurrency=10 \
    --node-count 4 \
    --service-account dataloader@fifaeng.iam.gserviceaccount.com &&
gcloud composer environments update fifaeng \
    --update-pypi-packages-from-file requirements_composer.txt \
    --location us-central1
