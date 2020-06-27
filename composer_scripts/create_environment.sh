#!/bin/bash

gcloud composer environments create fifaeng2 \
    --location us-central1 \
    --disk-size 20 \
    --env-variables SCRAPY_PATH=/home/airflow/gcs/dags/dependencies/scrapy_sofifa/ \
    --python-version 3 \
    --image-version composer-latest-airflow-1.10.2 \
    --machine-type n1-standard-4 \
    --zone us-central1-d \
    --airflow-configs core-worker_concurrency=10 \
    --node-count 4 \
    --service-account dataloader@fifaeng.iam.gserviceaccount.com &&
gcloud composer environments update fifaeng2 \
    --update-pypi-packages-from-file requirements_composer.txt \
    --location us-central1
