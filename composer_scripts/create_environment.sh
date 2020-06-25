#!/bin/bash

gcloud composer environments create fifaeng \
    --location us-central1 \
    --disk-size 20 \
    --env-variables SCRAPY_PATH=/home/airflow/gcs/dags/dependencies/scrapy_sofifa/ \
    --python-version 3 \
    --image-version composer-latest-airflow-1.10.2 \
    --service-account dataloader@fifaeng.iam.gserviceaccount.com &&
gcloud composer environments update fifaeng \
    --update-pypi-packages-from-file requirements_composer.txt \
    --location us-central1