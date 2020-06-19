import os
from datetime import timedelta, date, datetime

from airflow import DAG
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

SCRAPY_PATH = os.environ.get('SCRAPY_PATH', '/Users/airtongessner/projetos/fifaengineering/scrapy_sofifa/')


dag = DAG(
    'players',
    description='Populate the players data on BQ',
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=4),
)

generate_current_date = PythonOperator(
    task_id='generate_current_date',
    python_callable=lambda ds, **kwargs: str(datetime.now()),
    provide_context=True,
    dag=dag,
    depends_on_past=True,
    wait_for_downstream=True
)


get_urls_task = BashOperator(
    task_id='get_urls',
    bash_command='cd {scrapy_path} && scrapy crawl players_url_list'.format(scrapy_path=SCRAPY_PATH),
    dag=dag,
    depends_on_past=True,
    wait_for_downstream=True,
)

gcs_path = '{current_date}/urls-{current_date_time}.jl'.format(
    current_date=date.strftime(date.today(), '%Y-%m-%d'),
    current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
)

urls_list_to_gcs = FileToGoogleCloudStorageOperator(
    task_id='load_urls_to_gcs',
    src='{scrapy_path}/data/urls.jl'.format(scrapy_path=SCRAPY_PATH),
    dst=gcs_path,
    google_cloud_storage_conn_id='google_cloud_default',
    bucket='sofifa',
    depends_on_past=True,
    wait_for_downstream=True,
    mime_type='application/x-ndjson',
    dag=dag
)

urls_list_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id='load_urls_to_bq',
    bucket='sofifa',
    source_objects=[gcs_path],
    destination_project_dataset_table='fifaengineering.sofifa.urls',
    schema_fields=[
        {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
    ],
    source_format='NEWLINE_DELIMITED_JSON',
    bigquery_conn_id='google_cloud_default',
    write_disposition='WRITE_APPEND',
    depends_on_past=True,
    wait_for_downstream=True,
    dag=dag
)


generate_current_date >> get_urls_task >> urls_list_to_gcs >> urls_list_to_bigquery
