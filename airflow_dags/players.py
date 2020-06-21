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

get_players_task = BashOperator(
    task_id='get_players',
    bash_command='cd {scrapy_path} && scrapy crawl players'.format(scrapy_path=SCRAPY_PATH),
    dag=dag,
    depends_on_past=True,
    wait_for_downstream=True,
)


gcs_players_path = '{current_date}/players-{current_date_time}.jl'.format(
    current_date=date.strftime(date.today(), '%Y-%m-%d'),
    current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
)

players_list_to_gcs = FileToGoogleCloudStorageOperator(
    task_id='players_list_to_gcs',
    src='{scrapy_path}/data/players.jl'.format(scrapy_path=SCRAPY_PATH),
    dst=gcs_players_path,
    google_cloud_storage_conn_id='google_cloud_default',
    bucket='sofifa',
    depends_on_past=True,
    wait_for_downstream=True,
    mime_type='application/x-ndjson',
    dag=dag
)

players_list_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id='players_list_to_bq',
    bucket='sofifa',
    source_objects=[gcs_players_path],
    destination_project_dataset_table='fifaengineering.sofifa.players',
    schema_fields=[
        {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'positions', 'type': 'STRING', 'mode': 'REPEATED'},
        {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'birthdate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'height_in_meters', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'weight_in_kg', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'overall_rating', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'potential_rating', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'value_in_million_euros', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'wage_in_thousand_euros', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'preferred_foot', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'weak_foot', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'skill_moves', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'international_reputation', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'work_rate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'body_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'real_face', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'release_clause', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'specialities', 'type': 'STRUCT', 'mode': 'REPEATED', 'fields': (
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'link', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'},
        )},
        {'name': 'team_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'team_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'team_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'team_overall', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'team_position', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'team_jersey_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'joined', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'loaned_from', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'loaned_from_team_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contract_valid_until', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'national_team_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'national_team_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'national_team_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'national_team_overall', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'national_team_position', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'national_team_jersey_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
    ],
    source_format='NEWLINE_DELIMITED_JSON',
    bigquery_conn_id='google_cloud_default',
    write_disposition='WRITE_APPEND',
    depends_on_past=True,
    wait_for_downstream=True,
    dag=dag
)

generate_current_date >> get_urls_task >> urls_list_to_gcs >> urls_list_to_bigquery >> \
    get_players_task >> players_list_to_gcs >> players_list_to_bigquery
