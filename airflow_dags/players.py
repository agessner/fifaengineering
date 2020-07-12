import os
from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

SCRAPY_PATH = os.environ.get('SCRAPY_PATH', '/Users/airtongessner/projetos/fifaengineering/scrapy_sofifa/')
VERSIONS_BQ_SCHEMA = [
    {'name': 'version_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'version_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'release_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
]
URLS_BQ_SCHEMA = [
    {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'player_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'player_nickname', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'version_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'version_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
]
PLAYERS_BQ_SCHEMA = [
    {'name': 'version_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'version_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'country_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'positions', 'type': 'STRING', 'mode': 'REPEATED'},
    {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'birthdate', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'height_in_meters', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'weight_in_kg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'overall_rating', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'potential_overall_rating', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'value_in_million_euros', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'wage_in_thousand_euros', 'type': 'FLOAT', 'mode': 'NULLABLE'},
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
    {'name': 'best_position', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'crossing', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'finishing', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'heading_accuracy', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'short_passing', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'volleys', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'dribbling', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'curve', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'fk_accuracy', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'long_passing', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'ball_control', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
]

dag = DAG(
    'players',
    description='Populate the players data on BQ',
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=24),
    default_args={
        'depends_on_past': True,
        'wait_for_downstream': True
    }
)

generate_current_date = PythonOperator(
    task_id='generate_current_date',
    python_callable=lambda ds, **kwargs: datetime.now().strftime('%Y%m%d_%H%M%S'),
    provide_context=True,
    dag=dag,
)


SCRAPY_BASE_COMMAND = """
    cd {scrapy_path} && 
    scrapy crawl versions -o /tmp/{scrapy_path}data/{entity}.jl && 
    gsutil mv /tmp/{scrapy_path}data/{entity}.jl {gcs_versions_path}
"""


def get_scrapy_command(entity):
    return SCRAPY_BASE_COMMAND.format(
        scrapy_path=SCRAPY_PATH,
        entity=entity,
        gcs_versions_path='gs://sofifa/{entity}/{current_date_time}.jl'.format(
            entity='versions',
            current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
        )
    )


def get_source_gcs_path(entity):
    return '{entity}/{current_date_time}.jl'.format(
        entity=entity,
        current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
    )


get_versions_task = BashOperator(task_id='get_versions', bash_command=get_scrapy_command('versions'), dag=dag)


DEFAULT_GCS_TO_BQ_CONFIG = {
    'bucket': 'sofifa',
    'source_format': 'NEWLINE_DELIMITED_JSON',
    'bigquery_conn_id': 'google_cloud_default',
    'write_disposition': 'WRITE_APPEND',
    'dag': dag
}

load_versions_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='load_versions_to_bq',
    source_objects=[get_source_gcs_path('versions')],
    destination_project_dataset_table='fifaeng.staging.versions',
    schema_fields=VERSIONS_BQ_SCHEMA,
    **DEFAULT_GCS_TO_BQ_CONFIG
)

create_versions_bq_table = BigQueryOperator(
    task_id='create_versions_bq_table',
    sql="""
        SELECT * FROM fifaeng.staging.versions WHERE processed_at = (
            SELECT MAX(processed_at) FROM fifaeng.staging.versions
        )
    """,
    destination_dataset_table='fifaeng.sofifa.versions',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='google_cloud_default',
    dag=dag
)


def create_tasks_for_version(version):
    gcs_file_path = 'gs://sofifa/{entity}/{version}/{current_date_time}.jl'
    file_path = '{entity}/{version}/{current_date_time}.jl'

    def get_bash_command(entity, crawler):
        return """
            cd {scrapy_path} && 
            scrapy crawl {crawler} -o /tmp/{scrapy_path}{version}/data/{entity}.jl -a version={version} && 
            gsutil mv /tmp/{scrapy_path}{version}/data/{entity}.jl {gcs_versions_path}
        """.format(
            scrapy_path=SCRAPY_PATH,
            gcs_versions_path=gcs_file_path.format(
                entity=entity,
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
            ),
            crawler=crawler,
            entity=entity,
            version=version
        )

    get_urls_task = BashOperator(
        task_id='get_urls_{version}'.format(version=version),
        bash_command=get_bash_command('urls', 'players_url_list'),
        dag=dag
    )
    load_urls_to_bq_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_urls_to_bq_{version}'.format(version=version),
        source_objects=[
            file_path.format(
                entity='urls',
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}")
        ],
        destination_project_dataset_table='fifaeng.staging.urls',
        schema_fields=URLS_BQ_SCHEMA,
        **DEFAULT_GCS_TO_BQ_CONFIG
    )
    create_urls_bq_table = BigQueryOperator(
        task_id='create_urls_bq_table_{version}'.format(version=version),
        sql="""
            SELECT DISTINCT * FROM fifaeng.staging.urls WHERE processed_at = (
                SELECT MAX(processed_at) FROM fifaeng.staging.urls WHERE version_name = "{version}"
            ) AND version_name = "{version}"
        """.format(version=version),
        destination_dataset_table='fifaeng.sofifa.urls_{version}'.format(version=version),
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        dag=dag,
        use_legacy_sql=False
    )
    get_players_task = BashOperator(
        task_id='get_players_{version}'.format(version=version),
        bash_command=get_bash_command('players', 'players'),
        dag=dag
    )
    load_players_to_bq_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_players_to_bq_{version}'.format(version=version),
        source_objects=[
            file_path.format(
                entity='players',
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
            )
        ],
        destination_project_dataset_table='fifaeng.staging.players',
        schema_fields=PLAYERS_BQ_SCHEMA,
        **DEFAULT_GCS_TO_BQ_CONFIG
    )
    create_players_bq_table = BigQueryOperator(
        task_id='create_players_bq_table_{version}'.format(version=version),
        sql="""
            SELECT * FROM fifaeng.staging.players WHERE processed_at = (
                SELECT MAX(processed_at) FROM fifaeng.staging.players WHERE version_name = "{version}"
            ) AND version_name = "{version}"
        """.format(version=version),
        destination_dataset_table='fifaeng.sofifa.players_{version}'.format(version=version),
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        dag=dag,
        use_legacy_sql=False
    )
    create_versions_bq_table.set_downstream(get_urls_task)
    get_urls_task.set_downstream(load_urls_to_bq_task)
    load_urls_to_bq_task.set_downstream(create_urls_bq_table)
    create_urls_bq_table.set_downstream(get_players_task)
    get_players_task.set_downstream(load_players_to_bq_task)
    load_players_to_bq_task.set_downstream(create_players_bq_table)
    return get_urls_task, load_urls_to_bq_task, get_players_task, load_players_to_bq_task


generate_current_date >> get_versions_task >> load_versions_to_bq >> create_versions_bq_table

for i in range(7, 21):
    create_tasks_for_version('0' + str(i) if i < 10 else str(i))
