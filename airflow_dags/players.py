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
    schedule_interval=timedelta(hours=12),
)

generate_current_date = PythonOperator(
    task_id='generate_current_date',
    python_callable=lambda ds, **kwargs: datetime.now().strftime('%Y%m%d_%H%M%S'),
    provide_context=True,
    dag=dag,
    depends_on_past=True,
    wait_for_downstream=True
)


get_versions_task = BashOperator(
    task_id='get_versions',
    bash_command="""
        cd {scrapy_path} && 
        scrapy crawl versions -o /tmp/{scrapy_path}data/versions.jl && 
        gsutil mv /tmp/{scrapy_path}data/versions.jl {gcs_versions_path}
    """.format(
        scrapy_path=SCRAPY_PATH,
        gcs_versions_path='gs://sofifa/{entity}/{current_date_time}.jl'.format(
            entity='versions',
            current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
        )
    ),
    dag=dag,
    depends_on_past=True,
    wait_for_downstream=True,
)


load_versions_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='load_versions_to_bq',
    bucket='sofifa',
    source_objects=[
        '{entity}/{current_date_time}.jl'.format(
            entity='versions',
            current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
        )
    ],
    destination_project_dataset_table='fifaeng.sofifa.versions',
    schema_fields=[
        {'name': 'version_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'version_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'release_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
    ],
    source_format='NEWLINE_DELIMITED_JSON',
    bigquery_conn_id='google_cloud_default',
    write_disposition='WRITE_APPEND',
    depends_on_past=True,
    wait_for_downstream=True,
    dag=dag
)


def _create_tasks_factory(version):
    gcs_path = 'gs://sofifa/{entity}/{version}/{current_date_time}.jl'
    return BashOperator(
        task_id='get_urls_{version}'.format(version=version),
        bash_command="""
            cd {scrapy_path} && 
            scrapy crawl players_url_list -o /tmp/{scrapy_path}{version}/data/urls.jl -a version={version} && 
            gsutil mv /tmp/{scrapy_path}{version}/data/urls.jl {gcs_versions_path}
        """.format(
            scrapy_path=SCRAPY_PATH,
            gcs_versions_path=gcs_path.format(
                entity='urls',
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
            ),
            version=version
        ),
        dag=dag,
        depends_on_past=True,
        wait_for_downstream=True,
    ), GoogleCloudStorageToBigQueryOperator(
        task_id='load_urls_to_bq_{version}'.format(version=version),
        bucket='sofifa',
        source_objects=[
            '{entity}/{version}/{current_date_time}.jl'.format(
                entity='urls',
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
            )
        ],
        destination_project_dataset_table='fifaeng.sofifa.urls',
        schema_fields=[
            {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'version_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'version_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
        ],
        source_format='NEWLINE_DELIMITED_JSON',
        bigquery_conn_id='google_cloud_default',
        write_disposition='WRITE_APPEND',
        depends_on_past=True,
        wait_for_downstream=True,
        dag=dag
    ), BashOperator(
        task_id='get_players_{version}'.format(version=version),
        bash_command="""
            cd {scrapy_path} && 
            scrapy crawl players -o /tmp/{scrapy_path}{version}data/players.jl -a version={version} && 
            gsutil mv /tmp/{scrapy_path}{version}data/players.jl {gcs_versions_path}
        """.format(
            scrapy_path=SCRAPY_PATH,
            gcs_versions_path=gcs_path.format(
                entity='players',
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
            ),
            version=version
        ),
        dag=dag,
        depends_on_past=True,
        wait_for_downstream=True,
    ), GoogleCloudStorageToBigQueryOperator(
        task_id='load_players_to_bq_{version}'.format(version=version),
        bucket='sofifa',
        source_objects=[
            '{entity}/{version}/{current_date_time}.jl'.format(
                entity='players',
                version=version,
                current_date_time="{{ task_instance.xcom_pull(task_ids='generate_current_date') }}"
            )
        ],
        destination_project_dataset_table='fifaeng.sofifa.players',
        schema_fields=[
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
            {'name': 'processed_at', 'type': 'DATETIME', 'mode': 'NULLABLE'}
        ],
        source_format='NEWLINE_DELIMITED_JSON',
        bigquery_conn_id='google_cloud_default',
        write_disposition='WRITE_APPEND',
        depends_on_past=True,
        wait_for_downstream=True,
        dag=dag
    )


get_urls_fifa_20_task, load_urls_fifa_20_to_bq, get_players_fifa_20_task, load_players_fifa_20_to_bq = \
    _create_tasks_factory('20')

get_urls_fifa_19_task, load_urls_fifa_19_to_bq, get_players_fifa_19_task, load_players_fifa_19_to_bq = \
    _create_tasks_factory('19')

generate_current_date >> get_versions_task >> load_versions_to_bq

load_versions_to_bq.set_downstream([get_urls_fifa_20_task, get_urls_fifa_19_task])

get_urls_fifa_20_task.set_downstream(load_urls_fifa_20_to_bq)
load_urls_fifa_20_to_bq.set_downstream(get_players_fifa_20_task)
get_players_fifa_20_task.set_downstream(load_players_fifa_20_to_bq)

get_urls_fifa_19_task.set_downstream(load_urls_fifa_19_to_bq)
load_urls_fifa_19_to_bq.set_downstream(get_players_fifa_19_task)
get_players_fifa_19_task.set_downstream(load_players_fifa_19_to_bq)

get_urls_fifa_18_task.set_downstream(load_urls_fifa_18_to_bq)
load_urls_fifa_18_to_bq.set_downstream(get_players_fifa_18_task)
get_players_fifa_18_task.set_downstream(load_players_fifa_18_to_bq)

get_urls_fifa_17_task.set_downstream(load_urls_fifa_17_to_bq)
load_urls_fifa_17_to_bq.set_downstream(get_players_fifa_17_task)
get_players_fifa_17_task.set_downstream(load_players_fifa_17_to_bq)
