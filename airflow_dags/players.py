import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


SCRAPY_PATH = os.environ.get('SCRAPY_PATH', '/Users/airtongessner/projetos/fifaengineering/scrapy_sofifa/')


dag = DAG(
    'players',
    description='Populate the players data on BQ',
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=30),
)

get_urls_task = BashOperator(
    task_id='get_urls',
    bash_command='cd {scrapy_path} && scrapy crawl players_url_list'.format(scrapy_path=SCRAPY_PATH),
    dag=dag,
    depends_on_past=True,
    wait_for_downstream=True,
)
