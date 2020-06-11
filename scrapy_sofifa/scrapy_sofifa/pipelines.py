# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime

from google.cloud import bigquery
from google.cloud.bigquery import Dataset, Table, LoadJobConfig


class ScrapySofifaPipeline:
    def __init__(self):
        self.bigquery_connection = None
        self.table = None
        self.current_datetime = datetime.now()

    def process_item(self, item, spider):
        return {
            'value': item['url'],
            'processed_at': self.current_datetime
        }

    def open_spider(self, spider):
        self.bigquery_connection = bigquery.Client(project='fifaengineering')
        self.bigquery_connection.create_dataset(
            Dataset('fifaengineering.sofifa'),
            exists_ok=True
        )
        self.table = Table('fifaengineering.sofifa.urls', schema=[
            bigquery.schema.SchemaField('value', 'STRING'),
            bigquery.schema.SchemaField('processed_at', 'DATETIME')
        ])
        self.bigquery_connection.create_table(self.table, exists_ok=True)

    def close_spider(self, spider):
        job_config = LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        filename = spider.settings.get('PLAYERS_URL_LIST_URI')
        with open(filename, "rb") as source_file:
            job = self.bigquery_connection.load_table_from_file(
                source_file,
                self.table,
                job_config=job_config
            )

        job.result()
        self.bigquery_connection.close()
