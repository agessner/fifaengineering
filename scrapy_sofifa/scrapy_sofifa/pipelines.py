# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime


class PlayersURLListPipeline:
    def __init__(self):
        self.current_datetime = datetime.now()

    def process_item(self, item, spider):
        return {
            'value': item['url'],
            'processed_at': self.current_datetime,
        }


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
