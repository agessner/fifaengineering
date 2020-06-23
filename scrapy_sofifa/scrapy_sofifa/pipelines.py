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
            'version_id': item['version_id'],
            'processed_at': self.current_datetime,
        }


class PlayersPipeline:
    def __init__(self):
        self.current_datetime = datetime.now()

    def process_item(self, item, spider):
        return {**item, **{'processed_at': self.current_datetime}}


class DefaultPipeline:
    def __init__(self):
        self.current_datetime = datetime.now()

    def process_item(self, item, spider):
        return {**item, **{'processed_at': self.current_datetime}}

