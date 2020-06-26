# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime


class DefaultPipeline:
    def __init__(self):
        self.current_datetime = datetime.now()

    def process_item(self, item, spider):
        return {**item, **{'processed_at': self.current_datetime}}

