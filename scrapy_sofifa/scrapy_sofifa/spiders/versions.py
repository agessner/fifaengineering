from datetime import datetime

from scrapy import Spider, Request

import utils


class VersionsSpider(Spider):
    name = 'versions'
    start_urls = ['https://sofifa.com/players']
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_sofifa.pipelines.DefaultPipeline': 400
        }
    }

    def parse(self, response):
        current_page_main_version, next_page_main_version = get_current_and_next_page_main_versions(response)
        list_of_updates = list(zip(
            response.css('div.dropdown:nth-child(2) > div.bp3-menu > a::attr(href)').getall(),
            response.css('div.dropdown:nth-child(2) > div.bp3-menu > a::text').getall()
        ))
        list_of_updates = _remove_buggy_last_fifa_20_version(list_of_updates) \
            if current_page_main_version[1] == 'FIFA 20' else list_of_updates
        for (link, release_date) in list_of_updates:
            yield {
                'version_name': current_page_main_version[1],
                'version_id': utils.get_id_from_version_link(link),
                'release_date': _convert_date(release_date)
            }

        if next_page_main_version:
            yield Request('https://sofifa.com{link}'.format(link=next_page_main_version[0]), callback=self.parse)


def _remove_buggy_last_fifa_20_version(list_of_updates):
    return list_of_updates[:-1]


def _convert_date(sofifa_date):
    return datetime.strptime(sofifa_date, '%b %d, %Y').date() if sofifa_date else ''


def get_current_and_next_page_main_versions(response):
    current_page_main_version_name = response.css('div.dropdown:nth-child(1) > a > span.bp3-button-text::text').get()
    main_versions = list(zip(
        response.css('div.dropdown:nth-child(1) > div.bp3-menu > a::attr(href)').getall(),
        response.css('div.dropdown:nth-child(1) > div.bp3-menu > a::text').getall()
    ))
    for index, (link, name) in enumerate(main_versions, start=1):
        if name == current_page_main_version_name:
            current_page = (link, name)
            next_page = main_versions[index] if len(main_versions) > index else None
            return current_page, next_page

    return None, None
