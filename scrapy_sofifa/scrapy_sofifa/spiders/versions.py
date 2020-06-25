from scrapy import Spider, Request

from spiders.players import _convert_date


class VersionsSpider(Spider):
    name = 'versions'
    start_urls = ['https://sofifa.com/players']
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_sofifa.pipelines.DefaultPipeline': 400
        },
        'FEEDS': {
            'data/versions.jl': {
                'format': 'jsonlines',
                'store_empty': True,
                'encoding': 'utf8',
                'fields': None,
                'indent': 4
            }
        }
    }

    def parse(self, response):
        current_page_main_version, next_page_main_version = get_current_and_next_page_main_versions(response)
        for (link, release_date) in zip(
            response.css('div.dropdown:nth-child(2) > div.bp3-menu > a::attr(href)').getall(),
            response.css('div.dropdown:nth-child(2) > div.bp3-menu > a::text').getall()
        ):
            yield {
                'version_name': current_page_main_version[1],
                'version_id': get_id_from_version_link(link),
                'release_date': _convert_date(release_date)
            }

        if next_page_main_version:
            yield Request('https://sofifa.com{link}'.format(link=next_page_main_version[0]), callback=self.parse)


def get_id_from_version_link(link):
    return link.replace('/players?r=', '').replace('&set=true', '')


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
