from urllib.parse import urlparse, parse_qs

from scrapy import Spider, Request

SOFIFA_URL = 'https://sofifa.com'
LAST_KNOWN_PAGE = 6740
NUMBER_OF_PLAYERS_BY_PAGE = 60


class PlayersURLListSpider(Spider):
    name = 'players_url_list'
    start_urls = 'https://sofifa.com/?offset=0',
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_sofifa.pipelines.PlayersURLListPipeline': 400
        },
        'FEEDS': {
            'data/urls.jl': {
                'format': 'jsonlines',
                'store_empty': True,
                'encoding': 'utf8',
                'fields': None,
                'indent': 4
            }
        }
    }

    def parse(self, response):
        for row in response.css('tbody > tr'):
            yield {
                'url': '{sofifa_url}{player_url}'.format(
                    sofifa_url=SOFIFA_URL,
                    player_url=row.css('td.col-name')[0].css('a::attr(href)').get()
                )
            }

        next_page = self._get_next_page(response)
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield Request(next_page, callback=self.parse)

    def _get_next_page(self, response):
        pagination_links = response.css('div.pagination > a')
        if pagination_links.css('span::text').get() == 'Next':
            return pagination_links.attrib['href']

        if self._has_second_pagination_link(pagination_links):
            return pagination_links[1].attrib['href']

        if int(parse_qs(urlparse(response.url).query)['offset'][0]) < LAST_KNOWN_PAGE:
            return '/?offset=' + str(int(parse_qs(urlparse(response.url).query)['offset'][0]) + NUMBER_OF_PLAYERS_BY_PAGE)

        return None

    @staticmethod
    def _has_second_pagination_link(pagination_links):
        return len(pagination_links.getall()) == 2
