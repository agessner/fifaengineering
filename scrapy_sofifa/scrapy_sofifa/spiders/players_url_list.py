from urllib.parse import urlparse, parse_qs

from google.cloud import bigquery
from scrapy import Spider, Request

SOFIFA_URL = 'https://sofifa.com'
BASE_URL = 'https://sofifa.com/players?r={version_id}&set=true&offset={offset}'
LAST_KNOWN_PAGE = 5500
NUMBER_OF_PLAYERS_BY_PAGE = 60


class PlayersURLListSpider(Spider):
    name = 'players_url_list'
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_sofifa.pipelines.PlayersURLListPipeline': 400
        }
    }

    def start_requests(self):
        bigquery_connection = bigquery.Client(project='fifaeng')
        query = bigquery_connection.query('''
            SELECT 
                version_id
            FROM sofifa.versions WHERE processed_at = (SELECT MAX(processed_at) FROM sofifa.versions)
            AND version_name = "FIFA {version}" 
            ORDER BY version_id DESC
            LIMIT 1
        '''.format(
            version=self.version
        ))
        for url in query.result():
            yield Request(url=BASE_URL.format(
                version_id=url['version_id'],
                offset=0
            ))

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
            return '?r=' + parse_qs(urlparse(response.url).query)['r'][0] + '&set=true&offset=' + str(int(parse_qs(urlparse(response.url).query)['offset'][0]) + NUMBER_OF_PLAYERS_BY_PAGE)

        return None

    @staticmethod
    def _has_second_pagination_link(pagination_links):
        return len(pagination_links.getall()) == 2
