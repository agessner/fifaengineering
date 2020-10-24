from urllib.parse import urlparse, parse_qs

from scrapy import Spider, Request

import utils
from scrapy_sofifa.spiders.gateways import get_last_version_id_from_main_version

SOFIFA_URL = 'https://sofifa.com'
BASE_URL = 'https://sofifa.com/players?r={version_id}&set=true&offset={offset}'
LAST_KNOWN_PAGE = {
    '07': 10899,
    '08': 12785,
    '09': 16213,
    '10': 16708,
    '11': 15203,
    '12': 14472,
    '13': 14988,
    '14': 16614,
    '15': 16431,
    '16': 17061,
    '17': 17560,
    '18': 17927,
    '19': 17974,
    '20': 20000,
    '21': 20000

}
NUMBER_OF_PLAYERS_BY_PAGE = 60


class PlayersURLListSpider(Spider):
    name = 'players_url_list'
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_sofifa.pipelines.DefaultPipeline': 400
        }
    }

    def __init__(self, version=None, *args, **kwargs):
        super(PlayersURLListSpider, self).__init__(*args, **kwargs)
        self.version = version

    def start_requests(self):
        for url in get_last_version_id_from_main_version(self.version):
            yield Request(url=BASE_URL.format(
                version_id=url['version_id'],
                offset=0
            ))

    def parse(self, response):
        for row in response.css('tbody > tr'):
            yield {
                'value': '{sofifa_url}{player_url}'.format(sofifa_url=SOFIFA_URL, player_url=_get_player_url(row)),
                'player_id': _get_player_id(row),
                'player_nickname': _get_nickname(row),
                'version_id': utils.get_page_version_id(response),
                'version_name': self.version
            }

        next_page = _get_next_page(response, self.version)
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield Request(next_page, callback=self.parse)


def _get_player_url(row):
    return row.css('td.col-name')[0].css('a::attr(href)').get()


def _get_player_id(row):
    return int(_get_player_url(row).split('/')[2])


def _get_nickname(row):
    values_on_field_name = [name.strip() for name in row.css('td.col-name a div::text').getall() if name.strip()]
    if not values_on_field_name:
        return ''

    return values_on_field_name[0]


def _get_next_page(response, version):
    pagination_links = response.css('div.pagination > a')
    if pagination_links.css('span::text').get() == 'Next':
        return pagination_links.attrib['href']

    if _has_second_pagination_link(pagination_links):
        return pagination_links[1].attrib['href']

    if _current_offset_is_lower_than_the_last_known(response, version):
        return '?r={version_id}&set=true&offset={next_offset}'.format(
            version_id=_get_url_arg(response, 'r'),
            next_offset=str(int(_get_url_arg(response, 'offset')) + NUMBER_OF_PLAYERS_BY_PAGE)
        )

    return None


def _has_second_pagination_link(pagination_links):
    return len(pagination_links.getall()) == 2


def _current_offset_is_lower_than_the_last_known(response, version):
    return int(parse_qs(urlparse(response.url).query)['offset'][0]) < LAST_KNOWN_PAGE[version]


def _get_url_arg(response, arg):
    return parse_qs(urlparse(response.url).query)[arg][0]