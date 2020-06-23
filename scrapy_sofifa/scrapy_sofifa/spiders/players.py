import re
from datetime import datetime, date
from decimal import Decimal, InvalidOperation

from google.cloud import bigquery
from scrapy import Spider, Request

SOFIFA_URL = 'https://sofifa.com'
LAST_KNOWN_PAGE = 6740
NUMBER_OF_PLAYERS_BY_PAGE = 60


class PlayersSpider(Spider):
    name = 'players'
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_sofifa.pipelines.PlayersPipeline': 400
        },
        'FEEDS': {
            'data/players.jl': {
                'format': 'jsonlines',
                'store_empty': True,
                'encoding': 'utf8',
                'fields': None,
                'indent': 4
            }
        }
    }

    def start_requests(self):
        bigquery_connection = bigquery.Client(project='fifaengineering')
        query = bigquery_connection.query('''
            SELECT 
                value
            FROM sofifa.urls WHERE processed_at = (SELECT MAX(processed_at) FROM sofifa.urls) 
            LIMIT 100
        ''')
        for url in query.result():
            yield Request(url=url['value'])

    def parse(self, response):
        yield {
            'full_name': response.css('div.player .info > h1::text').get().split('(')[0].strip(),
            'id': int(re.findall('[0-9]+', response.css('div.player .info > h1::text').get())[0]),
            'image_url': response.css('div.player > img').attrib['data-src'],
            'country': response.css('div.player .info > div > a').attrib['title'],
            'country_image_url': response.css('div.player .info > div > a > img').attrib['data-src'],
            'positions': response.css('div.player .info > div > span::text').getall(),
            'age': int(re.findall('[0-9]+', response.css('div.player .info > div::text').getall()[-1])[0]),
            'birthdate': datetime.strptime(
                re.findall('(?<=\().+(?=\))', response.css('div.player .info > div::text').getall()[-1])[0],
                '%b %d, %Y'
            ).date(),
            'height_in_meters': _convert_feet_inches_to_meters(
                re.findall('(?<=\) ).+(?<=\")', response.css('div.player .info > div::text').getall()[-1])[0]
            ),
            'weight_in_kg': _convert_libras_to_kilograms(
                int(re.findall('[0-9]+(?=lbs)', response.css('div.player .info > div::text').getall()[-1])[0])
            ),
            'overall_rating': int(response.css('section.spacing > div > div > div > span::text').getall()[0]),
            'potential_overall_rating': int(response.css('section.spacing > div > div > div > span::text').getall()[1]),
            'value_in_million_euros': _get_value_in_million_euros(
                response.css('section.spacing > div > div > div::text').getall()[2]
            ),
            'wage_in_thousand_euros': _get_clean_value(
                response.css('section.spacing > div > div > div::text').getall()[3]
            ),
            'preferred_foot': response.css('li.bp3-text-overflow-ellipsis::text').getall()[0],
            'weak_foot': int(response.css('li.bp3-text-overflow-ellipsis::text').getall()[1]),
            'skill_moves': int(response.css('li.bp3-text-overflow-ellipsis::text').getall()[3]),
            'international_reputation': int(response.css('li.bp3-text-overflow-ellipsis::text').getall()[5]),
            'work_rate': response.css('li.bp3-text-overflow-ellipsis > span::text').getall()[3],
            'body_type': response.css('li.bp3-text-overflow-ellipsis > span::text').getall()[4],
            'real_face': response.css('li.bp3-text-overflow-ellipsis > span::text').getall()[5],
            'release_clause': _get_clean_value(
                response.css('li.bp3-text-overflow-ellipsis > span::text').getall()[6]
            ),
            'specialities': [
                _create_speciality(speciality) for speciality in response.css('li.bp3-text-overflow-ellipsis > a').getall()
            ],
            'team_name': _get_team_info(response, 'div.player-card > h5 > a::text'),
            'team_url': '{sofifa_url}{team_url}'.format(
                sofifa_url=SOFIFA_URL,
                team_url=_get_team_info(response, 'div.player-card > h5 > a::attr(href)')
            ) if _get_team_info(response, 'div.player-card > h5 > a::attr(href)') else '',
            'team_image_url': _get_team_info(response, 'div.player-card > img::attr(data-src)'),
            'team_overall': int(_get_team_info(response, 'div.player-card > ul > li > span:nth-child(1)::text')) if _get_team_info(response, 'div.player-card > ul > li > span:nth-child(1)::text') else '',
            'team_position': _get_team_info(response, 'div.player-card > ul > li:nth-child(2) > span::text'),
            'team_jersey_number': int(_get_team_info(response, 'div.player-card > ul > li:nth-child(3)::text')) if _get_team_info(response, 'div.player-card > ul > li:nth-child(3)::text') else '',
            'joined': _get_joined(response),
            'loaned_from': _get_loaned_from(response),
            'loaned_from_team_url': _get_loaned_from_team_url(response),
            'contract_valid_until': _get_contract_valid_until(response),
            'national_team_name': _get_national_team_info(response, TEAM_NAMES_PATH),
            'national_team_url': _get_national_team_url(response),
            'national_team_image_url': _get_national_team_info(response, 'div.player-card > img::attr(data-src)'),
            'national_team_overall': int(_get_national_team_info(response, 'div.player-card > ul > li > span:nth-child(1)::text')) if _get_national_team_info(response, 'div.player-card > ul > li > span:nth-child(1)::text') else '',
            'national_team_position': _get_national_team_info(response, 'div.player-card > ul > li:nth-child(2) > span::text'),
            'national_team_jersey_number': int(_get_national_team_info(response, 'div.player-card > ul > li:nth-child(3)::text')) if _get_national_team_info(response, 'div.player-card > ul > li:nth-child(3)::text') else '',
        }


def _convert_feet_inches_to_meters(feet_inches):
    feet = int(feet_inches.split('\'')[0])
    inches = int(feet_inches.split('\'')[1].replace('"', ''))
    return round(Decimal((feet * 30.48 + inches * 2.54) / 100), 2)


def _convert_libras_to_kilograms(libras):
    return round(Decimal(libras / 2.205), 2)


def _get_value_in_million_euros(value):
    is_thousand = value[-1] == 'K'
    clean_value = _get_clean_value(value)
    if is_thousand:
        return round(clean_value / 1000, 2)
    return clean_value


def _get_clean_value(value):
    clean_value = value.replace('€', '')[0:-1]
    return Decimal(clean_value) if _is_a_number(clean_value) else ''


def _is_a_number(value):
    try:
        return bool(Decimal(value))
    except InvalidOperation:
        return False


def _create_speciality(a_element):
    return {
        'id': int(re.findall('(?<=5D=)[0-9]+', a_element)[0]),
        'link': '{url}/players?sc[]={id}'.format(url=SOFIFA_URL, id=re.findall('(?<=5D=)[0-9]+', a_element)[0]),
        'text': re.findall('(?<=#).+(?=<)', a_element)[0]
    }


def _get_team_info(response, info_path):
    if not response.css(info_path).getall():
        return ''

    if _first_column_is_a_national_team(response):
        if _has_second_team_column(response):
            has_information_in_both_teams = len(response.css(info_path).getall()) > 1
            if has_information_in_both_teams:
                return response.css(info_path).getall()[1]
            return response.css(info_path).getall()[0]
        return ''
    return response.css(info_path).getall()[0]


def _get_joined(response):
    if not _has_joined_or_loaned_info(response):
        return ''
    if _is_loaned(response.css('div.player-card > ul > li:nth-child(4)::text').getall()):
        return ''
    return _convert_date(_get_team_info(response, 'div.player-card > ul > li:nth-child(4)::text'))


def _get_loaned_from(response):
    if not _has_joined_or_loaned_info(response):
        return ''
    if not _is_loaned(response.css('div.player-card > ul > li:nth-child(4)::text').getall()):
        return ''
    return _get_team_info(response, 'div.player-card > ul > li:nth-child(4) a::text')


def _get_loaned_from_team_url(response):
    if not _has_joined_or_loaned_info(response):
        return ''
    if not _is_loaned(response.css('div.player-card > ul > li:nth-child(4)::text').getall()):
        return ''
    return '{sofifa_url}{team_url}'.format(
        sofifa_url=SOFIFA_URL,
        team_url=_get_team_info(response, 'div.player-card > ul > li:nth-child(4) a::attr(href)')
    )


def _has_joined_or_loaned_info(response):
    if len(response.css('div.player-card > ul > li:nth-child(4) > label::text').getall()) == 0:
        return False
    return response.css('div.player-card > ul > li:nth-child(4) > label::text').getall()[0] in ['Joined', 'Loaned From']


NATIONAL_TEAMS = [
    'France', 'Germany', 'Spain', 'Belgium', 'Italy', 'Portugal', 'Netherlands', 'England', 'Argentina', 'Brazil',
    'Colombia', 'Uruguay', 'Austria', 'Denmark', 'Switzerland', 'Mexico', 'Poland', 'Russia', 'Turkey', 'Chile',
    'Czech Republic', 'Norway', 'Scotland', 'Sweden', 'Côte d\'Ivoire', 'Republic of Ireland', 'Cameroon', 'Egypt',
    'Greece', 'Slovenia', 'Wales', 'Paraguay', 'United States', 'Peru', 'Romania', 'Hungary', 'Ecuador', 'Venezuela',
    'Iceland', 'Australia', 'Northern Ireland', 'Canada', 'South Africa', 'Finland', 'China PR', 'Bolivia', 'New Zealand',
    'Bulgaria', 'India'
]


TEAM_NAMES_PATH = 'div.player-card > h5 > a::text'


def _get_national_team_url(response):
    return '{sofifa_url}{team_url}'.format(
        sofifa_url=SOFIFA_URL,
        team_url=_get_national_team_info(response, 'div.player-card > h5 > a::attr(href)')
    ) if _get_national_team_info(response, 'div.player-card > h5 > a::attr(href)') else ''


def _get_national_team_info(response, info_path):
    if _has_second_team_column(response) and not _first_column_is_a_national_team(response):
        return response.css(info_path).getall()[1]
    if _first_column_is_a_national_team(response):
        return response.css(info_path).getall()[0]

    return ''


def _has_second_team_column(response):
    return len(response.css(TEAM_NAMES_PATH).getall()) > 1


def _first_column_is_a_national_team(response):
    if not response.css(TEAM_NAMES_PATH).getall():
        return ''
    first_team_name = response.css(TEAM_NAMES_PATH).getall()[0]
    return _is_a_national_team(first_team_name)


def _second_column_is_a_national_team(response):
    if not _has_second_team_column(response):
        return False
    return _is_a_national_team(response.css(TEAM_NAMES_PATH).getall()[1])


def _is_a_national_team(team):
    return team in NATIONAL_TEAMS


def _convert_date(sofifa_date):
    return datetime.strptime(sofifa_date, '%b %d, %Y').date() if sofifa_date else ''


def _is_loaned(element):
    return not bool(element)


def _get_contract_valid_until(response):
    if _has_joined_or_loaned_info(response) \
            and not _get_team_info(response, 'div.player-card > ul > li:nth-child(5)::text'):
        return ''
    value = _get_team_info(response, 'div.player-card > ul > li:nth-child(5)::text') \
        if _has_joined_or_loaned_info(response) \
        else _get_team_info(response, 'div.player-card > ul > li:nth-child(4)::text')
    is_a_year = _is_a_number(value)
    return date(int(value), 12, 31) if is_a_year else _convert_date(value)

