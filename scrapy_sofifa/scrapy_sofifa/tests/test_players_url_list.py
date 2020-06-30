from functools import partial
from unittest import TestCase
from unittest.mock import patch

import scrapy
from scrapy.http import HtmlResponse

from scrapy_sofifa.spiders.players_url_list import PlayersURLListSpider, LAST_KNOWN_PAGE


class PlayersURLListTests(TestCase):
    def setUp(self):
        self.spider = PlayersURLListSpider(version='Fifa 20')
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://teste.com?offset=60&r=10',
            request=scrapy.Request(url='http://teste.com?offset=60&r=10'),
            encoding='utf-8'
        )

    def test_return_the_first_player(self):
        with open('test_pages/urls/test_first_page.htm', 'r') as file:
            urls = list(self.spider.parse(self.partial_html_response(body=file.read())))

        self.assertEqual(61, len(urls))
        self.assertEqual('https://sofifa.com/player/226807/cristian-roldan/200044/', urls[0]['value'])
        self.assertEqual(226807, urls[0]['player_id'])
        self.assertEqual('C. Roldan', urls[0]['player_nickname'])
        self.assertEqual('200044', urls[0]['version_id'])
        self.assertEqual('Fifa 20', urls[0]['version_name'])

    def test_return_players_when_nickname_is_empty(self):
        with open('test_pages/urls/test_no_name.htm', 'r') as file:
            urls = list(self.spider.parse(self.partial_html_response(body=file.read())))

        self.assertEqual(61, len(urls))

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_the_next_link_and_is_the_first_page(self, request_mock):
        with open('test_pages/urls/test_first_page.htm', 'r') as file:
            list(self.spider.parse(self.partial_html_response(body=file.read())))

        request_mock.assert_called_once_with('http://teste.com/?offset=60', callback=self.spider.parse)

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_the_next_link_and_is_the_second_page(self, request_mock):
        with open('test_pages/urls/test_second_page.htm', 'r') as file:
            list(self.spider.parse(self.partial_html_response(body=file.read())))

        request_mock.assert_called_once_with('http://teste.com/?offset=120', callback=self.spider.parse)

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_not_the_next_link_but_its_not_the_last_known_page(self, request_mock):
        with open('test_pages/urls/test_page_without_next_but_not_last.htm', 'r') as file:
            list(self.spider.parse(self.partial_html_response(body=file.read())))

        request_mock.assert_called_once_with('http://teste.com?r=10&set=true&offset=120', callback=self.spider.parse)

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_doesnt_create_the_next_request_when_it_is_the_last_page(self, request_mock):
        with open('test_pages/urls/test_last_page.htm', 'r') as file:
            list(self.spider.parse(HtmlResponse(
                url='http://teste.com?offset={last_page}'.format(last_page=LAST_KNOWN_PAGE),
                request=scrapy.Request(url='http://teste.com?offset={last_page}'.format(last_page=LAST_KNOWN_PAGE)),
                body=file.read(),
                encoding='utf-8'
            )))

        request_mock.assert_not_called()
