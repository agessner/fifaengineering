from functools import partial
from unittest import TestCase
from unittest.mock import patch

import scrapy
from scrapy.http import HtmlResponse

from scrapy_sofifa.spiders.players_url_list import PlayersURLListSpider, LAST_KNOWN_PAGE


class PlayersURLListTests(TestCase):
    def setUp(self):
        self.spider = PlayersURLListSpider(version='20')
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://teste.com?offset=60&r=10',
            request=scrapy.Request(url='http://teste.com?offset=60&r=10'),
            encoding='utf-8'
        )

    def _parse(self, page):
        with open('test_pages/urls/{page}'.format(page=page), 'r') as file:
            urls = list(self.spider.parse(self.partial_html_response(body=file.read())))

        urls_without_next_page_url = list(urls)[0:-1]
        return urls_without_next_page_url

    def test_return_the_first_player(self):
        urls = self._parse('test_first_page.htm')

        self.assertEqual(60, len(urls))
        self.assertEqual('https://sofifa.com/player/226807/cristian-roldan/200044/', urls[0]['value'])
        self.assertEqual(226807, urls[0]['player_id'])
        self.assertEqual('C. Roldan', urls[0]['player_nickname'])
        self.assertEqual('200044', urls[0]['version_id'])
        self.assertEqual('20', urls[0]['version_name'])

    def test_return_players_even_when_nickname_is_empty(self):
        urls = self._parse('test_no_name.htm')

        self.assertEqual(60, len(urls))

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_the_next_link_and_is_the_first_page(self, request_mock):
        self._parse('test_first_page.htm')

        request_mock.assert_called_once_with('http://teste.com/?offset=60', callback=self.spider.parse)

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_the_next_link_and_is_the_second_page(self, request_mock):
        self._parse('test_second_page.htm')

        request_mock.assert_called_once_with('http://teste.com/?offset=120', callback=self.spider.parse)

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_not_the_next_link_but_its_not_the_last_known_page(self, request_mock):
        self._parse('test_page_without_next_but_not_last.htm')

        request_mock.assert_called_once_with('http://teste.com?r=10&set=true&offset=120', callback=self.spider.parse)

    @patch('scrapy_sofifa.spiders.players_url_list.Request')
    def test_doesnt_create_the_next_request_when_it_is_the_last_page(self, request_mock):
        with open('test_pages/urls/test_last_page.htm', 'r') as file:
            list(self.spider.parse(HtmlResponse(
                url='http://teste.com?offset={last_page}'.format(last_page=LAST_KNOWN_PAGE['20']),
                request=scrapy.Request(url='http://teste.com?offset={last_page}'.format(last_page=LAST_KNOWN_PAGE['20'])),
                body=file.read(),
                encoding='utf-8'
            )))

        request_mock.assert_not_called()
