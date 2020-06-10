from unittest import TestCase
from unittest.mock import patch

import scrapy
from scrapy.http import HtmlResponse

from scrapy_sofifa.scrapy_sofifa.spiders.players_url_list import PlayersURLListSpider
from spiders.players_url_list import LAST_KNOWN_PAGE


class PlayersURLListTests(TestCase):
    def setUp(self):
        self.spider = PlayersURLListSpider()
        self.first_page_file = open('test_pages/test_first_page.htm', 'r')
        self.second_page_file = open('test_pages/test_second_page.htm', 'r')
        self.last_page_file = open('test_pages/test_last_page.htm', 'r')
        self.page_without_next_but_not_last_file = open('test_pages/test_page_without_next_but_not_last.htm', 'r')

    def tearDown(self):
        self.first_page_file.close()
        self.second_page_file.close()
        self.last_page_file.close()
        self.page_without_next_but_not_last_file.close()

    def test_return_the_url_of_the_first_player(self):
        urls = list(self.spider.parse(HtmlResponse(
            url='http://teste.com?offset=0',
            request=scrapy.Request(url='http://teste.com?offset=0'),
            body=self.first_page_file.read(),
            encoding='utf-8'
        )))

        self.assertEqual({'url': 'https://sofifa.com/player/226807/cristian-roldan/200044/'}, urls[0])

    @patch('scrapy_sofifa.scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_the_next_link_and_is_the_first_page(self, request_mock):
        list(self.spider.parse(HtmlResponse(
            url='http://teste.com?offset=0',
            request=scrapy.Request(url='http://teste.com?offset=0'),
            body=self.first_page_file.read(),
            encoding='utf-8'
        )))

        request_mock.assert_called_once_with('http://teste.com/?offset=60', callback=self.spider.parse)

    @patch('scrapy_sofifa.scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_the_next_link_and_is_the_second_page(self, request_mock):
        list(self.spider.parse(HtmlResponse(
            url='http://teste.com?offset=0',
            request=scrapy.Request(url='http://teste.com?offset=0'),
            body=self.second_page_file.read(),
            encoding='utf-8'
        )))

        request_mock.assert_called_once_with('http://teste.com/?offset=120', callback=self.spider.parse)

    @patch('scrapy_sofifa.scrapy_sofifa.spiders.players_url_list.Request')
    def test_create_the_next_request_when_it_has_not_the_next_link_but_its_not_the_last_known_page(self, request_mock):
        list(self.spider.parse(HtmlResponse(
            url='http://teste.com?offset=60',
            request=scrapy.Request(url='http://teste.com?offset=60'),
            body=self.page_without_next_but_not_last_file.read(),
            encoding='utf-8'
        )))

        request_mock.assert_called_once_with('http://teste.com/?offset=120', callback=self.spider.parse)

    @patch('scrapy_sofifa.scrapy_sofifa.spiders.players_url_list.Request')
    def test_doesnt_create_the_next_request_when_it_is_the_last_page(self, request_mock):
        list(self.spider.parse(HtmlResponse(
            url='http://teste.com?offset={last_page}'.format(last_page=LAST_KNOWN_PAGE),
            request=scrapy.Request(url='http://teste.com?offset={last_page}'.format(last_page=LAST_KNOWN_PAGE)),
            body=self.last_page_file.read(),
            encoding='utf-8'
        )))

        request_mock.assert_not_called()
