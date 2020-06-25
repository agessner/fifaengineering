from datetime import date
from functools import partial
from unittest import TestCase

import scrapy
from scrapy.http import HtmlResponse

from spiders.versions import VersionsSpider


class VersionsTests(TestCase):
    def setUp(self):
        self.spider = VersionsSpider()
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://test.com',
            request=scrapy.Request(url='http://test.com'),
            encoding='utf-8'
        )

    def test_return_versions(self):
        with open('test_pages/versions/test_fifa_08_page.htm', 'r') as page:
            versions_returned = list(self.spider.parse(self.partial_html_response(
                body=page.read()
            )))

        versions_without_next_request = versions_returned[:-1]
        self.assertEqual(2, len(versions_without_next_request))
        self.assertEqual('FIFA 08', versions_without_next_request[0]['version_name'])
        self.assertEqual('080002', versions_without_next_request[0]['version_id'])
        self.assertEqual(date(2008, 2, 22), versions_without_next_request[0]['release_date'])
        self.assertEqual('FIFA 08', versions_without_next_request[1]['version_name'])
        self.assertEqual('080001', versions_without_next_request[1]['version_id'])
        self.assertEqual(date(2007, 8, 30), versions_without_next_request[1]['release_date'])

    def test_return_next_request(self):
        with open('test_pages/versions/test_fifa_08_page.htm', 'r') as page:
            versions_returned = list(self.spider.parse(self.partial_html_response(
                body=page.read()
            )))

        next_request = versions_returned[-1]
        self.assertEqual('https://sofifa.com/players?r=070002&set=true', next_request.url)

    def test_return_only_versions_when_is_the_last_page(self):
        with open('test_pages/versions/test_last_page.htm', 'r') as page:
            versions_returned = list(self.spider.parse(self.partial_html_response(
                body=page.read()
            )))

        self.assertEqual(2, len(versions_returned))
        self.assertEqual('FIFA 07', versions_returned[0]['version_name'])
        self.assertEqual('070002', versions_returned[0]['version_id'])
        self.assertEqual(date(2007, 2, 22), versions_returned[0]['release_date'])
        self.assertEqual('FIFA 07', versions_returned[1]['version_name'])
        self.assertEqual('070001', versions_returned[1]['version_id'])
        self.assertEqual(date(2006, 8, 30), versions_returned[1]['release_date'])
