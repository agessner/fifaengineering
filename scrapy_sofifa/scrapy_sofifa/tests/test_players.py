from datetime import date
from decimal import Decimal
from functools import partial
from unittest import TestCase

import scrapy
from scrapy.http import HtmlResponse

from scrapy_sofifa.spiders.players import PlayersSpider


class PlayersURLListTests(TestCase):
    def setUp(self):
        self.spider = PlayersSpider(version='Fifa 20')
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://test.com',
            request=scrapy.Request(url='http://test.com'),
            encoding='utf-8'
        )

    def _parse(self, page):
        with open('test_pages/players/{page}'.format(page=page), 'r') as page:
            return next(self.spider.parse(self.partial_html_response(body=page.read())))

    def test_version_id(self):
        self.assertEqual('200048', self._parse('test_messi.htm')['version_id'])

    def test_version_name(self):
        self.assertEqual('20', self._parse('test_messi.htm')['version_name'])

    def test_id(self):
        self.assertEqual(158023, self._parse('test_messi.htm')['id'])

    def test_full_name(self):
        self.assertEqual('Lionel Andrés Messi Cuccittini', self._parse('test_messi.htm')['full_name'])

    def test_image_url(self):
        self.assertEqual(
            'https://cdn.sofifa.com/players/158/023/20_120.png',
            self._parse('test_messi.htm')['image_url']
        )

    def test_country(self):
        self.assertEqual('Argentina', self._parse('test_messi.htm')['country'])

    def test_country_image_url(self):
        self.assertEqual('https://cdn.sofifa.com/flags/ar.png', self._parse('test_messi.htm')['country_image_url'])

    def test_positions_when_multiple(self):
        self.assertEqual(['RW', 'ST', 'CF'], self._parse('test_messi.htm')['positions'])

    def test_positions_when_one(self):
        self.assertEqual(['ST'], self._parse('test_single_position.htm')['positions'])

    def test_age(self):
        self.assertEqual(32, self._parse('test_messi.htm')['age'])

    def test_birthdate(self):
        self.assertEqual(date(1987, 6, 24), self._parse('test_messi.htm')['birthdate'])

    def test_height_in_m(self):
        self.assertEqual(Decimal('1.70'), self._parse('test_messi.htm')['height_in_meters'])

    def test_weight_in_kg(self):
        self.assertEqual(Decimal('72.11'), self._parse('test_messi.htm')['weight_in_kg'])

    def test_overall_rating(self):
        self.assertEqual(94, self._parse('test_messi.htm')['overall_rating'])

    def test_potential_overall_rating(self):
        self.assertEqual(94, self._parse('test_messi.htm')['potential_overall_rating'])

    def test_value_in_million_euros(self):
        self.assertEqual(Decimal('95.5'), self._parse('test_messi.htm')['value_in_million_euros'])

    def test_value_in_million_euros_when_valued_lower_than_one_million(self):
        self.assertEqual(
            Decimal('0.35'),
            self._parse('test_less_than_one_million_value.htm')['value_in_million_euros']
        )

    def test_wage_in_thousand_euros(self):
        self.assertEqual(560, self._parse('test_messi.htm')['wage_in_thousand_euros'])

    def test_preferred_foot(self):
        self.assertEqual('Left', self._parse('test_messi.htm')['preferred_foot'])

    def test_weak_foot(self):
        self.assertEqual(4, self._parse('test_messi.htm')['weak_foot'])

    def test_skill_moves(self):
        self.assertEqual(4, self._parse('test_messi.htm')['skill_moves'])

    def test_international_reputation(self):
        self.assertEqual(5, self._parse('test_messi.htm')['international_reputation'])

    def test_work_rate(self):
        self.assertEqual('Medium/ Low', self._parse('test_messi.htm')['work_rate'])

    def test_body_type(self):
        self.assertEqual('Messi', self._parse('test_messi.htm')['body_type'])

    def test_body_type_when_no_body_type(self):
        self.assertEqual('', self._parse('test_fifa_07.htm')['body_type'])

    def test_real_face(self):
        self.assertEqual('Yes', self._parse('test_messi.htm')['real_face'])

    def test_real_face_when_no_real_face(self):
        self.assertEqual('', self._parse('test_fifa_07.htm')['real_face'])

    def test_release_clause(self):
        self.assertEqual(Decimal('195.8'), self._parse('test_messi.htm')['release_clause'])

    def test_release_clause_when_no_release_clause(self):
        self.assertEqual('', self._parse('test_no_release_clause.htm')['release_clause'])

    def test_release_clause_when_no_column_release_clause(self):
        self.assertEqual('', self._parse('test_fifa_07.htm')['release_clause'])

    def test_specialities(self):
        self.assertEqual(7, len(self._parse('test_messi.htm')['specialities']))

    def test_first_speciality(self):
        self.assertEqual({
            'id': 8,
            'link': 'https://sofifa.com/players?sc[]=8',
            'text': 'Dribbler'
        }, self._parse('test_messi.htm')['specialities'][0])

    def test_team_name(self):
        self.assertEqual('FC Barcelona', self._parse('test_messi.htm')['team_name'])

    def test_team_overall(self):
        self.assertEqual(86, self._parse('test_messi.htm')['team_overall'])

    def test_team_url(self):
        self.assertEqual('https://sofifa.com/team/241/fc-barcelona/', self._parse('test_messi.htm')['team_url'])

    def test_team_image_url(self):
        self.assertEqual(
            'https://cdn.sofifa.com/teams/241/light_60.png',
            self._parse('test_messi.htm')['team_image_url']
        )

    def test_team_position(self):
        self.assertEqual('RW', self._parse('test_messi.htm')['team_position'])

    def test_team_jersey_number(self):
        self.assertEqual(10, self._parse('test_messi.htm')['team_jersey_number'])

    def test_joined_when_not_loaned(self):
        self.assertEqual(date(2004, 7, 1), self._parse('test_messi.htm')['joined'])

    def test_joined_when_loaned(self):
        self.assertEqual('', self._parse('test_loaned.htm')['joined'])

    def test_loaned_from_when_not_loaned(self):
        self.assertEqual('', self._parse('test_messi.htm')['loaned_from'])

    def test_loaned_from_when_loaned(self):
        self.assertEqual('Napoli', self._parse('test_loaned.htm')['loaned_from'])

    def test_loaned_from_team_url_when_not_loaned(self):
        self.assertEqual('', self._parse('test_messi.htm')['loaned_from_team_url'])

    def test_loaned_from_team_url_from_when_loaned(self):
        self.assertEqual(
            'https://sofifa.com/team/48/napoli/',
            self._parse('test_loaned.htm')['loaned_from_team_url']
        )

    def test_contract_valid_until_when_year(self):
        self.assertEqual(date(2021, 12, 31), self._parse('test_messi.htm')['contract_valid_until'])

    def test_contract_valid_until_when_date(self):
        self.assertEqual(date(2020, 6, 30), self._parse('test_contract_valid_until_date_format.htm')['contract_valid_until'])

    def test_national_team_name_when_national_team(self):
        self.assertEqual('Argentina', self._parse('test_messi.htm')['national_team_name'])

    def test_national_team_name_when_no_national_team(self):
        self.assertEqual('', self._parse('test_no_national_team.htm')['national_team_name'])

    def test_national_team_url_when_national_team(self):
        self.assertEqual('https://sofifa.com/team/1369/argentina/', self._parse('test_messi.htm')['national_team_url'])

    def test_national_team_url_when_no_national_team(self):
        self.assertEqual('', self._parse('test_no_national_team.htm')['national_team_url'])

    def test_national_team_image_url_when_national_team(self):
        self.assertEqual('https://cdn.sofifa.com/teams/1369/light_60.png', self._parse('test_messi.htm')['national_team_image_url'])

    def test_national_team_image_url_when_no_national_team(self):
        self.assertEqual('', self._parse('test_no_national_team.htm')['national_team_image_url'])

    def test_national_team_overall_when_national_team(self):
        self.assertEqual(82, self._parse('test_messi.htm')['national_team_overall'])

    def test_national_team_overall_when_no_national_team(self):
        self.assertEqual('', self._parse('test_no_national_team.htm')['national_team_overall'])

    def test_national_team_position_when_national_team(self):
        self.assertEqual('RS', self._parse('test_messi.htm')['national_team_position'])

    def test_national_team_position_when_no_national_team(self):
        self.assertEqual('', self._parse('test_no_national_team.htm')['national_team_position'])

    def test_national_team_jersey_number_when_national_team(self):
        self.assertEqual(10, self._parse('test_messi.htm')['national_team_jersey_number'])

    def test_national_team_jersey_number_when_no_national_team(self):
        self.assertEqual('', self._parse('test_no_national_team.htm')['national_team_jersey_number'])

    def test_best_position(self):
        self.assertEqual('RW', self._parse('test_messi.htm')['best_position'])

    def test_crossing(self):
        self.assertEqual(88, self._parse('test_messi.htm')['crossing'])

    def test_finishing(self):
        self.assertEqual(95, self._parse('test_messi.htm')['finishing'])

    def test_heading_accuracy(self):
        self.assertEqual(70, self._parse('test_messi.htm')['heading_accuracy'])

    def test_short_passing(self):
        self.assertEqual(92, self._parse('test_messi.htm')['short_passing'])

    def test_volleys(self):
        self.assertEqual(88, self._parse('test_messi.htm')['volleys'])

    def test_dribbling(self):
        self.assertEqual(97, self._parse('test_messi.htm')['dribbling'])

    def test_curve(self):
        self.assertEqual(93, self._parse('test_messi.htm')['curve'])

    def test_fk_accuracy(self):
        self.assertEqual(94, self._parse('test_messi.htm')['fk_accuracy'])

    def test_long_passing(self):
        self.assertEqual(92, self._parse('test_messi.htm')['long_passing'])

    def test_ball_control(self):
        self.assertEqual(96, self._parse('test_messi.htm')['ball_control'])

    def test_acceleration(self):
        self.assertEqual(91, self._parse('test_messi.htm')['acceleration'])

    def test_sprint_speed(self):
        self.assertEqual(84, self._parse('test_messi.htm')['sprint_speed'])

    def test_agility(self):
        self.assertEqual(93, self._parse('test_messi.htm')['agility'])

    def test_reactions(self):
        self.assertEqual(95, self._parse('test_messi.htm')['reactions'])

    def test_balance(self):
        self.assertEqual(95, self._parse('test_messi.htm')['balance'])

    def test_shot_power(self):
        self.assertEqual(86, self._parse('test_messi.htm')['shot_power'])

    def test_jumping(self):
        self.assertEqual(68, self._parse('test_messi.htm')['jumping'])

    def test_stamina(self):
        self.assertEqual(75, self._parse('test_messi.htm')['stamina'])

    def test_strength(self):
        self.assertEqual(68, self._parse('test_messi.htm')['strength'])

    def test_long_shots(self):
        self.assertEqual(94, self._parse('test_messi.htm')['long_shots'])

    def test_aggression(self):
        self.assertEqual(48, self._parse('test_messi.htm')['aggression'])

    def test_interceptions(self):
        self.assertEqual(40, self._parse('test_messi.htm')['interceptions'])

    def test_positioning(self):
        self.assertEqual(94, self._parse('test_messi.htm')['positioning'])

    def test_vision(self):
        self.assertEqual(94, self._parse('test_messi.htm')['vision'])

    def test_penalties(self):
        self.assertEqual(75, self._parse('test_messi.htm')['penalties'])

    def test_composure(self):
        self.assertEqual(96, self._parse('test_messi.htm')['composure'])

    def test_defensive_awareness(self):
        self.assertEqual(33, self._parse('test_messi.htm')['defensive_awareness'])

    def test_standing_tackle(self):
        self.assertEqual(37, self._parse('test_messi.htm')['standing_tackle'])

    def test_sliding_tackle(self):
        self.assertEqual(26, self._parse('test_messi.htm')['sliding_tackle'])

    def test_gk_diving(self):
        self.assertEqual(6, self._parse('test_messi.htm')['gk_diving'])

    def test_gk_handling(self):
        self.assertEqual(11, self._parse('test_messi.htm')['gk_handling'])

    def test_gk_kicking(self):
        self.assertEqual(15, self._parse('test_messi.htm')['gk_kicking'])

    def test_gk_positioning(self):
        self.assertEqual(14, self._parse('test_messi.htm')['gk_positioning'])

    def test_gk_reflexes(self):
        self.assertEqual(8, self._parse('test_messi.htm')['gk_reflexes'])


class WhenNoTeamOnlyNationalTests(TestCase):
    def setUp(self):
        self.spider = PlayersSpider()
        self.spider.version = ''
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://test.com',
            request=scrapy.Request(url='http://test.com'),
            encoding='utf-8'
        )

    def _parse(self, page):
        with open('test_pages/players/{page}'.format(page=page), 'r') as page:
            return next(self.spider.parse(self.partial_html_response(body=page.read())))

    def test_team_name(self):
        self.assertEqual('', self._parse('test_no_team.htm')['team_name'])

    def test_team_url(self):
        self.assertEqual('', self._parse('test_no_team.htm')['team_url'])

    def test_team_image_url(self):
        self.assertEqual('', self._parse('test_no_team.htm')['team_image_url'])

    def test_team_overall(self):
        self.assertEqual('', self._parse('test_no_team.htm')['team_overall'])

    def test_team_position(self):
        self.assertEqual('', self._parse('test_no_team.htm')['team_position'])

    def test_team_jersey_number(self):
        self.assertEqual('', self._parse('test_no_team.htm')['team_jersey_number'])

    def test_joined(self):
        self.assertEqual('', self._parse('test_no_team.htm')['joined'])

    def test_loaned_from(self):
        self.assertEqual('', self._parse('test_no_team.htm')['loaned_from'])

    def test_loaned_from_team_url(self):
        self.assertEqual('', self._parse('test_no_team.htm')['loaned_from_team_url'])

    def test_contract_valid_until(self):
        self.assertEqual('', self._parse('test_no_team.htm')['contract_valid_until'])

    def test_national_team_name(self):
        self.assertEqual('Romania', self._parse('test_no_team.htm')['national_team_name'])

    def test_national_team_url(self):
        self.assertEqual('https://sofifa.com/team/1356/romania/', self._parse('test_no_team.htm')['national_team_url'])

    def test_national_team_image_url(self):
        self.assertEqual('https://cdn.sofifa.com/teams/1356/light_60.png', self._parse('test_no_team.htm')['national_team_image_url'])

    def test_national_team_overall(self):
        self.assertEqual(73, self._parse('test_no_team.htm')['national_team_overall'])

    def test_national_team_position(self):
        self.assertEqual('RCB', self._parse('test_no_team.htm')['national_team_position'])

    def test_national_team_jersey_number(self):
        self.assertEqual(4, self._parse('test_no_team.htm')['national_team_jersey_number'])


class WhenTeamAndNationalTeamAreSwitchedTests(TestCase):
    def setUp(self):
        self.spider = PlayersSpider()
        self.spider.version = ''
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://test.com',
            request=scrapy.Request(url='http://test.com'),
            encoding='utf-8'
        )

    def _parse(self, page):
        with open('test_pages/players/{page}'.format(page=page), 'r') as page:
            return next(self.spider.parse(self.partial_html_response(body=page.read())))

    def test_team_name(self):
        self.assertEqual('FCSB (Steaua)', self._parse('test_switched_national_and_team_places.htm')['team_name'])

    def test_team_url(self):
        self.assertEqual('https://sofifa.com/team/100761/fcsb-steaua/', self._parse('test_switched_national_and_team_places.htm')['team_url'])

    def test_team_image_url(self):
        self.assertEqual('https://cdn.sofifa.com/teams/100761/light_60.png', self._parse('test_switched_national_and_team_places.htm')['team_image_url'])

    def test_team_overall(self):
        self.assertEqual(70, self._parse('test_switched_national_and_team_places.htm')['team_overall'])

    def test_team_position(self):
        self.assertEqual('LW', self._parse('test_switched_national_and_team_places.htm')['team_position'])

    def test_team_jersey_number(self):
        self.assertEqual(7, self._parse('test_switched_national_and_team_places.htm')['team_jersey_number'])

    def test_joined(self):
        self.assertEqual(date(2017, 8, 1), self._parse('test_switched_national_and_team_places.htm')['joined'])

    def test_loaned_from(self):
        self.assertEqual('', self._parse('test_switched_national_and_team_places.htm')['loaned_from'])

    def test_loaned_from_team_url(self):
        self.assertEqual('', self._parse('test_switched_national_and_team_places.htm')['loaned_from_team_url'])

    def test_contract_valid_until(self):
        self.assertEqual(date(2022, 12, 31), self._parse('test_switched_national_and_team_places.htm')['contract_valid_until'])

    def test_national_team_name(self):
        self.assertEqual('Romania', self._parse('test_switched_national_and_team_places.htm')['national_team_name'])

    def test_national_team_url(self):
        self.assertEqual('https://sofifa.com/team/1356/romania/', self._parse('test_switched_national_and_team_places.htm')['national_team_url'])

    def test_national_team_image_url(self):
        self.assertEqual('https://cdn.sofifa.com/teams/1356/light_60.png', self._parse('test_switched_national_and_team_places.htm')['national_team_image_url'])

    def test_national_team_overall(self):
        self.assertEqual(73, self._parse('test_switched_national_and_team_places.htm')['national_team_overall'])

    def test_national_team_position(self):
        self.assertEqual('SUB', self._parse('test_switched_national_and_team_places.htm')['national_team_position'])

    def test_national_team_jersey_number(self):
        self.assertEqual(7, self._parse('test_switched_national_and_team_places.htm')['national_team_jersey_number'])


class WhenNoTeamNorNationalTeamTests(TestCase):
    def setUp(self):
        self.spider = PlayersSpider()
        self.spider.version = ''
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://test.com',
            request=scrapy.Request(url='http://test.com'),
            encoding='utf-8'
        )

    def _parse(self, page):
        with open('test_pages/players/{page}'.format(page=page), 'r') as page:
            return next(self.spider.parse(self.partial_html_response(body=page.read())))

    def test_team_name(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['team_name'])

    def test_team_url(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['team_url'])

    def test_team_image_url(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['team_image_url'])

    def test_team_overall(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['team_overall'])

    def test_team_position(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['team_position'])

    def test_team_jersey_number(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['team_jersey_number'])

    def test_joined(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['joined'])

    def test_loaned_from(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['loaned_from'])

    def test_loaned_from_team_url(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['loaned_from_team_url'])

    def test_contract_valid_until(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['contract_valid_until'])

    def test_national_team_name(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['national_team_name'])

    def test_national_team_url(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['national_team_url'])

    def test_national_team_image_url(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['national_team_image_url'])

    def test_national_team_overall(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['national_team_overall'])

    def test_national_team_position(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['national_team_position'])

    def test_national_team_jersey_number(self):
        self.assertEqual('', self._parse('test_no_tem_nor_national_team.htm')['national_team_jersey_number'])


class WhenNoJoinedDateNorLoanedTests(TestCase):
    def setUp(self):
        self.spider = PlayersSpider()
        self.spider.version = ''
        self.partial_html_response = partial(
            HtmlResponse,
            url='http://test.com',
            request=scrapy.Request(url='http://test.com'),
            encoding='utf-8'
        )

    def _parse(self, page):
        with open('test_pages/players/{page}'.format(page=page), 'r') as page:
            return next(self.spider.parse(self.partial_html_response(body=page.read())))

    def test_joined(self):
        self.assertEqual('', self._parse('test_no_joined_date_nor_loaned.htm')['joined'])

    def test_loaned_from(self):
        self.assertEqual('', self._parse('test_no_joined_date_nor_loaned.htm')['loaned_from'])

    def test_loaned_from_team_url(self):
        self.assertEqual('', self._parse('test_no_joined_date_nor_loaned.htm')['loaned_from_team_url'])

    def test_contract_valid_until_when_year(self):
        self.assertEqual(date(2009, 12, 31), self._parse('test_no_joined_date_nor_loaned.htm')['contract_valid_until'])
