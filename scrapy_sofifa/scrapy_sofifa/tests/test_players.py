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

    def test_version_id(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('200048', player['version_id'])

    def test_version_name(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Fifa 20', player['version_name'])

    def test_id(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(158023, player['id'])

    def test_full_name(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Lionel Andrés Messi Cuccittini', player['full_name'])

    def test_image_url(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/players/158/023/20_120.png', player['image_url'])

    def test_country(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Argentina', player['country'])

    def test_country_image_url(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/flags/ar.png', player['country_image_url'])

    def test_positions_when_multiple(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(['RW', 'ST', 'CF'], player['positions'])

    def test_positions_when_one(self):
        with open('test_pages/players/test_single_position.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(['ST'], player['positions'])

    def test_age(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(32, player['age'])

    def test_birthdate(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(1987, 6, 24), player['birthdate'])

    def test_height_in_m(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(Decimal('1.70'), player['height_in_meters'])

    def test_weight_in_kg(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(Decimal('72.11'), player['weight_in_kg'])

    def test_overall_rating(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(94, player['overall_rating'])

    def test_potential_overall_rating(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(94, player['potential_overall_rating'])

    def test_value_in_million_euros(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(Decimal('95.5'), player['value_in_million_euros'])

    def test_value_in_million_euros_when_valued_lower_than_one_million(self):
        with open('test_pages/players/test_less_than_one_million_value.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(Decimal('0.35'), player['value_in_million_euros'])

    def test_wage_in_thousand_euros(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(560, player['wage_in_thousand_euros'])

    def test_preferred_foot(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Left', player['preferred_foot'])

    def test_weak_foot(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(4, player['weak_foot'])

    def test_skill_moves(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(4, player['skill_moves'])

    def test_international_reputation(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(5, player['international_reputation'])

    def test_work_rate(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Medium/ Low', player['work_rate'])

    def test_body_type(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Messi', player['body_type'])

    def test_body_type_when_no_body_type(self):
        with open('test_pages/players/test_fifa_07.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['body_type'])

    def test_real_face(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Yes', player['real_face'])

    def test_real_face_when_no_real_face(self):
        with open('test_pages/players/test_fifa_07.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['real_face'])

    def test_release_clause(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(Decimal('195.8'), player['release_clause'])

    def test_release_clause_when_no_release_clause(self):
        with open('test_pages/players/test_no_release_clause.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['release_clause'])

    def test_release_clause_when_no_column_release_clause(self):
        with open('test_pages/players/test_fifa_07.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['release_clause'])

    def test_specialities(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(7, len(player['specialities']))

    def test_first_speciality(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual({
            'id': 8,
            'link': 'https://sofifa.com/players?sc[]=8',
            'text': 'Dribbler'
        }, player['specialities'][0])

    def test_team_name(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('FC Barcelona', player['team_name'])

    def test_team_overall(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(86, player['team_overall'])

    def test_team_url(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://sofifa.com/team/241/fc-barcelona/', player['team_url'])

    def test_team_image_url(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/teams/241/light_60.png', player['team_image_url'])

    def test_team_position(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('RW', player['team_position'])

    def test_team_jersey_number(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(10, player['team_jersey_number'])

    def test_joined_when_not_loaned(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(2004, 7, 1), player['joined'])

    def test_joined_when_loaned(self):
        with open('test_pages/players/test_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['joined'])

    def test_loaned_from_when_not_loaned(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from'])

    def test_loaned_from_when_loaned(self):
        with open('test_pages/players/test_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Napoli', player['loaned_from'])

    def test_loaned_from_team_url_when_not_loaned(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from_team_url'])

    def test_loaned_from_team_url_from_when_loaned(self):
        with open('test_pages/players/test_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://sofifa.com/team/48/napoli/', player['loaned_from_team_url'])

    def test_contract_valid_until_when_year(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(2021, 12, 31), player['contract_valid_until'])

    def test_contract_valid_until_when_date(self):
        with open('test_pages/players/test_contract_valid_until_date_format.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(2020, 6, 30), player['contract_valid_until'])

    def test_national_team_name_when_national_team(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Argentina', player['national_team_name'])

    def test_national_team_name_when_no_national_team(self):
        with open('test_pages/players/test_no_national_team.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_name'])

    def test_national_team_url_when_national_team(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://sofifa.com/team/1369/argentina/', player['national_team_url'])

    def test_national_team_url_when_no_national_team(self):
        with open('test_pages/players/test_no_national_team.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_url'])

    def test_national_team_image_url_when_national_team(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/teams/1369/light_60.png', player['national_team_image_url'])

    def test_national_team_image_url_when_no_national_team(self):
        with open('test_pages/players/test_no_national_team.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_image_url'])

    def test_national_team_overall_when_national_team(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(82, player['national_team_overall'])

    def test_national_team_overall_when_no_national_team(self):
        with open('test_pages/players/test_no_national_team.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_overall'])

    def test_national_team_position_when_national_team(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('RS', player['national_team_position'])

    def test_national_team_position_when_no_national_team(self):
        with open('test_pages/players/test_no_national_team.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_position'])

    def test_national_team_jersey_number_when_national_team(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(10, player['national_team_jersey_number'])

    def test_national_team_jersey_number_when_no_national_team(self):
        with open('test_pages/players/test_no_national_team.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_jersey_number'])

    def test_best_position(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('RW', player['best_position'])

    def test_crossing(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(88, player['crossing'])

    def test_finishing(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(95, player['finishing'])

    def test_heading_accuracy(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(70, player['heading_accuracy'])

    def test_short_passing(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(92, player['short_passing'])

    def test_volleys(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(88, player['volleys'])

    def test_dribbling(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(97, player['dribbling'])

    def test_curve(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(93, player['curve'])

    def test_fk_accuracy(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(94, player['fk_accuracy'])

    def test_long_passing(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(92, player['long_passing'])

    def test_ball_control(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(96, player['ball_control'])

    def test_acceleration(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(91, player['acceleration'])

    def test_sprint_speed(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(84, player['sprint_speed'])

    def test_agility(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(93, player['agility'])

    def test_reactions(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(95, player['reactions'])

    def test_balance(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(95, player['balance'])

    def test_shot_power(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(86, player['shot_power'])

    def test_jumping(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(68, player['jumping'])

    def test_stamina(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(75, player['stamina'])

    def test_strength(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(68, player['strength'])

    def test_long_shots(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(94, player['long_shots'])

    def test_aggression(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(48, player['aggression'])

    def test_interceptions(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(40, player['interceptions'])

    def test_positioning(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(94, player['positioning'])

    def test_vision(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(94, player['vision'])

    def test_penalties(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(75, player['penalties'])

    def test_composure(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(96, player['composure'])

    def test_defensive_awareness(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(33, player['defensive_awareness'])

    def test_standing_tackle(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(37, player['standing_tackle'])

    def test_sliding_tackle(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(26, player['sliding_tackle'])

    def test_gk_diving(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(6, player['gk_diving'])

    def test_gk_handling(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(11, player['gk_handling'])

    def test_gk_kicking(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(15, player['gk_kicking'])

    def test_gk_positioning(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(14, player['gk_positioning'])

    def test_gk_reflexes(self):
        with open('test_pages/players/test_messi.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(8, player['gk_reflexes'])


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

    def test_team_name(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_name'])

    def test_team_url(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_url'])

    def test_team_image_url(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_image_url'])

    def test_team_overall(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_overall'])

    def test_team_position(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_position'])

    def test_team_jersey_number(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_jersey_number'])

    def test_joined(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['joined'])

    def test_loaned_from(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from'])

    def test_loaned_from_team_url(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from_team_url'])

    def test_contract_valid_until(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['contract_valid_until'])

    def test_national_team_name(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Romania', player['national_team_name'])

    def test_national_team_url(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://sofifa.com/team/1356/romania/', player['national_team_url'])

    def test_national_team_image_url(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/teams/1356/light_60.png', player['national_team_image_url'])

    def test_national_team_overall(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(73, player['national_team_overall'])

    def test_national_team_position(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('RCB', player['national_team_position'])

    def test_national_team_jersey_number(self):
        with open('test_pages/players/test_no_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(4, player['national_team_jersey_number'])


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

    def test_team_name(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('FCSB (Steaua)', player['team_name'])

    def test_team_url(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://sofifa.com/team/100761/fcsb-steaua/', player['team_url'])

    def test_team_image_url(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/teams/100761/light_60.png', player['team_image_url'])

    def test_team_overall(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(70, player['team_overall'])

    def test_team_position(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('LW', player['team_position'])

    def test_team_jersey_number(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(7, player['team_jersey_number'])

    def test_joined(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(2017, 8, 1), player['joined'])

    def test_loaned_from(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from'])

    def test_loaned_from_team_url(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from_team_url'])

    def test_contract_valid_until(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(2022, 12, 31), player['contract_valid_until'])

    def test_national_team_name(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('Romania', player['national_team_name'])

    def test_national_team_url(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://sofifa.com/team/1356/romania/', player['national_team_url'])

    def test_national_team_image_url(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('https://cdn.sofifa.com/teams/1356/light_60.png', player['national_team_image_url'])

    def test_national_team_overall(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(73, player['national_team_overall'])

    def test_national_team_position(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('SUB', player['national_team_position'])

    def test_national_team_jersey_number(self):
        with open('test_pages/players/test_switched_national_and_team_places.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(7, player['national_team_jersey_number'])


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

    def test_team_name(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_name'])

    def test_team_url(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_url'])

    def test_team_image_url(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_image_url'])

    def test_team_overall(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_overall'])

    def test_team_position(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_position'])

    def test_team_jersey_number(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['team_jersey_number'])

    def test_joined(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['joined'])

    def test_loaned_from(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from'])

    def test_loaned_from_team_url(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from_team_url'])

    def test_contract_valid_until(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['contract_valid_until'])

    def test_national_team_name(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_name'])

    def test_national_team_url(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_url'])

    def test_national_team_image_url(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_image_url'])

    def test_national_team_overall(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_overall'])

    def test_national_team_position(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_position'])

    def test_national_team_jersey_number(self):
        with open('test_pages/players/test_no_tem_nor_national_team.htm') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['national_team_jersey_number'])


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

    def test_joined(self):
        with open('test_pages/players/test_no_joined_date_nor_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['joined'])

    def test_loaned_from(self):
        with open('test_pages/players/test_no_joined_date_nor_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from'])

    def test_loaned_from_team_url(self):
        with open('test_pages/players/test_no_joined_date_nor_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual('', player['loaned_from_team_url'])

    def test_contract_valid_until_when_year(self):
        with open('test_pages/players/test_no_joined_date_nor_loaned.htm', 'r') as page:
            player = next(self.spider.parse(self.partial_html_response(body=page.read())))

        self.assertEqual(date(2009, 12, 31), player['contract_valid_until'])
