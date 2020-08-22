WITH
  max_rating_by_position AS (
    SELECT 
      team_name,
      team_position, 
      MAX(overall_rating) AS rating
    FROM {{ref('players')}}
    WHERE team_position != 'RES' AND team_position != 'SUB'
    GROUP BY team_name, team_position
  ),
  players_from_max_rating AS (
    SELECT
        players.*
    FROM {{ref('unique_players_by_overall_first_version_and_team_position')}} players
    JOIN max_rating_by_position
    ON players.team_name = max_rating_by_position.team_name
    AND players.team_position = max_rating_by_position.team_position
    AND players.overall_rating = max_rating_by_position.rating
  ),
  second_max_rating_by_position AS (
    SELECT
        second_max_position.team_name,
        second_max_position.team_position,
        ARRAY_AGG(overall_rating ORDER BY overall_rating DESC LIMIT 1)[SAFE_OFFSET(0)] AS rating
    FROM {{ref('players')}} second_max_position
    JOIN max_rating_by_position
    ON second_max_position.team_name = max_rating_by_position.team_name
    AND second_max_position.team_position = max_rating_by_position.team_position
    AND second_max_position.overall_rating < max_rating_by_position.rating
    WHERE second_max_position.team_position != 'RES' AND second_max_position.team_position != 'SUB'
    GROUP BY team_name, team_position
  ),
  players_from_second_max_rating AS (
    SELECT
        players.*
    FROM {{ref('unique_players_by_overall_first_version_and_team_position')}} players
    JOIN second_max_rating_by_position
    ON players.team_name = second_max_rating_by_position.team_name
    AND players.team_position = second_max_rating_by_position.team_position
    AND players.overall_rating = second_max_rating_by_position.rating
  )

SELECT * FROM players_from_max_rating
UNION ALL
SELECT * FROM players_from_second_max_rating
