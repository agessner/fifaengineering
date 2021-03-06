WITH
  min_rating_by_position AS (
    SELECT 
      team_name,
      team_position, 
      MIN(overall_rating) AS rating
    FROM {{ref('players')}}
    WHERE team_position != 'RES' AND team_position != 'SUB'
    GROUP BY team_name, team_position
  ),
  players_from_min_rating AS (
    SELECT
        players.*
    FROM {{ref('unique_players_by_overall_first_version_and_team_position')}} players
    JOIN min_rating_by_position
    ON players.team_name = min_rating_by_position.team_name
    AND players.team_position = min_rating_by_position.team_position
    AND players.overall_rating = min_rating_by_position.rating
  ),
  duplicated_players AS (
    SELECT id, team_name, MAX(overall_rating) AS rating, ARRAY_AGG(DISTINCT team_position) AS team_positions FROM players_from_min_rating GROUP BY id, team_name HAVING COUNT(*) > 1
  ),
  duplicated_players_by_position AS (
    SELECT id, team_name, rating, team_position FROM duplicated_players, UNNEST(team_positions) AS team_position
  ),
  second_min_rating_by_position AS (
    SELECT
        second_min_position.team_name,
        second_min_position.team_position,
        ARRAY_AGG(overall_rating ORDER BY overall_rating LIMIT 1)[SAFE_OFFSET(0)] AS rating
    FROM {{ref('players')}} second_min_position
    JOIN duplicated_players_by_position
    ON second_min_position.team_name = duplicated_players_by_position.team_name
    AND second_min_position.team_position = duplicated_players_by_position.team_position
    AND second_min_position.overall_rating > duplicated_players_by_position.rating
    AND second_min_position.id != duplicated_players_by_position.id
    WHERE second_min_position.team_position != 'RES' AND second_min_position.team_position != 'SUB'
    GROUP BY team_name, team_position
  ),
  players_from_second_min_rating AS (
    SELECT
        players.*
    FROM {{ref('unique_players_by_overall_first_version_and_team_position')}} players
    JOIN second_min_rating_by_position
    ON players.team_name = second_min_rating_by_position.team_name
    AND players.team_position = second_min_rating_by_position.team_position
    AND players.overall_rating = second_min_rating_by_position.rating
  )

SELECT * FROM players_from_min_rating
UNION ALL
SELECT * FROM players_from_second_min_rating
