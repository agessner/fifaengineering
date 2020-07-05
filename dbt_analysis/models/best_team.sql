WITH
  players AS (
    SELECT
      team_name,
      overall_rating,
      players.version_name,
      urls.player_nickname AS name,
      team_position AS player_position,
      urls.value AS url,
      id,
      image_url
    FROM `sofifa.players_*` players
    JOIN `sofifa.urls_*` urls ON urls.player_id = players.id
    AND urls.version_id = players.version_id
    AND team_position != 'RES' AND team_position != 'SUB'
  ),
  max_rating_by_position AS (
    SELECT 
      team_name,
      player_position, 
      MAX(overall_rating) AS rating
    FROM players GROUP BY team_name, player_position
  ),
  players_in_each_position AS (
    SELECT
      MIN(players.version_name) AS first_version,
      players.team_name,
      players.player_position,
      players.overall_rating AS rating,
    FROM players
    GROUP BY team_name, player_position, rating
  ),
  positions_order AS (
      SELECT "GK" AS position, 1 AS position_order
      UNION ALL SELECT "SW" AS position, 2 AS position_order,
      UNION ALL SELECT "RWB" AS position, 3 AS position_order,
      UNION ALL SELECT "RB" AS position, 4 AS position_order,
      UNION ALL SELECT "RCB" AS position, 5 AS position_order,
      UNION ALL SELECT "CB" AS position, 6 AS position_order,
      UNION ALL SELECT "LCB" AS position, 7 AS position_order,
      UNION ALL SELECT "LB" AS position, 8 AS position_order,
      UNION ALL SELECT "LWB" AS position, 9 AS position_order,
      UNION ALL SELECT "RDM" AS position, 10 AS position_order,
      UNION ALL SELECT "CDM" AS position, 11 AS position_order,
      UNION ALL SELECT "LDM" AS position, 12 AS position_order,
      UNION ALL SELECT "RM" AS position, 13 AS position_order,
      UNION ALL SELECT "RCM" AS position, 14 AS position_order,
      UNION ALL SELECT "CM" AS position, 15 AS position_order,
      UNION ALL SELECT "LCM" AS position, 16 AS position_order,
      UNION ALL SELECT "LM" AS position, 17 AS position_order,
      UNION ALL SELECT "RW" AS position, 18 AS position_order,
      UNION ALL SELECT "RAM" AS position, 19 AS position_order,
      UNION ALL SELECT "CAM" AS position, 20 AS position_order,
      UNION ALL SELECT "LAM" AS position, 21 AS position_order,
      UNION ALL SELECT "LW" AS position, 22 AS position_order,
      UNION ALL SELECT "RF" AS position, 23 AS position_order,
      UNION ALL SELECT "RS" AS position, 24 AS position_order,
      UNION ALL SELECT "CF" AS position, 25 AS position_order,
      UNION ALL SELECT "LS" AS position, 26 AS position_order,
      UNION ALL SELECT "LF" AS position, 27 AS position_order,
      UNION ALL SELECT "ST" AS position, 28 AS position_order
  ),
  final_query AS (
    SELECT
      name,
      players.version_name,
      players.team_name,
      players.player_position,
      players.overall_rating,
      players.url,
      players.id,
      players.image_url,
      positions_order.position_order 
    FROM players
    LEFT JOIN positions_order ON positions_order.position = players.player_position
    JOIN max_rating_by_position 
    ON max_rating_by_position.player_position = players.player_position
    AND max_rating_by_position.team_name = players.team_name
    AND max_rating_by_position.rating = players.overall_rating
    JOIN players_in_each_position 
    ON players_in_each_position.player_position = players.player_position
    AND players_in_each_position.team_name = players.team_name
    AND players_in_each_position.rating = players.overall_rating
    AND players_in_each_position.first_version = players.version_name
  )

SELECT * FROM final_query