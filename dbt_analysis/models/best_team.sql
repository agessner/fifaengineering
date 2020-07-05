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
  final_query AS (
    SELECT
      name,
      players.version_name,
      players.team_name,
      players.player_position,
      players.overall_rating,
      players.url,
      players.id,
      players.image_url
    FROM players
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