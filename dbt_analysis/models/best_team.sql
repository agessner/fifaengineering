WITH
  players AS (
    SELECT
      team_name,
      overall_rating,
      players.version_name,
      urls.player_nickname AS name,
      position AS player_position,
      urls.value AS url,
      id,
      image_url
    FROM `sofifa.players_*` players, UNNEST(positions) position
    JOIN `sofifa.urls_*` urls ON urls.player_id = players.id
    AND urls.version_id = players.version_id
  ),
  max_rating_by_position AS (
    SELECT team_name, player_position, MAX(overall_rating) AS rating FROM players GROUP BY team_name, player_position
  ),
  players_in_each_position AS (
    SELECT
      name,
      version_name,
      players.team_name,
      players.player_position,
      players.overall_rating,
      players.url,
      players.id,
      players.image_url
    FROM players
    JOIN max_rating_by_position ON max_rating_by_position.player_position = players.player_position
    AND max_rating_by_position.team_name = players.team_name
    AND max_rating_by_position.rating = players.overall_rating
  )

SELECT * FROM players_in_each_position