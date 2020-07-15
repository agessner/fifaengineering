WITH
  players AS (
      SELECT
          players.id,
          players.name,
          players.age,
          players.team_name,
          players.player_position,
          players.potential_overall_rating AS rating,
          players.version_name,
          players.image_url,
          players.team_image_url
      FROM {{ref('players')}} players
  ),
  max_rating_by_position AS (
    SELECT 
      team_name,
      player_position,
      version_name,
      MAX(rating) AS rating
    FROM players 
    GROUP BY team_name, player_position, version_name
  )

SELECT
  players.* 
FROM players JOIN max_rating_by_position
ON max_rating_by_position.player_position = players.player_position
AND max_rating_by_position.team_name = players.team_name
AND max_rating_by_position.rating = players.rating
AND max_rating_by_position.version_name = players.version_name