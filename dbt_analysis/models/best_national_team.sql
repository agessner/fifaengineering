WITH
  max_rating_by_position AS (
    SELECT 
      country,
      player_position, 
      MAX(overall_rating) AS rating
    FROM {{ref('unique_players_by_national_team_rating_and_position')}} 
    GROUP BY country, player_position
  )

SELECT
  players.* 
FROM {{ref('unique_players_by_national_team_rating_and_position')}} players
JOIN max_rating_by_position 
ON max_rating_by_position.player_position = players.player_position
AND max_rating_by_position.country = players.country
AND max_rating_by_position.rating = players.overall_rating