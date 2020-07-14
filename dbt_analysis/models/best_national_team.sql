WITH
  max_rating_by_position AS (
    SELECT 
      country,
      player_position, 
      MAX(overall_rating) AS rating
    FROM {{ref('players')}} 
    GROUP BY country, player_position
  ),
  second_max_rating_by_position AS (
    SELECT 
      second_max_rating.country,
      second_max_rating.player_position, 
      ARRAY_AGG(second_max_rating.overall_rating ORDER BY second_max_rating.overall_rating DESC LIMIT 1)[SAFE_OFFSET(0)] AS rating
    FROM {{ref('players')}} second_max_rating
    JOIN max_rating_by_position ON max_rating_by_position.country = second_max_rating.country
    AND max_rating_by_position.player_position = second_max_rating.player_position 
    AND max_rating_by_position.rating >= second_max_rating.overall_rating
    GROUP BY country, player_position    
  )

SELECT
  players.* 
FROM {{ref('players')}} players
JOIN max_rating_by_position 
ON max_rating_by_position.player_position = players.player_position
AND max_rating_by_position.country = players.country
AND max_rating_by_position.rating = players.overall_rating
JOIN second_max_rating_by_position 
ON second_max_rating_by_position.player_position = players.player_position
AND second_max_rating_by_position.country = players.country
AND second_max_rating_by_position.rating = players.overall_rating