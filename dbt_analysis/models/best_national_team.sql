WITH
  max_rating_by_position AS (
    SELECT 
      country,
      player_position, 
      MAX(overall_rating) AS rating
    FROM {{ref('players')}} 
    GROUP BY country, player_position
  ),
  max_rating_player AS (
    SELECT
      players.*
    FROM {{ref('players')}} players
    JOIN max_rating_by_position 
    ON max_rating_by_position.player_position = players.player_position
    AND max_rating_by_position.country = players.country
    AND max_rating_by_position.rating = players.overall_rating
  ),
  second_max_rating_by_position AS (
    SELECT 
      second_max_rating.country,
      second_max_rating.player_position, 
      ARRAY_AGG(second_max_rating.overall_rating ORDER BY second_max_rating.overall_rating DESC LIMIT 1)[SAFE_OFFSET(0)] AS rating
    FROM {{ref('players')}} second_max_rating
    JOIN max_rating_player ON max_rating_player.country = second_max_rating.country
    AND max_rating_player.player_position = second_max_rating.player_position
    AND max_rating_player.id != second_max_rating.id
    AND max_rating_player.overall_rating > second_max_rating.overall_rating 
    GROUP BY country, player_position    
  )

SELECT
  players.* EXCEPT(position_order),
  positions.position_order  
FROM max_rating_player players
LEFT JOIN {{ref('positions')}} positions ON positions.position = players.player_position
UNION ALL
SELECT
  players.* EXCEPT(position_order),
  positions.position_order  
FROM {{ref('players')}} players
JOIN second_max_rating_by_position 
ON second_max_rating_by_position.player_position = players.player_position
AND second_max_rating_by_position.country = players.country
AND second_max_rating_by_position.rating = players.overall_rating
LEFT JOIN {{ref('positions')}} positions ON positions.position = players.player_position
