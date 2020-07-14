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
      ARRAY_AGG(max_rating_by_position.rating ORDER BY max_rating_by_position.rating DESC LIMIT 2)[SAFE_OFFSET(1)] AS rating
    FROM {{ref('players')}} second_max_rating
    JOIN max_rating_by_position ON max_rating_by_position.country = second_max_rating.country
    AND max_rating_by_position.player_position = second_max_rating.player_position 
    GROUP BY country, player_position    
  )

SELECT
  players.* EXCEPT(position_order),
  positions.position_order  
FROM {{ref('players')}} players
JOIN max_rating_by_position 
ON max_rating_by_position.player_position = players.player_position
AND max_rating_by_position.country = players.country
AND max_rating_by_position.rating = players.overall_rating
LEFT JOIN {{ref('positions')}} positions ON positions.position = players.player_position
WHERE players.country = 'England'
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
WHERE players.country = 'England' ORDER BY position_order