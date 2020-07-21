WITH 
  max_overall AS (
    SELECT 
        max_overall.version_name, 
        player_position, 
        MAX(max_overall.overall_rating) AS value
    FROM {{ref('players')}} max_overall
    GROUP BY version_name, player_position
  )
  
SELECT 
    max_overall.*, 
    players.name, 
    players.image_url 
FROM max_overall JOIN {{ref('players')}} players 
ON max_overall.value = players.overall_rating 
AND max_overall.version_name = players.version_name
AND max_overall.player_position = players.player_position
