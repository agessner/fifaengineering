WITH
  min_rating_by_position AS (
    SELECT 
      team_name,
      player_position, 
      MIN(overall_rating) AS rating
    FROM {{ref('players')}} 
    GROUP BY team_name, player_position
  )

SELECT
  players.* 
FROM {{ref('players')}} players
JOIN min_rating_by_position 
ON min_rating_by_position.player_position = players.player_position
AND min_rating_by_position.team_name = players.team_name
AND min_rating_by_position.rating = players.overall_rating
WHERE players.player_position != 'RES' AND players.player_position != 'SUB'