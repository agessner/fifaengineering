WITH
  max_rating_by_position AS (
    SELECT 
      team_name,
      player_position, 
      MAX(potential_overall_rating) AS rating
    FROM {{ref('unique_players_by_team_position')}} 
    GROUP BY team_name, player_position
  )

SELECT
  players.* 
FROM {{ref('unique_players_by_team_position')}} players
JOIN max_rating_by_position 
ON max_rating_by_position.player_position = players.player_position
AND max_rating_by_position.team_name = players.team_name
AND max_rating_by_position.rating = players.overall_rating
WHERE players.player_position != 'RES' AND players.player_position != 'SUB'