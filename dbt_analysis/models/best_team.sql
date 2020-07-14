WITH
  max_rating_by_position AS (
    SELECT 
      team_name,
      team_position, 
      MAX(overall_rating) AS rating
    FROM {{ref('unique_players_by_team_position')}} 
    GROUP BY team_name, team_position
  )

SELECT
  players.* 
FROM {{ref('unique_players_by_team_position')}} players
JOIN max_rating_by_position 
ON max_rating_by_position.team_position = players.team_position
AND max_rating_by_position.team_name = players.team_name
AND max_rating_by_position.rating = players.overall_rating
WHERE players.team_position != 'RES' AND players.team_position != 'SUB'