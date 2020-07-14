WITH
  min_rating_by_position AS (
    SELECT 
      team_name,
      team_position, 
      MIN(overall_rating) AS rating
    FROM {{ref('unique_players_by_team_position')}} 
    GROUP BY team_name, team_position
  )

SELECT
  players.* 
FROM {{ref('unique_players_by_team_position')}} players
JOIN min_rating_by_position 
ON min_rating_by_position.team_position = players.team_position
AND min_rating_by_position.team_name = players.team_name
AND min_rating_by_position.rating = players.overall_rating
WHERE players.team_position != 'RES' AND players.team_position != 'SUB'