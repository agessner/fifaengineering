WITH
  players AS (
    SELECT * FROM {{ref('players')}} WHERE team_position != 'RES' AND team_position != 'SUB'
  ),
  max_rating_by_position AS (
    SELECT 
      team_name,
      player_position, 
      MAX(overall_rating) AS rating
    FROM players GROUP BY team_name, player_position
  ),
  final_query AS (
    
  )

SELECT
  players.* 
FROM players
JOIN max_rating_by_position 
ON max_rating_by_position.player_position = players.player_position
AND max_rating_by_position.team_name = players.team_name
AND max_rating_by_position.rating = players.overall_rating