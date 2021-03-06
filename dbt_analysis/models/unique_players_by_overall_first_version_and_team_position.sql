WITH
    players_in_each_position AS (
        SELECT
            MIN(players.version_name) AS first_version,
            players.team_name,
            players.team_position,
            players.overall_rating AS rating,
        FROM {{ref('players')}} players
        GROUP BY team_name, team_position, rating
    )

SELECT players.* FROM {{ref('players')}} players 
JOIN players_in_each_position 
ON players_in_each_position.team_position = players.team_position
AND players_in_each_position.team_name = players.team_name
AND players_in_each_position.rating = players.overall_rating
AND players_in_each_position.first_version = players.version_name
