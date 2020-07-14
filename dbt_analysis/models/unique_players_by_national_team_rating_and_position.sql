WITH
    players_in_each_position AS (
        SELECT
            MIN(players.version_name) AS first_version,
            players.country,
            players.player_position,
            players.overall_rating AS rating,
        FROM {{ref('players')}} players
        GROUP BY country, player_position, rating
    )

SELECT players.* FROM {{ref('players')}} players 
JOIN players_in_each_position 
ON players_in_each_position.player_position = players.player_position
AND players_in_each_position.country = players.country
AND players_in_each_position.rating = players.overall_rating
AND players_in_each_position.first_version = players.version_name
