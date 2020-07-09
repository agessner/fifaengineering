WITH (
    players AS (
        SELECT
            id,
            urls.player_nickname AS name,
            overall_rating,
            team_name,
            team_position AS player_position,
            players.version_name,
            urls.value AS url,
            image_url,
            positions.position_order
        FROM `sofifa.players_*` players
        JOIN `sofifa.urls_*` urls ON urls.player_id = players.id
        AND urls.version_id = players.version_id
        LEFT JOIN {{ref('positions')}} positions ON positions.position = players.player_position
    ),
    players_in_each_position AS (
        SELECT
            MIN(players.version_name) AS first_version,
            players.team_name,
            players.player_position,
            players.overall_rating AS rating,
        FROM players
        GROUP BY team_name, player_position, rating
    )

)

SELECT players.* FROM players 
JOIN players_in_each_position 
ON players_in_each_position.player_position = players.player_position
AND players_in_each_position.team_name = players.team_name
AND players_in_each_position.rating = players.overall_rating
AND players_in_each_position.first_version = players.version_name
