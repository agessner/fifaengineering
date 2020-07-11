 SELECT
    id,
    urls.player_nickname AS name,
    overall_rating,
    team_name,
    team_position AS player_position,
    players.version_name,
    urls.value AS url,
    image_url,
    positions.position_order,
    team_image_url
FROM `sofifa.players_*` players
JOIN `sofifa.urls_*` urls ON urls.player_id = players.id
AND urls.version_id = players.version_id
LEFT JOIN {{ref('positions')}} positions ON positions.position = players.team_position