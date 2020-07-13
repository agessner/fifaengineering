 SELECT
    id,
    urls.player_nickname AS name,
    best_position AS player_position,
    players.version_name,
    urls.value AS url,
    positions.position_order,
    country, 
    country_image_url, 
    age, 
    birthdate, 
    height_in_meters, 
    weight_in_kg, 
    potential_overall_rating, 
    value_in_million_euros, 
    wage_in_thousand_euros, 
    preferred_foot, 
    weak_foot, 
    skill_moves, 
    international_reputation, 
    work_rate, 
    body_type, 
    team_name, 
    team_url, 
    team_image_url, 
    team_overall, 
    team_position, 
    team_jersey_number, 
    joined, 
    national_team_name, 
    national_team_url, 
    national_team_image_url, 
    national_team_overall, 
    national_team_position, 
    national_team_jersey_number, 
    crossing, 
    finishing, 
    heading_accuracy, 
    short_passing, 
    volleys, 
    dribbling, 
    curve, 
    fk_accuracy, 
    long_passing, 
    ball_control
FROM `sofifa.players_*` players
JOIN `sofifa.urls_*` urls ON urls.player_id = players.id
AND urls.version_id = players.version_id
LEFT JOIN {{ref('positions')}} positions ON positions.position = players.team_position