SELECT count FROM UNNEST(ARRAY(SELECT COUNT(*) - 16213 FROM sofifa.players_09)) count WHERE count != 0