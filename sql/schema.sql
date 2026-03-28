-- Players table for NHL player data
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    position VARCHAR(10),
    team VARCHAR(3),
    birth_date DATE,
    jersey_number SMALLINT,
    shoots VARCHAR(5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_games_season_date ON games(season, date);
CREATE INDEX IF NOT EXISTS idx_games_completed ON games(completed);
CREATE INDEX IF NOT EXISTS idx_games_home_team ON games(home_team);
CREATE INDEX IF NOT EXISTS idx_games_away_team ON games(away_team);
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);
CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);