-- Single source of truth for the schema. Applied by ingestion/common.py:ensure_schema() on every run,
-- so keep every statement idempotent and additive (IF NOT EXISTS, ADD COLUMN IF NOT EXISTS).

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

CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,
    date DATE NOT NULL,
    time TIME,
    location TEXT,
    home_team VARCHAR(3) NOT NULL,
    home_score SMALLINT,
    away_team VARCHAR(3) NOT NULL,
    away_score SMALLINT,
    overtime BOOLEAN,
    shootout BOOLEAN,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE games ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE TABLE IF NOT EXISTS play_by_play (
    event_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    sort_order INTEGER,
    period SMALLINT NOT NULL,
    period_type TEXT,
    time_in_period TEXT NOT NULL,
    time_remaining TEXT,
    event_type TEXT NOT NULL,
    type_code SMALLINT,
    situation_code VARCHAR(4),
    strength TEXT,
    home_team_defending_side TEXT,
    x_coord SMALLINT,
    y_coord SMALLINT,
    zone CHAR(1),
    event_owner_team_id INTEGER,
    distance SMALLINT,
    winning_player_id INTEGER,
    losing_player_id INTEGER,
    hitting_player_id INTEGER,
    hittee_player_id INTEGER,
    shooting_player_id INTEGER,
    shot_type TEXT,
    goalie_in_net_id INTEGER,
    away_sog SMALLINT,
    home_sog SMALLINT,
    scoring_player_id INTEGER,
    scoring_player_total SMALLINT,
    assist1_player_id INTEGER,
    assist1_player_total SMALLINT,
    assist2_player_id INTEGER,
    assist2_player_total SMALLINT,
    away_score SMALLINT,
    home_score SMALLINT,
    blocking_player_id INTEGER,
    block_reason TEXT,
    player_id INTEGER,
    stoppage_reason TEXT,
    stoppage_secondary_reason TEXT,
    miss_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, event_id)
);

CREATE TABLE IF NOT EXISTS shifts (
    shift_id INTEGER PRIMARY KEY,
    game_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    period SMALLINT NOT NULL,
    shift_number SMALLINT,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    duration TEXT,
    team_id INTEGER,
    team_abbrev VARCHAR(3),
    type_code SMALLINT,
    detail_code SMALLINT,
    event_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- One row per game per sportsbook per capture, so opening and closing lines are both retained
CREATE TABLE IF NOT EXISTS betting_lines (
    game_id INTEGER NOT NULL,
    sportsbook TEXT NOT NULL,
    home_moneyline SMALLINT,
    away_moneyline SMALLINT,
    home_implied_prob NUMERIC(5, 4),
    away_implied_prob NUMERIC(5, 4),
    home_fair_prob NUMERIC(5, 4),
    away_fair_prob NUMERIC(5, 4),
    over_under NUMERIC(3, 1),
    captured_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (game_id, sportsbook, captured_at)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);
CREATE INDEX IF NOT EXISTS idx_players_position ON players(position);
CREATE INDEX IF NOT EXISTS idx_games_season_date ON games(season, date);
CREATE INDEX IF NOT EXISTS idx_games_completed ON games(completed);
CREATE INDEX IF NOT EXISTS idx_games_home_team ON games(home_team);
CREATE INDEX IF NOT EXISTS idx_games_away_team ON games(away_team);
CREATE INDEX IF NOT EXISTS idx_pbp_game_id ON play_by_play(game_id);
CREATE INDEX IF NOT EXISTS idx_pbp_event_type ON play_by_play(event_type);
CREATE INDEX IF NOT EXISTS idx_pbp_shooting_player ON play_by_play(shooting_player_id);
CREATE INDEX IF NOT EXISTS idx_pbp_scoring_player ON play_by_play(scoring_player_id);
CREATE INDEX IF NOT EXISTS idx_shifts_game_id ON shifts(game_id);
CREATE INDEX IF NOT EXISTS idx_shifts_player_id ON shifts(player_id);
CREATE INDEX IF NOT EXISTS idx_shifts_period ON shifts(game_id, period);
