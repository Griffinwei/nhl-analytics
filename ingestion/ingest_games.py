import json
import os
from datetime import datetime, timedelta
import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Ingests game data using the NHL API and stores it in Neon Postgres

# Schema:
# - game_id: int
# - season: int
# - date: date
# - time: time
# - location: text
# - home_team: text
# - home_score: smallint
# - away_team: text
# - away_score: smallint
# - overtime: boolean
# - shootout: boolean
# - completed: boolean

def ingest_games(season: int):
    # Connect to Neon Postgres
    engine = sa.create_engine(os.getenv('DB_URL'))
    
    # Create table if it doesn't exist
    create_table_sql = """
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
    
    -- Add updated_at column if it doesn't exist (for existing tables)
    ALTER TABLE games ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    
    -- Create index for common queries
    CREATE INDEX IF NOT EXISTS idx_games_season_date ON games(season, date);
    CREATE INDEX IF NOT EXISTS idx_games_completed ON games(completed);
    """
    
    with engine.connect() as connection:
        connection.execute(sa.text(create_table_sql))
        connection.commit()
        
        # Collect all games across all teams to avoid duplicates
        all_games = []
        
        teams = [
            "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", 
            "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", 
            "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA", 
            "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WSH", "WPG"
        ]
        
        for team in teams:
            try:
                response = requests.get(f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}")
                response.raise_for_status()
                result = response.json()
                games = result.get("games", [])
                
                for game in games:
                    if game.get("gameType") != 2:  # Only regular season games
                        continue
                        
                    # Extract game data with safe access
                    game_id = game.get("id")
                    if not game_id:
                        continue
                        
                    # Skip if we already processed this game
                    if any(g['game_id'] == game_id for g in all_games):
                        continue
                    
                    date = game.get("gameDate")
                    
                    # Convert startTimeUTC to EST
                    time = None
                    start_time_utc = game.get("startTimeUTC")
                    if start_time_utc and date:
                        try:
                            utc_time = datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
                            offset_str = game.get("easternUTCOffset", "-05:00")
                            offset_hours = int(offset_str.split(":")[0])
                            offset_minutes = int(offset_str.split(":")[1]) if len(offset_str.split(":")) > 1 else 0
                            est_offset = timedelta(hours=offset_hours, minutes=offset_minutes)
                            time = (utc_time + est_offset).time()
                        except (ValueError, AttributeError):
                            pass  # Keep time as None if conversion fails
                    
                    # Extract venue and team info
                    venue = game.get("venue", {})
                    location = venue.get("default")
                    
                    home_team_data = game.get("homeTeam", {})
                    home_team = home_team_data.get("abbrev")
                    
                    away_team_data = game.get("awayTeam", {})
                    away_team = away_team_data.get("abbrev")
                    
                    # Handle scores and game outcome
                    game_state = game.get("gameState")
                    completed = game_state in ["OFF", "FINAL"] if game_state else False
                    
                    if completed:
                        home_score = home_team_data.get("score")
                        away_score = away_team_data.get("score")
                        game_outcome = game.get("gameOutcome", {})
                        last_period_type = game_outcome.get("lastPeriodType")
                        overtime = last_period_type != "REG" if last_period_type else False
                        shootout = last_period_type == "SO" if last_period_type else False
                    else:
                        home_score = None
                        away_score = None
                        overtime = None
                        shootout = None
                    
                    # Only add if we have essential data
                    if game_id and date and home_team and away_team:
                        all_games.append({
                            'game_id': game_id,
                            'season': season,
                            'date': date,
                            'time': time,
                            'location': location,
                            'home_team': home_team,
                            'home_score': home_score,
                            'away_team': away_team,
                            'away_score': away_score,
                            'overtime': overtime,
                            'shootout': shootout,
                            'completed': completed
                        })
                        
            except requests.RequestException as e:
                print(f"Error fetching data for {team}: {e}")
                continue
        
        # Insert all games with upsert logic
        if all_games:
            insert_sql = """
            INSERT INTO games (game_id, season, date, time, location, home_team, home_score, 
                             away_team, away_score, overtime, shootout, completed)
            VALUES (:game_id, :season, :date, :time, :location, :home_team, :home_score,
                   :away_team, :away_score, :overtime, :shootout, :completed)
            ON CONFLICT (game_id) 
            DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                overtime = EXCLUDED.overtime,
                shootout = EXCLUDED.shootout,
                completed = EXCLUDED.completed,
                updated_at = CURRENT_TIMESTAMP
            """
            
            connection.execute(sa.text(insert_sql), all_games)
            connection.commit()
            
            print(f"Successfully ingested {len(all_games)} games for season {season}")
        else:
            print(f"No games found for season {season}")

if __name__ == "__main__":
    ingest_games(20252026)