import os
from datetime import datetime, timedelta
import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Ingests player data using the NHL API and stores it in Neon Postgres

# Schema:
# - player_id: int
# - first_name: text
# - last_name: text
# - position: text
# - team: text
# - birth_date: date
# - jersey_number: smallint
# - shoots: text

def ingest_players():
    # Connect to Neon Postgres
    engine = sa.create_engine(os.getenv('DB_URL'))

    # Create table if it doesn't exist
    create_table_sql = """
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
    """
    with engine.connect() as connection:
        connection.execute(sa.text(create_table_sql))
        connection.commit()

    # Collect all players across all teams to avoid duplicates
    all_players = []

    teams = [
        "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", 
        "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", 
        "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA", 
        "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WSH", "WPG"
    ]

    for team in teams:
        try:
            response = requests.get(f"https://api-web.nhle.com/v1/roster/{team}/current")
            response.raise_for_status()
            result = response.json()
            
            # The API returns position groups as top-level keys: forwards, defensemen, goalies
            position_groups = ['forwards', 'defensemen', 'goalies']
            
            for group_name in position_groups:
                players = result.get(group_name, [])
                for player in players:
                    player_id = player.get("id")
                    if not player_id:
                        continue
                        
                    # Skip if we already processed this player (in case they appear in multiple teams)
                    if any(p['player_id'] == player_id for p in all_players):
                        continue
                    
                    first_name = player.get("firstName", {}).get("default")
                    last_name = player.get("lastName", {}).get("default")
                    position = player.get("positionCode")
                    birth_date = player.get("birthDate")
                    jersey_number = player.get("sweaterNumber")
                    shoots = player.get("shootsCatches")
                    
                    # Only add if we have essential data
                    if first_name and last_name and position:
                        all_players.append({
                            "player_id": player_id,
                            "first_name": first_name,
                            "last_name": last_name,
                            "position": position,
                            "team": team,
                            "birth_date": birth_date,
                            "jersey_number": jersey_number,
                            "shoots": shoots
                        })

        except requests.RequestException as e:
            print(f"Error fetching data for team {team}: {e}")
            continue

    # Insert all players with upsert logic (outside the team loop)
    if all_players:
        insert_sql = """
        INSERT INTO players (player_id, first_name, last_name, position, team, birth_date, jersey_number, shoots)
        VALUES (:player_id, :first_name, :last_name, :position, :team, :birth_date, :jersey_number, :shoots)
        ON CONFLICT (player_id) 
        DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            position = EXCLUDED.position,
            team = EXCLUDED.team,
            birth_date = EXCLUDED.birth_date,
            jersey_number = EXCLUDED.jersey_number,
            shoots = EXCLUDED.shoots,
            updated_at = CURRENT_TIMESTAMP
        """
        
        with engine.connect() as connection:
            connection.execute(sa.text(insert_sql), all_players)
            connection.commit()
        
        print(f"Successfully ingested {len(all_players)} players")
    else:
        print("No players found")

if __name__ == "__main__":
    ingest_players()