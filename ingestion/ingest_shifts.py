import os
import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Ingests shift data using the NHL API and stores it in Neon Postgres

# Schema:
# - shift_id: int                  data[].id
# - game_id: int                   data[].gameId
# - player_id: int                 data[].playerId
# - period: smallint               data[].period
# - shift_number: smallint         data[].shiftNumber
# - start_time: text               data[].startTime  (e.g. "02:20")
# - end_time: text                 data[].endTime
# - duration: text                 data[].duration   (e.g. "00:42")
# - team_id: int                   data[].teamId
# - team_abbrev: varchar(3)        data[].teamAbbrev
# - type_code: smallint            data[].typeCode
# - detail_code: smallint          data[].detailCode
# - event_number: int              data[].eventNumber

DB_URL = os.getenv('DB_URL')


def ingest_shifts(season: int):
    engine = sa.create_engine(DB_URL)

    create_table_sql = """
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

    CREATE INDEX IF NOT EXISTS idx_shifts_game_id ON shifts(game_id);
    CREATE INDEX IF NOT EXISTS idx_shifts_player_id ON shifts(player_id);
    CREATE INDEX IF NOT EXISTS idx_shifts_period ON shifts(game_id, period);
    """

    upsert_sql = """
    INSERT INTO shifts (
        shift_id, game_id, player_id, period, shift_number,
        start_time, end_time, duration,
        team_id, team_abbrev, type_code, detail_code, event_number
    ) VALUES (
        :shift_id, :game_id, :player_id, :period, :shift_number,
        :start_time, :end_time, :duration,
        :team_id, :team_abbrev, :type_code, :detail_code, :event_number
    )
    ON CONFLICT (shift_id) DO UPDATE SET
        game_id = EXCLUDED.game_id,
        player_id = EXCLUDED.player_id,
        period = EXCLUDED.period,
        shift_number = EXCLUDED.shift_number,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        duration = EXCLUDED.duration,
        team_id = EXCLUDED.team_id,
        team_abbrev = EXCLUDED.team_abbrev,
        type_code = EXCLUDED.type_code,
        detail_code = EXCLUDED.detail_code,
        event_number = EXCLUDED.event_number,
        updated_at = CURRENT_TIMESTAMP
    """

    with engine.connect() as conn:
        conn.execute(sa.text(create_table_sql))
        conn.commit()

        result = conn.execute(
            sa.text("SELECT game_id FROM games WHERE season = :season AND completed = TRUE"),
            {'season': season}
        )
        game_ids = [row[0] for row in result]

    if not game_ids:
        print(f"No completed games found for season {season}")
        return

    total_shifts = 0

    for game_id in game_ids:
        try:
            response = requests.get(
                f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"Error fetching shifts for game {game_id}: {e}")
            continue

        shifts = data.get('data', [])
        if not shifts:
            continue

        rows = [
            {
                'shift_id': s['id'],
                'game_id': s['gameId'],
                'player_id': s['playerId'],
                'period': s['period'],
                'shift_number': s.get('shiftNumber'),
                'start_time': s['startTime'],
                'end_time': s['endTime'],
                'duration': s.get('duration'),
                'team_id': s.get('teamId'),
                'team_abbrev': s.get('teamAbbrev'),
                'type_code': s.get('typeCode'),
                'detail_code': s.get('detailCode'),
                'event_number': s.get('eventNumber'),
            }
            for s in shifts
        ]

        with engine.connect() as conn:
            conn.execute(sa.text(upsert_sql), rows)
            conn.commit()

        total_shifts += len(rows)
        print(f"Game {game_id}: ingested {len(rows)} shifts")

    print(f"Successfully ingested {total_shifts} shifts across {len(game_ids)} games for season {season}")


if __name__ == "__main__":
    season = int(os.getenv('SEASON', '20252026'))
    ingest_shifts(season)
