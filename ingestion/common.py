import os
from datetime import date
from pathlib import Path

import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()


def current_season() -> int:
    """SEASON env var if set, otherwise derived from today's date (seasons start in the fall)."""
    if os.getenv('SEASON'):
        return int(os.environ['SEASON'])
    today = date.today()
    start = today.year if today.month >= 9 else today.year - 1
    return start * 10000 + start + 1


def get_engine() -> sa.Engine:
    # Batch executemany calls into pages instead of one round trip per row
    return sa.create_engine(os.environ['DB_URL'], executemany_mode='values_plus_batch', pool_pre_ping=True)


def ensure_schema(engine: sa.Engine) -> None:
    """Apply sql/schema.sql (idempotent CREATE ... IF NOT EXISTS statements)."""
    with engine.begin() as conn:
        conn.exec_driver_sql((Path(__file__).parent.parent / 'sql' / 'schema.sql').read_text())


FALLBACK_TEAMS = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH",
    "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA",
    "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WSH", "WPG",
]


def get_teams() -> list[str]:
    """Team abbreviations from current standings, falling back to FALLBACK_TEAMS."""
    try:
        response = requests.get("https://api-web.nhle.com/v1/standings/now", timeout=30)
        response.raise_for_status()
        teams = sorted(t['teamAbbrev']['default'] for t in response.json()['standings'])
    except (requests.RequestException, KeyError) as e:
        print(f"Error fetching teams, using fallback list: {e}")
        teams = FALLBACK_TEAMS
    print(f"Found {len(teams)} teams")
    if len(teams) != 32:
        raise RuntimeError(f"Expected 32 teams, got {len(teams)}: {teams}")
    return teams
