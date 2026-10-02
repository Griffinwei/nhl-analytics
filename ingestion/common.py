import os
from datetime import date

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
    return sa.create_engine(os.environ['DB_URL'])
