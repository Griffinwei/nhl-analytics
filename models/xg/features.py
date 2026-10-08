from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy as sa

from ingestion.common import get_engine

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / '.cache' / 'xg_shots.parquet'  # delete to re-pull from Neon
SEASONS = [20242025, 20252026, 20262027]


def load_shots() -> pd.DataFrame:
    if not CACHE.exists():
        CACHE.parent.mkdir(exist_ok=True)
        with get_engine().connect() as conn:
            df = pd.read_sql(sa.text((ROOT / 'sql' / 'xg_shots.sql').read_text()), conn, params={'seasons': SEASONS})
        df.to_parquet(CACHE)
    return add_features(pd.read_parquet(CACHE))


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    # Rotate (flip x and y) so every shooter attacks the net at (89, 0); 'left' = home net at x = -89
    attacks_positive = df.is_home == (df.home_team_defending_side == 'left')
    sign = np.where(attacks_positive, 1, -1)
    x, y = df.x_coord * sign, df.y_coord * sign
    df['distance'] = np.hypot(89 - x, y)
    df['angle'] = np.degrees(np.arctan2(np.abs(y), 89 - x))

    # situation_code digits: [away goalie][away skaters][home skaters][home goalie]
    away_goalie, away_skaters, home_skaters, home_goalie = (df.situation_code.str[i].astype(int) for i in range(4))
    df['shooter_skaters'] = np.where(df.is_home, home_skaters, away_skaters)
    df['defender_skaters'] = np.where(df.is_home, away_skaters, home_skaters)
    df['empty_net'] = (np.where(df.is_home, away_goalie, home_goalie) == 0).astype(int)
    # Empty-net goal odds fall off with distance far more slowly, so give them their own slope
    df['empty_net_distance'] = df.empty_net * df.distance
    return df
