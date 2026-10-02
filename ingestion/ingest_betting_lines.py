from datetime import datetime, timezone
from typing import Optional
import requests
import sqlalchemy as sa
from common import get_engine

# Ingests pre-game betting lines from the NHL partner odds endpoint and stores them in Neon Postgres.
# One row per game per sportsbook per capture, so opening and closing lines are both retained.

# Schema:
# - game_id: int                     games[].gameId
# - sportsbook: text                 bettingPartner.name
# - home_moneyline: smallint         homeTeam.odds[MONEY_LINE_2_WAY].value
# - away_moneyline: smallint         awayTeam.odds[MONEY_LINE_2_WAY].value
# - home_implied_prob: numeric       raw implied probability (includes vig)
# - away_implied_prob: numeric
# - home_fair_prob: numeric          de-vigged (home + away normalized to 1)
# - away_fair_prob: numeric
# - over_under: numeric              homeTeam.odds[OVER_UNDER].qualifier (e.g. "O5.5" -> 5.5)
# - captured_at: timestamptz         lastUpdatedUTC


def implied_prob(odds: float) -> float:
    """American odds -> implied probability."""
    return 100 / (odds + 100) if odds > 0 else -odds / (-odds + 100)


def find_odds(team: dict, description: str) -> Optional[dict]:
    return next((o for o in team.get('odds', []) if o.get('description') == description), None)


def ingest_betting_lines():
    engine = get_engine()

    create_table_sql = """
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
    """

    insert_sql = """
    INSERT INTO betting_lines (
        game_id, sportsbook, home_moneyline, away_moneyline,
        home_implied_prob, away_implied_prob, home_fair_prob, away_fair_prob,
        over_under, captured_at
    ) VALUES (
        :game_id, :sportsbook, :home_moneyline, :away_moneyline,
        :home_implied_prob, :away_implied_prob, :home_fair_prob, :away_fair_prob,
        :over_under, :captured_at
    )
    ON CONFLICT (game_id, sportsbook, captured_at) DO NOTHING
    """

    response = requests.get("https://api-web.nhle.com/v1/partner-game/US/now", timeout=30)
    response.raise_for_status()
    data = response.json()

    now = datetime.now(timezone.utc)
    sportsbook = data.get('bettingPartner', {}).get('name', 'unknown')
    captured_at = data.get('lastUpdatedUTC') or now.isoformat()

    rows = []
    for game in data.get('games', []):
        if game.get('gameType') != 2:  # Only regular season games
            continue
        # Skip games already underway so only pre-game lines are stored
        start = game.get('startTimeUTC')
        if start and datetime.fromisoformat(start.replace('Z', '+00:00')) <= now:
            continue

        home, away = game.get('homeTeam', {}), game.get('awayTeam', {})
        home_ml, away_ml = find_odds(home, 'MONEY_LINE_2_WAY'), find_odds(away, 'MONEY_LINE_2_WAY')
        if not home_ml or not away_ml:
            continue

        home_p, away_p = implied_prob(home_ml['value']), implied_prob(away_ml['value'])
        total = find_odds(home, 'OVER_UNDER')
        rows.append({
            'game_id': game['gameId'],
            'sportsbook': sportsbook,
            'home_moneyline': int(home_ml['value']),
            'away_moneyline': int(away_ml['value']),
            'home_implied_prob': round(home_p, 4),
            'away_implied_prob': round(away_p, 4),
            'home_fair_prob': round(home_p / (home_p + away_p), 4),
            'away_fair_prob': round(away_p / (home_p + away_p), 4),
            'over_under': float(total['qualifier'][1:]) if total and total.get('qualifier') else None,
            'captured_at': captured_at,
        })

    with engine.connect() as conn:
        conn.execute(sa.text(create_table_sql))
        if rows:
            conn.execute(sa.text(insert_sql), rows)
        conn.commit()

    print(f"Ingested {len(rows)} {sportsbook} lines captured at {captured_at}")


if __name__ == "__main__":
    ingest_betting_lines()
