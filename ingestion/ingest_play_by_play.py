import math
import os
from typing import Optional
from datetime import datetime
import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Ingests play-by-play data using the NHL API and stores it in Neon Postgres

# Schema:
# - event_id: int                    plays[].eventId
# - game_id: int                     top-level id
# - sort_order: int                  plays[].sortOrder
# - period: smallint                 plays[].periodDescriptor.number
# - period_type: text                plays[].periodDescriptor.periodType
# - time_in_period: text             plays[].timeInPeriod
# - time_remaining: text             plays[].timeRemaining
# - event_type: text                 plays[].typeDescKey
# - type_code: smallint              plays[].typeCode
# - situation_code: varchar(4)       plays[].situationCode
# - strength: text                   decoded from situationCode (EV/PP/SH)
# - home_team_defending_side: text   plays[].homeTeamDefendingSide
# - x_coord: smallint                plays[].details.xCoord
# - y_coord: smallint                plays[].details.yCoord
# - zone: char(1)                    plays[].details.zoneCode
# - event_owner_team_id: int         plays[].details.eventOwnerTeamId
# - distance: smallint               calculated from x/y for shot events
# --- faceoff ---
# - winning_player_id: int           plays[].details.winningPlayerId
# - losing_player_id: int            plays[].details.losingPlayerId
# --- hit ---
# - hitting_player_id: int           plays[].details.hittingPlayerId
# - hittee_player_id: int            plays[].details.hitteePlayerId
# --- shot-on-goal / goal / missed-shot ---
# - shooting_player_id: int          plays[].details.shootingPlayerId
# - shot_type: text                  plays[].details.shotType
# - goalie_in_net_id: int            plays[].details.goalieInNetId
# - away_sog: smallint               plays[].details.awaySOG
# - home_sog: smallint               plays[].details.homeSOG
# --- goal ---
# - scoring_player_id: int           plays[].details.scoringPlayerId
# - scoring_player_total: smallint   plays[].details.scoringPlayerTotal
# - assist1_player_id: int           plays[].details.assist1PlayerId
# - assist1_player_total: smallint   plays[].details.assist1PlayerTotal
# - assist2_player_id: int           plays[].details.assist2PlayerId
# - assist2_player_total: smallint   plays[].details.assist2PlayerTotal
# - away_score: smallint             plays[].details.awayScore
# - home_score: smallint             plays[].details.homeScore
# --- blocked-shot ---
# - blocking_player_id: int          plays[].details.blockingPlayerId
# - block_reason: text               plays[].details.reason
# --- giveaway / takeaway ---
# - player_id: int                   plays[].details.playerId
# --- stoppage ---
# - stoppage_reason: text            plays[].details.reason
# - stoppage_secondary_reason: text  plays[].details.secondaryReason
# --- missed-shot ---
# - miss_reason: text                plays[].details.reason


DB_URL = os.getenv('DB_URL')

SHOT_EVENT_TYPES = {'shot-on-goal', 'goal', 'missed-shot'}


def decode_strength(situation_code: str, event_owner_team_id: Optional[int], home_team_id: int) -> Optional[str]:
    """Decode situationCode to EV/PP/SH relative to the event owner team.

    situationCode format: [away_goalie][away_skaters][home_skaters][home_goalie]
    e.g. '1551' = away_goalie=1, away=5, home=5, home_goalie=1 → 5v5 EV
    """
    if not situation_code or len(situation_code) != 4 or event_owner_team_id is None:
        return None
    try:
        away_skaters = int(situation_code[1])
        home_skaters = int(situation_code[2])
    except ValueError:
        return None

    if event_owner_team_id == home_team_id:
        attacking, defending = home_skaters, away_skaters
    else:
        attacking, defending = away_skaters, home_skaters

    if attacking == defending:
        return 'EV'
    elif attacking > defending:
        return 'PP'
    else:
        return 'SH'


def calculate_distance(
    x: Optional[int],
    y: Optional[int],
    home_defending_side: Optional[str],
    event_owner_team_id: Optional[int],
    home_team_id: int,
) -> Optional[int]:
    """Calculate shot distance from the attacking net (feet).

    homeTeamDefendingSide='left' means:
      - home net is at x ≈ -89 (left side)
      - away net is at x ≈ +89 (right side)
      - home team attacks toward x=+89
    """
    if x is None or y is None or home_defending_side is None or event_owner_team_id is None:
        return None

    is_home = event_owner_team_id == home_team_id
    if home_defending_side == 'left':
        attacking_net_x = 89 if is_home else -89
    else:
        attacking_net_x = -89 if is_home else 89

    return round(math.sqrt((x - attacking_net_x) ** 2 + y ** 2))


def parse_play(play: dict, game_id: int, home_team_id: int) -> dict:
    """Map a single play dict from the API response to a row dict."""
    details = play.get('details', {}) or {}
    period_desc = play.get('periodDescriptor', {}) or {}
    situation_code = play.get('situationCode')
    event_type = play.get('typeDescKey')
    event_owner_team_id = details.get('eventOwnerTeamId')
    home_defending_side = play.get('homeTeamDefendingSide')

    x = details.get('xCoord')
    y = details.get('yCoord')

    distance = None
    if event_type in SHOT_EVENT_TYPES and details.get('zoneCode') == 'O':
        distance = calculate_distance(x, y, home_defending_side, event_owner_team_id, home_team_id)

    return {
        'event_id': play.get('eventId'),
        'game_id': game_id,
        'sort_order': play.get('sortOrder'),
        'period': period_desc.get('number'),
        'period_type': period_desc.get('periodType'),
        'time_in_period': play.get('timeInPeriod'),
        'time_remaining': play.get('timeRemaining'),
        'event_type': event_type,
        'type_code': play.get('typeCode'),
        'situation_code': situation_code,
        'strength': decode_strength(situation_code, event_owner_team_id, home_team_id),
        'home_team_defending_side': home_defending_side,
        'x_coord': x,
        'y_coord': y,
        'zone': details.get('zoneCode'),
        'event_owner_team_id': event_owner_team_id,
        'distance': distance,
        # faceoff
        'winning_player_id': details.get('winningPlayerId'),
        'losing_player_id': details.get('losingPlayerId'),
        # hit
        'hitting_player_id': details.get('hittingPlayerId'),
        'hittee_player_id': details.get('hitteePlayerId'),
        # shot / goal / missed-shot
        'shooting_player_id': details.get('shootingPlayerId'),
        'shot_type': details.get('shotType'),
        'goalie_in_net_id': details.get('goalieInNetId'),
        'away_sog': details.get('awaySOG'),
        'home_sog': details.get('homeSOG'),
        # goal
        'scoring_player_id': details.get('scoringPlayerId'),
        'scoring_player_total': details.get('scoringPlayerTotal'),
        'assist1_player_id': details.get('assist1PlayerId'),
        'assist1_player_total': details.get('assist1PlayerTotal'),
        'assist2_player_id': details.get('assist2PlayerId'),
        'assist2_player_total': details.get('assist2PlayerTotal'),
        'away_score': details.get('awayScore'),
        'home_score': details.get('homeScore'),
        # blocked-shot
        'blocking_player_id': details.get('blockingPlayerId'),
        'block_reason': details.get('reason') if event_type == 'blocked-shot' else None,
        # giveaway / takeaway
        'player_id': details.get('playerId'),
        # stoppage
        'stoppage_reason': details.get('reason') if event_type == 'stoppage' else None,
        'stoppage_secondary_reason': details.get('secondaryReason') if event_type == 'stoppage' else None,
        # missed-shot
        'miss_reason': details.get('reason') if event_type == 'missed-shot' else None,
    }


def ingest_play_by_play(season: int):
    engine = sa.create_engine(DB_URL)

    create_table_sql = """
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

    CREATE INDEX IF NOT EXISTS idx_pbp_game_id ON play_by_play(game_id);
    CREATE INDEX IF NOT EXISTS idx_pbp_event_type ON play_by_play(event_type);
    CREATE INDEX IF NOT EXISTS idx_pbp_shooting_player ON play_by_play(shooting_player_id);
    CREATE INDEX IF NOT EXISTS idx_pbp_scoring_player ON play_by_play(scoring_player_id);
    """

    upsert_sql = """
    INSERT INTO play_by_play (
        event_id, game_id, sort_order, period, period_type, time_in_period, time_remaining,
        event_type, type_code, situation_code, strength, home_team_defending_side,
        x_coord, y_coord, zone, event_owner_team_id, distance,
        winning_player_id, losing_player_id,
        hitting_player_id, hittee_player_id,
        shooting_player_id, shot_type, goalie_in_net_id, away_sog, home_sog,
        scoring_player_id, scoring_player_total,
        assist1_player_id, assist1_player_total,
        assist2_player_id, assist2_player_total,
        away_score, home_score,
        blocking_player_id, block_reason,
        player_id,
        stoppage_reason, stoppage_secondary_reason,
        miss_reason
    ) VALUES (
        :event_id, :game_id, :sort_order, :period, :period_type, :time_in_period, :time_remaining,
        :event_type, :type_code, :situation_code, :strength, :home_team_defending_side,
        :x_coord, :y_coord, :zone, :event_owner_team_id, :distance,
        :winning_player_id, :losing_player_id,
        :hitting_player_id, :hittee_player_id,
        :shooting_player_id, :shot_type, :goalie_in_net_id, :away_sog, :home_sog,
        :scoring_player_id, :scoring_player_total,
        :assist1_player_id, :assist1_player_total,
        :assist2_player_id, :assist2_player_total,
        :away_score, :home_score,
        :blocking_player_id, :block_reason,
        :player_id,
        :stoppage_reason, :stoppage_secondary_reason,
        :miss_reason
    )
    ON CONFLICT (game_id, event_id) DO UPDATE SET
        sort_order = EXCLUDED.sort_order,
        period = EXCLUDED.period,
        period_type = EXCLUDED.period_type,
        time_in_period = EXCLUDED.time_in_period,
        time_remaining = EXCLUDED.time_remaining,
        event_type = EXCLUDED.event_type,
        type_code = EXCLUDED.type_code,
        situation_code = EXCLUDED.situation_code,
        strength = EXCLUDED.strength,
        home_team_defending_side = EXCLUDED.home_team_defending_side,
        x_coord = EXCLUDED.x_coord,
        y_coord = EXCLUDED.y_coord,
        zone = EXCLUDED.zone,
        event_owner_team_id = EXCLUDED.event_owner_team_id,
        distance = EXCLUDED.distance,
        winning_player_id = EXCLUDED.winning_player_id,
        losing_player_id = EXCLUDED.losing_player_id,
        hitting_player_id = EXCLUDED.hitting_player_id,
        hittee_player_id = EXCLUDED.hittee_player_id,
        shooting_player_id = EXCLUDED.shooting_player_id,
        shot_type = EXCLUDED.shot_type,
        goalie_in_net_id = EXCLUDED.goalie_in_net_id,
        away_sog = EXCLUDED.away_sog,
        home_sog = EXCLUDED.home_sog,
        scoring_player_id = EXCLUDED.scoring_player_id,
        scoring_player_total = EXCLUDED.scoring_player_total,
        assist1_player_id = EXCLUDED.assist1_player_id,
        assist1_player_total = EXCLUDED.assist1_player_total,
        assist2_player_id = EXCLUDED.assist2_player_id,
        assist2_player_total = EXCLUDED.assist2_player_total,
        away_score = EXCLUDED.away_score,
        home_score = EXCLUDED.home_score,
        blocking_player_id = EXCLUDED.blocking_player_id,
        block_reason = EXCLUDED.block_reason,
        player_id = EXCLUDED.player_id,
        stoppage_reason = EXCLUDED.stoppage_reason,
        stoppage_secondary_reason = EXCLUDED.stoppage_secondary_reason,
        miss_reason = EXCLUDED.miss_reason,
        updated_at = CURRENT_TIMESTAMP
    """

    with engine.connect() as conn:
        conn.execute(sa.text(create_table_sql))
        conn.commit()

        # Fetch completed game IDs for the season from the games table
        result = conn.execute(
            sa.text("SELECT game_id FROM games WHERE season = :season AND completed = TRUE"),
            {'season': season}
        )
        game_ids = [row[0] for row in result]

    if not game_ids:
        print(f"No completed games found for season {season}")
        return

    total_plays = 0

    for game_id in game_ids:
        try:
            response = requests.get(
                f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"Error fetching play-by-play for game {game_id}: {e}")
            continue

        home_team_id = data.get('homeTeam', {}).get('id')
        plays = data.get('plays', [])

        if not plays or home_team_id is None:
            continue

        rows = [parse_play(play, game_id, home_team_id) for play in plays]

        with engine.connect() as conn:
            conn.execute(sa.text(upsert_sql), rows)
            conn.commit()

        total_plays += len(rows)
        print(f"Game {game_id}: ingested {len(rows)} plays")

    print(f"Successfully ingested {total_plays} plays across {len(game_ids)} games for season {season}")


if __name__ == "__main__":
    ingest_play_by_play(20252026)
