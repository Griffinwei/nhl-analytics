"""
NHL API Explorer — Run this locally to inspect every endpoint needed for your raw tables.

Usage:
    pip install requests
    python explore_nhl_api.py

What it does:
    1. Hits each NHL API endpoint relevant to your schema
    2. Prints the top-level keys and a sample object for each
    3. Writes the full JSON responses to an /api_responses folder so you can browse them

After running, you'll have the exact field names and nesting to write your CREATE TABLE statements.
"""

import requests
import json
import os
import time
import sys

OUTPUT_DIR = "api_responses"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------------------------------
# Config — change these to explore different games/players/dates
# --------------------------------------------------------------------------
EXPLORE_DATE = "2026-03-25"          # Pick a recent date with games
TEAM_CODE = "BOS"                    # Three-letter team code for roster
SEASON = "20252026"                  # Current season in YYYYYYYY format
GAME_TYPE = 2                        # 2 = regular season, 3 = playoffs

# We'll discover a real game_id from the schedule, but set a fallback
FALLBACK_GAME_ID = 2024021000

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def fetch(url, label):
    """Fetch a URL, print structure summary, save full response."""
    print(f"\n{'=' * 80}")
    print(f"  {label}")
    print(f"  GET {url}")
    print(f"{'=' * 80}")

    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

    # Save full response
    safe_name = label.lower().replace(" ", "_").replace("/", "_").replace(":", "")
    filepath = os.path.join(OUTPUT_DIR, f"{safe_name}.json")
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Saved full response → {filepath}")

    return data


def print_keys(obj, indent=2):
    """Print keys and types of a dict, recursing one level for nested dicts."""
    if not isinstance(obj, dict):
        print(f"{' ' * indent}(not a dict — type: {type(obj).__name__})")
        return
    for key, val in obj.items():
        type_label = type(val).__name__
        if isinstance(val, list):
            type_label = f"list[{len(val)} items]"
            if val and isinstance(val[0], dict):
                type_label += f" of dicts with keys: {list(val[0].keys())[:8]}"
        elif isinstance(val, dict):
            type_label = f"dict with keys: {list(val.keys())[:8]}"
        print(f"{' ' * indent}{key}: {type_label}")


def print_sample(obj, max_lines=40):
    """Pretty-print a truncated JSON sample."""
    text = json.dumps(obj, indent=2, default=str)
    lines = text.split("\n")
    if len(lines) > max_lines:
        half = max_lines // 2
        lines = lines[:half] + [f"  ... ({len(lines) - max_lines} lines omitted) ..."] + lines[-half:]
    print("\n".join(lines))


# ==========================================================================
# 1. SCHEDULE → games table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 1. SCHEDULE — for the 'games' table")
print("#" * 80)

data = fetch(
    f"https://api-web.nhle.com/v1/schedule/{EXPLORE_DATE}",
    f"Schedule for {EXPLORE_DATE}"
)

game_id = FALLBACK_GAME_ID
if data:
    print("\nTop-level keys:")
    print_keys(data)

    # Find a completed game to use for subsequent calls
    for week in data.get("gameWeek", []):
        for game in week.get("games", []):
            print(f"\n--- Sample game object ---")
            print_sample(game)
            # Prefer a final game so play-by-play is complete
            if game.get("gameState") in ("OFF", "FINAL"):
                game_id = game["id"]
            elif game_id == FALLBACK_GAME_ID:
                game_id = game["id"]
            break
        break

    print(f"\n>>> Using game_id = {game_id} for subsequent calls")

time.sleep(1)


# ==========================================================================
# 2. BOXSCORE — also for the 'games' table (final scores, team metadata)
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 2. BOXSCORE — enriches 'games' table with scores")
print("#" * 80)

data = fetch(
    f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore",
    f"Boxscore for game {game_id}"
)

if data:
    print("\nTop-level keys:")
    print_keys(data)
    # Show the key fields you care about
    for field in ["id", "gameState", "gameDate", "homeTeam", "awayTeam"]:
        if field in data:
            val = data[field]
            if isinstance(val, dict):
                print(f"\n  {field} keys: {list(val.keys())[:10]}")
            else:
                print(f"\n  {field}: {val}")

time.sleep(1)


# ==========================================================================
# 3. PLAY-BY-PLAY — for the 'play_by_play' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 3. PLAY-BY-PLAY — for the 'play_by_play' table")
print("#" * 80)

data = fetch(
    f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play",
    f"Play-by-play for game {game_id}"
)

if data:
    print("\nTop-level keys:")
    print_keys(data)

    plays = data.get("plays", [])
    print(f"\nTotal plays in this game: {len(plays)}")

    # Show a few different event types
    seen_types = set()
    print("\n--- Sample plays by event type ---")
    for play in plays:
        event_type = play.get("typeDescKey", "unknown")
        if event_type not in seen_types and len(seen_types) < 6:
            seen_types.add(event_type)
            print(f"\n  Event type: {event_type}")
            print_sample(play, max_lines=20)

    # List all unique event types
    all_types = set(p.get("typeDescKey") for p in plays)
    print(f"\nAll event types found: {sorted(all_types)}")

time.sleep(1)


# ==========================================================================
# 4. ROSTER — for the 'players' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 4. ROSTER — for the 'players' table")
print("#" * 80)

data = fetch(
    f"https://api-web.nhle.com/v1/roster/{TEAM_CODE}/current",
    f"Roster for {TEAM_CODE}"
)

if data:
    print("\nTop-level keys:")
    print_keys(data)

    # Show one player from each position group
    sample_player_id = None
    sample_goalie_id = None
    for group in ["forwards", "defensemen", "goalies"]:
        players = data.get(group, [])
        if players:
            print(f"\n--- Sample {group[:-1] if group != 'goalies' else 'goalie'} ---")
            print_sample(players[0], max_lines=25)
            if group != "goalies" and not sample_player_id:
                sample_player_id = players[0].get("id")
            if group == "goalies" and not sample_goalie_id:
                sample_goalie_id = players[0].get("id")

    print(f"\n>>> Sample skater ID: {sample_player_id}")
    print(f">>> Sample goalie ID: {sample_goalie_id}")

time.sleep(1)


# ==========================================================================
# 5. SHIFT CHARTS — for the 'shifts' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 5. SHIFT CHARTS — for the 'shifts' table")
print("#" * 80)

data = fetch(
    f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}",
    f"Shift charts for game {game_id}"
)

if data:
    print("\nTop-level keys:")
    print_keys(data)

    shifts = data.get("data", [])
    print(f"\nTotal shifts: {len(shifts)}")
    if shifts:
        print("\n--- Sample shift object ---")
        print_sample(shifts[0])

        # Show all keys available on a shift
        print(f"\nAll shift keys: {list(shifts[0].keys())}")

time.sleep(1)


# ==========================================================================
# 6. EDGE STATS (SKATER) — for the 'edge_stats' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 6. EDGE SKATER DETAIL — for the 'edge_stats' table")
print("#" * 80)

if sample_player_id:
    data = fetch(
        f"https://api-web.nhle.com/v1/edge/skater-detail/{sample_player_id}/now",
        f"EDGE skater detail for player {sample_player_id}"
    )
    if data:
        print("\nTop-level keys:")
        print_keys(data)
        print("\n--- Full response (truncated) ---")
        print_sample(data, max_lines=60)
else:
    print("  Skipped — no player ID found from roster")

time.sleep(1)


# Also fetch skater comparison for richer data
if sample_player_id:
    data = fetch(
        f"https://api-web.nhle.com/v1/edge/skater-comparison/{sample_player_id}/now",
        f"EDGE skater comparison for player {sample_player_id}"
    )
    if data:
        print("\nTop-level keys:")
        print_keys(data)

time.sleep(1)


# ==========================================================================
# 7. GOALIE EDGE STATS — for the 'goalie_stats' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 7. GOALIE EDGE DETAIL — for the 'goalie_stats' table")
print("#" * 80)

if sample_goalie_id:
    data = fetch(
        f"https://api-web.nhle.com/v1/edge/goalie-detail/{sample_goalie_id}/now",
        f"EDGE goalie detail for goalie {sample_goalie_id}"
    )
    if data:
        print("\nTop-level keys:")
        print_keys(data)
        print("\n--- Full response (truncated) ---")
        print_sample(data, max_lines=60)
else:
    print("  Skipped — no goalie ID found from roster")

time.sleep(1)


# Goalie shot location detail — zone-level save %
if sample_goalie_id:
    data = fetch(
        f"https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/{sample_goalie_id}/now",
        f"EDGE goalie shot locations for goalie {sample_goalie_id}"
    )
    if data:
        print("\nTop-level keys:")
        print_keys(data)
        print("\n--- Full response (truncated) ---")
        print_sample(data, max_lines=60)

time.sleep(1)


# Goalie 5v5 detail
if sample_goalie_id:
    data = fetch(
        f"https://api-web.nhle.com/v1/edge/goalie-5v5-detail/{sample_goalie_id}/now",
        f"EDGE goalie 5v5 detail for goalie {sample_goalie_id}"
    )
    if data:
        print("\nTop-level keys:")
        print_keys(data)

time.sleep(1)


# ==========================================================================
# 8. PARTNER GAME ODDS — potential source for 'betting_lines' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 8. PARTNER GAME ODDS — for the 'betting_lines' table")
print("#" * 80)

data = fetch(
    "https://api-web.nhle.com/v1/partner-game/US/now",
    "Partner game odds (US)"
)

if data:
    print("\nTop-level keys:")
    print_keys(data)
    print("\n--- Full response (truncated) ---")
    print_sample(data, max_lines=60)


# ==========================================================================
# 9. DAILY SCORES — alternative/supplement for 'games' table
# ==========================================================================
print("\n\n" + "#" * 80)
print("# 9. DAILY SCORES — alternative for game results")
print("#" * 80)

data = fetch(
    f"https://api-web.nhle.com/v1/score/{EXPLORE_DATE}",
    f"Daily scores for {EXPLORE_DATE}"
)

if data:
    print("\nTop-level keys:")
    print_keys(data)
    games = data.get("games", [])
    if games:
        print(f"\nTotal games: {len(games)}")
        print("\n--- Sample game from scores ---")
        print_sample(games[0], max_lines=40)


# ==========================================================================
# SUMMARY
# ==========================================================================
print("\n\n" + "=" * 80)
print("DONE! Full JSON responses saved to ./" + OUTPUT_DIR + "/")
print("=" * 80)
print("""
Next steps:
  1. Open each .json file in your editor (VS Code folds JSON nicely)
  2. For each raw table, identify which fields you need and their types
  3. Write your CREATE TABLE statements in sql/schema.sql
  4. Map:
     - schedule + boxscore/scores  →  games table
     - play-by-play plays array    →  play_by_play table
     - roster forwards/defense/goalies  →  players table
     - shiftcharts data array      →  shifts table
     - skater EDGE detail          →  edge_stats table
     - goalie EDGE detail + shot locations + 5v5  →  goalie_stats table
     - partner-game odds (or The Odds API)  →  betting_lines table
""")
