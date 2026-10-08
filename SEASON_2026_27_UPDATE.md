# 2026-27 Season Update — Status and Next Steps

> Last updated 2026-10-02 (evening). Handoff doc for Claude Code.
> Project background (architecture, schema, dashboard views, goals) lives in the Claude Project doc `NHL_ANALYTICS_PROJECT_CONTEXT.md`.
> This file tracks the season rollover work and what comes next.

## Season facts

- 2026-27 opening night: **2026-09-29**. Regular season ends **2027-04-10**.
- **84 games per team** (up from 82). Don't hard-code or normalize around 82.
- Season ID `20262027`; game IDs look like `2026020017`. `gameType`: 1 preseason, 2 regular season, 3 playoffs. Ingestion keeps type 2 only.
- The NHL club schedule endpoint appeared to return games only through mid-December for at least one team, so the full schedule may not be published yet. The nightly games ingest picks up new games as they appear.
- NHL APIs are unofficial. Confirmed working 2026-10-02: `schedule/now`, `club-schedule-season`, `standings/now`, `partner-game/US/now`.

## Rollover work: done

All seven tasks from the original handoff are implemented, merged to `main` via PR #1 (branch `season-2026-27`, last commit `003f10d`).

| Task | Result | Commits |
|---|---|---|
| Remove hard-coded season | `common.current_season()` uses `SEASON` env, else derives from today's date (month >= 9 → new season). All scripts and workflows use it. | `aa277c6` |
| Games upsert propagates reschedules; dict dedupe | Done | `1a70bd9` |
| Scheduled runs | `nightly_ingest.yml` (06:00 UTC: games → pbp → shifts), `weekly_players.yml` (Mon 07:00 UTC), `betting_lines.yml` (14:00, 16:30, 21:30 UTC). All keep `workflow_dispatch`. | `6fb09cf`, `70c84af` |
| Incremental pbp/shifts + `FULL_REFRESH` flag | Done | `e693028` |
| Team list from standings API (fallback + 32-team check) | `common.get_teams()` | `dfecb70` |
| Betting lines ingest | `ingest_betting_lines.py` from the NHL partner odds endpoint (DraftKings). Stores raw and de-vigged probabilities. PK `(game_id, sportsbook, captured_at)`. | `70c84af` |
| Housekeeping | Deps pinned, dev deps in `requirements-dev.txt`, all DDL in `sql/schema.sql` applied by `ensure_schema`, batch upserts, `.gitattributes` for line endings | `ee75838`, `e636708`, `10d1a4e`, `b6635ba`, `f319dfa`, `003f10d` |

## Current repo state

| Path | Status |
|---|---|
| `ingestion/common.py` | Shared helpers: `current_season`, `get_engine`, `ensure_schema`, `get_teams` |
| `ingestion/ingest_games.py`, `ingest_play_by_play.py`, `ingest_shifts.py`, `ingest_players.py`, `ingest_betting_lines.py` | Working (code-reviewed; not yet confirmed against production runs, see below) |
| `ingestion/ingest_edge_stats.py` | 16-line scaffold, no logic |
| `ingestion/ingest_goalie_stats.py` | Empty |
| `sql/schema.sql` | Single source of truth for DDL (players, games, play_by_play, shifts, betting_lines) |
| `dbt/`, `streamlit/` | Empty |
| xG model, MLflow, game prediction model | Not started |
| `explore_nhl_api.py` | Still has `SEASON = "20252026"` (exploration script; update or ignore) |

## Verify first (not yet confirmed)

The following could not be checked from the assistant's session (no direct DB connection, no GitHub API access). Confirm them yourself:

1. **Local sync.** Local `main` is 14 commits behind `origin/main`. Run `git checkout main && git pull`, then delete the merged branch. Commit this doc.
2. **Actions are enabled and green.** Open the repo's Actions tab. Trigger `Nightly Ingest` and `Betting Lines` once via "Run workflow" rather than waiting for the cron. Confirm `DB_URL` is set as a repo secret.
3. **Data is landing.** Run against Neon:
   ```sql
   SELECT season, count(*) AS games, count(*) FILTER (WHERE completed) AS completed,
          min(date), max(date) FROM games GROUP BY season ORDER BY season;

   -- completed games missing event/shift data (should be 0 after a run)
   SELECT g.season, count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM play_by_play p WHERE p.game_id = g.game_id)) AS no_pbp,
          count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM shifts s WHERE s.game_id = g.game_id)) AS no_shifts
   FROM games g WHERE g.completed GROUP BY g.season ORDER BY g.season;

   SELECT count(*), count(DISTINCT game_id), min(captured_at), max(captured_at) FROM betting_lines;
   ```
   Expect: 2026-27 games present with the Sept 29 onward games completed and loaded; 2025-26 complete; betting lines accumulating.
4. **Sanity-check the odds endpoint.** After a few days, check that `betting_lines` gets a distinct row when the line moves (not just one per day), and that the last pre-game capture is a believable closing line.
5. **Python version.** Local pycache shows Python 3.14; Actions uses 3.12. Fine as long as the pinned deps install on both; keep an eye on it.

## Next steps, in priority order

### 1. Backfill prior seasons for training data
Games ingest takes `SEASON`, so run `SEASON=20242025` (and optionally `20232024`) through games → pbp → shifts. Two or three completed seasons give a much better xG training set than one. This is a long first run; do it via `workflow_dispatch` with the season input, or locally.

### 2. Phase 2: xG model (the next unbuilt phase)
**Full spec for this phase is in `XG_MODEL_CONTEXT.md`.**
- Features from `play_by_play`: shot distance, angle, shot type, strength, rebound, rush, time since last event, score state, empty net.
- Baseline logistic regression, then XGBoost. Track both in MLflow.
- **Train on completed seasons, evaluate on 2026-27 as it accumulates.** That's a clean out-of-time test, and it's a good story for interviews. Also report calibration (reliability curve) and log loss, not just AUC.

### 3. Initialize dbt
`dbt/` is empty. Set up the project (Postgres adapter, sources for the raw tables), staging models, then `xg_shots`, `ev_bets`, `player_pairings`. Add basic tests (unique/not-null on keys; relationship from `betting_lines.game_id` to `games`).

### 4. Game prediction model (gap in the original plan)
The spec lists `game_predictions` but the build order only covers xG. A win-probability model is its own step. Start simple: rolling team xG differential, rest days, home ice, goalie, fed into logistic regression. Calibrate against `home_fair_prob` from `betting_lines`. Without it, the +EV tracker has nothing to compare against the market.

### 5. Streamlit v1
Build the shot map and xG view first (data and model exist), then Tonight's Predictions + EV tracker once the prediction model and a few weeks of betting lines exist. Deploy to Streamlit Cloud.

### 6. Pipeline health
- Add row-count assertions or a freshness check (e.g. fail the nightly job if the previous day's completed games have no play-by-play).
- GitHub emails on workflow failure by default; make sure those emails reach you.
- Optional second odds source (The Odds API, 500 requests/month) for multi-book comparison. The NHL endpoint gives one book only.

### 7. Phase 4 items, later
Pairing analysis (shifts × play-by-play), `ingest_goalie_stats.py`, `ingest_edge_stats.py` (EDGE endpoints are reverse-engineered and may be unstable), goalie matchup view, README polish.

## Season rollover checklist (for next September)
- Confirm `current_season()` rolls over (month >= 9) and the nightly job picks up the new season.
- Check games per team; check `get_teams()` still returns the right number (it raises if not 32, which will need updating on expansion).
- Check Actions workflows haven't been auto-disabled (60 days of repo inactivity pauses scheduled runs).
- Verify `DB_URL` secret and Neon database are still alive.
- Re-test the API endpoints; they are unofficial.

## Constraints
- Keep every script idempotent (upsert) and rate-limited.
- Regular season only (`gameType == 2`) for now.
- Schema changes additive (`ADD COLUMN IF NOT EXISTS`), applied through `sql/schema.sql`.
- Never print or commit `.env`.
