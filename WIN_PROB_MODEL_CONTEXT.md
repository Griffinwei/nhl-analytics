# Win-Probability Model — Implementation Context

> Started 2026-10-15. Phase 3 of the project; consumes the xG model's output (`XG_MODEL_CONTEXT.md`).

## Model

Pre-game logistic regression on home-minus-away differences (Δ) of rolling team values:

```
logit P(home win) = b0 + b1 Δxgf_5v5 + b2 Δxga_5v5 + b3 Δst_net + b4 Δgsax + b5a home_b2b + b5b away_b2b
```

| Term | Feature | Expected sign |
|---|---|---|
| b0 | Intercept: home ice | + (logit of the even-matchup home win rate) |
| b1 | Δ 5v5 xG for per game | + |
| b2 | Δ 5v5 xG against per game | − |
| b3 | Δ special-teams net: `pp_xgf − pk_xga` per game | + |
| b4 | Δ goalie GSAx per game: `xga_goalie − ga_goalie` | + |
| b5a/b | Back-to-back flags (`rest_days = 1`), one per side | − home, + away |

Rest is two flags rather than Δrest days: nearly all of the effect is the back-to-back, and away teams play more of them.

## Plan

1. ✅ Unit, target and splits (below)
2. ✅ Per-game stats on `fct_team_games`
3. Pre-game rolling values with shrinkage toward the league average (`int_team_game_rolling`)
4. Goalie: team-level GSAx first; the starter's own GSAx (needs `goalie_in_net_id` in `int_shot_features`) as an experiment
5. `fct_game_features`: one row per game (completed and scheduled), deltas, target, market `home_fair_prob`
6. Fit (statsmodels for coefficients/SEs, sklearn for the artifact), MLflow experiment `win-prob`
7. Evaluate on log loss vs. constant home rate, calibration, game bootstrap; vs. the market on 2026-27
8. Nightly scoring into a `win_predictions` table

## Step 1: unit, target, splits

- **Unit:** one completed regular-season game, from the home side. Only `gameType` 2 is ingested, so no filter is needed.
- **Target:** `home_win = home_score > away_score`. **OT and shootout wins count.** `fct_team_games.decided_in` (`REG`/`OT`/`SO`) is there for a regulation-only variant.
- **Splits:** train 2024-25, validate 2025-26 (all tuning: shrinkage `k`, rest coding, goalie definition), test 2026-27 once. Refit on both full seasons for live scoring.
- **Sample is small:** ~1,312 games a season, so b0's SE is ~0.05 and the weaker terms (GSAx, rest) may not reach the expected sign. Keep the feature count at about six.

Data facts (checked 2026-10-15):

| Season | Completed | Home win | OT (incl. SO) | SO | Home win: REG / OT / SO |
|---|---|---|---|---|---|
| 2024-25 | 1,312 | 56.3% | 20.7% | 5.9% | 56.9 / 54.1 / 53.2% |
| 2025-26 | 1,312 | 52.2% | 24.9% | 9.1% | 53.3 / 50.2 / 47.1% |
| 2026-27 | 107 (live) | 53.3% | 20.6% | 7.5% | — |

- No ties, no null scores, `home_team_id`/`away_team_id` never null on completed games.
- `shootout` implies `overtime` in every row.
- **Home win rate fell 4 points between seasons.** With 1,312 games that's about 2 SEs, so the season split will show b0 shrinking from train to validation. Consider a season-pooled b0 for live use.

## Step 2: per-game stats on `fct_team_games`

New columns (dbt mart, rebuilt by the nightly `fct_shots_xg+` step; no raw-schema change):

| Column | Definition |
|---|---|
| `decided_in` | `REG` / `OT` / `SO` |
| `pp_xgf` | xG on shots for with `strength = 'PP'` and both goalies in |
| `pk_xga` | xG on shots against with the shooter's `strength = 'PP'` and both goalies in |
| `xga_goalie` | xG on shots against with this team's goalie in net, all strengths |
| `ga_goalie` | Goals (from shots) on the same shots, so shootout goals and empty-netters are excluded |

- `situation_code` digits are away goalie, away skaters, home skaters, home goalie. `strength = 'PP'` includes 6-on-5 with a goalie pulled (`0651`, `1560`), so the goalie digits are checked explicitly.
- Sums are coalesced inside the aggregation, so a team with no power-play shots gets 0 (about 5% of team-games) while a game with no xG predictions stays null.
- Test `team_games_sides_agree`: one side's `pp_xgf` equals the other's `pk_xga`, `xgf` equals the opponent's `xga`, `ga_goalie <= ga`, exactly one winner.

Values (per team per game):

| Season | xGF | 5v5 xGF | PP xGF | xGA (goalie in) | GA (goalie in) | GSAx | Team-season SD: ST net / GSAx / 5v5 xGF |
|---|---|---|---|---|---|---|---|
| 2024-25 | 3.01 | 2.01 | 0.56 | 2.82 | 2.81 | +0.01 | 0.11 / 0.23 / 0.17 |
| 2025-26 | 3.20 | 2.08 | 0.65 | 3.00 | 2.89 | +0.11 | 0.12 / 0.22 / 0.18 |

- GSAx is ~0 in 2024-25 because the xG model was trained on it. The +0.11 in 2025-26 is the model's ~4% over-prediction from scorer drift. Δgsax cancels league-wide shifts, but per-season levels shouldn't be compared directly.
- Team-season spread in GSAx is about twice that of special-teams net, so goaltending should matter, if the rolling estimate is stable enough (use a large shrinkage `k`).
