# xG Model — Implementation Context

> Written 2026-10-05 as a handoff for Claude Code; updated 2026-10-06 with data-validation and feature-research findings. This is Phase 2 of the project.
> Project background: Claude Project doc `NHL_ANALYTICS_PROJECT_CONTEXT.md`. Pipeline status: `SEASON_2026_27_UPDATE.md`.

## Goal

Build an expected-goals (xG) model: for every unblocked shot attempt, predict the probability it becomes a goal. Output feeds the shot-map dashboard view, team/game xG totals, and later the game win-probability model.

- **Unit of observation:** one unblocked shot attempt (`event_type IN ('shot-on-goal', 'missed-shot', 'goal')`).
- **Target:** `is_goal = (event_type = 'goal')`. Base rate is about 7.0–7.4% (6.7% excluding empty-net shots).
- **Blocked shots are excluded** (their coordinates are unreliable for the shooter's location). They still count as prior attempts for the time-between-shots features.
- **Exclude shootouts and penalty shots** (`period_type = 'SO'`; penalty shots have situation codes `0101`/`1010`).
- Regular season only (already true of the data).

## Status (2026-10-06)

- Prerequisites 1 and 2 are done; 4 is done except committing. Prerequisite 3 (handedness) is open.
- A first prototype (`models/xg/`) was built, run, and then **deliberately reverted** so the spec could be revised first. Its results are recorded under [Prototype results](#prototype-results-2026-10-05-reverted-code). Rebuild from this spec.
- MLflow 3.16.1 is installed locally; use a SQLite tracking store (`sqlite:///mlflow.db`). `mlruns/`, `mlflow.db`, `.cache/` are in `.gitignore`.

## What exists in the data today

Source table: `play_by_play` (PK `game_id, event_id`), joined to `games` and `players`. DDL is in `sql/schema.sql`.

Relevant `play_by_play` columns: `sort_order`, `period`, `period_type`, `time_in_period` (text `MM:SS`), `event_type`, `situation_code`, `strength`, `home_team_defending_side`, `x_coord`, `y_coord`, `zone`, `event_owner_team_id`, `distance`, `shooting_player_id`, `scoring_player_id`, `shot_type`, `goalie_in_net_id`, `stoppage_reason`, `stoppage_secondary_reason`.

Loaded (checked 2026-10-05; counts after the filters above):

| Season | Completed games | Play-by-play | Shifts | Shots | Goals |
|---|---|---|---|---|---|
| 2024-25 | 1,312 | 1,312 | **1,255** (57 missing) | 112,207 | 7,894 |
| 2025-26 | 1,312 | 1,312 | 1,312 | 112,058 | 8,080 |
| 2026-27 | live (39 on 10-05) | all | all | 3,236 | 240 |

Things the ingestion already computes (see `ingestion/ingest_play_by_play.py`):
- `strength`: `EV` / `PP` / `SH` **from the shooter's perspective** (`decode_strength`). Coarse: it does not distinguish 5v4 from 5v3, or 5v5 from 4v4/3v3, and ignores the goalie digits.
- `distance`: rounded integer feet to `(±89, 0)` (`calculate_distance`). **Only populated for unblocked shots with `zone = 'O'`.** Verified: the recomputed distance matches it within 0.5 ft on all 218k comparable rows.

Data facts and gaps:
- **`games.home_team_id` / `away_team_id`** now exist and are populated for every game in all three seasons; every shot's `event_owner_team_id` matches one of them. `is_home = (p.event_owner_team_id = g.home_team_id)`.
- **The shooter is `COALESCE(shooting_player_id, scoring_player_id)`** (goals use `scoring_player_id`). Never null.
- **`home_team_defending_side` and coordinates are never null** on unblocked shots.
- **Null `shot_type` leaks the outcome:** all 55 null-`shot_type` attempts are goals. Do not make "missing" its own category; impute `wrist` (the mode).
- **`players` covers current rosters only:** 248 of 1,086 shooters have no row. See Prerequisite 3.
- **Scorer drift across seasons:** "snap" share rose 20.6% → 24.8% → 28.1% (2024-25 → 2026-27) while "wrist" fell 48.7% → 43.4% → 40.9%, and median shot distance fell 33.2 → 32.0 → 31.2 ft. Snap scores higher than wrist conditional on location, so a model trained on 2024-25 over-predicts 2025-26 by 4–5%. Mitigations to test: more training seasons, merging wrist + snap, per-season recalibration.
- **Arena scorer bias:** the mean distance of visiting teams' shots varies by arena with a standard deviation of 0.87 ft, about 2× what sampling noise alone gives (0.42 ft). Range: UTA +2.3 ft to CHI −1.3 ft; share of shots inside 10 ft ranges 10.8%–15.4%. Real but modest; an arena adjustment is a later experiment.
- **Storage:** `shifts` is 306 MB of a 476 MB database and grows about 150 MB per season. On Neon's free plan (0.5 GB) this runs out during 2026-27. Option: compute per-shot shift features at ingest time and stop storing raw shifts.

## Prerequisites

1. **Home/away team IDs on `games`.** ✅ Done (`sql/schema.sql`, `ingest_games.py`; all three seasons re-ingested 2026-10-05).
2. **Training data loaded.** ✅ 2024-25 and 2025-26 are complete (table above). Optional: backfill 2023-24 (games → play-by-play; shifts only if storage allows) to dilute season drift.
3. **Handedness for all shooters** (needed only for off-wing). Open.
   - Fill from `GET /v1/roster/{team}/{season}` (`shootsCatches`; `roster/BOS/20242025` appeared to return only 19 players, so check coverage). Fallback per player: `GET /v1/player/{id}/landing` (untested).
   - `rosterSpots` in the play-by-play response lists every player who dressed (no handedness), which shows exactly which IDs are needed.
   - Don't let this upsert overwrite a current player's `team`; insert missing players only, or use a separate small table.
4. **Dependencies.** ✅ `requirements-ml.txt` (pandas, pyarrow, scikit-learn, xgboost, matplotlib, mlflow; pinned). Not yet committed.

## Coordinates and goal location (verified 2026-10-05/06)

The public API references (`Zmalski/NHL-API-Reference`, `coreyjs/nhl-api-py`) document **nothing** about the coordinate system beyond field names. Everything below was verified empirically against the data and a live play-by-play response (game 2026020017).

- **Frame:** origin at center ice, integer feet. Observed range x −99…99, y −42…42 (rink is 200 × 85). **y is negative for 49% of shots.** Rulebook geometry: goal lines at x = ±89, net mouth 6 ft wide (posts at y = ±3) on the goal line, cage up to 40 in deep behind it, blue lines at x = ±25.
- **`homeTeamDefendingSide`** flips every period. `'left'` means the home net is at x = −89, so home attacks +x. Confirmed in the live game: home shots at +81…+89 under `left` and at −81…−96 under `right`.
- **Normalization** — flip **both** x and y so every shooter attacks `(89, 0)`:
  ```python
  attacks_positive = (is_home & (side == "left")) | (~is_home & (side == "right"))
  x = np.where(attacks_positive, x_coord, -x_coord)
  y = np.where(attacks_positive, y_coord, -y_coord)
  ```
  Switching ends is a 180° rotation, not a mirror. Evidence: Ovechkin (8471214, right shot, left-circle shooter) has 65% of zone-`O` attempts at normalized `y > 0`, stable in every period (0.68 / 0.65 / 0.62 / 0.62); flipping x only gives 0.41 / 0.61 / 0.43 by period. **Normalized `y > 0` is the attacking team's left.**
- **Zone-code agreement:** after normalization, every zone-`O` shot has x ≥ 25 (the 240 at exactly x = 25 sit on the blue line); every neutral-zone shot is within −25…25; only 20 zone-`D` shots (0.01%, spread across games) are at x > 25 (scorer error).
- **Net location sweep:** validation log loss for a spline(distance) + spline(angle) logistic, with distance and angle measured to `(gx, 0)`. Train 2024-25, validate 2025-26, non-empty-net. 95% CIs from a 500× game bootstrap of the paired difference:

  | gx | 86 | 87 | 88 | **89** | 90 | 91 | 92 | 93 | 94 |
  |---|---|---|---|---|---|---|---|---|---|
  | log loss | 0.22967 | 0.22903 | 0.22847 | **0.22817** | 0.22807 | 0.22813 | 0.22824 | 0.22836 | 0.22846 |
  | Δ vs 89 | +0.00151 | +0.00086 | +0.00030 | 0 | −0.00009 | −0.00004 | +0.00007 | +0.00019 | +0.00029 |
  | 95% CI | [+.00125, +.00175] | [+.00068, +.00103] | [+.00021, +.00039] | | [−.00016, −.00002] | [−.00017, +.00011] | [−.00010, +.00027] | [−.00002, +.00043] | [+.00005, +.00057] |

  The minimum is at 90 (the middle of the cage, just behind the goal line). 89–92 are effectively tied, degradation starts at 93–94, and the loss is about 5× steeper per foot in front of the goal line than behind it. **Decision: use `(89, 0)`** (rulebook goal line, matches the stored `distance`; costs 0.00009). Never put the reference point in front of the goal line.
- **Fallback if `home_team_defending_side` is ever null:** for `zone = 'O'`, the attacking net is on the side matching the sign of `x_coord`. (Never needed so far.)

## Distance: distribution and functional form (2026-10-05)

Non-empty-net shots, all three seasons (225k):
- **Right-skewed:** median 32.4 ft, IQR 16.3–48.3, mean 35.2, skew 1.9. 13.5% of shots are inside 10 ft, 30% inside 20 ft, 8% beyond the blue line (> 64 ft). Two humps: crease/slot (5–10 ft, 12.5% of shots) and the circles (30–40 ft, about 9% per 5-ft bin).
- **The goal-rate curve is not linear in distance or log-distance:**

  | Distance | Goal rate | Shape |
  |---|---|---|
  | 2 → 8 ft | 50% → 12% | very steep |
  | 8 → 25 ft | 12% → 10% | nearly flat |
  | 25 → 50 ft | 8% → 2.3% | steep |
  | 55–60 ft | 2.1% (bump vs 1.9% at 50–55) | point shots |
  | > 70 ft | 0.1–0.3% | near zero |

- **Behind the goal line:** 1.3% of shots, 3.6% goal rate. Angle > 90° handles them; keep distance to `(89, 0)`.
- **Functional-form comparison** (train 2024-25 → validate 2025-26, non-empty-net; "cal. error" = shot-weighted mean |predicted − observed| over distance bins):

  | Distance form | Val log loss | Cal. error | Worst bin |
  |---|---|---|---|
  | base rate | 0.24892 | 3.84 pts | 18.1 |
  | linear distance | 0.23313 | 1.07 | 7.8 |
  | log(distance + 1) alone | 0.23544 | 1.71 | 3.8 |
  | distance + log(distance) | 0.23302 | 1.00 | 9.3 |
  | **spline(distance)** (8 quantile knots) | **0.23175** | **0.31** | **1.2** |
  | distance + angle, linear | 0.23060 | 1.28 | 9.5 |
  | distance + log_d + angle + distance×angle | 0.23011 | 1.19 | 10.9 |
  | **spline(distance) + spline(angle)** | **0.22817** | **0.33** | **1.0** |
  | XGBoost(distance, angle), early-stopped on val | 0.22746 | 0.26 | 1.2 |

  Linear distance misses both ends (0–5 ft: 16.9% predicted vs 24.7% observed; 20–35 ft under-predicted). Log-distance alone fixes the doorstep but over-predicts long shots badly (2.2% vs 0.27% at 70–100 ft). **Use splines** (`SplineTransformer`, cubic, about 8 quantile knots, linear extrapolation) for distance and angle in logistic models. A spline logistic comes within 0.0007 of XGBoost and stays interpretable. Don't bin.
- **Empty-net shots follow a different curve** (median distance 89.5 ft; goal rate 91% inside 20 ft, 35% beyond 100 ft). A single `empty_net` dummy can't express that. Either interact empty net with distance or model empty-net shots separately (or exclude them from xG). `empty_net` from the situation code matches `goalie_in_net_id IS NULL` exactly (1,951 shots, 53.5% goal rate).

## Feature specification

### v1: location, game state, shot type
| Feature | Definition | Notes |
|---|---|---|
| `distance` | `hypot(89 - x, y)` | To the center of the goal line. Recompute unrounded for all zones. Spline in logistic models. |
| `angle` | `degrees(arctan2(abs(y), 89 - x))` | 0° straight on, 90° on the goal line, > 90° behind the net. `abs(y)` because the net is symmetric. Spline in logistic models. |
| `empty_net` | defending team's goalie digit in `situation_code` is `0` | Needs its own distance relationship (see above). |
| `shooter_skaters`, `defender_skaters` | from `situation_code` | `[away_goalie][away_skaters][home_skaters][home_goalie]` (digit order confirmed in NHL-API-Reference issue #28); flip using `is_home`. |
| `strength` | existing `EV`/`PP`/`SH` | Categorical, or derive from skater counts. |
| `shot_type` | categorical | Impute null → `wrist` (null is goals only). Fold `between-legs`, `cradle`, and unseen types into `other`. Fix category levels so train/val/test encodings line up. |
| `is_off_wing` | shooter handedness vs. sign of normalized `y` | Normalized `y > 0` = attacking team's left (verified). Population signal is weak (forwards at `y > 0`: L 48.8%, R 46.5%). Needs Prerequisite 3. |

### v2: pre-shot context (all derived from play-by-play + shifts)

**Ordering rule (critical, a confirmed leak):** order and compare events by `sort_order`, **never by clock time**. Many events share a clock second; in particular, the faceoff after a goal happens at the same second as the goal. A prototype that matched faceoffs to shots by clock counted the post-goal faceoff as a whistle "during the shift", so every goal had ≥ 1 whistle and every non-goal had 0. Use clock time only for durations.

**Play segment:** the events between two whistles. A new segment starts after any `faceoff`, `stoppage`, `period-start`, `goal` or `penalty`. "Previous attempt" features look only within the current segment.

#### Time between shots
| Feature | Definition |
|---|---|
| `since_prev_attempt` | Game-clock seconds since the previous **same-team** shot attempt (shot, miss, goal **or blocked**) in the same play segment. Cap at 30 s; use 30 when there is no prior attempt. Spline it. |
| `rebound_dy` | For attempts ≤ 3 s after a same-team attempt: abs(change in normalized `y`) between the two (lateral puck movement, a goalie-movement proxy). 0 otherwise. |
| `prev_attempt_type` | Type of that previous attempt (saved / missed / blocked). Optional. |

Note: in this API a `blocked-shot` event's `event_owner_team_id` is the **blocking** team; the shooting team is the other one.

Findings (2025-26, non-empty-net, location-only xG model trained on 2024-25):

| Since previous same-team attempt | Shots | Goals / xG | Median distance |
|---|---|---|---|
| no prior attempt in segment | 70,237 | 0.96 | 34.0 ft |
| 0–1 s | 4,406 | **0.30** | 8.1 ft |
| 1–2 s | 3,030 | **1.42** | 10.8 ft |
| 2–3 s | 1,752 | **2.47** | 17.5 ft |
| 3–5 s | 2,558 | 1.39 | 32.7 ft |
| 5–10 s | 6,712 | 0.93 | 33.5 ft |
| 10–20 s | 8,028 | 1.02 | 32.8 ft |
| 20 s+ | 14,343 | 1.02 | 33.2 ft |

- The relationship is **non-monotonic**. 0–1 s attempts (crease whacks while the goalie is still set) convert at 30% of their location xG; 1–3 s rebounds convert at 1.4–2.5×. **A binary "rebound ≤ 3 s" flag therefore adds nothing** (the two effects cancel): CV Δ log loss +0.00001. Use the continuous, splined time instead.
- Lateral movement on quick rebounds: |dy| ≤ 5 ft → 0.82 goals/xG; 5–15 ft → 1.12.
- **5-fold CV grouped by game** (2025-26, 111k shots, Δ log loss vs spline location): `since_prev_attempt` **−0.00068** [−0.00090, −0.00047]; + `rebound_dy` **−0.00075** [−0.00098, −0.00052]; adding "opponent attempt within 5 s" adds nothing.

#### Shift length / fatigue (from `shifts`)
On-ice rule for an event at time t: shift `start < t <= end`, same game and period, `type_code = 517` (505 rows are goal markers). Identify goalies by `goalie_in_net_id` ∪ `players.position = 'G'`.

Validated on all 112k 2025-26 shots: on-ice skater counts match the situation code for **99.0%** of shots (both teams), and the defending goalie's presence matches 100%.

| Feature | Definition |
|---|---|
| `def_mean_shift`, `off_mean_shift` | mean game-clock seconds the defending / shooting team's on-ice skaters have been on (`t - start`). Spline. |
| `since_whistle` | game-clock seconds since the segment began |
| `def_max_shift`, `def_whistles`, `def_iced`, `def_tv` | longest defender shift; mean whistles during defenders' shifts; defending team iced during these shifts (can't change); TV timeout during these shifts |

Icing team = the team whose defensive end hosts the next faceoff (`faceoff in home end = (x_coord < 0) == (side == 'left')`). About 8 icings and 8 TV timeouts per game.

Findings (2025-26, 5v5 `1551`, non-empty-net, 87k shots, 5-fold CV grouped by game, Δ log loss vs spline location):

| Added | Δ log loss | 95% CI |
|---|---|---|
| `since_whistle` | −0.00036 | [−0.00054, −0.00019] |
| **`def_mean_shift` + `off_mean_shift`** | **−0.00103** | [−0.00137, −0.00071] |
| `def_mean_shift − off_mean_shift` only | 0.00000 | [−0.00008, +0.00008] |
| def max shift, whistles, iced, TV | −0.00043 | [−0.00066, −0.00019] |
| all of the above | −0.00086 | [−0.00120, −0.00051] |

- Shift length is a real signal: shots in the first 15 s of a shift convert at 0.63–0.66× location xG, and shots deep into long shifts at about 1.1–1.35×. **But it is not a fatigue mismatch:** the defense-minus-offense difference adds nothing, and the attackers' shift length shows the same pattern as the defenders'. Treat it as "how far into the shift/possession", which may overlap with the time-between-shots features. Re-test once those are in.
- Icing alone does nothing measurable (stuck defenders allowed 5.8% vs 6.2% before adjustment). Neither does TV timeout.

### What the APIs cannot provide (investigated 2026-10-06)
- **Passes:** none. The play-by-play has no pass event or pass fields (all `details` keys checked on a 2026-27 game). Assists exist only on goals.
- **Tracking (`pptReplayUrl`, `wsr.nhle.com/sprites/{season}/{game}/ev{N}.json`):** about 14 s of player and puck positions before the event (140 frames, roughly 10 Hz), **goals only** (15 of 15 goals, 0 other events, across three games). It would give exact pass sequences, but only for goals, so it leaks the outcome. Usable for descriptive analysis of goals, never as a training feature.
- **Whistle durations:** every per-event time is game clock. The only wall-clock times are period start/end, to the minute, in the HTML play-by-play report (`nhl.com/scores/htmlreports/{season}/PL{02xxxx}.HTM`, "Local time: 7:37 EDT"). The average duration of each stoppage type could be estimated by regressing (real period length − 20 min) on stoppage-type counts across periods; minute precision makes this noisy.
- **Pre-shot proxies are sparse:** only 15% of shots have any recorded event in the prior 2 s, and 50% have none in the prior 10 s, so passing plays are mostly invisible. The best available proxies are the time-between-shots and shift features above.
- **Per-shot speed:** not available (EDGE endpoints give only top-10 lists and aggregates).

### Later experiments
- **Rush:** shot within about 4 s of an event in the neutral or defensive zone (zone change), from the previous event's coordinates and time.
- **Visible net angle:** angle subtended by the posts at `(89, ±3)`.
- **Arena adjustment** for scorer bias (e.g. match each arena's distance distribution to the league's).
- **Whistle-type rest** via the period wall-clock regression above.
- **Merging wrist + snap**, or per-season recalibration, to absorb scorer drift.

### Do not use
- **Outcome/leakage columns:** `scoring_player_id` (except as the shooter ID), assist columns, `away_sog`/`home_sog`, `away_score`/`home_score` as recorded on the event.
- **"Missing" as a `shot_type` category** (goals only; see above).
- **Anything matched by clock time across events** (see the ordering rule).
- **Shooter or goalie identity** (that's talent, not shot quality; save for goals-above-expected).
- **Tracking / `pptReplayUrl` data** (goals only).

## Modeling plan

1. **Baseline: logistic regression** (scikit-learn): spline(distance) + spline(angle), then add features one at a time. One-hot `shot_type` and `strength` with fixed levels.
2. **XGBoost** (`binary:logistic`): raw distance, angle (optionally normalized x, y); max depth 3–5; early stopping on the validation season.
3. **Calibration:** check the high-xG tail; if it bends, fit isotonic or Platt scaling on the validation season (and then report test only).
4. **Do not rebalance classes** (no oversampling, SMOTE, or `scale_pos_weight`). xG needs calibrated probabilities.

Skip neural nets, SVMs, and random forests.

### Splits (by season, never by random row)
- Train: 2024-25 (plus 2023-24 if backfilled)
- Validate: 2025-26
- Test: 2026-27, live and growing (out-of-time; small for now)
- For feature research within a season, cross-validate with folds **grouped by game** (shots within a game aren't independent), and bootstrap games for CIs.

### Evaluation
- **Log loss** (primary), **AUC**, **calibration curve**, Brier score. Not accuracy.
- Raw log loss depends on the base rate. Compare against the base-rate log loss (improvement %) and the spline location baseline; compare log loss only across models scored on the same shot set.
- Public NHL xG models typically report AUC around 0.75–0.80 (from memory; treat as approximate); they include pre-shot movement, which is likely most of the gap.
- Sanity checks: xG ≈ goals per season; coefficient signs (goal odds fall with distance, very high for empty net).
- Because xG totals drive the dashboard, calibration (including xG ≈ goals per season) matters more than small log-loss gains.

### Experiment order (MLflow; a feature that doesn't help is still a result)
1. spline(distance) + spline(angle) (logistic)
2. + empty net (with its own distance term), skater counts, shot type, strength
3. + off-wing
4. same features, XGBoost
5. + time between shots (`since_prev_attempt`, `rebound_dy`)
6. + shift length (`def_mean_shift`, `off_mean_shift`)
7. later experiments above

## Prototype results (2026-10-05, reverted code)

Train 2024-25 / validate 2025-26 / test 2026-27 (39 games). Features: "base" = distance, log(distance), angle, empty net, skater counts, shot type, strength (linear terms, not splines).

| Run | Val log loss (base rate 0.25907) | Val AUC | Test log loss (base rate 0.26439) | Test AUC | Val xG / goals |
|---|---|---|---|---|---|
| distance, log(distance), angle (logistic) | 0.24554 | 0.691 | 0.25198 | 0.682 | 1.026 |
| base (logistic) | 0.23043 | 0.752 | 0.23855 | 0.738 | 1.050 |
| base (XGBoost, depth 4, early-stopped) | 0.22616 | 0.761 | 0.23613 | 0.742 | 1.044 |

- Improvement over the base rate: 12.7% (XGBoost, val), 10.7% (test).
- The logistic's top probability bin is over-predicted (about 0.32 predicted vs 0.245 observed).
- 2025-26 over-predicted by 4–5% (scorer drift, see above).

## Suggested layout
```
models/xg/
  features.py     # SQL pull + normalization + feature engineering -> DataFrame
  train.py        # fit, log to MLflow, save model
  evaluate.py     # metrics + calibration plot
sql/xg_shots.sql  # base query for unblocked attempts (moves into dbt later)
```
Run as modules from the repo root (`python -m models.xg.train`) so `from ingestion.common import get_engine` resolves. Predictions eventually land in an `xg_shots` table (planned as a dbt model): `game_id, event_id, xg, model_version`.

## Verification checklist
- [x] `games.home_team_id` / `away_team_id` populated for all loaded seasons.
- [x] Recomputed distance matches the stored `distance` column within rounding (218k rows, max diff 0.5 ft).
- [x] After normalization, zone codes agree with normalized x (only 20 of 227k conflict).
- [x] Empty-net shots have a far higher goal rate (53.5% vs 6.7%), and the situation code agrees with `goalie_in_net_id IS NULL`.
- [x] Shooter ID is non-null for every row.
- [x] Off-wing sign verified with a known player (normalized `y > 0` = attacking team's left).
- [x] On-ice players from `shifts` match the situation code (99.0% of 2025-26 shots).
- [ ] Every event-sequence feature uses `sort_order`, and a leak check shows a flat goal rate across feature values where expected (e.g. whistles in shift).
- [ ] Baseline logistic beats the base-rate log loss; XGBoost is compared on the same split.
- [ ] Sum of xG ≈ goals on validation and test seasons.

## Constraints
- Schema changes additive, through `sql/schema.sql`.
- Never print or commit `.env`.
- Read-only queries against Neon for feature building; write only the final predictions table.
