-- Unblocked shot attempts for the xG model, one row per shot. Read-only; moves into dbt later.
-- Events are ordered by sort_order only: many events share a clock second (e.g. a goal and the next faceoff).
WITH events AS (
    SELECT
        p.*,
        g.season,
        g.home_team_id,
        g.away_team_id,
        split_part(p.time_in_period, ':', 1)::int * 60 + split_part(p.time_in_period, ':', 2)::int AS seconds,
        -- Whistle-to-whistle play segment: whistles strictly before this event (a goal ends its own segment)
        count(*) FILTER (WHERE p.event_type IN ('period-start', 'faceoff', 'stoppage', 'penalty', 'goal'))
            OVER (PARTITION BY p.game_id ORDER BY p.sort_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS segment
    FROM play_by_play p
    JOIN games g USING (game_id)
    WHERE g.completed AND g.season = ANY(:seasons)
),
attempts AS (
    SELECT
        *,
        -- blocked-shot events are owned by the blocking team
        CASE WHEN event_type <> 'blocked-shot' THEN event_owner_team_id
             WHEN event_owner_team_id = home_team_id THEN away_team_id
             ELSE home_team_id END AS shooting_team_id
    FROM events
    WHERE event_type IN ('shot-on-goal', 'missed-shot', 'goal', 'blocked-shot')
),
with_previous AS (
    SELECT
        *,
        seconds - lag(seconds) OVER previous AS since_prev_attempt,
        abs(y_coord - lag(y_coord) OVER previous) AS prev_attempt_dy
    FROM attempts
    WINDOW previous AS (PARTITION BY game_id, segment, shooting_team_id ORDER BY sort_order)
)
SELECT
    game_id,
    event_id,
    season,
    (event_type = 'goal')::int AS is_goal,
    (event_owner_team_id = home_team_id) AS is_home,
    home_team_defending_side,
    x_coord,
    y_coord,
    situation_code,
    strength,
    -- Null shot_type only ever occurs on goals, so a 'missing' category would leak the outcome
    COALESCE(shot_type, 'wrist') AS shot_type,
    since_prev_attempt,
    prev_attempt_dy
FROM with_previous
WHERE event_type <> 'blocked-shot'
  AND period_type <> 'SO'
  AND situation_code NOT IN ('0101', '1010')  -- penalty shots
