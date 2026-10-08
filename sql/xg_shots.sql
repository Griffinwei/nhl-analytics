-- Unblocked shot attempts for the xG model, one row per shot. Read-only; moves into dbt later.
SELECT
    p.game_id,
    p.event_id,
    g.season,
    (p.event_type = 'goal')::int AS is_goal,
    (p.event_owner_team_id = g.home_team_id) AS is_home,
    p.home_team_defending_side,
    p.x_coord,
    p.y_coord,
    p.situation_code,
    p.strength,
    -- Null shot_type only ever occurs on goals, so a 'missing' category would leak the outcome
    COALESCE(p.shot_type, 'wrist') AS shot_type
FROM play_by_play p
JOIN games g USING (game_id)
WHERE g.completed
  AND g.season = ANY(:seasons)
  AND p.event_type IN ('shot-on-goal', 'missed-shot', 'goal')
  AND p.period_type <> 'SO'
  AND p.situation_code NOT IN ('0101', '1010')  -- penalty shots
