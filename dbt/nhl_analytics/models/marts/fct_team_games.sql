-- One row per team per completed game, with xG for and against.
-- Scheduled games are added later, in fct_game_features.
with sides as (
    select game_id, season, date, home_team_id as team_id, away_team_id as opp_id, true as is_home,
           home_score as gf, away_score as ga
    from {{ ref('stg_games') }} where completed
    union all
    select game_id, season, date, away_team_id, home_team_id, false,
           away_score, home_score
    from {{ ref('stg_games') }} where completed
),
-- '1551' is 5v5 with both goalies in, so empty-net shots are excluded from the 5v5 columns
xg_for as (
    select game_id, shooting_team_id as team_id,
           sum(xg) as xgf,
           sum(xg) filter (where situation_code = '1551') as xgf_5v5
    from {{ ref('fct_shots_xg') }}
    group by 1, 2
),
xg_against as (
    select game_id, defending_team_id as team_id,
           sum(xg) as xga,
           sum(xg) filter (where situation_code = '1551') as xga_5v5
    from {{ ref('fct_shots_xg') }}
    group by 1, 2
)
select
    s.*,
    s.gf > s.ga as won,
    -- Per season, so a team's first game doesn't get the off-season as rest
    s.date - lag(s.date) over (partition by s.team_id, s.season order by s.date) as rest_days,
    f.xgf,
    a.xga,
    f.xgf_5v5,
    a.xga_5v5
from sides s
left join xg_for f using (game_id, team_id)
left join xg_against a using (game_id, team_id)
