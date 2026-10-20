-- One row per team per completed game, with xG for and against.
-- Scheduled games are added later, in fct_game_features.
with sides as (
    select game_id, season, date, home_team_id as team_id, away_team_id as opp_id, true as is_home,
           home_score as gf, away_score as ga, overtime, shootout
    from {{ ref('stg_games') }} where completed
    union all
    select game_id, season, date, away_team_id, home_team_id, false,
           away_score, home_score, overtime, shootout
    from {{ ref('stg_games') }} where completed
),
-- situation_code is away goalie, away skaters, home skaters, home goalie. '1551' is 5v5 with both goalies in.
-- strength is from the shooter's side and includes 6-on-5 with a goalie pulled, hence the goalie-digit checks.
-- Sums are coalesced here, so a team with no shots of a kind gets 0 but a game with no xG data stays null.
xg_for as (
    select game_id, shooting_team_id as team_id,
           sum(xg) as xgf,
           coalesce(sum(xg) filter (where situation_code = '1551'), 0) as xgf_5v5,
           coalesce(sum(xg) filter (where strength = 'PP'
                                      and left(situation_code, 1) = '1' and right(situation_code, 1) = '1'), 0) as pp_xgf
    from {{ ref('fct_shots_xg') }}
    group by 1, 2
),
xg_against as (
    select game_id, defending_team_id as team_id,
           sum(xg) as xga,
           coalesce(sum(xg) filter (where situation_code = '1551'), 0) as xga_5v5,
           -- The shooter's power play is this team's penalty kill
           coalesce(sum(xg) filter (where strength = 'PP'
                                      and left(situation_code, 1) = '1' and right(situation_code, 1) = '1'), 0) as pk_xga,
           -- Shots this team's goalie faced, all strengths: the away digit when the home team shoots, else the home digit.
           -- Goals come from shots, not the score, so shootout goals and empty-netters are left out of GSAx.
           coalesce(sum(xg) filter (where case when is_home then left(situation_code, 1) else right(situation_code, 1) end = '1'), 0) as xga_goalie,
           coalesce(sum(is_goal) filter (where case when is_home then left(situation_code, 1) else right(situation_code, 1) end = '1'), 0) as ga_goalie
    from {{ ref('fct_shots_xg') }}
    group by 1, 2
)
select
    s.game_id, s.season, s.date, s.team_id, s.opp_id, s.is_home, s.gf, s.ga,
    -- Win-probability target: OT and shootout wins count as wins, decided_in allows a regulation-only variant
    s.gf > s.ga as won,
    case when s.shootout then 'SO' when s.overtime then 'OT' else 'REG' end as decided_in,
    -- Per season, so a team's first game doesn't get the off-season as rest
    s.date - lag(s.date) over (partition by s.team_id, s.season order by s.date) as rest_days,
    f.xgf,
    a.xga,
    f.xgf_5v5,
    a.xga_5v5,
    f.pp_xgf,
    a.pk_xga,
    a.xga_goalie,
    a.ga_goalie
from sides s
left join xg_for f using (game_id, team_id)
left join xg_against a using (game_id, team_id)
