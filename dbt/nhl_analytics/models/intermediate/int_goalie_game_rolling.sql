-- One row per goalie per team-game with the goalie's GSAx before this date, over every loaded season (goalie
-- talent needs more shots than a season gives), shrunk per shot toward a replacement level.
{%- set k = var('shrink_k_goalie_shots') %}
{%- set prior_rate = var('goalie_prior_gsax_per_100') / 100 %}
with league_daily as (
    select season, date, sum(shots_faced) as shots, sum(gsax) as gsax
    from {{ ref('fct_goalie_games') }}
    group by 1, 2
),
league_season as (
    select season, sum(gsax) / sum(shots) as rate
    from league_daily
    group by 1
),
-- League GSAx per shot over earlier dates this season, blended with last season's rate as 20,000 pseudo-shots
-- (about three weeks). It moves with xG drift between seasons (+0.37 per 100 shots in 2025-26), so each game is
-- measured against it. The earliest season has no prior; the xG model was trained on it, so its rate is ~0.
league as (
    select d.season, d.date,
           (coalesce(sum(d.gsax) over w, 0) + 20000 * coalesce(p.rate, 0))
           / (coalesce(sum(d.shots) over w, 0) + 20000) as league_rate
    from league_daily d
    left join league_season p on p.season = d.season - 10001
    window w as (partition by d.season order by d.date rows between unbounded preceding and 1 preceding)
),
adjusted as (
    select g.*, l.league_rate, g.gsax - g.shots_faced * l.league_rate as gsax_adj
    from {{ ref('fct_goalie_games') }} g
    join league l using (season, date)
),
-- A goalie plays at most once a date, so the row frame excludes exactly the current game
prior as (
    select game_id, season, date, team_id, goalie_id, is_starter, league_rate,
           count(*) over w as games_prior,
           coalesce(sum(shots_faced) over w, 0) as shots_prior,
           coalesce(sum(gsax_adj) over w, 0) as gsax_adj_prior_sum
    from adjusted
    window w as (partition by goalie_id order by date rows between unbounded preceding and 1 preceding)
)
select
    *,
    (gsax_adj_prior_sum + {{ k }} * {{ prior_rate }}) / (shots_prior + {{ k }}) as gsax_per_shot_shrunk
from prior
