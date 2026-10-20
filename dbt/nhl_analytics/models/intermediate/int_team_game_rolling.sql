-- One row per team per game, completed or scheduled, with what was known before puck drop:
-- season-to-date sums over the team's earlier games, a league-average anchor, and per-game rates shrunk
-- toward that anchor by k pseudo-games. Sums and counts are kept so training can re-tune k without a rebuild.
{%- set stats = [
    ('xgf_5v5', var('shrink_k_5v5')),
    ('xga_5v5', var('shrink_k_5v5')),
    ('st_net', var('shrink_k_st')),
    ('gsax', var('shrink_k_gsax')),
] %}
with team_games as (
    select game_id, season, date, team_id, opp_id, is_home,
           xgf_5v5, xga_5v5, pp_xgf - pk_xga as st_net, xga_goalie - ga_goalie as gsax
    from {{ ref('fct_team_games') }}
    union all
    select game_id, season, date, home_team_id, away_team_id, true, null, null, null, null
    from {{ ref('stg_games') }} where not completed
    union all
    select game_id, season, date, away_team_id, home_team_id, false, null, null, null, null
    from {{ ref('stg_games') }} where not completed
),
-- League totals per date. Scheduled dates are included with zero games so every row finds its anchor.
daily as (
    select season, date, count(xgf_5v5) as n
        {%- for s, k in stats %}, sum({{ s }}) as {{ s }}{% endfor %}
    from team_games
    group by 1, 2
),
season_avg as (
    select season
        {%- for s, k in stats %}, sum({{ s }}) / nullif(sum(n), 0) as {{ s }}{% endfor %}
    from daily
    group by 1
),
-- Anchor: league average over earlier dates this season, else last season's (opening night). Same-night
-- games are excluded. It mostly cancels in the home-minus-away deltas, so its exact choice matters little.
-- Null only on the first night of the earliest season loaded.
league as (
    select d.season, d.date
        {%- for s, k in stats %},
        coalesce(sum(d.{{ s }}) over w / nullif(sum(d.n) over w, 0), p.{{ s }}) as {{ s }}{% endfor %}
    from daily d
    left join season_avg p on p.season = d.season - 10001
    window w as (partition by d.season order by d.date rows between unbounded preceding and 1 preceding)
),
-- A team plays at most once a date, so the row frame excludes exactly the current game
prior as (
    select game_id, season, date, team_id, opp_id, is_home,
           count(xgf_5v5) over w as games_prior
        {%- for s, k in stats %},
           sum({{ s }}) over w as {{ s }}_prior_sum{% endfor %},
           -- Over completed and scheduled games, so tonight's game gets rest from the last one played
           date - lag(date) over (partition by team_id, season order by date) as rest_days
    from team_games
    window w as (partition by team_id, season order by date rows between unbounded preceding and 1 preceding)
)
select
    p.game_id, p.season, p.date, p.team_id, p.opp_id, p.is_home,
    p.rest_days,
    coalesce(p.rest_days = 1, false) as is_b2b,
    p.games_prior
    {%- for s, k in stats %},
    p.{{ s }}_prior_sum,
    l.{{ s }} as {{ s }}_league_avg,
    (coalesce(p.{{ s }}_prior_sum, 0) + {{ k }} * l.{{ s }}) / (p.games_prior + {{ k }}) as {{ s }}_shrunk{% endfor %}
from prior p
join league l using (season, date)
