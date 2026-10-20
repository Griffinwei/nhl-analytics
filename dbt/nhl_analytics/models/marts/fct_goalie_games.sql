-- One row per goalie per team-game: unblocked shots faced with the goalie in net, and goals saved above expected.
-- The starter is the goalie in net for the team's first shot against.
with faced as (
    select game_id, season, defending_team_id as team_id, goalie_id, sort_order, xg, is_goal
    from {{ ref('fct_shots_xg') }}
    where goalie_id is not null
),
first_shot as (
    select distinct on (game_id, team_id) game_id, team_id, goalie_id as starter_id
    from faced
    order by game_id, team_id, sort_order
)
select
    f.game_id,
    f.season,
    g.date,
    f.team_id,
    f.goalie_id,
    f.goalie_id = s.starter_id as is_starter,
    count(*) as shots_faced,
    sum(f.xg) as xga,
    sum(f.is_goal) as ga,
    sum(f.xg) - sum(f.is_goal) as gsax
from faced f
join first_shot s using (game_id, team_id)
join {{ ref('stg_games') }} g using (game_id)
group by f.game_id, f.season, g.date, f.team_id, f.goalie_id, s.starter_id
