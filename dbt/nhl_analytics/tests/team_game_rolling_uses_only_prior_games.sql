-- Leakage check: recompute each pre-game sum from the team's games strictly before this date, same season
with recomputed as (
    select r.game_id, r.team_id,
           count(t.xgf_5v5) as games_prior,
           sum(t.xgf_5v5) as xgf_5v5,
           sum(t.pp_xgf - t.pk_xga) as st_net,
           sum(t.xga_goalie - t.ga_goalie) as gsax
    from {{ ref('int_team_game_rolling') }} r
    left join {{ ref('fct_team_games') }} t
      on t.team_id = r.team_id and t.season = r.season and t.date < r.date
    group by 1, 2
)
select r.game_id, r.team_id
from {{ ref('int_team_game_rolling') }} r
join recomputed c using (game_id, team_id)
where r.games_prior <> c.games_prior
   or abs(coalesce(r.xgf_5v5_prior_sum, 0) - coalesce(c.xgf_5v5, 0)) > 1e-9
   or abs(coalesce(r.st_net_prior_sum, 0) - coalesce(c.st_net, 0)) > 1e-9
   or abs(coalesce(r.gsax_prior_sum, 0) - coalesce(c.gsax, 0)) > 1e-9
