-- Leakage check: recompute each goalie's prior shots and games from strictly earlier dates
with recomputed as (
    select r.game_id, r.goalie_id, count(g.game_id) as games_prior, coalesce(sum(g.shots_faced), 0) as shots_prior
    from {{ ref('int_goalie_game_rolling') }} r
    left join {{ ref('fct_goalie_games') }} g on g.goalie_id = r.goalie_id and g.date < r.date
    group by 1, 2
)
select r.game_id, r.goalie_id
from {{ ref('int_goalie_game_rolling') }} r
join recomputed c using (game_id, goalie_id)
where r.games_prior <> c.games_prior or r.shots_prior <> c.shots_prior
