-- Each team-game with xG has exactly one starter, and its goalies' shots add up to the team's goalie-in-net totals
select t.game_id, t.team_id
from {{ ref('fct_team_games') }} t
left join {{ ref('fct_goalie_games') }} g using (game_id, team_id)
where t.xga_goalie is not null
group by t.game_id, t.team_id, t.xga_goalie, t.ga_goalie
having count(*) filter (where g.is_starter) <> 1
    or abs(coalesce(sum(g.xga), 0) - t.xga_goalie) > 1e-9
    or coalesce(sum(g.ga), 0) <> t.ga_goalie
