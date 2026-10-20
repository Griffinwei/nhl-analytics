-- The two rows of a game must mirror each other: one side's power play is the other's penalty kill,
-- and a goalie can't allow more goals than the final score (which adds shootout and empty-net goals)
select h.game_id
from {{ ref('fct_team_games') }} h
join {{ ref('fct_team_games') }} a on a.game_id = h.game_id and a.team_id = h.opp_id
where h.is_home
  and (abs(h.pp_xgf - a.pk_xga) > 1e-9
       or abs(a.pp_xgf - h.pk_xga) > 1e-9
       or abs(h.xgf - a.xga) > 1e-9
       or h.ga_goalie > h.ga
       or a.ga_goalie > a.ga
       or h.won = a.won)
