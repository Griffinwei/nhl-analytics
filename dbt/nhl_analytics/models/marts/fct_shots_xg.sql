-- One row per unblocked shot with its xG from a single model version
select
    f.*,
    case when f.is_home then g.home_team_id else g.away_team_id end as shooting_team_id,
    case when f.is_home then g.away_team_id else g.home_team_id end as defending_team_id,
    -- Null exactly when the defending net is empty. Kept out of int_shot_features, the xG model's input.
    e.goalie_in_net_id as goalie_id,
    e.sort_order,
    p.xg
from {{ ref('int_shot_features') }} f
join {{ ref('stg_games') }} g using (game_id)
join {{ ref('stg_play_by_play') }} e using (game_id, event_id)
join {{ source('raw', 'xg_predictions') }} p using (game_id, event_id)
where p.model_version = '{{ var("xg_model_version") }}'
