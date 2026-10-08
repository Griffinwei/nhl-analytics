select event_id, game_id, sort_order, period, period_type, time_in_period, time_remaining,
       event_type, type_code, situation_code, strength, home_team_defending_side,
       x_coord, y_coord, zone, event_owner_team_id, distance,
       winning_player_id, losing_player_id, hitting_player_id, hittee_player_id,
       shooting_player_id, shot_type, goalie_in_net_id, away_sog, home_sog,
       scoring_player_id, scoring_player_total, assist1_player_id, assist1_player_total,
       assist2_player_id, assist2_player_total, away_score, home_score,
       blocking_player_id, block_reason, player_id, stoppage_reason, stoppage_secondary_reason,
       miss_reason
from {{ source('raw', 'play_by_play') }}
