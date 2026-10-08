select shift_id, game_id, player_id, period, shift_number, start_time, end_time, duration,
       team_id, team_abbrev, type_code, detail_code, event_number
from {{ source('raw', 'shifts') }}
