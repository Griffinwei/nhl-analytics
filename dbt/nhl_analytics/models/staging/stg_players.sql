select player_id, first_name, last_name, position, team, birth_date, jersey_number, shoots
from {{ source('raw', 'players') }}
