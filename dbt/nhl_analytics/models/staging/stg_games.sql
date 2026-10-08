select game_id, season, date, home_team_id, away_team_id, home_team, away_team,
       home_score, away_score, overtime, shootout, completed
from {{ source('raw', 'games') }}