select game_id, sportsbook, home_moneyline, away_moneyline, home_implied_prob, away_implied_prob,
       home_fair_prob, away_fair_prob, over_under, captured_at
from {{ source('raw', 'betting_lines') }}
