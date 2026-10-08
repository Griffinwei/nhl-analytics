{{ config(severity='warn') }}
-- Soft check: a season's total xG should be within 10% of its goals. Scorer drift shows up here first.
select season, sum(xg) as xg, sum(is_goal) as goals
from {{ ref('fct_shots_xg') }}
group by season
having abs(sum(xg) / sum(is_goal) - 1) > 0.10
