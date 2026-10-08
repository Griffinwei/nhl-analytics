-- xG is a probability
select * from {{ ref('fct_shots_xg') }} where xg < 0 or xg > 1
