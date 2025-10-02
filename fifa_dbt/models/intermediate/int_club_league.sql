{{ config(materialized='view') }}

with stg as (
    select * from {{ ref('stg_players') }}
),
league_stats as (
    select 
    league_name,
    count(distinct long_name) as num_league_players,
    count(distinct club_name) as num_clubs,
    max(wage_eur) as max_league_wage,
    avg(wage_eur) as avg_league_wage,
    sum(value_eur) as total_league_value,
    avg(value_eur) as avg_league_value,
    max(updated_at) as updated_at
    from stg
    group by league_name
)

select * from league_stats