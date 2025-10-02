{{ config(materialized='view') }}

with stg as (
    select * from {{ ref('stg_players') }}
),
club_stats as (
    select 
    club_name,
    count(distinct long_name) as num_club_players,
    max(wage_eur) as max_club_wage,
    avg(wage_eur) as avg_club_wage,
    sum(value_eur) as total_club_value,
    avg(value_eur) as avg_club_value,
    avg(overall) as  avg_player_rating,
    avg(potential) as avg_player_potential_rating,
    avg(age) as avg_club_age,
    max(updated_at) as updated_at
    from stg
    group by club_name
)

select * from club_stats;