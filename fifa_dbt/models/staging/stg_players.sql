{{ config(materialized='view') }}

with source as (
    select 
        long_name,
        age,
        dob,
        height_cm,
        weight_kg,
        nationality,
        club_name,
        league_name,
        overall,
        potential,
        value_eur,
        wage_eur,
        player_positions,
        preferred_foot,
        work_rate,
        updated_at
    from {{ source('fifa_db', 'players') }}
)

select * from source