{{ config(
    materialized='view'
) }}

select
    league_name,
    max_league_wage,
    avg_league_wage,
    total_league_value,
    avg_league_value,
    updated_at
from {{ ref('int_club_league') }}