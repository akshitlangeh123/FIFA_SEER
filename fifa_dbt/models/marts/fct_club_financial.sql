{{ config(
    materialized='view'
) }}

select
    club_name,
    max_club_wage,
    avg_club_wage,
    total_club_value,
    avg_club_value,
    updated_at
from {{ ref('int_player_clubs') }}