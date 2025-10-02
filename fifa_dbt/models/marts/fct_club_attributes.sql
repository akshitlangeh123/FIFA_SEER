{{ config(
    materialized='view'
) }}

select
    club_name,
    num_club_players,
    avg_player_rating,
    avg_player_potential_rating,
    avg_club_age,
    updated_at
from {{ ref('int_player_clubs') }}