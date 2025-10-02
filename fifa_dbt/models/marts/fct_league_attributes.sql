{{ config(
    materialized='view'
) }}

select
    league_name,
    num_league_players,
    num_clubs
    updated_at
from {{ ref('int_club_league') }}