{{ config(
    materialized='view'
) }}

select
    long_name,
    nationality,
    club_name,
    league_name,
    player_positions,
    preferred_foot,
    work_rate,
    updated_at
from {{ ref('stg_players') }}