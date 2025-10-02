{{ config(
    materialized='view'
) }}

select
    long_name,
    nationality,
    age,
    height_cm,
    weight_kg,
    overall,
    potential,
    updated_at
from {{ ref('stg_players') }}