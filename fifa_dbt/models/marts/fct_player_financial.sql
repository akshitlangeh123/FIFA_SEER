{{ config(
    materialized='view'
) }}

select
    long_name,
    nationality,
    value_eur,
    wage_eur,
    updated_at
from {{ ref('stg_players') }}