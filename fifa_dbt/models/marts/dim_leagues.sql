{{ config(
    materialized='view'
) }}

select distinct league_name,
    updated_at
  from {{ ref('stg_players') }}