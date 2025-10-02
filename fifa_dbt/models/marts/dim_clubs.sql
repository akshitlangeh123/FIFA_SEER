{{ config(
    materialized='view'
) }}

select distinct club_name,
    updated_at
  from {{ ref('stg_players') }}
  where club_name is not null