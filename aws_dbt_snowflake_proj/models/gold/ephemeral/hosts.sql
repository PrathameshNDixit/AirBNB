{{
  config(
    materialized='ephemeral'
  )
}}

WITH hosts AS (
    SELECT
        host_id,
        host_name_cleaned,
        response_rate_quality,
        host_created_at
    FROM {{ ref('obt') }}
)

SELECT * FROM hosts