{{
  config(
    materialized='incremental',
    unique_key='host_id'
  )
}}

SELECT
    host_id,
    -- 1. Ensure REPLACE has an 'AS' alias
    REPLACE(host_name, ' ', '_') AS host_name_cleaned, 
    host_since,
    is_superhost,
    -- 2. Ensure your CASE statement has an 'AS' alias!
    CASE 
        WHEN response_rate > 95 THEN 'very good'
        WHEN response_rate > 80 THEN 'good'
        ELSE 'poor' 
    END AS response_rate_quality, 
    created_at
FROM {{ ref('bronze_hosts') }}

{% if is_incremental() %}
  WHERE created_at > (SELECT COALESCE(MAX(created_at), '1900-01-01') FROM {{ this }})
{% endif %}