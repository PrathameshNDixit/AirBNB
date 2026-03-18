{{
  config(
    materialized='incremental',
    unique_key='host_id'
  )
}}

SELECT
    host_id,
    REPLACE(host_name, ' ', '_') AS host_name_cleaned, 
    host_since,
    is_superhost,
    -- 1. Create the alias here
    created_at AS host_created_at, 
    CASE 
        WHEN response_rate > 95 THEN 'very good'
        WHEN response_rate > 80 THEN 'good'
        ELSE 'poor' 
    END AS response_rate_quality
FROM {{ ref('bronze_hosts') }}

{% if is_incremental() %}
  WHERE 
    -- 2. Use the SOURCE name (created_at) to filter the new data
    created_at > (
        -- 3. Use the ALIAS (host_created_at) to find the max in the target table
        SELECT COALESCE(MAX(host_created_at), '1900-01-01') 
        FROM {{ this }}
    )
{% endif %}