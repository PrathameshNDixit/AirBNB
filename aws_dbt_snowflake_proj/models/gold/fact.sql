{{
  config(
    materialized='table',
    schema='gold'
  )
}}

WITH obt_source AS (
    SELECT * FROM {{ ref('obt') }}
),

dim_listings AS (
    SELECT * FROM {{ ref('dim_listings') }} 
),

dim_hosts AS (
    SELECT * FROM {{ ref('dim_hosts') }}
)

SELECT
    -- 1. Primary Key
    o.booking_id,

    -- 2. Foreign Keys (Validated against Dimensions)
    l.listing_id,
    h.host_id,

    -- 3. Facts / Metrics (Extracted from OBT)
    o.booking_date,
    o.total_amount,
    o.booking_status,
    o.created_at

FROM obt_source o

-- Join to the Listing Dimension
LEFT JOIN dim_listings l 
    ON o.listing_id = l.listing_id

-- Join to the Host Dimension 
LEFT JOIN dim_hosts h 
    ON o.host_id = h.host_id
    -- If your OBT contains the historical booking date, this ensures 
    -- it maps to the correct SCD Type 2 snapshot version
    AND o.created_at >= h.dbt_valid_from 
    AND o.created_at < COALESCE(h.dbt_valid_to, to_date('9999-12-31'))