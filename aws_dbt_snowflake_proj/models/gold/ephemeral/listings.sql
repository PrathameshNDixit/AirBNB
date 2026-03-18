{{
  config(
    materialized='ephemeral'
  )
}}

WITH listings AS (
    SELECT
            property_type, room_type, city, 
            price_per_night, price_per_night_flag,listings_created_at
    FROM {{ ref('obt') }}
)

SELECT * FROM listings