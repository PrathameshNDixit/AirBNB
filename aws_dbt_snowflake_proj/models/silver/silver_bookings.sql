{{
  config(
    materialized = 'incremental',
    unique_key = 'booking_id',
    )
}}
select booking_id,listing_id,booking_date,nights_booked,booking_amount,cleaning_fee , service_fee,
{{multiply('nights_booked','booking_amount',2)}} + cleaning_fee + service_fee as Total_Amount,
Booking_status,
created_at
from {{ ref('bronze_bookings') }}