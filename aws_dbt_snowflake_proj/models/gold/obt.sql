{# --- METADATA CONFIGURATION --- #}
{% set obt_config = [
    {
        "table": ref('silver_bookings'),
        "alias": "b",
        "columns": [
            "booking_id", "booking_date", "listing_id", 
            "total_amount", "booking_status", "created_at"
        ]
    },
    {
        "table": ref('silver_listings'),
        "alias": "l",
        "join_on": "b.listing_id = l.listing_id",
        "columns": [
            "property_type", "room_type", "city", 
            "price_per_night", "price_per_night_flag","listings_created_at"
        ]
    },
    {
        "table": ref('silver_hosts'),
        "alias": "h",
        "join_on": "l.host_id = h.host_id",
        "columns": [
            "host_id","host_name_cleaned", "is_superhost", "response_rate_quality","host_created_at"
        ]
    }
] %}

{# --- DYNAMIC SQL GENERATION --- #}

SELECT
    {% for table_meta in obt_config %}
        {% set outer_loop = loop %}
        {% for col in table_meta.columns %}
            {{ table_meta.alias }}.{{ col }}{% if not (outer_loop.last and loop.last) %}, {% endif %}
        {% endfor %}
    {% endfor %}

FROM {{ obt_config[0].table }} {{ obt_config[0].alias }}

{% for table_meta in obt_config[1:] %}
    LEFT JOIN {{ table_meta.table }} {{ table_meta.alias }} 
        ON {{ table_meta.join_on }}
{% endfor %}