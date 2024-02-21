{{
    config(
        materialized='incremental',
        partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "month"
        }
    )
}}

with

green_tripdata AS (
    SELECT *,
    'Green' AS service_type
    FROM {{ ref('stg_green_tripdata') }}
),
yellow_tripdata AS (
    SELECT *,
    'Yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata') }}
),
trips_unioned AS (
    SELECT * FROM yellow_tripdata
    UNION ALL
    SELECT * FROM green_tripdata
),
zones AS (
    SELECT *
    FROM {{ ref('dim_zones') }}
)

SELECT
    trips_unioned.trip_id, 
    trips_unioned.vendor_id, 
    trips_unioned.service_type,
    trips_unioned.ratecode_id, 
    trips_unioned.pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount,
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
FROM
    trips_unioned
    INNER JOIN
    zones AS pickup_zone
    ON
    trips_unioned.pickup_location_id = pickup_zone.location_id
    INNER JOIN
    zones AS dropoff_zone
    ON
    trips_unioned.dropoff_location_id = dropoff_zone.location_id

{% if is_incremental() %}
WHERE pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}