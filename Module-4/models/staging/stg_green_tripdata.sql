{{ config(materialized="view") }}

with
    tripdata as (
        select *, row_number() over (partition by vendor_id, pickup_datetime) as rn
        from {{ source("staging", "green_tripdata") }}
        where vendor_id is not null
    )
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["vendor_id", "pickup_datetime"]) }} as trip_id,
    {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} as vendor_id,
    {{ dbt.safe_cast("rate_code", api.Column.translate_type("integer")) }} as ratecode_id,
    {{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} as dropoff_location_id,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

    -- payment info
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    imp_surcharge as improvement_surcharge,
    total_amount,
    coalesce(
        {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}, 0
    ) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description
from tripdata
where rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
