{{
    config(
        materialized='incremental',
        unique_key="trip_id"
    )
}}

with
    tripdata as (
        select *,
        row_number() over (partition by dispatching_base_num, pickup_datetime) as rn
        from {{ source("staging", "fhv_tripdata") }}
        where pickup_datetime
        BETWEEN CAST("2019-01-01" AS TIMESTAMP)
            AND CAST("2019-12-31" AS TIMESTAMP)         
        AND dispatching_base_num is not null
    )
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(["dispatching_base_num", "pickup_datetime"]) }} as trip_id,

    -- timestamps
    {{ dbt.safe_cast("pickup_datetime", api.Column.translate_type("timestamp")) }} as pickup_datetime,
    {{ dbt.safe_cast("dropoff_datetime", api.Column.translate_type("timestamp")) }} as dropoff_datetime,
    dispatching_base_num,
    -- trip info
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,
    {{ dbt.safe_cast("SR_flag", api.Column.translate_type("integer")) }} as shared_ride_flag,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("string")) }} as affiliated_base_number
from tripdata
where rn = 1

{% if is_incremental() %}
AND pickup_datetime >= (select max(pickup_datetime) from {{ this }})
{% endif %}

