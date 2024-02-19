{{ config(materialized='table') }}

with trips_data as (
    SELECT * FROM {{ ref('fact_trips') }}
)
    SELECT 
    -- Reveneue grouping 
    pickup_zone as revenue_zone,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month, 

    service_type,

    -- Revenue calculation 
    SUM(fare_amount) as revenue_monthly_fare,
    SUM(extra) as revenue_monthly_extra,
    SUM(mta_tax) as revenue_monthly_mta_tax,
    SUM(tip_amount) as revenue_monthly_tip_amount,
    SUM(tolls_amount) as revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    SUM(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    COUNT(trip_id) as total_monthly_trips,
    AVG(passenger_count) as avg_monthly_passenger_count,
    AVG(trip_distance) as avg_monthly_trip_distance

    FROM trips_data
    GROUP BY 1,2,3