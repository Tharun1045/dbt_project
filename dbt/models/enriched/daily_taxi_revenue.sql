{{ config(
    database='dbt_project',
    schema='enriched',
    materialized='table',
    tags=['taxi_pipeline', 'enriched']
) }}

select
    pickup_date,
    payment_type_name,
    count(*) as trip_count,
    sum(passenger_count) as passenger_count,
    sum(trip_distance) as total_trip_distance,
    sum(fare_amount) as fare_amount,
    sum(tip_amount) as tip_amount,
    sum(tolls_amount) as tolls_amount,
    sum(total_amount) as total_revenue,
    avg(total_amount) as average_trip_amount,
    current_timestamp() as _enriched_processed_at
from {{ source('curated', 'taxi_trips') }}
group by
    pickup_date,
    payment_type_name
