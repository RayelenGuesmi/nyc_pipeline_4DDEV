{{
    config(
        materialized = 'view',
        schema = 'raw'
    )
}}

/*
    stg_taxi_trips.sql
    Vue de staging sur les trajets taxi bruts.

    NOTE : trip_id n'existe pas dans la table recree par PySpark (mode overwrite
    supprime la sequence BIGSERIAL). On genere un identifiant de substitution
    avec ROW_NUMBER() pour les modeles aval qui en ont besoin.
*/

SELECT
    -- Identifiant genere (PySpark overwrite supprime le BIGSERIAL original)
    ROW_NUMBER() OVER (ORDER BY pickup_datetime, pickup_location_id) AS trip_id,

    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    trip_distance,
    distance_bucket,
    fare_amount,
    tip_amount,
    tip_percentage,
    total_amount,
    extra,
    mta_tax,
    tolls_amount,
    passenger_count,
    payment_type,
    payment_type_desc,
    pickup_location_id,
    dropoff_location_id,
    pickup_hour,
    pickup_day_of_week,
    pickup_date,
    pickup_month,
    ingested_at,
    source_file

FROM {{ source('raw', 'fact_taxi_trips') }}
