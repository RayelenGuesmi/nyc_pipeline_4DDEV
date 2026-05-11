{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
    trip_enriched.sql
    Table enrichie : chaque trajet taxi avec sa météo correspondante.

    LOGIQUE DE JOINTURE :
      Un trajet à 14h37 reçoit la météo de 14h00.
      LEFT JOIN pour conserver tous les trajets, même sans météo.
      Résultat attendu : ~8,4 millions de lignes.
*/

SELECT
    -- Données taxi
    t.trip_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.trip_duration_minutes,
    t.trip_distance,
    t.distance_bucket,
    t.fare_amount,
    t.tip_amount,
    t.tip_percentage,
    t.total_amount,
    t.passenger_count,
    t.payment_type,
    t.payment_type_desc,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.pickup_hour,
    t.pickup_day_of_week,
    t.pickup_date,
    t.pickup_month,

    -- Données météo jointes
    -- COALESCE : retourne 'Inconnu' si pas de données météo pour cette heure
    w.temp_celsius,
    w.temp_feels_like,
    w.humidity_pct,
    w.wind_speed_ms,
    w.wind_speed_kmh,
    w.weather_main,
    w.weather_description,
    COALESCE(w.weather_category, 'Inconnu') AS weather_category

FROM {{ ref('stg_taxi_trips') }} t

-- Jointure sur l'heure tronquée : chaque trajet reçoit la météo de son heure
LEFT JOIN {{ ref('stg_weather') }} w
    ON DATE_TRUNC('hour', t.pickup_datetime) = w.observation_hour
