{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
    trip_summary_per_hour.sql
    Agrégation horaire des trajets avec contexte météo.

    Agrège les 8,4M trajets en ~2 000 lignes (une par heure/date/météo).
    Utilisé pour les analyses temporelles et graphiques.
*/

SELECT
    -- Dimensions de groupement
    pickup_hour,
    pickup_date,
    weather_category,

    -- Volume
    COUNT(*)                                                AS trip_count,

    -- Durée
    ROUND(AVG(trip_duration_minutes)::numeric, 1)          AS avg_duration_min,
    ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY trip_duration_minutes)::numeric, 1
    )                                                       AS median_duration_min,

    -- Finances
    ROUND(AVG(tip_percentage)::numeric, 2)                 AS avg_tip_pct,
    ROUND(AVG(fare_amount)::numeric, 2)                    AS avg_fare,
    ROUND(SUM(total_amount)::numeric, 2)                   AS total_revenue,

    -- Distance
    ROUND(AVG(trip_distance)::numeric, 2)                  AS avg_distance_miles,

    -- Météo agrégée
    ROUND(AVG(temp_celsius)::numeric, 1)                   AS avg_temp_celsius,
    ROUND(AVG(wind_speed_ms)::numeric, 1)                  AS avg_wind_speed_ms,
    ROUND(AVG(humidity_pct)::numeric, 0)                   AS avg_humidity_pct

FROM {{ ref('trip_enriched') }}

WHERE weather_category != 'Inconnu'

GROUP BY
    pickup_hour,
    pickup_date,
    weather_category

ORDER BY
    pickup_date,
    pickup_hour
