{{
    config(
        materialized = 'table',
        schema = 'marts'
    )
}}

/*
    high_value_customers.sql
    Zones de prise en charge a haute valeur commerciale.

    Criteres : >=10 trajets, >300$ total, pourboire moyen >15%.
    Paiements carte de credit uniquement (pourboires enregistres).
    Resultat attendu : ~70-80 zones sur les 263 zones TLC de NYC.
*/

SELECT
    pickup_location_id,

    -- Volume
    COUNT(*)                                AS trip_count,
    COUNT(DISTINCT pickup_date)             AS active_days,

    -- Finances
    ROUND(SUM(total_amount)::numeric, 2)    AS total_spent,
    ROUND(AVG(fare_amount)::numeric, 2)     AS avg_fare,
    ROUND(AVG(tip_percentage)::numeric, 2)  AS avg_tip_pct,
    ROUND(AVG(total_amount)::numeric, 2)    AS avg_total_per_trip,

    -- Trajet
    ROUND(AVG(trip_distance)::numeric, 2)         AS avg_distance_miles,
    ROUND(AVG(trip_duration_minutes)::numeric, 1) AS avg_duration_min,

    -- Meteo dominante
    MODE() WITHIN GROUP (ORDER BY weather_category) AS dominant_weather

FROM {{ ref('trip_enriched') }}

-- Carte de credit uniquement
WHERE payment_type = 1

GROUP BY pickup_location_id

-- Seuils de qualification haute valeur
HAVING
    COUNT(*) >= 10
    AND SUM(total_amount) > 300
    AND AVG(tip_percentage) > 15

ORDER BY total_spent DESC
