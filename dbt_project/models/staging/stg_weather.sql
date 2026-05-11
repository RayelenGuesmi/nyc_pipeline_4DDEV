{{
    config(
        materialized = 'view',
        schema = 'raw'
    )
}}

/*
    stg_weather.sql
    Vue de staging sur les observations meteo brutes.

    CAST ::numeric requis : PostgreSQL n'accepte pas ROUND(double precision, n).
    Il faut d'abord convertir en numeric : ROUND(valeur::numeric, decimales).
*/

SELECT
    observation_ts,
    observation_hour,
    temp_celsius,
    temp_feels_like,
    humidity_pct,
    wind_speed_ms,
    ROUND((wind_speed_ms * 3.6)::numeric, 1)  AS wind_speed_kmh,
    wind_direction_deg,
    pressure_hpa,
    visibility_m,
    weather_main,
    weather_description,
    weather_category,
    obs_hour,
    obs_day_of_week,
    obs_date,
    ingested_at

FROM {{ source('raw', 'dim_weather') }}

WHERE temp_celsius IS NOT NULL
