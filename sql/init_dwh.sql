-- =============================================================================
-- sql/init_dwh.sql — Initialisation du Data Warehouse NYC Taxi + Météo
-- =============================================================================
--
-- CE FICHIER EST EXÉCUTÉ AUTOMATIQUEMENT par PostgreSQL au premier démarrage
-- du conteneur Docker (monté dans /docker-entrypoint-initdb.d/).
--
-- ORDRE D'EXÉCUTION :
--   1. Création de la base de données nyc_pipeline et de l'utilisateur
--   2. Création des schémas (namespaces qui organisent les tables)
--   3. Tables du schéma 'raw'   → données chargées par PySpark
--   4. Tables du schéma 'marts' → modèles finaux créés par dbt
--   5. Index pour accélérer les requêtes analytiques
--
-- CONCEPT SCHÉMA PostgreSQL :
--   Un schéma est comme un dossier dans la base de données.
--   raw.*    = données semi-transformées (directement de PySpark)
--   marts.*  = données finales agrégées (créées par dbt)
-- =============================================================================


-- =============================================================================
-- 1. BASE DE DONNÉES ET UTILISATEUR
-- =============================================================================

CREATE DATABASE nyc_pipeline
    WITH OWNER = airflow ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';

-- Utilisateur dédié au pipeline (bonne pratique : un user par application)
CREATE USER pipeline WITH PASSWORD 'pipeline';

\c nyc_pipeline


-- =============================================================================
-- 2. SCHÉMAS ET DROITS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;

GRANT ALL PRIVILEGES ON DATABASE nyc_pipeline TO pipeline;
GRANT ALL PRIVILEGES ON SCHEMA raw   TO pipeline;
GRANT ALL PRIVILEGES ON SCHEMA marts TO pipeline;

-- Droits automatiques sur les futures tables créées dans ces schémas
ALTER DEFAULT PRIVILEGES IN SCHEMA raw   GRANT ALL ON TABLES TO pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO pipeline;


-- =============================================================================
-- 3. TABLE raw.fact_taxi_trips — Table de faits (données mesurées)
-- =============================================================================
--
-- CONCEPT TABLE DE FAITS :
--   Contient les ÉVÉNEMENTS mesurables. Chaque ligne = un trajet taxi.
--   Les colonnes sont des métriques numériques (distance, tarif, durée).
--
-- TYPES DE DONNÉES CHOISIS :
--   BIGSERIAL        → entier auto-incrémenté (clé primaire)
--   TIMESTAMP        → date + heure sans timezone (toutes nos données sont NYC)
--   DOUBLE PRECISION → nombre décimal 64 bits (tarifs, distances)
--   INTEGER          → entier 32 bits (heure, nb passagers, zones)
--   VARCHAR(n)       → texte de longueur max n (catégories, descriptions)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.fact_taxi_trips (

    -- Identifiant unique auto-généré
    trip_id                 BIGSERIAL        PRIMARY KEY,

    -- Horodatages du trajet
    pickup_datetime         TIMESTAMP        NOT NULL,
    dropoff_datetime        TIMESTAMP        NOT NULL,
    trip_duration_minutes   DOUBLE PRECISION,

    -- Distance
    trip_distance           DOUBLE PRECISION,
    -- Tranche calculée par PySpark : '0-2km', '2-5km', '>5km'
    distance_bucket         VARCHAR(10),

    -- Montants financiers
    fare_amount             DOUBLE PRECISION,  -- Tarif de base
    extra                   DOUBLE PRECISION,  -- Suppléments
    mta_tax                 DOUBLE PRECISION,  -- Taxe MTA
    tip_amount              DOUBLE PRECISION,  -- Pourboire en $
    tolls_amount            DOUBLE PRECISION,  -- Péages
    total_amount            DOUBLE PRECISION,  -- Total à payer
    tip_percentage          DOUBLE PRECISION,  -- Pourboire en % du tarif

    -- Informations passager et paiement
    passenger_count         INTEGER,
    payment_type            INTEGER,           -- Code : 1=CB, 2=Cash, 3=Gratuit
    payment_type_desc       VARCHAR(20),       -- Description lisible

    -- Zones NYC (identifiants TLC de 1 à 263)
    pickup_location_id      INTEGER,
    dropoff_location_id     INTEGER,

    -- Colonnes temporelles dérivées (pré-calculées par PySpark pour accélérer
    -- les agrégations — évite de recalculer EXTRACT(HOUR FROM ...) à chaque fois)
    pickup_hour             INTEGER,           -- 0 à 23
    pickup_day_of_week      INTEGER,           -- 1=Lundi, 7=Dimanche
    pickup_date             DATE,
    pickup_month            INTEGER,           -- 1 à 12

    -- Traçabilité pipeline
    ingested_at             TIMESTAMP DEFAULT NOW(),
    source_file             VARCHAR(255)

);

COMMENT ON TABLE raw.fact_taxi_trips IS
    'Trajets Yellow Taxi NYC Jan-Mars 2024. '
    'Source : TLC. Transformé par PySpark depuis MinIO.';


-- =============================================================================
-- 4. TABLE raw.dim_weather — Table de dimensions (contexte météo)
-- =============================================================================
--
-- CONCEPT TABLE DE DIMENSIONS :
--   Contient le CONTEXTE qui qualifie les faits.
--   Ici : la météo à chaque heure.
--
-- CLÉ DE JOINTURE avec fact_taxi_trips :
--   DATE_TRUNC('hour', pickup_datetime) = observation_hour
--   → Un trajet à 14h37 reçoit la météo de 14h00
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.dim_weather (

    -- Horodatages (pas de SERIAL car PySpark réécrit la table complètement)
    observation_ts          TIMESTAMP        NOT NULL,
    observation_hour        TIMESTAMP        NOT NULL,  -- Clé de jointure (tronquée à l'heure)

    -- Température
    temp_celsius            DOUBLE PRECISION,
    temp_feels_like         DOUBLE PRECISION,

    -- Conditions atmosphériques
    humidity_pct            INTEGER,
    wind_speed_ms           DOUBLE PRECISION,
    wind_direction_deg      INTEGER,
    pressure_hpa            INTEGER,
    visibility_m            INTEGER,

    -- Description météo
    weather_main            VARCHAR(50),     -- Catégorie OWM : 'Rain', 'Clear', 'Snow'...
    weather_description     VARCHAR(100),    -- Détail : 'light rain', 'overcast clouds'...
    -- Notre catégorie simplifiée (calculée par PySpark) :
    -- 'Clair', 'Pluvieux', 'Orageux', 'Autre'
    weather_category        VARCHAR(20),

    -- Colonnes temporelles dérivées
    obs_hour                INTEGER,
    obs_day_of_week         INTEGER,
    obs_date                DATE,

    -- Traçabilité pipeline
    ingested_at             TIMESTAMP DEFAULT NOW(),
    source_file             VARCHAR(255)

);

COMMENT ON TABLE raw.dim_weather IS
    'Observations météo NYC toutes les heures, Jan-Mars 2024. '
    'Source : API OpenWeatherMap. Chargé par PySpark Streaming.';


-- =============================================================================
-- 5. TABLES MARTS — Modèles dbt (définies ici, remplies par dbt run)
-- =============================================================================
--
-- Ces tables sont vides au démarrage.
-- dbt les remplira avec CREATE TABLE AS SELECT lors de "dbt run".
-- On les définit ici uniquement pour les droits et la documentation.
-- =============================================================================

-- Jointure complète taxi + météo (8,5M lignes)
CREATE TABLE IF NOT EXISTS marts.trip_enriched (
    trip_id                 BIGINT,
    pickup_datetime         TIMESTAMP,
    trip_distance           DOUBLE PRECISION,
    trip_duration_minutes   DOUBLE PRECISION,
    fare_amount             DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tip_percentage          DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    passenger_count         INTEGER,
    payment_type            INTEGER,
    payment_type_desc       VARCHAR(20),
    distance_bucket         VARCHAR(10),
    pickup_location_id      INTEGER,
    pickup_hour             INTEGER,
    pickup_date             DATE,
    -- Colonnes météo jointes
    temp_celsius            DOUBLE PRECISION,
    weather_category        VARCHAR(20),
    weather_main            VARCHAR(50),
    wind_speed_ms           DOUBLE PRECISION,
    humidity_pct            INTEGER
);

COMMENT ON TABLE marts.trip_enriched IS
    'Table principale enrichie : chaque trajet avec sa météo. '
    'Créée par le modèle dbt trip_enriched.sql.';

-- Agrégation par heure (pour les analyses temporelles)
CREATE TABLE IF NOT EXISTS marts.trip_summary_per_hour (
    pickup_hour             INTEGER,
    pickup_date             DATE,
    weather_category        VARCHAR(20),
    trip_count              INTEGER,
    avg_duration_min        DOUBLE PRECISION,
    avg_tip_pct             DOUBLE PRECISION,
    avg_fare                DOUBLE PRECISION,
    total_revenue           DOUBLE PRECISION
);

COMMENT ON TABLE marts.trip_summary_per_hour IS
    'Agrégation horaire : nombre de trajets, durée, pourboire, revenu. '
    'Créée par dbt.';

-- Zones à haute valeur (pour identifier les zones premium)
CREATE TABLE IF NOT EXISTS marts.high_value_customers (
    pickup_location_id      INTEGER,
    trip_count              INTEGER,
    total_spent             DOUBLE PRECISION,
    avg_tip_pct             DOUBLE PRECISION
);

COMMENT ON TABLE marts.high_value_customers IS
    'Zones de départ premium : >10 trajets, >300$, pourboire >15%. '
    'Créée par dbt.';


-- =============================================================================
-- 6. INDEX — Accélération des requêtes analytiques
-- =============================================================================
--
-- CONCEPT INDEX :
--   Sans index : PostgreSQL lit TOUTES les 8,5M lignes pour filtrer.
--   Avec index  : accès direct aux lignes correspondantes.
--
-- TYPES UTILISÉS :
--   BTREE (défaut) : parfait pour =, <, >, BETWEEN, ORDER BY
--   BRIN           : très compact pour les colonnes corrélées avec
--                    l'ordre d'insertion (ex: dates insérées chronologiquement)
--   UNIQUE         : garantit l'unicité ET accélère les recherches
-- =============================================================================

-- Filtre et jointures sur la date/heure (requête la plus fréquente)
CREATE INDEX IF NOT EXISTS idx_taxi_pickup_datetime
    ON raw.fact_taxi_trips (pickup_datetime);

-- BRIN sur pickup_datetime : données insérées en ordre chronologique →
-- BRIN est 100x plus compact qu'un BTREE avec des performances similaires
CREATE INDEX IF NOT EXISTS idx_taxi_pickup_brin
    ON raw.fact_taxi_trips USING BRIN (pickup_datetime);

-- Agrégations par heure (GROUP BY pickup_hour très courant)
CREATE INDEX IF NOT EXISTS idx_taxi_pickup_hour
    ON raw.fact_taxi_trips (pickup_hour);

-- Jointures pour high_value_customers
CREATE INDEX IF NOT EXISTS idx_taxi_location
    ON raw.fact_taxi_trips (pickup_location_id);

-- Clé de jointure principale taxi ↔ météo
CREATE INDEX IF NOT EXISTS idx_weather_observation_hour
    ON raw.dim_weather (observation_hour);

-- UNIQUE : une seule observation météo par heure (contrainte de qualité)
CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique_hour
    ON raw.dim_weather (observation_hour);


-- =============================================================================
-- Message de confirmation (visible dans les logs Docker)
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '================================================';
    RAISE NOTICE '  Base nyc_pipeline initialisee avec succes !';
    RAISE NOTICE '  Schemas  : raw, marts';
    RAISE NOTICE '  Tables   : fact_taxi_trips, dim_weather';
    RAISE NOTICE '             trip_enriched, trip_summary_per_hour';
    RAISE NOTICE '             high_value_customers';
    RAISE NOTICE '  Index    : 6 (BTREE + BRIN + UNIQUE)';
    RAISE NOTICE '================================================';
    RAISE NOTICE '';
END $$;
