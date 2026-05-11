# NYC Yellow Taxi + Météo — Pipeline de données modulaire

> **Projet Data Engineering** — Pipeline de bout en bout pour analyser l'impact de la météo sur les trajets Yellow Taxi de New York City .

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Architecture](#2-architecture)
3. [Stack technique](#3-stack-technique)
4. [Structure du projet](#4-structure-du-projet)
5. [Infrastructure Docker](#5-infrastructure-docker)
6. [Données](#6-données)
7. [Installation et démarrage](#7-installation-et-démarrage)
8. [Partie 1 — Ingestion des données](#8-partie-1--ingestion-des-données)
9. [Partie 2 — Transformation PySpark](#9-partie-2--transformation-pyspark)
10. [Partie 3 — Modélisation dbt](#10-partie-3--modélisation-dbt)
11. [Orchestration Airflow](#11-orchestration-airflow)
12. [Schéma de données](#12-schéma-de-données)
13. [Notebook d'analyse](#13-notebook-danalyse)
14. [Résultats et insights](#14-résultats-et-insights)
15. [Choix techniques justifiés](#15-choix-techniques-justifiés)

---

## 1. Vue d'ensemble

Ce projet implémente un **pipeline de données modulaire et évolutif** capable d'ingérer, stocker, transformer et modéliser deux sources de données complémentaires :

- **Données Yellow Taxi NYC** : 9,5 millions de trajets enregistrés par la TLC de janvier à mars 2024, au format Parquet.
- **Données météorologiques** : 2 184 observations horaires simulées via l'API OpenWeatherMap pour la même période, au format JSON.

### Objectif métier

Répondre à 9 questions analytiques sur la mobilité urbaine à New York :
- Comment la météo influence-t-elle la demande de taxis ?
- Les conditions climatiques affectent-elles les pourboires ?
- Quelles zones et quelles heures génèrent le plus de valeur ?

### Flux de données simplifié

```
TLC Website          OpenWeatherMap API
     |                      |
     v                      v
[ingest_taxi.py]    [ingest_weather.py]
     |                      |
     v                      v
  MinIO Data Lake (nyc-datalake)
  raw/taxi/           raw/weather/
     |                      |
     v                      v
[PySpark Batch]    [PySpark Streaming]
transform_taxi.py  transform_weather.py
     |                      |
     v                      v
       PostgreSQL DWH
    raw.fact_taxi_trips
    raw.dim_weather
              |
              v
           [dbt]
    marts.trip_enriched
    marts.trip_summary_per_hour
    marts.high_value_customers
              |
              v
    Notebook d'analyse
    (9 questions / 9 graphiques)
```

---

## 2. Architecture

```
SOURCES
  TLC Yellow Taxi (Parquet, mensuel)
  OpenWeatherMap API (JSON, horaire simule)
        |
        v
INGESTION — Python
  ingest_taxi.py     : telecharge, valide (MD5), uploade vers MinIO
  ingest_weather.py  : genere JSON simules realistes, uploade vers MinIO
  Orchestre par les DAGs Airflow
        |
        v
DATA LAKE — MinIO (S3-compatible)
  nyc-datalake/raw/taxi/2024/MM/   → 3 fichiers Parquet (~153 Mo)
  nyc-datalake/raw/weather/2024/   → 2184 fichiers JSON
        |
        v
TRANSFORMATION — Apache Spark 3.5.1
  transform_taxi.py    : PySpark Batch    9.5M → 8.4M lignes
  transform_weather.py : PySpark Streaming  2184 observations
        |
        v
DATA WAREHOUSE — PostgreSQL 15
  raw.fact_taxi_trips  (8 441 641 lignes)
  raw.dim_weather      (2 184 observations)
        |
        v
MODELISATION — dbt 1.8.2
  Staging : stg_taxi_trips, stg_weather (vues)
  Marts   : trip_enriched, trip_summary_per_hour, high_value_customers (tables)
  Tests   : 25/25 PASS
        |
        v
ANALYSE — Jupyter Notebook
  9 questions analytiques | 9 graphiques | Insights metier
```

---

## 3. Stack technique

| Outil | Version | Role |
|-------|---------|------|
| Python | 3.12 | Scripts d'ingestion, DAGs Airflow, notebook |
| Apache Spark | 3.5.1 | Transformation batch (taxi) et streaming simule (meteo) |
| Apache Airflow | 2.8.1 | Orchestration — planification et enchainement des taches |
| MinIO | latest | Data Lake local compatible API Amazon S3 |
| PostgreSQL | 15 | Data Warehouse — stockage structure pour l'analyse |
| dbt-postgres | 1.8.2 | Modelisation SQL — jointures, agregations, tests qualite |
| Docker Compose | v2 | Infrastructure conteneurisee reproductible |
| pandas | 2.x | Manipulation des donnees dans le notebook |
| matplotlib / seaborn | — | Visualisations et graphiques analytiques |
| boto3 | — | SDK Python pour interagir avec MinIO (API S3) |
| psycopg2 | — | Driver PostgreSQL pour Python |
| pyarrow | — | Lecture et validation des fichiers Parquet |

---

## 4. Structure du projet

```
nyc_pipeline/
|
+-- .env                        Variables d'environnement (credentials, ports)
+-- .gitignore                  Fichiers a exclure du versioning
+-- docker-compose.yml          Definition des 6 services Docker
+-- README.md                   Ce fichier
|
+-- ingestion/
|   +-- ingest_taxi.py          Telecharge Parquet TLC -> valide -> MinIO
|   +-- ingest_weather.py       Genere JSON meteo simules -> MinIO
|
+-- spark/
|   +-- transform_taxi.py       PySpark : lit Parquet -> nettoie -> PostgreSQL
|
+-- streaming/
|   +-- transform_weather.py    PySpark : lit JSON -> extrait -> PostgreSQL
|
+-- airflow/
|   +-- dags/
|       +-- dag_taxi_pipeline.py     DAG taxi : ingest -> transform -> verify
|       +-- dag_weather_pipeline.py  DAG meteo : ingest -> transform -> verify
|
+-- dbt_project/
|   +-- dbt_project.yml         Configuration du projet dbt
|   +-- profiles.yml            Connexion PostgreSQL
|   +-- models/
|       +-- staging/
|       |   +-- sources.yml          Declaration des tables sources + tests
|       |   +-- stg_taxi_trips.sql   Vue sur fact_taxi_trips
|       |   +-- stg_weather.sql      Vue sur dim_weather
|       +-- marts/
|           +-- schema.yml           Documentation + tests des modeles
|           +-- trip_enriched.sql            Jointure taxi + meteo (8.4M lignes)
|           +-- trip_summary_per_hour.sql    Agregation horaire (2183 lignes)
|           +-- high_value_customers.sql     Zones premium (90 zones)
|
+-- sql/
|   +-- init_dwh.sql            DDL PostgreSQL : schemas, tables, index
|
+-- config/
|   +-- settings.py             Configuration centralisee (dataclasses Python)
|   +-- __init__.py
|
+-- docker/
|   +-- webserver_start.sh      Init DB + creation user + lancement webserver
|   +-- scheduler_start.sh      Attente 90s + lancement scheduler
|
+-- notebooks/
    +-- analysis.ipynb          Notebook d'analyse (9 questions)
    +-- q1_distribution_durees.png
    +-- q2_pourboire_distance.png
    +-- q3_heures_chargees.png
    +-- q4_correlation.png
    +-- q5_temperature_pics.png
    +-- q6_impact_meteo.png
    +-- q7_comportements_meteo.png
    +-- q8_clients_premium.png
    +-- q9_meteo_pourboire.png
```

---

## 5. Infrastructure Docker

Le fichier `docker-compose.yml` orchestre **6 services** dans un reseau Docker isole.

### Services et ports

| Service | Conteneur | Port | Interface |
|---------|-----------|------|-----------|
| PostgreSQL | nyc_postgres | 5435 | — |
| MinIO API S3 | nyc_minio | 9020 | — |
| MinIO Console | nyc_minio | 9021 | http://localhost:9021 |
| Spark Master | nyc_spark_master | 8083 | http://localhost:8083 |
| Spark Worker | nyc_spark_worker | — | — |
| Airflow Webserver | nyc_airflow_webserver | 8084 | http://localhost:8084 |
| Airflow Scheduler | nyc_airflow_scheduler | — | — |

### Role de chaque service

**PostgreSQL** joue un double role :
- Base `airflow` : metadonnees Airflow (DAGs, runs, variables, logs)
- Base `nyc_pipeline` : Data Warehouse du pipeline (schemas `raw` et `marts`)

**MinIO** est notre Data Lake local. Il implemente l'API Amazon S3, ce qui permet d'utiliser boto3 et Spark S3A exactement comme sur AWS. Changement de MinIO vers S3 = juste changer les variables d'environnement.

**Apache Spark** fonctionne en mode master/worker. Le Master coordonne, le Worker execute les calculs. Mode `local[*]` : utilise tous les CPUs disponibles.

**Apache Airflow** orchestre l'ensemble. Le Webserver expose l'UI et initialise la base au demarrage. Le Scheduler surveille les DAGs en permanence.

### Credentials par defaut

```
MinIO      : minioadmin / minioadmin
PostgreSQL : pipeline / pipeline  (base nyc_pipeline)
             airflow / airflow    (base airflow)
Airflow UI : admin / admin
```

---

## 6. Données

### Source 1 : Yellow Taxi NYC

| Attribut | Valeur |
|----------|--------|
| Fournisseur | TLC — Taxi & Limousine Commission of NYC |
| URL | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| Format | Parquet (colonnes compressees, 10x plus compact que CSV) |
| Periode | Janvier, Fevrier, Mars 2024 |
| Taille brute | ~153 Mo (3 fichiers) |
| Lignes brutes | 9 554 778 trajets |
| Lignes apres nettoyage | 8 441 641 trajets valides (11.5% filtres) |

**Colonnes principales utilisees :**

| Colonne TLC | Description | Transformation |
|-------------|-------------|----------------|
| tpep_pickup_datetime | Heure de prise en charge | Renomme en pickup_datetime |
| tpep_dropoff_datetime | Heure de depose | Renomme en dropoff_datetime |
| trip_distance | Distance en miles | Base pour distance_bucket |
| fare_amount | Tarif de base ($) | Conserve |
| tip_amount | Pourboire ($) | Base pour tip_percentage |
| total_amount | Total ($) | Conserve |
| passenger_count | Nombre de passagers | Filtre 1-6 |
| payment_type | Code paiement (1=CB, 2=Cash...) | + payment_type_desc |
| PULocationID | Zone de depart (1-263) | Renomme en pickup_location_id |
| DOLocationID | Zone d'arrivee (1-263) | Renomme en dropoff_location_id |

**Criteres de filtrage (11.5% rejetes) :**
- Distance : entre 0.1 et 100 miles
- Tarif : entre 2.50$ et 500$
- Duree calculee : entre 1 et 180 minutes
- Passagers : entre 1 et 6
- Periode : exclusivement janvier-mars 2024
- Coherence temporelle : dropoff > pickup

### Source 2 : Meteo OpenWeatherMap

| Attribut | Valeur |
|----------|--------|
| Fournisseur | OpenWeatherMap |
| URL | https://openweathermap.org/api |
| Format | JSON (un fichier par heure) |
| Periode | Janvier, Fevrier, Mars 2024 (simule) |
| Granularite | 1 observation par heure |
| Nombre de fichiers | 2 184 (91 jours x 24 heures) |

**Pourquoi des donnees simulees ?**
L'API OWM gratuite donne uniquement la meteo en temps reel. Les donnees historiques sont payantes (abonnement). Pour ce projet pedagogique, on simule des donnees realistes basees sur les normales climatiques NYC en hiver :

| Mois | Temp. moyenne | Ecart-type | Prob. precipitations |
|------|---------------|------------|----------------------|
| Janvier | 0.5°C | 4.0°C | 35% |
| Fevrier | 2.0°C | 4.5°C | 30% |
| Mars | 7.0°C | 5.0°C | 35% |

La simulation ajoute un cycle diurne realiste : plus froid la nuit (2-5h), plus chaud l'apres-midi (14-16h). La graine aleatoire fixe (`random.seed(42)`) garantit la reproducibilite.

**Categories meteo creees :**
- `Clair` : conditions Clear et Clouds (ciel couvert sans pluie)
- `Pluvieux` : Rain, Drizzle, Snow
- `Orageux` : Thunderstorm
- `Nuageux` : Clouds epais
- `Autre` : cas non classifies

---

## 7. Installation et demarrage

### Prerequis

- Docker Desktop installe et demarre (Windows/Mac/Linux)
- Python 3.12+
- 8 Go de RAM recommandes (Spark + PostgreSQL + Airflow en parallele)
- 2 Go d'espace disque libre

### Etape 1 — Cloner et installer les dependances

```bash
git clone <url-du-repo>
cd nyc_pipeline/

pip install boto3 requests pyarrow psycopg2-binary dbt-postgres \
            pandas matplotlib seaborn jupyter
```

### Etape 2 — Configurer le fichier .env

Editer `.env` et renseigner la cle API OpenWeatherMap (optionnel — le script fonctionne en mode simulation sans cle) :

```
OPENWEATHER_API_KEY=votre_cle_api_ici
```

### Etape 3 — Lancer l'infrastructure

```bash
docker-compose up -d

# Verifier que tous les services sont up
docker-compose ps
```

Attendre environ 2 minutes. Etat attendu :
```
nyc_postgres           healthy
nyc_minio              healthy
nyc_spark_master       running
nyc_spark_worker       running
nyc_airflow_webserver  healthy
nyc_airflow_scheduler  running
```

### Etape 4 — Verifier la base de donnees

```bash
docker exec nyc_postgres psql -U pipeline -d nyc_pipeline -c "\dt raw.*"
```

Resultat attendu :
```
 Schema |      Name       | Type
--------+-----------------+-------
 raw    | dim_weather     | table
 raw    | fact_taxi_trips | table
```

---

## 8. Partie 1 — Ingestion des donnees

### 8.1 Ingestion Taxi — `ingest_taxi.py`

3 etapes pour chaque mois (janvier, fevrier, mars) :

**1. Telechargement par chunks**
Le fichier (~50 Mo) est telecharge par morceaux de 8 Mo pour ne pas saturer la RAM. Timeout de 300 secondes.

**2. Validation Parquet**
- Lecture du schema integre (rapide, stocke en tete de fichier)
- Verification des 6 colonnes obligatoires
- Verification > 0 lignes
- Calcul hash MD5 pour l'integrite

**3. Upload vers MinIO**
```
Destination : s3://nyc-datalake/raw/taxi/2024/MM/yellow_tripdata_2024-MM.parquet
```
Verification de la taille apres upload pour detecter toute corruption.

**Lancement :**
```bash
python ingestion/ingest_taxi.py
```

**Sortie attendue :**
```
INGESTION YELLOW TAXI NYC
Mois 01 : telechargement 47.6 Mo | validation OK 2964624 lignes | upload OK
Mois 02 : telechargement 48.0 Mo | validation OK 3007526 lignes | upload OK
Mois 03 : telechargement 57.3 Mo | validation OK 3582628 lignes | upload OK
Resultat : 3/3 mois ingeres | 152.9 Mo total
```

### 8.2 Ingestion Meteo — `ingest_weather.py`

Genere **2 184 fichiers JSON** (1 par heure du 1er janvier au 31 mars 2024) avec la structure exacte de l'API OpenWeatherMap reelle.

**Organisation dans MinIO :**
```
raw/weather/2024/
  01/01/weather_20240101_0000.json
  01/01/weather_20240101_0100.json
  ...
  03/31/weather_20240331_2300.json
```

**Lancement :**
```bash
python ingestion/ingest_weather.py
```

**Sortie attendue :**
```
INGESTION METEO NYC Jan-Mars 2024
Fichiers uploades : 2184 | Erreurs : 0
Clair   : 798 (36.5%) | Nuageux : 665 (30.4%)
Pluvieux: 645 (29.5%) | Orageux :  76 (3.5%)
```

---

## 9. Partie 2 — Transformation PySpark

Les scripts s'executent **dans le conteneur Docker Spark** via spark-submit.

### Prerequis : copier les scripts dans le conteneur

```bash
docker cp spark/transform_taxi.py nyc_spark_master:/opt/spark/work-dir/
docker cp streaming/transform_weather.py nyc_spark_master:/opt/spark/work-dir/
```

### Variable packages (raccourci)

```bash
PKGS="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.592,org.postgresql:postgresql:42.7.1"
```

### 9.1 Transformation Taxi Batch — `transform_taxi.py`

**Pipeline en 5 etapes (toutes LAZY, executees en 1 seul parcours) :**

```
1. Lecture     : spark.read.parquet("s3a://nyc-datalake/raw/taxi/2024/*/*.parquet")
2. Nettoyage   : filter(distance entre 0.1 et 100 miles, tarif entre 2.5 et 500$...)
3. Enrichissement : withColumn("trip_duration_minutes", (dropoff - pickup) / 60)
                    withColumn("tip_percentage", tip_amount / fare_amount * 100)
                    withColumn("distance_bucket", CASE WHEN distance*1.6 < 2 THEN...)
4. Temporel    : withColumn("pickup_hour", hour(pickup_datetime))
                 withColumn("pickup_day_of_week", dayofweek(pickup_datetime))
5. Ecriture    : df.write.mode("overwrite").jdbc(url, "raw.fact_taxi_trips", ...)
```

**Lancement :**
```bash
docker exec -u root nyc_spark_master \
    /opt/spark/bin/spark-submit \
    --master local[*] --packages "$PKGS" \
    /opt/spark/work-dir/transform_taxi.py
```

**Resultats :**
```
Donnees brutes    : 9 554 778 lignes
Apres nettoyage   : 8 441 641 lignes (-11.5%)
Lignes ecrites    : 8 441 641 dans raw.fact_taxi_trips
Duree totale      : 97 secondes | Debit : 87 235 lignes/seconde
```

### 9.2 Transformation Meteo Streaming — `transform_weather.py`

**Simulation du streaming :** lecture mois par mois avec union des DataFrames.

**Defi technique — JSON imbrique :**
```python
# Objet imbrique : main.temp
F.col("main.temp").cast("float")

# Tableau : premier element du tableau weather
F.col("weather").getItem(0).getField("main")

# Cle de jointure creee : tronquer a l'heure
F.date_trunc("hour", F.col("observation_ts"))  # → observation_hour
```

**Lancement :**
```bash
docker exec -u root nyc_spark_master \
    /opt/spark/bin/spark-submit \
    --master local[*] --packages "$PKGS" \
    /opt/spark/work-dir/transform_weather.py
```

**Resultats :**
```
Fichiers JSON lus  : 2184 (744 + 696 + 744 par mois)
Deduplication      : 2184 → 2184 (0 doublons)
Temp. moyenne      : 3.1°C | Min : -14.1°C | Max : 21.4°C
Observations ecrites : 2184 dans raw.dim_weather
Duree totale        : 79 secondes
```

---

## 10. Partie 3 — Modelisation dbt

### Architecture en 2 couches

**Staging (vues)** — couche d'interface stable :
- `stg_taxi_trips` : expose `raw.fact_taxi_trips` avec noms propres
- `stg_weather` : expose `raw.dim_weather` + conversion m/s → km/h

**Marts (tables physiques)** — couche metier :
- `trip_enriched` : jointure taxi + meteo, 8.4M lignes
- `trip_summary_per_hour` : agregation horaire, 2183 lignes
- `high_value_customers` : zones premium NYC, 90 zones

### Logique de jointure dans trip_enriched

```sql
-- Chaque trajet recoit la meteo de son heure de depart
-- Un trajet a 14h37 → meteo de 14h00
LEFT JOIN stg_weather w
    ON DATE_TRUNC('hour', t.pickup_datetime) = w.observation_hour
-- LEFT JOIN : conserve les trajets meme sans donnee meteo
```

### Criteres high_value_customers

```sql
HAVING
    COUNT(*) >= 10              -- Au moins 10 trajets (representativite)
    AND SUM(total_amount) > 300 -- Plus de 300$ depenses
    AND AVG(tip_percentage) > 15 -- Pourboire moyen > 15%
WHERE payment_type = 1          -- Carte credit uniquement
```

### Execution dbt (Windows)

```powershell
cd dbt_project\
$env:PYTHONUTF8=1

# Test de connexion
python C:\...\dbt\cli\main.py debug --profiles-dir .

# Execution des modeles
python C:\...\dbt\cli\main.py run --profiles-dir .

# Tests de qualite
python C:\...\dbt\cli\main.py test --profiles-dir .
```

**Resultats :**
```
dbt run  : PASS=5  WARN=0  ERROR=0  (76 secondes)
dbt test : PASS=25 WARN=0  ERROR=0  (26 secondes)
```

### Tests de qualite (25 tests)

| Test | Colonne | Modele |
|------|---------|--------|
| unique + not_null | trip_id | trip_enriched |
| not_null | pickup_datetime, tip_percentage, weather_category, distance_bucket | trip_enriched |
| accepted_values | weather_category (5 valeurs) | trip_enriched |
| accepted_values | distance_bucket (3 valeurs) | trip_enriched |
| not_null | pickup_hour, trip_count, avg_tip_pct | trip_summary_per_hour |
| unique + not_null | pickup_location_id, total_spent, trip_count | high_value_customers |
| Tests sources | observation_hour, weather_category, pickup_datetime... | sources |

---

## 11. Orchestration Airflow

### Interface graphique

Accessible sur **http://localhost:8084** (admin / admin)

L'interface montre les 2 DAGs avec leurs tags, leur planning et l'historique des executions.

### DAG 1 : nyc_taxi_pipeline

```
ingest_taxi_data  →  transform_taxi_spark  →  verify_taxi_data
  PythonOperator      BashOperator (spark-submit)  PythonOperator
  Telecharge TLC      Lit Parquet → PostgreSQL       Verifie > 1M lignes
  → MinIO
```

| Parametre | Valeur |
|-----------|--------|
| Schedule | @monthly (1er du mois) |
| Retries | 2 tentatives, 5 min d'intervalle |
| Tags | batch, nyc, pyspark, taxi |
| start_date | 2024-01-01 |

### DAG 2 : nyc_weather_pipeline

```
ingest_weather_data  →  transform_weather_spark  →  verify_weather_data
  PythonOperator          BashOperator               PythonOperator
  Genere JSON             Lit JSON → PostgreSQL       Verifie 2184 obs
  → MinIO                                            + 4 categories
```

| Parametre | Valeur |
|-----------|--------|
| Schedule | @once (simulation) / @hourly (production reelle) |
| Retries | 3 tentatives, 2 min d'intervalle |
| Tags | nyc, pyspark, streaming, weather |
| start_date | 2024-01-01 |

---

## 12. Schema de donnees

### Tables PostgreSQL

**raw.fact_taxi_trips** (8 441 641 lignes, 23 colonnes)

| Colonne | Type | Description |
|---------|------|-------------|
| pickup_datetime | TIMESTAMP | Heure de prise en charge |
| dropoff_datetime | TIMESTAMP | Heure de depose |
| trip_duration_minutes | DOUBLE | Duree calculee en minutes |
| trip_distance | DOUBLE | Distance en miles |
| distance_bucket | TEXT | Tranche : >0-2km, 2-5km, >5km |
| fare_amount | DOUBLE | Tarif de base ($) |
| tip_amount | DOUBLE | Pourboire ($) |
| tip_percentage | DOUBLE | Pourboire en % du tarif |
| total_amount | DOUBLE | Total ($) |
| passenger_count | INTEGER | Nombre de passagers (1-6) |
| payment_type | INTEGER | Code : 1=CB, 2=Cash, 3=Gratuit |
| payment_type_desc | TEXT | Description lisible |
| pickup_location_id | INTEGER | Zone TLC depart (1-263) |
| dropoff_location_id | INTEGER | Zone TLC arrivee (1-263) |
| pickup_hour | INTEGER | Heure 0-23 |
| pickup_day_of_week | INTEGER | Jour 1-7 |
| pickup_date | DATE | Date seule |
| pickup_month | INTEGER | Mois 1-12 |

**raw.dim_weather** (2 184 lignes, 17 colonnes)

| Colonne | Type | Description |
|---------|------|-------------|
| observation_ts | TIMESTAMP | Horodatage exact |
| observation_hour | TIMESTAMP | Cle de jointure (tronque a l'heure) |
| temp_celsius | FLOAT | Temperature (degres C) |
| humidity_pct | INTEGER | Humidite relative (0-100%) |
| wind_speed_ms | FLOAT | Vitesse du vent (m/s) |
| weather_main | TEXT | Categorie OWM : Rain, Clear... |
| weather_category | TEXT | Notre categorie : Clair/Pluvieux/... |
| pressure_hpa | INTEGER | Pression (hPa) |
| visibility_m | INTEGER | Visibilite (metres) |

### Index PostgreSQL

| Index | Type | Table | Utilite |
|-------|------|-------|---------|
| idx_taxi_pickup_datetime | BTREE | fact_taxi_trips | Filtres et jointures |
| idx_taxi_pickup_brin | BRIN | fact_taxi_trips | Compact, donnees chronologiques |
| idx_taxi_pickup_hour | BTREE | fact_taxi_trips | GROUP BY horaires |
| idx_taxi_location | BTREE | fact_taxi_trips | Jointure high_value_customers |
| idx_weather_observation_hour | BTREE | dim_weather | Cle de jointure taxi-meteo |
| idx_weather_unique_hour | UNIQUE | dim_weather | 1 observation/heure garanti |

---

## 13. Notebook d'analyse

Le notebook `notebooks/analysis.ipynb` se connecte directement a PostgreSQL et repond aux 9 questions analytiques avec des graphiques.

**Lancement :**
```bash
jupyter notebook
# Ouvrir notebooks/analysis.ipynb
# Kernel → Restart & Run All
```

**9 questions / 9 graphiques :**

| # | Question | Type de graphique |
|---|----------|-------------------|
| Q1 | Distribution des durees de trajets | Histogramme + barres par tranche |
| Q2 | Longs trajets = plus de pourboires ? | Barres groupees + nuage de points |
| Q3 | Heures de prise en charge les plus chargees | Barres (pics en rouge) + courbe duree |
| Q4 | Correlation distance / pourboire | Barres + coefficients de correlation |
| Q5 | Temperature lors des pics de trajets | Double axe : volume + temperature |
| Q6 | Impact vent / pluie sur le nombre de trajets | Barres par meteo + vent moyen |
| Q7 | Comportements selon la meteo | 4 graphiques (duree, distance, tip, tarif) |
| Q8 | Heure des clients a haute valeur | Barres horaires (pic en rouge) |
| Q9 | La meteo influence-t-elle les pourboires ? | Barres avec ecart-type + % sans tip |

---

## 14. Resultats et insights

### Partie 1 — Spark

**Q1 — Distribution des durees :**
- Mediane : **11.4 min** · Moyenne : **14.1 min** · 75% < 18.1 min
- Distribution tres asymetrique (queue a droite : quelques trajets de +1h)
- Majorite des trajets : tranche 2-5 km (3.4M), puis 0-2 km (2.9M)

**Q2 — Pourboires selon la distance :**
- Trajets courts (0-2 km) : **30.0%** de pourboire en moyenne
- Trajets moyens (2-5 km) : 24.3%
- Trajets longs (>5 km) : 20.6%
- Correlation globale : **-0.18** (legere tendance inverse)
- Conclusion : la distance n'est pas le principal determinant du pourboire

**Q3 — Heures les plus chargees :**
- Pic du soir : **18h = 608 808 trajets** (heure la plus chargee)
- 17h = 587 701 · 16h = 542 097
- Creux de nuit : 4h = ~40 000 trajets (15x moins que le pic)
- Les durees les plus longues : 15h-16h (embouteillages)

**Q4 — Correlation distance/pourboire :**
- Par tranche : -0.19 (courts) · -0.13 (moyens) · -0.05 (longs)
- Correlations toutes faibles → autres facteurs plus determinants

### Partie 2 — Spark Streaming

**Q5 — Temperature lors des pics :**
- 18h : 608k trajets | **2.2°C**
- 17h : 588k trajets | 0.9°C
- Les pics de trafic = heures de bureau, independamment de la temperature

**Q6 — Impact meteo sur le volume :**
- Nuageux : **3887** trajets/h · Clair : 3879 · Pluvieux : 3855 · Orageux : 3665
- Ecart maximal : seulement 6% entre Nuageux et Orageux
- Conclusion : impact faible — les New-Yorkais prennent le taxi quelle que soit la meteo

### Partie 3 — dbt / Analyse

**Q7 — Comportements selon la meteo :**
- Duree : ~15.5 min pour toutes les categories (stable)
- Distance : ~3.3 miles (stable)
- Conclusion : la meteo ne change pas significativement le comportement de trajet

**Q8 — Heure des clients haute valeur :**
- Pic a **18h : 605 819 trajets** depuis zones premium
- Pourboire moyen a 18h : **23.33%** (superieur a la moyenne globale de 25%)
- Top 5 heures : 18h, 17h, 16h, 19h, 15h

**Q9 — Meteo et pourboires :**
- Orageux : 25.4% · Clair : 25.3% · Nuageux : 25.3% · Pluvieux : 25.2%
- Ecart maximal : **0.2%** entre conditions extremes
- Conclusion : influence marginale, statistiquement non significative

---

## 15. Choix techniques justifies

### Parquet vs CSV

| Critere | CSV | Parquet |
|---------|-----|---------|
| Taille | ~500 Mo/mois | ~50 Mo/mois (10x plus compact) |
| Lecture Spark | Toutes les colonnes | Seulement les colonnes demandees |
| Schema | A deviner | Integre dans le fichier |
| Compression | Non (sauf .gz) | Snappy integree |
| Vitesse lecture | Lente | 10-100x plus rapide |

### PySpark vs pandas

pandas charge tout en RAM. Avec 9.5M lignes x 19 colonnes de doubles, on atteint ~1.5 Go — problematique avec les autres services qui tournent en parallele. PySpark distribue le calcul sur tous les CPUs et est scalable sans changer le code.

### MinIO vs dossier local

MinIO implemente l'API Amazon S3. Le code boto3 et Spark S3A ecrit pour MinIO fonctionne sans modification sur AWS S3 en production. Migration cloud = changer les variables d'environnement, pas le code.

### dbt apres PySpark — separation des responsabilites

- **PySpark** : travail lourd (lire 153 Mo de Parquet, filtrer, calculs distribues)
- **dbt** : modelisation metier (SQL lisible, versionne, documente, teste)

dbt ajoute une couche de gouvernance : 25 tests automatiques detectent les anomalies de donnees a chaque execution.

### Airflow vs cron jobs

| Critere | Cron | Airflow |
|---------|------|---------|
| Dependances entre taches | Non | Oui (B attend A) |
| Retry automatique | Non | Oui (configurable) |
| Observabilite | Logs fichiers | UI graphique + historique |
| Alertes echec | Non (sans plugin) | Email, Slack... |
| Scalabilite | Non | Oui (CeleryExecutor, K8s) |

### Schema en etoile (Star Schema)

Standard des Data Warehouses analytiques (methodologie Kimball). La table de faits (`fact_taxi_trips`) contient les mesures. La table de dimension (`dim_weather`) fournit le contexte. Ce modele optimise les requetes analytiques en limitant les jointures.


### Auteurs
Projet réalisé en groupe dans le cadre d'un cours de Data Development. Rayelen GUESMI - Tharshan SIVAPLAN - Zakaria RRHAIBI