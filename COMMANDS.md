# Commandes du pipeline — Cheat Sheet

Guide de démarrage rapide et référence des commandes pour relancer le pipeline NYC Yellow Taxi + Météo.

---

## Prérequis

- Docker Desktop installé et démarré
- Python 3.12+
- Dépendances Python : `pip install boto3 requests pyarrow psycopg2-binary dbt-postgres pandas matplotlib seaborn jupyter`

---

## Démarrage rapide

```powershell
# 1. Aller dans le dossier du projet
cd C:\Users\<votre-user>\Desktop\nyc_pipeline

# 2. Lancer l'infrastructure (attendre ~2 minutes)
docker-compose up -d

# 3. Vérifier que les 6 services tournent
docker-compose ps
```

---

## 1. Ingestion des données

```powershell
# Météo simulée (~30 secondes — génère 2184 fichiers JSON dans MinIO)
python ingestion/ingest_weather.py

# Taxi TLC (~1 minute — télécharge 3 fichiers Parquet depuis TLC)
python ingestion/ingest_taxi.py
```

---

## 2. Transformation PySpark

```powershell
# Copier les scripts dans le conteneur Spark
docker cp spark/transform_taxi.py nyc_spark_master:/opt/spark/work-dir/
docker cp streaming/transform_weather.py nyc_spark_master:/opt/spark/work-dir/

# Donner les droits PostgreSQL (obligatoire avant la première exécution)
docker exec nyc_postgres psql -U airflow -d nyc_pipeline -c "
ALTER TABLE raw.fact_taxi_trips OWNER TO pipeline;
ALTER TABLE raw.dim_weather OWNER TO pipeline;
ALTER TABLE marts.trip_enriched OWNER TO pipeline;
ALTER TABLE marts.trip_summary_per_hour OWNER TO pipeline;
ALTER TABLE marts.high_value_customers OWNER TO pipeline;"

# Variable packages Spark
$PKGS = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.592,org.postgresql:postgresql:42.7.1"

# Transformation taxi (~2 minutes — 9.5M → 8.4M lignes)
docker exec -u root nyc_spark_master /opt/spark/bin/spark-submit `
    --master local[*] `
    --packages $PKGS `
    /opt/spark/work-dir/transform_taxi.py

# Transformation météo (~2 minutes — 2184 observations)
docker exec -u root nyc_spark_master /opt/spark/bin/spark-submit `
    --master local[*] `
    --packages $PKGS `
    /opt/spark/work-dir/transform_weather.py
```

---

## 3. Modélisation dbt

```powershell
cd dbt_project

# Encodage UTF-8 obligatoire sur Windows
$env:PYTHONUTF8=1

# Raccourci commande dbt
$DBT = "python C:\Users\<votre-user>\.pyenv\pyenv-win\versions\3.12.8\Lib\site-packages\dbt\cli\main.py"

# Tester la connexion
& $DBT debug --profiles-dir .

# Exécuter les 5 modèles (staging + marts)
& $DBT run --profiles-dir .

# Lancer les 25 tests de qualité
& $DBT test --profiles-dir .

cd ..
```

---

## 4. Notebook d'analyse

```powershell
cd notebooks
jupyter notebook
# Ouvrir analysis.ipynb → Kernel → Restart & Run All
cd ..
```

---

## 5. Vérification finale

```powershell
docker exec nyc_postgres psql -U pipeline -d nyc_pipeline -c "
SELECT 'fact_taxi_trips'        AS table_name, COUNT(*) FROM raw.fact_taxi_trips
UNION ALL
SELECT 'dim_weather',                           COUNT(*) FROM raw.dim_weather
UNION ALL
SELECT 'trip_enriched',                         COUNT(*) FROM raw_marts.trip_enriched
UNION ALL
SELECT 'trip_summary_per_hour',                 COUNT(*) FROM raw_marts.trip_summary_per_hour
UNION ALL
SELECT 'high_value_customers',                  COUNT(*) FROM raw_marts.high_value_customers;"
```

Résultats attendus :

| Table | Lignes |
|-------|--------|
| fact_taxi_trips | 8 441 641 |
| dim_weather | 2 184 |
| trip_enriched | 8 441 641 |
| trip_summary_per_hour | 2 183 |
| high_value_customers | 90 |

---

## Interfaces web

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Airflow | http://localhost:8084 | admin / admin |
| MinIO | http://localhost:9021 | minioadmin / minioadmin |
| Spark Master | http://localhost:8083 | — |

---

## Arrêter le projet

```powershell
# Arrêter les services (conserve les données)
docker-compose down

# Arrêter ET supprimer les volumes (repart de zéro)
docker-compose down -v
```

---

## Credentials

```
PostgreSQL  : pipeline / pipeline  (base nyc_pipeline)
             airflow  / airflow    (base airflow)
MinIO       : minioadmin / minioadmin
Airflow UI  : admin / admin
```
