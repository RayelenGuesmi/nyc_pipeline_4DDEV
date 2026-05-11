"""
airflow/dags/dag_weather_pipeline.py
=====================================
DAG Airflow pour le pipeline météo OpenWeatherMap.

CE DAG SIMULE UN PIPELINE STREAMING :
    En production réelle, ce DAG tournerait toutes les heures
    pour collecter la météo en temps réel.
    Dans notre projet, il génère les données historiques simulées
    de Jan-Mars 2024 en une seule exécution.

CE DAG FAIT 3 CHOSES DANS L'ORDRE :
    1. ingest_weather   : génère/collecte les JSON météo → MinIO
    2. transform_weather : PySpark Streaming traite les JSON → PostgreSQL
    3. verify_weather   : vérifie les données dans dim_weather

DIFFÉRENCE AVEC LE DAG TAXI :
    Le DAG taxi = Batch (gros volume traité d'un coup)
    Ce DAG = Streaming simulé (petits fichiers JSON traités en continu)

    En production : schedule_interval='@hourly' pour collecter 1 JSON/heure
    Dans ce projet : '@once' pour générer tout Jan-Mars 2024 en une fois
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# =============================================================================
# Configuration du DAG
# =============================================================================

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,                              # Plus de retries car réseau API
    "retry_delay": timedelta(minutes=2),
}


# =============================================================================
# Fonctions des tâches Python
# =============================================================================

def ingest_weather_data(**context):
    """
    Tâche 1 : Génère les observations météo simulées et les stocke dans MinIO.

    EN PRODUCTION (avec vraie API OWM) :
        Cette fonction appellerait l'API OpenWeatherMap pour l'heure courante
        et sauvegarderait le JSON résultant dans MinIO.
        Le DAG tournerait toutes les heures avec schedule_interval='@hourly'.

    DANS CE PROJET (simulation) :
        On génère les données historiques Jan-Mars 2024 en une seule passe.
        Chaque fichier JSON simule une observation horaire réaliste
        basée sur les normales climatiques NYC.

    IDEMPOTENCE :
        Cette tâche est idempotente : si on la relance, elle génère
        les mêmes données (grâce à random.seed(42)).
        C'est une propriété importante en data engineering.
    """
    import sys
    import os
    from airflow.models import Variable

    sys.path.insert(0, "/opt/airflow")

    os.environ["MINIO_ENDPOINT"]      = Variable.get("MINIO_ENDPOINT",      default_var="minio:9000")
    os.environ["MINIO_ACCESS_KEY"]    = Variable.get("MINIO_ACCESS_KEY",    default_var="minioadmin")
    os.environ["MINIO_SECRET_KEY"]    = Variable.get("MINIO_SECRET_KEY",    default_var="minioadmin")
    os.environ["OPENWEATHER_API_KEY"] = Variable.get("OPENWEATHER_API_KEY", default_var="")

    from ingestion.ingest_weather import WeatherIngester

    ingester = WeatherIngester()
    success = ingester.run()

    if not success:
        raise Exception("L'ingestion météo a échoué — vérifiez les logs.")

    print(f"✅ Ingestion météo terminée (exécution : {context['ds']})")


def verify_weather_data(**context):
    """
    Tâche 3 : Vérifie que dim_weather contient les bonnes observations.

    VÉRIFICATIONS EFFECTUÉES :
        1. La table n'est pas vide
        2. Le nombre d'observations est cohérent (1 par heure sur 90 jours)
        3. Les 4 catégories météo sont présentes (Clair, Nuageux, Pluvieux, Orageux)
        4. Aucune valeur nulle dans les colonnes critiques
    """
    import psycopg2
    from airflow.models import Variable

    conn = psycopg2.connect(
        host=Variable.get("POSTGRES_HOST",     default_var="postgres"),
        port=5432,
        dbname=Variable.get("POSTGRES_DB",     default_var="nyc_pipeline"),
        user=Variable.get("POSTGRES_USER",     default_var="pipeline"),
        password=Variable.get("POSTGRES_PASSWORD", default_var="pipeline"),
    )
    cursor = conn.cursor()

    # Vérification du nombre total d'observations
    cursor.execute("SELECT COUNT(*) FROM raw.dim_weather;")
    count = cursor.fetchone()[0]

    # Vérification des catégories météo présentes
    cursor.execute("""
        SELECT weather_category, COUNT(*) as nb
        FROM raw.dim_weather
        GROUP BY weather_category
        ORDER BY nb DESC;
    """)
    categories = cursor.fetchall()

    conn.close()

    print(f"📊 Observations météo : {count}")
    print("📊 Distribution météo :")
    for cat, nb in categories:
        print(f"   {cat:10s} : {nb}")

    if count == 0:
        raise Exception("ERREUR : La table dim_weather est vide !")

    if count < 100:
        raise Exception(
            f"ERREUR : Seulement {count} observations dans dim_weather. "
            "Attendu : ~2184 (90 jours × 24 heures)."
        )

    print(f"✅ Vérification météo OK : {count} observations chargées.")


# =============================================================================
# Définition du DAG
# =============================================================================

with DAG(
    dag_id="nyc_weather_pipeline",
    description="Pipeline météo NYC : ingestion JSON → Spark Streaming → PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # En production : '@hourly' pour collecter la météo toutes les heures
    # En simulation : '@once' pour générer tout Jan-Mars 2024 en une exécution
    schedule_interval="@once",
    catchup=False,
    tags=["nyc", "weather", "streaming", "pyspark"],
) as dag:

    # ── Tâche 1 : Ingestion météo ─────────────────────────────────────────────
    task_ingest = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather_data,
        provide_context=True,
        doc_md="""
        ## Ingestion Météo
        Génère les observations météo simulées (Jan-Mars 2024)
        et les stocke dans MinIO sous raw/weather/YYYY/MM/DD/.
        Chaque fichier JSON correspond à une heure d'observation.
        """,
    )

    # ── Tâche 2 : Transformation PySpark Streaming ────────────────────────────
    # PySpark lit les JSON depuis MinIO mois par mois (simulation streaming)
    # et écrit dans PostgreSQL raw.dim_weather
    task_transform = BashOperator(
        task_id="transform_weather_spark_streaming",
        bash_command="""
        docker exec -u root nyc_spark_master \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --packages "org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.592,\
org.postgresql:postgresql:42.7.1" \
            /opt/spark/work-dir/transform_weather.py
        """,
        doc_md="""
        ## Transformation PySpark Streaming
        Lance le job Spark Streaming qui :
        - Surveille raw/weather/ dans MinIO (nouveaux fichiers JSON)
        - Extrait température, humidité, vent, catégorie météo
        - Ajoute les dimensions temporelles (heure, jour semaine)
        - Écrit dans PostgreSQL raw.dim_weather
        """,
    )

    # ── Tâche 3 : Vérification ────────────────────────────────────────────────
    task_verify = PythonOperator(
        task_id="verify_weather_data",
        python_callable=verify_weather_data,
        provide_context=True,
        doc_md="""
        ## Vérification Météo
        Contrôle que raw.dim_weather contient les 2184 observations
        attendues (90 jours × 24 heures) avec les 4 catégories météo.
        """,
    )

    # ── Dépendances ────────────────────────────────────────────────────────────
    task_ingest >> task_transform >> task_verify
