"""
airflow/dags/dag_taxi_pipeline.py
==================================
DAG Airflow pour le pipeline Yellow Taxi NYC.

CONCEPT AIRFLOW DAG :
    Un DAG (Directed Acyclic Graph) est un workflow Airflow.
    Il définit UNE SÉQUENCE DE TÂCHES avec leurs dépendances.
    "Directed" = les tâches s'enchaînent dans un sens.
    "Acyclic"  = pas de boucle (A → B → C, jamais C → A).

CE DAG FAIT 3 CHOSES DANS L'ORDRE :
    1. ingest_taxi   : télécharge les fichiers Parquet TLC → MinIO
    2. transform_taxi : PySpark nettoie et enrichit → PostgreSQL
    3. verify_taxi   : vérifie que les données sont bien arrivées

PLANIFICATION :
    schedule_interval='@monthly' : s'exécute le 1er de chaque mois.
    En production, on pourrait mettre '@weekly' ou une cron expression.
    Pour les tests : déclencher manuellement via l'UI Airflow.

OPÉRATEURS UTILISÉS :
    PythonOperator  : exécute une fonction Python directement
    BashOperator    : exécute une commande shell (spark-submit)
    Ces opérateurs sont les plus simples et les plus courants.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# =============================================================================
# Configuration du DAG
# =============================================================================
# Ces paramètres s'appliquent à TOUTES les tâches du DAG par défaut.
# Chaque tâche peut les surcharger individuellement.

default_args = {
    "owner": "data_team",           # Propriétaire du DAG (affiché dans l'UI)
    "depends_on_past": False,        # Ne pas attendre le run précédent
    "email_on_failure": False,       # Pas d'email en cas d'échec (à activer en prod)
    "email_on_retry": False,
    "retries": 2,                    # Réessaie 2 fois en cas d'échec
    "retry_delay": timedelta(minutes=5),  # Attend 5 min entre chaque retry
}


# =============================================================================
# Fonctions des tâches Python
# =============================================================================

def ingest_taxi_data(**context):
    """
    Tâche 1 : Télécharge les données taxi depuis TLC et les stocke dans MinIO.

    POURQUOI UNE FONCTION ET PAS DIRECTEMENT LE SCRIPT ?
        Airflow peut importer et exécuter des fonctions Python directement.
        C'est plus propre que d'appeler subprocess ou BashOperator
        pour du Python pur.

    context : dictionnaire Airflow avec les métadonnées de l'exécution
        - context['ds']           : date d'exécution (YYYY-MM-DD)
        - context['execution_date'] : datetime complet
        - context['task_instance'] : l'objet TaskInstance
    """
    import sys
    import os

    # Ajout du dossier racine au path Python
    # Nécessaire car Airflow exécute dans son propre contexte
    sys.path.insert(0, "/opt/airflow")

    # Lecture des variables Airflow (configurées dans docker-compose.yml)
    # Ces variables permettent de ne pas hardcoder les credentials
    from airflow.models import Variable

    # On surcharge les variables d'environnement avec les valeurs Airflow
    os.environ["MINIO_ENDPOINT"]   = Variable.get("MINIO_ENDPOINT",   default_var="minio:9000")
    os.environ["MINIO_ACCESS_KEY"] = Variable.get("MINIO_ACCESS_KEY", default_var="minioadmin")
    os.environ["MINIO_SECRET_KEY"] = Variable.get("MINIO_SECRET_KEY", default_var="minioadmin")

    # Import et exécution du script d'ingestion
    from ingestion.ingest_taxi import TaxiIngester

    ingester = TaxiIngester()
    success = ingester.run()

    if not success:
        raise Exception("L'ingestion taxi a échoué — vérifiez les logs.")

    print(f"✅ Ingestion taxi terminée avec succès (exécution : {context['ds']})")


def verify_taxi_data(**context):
    """
    Tâche 3 : Vérifie que les données taxi ont bien été chargées dans PostgreSQL.

    POURQUOI UNE TÂCHE DE VÉRIFICATION ?
        En data engineering, on ne suppose jamais que les données sont là.
        On vérifie toujours. Si la table est vide ou incohérente,
        mieux vaut le savoir immédiatement plutôt que de propager
        des erreurs en aval.

    Cette tâche lève une exception si :
        - La table est vide (0 lignes)
        - Le nombre de lignes est anormalement bas (< 1M)
    """
    import psycopg2
    import os
    from airflow.models import Variable

    # Connexion PostgreSQL (via les variables Airflow)
    conn = psycopg2.connect(
        host=Variable.get("POSTGRES_HOST", default_var="postgres"),
        port=5432,
        dbname=Variable.get("POSTGRES_DB", default_var="nyc_pipeline"),
        user=Variable.get("POSTGRES_USER", default_var="pipeline"),
        password=Variable.get("POSTGRES_PASSWORD", default_var="pipeline"),
    )

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw.fact_taxi_trips;")
    count = cursor.fetchone()[0]
    conn.close()

    print(f"📊 Lignes dans raw.fact_taxi_trips : {count:,}")

    if count == 0:
        raise Exception("ERREUR : La table fact_taxi_trips est vide !")

    if count < 1_000_000:
        raise Exception(
            f"AVERTISSEMENT : Seulement {count:,} lignes dans fact_taxi_trips. "
            "Attendu : > 1 000 000. Vérifiez l'ingestion."
        )

    print(f"✅ Vérification OK : {count:,} trajets chargés.")


# =============================================================================
# Définition du DAG
# =============================================================================
# Le bloc 'with DAG(...)' est le pattern standard Airflow.
# Toutes les tâches définies à l'intérieur appartiennent à ce DAG.

with DAG(
    dag_id="nyc_taxi_pipeline",                    # Identifiant unique du DAG
    description="Pipeline complet Yellow Taxi NYC : ingestion → transformation → vérification",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),               # Date de début des exécutions
    schedule_interval="@monthly",                  # Exécution mensuelle
    catchup=False,                                 # Ne pas rattraper les runs passés
    tags=["nyc", "taxi", "batch", "pyspark"],      # Tags pour filtrer dans l'UI
) as dag:

    # ── Tâche 1 : Ingestion ───────────────────────────────────────────────────
    task_ingest = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data,
        provide_context=True,
        doc_md="""
        ## Ingestion Yellow Taxi
        Télécharge les fichiers Parquet mensuels depuis le site TLC
        et les uploade dans MinIO (raw/taxi/YYYY/MM/).
        """,
    )

    # ── Tâche 2 : Transformation PySpark ─────────────────────────────────────
    # BashOperator exécute spark-submit dans le conteneur Spark
    # docker exec permet d'exécuter une commande dans un autre conteneur
    task_transform = BashOperator(
        task_id="transform_taxi_spark",
        bash_command="""
        docker exec -u root nyc_spark_master \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --packages "org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.592,\
org.postgresql:postgresql:42.7.1" \
            /opt/spark/work-dir/transform_taxi.py
        """,
        doc_md="""
        ## Transformation PySpark Batch
        Lance le job Spark qui :
        - Lit les Parquet bruts depuis MinIO
        - Nettoie (filtre valeurs aberrantes, nulls)
        - Enrichit (durée, % pourboire, tranches distance)
        - Écrit dans PostgreSQL raw.fact_taxi_trips
        """,
    )

    # ── Tâche 3 : Vérification ────────────────────────────────────────────────
    task_verify = PythonOperator(
        task_id="verify_taxi_data",
        python_callable=verify_taxi_data,
        provide_context=True,
        doc_md="""
        ## Vérification des données
        Contrôle que raw.fact_taxi_trips contient un nombre
        de lignes cohérent avec les données attendues.
        Lève une exception si la table est vide ou incomplète.
        """,
    )

    # ── Définition des dépendances ────────────────────────────────────────────
    # L'opérateur >> définit l'ordre d'exécution :
    # task_ingest DOIT réussir avant task_transform
    # task_transform DOIT réussir avant task_verify
    #
    # Si une tâche échoue, les suivantes sont marquées "upstream_failed"
    # et ne s'exécutent pas.
    task_ingest >> task_transform >> task_verify
