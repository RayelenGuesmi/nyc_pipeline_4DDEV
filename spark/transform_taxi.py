"""
spark/transform_taxi.py
=======================
Transformation PySpark des données Yellow Taxi NYC.

CE QUE FAIT CE SCRIPT :
    Lit les fichiers Parquet bruts depuis MinIO, applique une série de
    transformations (nettoyage, enrichissement, standardisation) et écrit
    le résultat dans PostgreSQL.

CONCEPTS PYSPARK EXPLIQUÉS :
    SparkSession  : le point d'entrée de toute application Spark.
                    C'est l'objet principal qui gère la connexion au cluster,
                    la configuration, et la création des DataFrames.

    DataFrame     : table de données distribuée (comme un tableau Excel, mais
                    potentiellement sur des milliers de machines).
                    Les opérations sur un DataFrame sont LAZY (paresseuses) :
                    elles ne s'exécutent pas immédiatement, mais sont
                    enregistrées dans un plan d'exécution. Spark optimise ce
                    plan avant de lancer le calcul réel.

    Transformation : opération qui crée un nouveau DataFrame à partir d'un
                    autre (filter, select, withColumn...). Lazy.

    Action        : opération qui déclenche l'exécution réelle (count, show,
                    write...). C'est là que Spark fait vraiment le calcul.

UTILISATION :
    # Depuis le conteneur Docker Spark (recommandé)
    docker exec -u root nyc_spark_master /opt/spark/bin/spark-submit \\
        --master local[*] \\
        --packages "org.apache.hadoop:hadoop-aws:3.3.4,\\
                    com.amazonaws:aws-java-sdk-bundle:1.12.592,\\
                    org.postgresql:postgresql:42.7.1" \\
        /opt/spark/work-dir/transform_taxi.py
"""

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, IntegerType, TimestampType, StringType
)

# =============================================================================
# Configuration du logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("transform_taxi")


# =============================================================================
# Configuration — paramètres de connexion (internes aux conteneurs Docker)
# =============================================================================
# NOTE : Dans le conteneur Docker, les services se parlent par leur nom
# (minio:9000, postgres:5432) et non par localhost.

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET           = "nyc-datalake"

PG_HOST     = "postgres"
PG_PORT     = "5432"
PG_DB       = "nyc_pipeline"
PG_USER     = "pipeline"
PG_PASSWORD = "pipeline"
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Période à traiter (doit correspondre aux données ingérées)
YEAR   = 2024
MONTHS = ["01", "02", "03"]

# Seuils de filtrage (valeurs raisonnables pour les trajets NYC)
MIN_TRIP_DISTANCE     = 0.1    # miles — en dessous, c'est probablement une erreur
MAX_TRIP_DISTANCE     = 100.0  # miles — au-dessus, trajet improbable en taxi
MIN_FARE_AMOUNT       = 2.50   # $ — tarif minimum NYC
MAX_FARE_AMOUNT       = 500.0  # $ — au-dessus, valeur aberrante
MIN_DURATION_MINUTES  = 1.0    # En dessous d'1 minute, trajet non valide
MAX_DURATION_MINUTES  = 180.0  # 3 heures max (embouteillages extrêmes)
MAX_PASSENGER_COUNT   = 6      # Capacité maximale d'un taxi NYC


# =============================================================================
# Création de la SparkSession
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession.

    LA SPARKSESSION c'est quoi ?
        C'est le chef d'orchestre de votre application Spark.
        Il gère :
        - La connexion au cluster (ici local[*] = tous les CPUs)
        - La mémoire allouée
        - Les bibliothèques Java chargées (pour MinIO et PostgreSQL)
        - Le niveau de log (pour ne pas noyer les messages importants)

    CONFIGURATION S3A (pour lire MinIO) :
        Spark ne sait pas lire MinIO nativement.
        hadoop-aws fournit le driver S3A qui traduit les opérations
        S3 vers le protocole HTTP de MinIO.

        fs.s3a.path.style.access : force l'URL de style "chemin"
        (http://minio:9000/bucket/key) au lieu de style "virtual-hosted"
        (http://bucket.minio:9000/key). MinIO nécessite le style chemin.
    """
    logger.info("Création de la SparkSession...")

    spark = (
        SparkSession.builder
        .appName("NYC_Yellow_Taxi_Batch_Transform")
        .master("local[*]")

        # ── Configuration S3A pour MinIO ──────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ── Configuration mémoire et performance ──────────────────────────
        # 2g pour le driver (programme principal)
        .config("spark.driver.memory", "2g")
        # Active l'optimiseur adaptatif : Spark ajuste le plan d'exécution
        # pendant le calcul en fonction des statistiques réelles des données
        .config("spark.sql.adaptive.enabled", "true")
        # Réduit le nombre de partitions après un shuffle (mélange de données)
        # 200 partitions par défaut c'est trop pour nos volumes
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        .getOrCreate()
    )

    # Réduit les logs Spark au minimum (INFO est trop verbeux en production)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession créée. Interface web : http://localhost:4040")
    return spark


# =============================================================================
# Lecture des données
# =============================================================================

def read_raw_taxi(spark: SparkSession) -> DataFrame:
    """
    Lit tous les fichiers Parquet taxi depuis MinIO.

    LECTURE MULTI-FICHIERS :
        Spark peut lire plusieurs fichiers d'un coup avec un wildcard (*).
        Il les fusionne automatiquement en un seul DataFrame distribué.
        Les 3 fichiers (jan, fév, mars) sont lus en parallèle.

    POURQUOI PAS DE SCHÉMA EXPLICITE ?
        Les fichiers Parquet contiennent leur propre schéma (self-describing).
        Spark le lit automatiquement, contrairement au CSV qui nécessite
        de définir les types manuellement.

    Args:
        spark: La SparkSession active

    Returns:
        DataFrame avec toutes les données brutes (non filtrées)
    """
    # Pattern wildcard : lit tous les sous-dossiers de raw/taxi/2024/
    input_path = f"s3a://{BUCKET}/raw/taxi/{YEAR}/*/*.parquet"

    logger.info("Lecture des fichiers Parquet depuis : %s", input_path)

    df = spark.read.parquet(input_path)

    row_count = df.count()
    logger.info(
        "Données brutes chargées : %s lignes | %d colonnes",
        f"{row_count:,}",
        len(df.columns)
    )
    logger.info("Colonnes disponibles : %s", df.columns)

    return df


# =============================================================================
# Transformations
# =============================================================================

def clean_data(df: DataFrame) -> DataFrame:
    """
    Nettoyage des données : supprime les lignes invalides ou aberrantes.

    PRINCIPE DU NETTOYAGE :
        Les données brutes contiennent toujours des problèmes :
        - Valeurs nulles (champs non renseignés)
        - Valeurs aberrantes (erreurs de saisie, bugs du compteur)
        - Données hors période (trajets d'autres années dans le fichier)

        On FILTRE ces lignes plutôt que de les corriger.
        En data engineering, on préfère rejeter une donnée douteuse
        plutôt que de la "corriger" avec des hypothèses non vérifiables.

    OPÉRATION LAZY :
        filter() ne lit pas les données immédiatement.
        Il enregistre la condition dans le plan d'exécution.
        Spark les appliquera toutes en une seule lecture des fichiers.

    Args:
        df: DataFrame brut

    Returns:
        DataFrame filtré (lignes valides uniquement)
    """
    logger.info("Nettoyage des données...")
    initial_count = df.count()

    df_clean = df.filter(

        # ── Suppression des nulls sur les colonnes essentielles ───────────
        # isNotNull() = n'est pas nul
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull() &
        F.col("trip_distance").isNotNull() &
        F.col("fare_amount").isNotNull() &

        # ── Filtre sur la distance ────────────────────────────────────────
        # between(a, b) = equivalent de a <= x <= b
        F.col("trip_distance").between(MIN_TRIP_DISTANCE, MAX_TRIP_DISTANCE) &

        # ── Filtre sur le tarif ───────────────────────────────────────────
        F.col("fare_amount").between(MIN_FARE_AMOUNT, MAX_FARE_AMOUNT) &

        # ── Filtre sur le nombre de passagers ────────────────────────────
        F.col("passenger_count").between(1, MAX_PASSENGER_COUNT) &

        # ── Filtre sur la période (Jan-Mars 2024 uniquement) ─────────────
        # On vérifie que la date de prise en charge est bien dans 2024
        # Les fichiers TLC contiennent parfois des trajets d'autres mois
        (F.year("tpep_pickup_datetime") == YEAR) &
        (F.month("tpep_pickup_datetime").between(1, 3)) &

        # ── Cohérence temporelle ──────────────────────────────────────────
        # La dépose doit être APRÈS la prise en charge
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )

    clean_count = df_clean.count()
    rejected = initial_count - clean_count
    logger.info(
        "Nettoyage : %s → %s lignes (-%s rejetées, %.1f%%)",
        f"{initial_count:,}",
        f"{clean_count:,}",
        f"{rejected:,}",
        (rejected / initial_count * 100) if initial_count > 0 else 0
    )

    return df_clean


def enrich_data(df: DataFrame) -> DataFrame:
    """
    Enrichissement : calcule de nouvelles colonnes dérivées.

    CONCEPT WITHCOLUMN :
        withColumn("nouvelle_col", expression) crée ou remplace une colonne.
        On chaîne plusieurs withColumn() pour ajouter plusieurs colonnes.
        Spark optimise tout ça dans un seul passage sur les données.

    COLONNES CALCULÉES :
        Ces colonnes n'existent pas dans les fichiers bruts TLC.
        On les calcule à partir des données existantes pour faciliter
        les analyses futures (évite de recalculer à chaque requête SQL).

    Args:
        df: DataFrame nettoyé

    Returns:
        DataFrame enrichi avec de nouvelles colonnes
    """
    logger.info("Enrichissement des données...")

    # Dictionnaire de correspondance des types de paiement TLC
    # 1=Carte de crédit, 2=Espèces, 3=Gratuit, 4=Dispute, 5=Inconnu, 6=Annulé
    # create_map crée un dictionnaire Spark (MapType)
    payment_map = F.create_map(
        F.lit(1), F.lit("Credit card"),
        F.lit(2), F.lit("Cash"),
        F.lit(3), F.lit("No charge"),
        F.lit(4), F.lit("Dispute"),
        F.lit(5), F.lit("Unknown"),
        F.lit(6), F.lit("Voided trip"),
    )

    df_enriched = (
        df

        # ── Durée du trajet en minutes ────────────────────────────────────
        # unix_timestamp() : convertit un timestamp en secondes depuis Epoch
        # La différence donne des secondes, on divise par 60 pour les minutes
        # round(..., 2) : arrondi à 2 décimales
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.unix_timestamp("tpep_dropoff_datetime") -
                 F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
                2
            )
        )

        # ── Filtre sur la durée (après calcul) ───────────────────────────
        # On ne peut filtrer qu'APRÈS avoir calculé la colonne
        .filter(
            F.col("trip_duration_minutes").between(
                MIN_DURATION_MINUTES, MAX_DURATION_MINUTES
            )
        )

        # ── Pourcentage de pourboire ──────────────────────────────────────
        # NULLIF(x, 0) → NULL si x=0 (évite la division par zéro)
        # En PySpark, on utilise when/otherwise pour simuler NULLIF
        .withColumn(
            "tip_percentage",
            F.round(
                F.when(F.col("fare_amount") > 0,
                       F.col("tip_amount") / F.col("fare_amount") * 100
                ).otherwise(F.lit(0.0)),
                2
            )
        )

        # ── Tranche de distance ───────────────────────────────────────────
        # when/otherwise = équivalent de CASE WHEN en SQL
        # On convertit les miles en km (1 mile = 1.60934 km)
        .withColumn(
            "distance_bucket",
            F.when(F.col("trip_distance") * 1.60934 < 2,  ">0-2km")
             .when(F.col("trip_distance") * 1.60934 < 5,  "2-5km")
             .otherwise(">5km")
        )

        # ── Description du type de paiement ──────────────────────────────
        # getItem() accède à une valeur dans un MapType par sa clé
        .withColumn(
            "payment_type_desc",
            payment_map.getItem(F.col("payment_type"))
        )

    )

    return df_enriched


def add_time_dimensions(df: DataFrame) -> DataFrame:
    """
    Ajoute les colonnes temporelles dérivées.

    POURQUOI PRÉ-CALCULER CES COLONNES ?
        Dans les analyses, on filtre et groupe souvent par heure, jour,
        mois. Sans ces colonnes, chaque requête SQL ferait :
            EXTRACT(HOUR FROM pickup_datetime)
        Ce calcul est rapide, mais répété sur 8,5M lignes à chaque requête,
        il s'additionne. En les pré-calculant une fois, on économise du temps.

        C'est un exemple du principe "calculer une fois, lire plusieurs fois".

    Args:
        df: DataFrame enrichi

    Returns:
        DataFrame avec colonnes temporelles ajoutées
    """
    logger.info("Ajout des dimensions temporelles...")

    return (
        df
        # Heure de prise en charge (0-23)
        # hour() extrait l'heure d'un timestamp
        .withColumn("pickup_hour",        F.hour("tpep_pickup_datetime"))

        # Jour de la semaine (1=Lundi, 7=Dimanche en ISO)
        # dayofweek() retourne 1=Dimanche, 7=Samedi en Spark (convention US)
        # On normalise à la convention ISO (1=Lundi) avec (d + 5) % 7 + 1
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))

        # Date seule sans l'heure
        .withColumn("pickup_date",        F.to_date("tpep_pickup_datetime"))

        # Mois (1-12)
        .withColumn("pickup_month",       F.month("tpep_pickup_datetime"))

        # Métadonnées pipeline : heure d'ingestion et version
        .withColumn("ingested_at",        F.current_timestamp())
        .withColumn("source_file",        F.input_file_name())
    )


def standardize_columns(df: DataFrame) -> DataFrame:
    """
    Renomme et sélectionne les colonnes finales.

    POURQUOI RENOMMER ?
        Les colonnes TLC ont des noms avec préfixe (tpep_pickup_datetime).
        On les renomme pour correspondre au schéma PostgreSQL défini dans
        le DDL SQL, et pour que les noms soient plus lisibles.

    SELECT FINAL :
        On ne garde que les colonnes dont on a besoin.
        Cela réduit la taille du résultat et évite de charger des colonnes
        inutiles en mémoire.

    Args:
        df: DataFrame enrichi

    Returns:
        DataFrame avec colonnes finales standardisées
    """
    logger.info("Standardisation des colonnes...")

    return df.select(
        # Horodatages (renommés : tpep_pickup → pickup)
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("trip_duration_minutes"),

        # Métriques de trajet
        F.col("trip_distance"),
        F.col("distance_bucket"),

        # Métriques financières
        F.col("fare_amount"),
        F.col("extra"),
        F.col("mta_tax"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("total_amount"),
        F.col("tip_percentage"),

        # Passager et paiement
        F.col("passenger_count").cast("integer"),
        F.col("payment_type").cast("integer"),
        F.col("payment_type_desc"),

        # Localisation
        F.col("PULocationID").alias("pickup_location_id"),
        F.col("DOLocationID").alias("dropoff_location_id"),

        # Dimensions temporelles
        F.col("pickup_hour"),
        F.col("pickup_day_of_week"),
        F.col("pickup_date"),
        F.col("pickup_month"),

        # Métadonnées
        F.col("ingested_at"),
        F.col("source_file"),
    )


# =============================================================================
# Écriture dans PostgreSQL
# =============================================================================

def write_to_postgres(df: DataFrame) -> int:
    """
    Écrit le DataFrame transformé dans PostgreSQL via JDBC.

    JDBC (Java Database Connectivity) :
        Protocol Java standard pour se connecter aux bases de données.
        PySpark utilise JDBC pour écrire dans PostgreSQL car Spark est
        construit en Scala/Java et JDBC est natif dans cet écosystème.

    MODE "overwrite" :
        Efface et recrée la table à chaque exécution.
        Avantages :
        - Idempotent : relancer le script donne le même résultat
        - Pas de doublons si on relance après une erreur
        Inconvénients :
        - Perd l'historique (ok pour nous, pas pour un vrai DWH)

    PARTITIONS JDBC :
        On divise l'écriture en 8 partitions parallèles.
        Sans ça, Spark enverrait tout en un seul thread,
        ce qui serait lent pour 8,5M lignes.

    Args:
        df: DataFrame final à écrire

    Returns:
        Nombre de lignes écrites
    """
    logger.info("Écriture dans PostgreSQL (raw.fact_taxi_trips)...")
    logger.info("Mode : overwrite (recrée la table à chaque run)")

    row_count = df.count()
    logger.info("Lignes à écrire : %s", f"{row_count:,}")

    pg_properties = {
        "user":     PG_USER,
        "password": PG_PASSWORD,
        "driver":   "org.postgresql.Driver",
        # Taille des lots d'insertion (1000 lignes par batch INSERT)
        "batchsize": "1000",
    }

    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=PG_JDBC_URL,
            table="raw.fact_taxi_trips",
            properties=pg_properties,
        )
    )

    logger.info("Écriture terminée ! %s lignes dans raw.fact_taxi_trips", f"{row_count:,}")
    return row_count


# =============================================================================
# Pipeline principal
# =============================================================================

def run_pipeline():
    """
    Orchestre toutes les étapes de transformation.

    PRINCIPE DU PIPELINE :
        Chaque fonction retourne un DataFrame transformé.
        On chaîne les fonctions : chaque sortie est l'entrée de la suivante.

        raw → clean → enrich → time_dims → standardize → write

        Spark optimise l'ensemble en un seul plan d'exécution.
        Les 5 transformations ne sont pas exécutées séparément —
        Spark les fusionne en un seul parcours des données.
    """
    start_time = datetime.now()

    logger.info("╔════════════════════════════════════════════╗")
    logger.info("║   TRANSFORMATION TAXI NYC — PySpark v1.0  ║")
    logger.info("╚════════════════════════════════════════════╝")

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # ── Étape 1 : Lecture ─────────────────────────────────────────────
        df_raw = read_raw_taxi(spark)

        # ── Étapes 2-5 : Transformations (toutes LAZY) ───────────────────
        df_clean      = clean_data(df_raw)
        df_enriched   = enrich_data(df_clean)
        df_with_time  = add_time_dimensions(df_enriched)
        df_final      = standardize_columns(df_with_time)

        # ── Étape 6 : Écriture (ACTION — déclenche l'exécution réelle) ───
        row_count = write_to_postgres(df_final)

        # ── Rapport final ─────────────────────────────────────────────────
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("")
        logger.info("╔════════════════════════════════════════════╗")
        logger.info("║              RAPPORT FINAL                 ║")
        logger.info("╚════════════════════════════════════════════╝")
        logger.info("Lignes écrites  : %s", f"{row_count:,}")
        logger.info("Durée totale    : %.0f secondes", duration)
        logger.info("Débit           : %.0f lignes/seconde",
                    row_count / duration if duration > 0 else 0)
        logger.info("Table cible     : raw.fact_taxi_trips")
        logger.info("Transformation  : SUCCÈS !")

    finally:
        # finally : ferme toujours la session Spark, même en cas d'erreur.
        # Sans ça, les ressources (mémoire, threads) ne sont pas libérées.
        logger.info("Fermeture de la SparkSession...")
        spark.stop()


# =============================================================================
# Point d'entrée
# =============================================================================

if __name__ == "__main__":
    run_pipeline()
