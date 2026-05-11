"""
streaming/transform_weather.py
================================
Transformation PySpark des données météo JSON depuis MinIO.

POURQUOI "STREAMING" DANS LE NOM ?
    En production réelle, ce script tournerait en mode Structured Streaming :
    il lirait les nouveaux fichiers JSON au fur et à mesure qu'ils arrivent
    (comme un tuyau qui coule en continu).

    Dans notre projet pédagogique, on simule ce comportement en lisant
    les fichiers mois par mois (batch par batch), ce qui produit le
    même résultat final.

CE QUE FAIT CE SCRIPT :
    1. Lit les 2 184 fichiers JSON météo depuis MinIO
    2. Extrait les champs imbriqués (structure JSON → colonnes plates)
    3. Calcule des colonnes dérivées (catégorie météo, colonnes temporelles)
    4. Déduplique par heure (une seule observation par heure)
    5. Écrit dans PostgreSQL (raw.dim_weather)

DÉFI TECHNIQUE : LES JSON IMBRIQUÉS
    Les fichiers JSON de l'API OpenWeatherMap ont des objets imbriqués :
    {
        "main": { "temp": 5.1, "humidity": 65 },
        "wind": { "speed": 4.2 },
        "weather": [{ "main": "Rain" }]   ← tableau !
    }

    PySpark peut lire cette structure, mais il faut "aplatir" les champs
    avec la notation pointée : F.col("main.temp"), F.col("wind.speed")
    Pour les tableaux : F.col("weather").getItem(0).getField("main")
"""

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# =============================================================================
# Configuration du logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("transform_weather")


# =============================================================================
# Configuration
# =============================================================================
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

YEAR   = 2024
MONTHS = ["01", "02", "03"]


# =============================================================================
# Création de la SparkSession
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Crée la SparkSession pour le traitement météo.
    Configuration identique au script taxi.
    """
    logger.info("Création de la SparkSession météo...")

    spark = (
        SparkSession.builder
        .appName("NYC_Weather_Stream_Transform")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.driver.memory",                     "2g")
        .config("spark.sql.adaptive.enabled",              "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession créée.")
    return spark


# =============================================================================
# Lecture des données JSON
# =============================================================================

def read_weather_json(spark: SparkSession) -> DataFrame:
    """
    Lit tous les fichiers JSON météo depuis MinIO, mois par mois.

    POURQUOI LIRE MOIS PAR MOIS ET PAS TOUT D'UN COUP ?
        Avec recursiveFileLookup=True sur 2000+ fichiers, Spark peut
        détecter des dossiers vides comme des "fichiers corrompus".
        Lire par wildcards mensuels (01/*/*.json) est plus robuste.

    UNION :
        On lit chaque mois séparément et on fusionne les DataFrames.
        unionByName : fusionne par nom de colonne (pas par position).
        C'est plus sûr si les schémas diffèrent légèrement entre fichiers.

    INFÉRENCE DE SCHÉMA :
        On laisse Spark déduire le schéma JSON automatiquement.
        C'est plus simple que de définir un StructType manuel.
        Inconvénient : nécessite un scan partiel des fichiers au démarrage.

    Args:
        spark: SparkSession active

    Returns:
        DataFrame avec tous les fichiers JSON fusionnés
    """
    logger.info("Lecture des fichiers JSON météo...")

    monthly_dfs = []

    for month in MONTHS:
        path = f"s3a://{BUCKET}/raw/weather/{YEAR}/{month}/*/*.json"
        logger.info("Lecture mois %s : %s", month, path)

        try:
            df_month = (
                spark.read
                .option("multiLine", "true")    # JSON multi-lignes (formatté avec indent)
                .option("inferSchema", "true")  # Déduit les types automatiquement
                .json(path)
            )

            count = df_month.count()
            logger.info("  → Mois %s : %d fichiers lus", month, count)
            monthly_dfs.append(df_month)

        except Exception as e:
            logger.error("Erreur lecture mois %s : %s", month, e)

    if not monthly_dfs:
        raise RuntimeError("Aucune donnée météo trouvée !")

    # Fusion de tous les mois en un seul DataFrame
    # reduce applique une fonction binaire à une liste :
    # reduce(union, [df1, df2, df3]) = df1.union(df2).union(df3)
    from functools import reduce
    df_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                    monthly_dfs)

    total = df_all.count()
    logger.info("Total fichiers JSON lus : %d", total)

    return df_all


# =============================================================================
# Transformation des données JSON
# =============================================================================

def flatten_and_enrich(df: DataFrame) -> DataFrame:
    """
    Aplatit la structure JSON imbriquée et calcule les colonnes dérivées.

    STRUCTURE JSON OPENWEATHERMAP :
        {
            "_metadata": {
                "collected_at_iso": "2024-01-15T14:00:00",
                "weather_category": "Pluvieux"
            },
            "main": {
                "temp": 5.1,          ← F.col("main.temp")
                "humidity": 65,
                "pressure": 1015
            },
            "wind": {
                "speed": 4.2,         ← F.col("wind.speed")
                "deg": 270
            },
            "weather": [              ← TABLEAU : getItem(0) = premier élément
                {
                    "main": "Rain",   ← F.col("weather").getItem(0).getField("main")
                    "description": "light rain"
                }
            ]
        }

    NOTATION POINTÉE :
        F.col("main.temp") accède au champ "temp" dans l'objet "main".
        C'est la façon PySpark d'accéder aux champs d'un StructType.

    Args:
        df: DataFrame avec structure JSON imbriquée

    Returns:
        DataFrame avec colonnes plates et enrichies
    """
    logger.info("Aplatissement de la structure JSON et enrichissement...")

    df_flat = (
        df

        # ── Horodatages ───────────────────────────────────────────────────
        # to_timestamp convertit la chaîne ISO en vrai type TIMESTAMP
        .withColumn(
            "observation_ts",
            F.to_timestamp(F.col("`_metadata`.collected_at_iso"))
        )
        # Tronque à l'heure pour la clé de jointure avec les trajets taxi
        # DATE_TRUNC('hour', ...) en SQL = date_trunc("hour", ...) en PySpark
        .withColumn(
            "observation_hour",
            F.date_trunc("hour", F.col("observation_ts"))
        )

        # ── Température (champs de l'objet "main") ────────────────────────
        .withColumn("temp_celsius",    F.round(F.col("main.temp").cast("float"), 1))
        .withColumn("temp_feels_like", F.round(F.col("main.feels_like").cast("float"), 1))

        # ── Conditions atmosphériques ─────────────────────────────────────
        .withColumn("humidity_pct",      F.col("main.humidity").cast("integer"))
        .withColumn("wind_speed_ms",     F.round(F.col("wind.speed").cast("float"), 2))
        .withColumn("wind_direction_deg",F.col("wind.deg").cast("integer"))
        .withColumn("pressure_hpa",      F.col("main.pressure").cast("integer"))
        .withColumn("visibility_m",      F.col("visibility").cast("integer"))

        # ── Description météo (premier élément du tableau "weather") ──────
        # getItem(0) : prend le premier élément du tableau ArrayType
        # getField("main") : extrait le champ "main" de cet élément
        .withColumn("weather_main",
                    F.col("weather").getItem(0).getField("main"))
        .withColumn("weather_description",
                    F.col("weather").getItem(0).getField("description"))

        # ── Catégorie météo simplifiée ────────────────────────────────────
        # On utilise la catégorie pré-calculée lors de la génération,
        # stockée dans _metadata.weather_category
        # Si absente (données réelles OWM), on la recalcule avec when/otherwise
        .withColumn(
            "weather_category",
            F.coalesce(
                # Essaie d'abord la valeur pré-calculée
                F.col("`_metadata`.weather_category"),
                # Sinon, recalcule à partir de weather_main
                F.when(F.col("weather").getItem(0).getField("main")
                       .isin(["Thunderstorm"]), "Orageux")
                 .when(F.col("weather").getItem(0).getField("main")
                       .isin(["Rain", "Drizzle", "Snow"]), "Pluvieux")
                 .when(F.col("weather").getItem(0).getField("main")
                       .isin(["Clear", "Clouds"]), "Clair")
                 .otherwise("Autre")
            )
        )

        # ── Dimensions temporelles ────────────────────────────────────────
        .withColumn("obs_hour",        F.hour("observation_ts"))
        .withColumn("obs_day_of_week", F.dayofweek("observation_ts"))
        .withColumn("obs_date",        F.to_date("observation_ts"))

        # ── Métadonnées pipeline ──────────────────────────────────────────
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    return df_flat


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Supprime les doublons par heure d'observation.

    POURQUOI DES DOUBLONS ?
        En streaming réel, il peut y avoir des ré-ingestions, des retransmissions
        ou des micro-batches qui se chevauchent. On garantit l'unicité.

        Pour notre simulation, il ne devrait pas y avoir de doublons,
        mais cette étape est une bonne pratique en data engineering.

    DROPDUPLICATES VS DISTINCT :
        dropDuplicates(["col"]) : déduplique sur des colonnes spécifiques
        distinct() : déduplique sur toutes les colonnes

        On utilise dropDuplicates sur observation_hour car c'est notre
        clé d'unicité : une seule observation météo par heure.

    Args:
        df: DataFrame avec potentiels doublons

    Returns:
        DataFrame sans doublons (une ligne par heure)
    """
    before = df.count()

    df_dedup = (
        df
        .filter(F.col("observation_ts").isNotNull())
        .filter(F.col("temp_celsius").isNotNull())
        .dropDuplicates(["observation_hour"])
        .orderBy("observation_ts")
    )

    after = df_dedup.count()
    logger.info(
        "Déduplication : %d → %d observations (-%d doublons)",
        before, after, before - after
    )

    return df_dedup


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Sélectionne et ordonne les colonnes finales pour PostgreSQL.

    L'ordre des colonnes doit correspondre au schéma de raw.dim_weather
    défini dans init_dwh.sql.
    """
    return df.select(
        "observation_ts",
        "observation_hour",
        "temp_celsius",
        "temp_feels_like",
        "humidity_pct",
        "wind_speed_ms",
        "wind_direction_deg",
        "weather_main",
        "weather_description",
        "weather_category",
        "pressure_hpa",
        "visibility_m",
        "obs_hour",
        "obs_day_of_week",
        "obs_date",
        "ingested_at",
        "source_file",
    )


# =============================================================================
# Écriture dans PostgreSQL
# =============================================================================

def write_to_postgres(df: DataFrame) -> int:
    """
    Écrit les observations météo dans raw.dim_weather.

    Même logique que pour les trajets taxi (mode overwrite, JDBC).
    La table contient une seule observation par heure → 2184 lignes max.
    """
    logger.info("Écriture dans PostgreSQL (raw.dim_weather)...")

    row_count = df.count()
    logger.info("Observations à écrire : %d", row_count)

    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=PG_JDBC_URL,
            table="raw.dim_weather",
            properties={
                "user":      PG_USER,
                "password":  PG_PASSWORD,
                "driver":    "org.postgresql.Driver",
                "batchsize": "500",
            }
        )
    )

    logger.info("Écriture terminée ! %d lignes dans raw.dim_weather", row_count)
    return row_count


# =============================================================================
# Statistiques de validation
# =============================================================================

def log_statistics(df: DataFrame) -> None:
    """
    Affiche des statistiques de validation après transformation.

    Bonne pratique : toujours vérifier les données après transformation.
    Ça permet de détecter rapidement des anomalies (ex: toutes les valeurs
    de température sont NULL → problème de schéma JSON).
    """
    logger.info("")
    logger.info("=== Statistiques de validation ===")

    # Distribution des catégories météo
    logger.info("Distribution météo :")
    df.groupBy("weather_category").count().orderBy("count", ascending=False).show()

    # Statistiques de température
    logger.info("Statistiques de température :")
    df.agg(
        F.round(F.avg("temp_celsius"), 1).alias("temp_moy_C"),
        F.round(F.min("temp_celsius"), 1).alias("temp_min_C"),
        F.round(F.max("temp_celsius"), 1).alias("temp_max_C"),
        F.round(F.avg("humidity_pct"), 0).alias("humidite_moy_pct"),
        F.round(F.avg("wind_speed_ms"), 1).alias("vent_moy_ms"),
    ).show()


# =============================================================================
# Pipeline principal
# =============================================================================

def run_pipeline():
    """
    Orchestre toutes les étapes de transformation météo.
    """
    start_time = datetime.now()

    logger.info("╔════════════════════════════════════════════╗")
    logger.info("║  TRANSFORMATION MÉTÉO NYC — PySpark v1.0  ║")
    logger.info("╚════════════════════════════════════════════╝")

    spark = create_spark_session()

    try:
        # ── Lecture ───────────────────────────────────────────────────────
        df_raw = read_weather_json(spark)

        # ── Transformations ───────────────────────────────────────────────
        df_flat    = flatten_and_enrich(df_raw)
        df_dedup   = deduplicate(df_flat)
        df_final   = select_final_columns(df_dedup)

        # ── Validation (avant écriture) ───────────────────────────────────
        log_statistics(df_dedup)

        # ── Écriture ──────────────────────────────────────────────────────
        row_count = write_to_postgres(df_final)

        # ── Rapport final ─────────────────────────────────────────────────
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("")
        logger.info("╔════════════════════════════════════════════╗")
        logger.info("║              RAPPORT FINAL                 ║")
        logger.info("╚════════════════════════════════════════════╝")
        logger.info("Observations écrites : %d", row_count)
        logger.info("Durée totale         : %.0f secondes", duration)
        logger.info("Table cible          : raw.dim_weather")
        logger.info("Transformation       : SUCCÈS !")

    finally:
        spark.stop()


# =============================================================================
# Point d'entrée
# =============================================================================

if __name__ == "__main__":
    run_pipeline()
