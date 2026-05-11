"""
config/settings.py
==================
Configuration centralisée du pipeline NYC Taxi + Météo.

POURQUOI CE FICHIER EXISTE :
    Imaginez que vous ayez 10 scripts Python qui utilisent tous le port de
    MinIO. Si ce port change, sans ce fichier vous devez modifier 10 fichiers.
    Avec ce fichier, vous ne modifiez qu'un seul endroit.

COMMENT ÇA MARCHE :
    1. Les valeurs sont lues depuis les variables d'environnement (fichier .env)
    2. os.getenv("NOM", "valeur_par_defaut") lit la variable, ou utilise
       la valeur par défaut si elle n'est pas définie
    3. Chaque module du projet importe ce qu'il a besoin :
       from config.settings import minio_cfg, pg_cfg

PATTERN UTILISÉ : Dataclass
    Une dataclass est une classe Python simplifiée pour stocker des données.
    @dataclass remplace le besoin d'écrire __init__ manuellement.
"""

import os
from dataclasses import dataclass, field
from typing import List


# =============================================================================
# CONFIGURATION MINIO — Data Lake
# =============================================================================

@dataclass
class MinIOConfig:
    """
    Paramètres de connexion à MinIO (notre Data Lake).

    MinIO est un serveur de stockage objet compatible avec l'API Amazon S3.
    Cela signifie que le code écrit pour S3 fonctionne aussi avec MinIO,
    ce qui est pratique pour tester localement avant de passer au cloud.

    PORTS :
        - 9020 : port exposé sur votre machine (accès depuis Python local)
        - 9000 : port interne Docker (accès depuis les conteneurs)
        Le docker-compose mappe 9020 → 9000.
    """

    # Adresse du serveur MinIO (depuis votre machine locale)
    endpoint: str   = os.getenv("MINIO_ENDPOINT", "localhost:9020")

    # Identifiants (équivalent au login/mot de passe S3)
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    # Nom du "bucket" (équivalent d'un dossier racine dans S3/MinIO)
    bucket: str     = os.getenv("MINIO_BUCKET", "nyc-datalake")

    # Connexion non chiffrée en local (False = HTTP, True = HTTPS)
    secure: bool    = False

    # ---- Chemins dans le bucket ----
    # Convention : raw/ = données brutes non modifiées
    #              processed/ = données après transformation
    raw_taxi_prefix: str            = "raw/taxi"
    raw_weather_prefix: str         = "raw/weather"
    processed_taxi_prefix: str      = "processed/taxi"
    processed_weather_prefix: str   = "processed/weather"

    @property
    def s3a_endpoint(self) -> str:
        """
        Retourne l'URL complète pour Spark (protocole S3A).

        S3A est le pilote Hadoop qui permet à Spark de lire/écrire
        sur des stockages compatibles S3 (comme MinIO).
        On préfixe avec http:// car notre MinIO local n'a pas de TLS.
        """
        return f"http://{self.endpoint}"

    @property
    def endpoint_docker(self) -> str:
        """
        Adresse de MinIO depuis l'intérieur des conteneurs Docker.

        Dans le réseau Docker, les conteneurs se parlent par leur
        nom de service (défini dans docker-compose.yml), pas par
        localhost. Le port interne est 9000.
        """
        return "minio:9000"


# =============================================================================
# CONFIGURATION POSTGRESQL — Entrepôt de données (DWH)
# =============================================================================

@dataclass
class PostgresConfig:
    """
    Paramètres de connexion à PostgreSQL.

    PostgreSQL est notre Data Warehouse (DWH) : l'endroit où les données
    transformées et modélisées sont stockées pour l'analyse finale.

    DIFFÉRENCE Data Lake vs Data Warehouse :
        - Data Lake (MinIO) : stocke TOUT, dans le format brut d'origine.
          Pas de schéma imposé. Bon pour l'exploration.
        - Data Warehouse (PostgreSQL) : stocke les données STRUCTURÉES,
          nettoyées, optimisées pour les requêtes analytiques.

    SCHÉMAS PostgreSQL utilisés dans ce projet :
        - raw    : tables chargées par PySpark (données transformées mais brutes)
        - marts  : tables créées par dbt (données métier finales)
    """

    host: str     = os.getenv("POSTGRES_HOST", "localhost")
    port: int     = int(os.getenv("POSTGRES_PORT", "5435"))
    db: str       = os.getenv("POSTGRES_DB", "nyc_pipeline")
    user: str     = os.getenv("POSTGRES_USER", "pipeline")
    password: str = os.getenv("POSTGRES_PASSWORD", "pipeline")

    @property
    def jdbc_url(self) -> str:
        """
        URL de connexion au format JDBC, utilisée par Spark.

        JDBC (Java Database Connectivity) est le protocole standard
        Java/Scala pour se connecter à des bases de données.
        PySpark utilise JDBC pour écrire dans PostgreSQL.

        Format : jdbc:postgresql://host:port/database
        """
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def jdbc_url_docker(self) -> str:
        """URL JDBC depuis l'intérieur des conteneurs Docker."""
        return f"jdbc:postgresql://postgres:5432/{self.db}"

    @property
    def connection_string(self) -> str:
        """
        Chaîne de connexion au format psycopg2 (driver Python natif).

        psycopg2 est la bibliothèque Python standard pour PostgreSQL.
        On l'utilise pour les vérifications légères (counts, checks)
        sans passer par Spark.
        """
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.db} user={self.user} password={self.password}"
        )


# =============================================================================
# CONFIGURATION OPENWEATHERMAP — API Météo
# =============================================================================

@dataclass
class WeatherConfig:
    """
    Paramètres pour l'API OpenWeatherMap.

    OpenWeatherMap fournit une API REST : on envoie une requête HTTP
    avec notre clé API et la ville, on reçoit un JSON avec la météo.

    PLAN GRATUIT : 1 000 appels/jour, données toutes les heures.
    C'est suffisant pour notre projet qui collecte 24 observations/jour.

    CITY ID : Chaque ville a un identifiant unique dans OWM.
    New York City = 5128581 (ne pas confondre avec d'autres "New York")
    """

    api_key: str   = os.getenv("OPENWEATHER_API_KEY", "")
    city_id: int   = int(os.getenv("OWM_CITY_ID", "5128581"))
    city_name: str = os.getenv("OWM_CITY_NAME", "New York City")
    base_url: str  = "https://api.openweathermap.org/data/2.5"

    @property
    def current_weather_url(self) -> str:
        """
        URL complète pour obtenir la météo actuelle.

        Paramètres :
            id      : identifiant de la ville
            appid   : votre clé API
            units   : metric = températures en Celsius (sinon Kelvin)
        """
        return (
            f"{self.base_url}/weather"
            f"?id={self.city_id}"
            f"&appid={self.api_key}"
            f"&units=metric"
        )

    def validate(self) -> bool:
        """Vérifie que la clé API est bien configurée."""
        if not self.api_key or self.api_key == "votre_cle_api_ici":
            raise ValueError(
                "OPENWEATHER_API_KEY n'est pas configurée.\n"
                "Editez le fichier .env et remplacez 'votre_cle_api_ici' "
                "par votre vraie clé depuis openweathermap.org/api"
            )
        return True


# =============================================================================
# CONFIGURATION TAXI NYC — Source de données TLC
# =============================================================================

@dataclass
class TaxiConfig:
    """
    Paramètres pour télécharger les données Yellow Taxi NYC.

    SOURCE : TLC (Taxi & Limousine Commission) publie chaque mois
    les données de tous les trajets en taxi de NYC.
    URL : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    FORMAT : Parquet
        Parquet est un format de fichier en colonnes, très efficace pour
        l'analyse de données. Avantages vs CSV :
        - 5 à 10x plus compact (compression intégrée)
        - 10 à 100x plus rapide à lire avec Spark (lecture par colonne)
        - Schéma intégré (pas besoin de deviner les types)

    TAILLE : chaque fichier mensuel fait environ 50 Mo.
    3 mois (Jan-Mars 2024) = ~150 Mo à télécharger.
    """

    # URL de base des fichiers Parquet du TLC
    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    # Période à traiter (lue depuis .env)
    year: int          = int(os.getenv("TAXI_YEAR", "2024"))
    months: List[str]  = field(default_factory=list)

    def __post_init__(self):
        """
        __post_init__ est appelé automatiquement après __init__ par @dataclass.
        On l'utilise pour initialiser la liste des mois depuis .env,
        car on ne peut pas appeler os.getenv dans un default_factory proprement.
        """
        months_str = os.getenv("TAXI_MONTHS", "01,02,03")
        # zfill(2) ajoute un zéro devant si nécessaire : "1" → "01"
        self.months = [m.strip().zfill(2) for m in months_str.split(",")]

    def get_parquet_url(self, month: str) -> str:
        """
        Construit l'URL de téléchargement pour un mois donné.

        Exemple : get_parquet_url("01")
        → "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        """
        return f"{self.base_url}/yellow_tripdata_{self.year}-{month}.parquet"

    def get_filename(self, month: str) -> str:
        """Nom du fichier local/MinIO pour un mois donné."""
        return f"yellow_tripdata_{self.year}-{month}.parquet"


# =============================================================================
# CONFIGURATION SPARK
# =============================================================================

@dataclass
class SparkConfig:
    """
    Paramètres pour les jobs PySpark.

    SPARK EN LOCAL vs EN CLUSTER :
        - local[*] : Spark tourne sur votre machine, utilise tous les CPU.
          Mode utilisé pour le développement et ce projet.
        - spark://host:port : Spark tourne sur un cluster distant.
          Mode production avec des milliers de machines.

    PACKAGES (JARs Java) nécessaires :
        - hadoop-aws : permet à Spark de lire/écrire sur S3/MinIO
        - aws-java-sdk-bundle : SDK AWS nécessaire pour hadoop-aws
        - postgresql : driver JDBC pour écrire dans PostgreSQL

    Ces packages sont téléchargés automatiquement depuis Maven Central
    au premier lancement. Ils sont mis en cache ensuite.
    """

    # Mode d'exécution Spark (* = tous les cœurs disponibles)
    master: str = os.getenv("SPARK_MASTER", "local[*]")

    # Noms des applications (visibles dans l'interface Spark UI)
    app_name_taxi: str    = "NYC_Yellow_Taxi_Batch_Transform"
    app_name_weather: str = "NYC_Weather_Stream_Process"

    # Versions des dépendances Java
    hadoop_aws_version: str    = "3.3.4"
    aws_sdk_version: str       = "1.12.592"
    postgresql_version: str    = "42.7.1"

    @property
    def maven_packages(self) -> str:
        """
        Liste des packages Maven à charger au démarrage de Spark.

        FORMAT : "groupId:artifactId:version,groupId:artifactId:version"
        C'est le format attendu par spark.jars.packages.
        """
        return (
            f"org.apache.hadoop:hadoop-aws:{self.hadoop_aws_version},"
            f"com.amazonaws:aws-java-sdk-bundle:{self.aws_sdk_version},"
            f"org.postgresql:postgresql:{self.postgresql_version}"
        )


# =============================================================================
# INSTANCES GLOBALES — Ce que les autres modules importent
# =============================================================================
#
# PATTERN SINGLETON :
#   En créant les instances ici (une seule fois), tous les modules qui
#   importent ce fichier partagent les mêmes objets de configuration.
#   C'est économique en mémoire et garantit la cohérence.
#
# UTILISATION dans un script :
#   from config.settings import minio_cfg, pg_cfg, weather_cfg
#   client = boto3.client("s3", endpoint_url=minio_cfg.s3a_endpoint)
# =============================================================================

minio_cfg   = MinIOConfig()
pg_cfg      = PostgresConfig()
weather_cfg = WeatherConfig()
taxi_cfg    = TaxiConfig()
spark_cfg   = SparkConfig()
