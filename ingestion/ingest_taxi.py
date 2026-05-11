"""
ingestion/ingest_taxi.py
========================
Script d'ingestion des données Yellow Taxi NYC.

CE QUE FAIT CE SCRIPT :
    1. Télécharge les fichiers Parquet mensuels depuis le site TLC
    2. Valide que chaque fichier n'est pas corrompu
    3. Uploade les fichiers dans MinIO (notre Data Lake)
    4. Génère un rapport de synthèse

POURQUOI PARQUET ET PAS CSV ?
    Le format Parquet est un format de fichier en colonnes optimisé pour
    l'analyse. Avantages par rapport au CSV :

    ┌─────────────────┬──────────────┬──────────────────┐
    │ Caractéristique │ CSV          │ Parquet          │
    ├─────────────────┼──────────────┼──────────────────┤
    │ Taille          │ ~500 Mo/mois │ ~50 Mo/mois      │
    │ Lecture Spark   │ Lente (tout) │ Rapide (colonnes)│
    │ Schéma intégré  │ Non          │ Oui              │
    │ Compression     │ Non          │ Snappy/Zstd      │
    └─────────────────┴──────────────┴──────────────────┘

UTILISATION :
    # Depuis le dossier nyc_pipeline/
    python ingestion/ingest_taxi.py

    # Ou via le DAG Airflow (recommandé en production)
"""

import os
import sys
import logging
import hashlib
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Optional

import requests
import boto3
from botocore.client import Config
import pyarrow.parquet as pq

# ── Ajout du dossier parent au path Python ────────────────────────────────────
# Nécessaire pour importer config.settings depuis n'importe où
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import minio_cfg, taxi_cfg

# ── Configuration du logging ──────────────────────────────────────────────────
# Le logging est préférable aux print() en production car :
# - On peut filtrer par niveau (DEBUG, INFO, WARNING, ERROR)
# - Les messages incluent automatiquement l'heure et le module
# - On peut rediriger vers un fichier, Airflow, ou un système centralisé
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest_taxi")


# =============================================================================
# Classe principale d'ingestion
# =============================================================================

class TaxiIngester:
    """
    Gère le téléchargement et l'upload des données Yellow Taxi NYC.

    POURQUOI UNE CLASSE ?
        On aurait pu tout écrire en fonctions, mais une classe permet de :
        - Partager l'état (client MinIO, config) entre les méthodes
        - Organiser le code de façon logique
        - Faciliter les tests unitaires
        - Réutiliser facilement dans d'autres contextes

    PATTERN UTILISÉ : Resource Manager
        Le client boto3 (MinIO) est créé une seule fois dans __init__
        et réutilisé pour tous les uploads. Créer une connexion à chaque
        appel serait coûteux en temps.
    """

    def __init__(self):
        """Initialise la connexion à MinIO et vérifie que le bucket existe."""
        logger.info("Initialisation du TaxiIngester...")

        # Création du client boto3 pour MinIO
        # boto3 est la bibliothèque AWS officielle, compatible avec MinIO
        # car MinIO implémente l'API S3 d'Amazon
        self.s3_client = boto3.client(
            service_name="s3",
            endpoint_url=minio_cfg.s3a_endpoint,
            aws_access_key_id=minio_cfg.access_key,
            aws_secret_access_key=minio_cfg.secret_key,
            # SIGNATURE_V4 est requis pour MinIO
            config=Config(signature_version="s3v4"),
            # Désactive la vérification SSL (on est en local sans certificat)
            verify=False,
        )

        self._ensure_bucket_exists()
        logger.info("Connexion MinIO établie sur %s", minio_cfg.s3a_endpoint)

    def _ensure_bucket_exists(self) -> None:
        """
        Vérifie que le bucket MinIO existe, le crée sinon.

        Méthode privée (préfixe _) : convention Python pour indiquer
        qu'elle n'est pas destinée à être appelée de l'extérieur.
        """
        try:
            self.s3_client.head_bucket(Bucket=minio_cfg.bucket)
            logger.info("Bucket '%s' trouvé.", minio_cfg.bucket)
        except Exception:
            logger.info("Création du bucket '%s'...", minio_cfg.bucket)
            self.s3_client.create_bucket(Bucket=minio_cfg.bucket)

    def download_parquet(self, month: str) -> Optional[bytes]:
        """
        Télécharge le fichier Parquet d'un mois donné depuis le TLC.

        Args:
            month: Numéro du mois sur 2 chiffres (ex: "01" pour janvier)

        Returns:
            Le contenu binaire du fichier, ou None en cas d'erreur.

        GESTION D'ERREURS :
            On utilise try/except pour capturer les problèmes réseau
            et retourner None au lieu de faire planter tout le script.
            L'appelant décide quoi faire en cas d'échec.
        """
        url = taxi_cfg.get_parquet_url(month)
        filename = taxi_cfg.get_filename(month)

        logger.info("Téléchargement : %s", url)
        logger.info("Fichier        : %s (~50 Mo, patience...)", filename)

        try:
            # stream=True : télécharge par morceaux (chunks) pour ne pas
            # charger tout le fichier en mémoire d'un coup
            response = requests.get(url, stream=True, timeout=300)

            # raise_for_status() lève une exception si le code HTTP
            # indique une erreur (4xx, 5xx)
            response.raise_for_status()

            # Assemblage des chunks en un seul objet bytes
            chunks = []
            total_bytes = 0
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):  # 8 Mo par chunk
                if chunk:
                    chunks.append(chunk)
                    total_bytes += len(chunk)

            content = b"".join(chunks)
            size_mb = total_bytes / (1024 * 1024)
            logger.info("Téléchargement terminé : %.1f Mo", size_mb)
            return content

        except requests.exceptions.Timeout:
            logger.error("Timeout lors du téléchargement de %s (>300s)", url)
            return None
        except requests.exceptions.HTTPError as e:
            logger.error("Erreur HTTP %s : %s", e.response.status_code, url)
            return None
        except requests.exceptions.RequestException as e:
            logger.error("Erreur réseau : %s", e)
            return None

    def validate_parquet(self, content: bytes, month: str) -> bool:
        """
        Valide que le fichier Parquet n'est pas corrompu.

        La validation est une étape CRUCIALE en ingestion.
        Un fichier corrompu peut :
        - Faire planter PySpark des heures plus tard
        - Produire des résultats incorrects silencieusement (pire cas)
        - Être difficile à diagnostiquer

        On valide ici :
        1. Que le fichier est un Parquet valide (lecture de ses métadonnées)
        2. Qu'il contient des données (au moins 1 ligne)
        3. Que les colonnes essentielles sont présentes

        Args:
            content: Contenu binaire du fichier
            month: Mois pour les messages de log

        Returns:
            True si valide, False sinon
        """
        logger.info("Validation du fichier Parquet (mois %s)...", month)

        # Colonnes indispensables pour notre pipeline
        REQUIRED_COLUMNS = {
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
        }

        try:
            # Écriture temporaire pour que pyarrow puisse lire le fichier
            # tempfile.NamedTemporaryFile crée un fichier temporaire qui
            # sera automatiquement supprimé à la sortie du bloc with
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name

            # Lecture des métadonnées seulement (pas des données)
            # C'est rapide : Parquet stocke le schéma en début de fichier
            # On utilise un bloc with pour fermer le fichier proprement
            # avant de le supprimer (important sur Windows qui verrouille
            # les fichiers ouverts et empêche leur suppression)
            with pq.ParquetFile(tmp_path) as parquet_file:
                schema = parquet_file.schema_arrow
                row_count = parquet_file.metadata.num_rows

            # Vérification du nombre de lignes
            if row_count == 0:
                logger.error("Fichier vide ! 0 lignes.")
                return False

            # Vérification des colonnes requises
            actual_columns = set(schema.names)
            missing = REQUIRED_COLUMNS - actual_columns
            if missing:
                logger.error("Colonnes manquantes : %s", missing)
                return False

            logger.info(
                "Validation OK — %d lignes, %d colonnes",
                row_count,
                len(actual_columns)
            )
            return True

        except Exception as e:
            logger.error("Fichier Parquet invalide : %s", e)
            return False
        finally:
            # finally : exécuté qu'il y ait eu une erreur ou non.
            # Le fichier est maintenant fermé (sorti du bloc with ci-dessus)
            # donc Windows accepte sa suppression.
            if "tmp_path" in locals():
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass  # Si la suppression échoue, ce n'est pas critique

    def upload_to_minio(self, content: bytes, month: str) -> bool:
        """
        Upload le fichier Parquet vers MinIO.

        STRUCTURE DES CLÉS S3/MinIO :
            Clé = chemin dans le bucket, comme un chemin de fichier.
            On organise par type/année/mois pour faciliter la navigation
            et permettre à Spark de lire par partition.

            Exemple : raw/taxi/2024/01/yellow_tripdata_2024-01.parquet
                       ^^^  ^^^^  ^^^^ ^^
                       │    │     │    └── mois
                       │    │     └─────── année
                       │    └───────────── type de données
                       └────────────────── zone du lac (raw = brut)

        Args:
            content: Contenu binaire du fichier validé
            month: Mois (ex: "01")

        Returns:
            True si l'upload a réussi, False sinon
        """
        filename = taxi_cfg.get_filename(month)
        # Clé S3 = chemin dans le bucket
        s3_key = f"{minio_cfg.raw_taxi_prefix}/{taxi_cfg.year}/{month}/{filename}"

        # Calcul du hash MD5 pour vérifier l'intégrité après upload
        # MD5 produit une "empreinte" de 128 bits du contenu.
        # Si un seul bit change, le hash change complètement.
        md5_hash = hashlib.md5(content).hexdigest()
        size_mb = len(content) / (1024 * 1024)

        logger.info("Upload vers MinIO : s3://%s/%s", minio_cfg.bucket, s3_key)
        logger.info("Taille : %.1f Mo | MD5 : %s", size_mb, md5_hash)

        try:
            self.s3_client.put_object(
                Bucket=minio_cfg.bucket,
                Key=s3_key,
                Body=content,
                # Métadonnées S3 : informations supplémentaires stockées
                # avec l'objet, consultables sans télécharger le fichier
                Metadata={
                    "source": "TLC Yellow Taxi",
                    "year": str(taxi_cfg.year),
                    "month": month,
                    "md5": md5_hash,
                    "ingested_at": datetime.utcnow().isoformat(),
                    "pipeline_version": "2.0",
                },
                ContentType="application/octet-stream",
            )

            # Vérification post-upload : on relit les métadonnées de l'objet
            # pour confirmer qu'il est bien arrivé intact
            head = self.s3_client.head_object(
                Bucket=minio_cfg.bucket, Key=s3_key
            )
            uploaded_size = head["ContentLength"]

            if uploaded_size != len(content):
                logger.error(
                    "Taille incorrecte après upload ! Attendu: %d, Reçu: %d",
                    len(content), uploaded_size
                )
                return False

            logger.info("Upload réussi ! Taille vérifiée : %d octets", uploaded_size)
            return True

        except Exception as e:
            logger.error("Erreur lors de l'upload : %s", e)
            return False

    def ingest_month(self, month: str) -> dict:
        """
        Pipeline complet pour un mois : télécharge → valide → uploade.

        Retourne un dictionnaire de résultat pour le rapport final.
        Ce pattern (retourner un dict de résultat) est courant dans les
        pipelines : chaque étape retourne son statut et ses métriques.
        """
        logger.info("=" * 50)
        logger.info("INGESTION : %s/%s", taxi_cfg.year, month)
        logger.info("=" * 50)

        result = {
            "month": month,
            "year": taxi_cfg.year,
            "status": "FAILED",
            "size_mb": 0,
            "error": None,
        }

        # Étape 1 : Téléchargement
        content = self.download_parquet(month)
        if content is None:
            result["error"] = "Échec du téléchargement"
            return result

        result["size_mb"] = round(len(content) / (1024 * 1024), 1)

        # Étape 2 : Validation
        if not self.validate_parquet(content, month):
            result["error"] = "Fichier Parquet invalide"
            return result

        # Étape 3 : Upload
        if not self.upload_to_minio(content, month):
            result["error"] = "Échec de l'upload MinIO"
            return result

        result["status"] = "SUCCESS"
        logger.info("Mois %s ingéré avec succès !", month)
        return result

    def run(self) -> bool:
        """
        Point d'entrée principal : ingère tous les mois configurés.

        Retourne True si tous les mois ont été ingérés avec succès.
        """
        logger.info("╔══════════════════════════════════════╗")
        logger.info("║  INGESTION YELLOW TAXI NYC           ║")
        logger.info("╚══════════════════════════════════════╝")
        logger.info("Mois à ingérer : %s", taxi_cfg.months)

        results = []
        for month in taxi_cfg.months:
            result = self.ingest_month(month)
            results.append(result)

        # ── Rapport de synthèse ───────────────────────────────────────────
        logger.info("")
        logger.info("╔══════════════════════════════════════╗")
        logger.info("║            RAPPORT FINAL             ║")
        logger.info("╚══════════════════════════════════════╝")

        total_size = 0
        success_count = 0

        for r in results:
            status_icon = "✓" if r["status"] == "SUCCESS" else "✗"
            logger.info(
                "%s  %s/%s — %s (%.1f Mo)",
                status_icon, r["year"], r["month"],
                r["status"], r["size_mb"]
            )
            if r["status"] == "SUCCESS":
                success_count += 1
                total_size += r["size_mb"]
            elif r["error"]:
                logger.info("   Erreur : %s", r["error"])

        logger.info("")
        logger.info(
            "Résultat : %d/%d mois ingérés | %.1f Mo total",
            success_count, len(results), total_size
        )

        all_success = success_count == len(results)
        if all_success:
            logger.info("Ingestion terminée avec succès !")
        else:
            logger.error("%d mois ont échoué.", len(results) - success_count)

        return all_success


# =============================================================================
# Point d'entrée du script
# =============================================================================
# if __name__ == "__main__" : ce bloc s'exécute uniquement quand on lance
# le script directement (python ingest_taxi.py), PAS quand il est importé
# par un autre module. C'est une convention Python importante.
# =============================================================================

if __name__ == "__main__":
    ingester = TaxiIngester()
    success = ingester.run()
    # sys.exit(0) = succès, sys.exit(1) = erreur
    # Airflow utilise ce code de retour pour savoir si la tâche a réussi
    sys.exit(0 if success else 1)