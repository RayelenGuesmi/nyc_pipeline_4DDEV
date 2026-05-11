"""
ingestion/ingest_weather.py
===========================
Script d'ingestion des données météo OpenWeatherMap pour NYC.

STRATÉGIE : SIMULATION HISTORIQUE
    Pour notre projet, on ne collecte pas la météo en temps réel.
    On SIMULE les données historiques de Jan-Mars 2024.

    Pourquoi simuler ?
    - L'API gratuite OWM donne uniquement la météo ACTUELLE
    - Les données historiques sont payantes (>60€/mois)
    - Pour un projet pédagogique, des données simulées réalistes suffisent

    Comment on simule de façon réaliste ?
    - On utilise les vraies températures moyennes de NYC en hiver
      (janvier : 0-5°C, février : 0-7°C, mars : 3-12°C)
    - On ajoute une variation aléatoire gaussienne (distribution naturelle)
    - On simule des épisodes de pluie/neige cohérents avec le climat
    - Chaque fichier JSON a exactement la même structure que l'API OWM réelle

    Si vous avez une clé API OWM valide et des données historiques,
    remplacez la méthode generate_simulated_observation() par un vrai
    appel API.

STRUCTURE DES FICHIERS GÉNÉRÉS :
    raw/weather/2024/01/01/weather_20240101_0000.json  ← 00h00
    raw/weather/2024/01/01/weather_20240101_0100.json  ← 01h00
    ...
    raw/weather/2024/03/31/weather_20240331_2300.json  ← 23h00

    = 2 161 fichiers JSON (90 jours × 24 heures + 1 jour = 2161)
"""

import os
import sys
import json
import logging
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import boto3
from botocore.client import Config
import requests

# ── Ajout du dossier parent au path Python ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import minio_cfg, weather_cfg

# ── Configuration du logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest_weather")


# =============================================================================
# Paramètres climatiques de NYC en hiver
# =============================================================================
# Ces données sont basées sur les normales climatiques de NYC (Central Park).
# Source : NOAA (National Oceanic and Atmospheric Administration)
#
# FORMAT DU DICTIONNAIRE :
#   mois → { "temp_mean": température moyenne °C,
#             "temp_std":  écart-type (variation journalière),
#             "rain_prob": probabilité de pluie/neige sur 24h }

NYC_CLIMATE = {
    1:  {"temp_mean": 0.5,  "temp_std": 4.0, "rain_prob": 0.35},  # Janvier
    2:  {"temp_mean": 2.0,  "temp_std": 4.5, "rain_prob": 0.30},  # Février
    3:  {"temp_mean": 7.0,  "temp_std": 5.0, "rain_prob": 0.35},  # Mars
}

# Codes météo OpenWeatherMap utilisés dans la simulation
# Voir : https://openweathermap.org/weather-conditions
WEATHER_CONDITIONS = {
    "Clair":   [("Clear", "clear sky",       800)],
    "Nuageux": [("Clouds", "few clouds",     801),
                ("Clouds", "scattered clouds", 802),
                ("Clouds", "overcast clouds", 804)],
    "Pluvieux":[("Rain",   "light rain",     500),
                ("Rain",   "moderate rain",  501),
                ("Drizzle","light drizzle",  300),
                ("Snow",   "light snow",     600)],
    "Orageux": [("Thunderstorm", "thunderstorm", 200)],
}


# =============================================================================
# Classe principale d'ingestion météo
# =============================================================================

class WeatherIngester:
    """
    Génère et stocke les observations météo simulées pour NYC.

    APPROCHE OBJECT-ORIENTED :
        Comme pour TaxiIngester, on encapsule la logique dans une classe.
        Cela permet de partager le client S3 et les paramètres entre
        toutes les méthodes sans les passer en argument à chaque fois.
    """

    def __init__(self):
        """Initialise la connexion MinIO."""
        logger.info("Initialisation du WeatherIngester...")

        self.s3_client = boto3.client(
            service_name="s3",
            endpoint_url=minio_cfg.s3a_endpoint,
            aws_access_key_id=minio_cfg.access_key,
            aws_secret_access_key=minio_cfg.secret_key,
            config=Config(signature_version="s3v4"),
            verify=False,
        )

        # Graine aléatoire fixe : garantit que la simulation est
        # REPRODUCTIBLE. Chaque relancement génère exactement les mêmes
        # données. Important pour la cohérence du projet.
        random.seed(42)

        logger.info("Connexion MinIO établie.")

    def generate_simulated_observation(
        self,
        observation_datetime: datetime,
    ) -> dict:
        """
        Génère une observation météo simulée réaliste pour une heure donnée.

        MODÈLE DE SIMULATION :
            1. Température de base selon le mois (normales climatiques NYC)
            2. Variation gaussienne (random.gauss) : simule les fluctuations
               naturelles. La distribution gaussienne est réaliste car les
               températures suivent une courbe en cloche autour de la moyenne.
            3. Variation horaire : plus froid la nuit, plus chaud l'après-midi
            4. Conditions météo choisies aléatoirement selon les probabilités

        Args:
            observation_datetime: L'heure exacte de l'observation simulée

        Returns:
            Dictionnaire JSON avec la même structure que l'API OWM réelle
        """
        month = observation_datetime.month
        hour = observation_datetime.hour
        climate = NYC_CLIMATE[month]

        # ── Calcul de la température ──────────────────────────────────────
        # Température de base du mois + variation aléatoire gaussienne
        base_temp = climate["temp_mean"] + random.gauss(0, climate["temp_std"])

        # Cycle diurne : -3°C la nuit (2-5h), +3°C l'après-midi (14-16h)
        # cos() crée une courbe sinusoïdale sur 24h.
        # pi * (hour - 14) / 12 : décale le maximum à 14h (pic de chaleur)
        import math
        diurnal_variation = -3 * math.cos(math.pi * (hour - 14) / 12)
        temp = round(base_temp + diurnal_variation, 1)

        # Température ressentie : plus basse si vent fort
        feels_like = round(temp - random.uniform(1, 4), 1)

        # ── Choix des conditions météo ────────────────────────────────────
        rand = random.random()  # Nombre aléatoire entre 0 et 1

        if rand < climate["rain_prob"] * 0.1:
            category = "Orageux"
        elif rand < climate["rain_prob"]:
            category = "Pluvieux"
        elif rand < climate["rain_prob"] + 0.3:
            category = "Nuageux"
        else:
            category = "Clair"

        # Sélection aléatoire d'une condition dans la catégorie
        weather_main, weather_desc, weather_id = random.choice(
            WEATHER_CONDITIONS[category]
        )

        # ── Paramètres atmosphériques ─────────────────────────────────────
        humidity = random.randint(40, 90)
        wind_speed = round(random.uniform(1, 15), 1)
        wind_deg = random.randint(0, 359)
        pressure = random.randint(1005, 1030)
        visibility = random.randint(3000, 10000)
        clouds_pct = random.randint(0, 100)

        # ── Construction du JSON (structure identique à l'API OWM réelle) ─
        # Timestamps Unix = secondes depuis le 1er janvier 1970 (Epoch)
        # C'est le format utilisé par OpenWeatherMap
        timestamp_unix = int(observation_datetime.timestamp())

        return {
            "coord": {"lon": -74.006, "lat": 40.7128},  # Coordonnées NYC
            "weather": [{
                "id": weather_id,
                "main": weather_main,
                "description": weather_desc,
                "icon": "01d",
            }],
            "base": "simulated",
            "main": {
                "temp": temp,
                "feels_like": feels_like,
                "temp_min": round(temp - 1.5, 1),
                "temp_max": round(temp + 1.5, 1),
                "pressure": pressure,
                "humidity": humidity,
            },
            "visibility": visibility,
            "wind": {"speed": wind_speed, "deg": wind_deg},
            "clouds": {"all": clouds_pct},
            "dt": timestamp_unix,
            "sys": {"country": "US", "sunrise": 0, "sunset": 0},
            "timezone": -18000,   # UTC-5 (heure de NYC en hiver)
            "id": weather_cfg.city_id,
            "name": "New York",
            "cod": 200,           # Code HTTP 200 = succès (comme l'API réelle)
            # Métadonnées ajoutées par notre pipeline
            "_metadata": {
                "collected_at_iso": observation_datetime.isoformat(),
                "collected_at_ts": timestamp_unix,
                "city_id": weather_cfg.city_id,
                "city_name": weather_cfg.city_name,
                "pipeline_version": "2.0",
                "simulated": True,
                "weather_category": category,
            },
        }

    def get_s3_key(self, dt: datetime) -> str:
        """
        Calcule la clé S3 pour une observation donnée.

        STRATÉGIE DE PARTITIONNEMENT :
            On organise les fichiers par année/mois/jour.
            Cela permet à Spark de lire uniquement les données d'un jour
            ou d'un mois sans parcourir tout le dataset (partition pruning).

            Exemple : raw/weather/2024/01/15/weather_20240115_1400.json
                                   ^^^^  ^^  ^^                   ^^^^
                                   année mois jour                heure
        """
        return (
            f"{minio_cfg.raw_weather_prefix}/"
            f"{dt.year}/{dt.month:02d}/{dt.day:02d}/"
            f"weather_{dt.strftime('%Y%m%d_%H%M')}.json"
        )

    def upload_observation(self, dt: datetime, observation: dict) -> bool:
        """
        Uploade une observation JSON vers MinIO.

        Args:
            dt: Datetime de l'observation
            observation: Dictionnaire Python (sera converti en JSON)

        Returns:
            True si l'upload a réussi
        """
        s3_key = self.get_s3_key(dt)

        try:
            # json.dumps : convertit un dictionnaire Python en chaîne JSON
            # indent=2 : formatage lisible (2 espaces d'indentation)
            # ensure_ascii=False : permet les caractères non-ASCII (accents)
            json_content = json.dumps(observation, indent=2, ensure_ascii=False)

            self.s3_client.put_object(
                Bucket=minio_cfg.bucket,
                Key=s3_key,
                Body=json_content.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "observation_datetime": dt.isoformat(),
                    "weather_category": observation["_metadata"]["weather_category"],
                    "temperature_c": str(observation["main"]["temp"]),
                },
            )
            return True

        except Exception as e:
            logger.error("Erreur upload %s : %s", s3_key, e)
            return False

    def generate_date_range(self, start: datetime, end: datetime):
        """
        Générateur de datetimes heure par heure entre start et end.

        CONCEPT GÉNÉRATEUR Python :
            Un générateur (mot-clé yield) produit les valeurs une par une
            au lieu de toutes les calculer d'avance et les stocker en mémoire.

            Pour 2161 heures, la différence est minime.
            Mais pour 1 an de données (8760 heures), un générateur
            consomme ~0 Mo là où une liste consommerait des dizaines de Mo.
        """
        current = start
        while current <= end:
            yield current
            current += timedelta(hours=1)

    def run(self) -> bool:
        """
        Génère et uploade toutes les observations météo Jan-Mars 2024.
        """
        logger.info("╔═══════════════════════════════════════╗")
        logger.info("║  INGESTION MÉTÉO NYC Jan-Mars 2024    ║")
        logger.info("╚═══════════════════════════════════════╝")

        # Période : 1er janvier 2024 00h00 → 31 mars 2024 23h00
        start_dt = datetime(2024, 1, 1, 0, 0, 0)
        end_dt   = datetime(2024, 3, 31, 23, 0, 0)

        # Comptage préalable du nombre d'observations à générer
        total_hours = int((end_dt - start_dt).total_seconds() / 3600) + 1
        logger.info("Observations à générer : %d (3 mois × 24h)", total_hours)

        success_count = 0
        error_count = 0
        category_counts = {"Clair": 0, "Nuageux": 0, "Pluvieux": 0, "Orageux": 0}

        # Traitement heure par heure
        for i, dt in enumerate(self.generate_date_range(start_dt, end_dt)):

            # Log de progression toutes les 100 heures
            if i % 100 == 0:
                progress = (i / total_hours) * 100
                logger.info(
                    "Progression : %d/%d (%.0f%%) — %s",
                    i, total_hours, progress,
                    dt.strftime("%Y-%m-%d %H:00")
                )

            # Génération et upload de l'observation
            observation = self.generate_simulated_observation(dt)
            category = observation["_metadata"]["weather_category"]

            if self.upload_observation(dt, observation):
                success_count += 1
                category_counts[category] = category_counts.get(category, 0) + 1
            else:
                error_count += 1

        # ── Rapport final ─────────────────────────────────────────────────
        logger.info("")
        logger.info("╔═══════════════════════════════════════╗")
        logger.info("║           RAPPORT FINAL               ║")
        logger.info("╚═══════════════════════════════════════╝")
        logger.info("Fichiers uploadés   : %d", success_count)
        logger.info("Erreurs             : %d", error_count)
        logger.info("")
        logger.info("Distribution météo simulée :")
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
            pct = (count / success_count * 100) if success_count > 0 else 0
            logger.info("  %-10s : %4d observations (%.1f%%)", cat, count, pct)

        all_success = error_count == 0
        if all_success:
            logger.info("")
            logger.info("Ingestion météo terminée avec succès !")
        else:
            logger.error("%d observations ont échoué.", error_count)

        return all_success


# =============================================================================
# Point d'entrée
# =============================================================================

if __name__ == "__main__":
    ingester = WeatherIngester()
    success = ingester.run()
    sys.exit(0 if success else 1)
