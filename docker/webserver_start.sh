#!/bin/bash
# =============================================================================
# docker/webserver_start.sh
# Script de démarrage du Webserver Airflow
# =============================================================================
#
# POURQUOI CE SCRIPT EXISTE :
#   Le conteneur Airflow officiel a besoin qu'on initialise sa base de données
#   avant de pouvoir démarrer. Ce script fait 3 choses dans l'ordre :
#   1. Initialise la base de données Airflow (tables, index, etc.)
#   2. Crée l'utilisateur administrateur
#   3. Lance le serveur web
#
# set -e : arrête le script immédiatement si une commande échoue.
#           Sans ça, le script continuerait même en cas d'erreur.
# =============================================================================

set -e

echo "============================================"
echo "  Demarrage du Webserver Airflow"
echo "============================================"

# Étape 1 : Initialisation de la base de données
# airflow db init crée toutes les tables nécessaires à Airflow
# (DAGs, TaskInstances, Logs, Variables, Connections...)
echo "[1/3] Initialisation de la base de données Airflow..."
/home/airflow/.local/bin/airflow db init

# Étape 2 : Création de l'utilisateur admin
# || true : si l'utilisateur existe déjà, ne pas planter
# (au redémarrage, l'utilisateur existe déjà → on ignore l'erreur)
echo "[2/3] Creation de l'utilisateur admin..."
/home/airflow/.local/bin/airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@nyc-pipeline.local \
  --password admin \
  || echo "  → Utilisateur admin existe deja, on continue."

# Étape 3 : Démarrage du serveur web
# exec remplace le processus shell par airflow webserver.
# C'est important : Docker surveille ce processus. Si on n'utilise pas exec,
# le shell est le processus principal et quand il se termine, le conteneur s'arrête.
echo "[3/3] Lancement du webserver..."
echo "  → Interface disponible sur http://localhost:8084"
echo "  → Identifiants : admin / admin"
exec /home/airflow/.local/bin/airflow webserver
