#!/bin/bash
# =============================================================================
# docker/scheduler_start.sh
# Script de démarrage du Scheduler Airflow
# =============================================================================
#
# Le Scheduler surveille les DAGs et déclenche les tâches.
# Il doit démarrer APRES que le Webserver ait initialisé la base de données.
# On attend 90 secondes pour être sûr que le Webserver a fini son init.
# =============================================================================

set -e

echo "============================================"
echo "  Demarrage du Scheduler Airflow"
echo "============================================"

echo "[1/2] Attente que la base de données soit initialisée (90 secondes)..."
echo "  → Le Webserver est en train d'initialiser la base Airflow."
echo "  → Le Scheduler ne peut démarrer qu'après."
sleep 90

echo "[2/2] Lancement du scheduler..."
exec /home/airflow/.local/bin/airflow scheduler
