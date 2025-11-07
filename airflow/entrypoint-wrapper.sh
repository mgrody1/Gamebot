#!/bin/bash
# Gamebot Airflow Entrypoint Script
# This script ensures proper permissions for mounted volumes before starting Airflow
# Industry standard approach used by official Docker images

set -e

# Create run_logs directory structure if it doesn't exist
# and ensure airflow user can write to it
if [ -d "/opt/airflow/run_logs" ]; then
    echo "Ensuring run_logs directory permissions..."

    # Create subdirectories if they don't exist
    mkdir -p /opt/airflow/run_logs/validation
    mkdir -p /opt/airflow/run_logs/notifications

    # Fix ownership if we have root privileges
    # This runs during container startup before switching to airflow user
    if [ "$(id -u)" = "0" ]; then
        chown -R airflow:0 /opt/airflow/run_logs
        echo "✓ Permissions fixed for run_logs"
    fi
fi

# Execute the original Airflow entrypoint with all arguments
exec /entrypoint "$@"
