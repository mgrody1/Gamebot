#!/bin/bash
# Gamebot Production Deployment Initialization Script
# This script sets up the necessary directory structure and permissions

set -e  # Exit on any error

echo "Initializing Gamebot Production Deployment..."

# Create required directories
echo "Creating run_logs directories..."
mkdir -p run_logs/validation run_logs/notifications

# Get AIRFLOW_UID from .env file, default to 50000
if [ -f .env ]; then
    AIRFLOW_UID=$(grep -E "^AIRFLOW_UID=" .env | cut -d'=' -f2)
    AIRFLOW_UID=${AIRFLOW_UID:-50000}
else
    echo "Warning: .env file not found, using default AIRFLOW_UID=50000"
    AIRFLOW_UID=50000
fi

echo "Setting ownership to UID ${AIRFLOW_UID}..."

# Set ownership - check if sudo is needed
if [ -w run_logs ]; then
    # We can write directly
    chown -R ${AIRFLOW_UID}:0 run_logs/
else
    # Need sudo
    echo "🔐 Requesting sudo access to set directory ownership..."
    sudo chown -R ${AIRFLOW_UID}:0 run_logs/
fi

echo "Initialization complete!"
echo ""
echo "Next steps:"
echo "  1. Review/edit .env file with your configuration"
echo "  2. Run: docker compose up -d"
echo "  3. Access Airflow UI at http://localhost:\${AIRFLOW_PORT:-8081}"
echo ""
