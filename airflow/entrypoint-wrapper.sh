#!/bin/bash
# Gamebot Airflow Entrypoint Script
# This script ensures proper permissions for mounted volumes before starting Airflow

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

# Branch protection check for production runs
# Only enforce when SURVIVOR_ENV=prod to prevent production pipeline runs on wrong branches
if [ "$SURVIVOR_ENV" = "prod" ]; then
    # Check if we're in a git repository with .git directory mounted
    if [ -d "/opt/airflow/.git" ]; then
        # Try to get current branch, handle detached HEAD and other edge cases
        CURRENT_BRANCH=$(cd /opt/airflow && git symbolic-ref --short HEAD 2>/dev/null || git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

        # If still unknown, try reading from .git/HEAD directly
        if [ "$CURRENT_BRANCH" = "unknown" ] || [ "$CURRENT_BRANCH" = "HEAD" ]; then
            if [ -f "/opt/airflow/.git/HEAD" ]; then
                CURRENT_BRANCH=$(cat /opt/airflow/.git/HEAD | sed 's/ref: refs\/heads\///')
            fi
        fi

        # Allow production runs only on main, release/*, or data-release/* branches
        if [[ ! "$CURRENT_BRANCH" =~ ^(main|release/|data-release/) ]]; then
            echo "ERROR: Production runs (SURVIVOR_ENV=prod) are only allowed on:"
            echo "  - main"
            echo "  - release/*"
            echo "  - data-release/*"
            echo ""
            echo "Current branch: $CURRENT_BRANCH"
            echo ""
            echo "To fix this:"
            echo "  1. Switch to a release branch: git checkout main"
            echo "  2. Or use SURVIVOR_ENV=dev for development work"
            exit 1
        else
            echo "✓ Branch protection: Production run allowed on branch '$CURRENT_BRANCH'"
        fi
    else
        echo "Warning: .git directory not mounted, skipping branch protection check"
    fi
fi

# Execute the original Airflow entrypoint with all arguments
exec /entrypoint "$@"
