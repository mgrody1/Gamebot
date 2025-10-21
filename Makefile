# Makefile for Survivor Prediction / Gamebot Warehouse
# ----------------------------------------------------
# Provides unified commands for starting/stopping the Airflow + Postgres stack
# and syncing environment files between the root and Airflow subdirectory.

# Variables
PROJECT_NAME=airflow
COMPOSE_FILE=$(PROJECT_NAME)/docker-compose.yaml
ROOT_ENV=.env
AIRFLOW_ENV=$(PROJECT_NAME)/.env

# Default target
.DEFAULT_GOAL := help

# ---------------------------------------
# Environment sync
# ---------------------------------------
sync-env: ## Copy or regenerate airflow/.env from root .env
	@echo "Syncing environment files..."
	@if [ -f $(ROOT_ENV) ]; then \
		cp $(ROOT_ENV) $(AIRFLOW_ENV); \
		echo "Copied $(ROOT_ENV) -> $(AIRFLOW_ENV)"; \
	else \
		echo "No root .env found! Skipping."; \
	fi

# ---------------------------------------
# Docker Compose Commands
# ---------------------------------------
up: ## Bring up Airflow stack (init + detached)
	@echo "Starting Airflow stack..."
	cd $(PROJECT_NAME) && docker compose --env-file ../$(ROOT_ENV) up airflow-init
	cd $(PROJECT_NAME) && docker compose --env-file ../$(ROOT_ENV) up -d
	@echo "Airflow and Postgres services are up!"
	@echo "Visit http://localhost:$$(grep AIRFLOW_PORT $(ROOT_ENV) | cut -d '=' -f2 | tr -d '\r' || echo 8080)"

down: ## Stop and remove containers, networks, and volumes
	@echo "Stopping Airflow stack and cleaning up..."
	cd $(PROJECT_NAME) && docker compose down -v
	@echo "All containers and volumes removed."

logs: ## Tail logs for all Airflow services
	cd $(PROJECT_NAME) && docker compose logs -f

ps: ## List running services
	cd $(PROJECT_NAME) && docker compose ps

restart: down up ## Restart entire stack cleanly

clean: ## Full cleanup (containers, images, volumes)
	@echo "Cleaning up Docker environment..."
	cd $(PROJECT_NAME) && docker compose down -v --remove-orphans
	docker system prune -f
	docker volume prune -f
	@echo "Cleaned up Docker system."

# ---------------------------------------
# Utility
# ---------------------------------------
help: ## Show this help message
	@echo ""
	@echo "Survivor Prediction Makefile Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Usage:"
	@echo "  make sync-env     Sync root .env -> airflow/.env"
	@echo "  make up           Start Airflow + Postgres containers"
	@echo "  make down         Stop and remove the stack"
	@echo "  make logs         View Airflow logs"
	@echo "  make ps           Show running containers"
	@echo "  make clean        Remove containers, images, and volumes"
	@echo "  make restart      Restart from scratch"
	@echo ""
