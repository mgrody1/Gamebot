# Makefile for Survivor Prediction / Gamebot Warehouse
# ----------------------------------------------------
# Provides unified commands for starting/stopping the Airflow + Postgres stack
# and syncing environment files between the root and Airflow subdirectory.

# Set environment variables for Docker Compose
export AIRFLOW_UID := $(shell id -u)

# Variables
PROJECT_NAME=airflow
COMPOSE_FILE=$(PROJECT_NAME)/docker-compose.yaml
ROOT_ENV=.env
AIRFLOW_ENV=$(PROJECT_NAME)/.env

# Default target
.DEFAULT_GOAL := help


# ---------------------------------------
# dbt Commands
# ---------------------------------------
dbt-clean: ## Clean dbt artifacts
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler dbt clean --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --log-path /tmp/dbt_logs --target-path /tmp/dbt_target

dbt-deps: ## Install dbt dependencies
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --log-path /tmp/dbt_logs --target-path /tmp/dbt_target

dbt-build: ## Build all dbt models (silver and gold layers)
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --log-path /tmp/dbt_logs --target-path /tmp/dbt_target

dbt-build-silver: ## Build only silver layer models
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select silver --log-path /tmp/dbt_logs --target-path /tmp/dbt_target

dbt-build-gold: ## Build only gold layer models
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select gold --log-path /tmp/dbt_logs --target-path /tmp/dbt_target

# ---------------------------------------
# Airflow Commands
# ---------------------------------------
airflow-dags-list: ## List all Airflow DAGs
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler airflow dags list

airflow-trigger: ## Trigger the survivor medallion pipeline
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler airflow dags trigger survivor_medallion_pipeline

airflow-clear: ## Clear failed tasks for today's date
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler airflow tasks clear survivor_medallion_pipeline --start-date $$(date +%Y-%m-%d) --end-date $$(date +%Y-%m-%d) --yes

# ---------------------------------------
# SQLite Export Commands
# ---------------------------------------
export-sqlite: ## Export all layers to SQLite for analysis
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler python /opt/airflow/scripts/export_sqlite.py --layer gold --output /tmp/gamebot_full.sqlite

export-sqlite-bronze: ## Export bronze layer only to SQLite
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler python /opt/airflow/scripts/export_sqlite.py --layer bronze --output /tmp/gamebot_bronze.sqlite

export-sqlite-silver: ## Export bronze+silver layers to SQLite
	cd $(PROJECT_NAME) && docker compose exec airflow-scheduler python /opt/airflow/scripts/export_sqlite.py --layer silver --output /tmp/gamebot_silver.sqlite

# ---------------------------------------
# Docker Compose Commands
# ---------------------------------------

# Helper function to check for production environment and confirm dangerous operations
check-production-delete:
	@if grep -q "^SURVIVOR_ENV=prod" $(ROOT_ENV) 2>/dev/null || \
	   git rev-parse --abbrev-ref HEAD 2>/dev/null | grep -qE "^(main|release/|data-release/)"; then \
		echo ""; \
		echo "WARNING: PRODUCTION ENVIRONMENT DETECTED"; \
		echo ""; \
		if grep -q "^SURVIVOR_ENV=prod" $(ROOT_ENV) 2>/dev/null; then \
			echo "  .env file has SURVIVOR_ENV=prod"; \
		fi; \
		if git rev-parse --abbrev-ref HEAD 2>/dev/null | grep -qE "^(main|release/|data-release/)"; then \
			echo "  Current branch: $$(git rev-parse --abbrev-ref HEAD 2>/dev/null)"; \
		fi; \
		echo ""; \
		echo "  This operation will PERMANENTLY DELETE:"; \
		echo "  - All containers and their data"; \
		echo "  - Database volumes (including survivor_dw_prod)"; \
		echo "  - Docker images"; \
		echo ""; \
		read -p "  Type 'DELETE' to confirm: " confirm; \
		if [ "$$confirm" != "DELETE" ]; then \
			echo ""; \
			echo "Operation cancelled."; \
			echo ""; \
			exit 1; \
		fi; \
		echo ""; \
		echo "✓ Confirmed. Proceeding with deletion..."; \
		echo ""; \
	fi

up: ## Bring up Airflow stack (init + detached)
	@echo "Starting Airflow stack..."
	@echo "Setting user permissions: AIRFLOW_UID=$$(id -u) (GID=0 required by Airflow)"
	cd $(PROJECT_NAME) && AIRFLOW_UID=$$(id -u) docker compose --env-file ../$(ROOT_ENV) build
	cd $(PROJECT_NAME) && AIRFLOW_UID=$$(id -u) docker compose --env-file ../$(ROOT_ENV) up airflow-init
	cd $(PROJECT_NAME) && AIRFLOW_UID=$$(id -u) docker compose --env-file ../$(ROOT_ENV) up -d
	@echo "Airflow and Postgres services are up!"
	@echo "Visit http://localhost:$$(grep AIRFLOW_PORT $(ROOT_ENV) | cut -d '=' -f2 | tr -d '\r' || echo 8080)"

down: ## Stop and remove containers, networks, and volumes
	@echo "Stopping Airflow stack and cleaning up..."
	cd $(PROJECT_NAME) && AIRFLOW_UID=$$(id -u) docker compose down
	@echo "All containers and volumes removed."

logs: ## Tail logs for all Airflow services
	cd $(PROJECT_NAME) && docker compose logs -f

ps: ## List running services
	cd $(PROJECT_NAME) && docker compose ps

restart: down up ## Restart entire stack cleanly

clean: ## Full cleanup (containers, images, volumes)
	@$(MAKE) check-production-delete
	@echo "Cleaning up Docker environment..."
	cd $(PROJECT_NAME) && AIRFLOW_UID=$$(id -u) AIRFLOW_GID=$$(id -g) docker compose down -v --remove-orphans
	docker system prune -f
	docker volume prune -f
	docker builder prune -f
	@echo "Cleaned up Docker system."

loader: ## Run the on-demand bronze loader profile container
	cd $(PROJECT_NAME) && AIRFLOW_UID=$$(id -u) AIRFLOW_GID=$$(id -g) docker compose --env-file ../$(ROOT_ENV) run --rm --profile loader survivor-loader

# Completely fresh Docker run: clean everything, then build and start
fresh: ## Remove all containers/images/volumes and start fresh stack
	@$(MAKE) check-production-delete
	$(MAKE) clean
	$(MAKE) up

# Tail the latest load_bronze_layer log for the survivor_medallion_pipeline DAG (runs inside airflow-worker container)
.PHONY: tail-bronze-log
tail-bronze-log:
	cd airflow && \
	docker compose exec airflow-worker bash -c "\
	latest_run_dir=\$$(ls -dt /opt/airflow/logs/dag_id=survivor_medallion_pipeline/run_id=* 2>/dev/null | head -n1) && \
	log_file=\"\$$latest_run_dir/task_id=load_bronze_layer/attempt=1.log\" && \
	if [ -f \"\$$log_file\" ]; then \
		echo \"==> \$$log_file <==\"; \
		tail -n 200 \"\$$log_file\"; \
	else \
		echo \"Log file not found: \$$log_file\"; \
	fi"

.PHONY: show-last-run
show-last-run: ## Display the most recent file under run_logs/ (use ARGS="--tail")
	pipenv run python scripts/show_last_run.py $(ARGS)

# ---------------------------------------
# Utility
# ---------------------------------------
help: ## Show this help message
	@echo ""
	@echo "Survivor Prediction Makefile Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Common workflows:"
	@echo "  make fresh           Start completely fresh environment"
	@echo "  make pipeline-fresh  Reset schemas and rebuild entire data pipeline"
	@echo "  make up              Start Airflow + Postgres containers"
	@echo "  make db-connect      Connect to database for manual queries"
	@echo "  make export-sqlite   Export all data layers to SQLite"
	@echo ""
