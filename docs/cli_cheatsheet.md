# Gamebot CLI & Makefile Cheat Sheet

This page explains the most common commands for running, debugging, and managing the Gamebot data stack. It tells you exactly where to run each command, what it does, and when you might want to use it. If you're new to Docker, Airflow, or this project, start here.

## Where to Run Each Command

- **Local terminal**: Your regular command prompt (not inside VS Code's Dev Container). Use this for all `make` and `docker compose` commands.
- **Dev Container (`devshell`)**: VS Code now targets the `devshell` service defined in Docker Compose, so it shares the same Docker network and mounts as the runtime stack. Use it for editing, Python/dbt commands, and scripts like `setup_env.py`—but still run `make`/`docker compose` from the host terminal.
- **"Exec" into a container**: Some commands need to be run "inside" a running container. You do this from your local terminal using `docker compose exec ...`, which temporarily opens a shell or runs a command inside the container.

## Command Table

| Command                                      | Where to Run      | What It Does / When to Use                                                                                 | Will it delete my database? |
|-----------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------|-----------------------------|
| `make up`                                    | Local terminal    | Builds and starts Airflow + Postgres. Use to start the stack.                                              | No                          |
| `make down`                                  | Local terminal    | Stops and removes containers and networks, but keeps your database data (unless you run `clean`).          | No                          |
| `make logs`                                  | Local terminal    | Shows live logs from all Airflow services. Use to debug or watch progress.                                 | No                          |
| `make show-last-run ARGS="--tail"`           | Local terminal / Dev Container | Print the newest artefact in `run_logs/` (validation summary, schema drift, etc.). Add `--category` or `--pattern` to narrow results. | No |
| `make ps`                                    | Local terminal    | Lists running containers. Use to check if Airflow/Postgres are up.                                         | No                          |
| `make clean`                                 | Local terminal    | Stops everything and deletes all containers, images, and **volumes** (including your database).            | **Yes**                     |
| `make restart`                               | Local terminal    | Runs `make down` then `make up`. Use for a fresh start. Database is kept unless you ran `clean`.           | No                          |
| `make sync-env`                              | Local terminal    | Copies `.env` to `airflow/.env`. (`scripts/setup_env.py` runs this automatically when you switch profiles.) | No                          |
| `make loader`                                | Local terminal    | Runs the ETL loader container for a one-off data load. Use for manual/advanced runs.                       | No                          |
| `docker compose run --rm -e GAMEBOT_RUN_LOG_DIR=/workspace/run_logs -v $(pwd)/run_logs:/workspace/run_logs --profile loader survivor-loader` | Local terminal | Run the loader with host-mounted `run_logs/` so validation JSON/Excel artefacts persist outside the container. | No |
| `make airflow-dag-runs`                      | Local terminal    | Lists Airflow DAG runs (pipeline history). Use to check if your pipeline ran.                              | No                          |
| `docker compose logs -f airflow-scheduler`    | Local terminal    | Shows live logs from the Airflow scheduler container.                                                      | No                          |
| `docker compose logs -f airflow-worker`       | Local terminal    | Shows live logs from the Airflow worker container.                                                         | No                          |
| `docker compose exec airflow-scheduler bash`  | Local terminal    | Opens a shell inside the scheduler container. Use for advanced debugging.                                  | No                          |
| `docker compose exec airflow-scheduler airflow tasks logs <dag_id> <task_id> --latest` | Local terminal | Shows logs for the latest run of a specific Airflow task. Replace `<dag_id>` and `<task_id>`.              | No                          |
| `make show-last-run ARGS="--tail"`           | Local terminal / Dev Container | Print the newest artefact in `run_logs/` (validation summaries, schema drift, Excel paths, etc.). Add `--category` or `--pattern` to narrow results. | No |

## When to Use Each Command

- **make up**: Start the stack (do this first, or after a restart/clean).
- **make down**: Stop everything, but keep your data. Use before switching branches or if you want to free up resources.
- **make clean**: Use only if you want to delete everything, including your database. Good for a totally fresh start, but you will lose all data.
- **make restart**: Quick way to stop and start the stack. Keeps your data.
- **make logs**: Watch what's happening in Airflow/Postgres. Use to debug or see progress.
- **make loader**: Manually run the ETL loader (advanced/optional).
- **make airflow-dag-runs**: See if your pipeline ran and when.
- **docker compose exec ...**: Run a command inside a container. Needed for some Airflow CLI commands (see above).

## Notes for Beginners

- If you want your database to persist, avoid `make clean`.
- You can always stop and start the stack with `make down` and `make up` without losing data.
- If you get stuck, check logs with `make logs` or the `docker compose logs` commands.
- For Airflow CLI commands (like checking task logs), you always run them from your local terminal, but they execute inside the container.
- If you see errors about missing DAGs, try `make restart` after checking your code.

---

See the [README](../README.md) for a project overview and more links.
