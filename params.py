import json
import os

from dotenv import load_dotenv

### Environmental Variables

# Load .env from repository root
load_dotenv()

# Determine active environment (dev/prod)
environment = os.getenv("SURVIVOR_ENV", "dev").lower()
if environment not in {"dev", "prod"}:
    raise ValueError("SURVIVOR_ENV must be 'dev' or 'prod'")

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT")

### DB Table Config
with open("Database/table_config.json", "r") as f:
    table_config = json.load(f)

timestamp_columns = table_config.get("timestamp_columns", [])
boolean_columns = table_config.get("boolean_columns", [])

### DB Run Config
with open("Database/db_run_config.json", "r") as f:
    db_run_config = json.load(f)

first_run = db_run_config["first_run"]
truncate_on_load = db_run_config["truncate_on_load"]
bronze_schema = db_run_config.get("bronze_schema", "bronze")
source_config = db_run_config.get("source", {})
source_type = source_config.get("type", "github")
base_raw_url = source_config.get("base_raw_url")
json_raw_url = source_config.get("json_base_url")
if not json_raw_url and base_raw_url:
    json_raw_url = base_raw_url.rstrip("/").replace("/data", "/dev/json")
dataset_order = source_config.get("datasets", [])

pipeline_target = os.getenv(
    "GAMEBOT_TARGET_LAYER", db_run_config.get("target_layer", "gold")
).lower()
valid_layers = {"bronze", "silver", "gold"}
if pipeline_target not in valid_layers:
    raise ValueError(
        "GAMEBOT_TARGET_LAYER must be one of 'bronze', 'silver', or 'gold'"
    )
