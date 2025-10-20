from dotenv import load_dotenv
import os
import json

### Environmental Variabls

# Load environment variables from the .env file
load_dotenv()

# Create python variables from .env variables
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
port = os.getenv("PORT")

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
dataset_order = source_config.get("datasets", [])

