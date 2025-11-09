# Gamebot Lite

Gamebot Lite is a lightweight snapshot of the Survivor warehouse bundled as a SQLite database. It mirrors the `survivoR` R package experience so analysts can explore the data in python and python notebooks, while also including two additional layers of curated tables.


> **Note:** In Gamebot Lite, all tables in the SQLite database are stored with simple, friendly names (e.g., `castaway_details`, `ml_features_hybrid`).
> There are **no schema prefixes** like `bronze.`, `silver.`, or `gold.` in the actual SQLite file. These prefixes are only used in documentation and SQL examples for clarity.

> To see the exact table names and their schemas in your local Gamebot Lite snapshot, use:

```python
from gamebot_lite import GamebotClient, get_default_client
client = get_default_client()
print(client.list_tables())  # List all available tables
client.show_table_schema("castaway_details")  # Show columns/types for a table
```

Looking for the full warehouse documentation? See the [Gamebot Warehouse schema guide](gamebot_warehouse_schema_guide.md) and the [join cheat sheet + IDE tips](gamebot_warehouse_cheatsheet.md).

## Installation

For most users (analysts and notebook authors):

```bash
python -m pip install --upgrade gamebot-lite
```

If you need DuckDB features (for `duckdb_query` and other helpers), install the optional extra:

```bash
python -m pip install --upgrade "gamebot-lite[duckdb]"
```

Notebook / Jupyter-friendly cell (copy straight into a notebook cell):

```python
# Default install inside a notebook kernel
%pip install --upgrade gamebot-lite

# Or, include DuckDB support
%pip install --upgrade "gamebot-lite[duckdb]"
```

Notes:
- Use the plain `gamebot-lite` install by default; include the `duckdb` extra when you need SQL helper functions that rely on DuckDB and do not have DuckDB already installed.
- The `%pip` form is recommended inside notebook cells so the kernel picks up newly-installed packages immediately.

## Layers

- **Bronze (Raw)** – Direct exports of the survivoR tables (e.g., `bronze.castaway_details`, `bronze.season_summary`). These retain their original column names and provide provenance-friendly raw data.
- **Silver (Curated)** – Analytics-ready tables with clearer names (e.g., `silver.castaway_profile`, `silver.challenge_results_curated`). These consolidate joins and add derived columns.
- **Gold (ML Feature)** – Feature-layer snapshots (e.g., `gold.features_castaway_episode`) tailored for modeling and longitudinal analysis.

Inside the SQLite file every table is stored with its Gamebot Lite-friendly name. The Python helpers keep the warehouse layer visible: `load_table` accepts either `layer="silver"` or a fully-qualified identifier, and `duckdb_query` registers `bronze.*`, `silver.*`, and `gold.*` views automatically so layer prefixes stay in your SQL. Metadata about the export itself lives in `metadata.gamebot_ingestion_metadata`.

## Usage

```python
from gamebot_lite import GamebotClient, get_default_client, load_table, duckdb_query

# Pandas example (bronze layer) — explicit layer keeps intent obvious
df_castaways = load_table("castaway_details", layer="bronze")
print(df_castaways.head())

# You can also work with the client directly
client = get_default_client()
silver_tables = client.list_tables(layer="silver")

# DuckDB SQL example (requires `pip install duckdb`)
sql = """
SELECT
  gender,
  AVG(challenges_won) as avg_challenge_wins,
  AVG(vote_accuracy_rate) as avg_vote_accuracy,
  COUNT(*) as winner_count
FROM gold.ml_features_non_edit
WHERE target_winner = 1
GROUP BY gender
ORDER BY winner_count DESC
"""

df_winners = duckdb_query(sql)
print(df_winners)
```

Need a different SQLite file? Instantiate `GamebotClient(Path(...))` with your custom export path (`from pathlib import Path`).

Each dataframe returned by `load_table` includes `df.attrs["gamebot_layer"]` and `df.attrs["warehouse_table"]` so you can audit which warehouse object produced the data in downstream notebooks.

### Available table names

#### Bronze tables (Raw)

| Table | Description |
| --- | --- |
| `advantage_details` | Advantage inventory with type, owner, and metadata. |
| `advantage_movement` | Advantage lifecycle events (found, passed, played). |
| `boot_mapping` | Episode boot mapping to castaways and outcomes. |
| `castaway_details` | Master castaway information (matches survivoR). |
| `castaways` | Castaway-season relationship table. |
| `challenge_description` | Challenge catalog with type, recurring name, and description. |
| `challenge_results` | Raw challenge outcomes (team/individual). |
| `confessionals` | Raw confessional transcripts and metadata. |
| `episodes` | Episode metadata including numbers and air dates. |
| `jury_votes` | Raw jury vote outcomes. |
| `season_summary` | Season-level metadata. |
| `tribe_mapping` | Tribe membership timeline per castaway. |
| `vote_history` | Vote outcomes with round-by-round details. |
| `vote_history_extended` | Extended vote context (revotes, idols, etc.). |

#### Metadata

| Table | Description |
| --- | --- |
| `metadata.gamebot_ingestion_metadata` | Loader run metadata (environment, git details). |

Load metadata with `load_table("gamebot_ingestion_metadata", layer="metadata")` or via SQL `SELECT * FROM metadata.gamebot_ingestion_metadata`.

#### Silver tables (ML Feature Categories)

| Gamebot Lite table | Description |
| --- | --- |
| `advantage_strategy` | Advantage finding, playing, and strategic decision analysis. |
| `castaway_profile` | Demographics, background, and season context per castaway. |
| `challenge_performance` | Individual and team challenge performance across skill categories. |
| `edit_features` | Confessional counts, screen time, and edit presence indicators. |
| `jury_analysis` | Jury voting patterns and endgame relationship analysis. |
| `season_context` | Season-level meta-features, format changes, and cast composition. |
| `social_positioning` | Tribe composition, demographic dynamics, and social minority/majority tracking. |
| `vote_dynamics` | Voting behavior, tribal council strategy, and alliance patterns. |

#### Gold tables (ML-Ready Features)

| Table | Description |
| --- | --- |
| `ml_features_non_edit` | Complete gameplay features for pure strategic analysis (excludes edit/production signals). |
| `ml_features_hybrid` | Complete feature set combining gameplay + edit features for maximum prediction accuracy. |

Planned development: NLP features (sentiment analysis, topic modeling) for confessional text content will be added to silver models once text data is available.

### ML Feature Usage Examples

```python
# Load non-edit features for pure gameplay analysis
df_non_edit = load_table("ml_features_non_edit", layer="gold")

# Compare winner characteristics
winners = df_non_edit[df_non_edit['target_winner'] == 1]
print(f"Average challenge wins by winners: {winners['challenges_won'].mean()}")
print(f"Average vote accuracy by winners: {winners['vote_accuracy_rate'].mean()}")

# Load hybrid features for complete prediction model
df_hybrid = load_table("ml_features_hybrid", layer="gold")

# Analyze edit vs gameplay correlation with winning
import pandas as pd
correlations = df_hybrid[['target_winner', 'challenges_won', 'vote_accuracy_rate', 'total_confessional_count']].corr()['target_winner']
print(correlations)
```

### Strategic Feature Analysis Examples

```python
# Analyze challenge performance by skill category
challenge_perf = load_table("challenge_performance", layer="silver")

# Advantage strategy patterns
advantage_strat = load_table("advantage_strategy", layer="silver")

# Social positioning dynamics
social_pos = load_table("social_positioning", layer="silver")

# Voting behavior analysis
vote_dynamics = load_table("vote_dynamics", layer="silver")
```

## Keeping Data Fresh

**For developers with PiPY API access for the package**
1. Run `pipenv run python scripts/export_sqlite.py --layer silver --package --output gamebot_lite/data/gamebot.sqlite`.
2. Bump `pyproject.toml` version, build (`pipenv run python -m build`), and upload via twine.

**Users of gamebot-lite wanting the most recent available data**
1. Users can simply run `python -m pip install --upgrade gamebot-lite` to fetch the latest snapshot.
